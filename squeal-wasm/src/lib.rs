//! The wasm-facing shim: `squeal-cli`'s equivalent for a browser, not a
//! modification of it — `squeal-cli` itself can't compile for wasm32 at all
//! (`rustyline` pulls in a `home` crate with no wasm32 implementation; see
//! this session's own investigation). Every SQL-executing call here is
//! synchronous, matching how the engine itself now runs on wasm32 (see
//! store's clock/logger/buffer/maintenance changes) — there is no async
//! boundary to cross, so none is exposed.
//!
//! Only `MemFile` is wired up: the real-file `DBFile` backend doesn't exist
//! on wasm32 (see store::memfile's own `#[cfg(not(target_arch = "wasm32"))]`
//! gate on `impl Opener for std::fs::File`), and there is nothing durable
//! to persist to yet regardless (IndexedDB is a later option, not this).
//! Every database created here is fully in-memory and gone once the
//! `SquealDb` handle is dropped or the page unloads.

use std::cell::RefCell;
use std::sync::Arc;

use serde::Serialize;
use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::{ResultSet, ResultType, StreamingResultSet},
    source::QueryStats,
};
use store::memfile::MemFile;
use wasm_bindgen::prelude::*;

const DEFAULT_SCHEMA: &str = "default";

// Runs once, automatically, the moment the wasm module is instantiated —
// no JS-side call needed. console_error_panic_hook turns "the whole page
// silently stops responding" into an actual message in devtools (the
// default wasm32 panic hook writes nowhere visible); console_log wires
// this crate's own `log` dependency AND, transitively, every log::error!/
// log::trace! call already in store/squeal-sql (see e.g. store/src/
// logger.rs, arclock.rs) to the browser console — both are swapping the
// *backend* behind an abstraction those crates already use, not adding
// logging calls anywhere.
#[wasm_bindgen(start)]
fn init() {
    #[cfg(target_arch = "wasm32")]
    {
        console_error_panic_hook::set_once();
        let _ = console_log::init_with_level(log::Level::Warn);
    }
}

// One JS-visible handle per open (in-memory, ephemeral) database. Holds its
// own `ConnectionManager` rather than sharing the process-wide native
// singleton (`ConnectionManager::<File>::get_manager`, gated off wasm32
// entirely — see conn::connection's own comment) — there is exactly one
// wasm module instance per page anyway, so "process-wide" and "per-SquealDb"
// coincide for the common case of one open database, and nothing stops a
// page from creating more than one under different names.
#[wasm_bindgen]
pub struct SquealDb {
    conn: Arc<Connection<MemFile>>,
    // Stats from the most recently executed StreamingResult, so `!print
    // stats` can be sent as its own, separate `execute()` call rather than
    // needing to be bolted onto the query itself — mirrors squeal-cli's
    // own `last_stats` local. A RefCell (not a plain field) because every
    // `#[wasm_bindgen]` method here takes `&self`, not `&mut self` — JS
    // only ever sees one handle to a given SquealDb, so there's no real
    // aliasing risk, just Rust's borrow rules needing satisfying.
    last_stats: RefCell<Option<Vec<(String, QueryStats)>>>,
}

#[wasm_bindgen]
impl SquealDb {
    /// Creates a fresh, empty, in-memory database and connects to it.
    /// `name` only needs to be unique within this page — nothing is ever
    /// read back from a previous session (see this module's own doc
    /// comment on why there's nothing to persist to yet).
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Result<SquealDb, JsError> {
        let mgr: Arc<ConnectionManager<MemFile>> = Arc::new(ConnectionManager::new());
        let conn = mgr.create_and_connect(name).map_err(to_js_error)?;
        // Lands on a usable schema immediately, matching squeal-cli's own
        // bootstrap — CREATE TABLE etc. work right away without the JS
        // side needing to know to issue USE SCHEMA first. Not fatal if
        // this fails for some reason: the connection is still usable, it
        // would just need an explicit USE/CREATE SCHEMA call first.
        let _ = conn.use_schema(DEFAULT_SCHEMA);
        Ok(SquealDb {
            conn,
            last_stats: RefCell::new(None),
        })
    }

    /// Runs `sql` and returns every result as one JSON array, one entry per
    /// statement that produced a result — see `execute_results`' own doc
    /// comment for the shape of each entry. Throws (a JS exception, via
    /// `JsError`) if the batch fails to parse/validate at all, or if any
    /// statement in it fails; whatever ran before the failing statement has
    /// already taken effect (matches Statement::execute's own
    /// all-or-nothing-only-at-parse-time contract — see its own doc comment
    /// in stmt.rs).
    ///
    /// If `sql` (after trimming) starts with `!`, it's dispatched as a
    /// REPL-style command instead of parsed as SQL — same `!help`/`!print
    /// stats`/`!reset stats`/`!show table stats` set squeal-cli supports
    /// (see squeal-cli's own `COMMANDS`), for parity between the two
    /// front-ends. Always exactly one `sql` per call for these — unlike
    /// plain SQL, a `!` command is never batched with other statements.
    pub fn execute(&self, sql: &str) -> Result<String, JsError> {
        let trimmed = sql.trim();
        if let Some(command) = trimmed.strip_prefix('!') {
            let result = run_custom_command(command.trim(), &self.conn, &self.last_stats);
            return serde_json::to_string(&[result])
                .map_err(|e| JsError::new(&format!("failed to serialize results: {e}")));
        }

        let (results, stats) = execute_results(&self.conn, sql).map_err(to_js_error)?;
        if stats.is_some() {
            *self.last_stats.borrow_mut() = stats;
        }
        serde_json::to_string(&results)
            .map_err(|e| JsError::new(&format!("failed to serialize results: {e}")))
    }
}

// One JSON-serializable entry per ResultType a statement produced — the
// same four shapes `squeal-cli::print_result` renders as a table/line of
// text, here as data instead. `#[serde(tag = "kind")]` so JS can switch on
// `result.kind` without also needing to know which fields go with which
// variant.
#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum JsonResult {
    /// INSERT/UPDATE/DELETE.
    Count { rows_affected: usize },
    /// CREATE TABLE, USE SCHEMA, BEGIN/COMMIT/ROLLBACK, ... — anything
    /// whose outcome is a human-readable line, not rows.
    Message { text: String },
    /// SHOW/DESCRIBE and friends — materialized eagerly, unlike Rows below.
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        message: String,
    },
    /// SELECT — drained fully here (there is no async/streaming boundary
    /// to hand a live cursor across into JS), but built the same
    /// column-then-row-at-a-time way StreamingResultSet already streams
    /// internally.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        message: String,
    },
}

// Returns every JSON-shaped result alongside whichever StreamingResult's
// query stats were seen last (a statement can produce several results;
// same "last one wins" rule squeal-cli's own `run()` uses) — `None` if
// nothing in this batch was a SELECT at all, in which case the caller
// leaves SquealDb::last_stats untouched rather than clobbering it.
fn execute_results(
    conn: &Arc<Connection<MemFile>>,
    sql: &str,
) -> Result<(Vec<JsonResult>, Option<Vec<(String, QueryStats)>>), squeal_sql::error::SchemaError> {
    let mut stmt = conn.clone().create_statement(sql)?;
    stmt.execute()?;
    let mut out = vec![];
    let mut stats = None;
    let mut next = stmt.get_results();
    loop {
        match next? {
            Some(r) => {
                let (json, s) = to_json_result(r)?;
                out.push(json);
                if s.is_some() {
                    stats = s;
                }
                next = stmt.get_nextresult();
            }
            None => break,
        }
    }
    Ok((out, stats))
}

fn to_json_result(
    r: ResultType,
) -> Result<(JsonResult, Option<Vec<(String, QueryStats)>>), squeal_sql::error::SchemaError> {
    Ok(match r {
        ResultType::Count(n) => (JsonResult::Count { rows_affected: n }, None),
        ResultType::ResultString(text) => (JsonResult::Message { text }, None),
        ResultType::Result(rs) => {
            let (columns, rows, message) = drain_materialized(rs);
            (
                JsonResult::Result {
                    columns,
                    rows,
                    message,
                },
                None,
            )
        }
        ResultType::StreamingResult(mut stream) => {
            let (columns, rows) = drain_streaming(&mut stream)?;
            let message = stream.get_final_message();
            // Only available once the stream is fully drained (see
            // drain_streaming above) — Source::stats() reports totals
            // accumulated during next(), so reading it any earlier would
            // miss whatever work the remaining rows still had to do.
            let stats = stream.get_query_stats();
            (
                JsonResult::Rows {
                    columns,
                    rows,
                    message,
                },
                stats,
            )
        }
    })
}

// The full set of `!`-commands this shim understands, as (syntax,
// description) pairs — mirrors squeal-cli's own `COMMANDS` const (kept as
// a separate, duplicated list rather than a shared one: squeal-cli's own
// comment on why this stays a flat match, not a registry, applies here
// too — four short strings isn't worth a cross-crate API for).
const COMMANDS: &[(&str, &str)] = &[
    ("!help", "show this list of commands"),
    (
        "!print stats",
        "show per-operator timing/row-count stats from the last query",
    ),
    ("!reset stats", "zero the allocator stats counters"),
    (
        "!show table stats",
        "show collected table statistics for the current schema",
    ),
];

// Dispatches a `!`-prefixed command (stripped of that prefix and
// trimmed) — see SquealDb::execute's own doc comment for how it gets
// here. Always returns a JsonResult, never errors: an unrecognized
// command is reported as a Message, not a thrown JsError, so `!nonsense`
// behaves the same as it does in squeal-cli (a printed line, not a
// crash).
fn run_custom_command(
    command: &str,
    conn: &Arc<Connection<MemFile>>,
    last_stats: &RefCell<Option<Vec<(String, QueryStats)>>>,
) -> JsonResult {
    const USAGE_HINT: &str = "try '!help' for a list of commands";
    match command {
        "help" => JsonResult::Message { text: help_text() },
        "print stats" => JsonResult::Message {
            text: print_query_stats_text(&last_stats.borrow()),
        },
        "reset stats" => {
            store::alloc::reset();
            JsonResult::Message {
                text: "allocator stats reset".to_string(),
            }
        }
        "show table stats" => match conn.table_stats_report() {
            Ok(rs) => {
                let (columns, rows, message) = drain_materialized(rs);
                JsonResult::Result {
                    columns,
                    rows,
                    message,
                }
            }
            Err(e) => JsonResult::Message {
                text: format!("error: {e}"),
            },
        },
        "" => JsonResult::Message {
            text: format!("empty command — {USAGE_HINT}"),
        },
        other => JsonResult::Message {
            text: format!("unrecognized command: {other:?} — {USAGE_HINT}"),
        },
    }
}

fn help_text() -> String {
    let width = COMMANDS.iter().map(|(cmd, _)| cmd.len()).max().unwrap_or(0);
    let mut out = String::from("Available commands:\n");
    for (cmd, desc) in COMMANDS {
        out += &format!("  {cmd:<width$}  {desc}\n");
    }
    out.pop(); // drop the trailing newline — JsonResult::Message renders as one block
    out
}

// Mirrors squeal-cli's own print_query_stats, building a String instead
// of printing line-by-line.
fn print_query_stats_text(stats: &Option<Vec<(String, QueryStats)>>) -> String {
    let Some(stats) = stats else {
        return "no query stats available yet — run a query first".to_string();
    };
    let mut out = String::new();
    for (name, query_stats) in stats {
        let indent = "  ".repeat(query_stats.level());
        out += &format!("{indent}{name}\n");
        let mut entries: Vec<_> = query_stats.stats().iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in entries {
            if let Some(label) = key.strip_suffix("_ns") {
                out += &format!("{indent}  {label}: {:.3} ms\n", value / 1_000_000.0);
            } else {
                out += &format!("{indent}  {key}: {value}\n");
            }
        }
    }
    out.pop();
    out
}

fn drain_materialized(rs: ResultSet) -> (Vec<String>, Vec<Vec<String>>, String) {
    let columns = rs.columns().to_vec();
    let rows = rs.rows_as_strings();
    let message = rs.get_final_message();
    (columns, rows, message)
}

fn drain_streaming(
    stream: &mut StreamingResultSet,
) -> Result<(Vec<String>, Vec<Vec<String>>), squeal_sql::error::SchemaError> {
    let columns = stream.columns();
    let mut rows = vec![];
    while let Some(row) = stream.next_result_as_strings()? {
        rows.push(row);
    }
    Ok((columns, rows))
}

fn to_js_error(e: squeal_sql::error::SchemaError) -> JsError {
    JsError::new(&e.to_string())
}

// Exercised by plain `cargo test -p squeal-wasm` off wasm32 — with one
// exception: JsError::new (used at the SchemaError -> JsError boundary,
// i.e. any `db.execute(...)` call that errors) calls an actual
// wasm-bindgen *imported* function that constructs a real JS Error, not
// something wasm-bindgen's macros can synthesize on their own — that
// specific path panics natively ("cannot call wasm-bindgen imported
// functions on non-wasm targets"), so error cases are verified one layer
// down, at execute_results, instead of through the public db.execute(...)
// API. That the JsError conversion and the actual JS-side throw behave
// correctly is therefore only checked by a real wasm-bindgen-test run
// (not done in this environment — no wasm-pack/wasmtime available here;
// see this session's own earlier note), same caveat as the store/
// squeal-sql wasm32 clock work before this.
#[cfg(test)]
mod tests {
    use super::*;

    // JsonResult is Serialize-only by design (this crate's boundary only
    // ever needs to write it; JS reads it, nothing here reads it back as
    // that type) — tests parse the JSON generically instead.
    fn exec(db: &SquealDb, sql: &str) -> Vec<serde_json::Value> {
        let json = db.execute(sql).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    fn kinds(results: &[serde_json::Value]) -> Vec<&str> {
        results
            .iter()
            .map(|r| r["kind"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn test_new_lands_on_a_usable_schema_immediately() {
        let db = SquealDb::new("t1").unwrap();
        let json = db
            .execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        let v: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(kinds(&v), ["Message"]);
    }

    #[test]
    fn test_insert_reports_count_and_select_reports_rows() {
        let db = SquealDb::new("t2").unwrap();
        db.execute("create table t (id integer not null, name varchar(20), primary key(id))")
            .unwrap();
        let json = db.execute("insert into t values (1, 'alice')").unwrap();
        let v: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(kinds(&v), ["Count"]);
        assert_eq!(v[0]["rows_affected"], 1);

        let json = db.execute("select id, name from t").unwrap();
        let v: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(kinds(&v), ["Rows"]);
        assert_eq!(v[0]["columns"], serde_json::json!(["id", "name"]));
        assert_eq!(v[0]["rows"], serde_json::json!([["1", "alice"]]));
    }

    #[test]
    fn test_a_batch_of_statements_returns_one_result_per_statement() {
        let db = SquealDb::new("t3").unwrap();
        let json = db
            .execute(
                "create table t (id integer not null, primary key(id)); \
                 insert into t values (1); \
                 select id from t;",
            )
            .unwrap();
        let v: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(kinds(&v), ["Message", "Count", "Rows"]);
    }

    // Not `db.execute(...)`: JsError::new calls an actual wasm-bindgen
    // *imported* function (it constructs a real JS Error, not just a value
    // wasm-bindgen's macros can synthesize on their own) — unlike the rest
    // of this crate, that specific path genuinely cannot run outside a
    // wasm+JS host, and panics under plain `cargo test` ("cannot call
    // wasm-bindgen imported functions on non-wasm targets"). So the
    // error-message contract is verified at `execute_results`, the
    // pre-JsError boundary; that `db.execute(...)` on the same input
    // reaches `to_js_error` and throws correctly is only checked by an
    // actual wasm-bindgen-test run (not done in this environment — see
    // this session's own earlier note on lacking wasm-pack/wasmtime here).
    #[test]
    fn test_a_bad_statement_is_reported_as_an_error_with_a_useful_message() {
        let db = SquealDb::new("t4").unwrap();
        let err = execute_results(&db.conn, "select * from nope").unwrap_err();
        assert!(err.to_string().to_lowercase().contains("nope"), "{err}");
    }

    #[test]
    fn test_two_databases_are_independent() {
        let a = SquealDb::new("iso-a").unwrap();
        let b = SquealDb::new("iso-b").unwrap();
        a.execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        // b never created "t" — a query against it must fail, proving the
        // two SquealDb handles aren't secretly sharing one database. Via
        // execute_results, not db.execute(...) — see the comment on the
        // bad-statement test above for why.
        assert!(execute_results(&b.conn, "select * from t").is_err());
    }

    #[test]
    fn test_null_and_special_values_round_trip_as_strings() {
        let db = SquealDb::new("t5").unwrap();
        db.execute("create table t (id integer not null, n integer, primary key(id))")
            .unwrap();
        db.execute("insert into t values (1, null)").unwrap();
        let results = exec(&db, "select id, n from t");
        assert_eq!(results[0]["rows"][0][1], serde_json::json!("(null)"));
    }

    #[test]
    fn test_help_lists_every_command_with_its_syntax() {
        let db = SquealDb::new("t6").unwrap();
        let results = exec(&db, "!help");
        assert_eq!(kinds(&results), ["Message"]);
        let text = results[0]["text"].as_str().unwrap();
        for (cmd, _) in COMMANDS {
            assert!(text.contains(cmd), "missing {cmd:?} in:\n{text}");
        }
    }

    #[test]
    fn test_print_stats_before_any_query_says_so_rather_than_erroring() {
        let db = SquealDb::new("t7").unwrap();
        let results = exec(&db, "!print stats");
        assert_eq!(kinds(&results), ["Message"]);
        assert!(results[0]["text"]
            .as_str()
            .unwrap()
            .contains("no query stats available"));
    }

    #[test]
    fn test_print_stats_reports_the_most_recent_select() {
        let db = SquealDb::new("t8").unwrap();
        db.execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        db.execute("insert into t values (1)").unwrap();
        exec(&db, "select id from t");
        let results = exec(&db, "!print stats");
        assert_eq!(kinds(&results), ["Message"]);
        assert!(results[0]["text"].as_str().unwrap().contains("TableScan"));
    }

    #[test]
    fn test_show_table_stats_returns_a_table_not_a_message() {
        let db = SquealDb::new("t9").unwrap();
        db.execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        let results = exec(&db, "!show table stats");
        assert_eq!(kinds(&results), ["Result"]);
    }

    #[test]
    fn test_reset_stats_confirms_rather_than_erroring() {
        let db = SquealDb::new("t10").unwrap();
        let results = exec(&db, "!reset stats");
        assert_eq!(kinds(&results), ["Message"]);
        assert!(results[0]["text"].as_str().unwrap().contains("reset"));
    }

    #[test]
    fn test_an_unrecognized_bang_command_is_reported_not_thrown() {
        let db = SquealDb::new("t11").unwrap();
        let results = exec(&db, "!nonsense");
        assert_eq!(kinds(&results), ["Message"]);
        let text = results[0]["text"].as_str().unwrap();
        assert!(text.contains("unrecognized") && text.contains("nonsense"), "{text}");
    }
}
