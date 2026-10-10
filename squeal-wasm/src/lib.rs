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
//! gate on `impl Opener for std::fs::File`). Every database is in memory;
//! persistence is whole-database snapshots — `snapshot()` hands JS the
//! committed state as bytes to store wherever it likes (www/persist.js
//! uses IndexedDB), `SquealDb.fromSnapshot(bytes)` reopens it. Meant for
//! small databases in a demo/scratchpad, not as a server database.

use std::cell::RefCell;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::{ResultSet, ResultType, StreamingResultSet},
    source::QueryStats,
};
use sq_json::{
    Client,
    shell::{Outcome, Shell},
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
    name: String,
    conn: Arc<Connection<MemFile>>,
    // Stats from the most recently executed StreamingResult, so `!print
    // stats` can be sent as its own, separate `execute()` call rather than
    // needing to be bolted onto the query itself — mirrors squeal-cli's
    // own `last_stats` local. A RefCell (not a plain field) because every
    // `#[wasm_bindgen]` method here takes `&self`, not `&mut self` — JS
    // only ever sees one handle to a given SquealDb, so there's no real
    // aliasing risk, just Rust's borrow rules needing satisfying.
    last_stats: RefCell<Option<Vec<(String, QueryStats)>>>,
    // The document (JSON) side: sq-json's mongosh-style shell, over the
    // same store database as the SQL connection — so one snapshot holds
    // tables and collections both. Started on first use (see `json`).
    json: RefCell<Option<Shell<MemFile>>>,
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
            name: name.to_string(),
            conn,
            last_stats: RefCell::new(None),
            json: RefCell::new(None),
        })
    }

    /// Reopens a database from bytes a previous `snapshot()` returned
    /// (possibly in an earlier page load), under the name it was saved
    /// with.
    #[wasm_bindgen(js_name = fromSnapshot)]
    pub fn from_snapshot(bytes: &[u8]) -> Result<SquealDb, JsError> {
        restore(bytes).map_err(|e| JsError::new(&e))
    }

    /// The database's committed state as one byte array: its data file
    /// and WAL segments as of the last commit (`Db::synced_snapshot`), so
    /// an open transaction's uncommitted writes are never included. The
    /// whole database every time — fine for the small databases this
    /// crate is for.
    pub fn snapshot(&self) -> Result<Vec<u8>, JsError> {
        snapshot_bytes(self).map_err(|e| JsError::new(&e))
    }

    /// A number that changes whenever committed data changes (see
    /// store::memfile::sync_generation). Read it before `snapshot()`;
    /// if it differs later, there are changes that snapshot doesn't have.
    /// Reads don't move it.
    #[wasm_bindgen(js_name = syncGeneration)]
    pub fn sync_generation(&self) -> f64 {
        store::memfile::sync_generation() as f64
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

    /// Runs document (JSON) statements — sq-json's mongosh-style shell:
    /// `db.orders.insertOne({...})`, `db.orders.find({qty: {$gt: 5}})`,
    /// `use shop`, `show collections`, `begin`/`commit`, `help`. Several
    /// may be given at once, separated by `;` or by line breaks (a line
    /// that starts with `.` continues the statement before it, as in
    /// `db.c.find({})` then `.sort({a: 1})`). Returns the same JSON-array
    /// shape `execute()` does: one `Message` per statement, its text what
    /// the shell printed (documents as JSON, one per line). Throws at the
    /// first statement that fails, naming it; the ones before it have
    /// taken effect.
    ///
    /// Collections live in the same store as the SQL tables (as `sqjson.*`
    /// store tables, under their own catalog — a SQL schema named
    /// `sqjson` would collide with them), so `snapshot()` saves both.
    #[wasm_bindgen(js_name = executeJson)]
    pub fn execute_json(&self, input: &str) -> Result<String, JsError> {
        let out = self.run_json(input).map_err(|e| JsError::new(&e))?;
        serde_json::to_string(&out)
            .map_err(|e| JsError::new(&format!("failed to serialize results: {e}")))
    }

    /// The JSON shell's prompt: its current database, and `(txn)` while a
    /// transaction is open — e.g. `test> `.
    #[wasm_bindgen(js_name = jsonPrompt)]
    pub fn json_prompt(&self) -> String {
        match &*self.json.borrow() {
            Some(shell) => shell.prompt(),
            None => "test> ".into(),
        }
    }

    /// Every help text the page shows, as JSON — taken from the engines
    /// themselves, so the page can't drift from what they accept:
    /// `{ commands: [[cmd, what]], sql: [{ title, entries: [[syntax,
    /// what]] }], json: "<sq-json's help>" }`.
    pub fn help() -> String {
        let sql: Vec<serde_json::Value> = squeal_sql::help::SQL_HELP
            .iter()
            .map(|s| serde_json::json!({ "title": s.title, "entries": s.entries }))
            .collect();
        serde_json::json!({
            "commands": COMMANDS,
            "sql": sql,
            "json": sq_json::shell::HELP,
        })
        .to_string()
    }

    /// Non-SQL: infers a table's columns/types from `csv_text` (a whole
    /// CSV document, header row included), creates `table_name` with
    /// them, and loads every row — this crate's own way to offer `CREATE
    /// TABLE <name> AS COPY FROM @<path>`'s capability (see that
    /// statement's own grammar/dispatch in squeal-sql) where `@path`
    /// can't mean anything at all: a browser tab has no filesystem to
    /// resolve one against. `csv_text` comes from wherever the JS side
    /// itself read it — a `File` object's own `.text()`, a `fetch()`
    /// response, a `<textarea>`, anything — no path/file I/O happens on
    /// this side of the call at all. Returns the same one-element
    /// JSON-array shape `execute()` does (a single `Message`), so
    /// existing JS-side result handling doesn't need a second code path.
    ///
    /// Exposed to JS as `createTableFromCsv` — the same name ws-napi's
    /// napi-rs binding gives its own method (napi-rs camelCases by
    /// default; wasm-bindgen doesn't), so one capability has one
    /// spelling across both JS APIs.
    #[wasm_bindgen(js_name = createTableFromCsv)]
    pub fn create_table_from_csv(
        &self,
        table_name: &str,
        csv_text: &str,
    ) -> Result<String, JsError> {
        let (loaded, failed) = self
            .conn
            .create_table_from_csv(table_name, csv_text)
            .map_err(to_js_error)?;
        // Same wording as CREATE TABLE ... AS COPY's own SQL-dispatched
        // result message (squeal-sql's stmt.rs) — duplicated, not
        // shared, matching this crate's own established precedent for
        // small front-end-specific formatting (see COMMANDS below).
        let text = format!(
            "Table {table_name:?} created, {loaded} row(s) loaded{}",
            if failed > 0 {
                format!(", {failed} row(s) failed")
            } else {
                String::new()
            }
        );
        serde_json::to_string(&[JsonResult::Message { text }])
            .map_err(|e| JsError::new(&format!("failed to serialize results: {e}")))
    }
}

impl SquealDb {
    // execute_json's work, its error as text (a JsError can't be made off
    // wasm32, where the tests run).
    fn run_json(&self, input: &str) -> Result<Vec<JsonResult>, String> {
        let mut json = self.json.borrow_mut();
        let shell = match &mut *json {
            Some(shell) => shell,
            None => {
                let client = Client::start(self.conn.store()).map_err(|e| e.to_string())?;
                json.insert(Shell::new(client))
            }
        };
        let mut out = vec![];
        for statement in split_json_statements(input) {
            match shell.execute(&statement) {
                Ok(Outcome::Output(text)) => out.push(JsonResult::Message {
                    text: if text.is_empty() { "ok".into() } else { text },
                }),
                // There is no shell to leave: the page stays.
                Ok(Outcome::Exit) => out.push(JsonResult::Message {
                    text: "(nothing to exit — this is a page; close the tab to leave)".into(),
                }),
                Err(e) => return Err(format!("{statement}\n  error: {e}")),
            }
        }
        Ok(out)
    }
}

// Document statements in `input`, one per entry: split at a `;` or a line
// break outside brackets and quotes — except before a line whose first
// non-blank character is `.`, which continues the statement (a chained
// `.sort(...)`). Empty pieces are dropped.
fn split_json_statements(input: &str) -> Vec<String> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = vec![];
    let mut current = String::new();
    let (mut depth, mut quote): (i32, Option<char>) = (0, None);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match quote {
            Some(q) => {
                current.push(c);
                if c == '\\' {
                    if let Some(&next) = chars.get(i + 1) {
                        current.push(next);
                        i += 1;
                    }
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    current.push(c);
                }
                '(' | '[' | '{' => {
                    depth += 1;
                    current.push(c);
                }
                ')' | ']' | '}' => {
                    depth -= 1;
                    current.push(c);
                }
                ';' if depth <= 0 => {
                    out.push(std::mem::take(&mut current));
                }
                '\n' if depth <= 0 => {
                    let continues = chars[i + 1..]
                        .iter()
                        .find(|c| !c.is_whitespace())
                        .is_some_and(|c| *c == '.');
                    if continues {
                        current.push(c);
                    } else {
                        out.push(std::mem::take(&mut current));
                    }
                }
                _ => current.push(c),
            },
        }
        i += 1;
    }
    out.push(current);
    out.into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// What snapshot() produces and fromSnapshot() reads back. `version` so a
// future format change refuses an old saved blob instead of misreading it.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    name: String,
    data: Vec<u8>,
    wal: Vec<(String, Vec<u8>)>,
}

const SNAPSHOT_VERSION: u32 = 1;

fn snapshot_bytes(db: &SquealDb) -> Result<Vec<u8>, String> {
    let (data, disk) = db.conn.synced_snapshot();
    let snapshot = Snapshot {
        version: SNAPSHOT_VERSION,
        name: db.name.clone(),
        data: data.synced_data(),
        // `disk` is a fresh namespace holding only the WAL segments.
        wal: disk.synced_siblings(""),
    };
    postcard::to_allocvec(&snapshot).map_err(|e| format!("failed to encode snapshot: {e}"))
}

fn restore(bytes: &[u8]) -> Result<SquealDb, String> {
    let snapshot: Snapshot =
        postcard::from_bytes(bytes).map_err(|e| format!("not a squeal snapshot: {e}"))?;
    if snapshot.version != SNAPSHOT_VERSION {
        return Err(format!(
            "snapshot format version {} is not supported (expected {SNAPSHOT_VERSION})",
            snapshot.version
        ));
    }
    // Same shape Db::synced_snapshot hands the crash harness: the data
    // file on its own, the WAL segments as siblings in one namespace.
    let data = MemFile::from_bytes(snapshot.data);
    let disk = MemFile::new();
    for (path, bytes) in snapshot.wal {
        disk.add_sibling_from_bytes(&path, bytes);
    }
    let mgr: Arc<ConnectionManager<MemFile>> = Arc::new(ConnectionManager::new());
    let conn = mgr
        .connect_using(&snapshot.name, data, disk)
        .map_err(|e| e.to_string())?;
    let _ = conn.use_schema(DEFAULT_SCHEMA);
    Ok(SquealDb {
        name: snapshot.name,
        conn,
        last_stats: RefCell::new(None),
        json: RefCell::new(None),
    })
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
#[allow(clippy::type_complexity)]
//
// The statements in `sql` are run one at a time, each one's results drained
// before the next runs: a batch run as one Statement produces a SELECT's
// rows only after every statement in it has run, so `begin; select ...;
// rollback` read the SELECT after the rollback had ended its transaction.
fn execute_results(
    conn: &Arc<Connection<MemFile>>,
    sql: &str,
) -> Result<(Vec<JsonResult>, Option<Vec<(String, QueryStats)>>), squeal_sql::error::SchemaError> {
    let mut out = vec![];
    let mut stats = None;
    let statements = split_sql_statements(sql);
    // A batch of nothing (blank, or only comments) still goes to the parser,
    // for the same answer as ever.
    let statements = if statements.is_empty() { vec![sql.to_string()] } else { statements };
    for statement in statements {
        let (results, s) = execute_one(conn, &statement)?;
        out.extend(results);
        if s.is_some() {
            stats = s;
        }
    }
    Ok((out, stats))
}

// SQL statements in `sql`, split at each `;` outside quotes ('...' with ''
// for a quote inside, "..." identifiers) and comments (-- to the end of
// the line, /* ... */). Empty pieces are dropped.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut out = vec![];
    let (mut start, mut i) = (0, 0);
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        // '' inside a string is a quote, not its end.
                        if b.get(i + 1) == Some(&q) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i += 1;
            }
            b';' => {
                out.push(sql[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(sql[start.min(sql.len())..].to_string());
    // A piece that is only blanks and comments isn't a statement.
    out.into_iter()
        .filter(|s| {
            let mut rest = s.trim();
            loop {
                if let Some(r) = rest.strip_prefix("--") {
                    rest = r.split_once('\n').map_or("", |(_, r)| r).trim();
                } else if let Some(r) = rest.strip_prefix("/*") {
                    rest = r.split_once("*/").map_or("", |(_, r)| r).trim();
                } else {
                    return !rest.is_empty();
                }
            }
        })
        .collect()
}

#[allow(clippy::type_complexity)]
fn execute_one(
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

#[allow(clippy::type_complexity)]
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
    out.push('\n');
    // squeal_sql::help::SQL_HELP is the single source of truth for the SQL
    // syntax listing — shared with squeal-cli's own !help so the two
    // front-ends can't drift apart on what SQL this engine supports.
    out += &squeal_sql::help::sql_help_text();
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

    fn json(db: &SquealDb, input: &str) -> Vec<String> {
        let out = db.run_json(input).unwrap();
        let v: Vec<serde_json::Value> =
            serde_json::from_str(&serde_json::to_string(&out).unwrap()).unwrap();
        v.iter().map(|r| r["text"].as_str().unwrap().to_string()).collect()
    }

    #[test]
    fn test_json_statements_insert_find_and_aggregate() {
        let db = SquealDb::new("j1").unwrap();
        let out = json(
            &db,
            "use shop\n\
             db.orders.insertMany([{_id: 1, item: 'pen', qty: 5}, {_id: 2, item: 'ink', qty: 1},\n\
                                   {_id: 3, item: 'pad', qty: 9}]);\n\
             db.orders.find({qty: {$gt: 2}}, {item: 1, _id: 0})\n\
               .sort({qty: -1})\n\
             db.orders.aggregate([{$group: {_id: null, total: {$sum: '$qty'}}}])",
        );
        assert_eq!(out.len(), 4, "{out:?}");
        assert_eq!(out[0], "switched to db shop");
        assert_eq!(out[2], "{\"item\":\"pad\"}\n{\"item\":\"pen\"}");
        assert!(out[3].contains("\"total\":15"), "{}", out[3]);
        assert_eq!(db.json_prompt(), "shop> ");
    }

    #[test]
    fn test_a_failing_json_statement_names_itself() {
        let db = SquealDb::new("j2").unwrap();
        let err = db.run_json("db.c.insertOne({a: 1})\ndb.c.find({a: })").unwrap_err();
        assert!(err.starts_with("db.c.find({a: })"), "{err}");
        // The statement before it took effect.
        assert_eq!(json(&db, "db.c.countDocuments({})"), ["1"]);
    }

    // Collections live in the same store as tables: one snapshot keeps
    // both.
    #[test]
    fn test_a_snapshot_keeps_collections_and_tables() {
        let db = SquealDb::new("j3").unwrap();
        db.execute("create table t (id integer not null, primary key(id)); insert into t values (7)")
            .unwrap();
        json(&db, "db.notes.insertOne({_id: 1, text: 'hello', tags: ['a', 'b']})");
        let db = restore(&snapshot_bytes(&db).unwrap()).unwrap();
        assert_eq!(
            json(&db, "db.notes.findOne({_id: 1})"),
            ["{\"_id\":1,\"text\":\"hello\",\"tags\":[\"a\",\"b\"]}"]
        );
        assert_eq!(exec(&db, "select id from t")[0]["rows"], serde_json::json!([["7"]]));
    }

    #[test]
    fn test_json_statements_split_where_they_end() {
        assert_eq!(
            split_json_statements("show dbs; use x\n\ndb.a.find({b: ';\\n', c: [1,\n2]})\n  .limit(1)\n"),
            ["show dbs", "use x", "db.a.find({b: ';\\n', c: [1,\n2]})\n  .limit(1)"]
        );
    }

    // Statements in one batch run in order, each finished before the next:
    // a SELECT inside a transaction is read before the ROLLBACK after it.
    #[test]
    fn test_a_select_inside_a_batched_transaction_reads_before_it_ends() {
        let db = SquealDb::new("b1").unwrap();
        db.execute("create table o (id integer not null, qty integer, primary key(id)); insert into o values (1, 5)")
            .unwrap();
        let out = exec(
            &db,
            "begin; update o set qty = 0 where id = 1; select qty from o; rollback; select qty from o",
        );
        assert_eq!(kinds(&out), ["Message", "Count", "Rows", "Message", "Rows"]);
        assert_eq!(out[2]["rows"], serde_json::json!([["0"]]));
        assert_eq!(out[4]["rows"], serde_json::json!([["5"]]));
    }

    #[test]
    fn test_sql_statements_split_outside_quotes_and_comments() {
        assert_eq!(
            split_sql_statements(
                "insert into t values ('a;b', 'it''s'); -- a; comment\nselect \"x;y\" from t /* ; */ ;  ;\n-- only a comment"
            ),
            [
                "insert into t values ('a;b', 'it''s')",
                " -- a; comment\nselect \"x;y\" from t /* ; */ ",
            ]
        );
    }

    #[test]
    fn test_help_is_json_with_every_part() {
        let help: serde_json::Value = serde_json::from_str(&SquealDb::help()).unwrap();
        assert!(help["commands"].as_array().unwrap().len() >= 4);
        assert_eq!(
            help["sql"].as_array().unwrap().len(),
            squeal_sql::help::SQL_HELP.len()
        );
        assert!(help["json"].as_str().unwrap().contains("db.<coll>.find("));
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
    fn test_a_snapshot_reopens_with_its_committed_data() {
        let db = SquealDb::new("snap1").unwrap();
        db.execute("create table t (id integer not null, name varchar(10), primary key(id))")
            .unwrap();
        db.execute("insert into t values (1, 'alice'), (2, 'bob')")
            .unwrap();
        let bytes = snapshot_bytes(&db).unwrap();
        drop(db);

        let db = restore(&bytes).unwrap();
        let rows = exec(&db, "select id, name from t order by id");
        assert_eq!(
            rows[0]["rows"],
            serde_json::json!([["1", "alice"], ["2", "bob"]])
        );
        // Still writable, and a snapshot of the restored database
        // round-trips too.
        db.execute("insert into t values (3, 'carol')").unwrap();
        let db = restore(&snapshot_bytes(&db).unwrap()).unwrap();
        let rows = exec(&db, "select count(*) from t");
        assert_eq!(rows[0]["rows"], serde_json::json!([["3"]]));
    }

    #[test]
    fn test_a_snapshot_leaves_out_an_open_transactions_writes() {
        let db = SquealDb::new("snap2").unwrap();
        db.execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        db.execute("insert into t values (1)").unwrap();
        db.execute("begin").unwrap();
        db.execute("insert into t values (2)").unwrap();
        let db2 = restore(&snapshot_bytes(&db).unwrap()).unwrap();
        let rows = exec(&db2, "select id from t");
        assert_eq!(rows[0]["rows"], serde_json::json!([["1"]]));
    }

    #[test]
    fn test_a_commit_moves_the_sync_generation() {
        let db = SquealDb::new("snap3").unwrap();
        db.execute("create table t (id integer not null, primary key(id))")
            .unwrap();
        let before = db.sync_generation();
        db.execute("insert into t values (1)").unwrap();
        assert!(db.sync_generation() > before);
    }

    #[test]
    fn test_garbage_is_refused_as_a_snapshot() {
        let err = restore(b"definitely not a snapshot").err().unwrap();
        assert!(err.contains("snapshot"), "{err}");
    }

    #[test]
    fn test_create_table_from_csv_infers_creates_and_loads() {
        let db = SquealDb::new("csv1").unwrap();
        let json = db
            .create_table_from_csv("t", "id,name,age\n1,alice,30\n2,bob,25\n")
            .unwrap();
        let results: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(kinds(&results), ["Message"]);
        assert_eq!(
            results[0]["text"].as_str().unwrap(),
            "Table \"t\" created, 2 row(s) loaded"
        );

        let rows = exec(&db, "select id, name, age from t order by id");
        assert_eq!(
            rows[0]["rows"],
            serde_json::json!([["1", "alice", "30"], ["2", "bob", "25"]])
        );
    }

    #[test]
    fn test_create_table_from_csv_reports_a_bad_document_as_an_error() {
        // Via self.conn directly, not db.create_table_from_csv: the
        // latter's error path goes through to_js_error, which (like
        // JsError::new elsewhere in this file) calls a real wasm-bindgen
        // imported function and panics outside an actual JS host — see
        // this module's own note on execute_results for the same reason.
        let db = SquealDb::new("csv2").unwrap();
        assert!(db.conn.create_table_from_csv("t", "").is_err());
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
        // Same SQL cheat sheet squeal-cli's own !help renders — squeal_sql
        // ::help is the single source of truth, this just checks it's
        // actually wired in here too, not silently dropped.
        for section in squeal_sql::help::SQL_HELP {
            assert!(
                text.contains(section.title),
                "missing section {:?}",
                section.title
            );
        }
    }

    #[test]
    fn test_print_stats_before_any_query_says_so_rather_than_erroring() {
        let db = SquealDb::new("t7").unwrap();
        let results = exec(&db, "!print stats");
        assert_eq!(kinds(&results), ["Message"]);
        assert!(
            results[0]["text"]
                .as_str()
                .unwrap()
                .contains("no query stats available")
        );
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
        assert!(
            text.contains("unrecognized") && text.contains("nonsense"),
            "{text}"
        );
    }
}

