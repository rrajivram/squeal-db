//! The napi-rs-facing shim: like `squeal-wasm` (its browser counterpart),
//! but for Node.js, backed by a real, persistent `std::fs::File` instead
//! of an ephemeral `MemFile` — see this crate's own README for the use
//! case (an embedded, persistent database for a Node.js host app) and why
//! napi-rs rather than wasm-bindgen.
//!
//! Compiles two ways from one Rust API (`#[napi]` handles both):
//! - A normal native addon (`.node`) on whatever platform builds it —
//!   best performance, the default napi-rs story.
//! - `wasm32-wasip1-threads`, loaded by napi-rs's own generated Node.js
//!   WASI loader — no prebuilt binary needed per OS/arch, at some
//!   performance cost. This is the one this crate is actually meant to be
//!   exercised on: see the README for why (npm packaging, not sandboxing —
//!   napi-rs's own docs are explicit that a WASI addon should be treated
//!   as trusted code, not a security boundary).
//!
//! Real persistence needed one addition outside this crate:
//! `store::memfile::WasiFile`, a `target_os = "wasi"`-only `Opener`
//! implementation — not just un-gating `std::fs::File`'s existing native
//! impl, because two things it leans on (`try_lock`/`try_clone`) compile
//! fine for WASI but are confirmed broken at runtime there. See
//! `WasiFile`'s own doc comment for the full story, and this crate's own
//! README for how it was found (by actually running a build under Node,
//! not by reading documentation — both were documented as supported).

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde::Serialize;
use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::{ResultSet, ResultType, StreamingResultSet},
    source::QueryStats,
};

// napi::bindgen_prelude's glob import above shadows std's Result with its
// own alias (error type must be napi::Error, which requires AsRef<str> —
// SchemaError doesn't implement that). Every #[napi]-exported method
// below wants that napi alias (bare `Result<T>`); every internal helper
// that still deals in SchemaError directly needs the real std one —
// named distinctly rather than fully-qualifying std::result::Result
// everywhere below.
type SqlResult<T> = std::result::Result<T, squeal_sql::error::SchemaError>;

const DEFAULT_SCHEMA: &str = "default";

// A real, persistent file — the entire point of this crate, unlike
// squeal-wasm's ephemeral MemFile. Native builds use std::fs::File
// directly; wasm32-wasip1(-threads) uses store::memfile::WasiFile
// instead — a thin wrapper std::fs::File itself can't be, because
// try_clone()/try_lock() are both confirmed broken at runtime under
// WASI even though they compile (see WasiFile's own doc comment for the
// full story). Both satisfy DBFile via store::db's blanket impl once
// Opener is implemented, so nothing downstream of this alias needs to
// know or care which one F actually is.
#[cfg(not(target_os = "wasi"))]
type F = std::fs::File;
#[cfg(target_os = "wasi")]
type F = store::memfile::WasiFile;

/// One persistent, file-backed database. `path` is a real filesystem
/// path — relative paths resolve against the host process's cwd, same as
/// any other file API. Call `close()` when done with it (flushes schema
/// metadata and truncates the WAL — see `Connection::close`'s own doc
/// comment); dropping the handle without closing leaves that work for
/// next open to redo via WAL replay, which is correct but slower.
#[napi]
pub struct SquealDb {
    // A fresh Arc<ConnectionManager<F>> per SquealDb, not a process-wide
    // singleton (unlike squeal-cli's ConnectionManager::<File>::get_manager,
    // which doesn't exist on wasm32 anyway — see connection.rs's own
    // cfg(not(target_arch = "wasm32")) gate on it). Safe regardless:
    // Database::open/create take a real OS file lock (do_lock, called
    // from store::db) independent of which ConnectionManager asked for
    // it, so two SquealDb handles racing to open the same path fail
    // loudly (a locked-file error) rather than corrupting anything — see
    // this crate's own tests.
    //
    // A Mutex<Option<..>>, not a plain Arc<Connection<F>>: close() needs
    // to consume the very last strong reference to hand ownership to
    // Connection::close (see its own doc comment on why it takes
    // `Arc<Self>` and requires being the sole reference) — `.take()` is
    // how a `&self` napi method gets to move something out.
    conn: Mutex<Option<Arc<Connection<F>>>>,
    // Stats from the most recently executed StreamingResult — mirrors
    // squeal-cli's own `last_stats` local and squeal-wasm's own field of
    // the same name/purpose; see either's doc comment.
    last_stats: Mutex<Option<Vec<(String, QueryStats)>>>,
}

#[napi]
impl SquealDb {
    /// Opens `path` if it already exists, otherwise creates it fresh.
    /// Lands on a usable schema immediately (same bootstrap as
    /// squeal-cli/squeal-wasm) so CREATE TABLE etc. work right away.
    #[napi(constructor)]
    pub fn new(path: String) -> Result<SquealDb> {
        let mgr: Arc<ConnectionManager<F>> = Arc::new(ConnectionManager::new());
        let conn = mgr
            .connect(&path)
            .or_else(|_| mgr.create_and_connect(&path))
            .map_err(to_napi_error)?;
        let _ = conn.use_schema(DEFAULT_SCHEMA);
        Ok(SquealDb {
            conn: Mutex::new(Some(conn)),
            last_stats: Mutex::new(None),
        })
    }

    /// Runs `sql` and returns every result as one JSON array — see
    /// `execute_results`' own doc comment for the shape of each entry.
    /// Same `!`-command handling as squeal-cli/squeal-wasm (`!help`,
    /// `!print stats`, `!reset stats`, `!show table stats`) — see
    /// `run_custom_command`.
    #[napi]
    pub fn execute(&self, sql: String) -> Result<String> {
        let conn = self.current_conn()?;
        let trimmed = sql.trim();
        if let Some(command) = trimmed.strip_prefix('!') {
            let result = run_custom_command(command.trim(), &conn, &self.last_stats);
            return serde_json::to_string(&[result])
                .map_err(|e| Error::from_reason(format!("failed to serialize results: {e}")));
        }

        let (results, stats) = execute_results(&conn, &sql).map_err(to_napi_error)?;
        if stats.is_some() {
            *self.last_stats.lock() = stats;
        }
        serde_json::to_string(&results)
            .map_err(|e| Error::from_reason(format!("failed to serialize results: {e}")))
    }

    /// Non-SQL counterpart to `CREATE TABLE <name> AS COPY FROM @<path>`:
    /// infers columns/types from `csv_text` (a whole CSV document, header
    /// row included), creates `table_name`, loads every row. Node *can*
    /// use the `@path` SQL form directly (real filesystem, unlike the
    /// browser), but this spares a host that already has the CSV in
    /// memory (an HTTP upload, a fetched body, ...) from writing it to a
    /// temp file first — and keeps parity with squeal-wasm's own method
    /// of the same name. Same one-element JSON-array result shape as
    /// `execute()`.
    #[napi]
    pub fn create_table_from_csv(&self, table_name: String, csv_text: String) -> Result<String> {
        let conn = self.current_conn()?;
        let (loaded, failed) = conn
            .create_table_from_csv(&table_name, &csv_text)
            .map_err(to_napi_error)?;
        // Same wording as the SQL-dispatched form's own result message
        // (squeal-sql's stmt.rs) — duplicated rather than shared, same
        // precedent as COMMANDS below.
        let text = format!(
            "Table {table_name:?} created, {loaded} row(s) loaded{}",
            if failed > 0 {
                format!(", {failed} row(s) failed")
            } else {
                String::new()
            }
        );
        serde_json::to_string(&[JsonResult::Message { text }])
            .map_err(|e| Error::from_reason(format!("failed to serialize results: {e}")))
    }

    /// Flushes every loaded schema's metadata and truncates the WAL, then
    /// marks this handle unusable — a later `execute()` call returns an
    /// error instead of silently reopening. Safe to call more than once
    /// (a second call is a no-op, not an error).
    #[napi]
    pub fn close(&self) -> Result<()> {
        if let Some(conn) = self.conn.lock().take() {
            conn.close().map_err(to_napi_error)?;
        }
        Ok(())
    }

    fn current_conn(&self) -> Result<Arc<Connection<F>>> {
        self.conn
            .lock()
            .clone()
            .ok_or_else(|| Error::from_reason("this SquealDb has already been closed"))
    }
}

// One JSON-serializable entry per ResultType a statement produced — see
// squeal-wasm's own JsonResult (same shape, duplicated rather than
// shared: it's a serialization DTO tied to each binding's own JS
// marshaling story, not SQL-grammar knowledge, so the
// squeal_sql::help-style "one source of truth" argument doesn't apply
// here the way it did for the !help SQL cheat sheet).
#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
enum JsonResult {
    Count { rows_affected: usize },
    Message { text: String },
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        message: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        message: String,
    },
}

fn execute_results(
    conn: &Arc<Connection<F>>,
    sql: &str,
) -> SqlResult<(Vec<JsonResult>, Option<Vec<(String, QueryStats)>>)> {
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
) -> SqlResult<(JsonResult, Option<Vec<(String, QueryStats)>>)> {
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

fn drain_materialized(rs: ResultSet) -> (Vec<String>, Vec<Vec<String>>, String) {
    let columns = rs.columns().to_vec();
    let rows = rs.rows_as_strings();
    let message = rs.get_final_message();
    (columns, rows, message)
}

fn drain_streaming(
    stream: &mut StreamingResultSet,
) -> SqlResult<(Vec<String>, Vec<Vec<String>>)> {
    let columns = stream.columns();
    let mut rows = vec![];
    while let Some(row) = stream.next_result_as_strings()? {
        rows.push(row);
    }
    Ok((columns, rows))
}

fn to_napi_error(e: squeal_sql::error::SchemaError) -> Error {
    Error::from_reason(e.to_string())
}

// The full set of `!`-commands this shim understands — mirrors
// squeal-cli's own COMMANDS/squeal-wasm's own COMMANDS (kept separate,
// not shared, per the same reasoning squeal-wasm's own copy documents).
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

fn run_custom_command(
    command: &str,
    conn: &Arc<Connection<F>>,
    last_stats: &Mutex<Option<Vec<(String, QueryStats)>>>,
) -> JsonResult {
    const USAGE_HINT: &str = "try '!help' for a list of commands";
    match command {
        "help" => JsonResult::Message { text: help_text() },
        "print stats" => JsonResult::Message {
            text: print_query_stats_text(&last_stats.lock()),
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
    // squeal_sql::help::SQL_HELP is the single source of truth for the
    // SQL syntax listing — shared with squeal-cli's and squeal-wasm's own
    // !help so all three front-ends can't drift apart on what SQL this
    // engine supports.
    out += &squeal_sql::help::sql_help_text();
    out
}

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

// Runs on native (not WASI — this crate's own cargo test doesn't cross-
// compile) but exercises the same store/squeal-sql code every WASI call
// does; only the real WASI-specific findings (try_lock/try_clone/pread/
// pwrite's runtime behavior — see WasiFile's own doc comment) needed an
// actual Node run to catch, documented in this crate's README rather
// than re-asserted here.
//
// Deliberately does NOT go through SquealDb's own #[napi] methods (new/
// execute/close): napi::Error's Drop impl unconditionally references
// real N-API host functions (napi_call_threadsafe_function and friends)
// — fine for a cdylib, which Node resolves those against when it loads
// it, but a `cargo test` harness is a fully, statically linked
// executable with nowhere to defer that to, so it fails at link time
// the moment anything in the binary touches napi::Error at all (not
// just when an error path actually runs) — the same root cause as
// squeal-wasm's JsError not working outside a JS host, just total
// rather than one call. So these tests call execute_results/
// run_custom_command directly (SchemaError-typed, no napi involved)
// against a Connection<F> built the same way SquealDb::new builds one —
// verifying the exact same logic execute()/new() wrap, just without
// going through the napi boundary itself.
#[cfg(test)]
mod tests {
    use super::*;

    // std::env::temp_dir().join(format!("{tag}_{}", std::process::id()))
    // matches store's own test convention (see store/src/db.rs's
    // temp_db_path) — process id keeps concurrent test runs from
    // colliding on the same path.
    fn temp_db_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("ws_napi_test_{tag}_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    // Removes the main file plus every WAL segment/temp-pool sibling
    // (`<path>.wal.<n>`, `<path>.tmp`) so one test's leftovers can't be
    // mistaken for another's data on a later run reusing the same tag.
    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
        if let Some(dir) = std::path::Path::new(path).parent() {
            if let Ok(entries) = std::fs::read_dir(dir) {
                let stem = std::path::Path::new(path)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name.starts_with(&stem) && name != stem {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    // Mirrors SquealDb::new's own connect-or-create + land-on-a-schema
    // logic exactly, minus the napi::Error wrapping (see this module's
    // own doc comment for why that can't be exercised via cargo test).
    fn open_or_create(path: &str) -> SqlResult<Arc<Connection<F>>> {
        let mgr: Arc<ConnectionManager<F>> = Arc::new(ConnectionManager::new());
        let conn = mgr.connect(path).or_else(|_| mgr.create_and_connect(path))?;
        let _ = conn.use_schema(DEFAULT_SCHEMA);
        Ok(conn)
    }

    fn exec(conn: &Arc<Connection<F>>, sql: &str) -> Vec<serde_json::Value> {
        let (results, _) = execute_results(conn, sql).unwrap();
        let json = serde_json::to_string(&results).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    fn kinds(results: &[serde_json::Value]) -> Vec<&str> {
        results
            .iter()
            .map(|r| r["kind"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn test_new_creates_the_file_and_lands_on_a_usable_schema_immediately() {
        let path = temp_db_path("t1");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        let results = exec(&conn, "create table t (id integer not null, primary key(id))");
        assert_eq!(kinds(&results), ["Message"]);
        conn.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn test_insert_reports_count_and_select_reports_rows() {
        let path = temp_db_path("t2");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        execute_results(
            &conn,
            "create table t (id integer not null, name varchar(20), primary key(id))",
        )
        .unwrap();
        let results = exec(&conn, "insert into t values (1, 'alice')");
        assert_eq!(kinds(&results), ["Count"]);
        assert_eq!(results[0]["rows_affected"], 1);

        let results = exec(&conn, "select id, name from t");
        assert_eq!(kinds(&results), ["Rows"]);
        assert_eq!(results[0]["rows"], serde_json::json!([["1", "alice"]]));
        conn.close().unwrap();
        cleanup(&path);
    }

    // The actual point of this crate, verifiable natively: data survives
    // closing the connection and opening a brand-new one at the same
    // path — not just staying alive across execute() calls on the same
    // handle (MemFile could do that trivially; this can't be faked by an
    // in-memory backend). This is exactly what the README's three-
    // separate-node-process test also confirmed for real, under WASI.
    #[test]
    fn test_data_persists_across_close_and_reopen() {
        let path = temp_db_path("t3");
        cleanup(&path);
        {
            let conn = open_or_create(&path).unwrap();
            execute_results(
                &conn,
                "create table t (id integer not null, name varchar(20), primary key(id))",
            )
            .unwrap();
            execute_results(&conn, "insert into t values (1, 'alice')").unwrap();
            execute_results(&conn, "insert into t values (2, 'bob')").unwrap();
            conn.close().unwrap();
        }
        {
            let conn = open_or_create(&path).unwrap();
            let results = exec(&conn, "select id, name from t order by id");
            assert_eq!(
                results[0]["rows"],
                serde_json::json!([["1", "alice"], ["2", "bob"]])
            );
            execute_results(&conn, "insert into t values (3, 'carol')").unwrap();
            conn.close().unwrap();
        }
        {
            let conn = open_or_create(&path).unwrap();
            let results = exec(&conn, "select count(*) as n from t");
            assert_eq!(results[0]["rows"], serde_json::json!([["3"]]));
            conn.close().unwrap();
        }
        cleanup(&path);
    }

    #[test]
    fn test_a_bad_statement_is_reported_as_an_error_with_a_useful_message() {
        let path = temp_db_path("t5");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        let err = execute_results(&conn, "select * from nope").unwrap_err();
        assert!(err.to_string().to_lowercase().contains("nope"), "{err}");
        conn.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn test_two_paths_are_independent_databases() {
        let path_a = temp_db_path("t6a");
        let path_b = temp_db_path("t6b");
        cleanup(&path_a);
        cleanup(&path_b);
        let a = open_or_create(&path_a).unwrap();
        let b = open_or_create(&path_b).unwrap();
        execute_results(&a, "create table t (id integer not null, primary key(id))").unwrap();
        assert!(execute_results(&b, "select * from t").is_err());
        a.close().unwrap();
        b.close().unwrap();
        cleanup(&path_a);
        cleanup(&path_b);
    }

    #[test]
    fn test_opening_the_same_path_twice_concurrently_is_rejected_not_silently_shared() {
        let path = temp_db_path("t7");
        cleanup(&path);
        let a = open_or_create(&path).unwrap();
        execute_results(&a, "create table t (id integer not null, primary key(id))").unwrap();
        // A second, independent connect-or-create at the same path, while
        // `a` still has it open, must fail — not silently succeed and
        // desync from `a`. Exercises the same do_lock() File::try_lock()
        // real native locking every SquealDb::new call relies on.
        let second = open_or_create(&path);
        assert!(second.is_err(), "opening an already-open path should fail");
        // `a` is unaffected by the failed second attempt — this used to
        // fail (store::db::Db::open_using wrote a fresh WAL segment as
        // part of "opening," before checking the lock, so a doomed
        // second attempt still left a stray segment file that collided
        // with `a`'s own later segment rolls); fixed upstream in store,
        // now covered directly by store's own
        // test_a_failed_concurrent_open_leaves_no_stray_wal_segment_behind.
        a.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn test_create_table_from_csv_persists_across_close_and_reopen() {
        let path = temp_db_path("csv1");
        cleanup(&path);
        {
            let conn = open_or_create(&path).unwrap();
            let (loaded, failed) = conn
                .create_table_from_csv("t", "id,name\n1,alice\n2,bob\n")
                .unwrap();
            assert_eq!((loaded, failed), (2, 0));
            conn.close().unwrap();
        }
        {
            let conn = open_or_create(&path).unwrap();
            let results = exec(&conn, "select id, name from t order by id");
            assert_eq!(
                results[0]["rows"],
                serde_json::json!([["1", "alice"], ["2", "bob"]])
            );
            conn.close().unwrap();
        }
        cleanup(&path);
    }

    #[test]
    fn test_help_lists_every_command_and_the_shared_sql_cheat_sheet() {
        let path = temp_db_path("t8");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        let result = run_custom_command("help", &conn, &Mutex::new(None));
        let json = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let text = value["text"].as_str().unwrap();
        for (cmd, _) in COMMANDS {
            assert!(text.contains(cmd), "missing {cmd:?} in:\n{text}");
        }
        for section in squeal_sql::help::SQL_HELP {
            assert!(text.contains(section.title), "missing section {:?}", section.title);
        }
        conn.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn test_print_stats_reports_the_most_recent_select() {
        let path = temp_db_path("t9");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        execute_results(&conn, "create table t (id integer not null, primary key(id))").unwrap();
        execute_results(&conn, "insert into t values (1)").unwrap();
        let (_, stats) = execute_results(&conn, "select id from t").unwrap();
        let result = run_custom_command("print stats", &conn, &Mutex::new(stats));
        let json = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value["text"].as_str().unwrap().contains("TableScan"));
        conn.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn test_show_table_stats_returns_a_table_not_a_message() {
        let path = temp_db_path("t10");
        cleanup(&path);
        let conn = open_or_create(&path).unwrap();
        execute_results(&conn, "create table t (id integer not null, primary key(id))").unwrap();
        let result = run_custom_command("show table stats", &conn, &Mutex::new(None));
        let json = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["kind"], "Result");
        conn.close().unwrap();
        cleanup(&path);
    }
}
