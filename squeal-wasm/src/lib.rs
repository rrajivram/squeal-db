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

use std::sync::Arc;

use serde::Serialize;
use squeal_sql::{
    conn::connection::{Connection, ConnectionManager},
    rslt::resultset::{ResultSet, ResultType, StreamingResultSet},
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
        Ok(SquealDb { conn })
    }

    /// Runs `sql` (which may itself be several `;`-separated statements)
    /// and returns every result as one JSON array, one entry per statement
    /// that produced a result — see `execute_results`' own doc comment for
    /// the shape of each entry. Throws (a JS exception, via `JsError`) if
    /// the batch fails to parse/validate at all, or if any statement in it
    /// fails; whatever ran before the failing statement has already taken
    /// effect (matches Statement::execute's own all-or-nothing-only-at-
    /// parse-time contract — see its own doc comment in stmt.rs).
    pub fn execute(&self, sql: &str) -> Result<String, JsError> {
        let results = execute_results(&self.conn, sql).map_err(to_js_error)?;
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

fn execute_results(
    conn: &Arc<Connection<MemFile>>,
    sql: &str,
) -> Result<Vec<JsonResult>, squeal_sql::error::SchemaError> {
    let mut stmt = conn.clone().create_statement(sql)?;
    stmt.execute()?;
    let mut out = vec![];
    let mut next = stmt.get_results();
    loop {
        match next? {
            Some(r) => {
                out.push(to_json_result(r)?);
                next = stmt.get_nextresult();
            }
            None => break,
        }
    }
    Ok(out)
}

fn to_json_result(r: ResultType) -> Result<JsonResult, squeal_sql::error::SchemaError> {
    Ok(match r {
        ResultType::Count(n) => JsonResult::Count { rows_affected: n },
        ResultType::ResultString(text) => JsonResult::Message { text },
        ResultType::Result(rs) => {
            let (columns, rows, message) = drain_materialized(rs);
            JsonResult::Result {
                columns,
                rows,
                message,
            }
        }
        ResultType::StreamingResult(mut stream) => {
            let (columns, rows) = drain_streaming(&mut stream)?;
            let message = stream.get_final_message();
            JsonResult::Rows {
                columns,
                rows,
                message,
            }
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
}
