use std::sync::Arc;

use store::memfile::MemFile;

use super::*;
use crate::conn::connection::{ConMgr, ConnectionManager};
use crate::constant::DEFAULT_SCHEMA_NAME;
use crate::rslt::resultset::{ResultType, StreamingResultSet};

fn conn() -> Arc<Connection<MemFile>> {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn run(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<(), SchemaError> {
    c.clone().create_statement(sql)?.execute()
}

// Test-only accessor: get_results/get_nextresult now *take* a result
// out of Statement::results (see get_results' own doc comment — a
// StreamingResult can't be cloned, so every result is retrievable at
// most once), but plenty of tests want to just peek at
// Statement::results by index, without going through that consuming
// path. Panics with a clearer message than a raw index would if the
// slot is empty (out of range, or already taken by a real
// get_results/get_nextresult call elsewhere in the same test).
fn nth_result(stmt: &Statement<MemFile>, i: usize) -> &ResultType {
    stmt.results
        .get(i)
        .and_then(|r| r.as_ref())
        .unwrap_or_else(|| panic!("no result at index {i} (out of range, or already taken)"))
}

// Drains a StreamingResultSet into plain (columns, rows) — the
// streaming equivalent of ResultSet::columns()/rows(), for tests that
// just want to assert on fully-materialized data rather than exercise
// incremental streaming itself.
fn drain_streaming(mut stream: StreamingResultSet) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let columns = stream.columns();
    let mut rows = Vec::new();
    while let Some(key) = stream.next_result().unwrap() {
        rows.push(key.values().to_vec());
    }
    (columns, rows)
}

// SELECT always produces a ResultType::StreamingResult (see
// Statement::execute's Select arm) — since a StreamingResult can't be
// cloned/peeked (see nth_result's own doc comment), reading one
// requires *taking* the slot, not borrowing it the way nth_result does.
// Panics if the slot is empty/already taken, or holds a different
// ResultType variant.
fn take_streaming_result(
    stmt: &mut Statement<MemFile>,
    i: usize,
) -> (Vec<String>, Vec<Vec<ValueItem>>) {
    let result = stmt
        .results
        .get_mut(i)
        .and_then(Option::take)
        .unwrap_or_else(|| panic!("no result at index {i} (out of range, or already taken)"));
    match result {
        ResultType::StreamingResult(stream) => drain_streaming(stream),
        other => panic!("expected a StreamingResult at index {i}, got {other:?}"),
    }
}

#[test]
fn test_execute_creates_table_via_current_schema() {
    let c = conn();
    let mut stmt = c
        .clone()
        .create_statement("create table t (id integer not null, primary key(id))")
        .unwrap();
    stmt.execute().unwrap();

    let schema = c.current_schema().unwrap();
    assert!(schema.table_exists("t"));
    assert_eq!(schema.get_table("t").unwrap().indices.len(), 1);
}

#[test]
fn test_execute_records_a_result_string_for_create_table() {
    let c = conn();
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    let ResultType::ResultString(s) = nth_result(&stmt, 0) else {
        panic!("expected a ResultString, got a different ResultType variant");
    };
    assert_eq!(s, "Table 't' created");
}

#[test]
fn test_execute_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema").unwrap();
    // Deliberately skip use_schema/create_schema.
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_propagates_create_table_errors() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t (id integer, primary key(id))")
        .unwrap();
    // A nullable primary key is rejected by TableBuilder::build, deep
    // inside Schema::create_table — must surface through execute()
    // unchanged, not get swallowed or mistranslated.
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_ignores_non_create_table_statements() {
    let c = conn();
    let mut stmt = c.clone().create_statement("drop table t").unwrap();
    stmt.execute().unwrap();
    assert!(stmt.results.is_empty());
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_create_database_switches_the_connection() {
    let c = conn();
    run(&c, "create database db2").unwrap();
    assert_eq!(c.database_name(), "db2");
    // The old database's schema selection doesn't carry over.
    assert!(c.current_schema().is_none());
}

#[test]
fn test_execute_create_database_rejects_an_already_open_name_by_default() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    let err = run(&c, "create database db2").unwrap_err();
    assert!(matches!(err, SchemaError::DatabaseInUseError(_)));
}

#[test]
fn test_execute_create_database_if_not_exists_is_idempotent() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    // db2 already open — must not error, and must still switch to it.
    run(&c, "create database if not exists db2").unwrap();
    assert_eq!(c.database_name(), "db2");
}

#[test]
fn test_execute_use_database_switches_the_connection() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("db1").unwrap();
    mgr.create_and_connect("db2").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c, "use database db2").unwrap();
    assert_eq!(c.database_name(), "db2");
    assert!(c.current_schema().is_none());
}

#[test]
fn test_execute_use_database_reuses_the_already_open_instance() {
    // Two independent connections to the same manager; c1 switches to
    // db2 via SQL and must land on the *same* Database c2 is already
    // connected to (sharing state), not a freshly re-opened, separate
    // instance.
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("db1").unwrap();
    let c2 = mgr.create_and_connect("db2").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    run(&c2, "create table shared (id integer)").unwrap();

    run(&c1, "use database db2").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    assert!(
        c1.current_schema().unwrap().table_exists("shared"),
        "c1 must see c2's table after switching to the same open database"
    );
}

#[test]
fn test_execute_create_schema_switches_current_schema() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap(); // lives only in "default"

    run(&c, "create schema extra").unwrap();
    // create_schema also makes the new schema current — it must start
    // empty, not somehow see "default"'s table.
    assert!(!c.current_schema().unwrap().table_exists("t"));

    run(&c, "use schema default").unwrap();
    assert!(c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_create_schema_rejects_an_already_existing_name_by_default() {
    let c = conn();
    run(&c, "create schema extra").unwrap();
    let err = run(&c, "create schema extra").unwrap_err();
    assert!(matches!(err, SchemaError::SchemaInUseError(_)));
}

#[test]
fn test_execute_create_schema_if_not_exists_is_idempotent() {
    let c = conn();
    run(&c, "create schema extra").unwrap();
    run(&c, "create schema if not exists extra").unwrap();
}

#[test]
fn test_execute_use_schema_fails_for_an_unknown_name() {
    let c = conn();
    let err = run(&c, "use schema nonexistent").unwrap_err();
    assert!(matches!(err, SchemaError::SchemaNotFound(_)));
}

#[test]
fn test_execute_insert_stores_a_row_and_records_a_count_result() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_statement("insert into t values (1)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert!(matches!(nth_result(&stmt, 0), ResultType::Count(1)));
}

#[test]
fn test_execute_insert_multi_row_records_the_right_count() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement("insert into t values (1), (2), (3)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert!(matches!(nth_result(&stmt, 0), ResultType::Count(3)));
}

#[test]
fn test_execute_insert_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_insert").unwrap();
    let mut stmt = c.create_statement("insert into t values (1)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_insert_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c.create_statement("insert into nope values (1)").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_returns_a_result_set() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(50), primary key(id))",
    )
    .unwrap();
    run(&c, "insert into t values (1, 'alice')").unwrap();
    let mut stmt = c.create_statement("select * from t").unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    let (columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![vec![
            store::valueitem::ValueItem::Integer(1),
            store::valueitem::ValueItem::Str(("alice".into(), 50))
        ]]
    );
}

#[test]
fn test_execute_insert_and_select_support_schema_qualified_table_names() {
    let c = conn();
    // "default" is the schema `conn()` already selected — create a
    // second schema and a table in it, then switch back to "default" so
    // `other.t` can only resolve by explicitly qualifying it, not by
    // accidentally falling back to whatever's current.
    run(&c, "create schema other").unwrap();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c, "insert into other.t values (1)").unwrap();

    let mut stmt = c.clone().create_statement("select * from other.t").unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(rows, vec![vec![store::valueitem::ValueItem::Integer(1)]]);

    // "default" never got the table — proves the qualified INSERT above
    // actually landed in "other", not silently in whatever's current.
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_execute_rejects_a_table_reference_with_too_many_parts() {
    // Three parts (schema.table.field) is now a legal, meaningful shape
    // (see Connection::resolve_table_ref) — this needs a genuinely
    // too-long, four-part reference to still exercise the "too many
    // parts" rejection.
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c.create_statement("select * from a.b.default.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_select_rejects_a_field_qualified_table_reference() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    // Three parts resolves to schema="default", table="t", field="id" —
    // valid for resolve_table_ref, but a FROM target can't carry a
    // trailing field (see QueryVisitor::validate_table's own rejection).
    let mut stmt = c.create_statement("select * from default.t.id").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_qualified_table_reference_fails_for_an_unknown_schema() {
    let c = conn();
    let mut stmt = c.create_statement("select * from nope.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::SchemaNotFound(_)), "got {err:?}");
}

#[test]
fn test_temp_table_create_insert_select_roundtrip() {
    let c = conn();
    run(
        &c,
        "create table temp.t (id integer not null, name varchar(50))",
    )
    .unwrap();
    run(&c, "insert into temp.t values (1, 'alice')").unwrap();
    run(&c, "insert into temp.t values (2, 'bob')").unwrap();

    let mut stmt = c.clone().create_statement("select * from temp.t").unwrap();
    stmt.execute().unwrap();
    let (columns, rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Str(("alice".into(), 50))],
            vec![ValueItem::Integer(2), ValueItem::Str(("bob".into(), 50))],
        ]
    );

    // A temp table never touches the real schema system at all.
    assert!(!c.current_schema().unwrap().table_exists("t"));
}

#[test]
fn test_temp_table_is_private_to_its_own_connection() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("temp_isolation_db").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let c2 = mgr.connect("temp_isolation_db").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c1, "create table temp.t (id integer not null)").unwrap();
    run(&c1, "insert into temp.t values (1)").unwrap();

    // c2 is a different connection to the SAME database — its own
    // temp.t must not exist at all, let alone see c1's row.
    let mut stmt = c2.create_statement("select * from temp.t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_temp_table_create_rejects_constraints() {
    let c = conn();
    let err = run(
        &c,
        "create table temp.t (id integer not null, primary key(id))",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_temp_table_insert_fails_for_an_unknown_table() {
    let c = conn();
    let err = run(&c, "insert into temp.nope values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_use_schema_temp_is_rejected() {
    let c = conn();
    let err = run(&c, "use schema temp").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_schema_temp_is_rejected() {
    let c = conn();
    let err = run(&c, "create schema temp").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_select").unwrap();
    let mut stmt = c.create_statement("select * from t").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_execute_select_star_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c.create_statement("select * from nope").unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_select_star_participates_in_a_multi_statement_batch() {
    // parse_sql returns a Vec<Statement> for a single `;`-separated
    // input, and both semantic_validate and execute already loop over
    // every element — this confirms SELECT's new arm actually
    // participates in that loop rather than only working as the sole
    // statement in a batch.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let mut stmt = c
        .create_statement("select * from t; insert into t values (2); select * from t")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 3);

    let (_, first_rows) = take_streaming_result(&mut stmt, 0);
    assert_eq!(first_rows.len(), 1);

    assert!(matches!(nth_result(&stmt, 1), ResultType::Count(1)));

    let (_, third_rows) = take_streaming_result(&mut stmt, 2);
    assert_eq!(third_rows.len(), 2);
}

#[test]
fn test_new_rejects_select_with_an_explicit_column_list() {
    let c = conn();
    let err = c.create_statement("select id from t").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_select_with_a_where_clause() {
    let c = conn();
    let err = c
        .create_statement("select * from t where id = 1")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_select_with_a_join() {
    let c = conn();
    let err = c
        .create_statement("select * from t join u on t.id = u.id")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_insert_without_a_transaction_still_autocommits() {
    // No BEGIN issued — each INSERT manages (and commits) its own
    // transaction, same as before explicit transactions existed.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let err = run(&c, "insert into t values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
}

#[test]
fn test_execute_begin_commit_persists_inserts() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "insert into t values (2)").unwrap();
    run(&c, "commit").unwrap();

    let s = c.current_schema().unwrap();
    assert!(s.table_exists("t"));
    assert_eq!(select_row_count(&c, "select * from t"), 2);
}

#[test]
fn test_execute_select_star_sees_uncommitted_inserts_within_the_same_transaction() {
    // The actual ask: a connection must be able to read its own writes
    // before COMMIT, not just after — see Db::table_scan_in_txn and
    // find_visible_to's self-write exception in store.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 1);
    run(&c, "insert into t values (2)").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 2);
    run(&c, "commit").unwrap();
    assert_eq!(select_row_count(&c, "select * from t"), 2);
}

#[test]
fn test_execute_select_star_on_a_different_connection_does_not_see_uncommitted_inserts() {
    // Read-your-own-writes must not leak into cross-connection isolation:
    // a second, separate connection to the SAME database (autocommit, no
    // BEGIN of its own) still can't see the first connection's
    // uncommitted insert — only after commit does it become visible.
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c1 = mgr.create_and_connect("shared_db").unwrap();
    c1.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let c2 = mgr.connect("shared_db").unwrap();
    c2.use_schema(DEFAULT_SCHEMA_NAME).unwrap();

    run(&c1, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c1, "begin").unwrap();
    run(&c1, "insert into t values (1)").unwrap();
    assert_eq!(select_row_count(&c1, "select * from t"), 1);
    assert_eq!(select_row_count(&c2, "select * from t"), 0);

    run(&c1, "commit").unwrap();
    assert_eq!(select_row_count(&c2, "select * from t"), 1);
}

// Materializes a SELECT's row count via a fresh Statement — the direct
// way every SELECT-visibility test below checks what a connection can
// currently see, mirroring take_streaming_result's own draining.
fn select_row_count(c: &Arc<Connection<MemFile>>, sql: &str) -> usize {
    let mut stmt = c.clone().create_statement(sql).unwrap();
    stmt.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut stmt, 0);
    rows.len()
}

#[test]
fn test_execute_begin_rollback_discards_inserts() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    run(&c, "rollback").unwrap();

    // If the row had survived the rollback, this would fail with
    // DuplicateKey instead of succeeding.
    run(&c, "insert into t values (1)").unwrap();
}

#[test]
fn test_execute_rollback_discards_every_insert_in_the_transaction_not_just_the_last() {
    // No auto-abort-on-error: a failed statement inside an open
    // transaction doesn't end it, and rows from *earlier*, individually
    // successful statements in the same transaction stay uncommitted
    // until an explicit COMMIT/ROLLBACK — so ROLLBACK here must discard
    // row 1 too, not just row 2's failed attempt.
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    run(&c, "begin").unwrap();
    run(&c, "insert into t values (1)").unwrap();
    let err = run(&c, "insert into t values (1)").unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateKey(_)), "got {err:?}");
    run(&c, "rollback").unwrap();

    // Row 1 must be gone too — succeeds only if nothing from the aborted
    // transaction survived.
    run(&c, "insert into t values (1)").unwrap();
}

#[test]
fn test_execute_begin_twice_errors() {
    let c = conn();
    run(&c, "begin").unwrap();
    let err = run(&c, "begin").unwrap_err();
    assert!(matches!(err, SchemaError::TransactionAlreadyActive));
}

#[test]
fn test_execute_commit_without_begin_errors() {
    let c = conn();
    let err = run(&c, "commit").unwrap_err();
    assert!(matches!(err, SchemaError::NoActiveTransaction));
}

#[test]
fn test_execute_rollback_without_begin_errors() {
    let c = conn();
    let err = run(&c, "rollback").unwrap_err();
    assert!(matches!(err, SchemaError::NoActiveTransaction));
}

#[test]
fn test_execute_begin_again_after_commit_succeeds() {
    let c = conn();
    run(&c, "begin").unwrap();
    run(&c, "commit").unwrap();
    // The slot was cleared by commit, so a second BEGIN must not hit
    // TransactionAlreadyActive.
    run(&c, "begin").unwrap();
    run(&c, "rollback").unwrap();
}

// semantic_validate: caught at Statement::new() (create_statement) time,
// before execute() even runs — so these all fail there, not on execute().

#[test]
fn test_new_rejects_duplicate_columns_in_create_table() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("create table t (id integer, id integer)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_constraint_referencing_an_unknown_column() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("create table t (id integer, primary key(nope))")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_an_oversized_table_name() {
    let c = conn();
    let long_name = "a".repeat(129);
    let err = c
        .clone()
        .create_statement(&format!("create table {long_name} (id integer)"))
        .unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_an_oversized_database_name() {
    let c = conn();
    let long_name = "a".repeat(129);
    let err = c
        .clone()
        .create_statement(&format!("create database {long_name}"))
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_duplicate_columns_in_insert_column_list() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("insert into t (id, id) values (1, 2)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_values_row_that_does_not_match_the_column_list_width() {
    let c = conn();
    let err = c
        .clone()
        .create_statement("insert into t (id, name) values (1)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_accepts_a_values_row_matching_the_implicit_column_count() {
    // No explicit column list — width can't be checked without the
    // schema, so this must pass semantic_validate cleanly regardless of
    // whether "t" even exists (any mismatch surfaces later, in
    // rows_from_insert, once there's a real schema to check against).
    let c = conn();
    c.clone()
        .create_statement("insert into t values (1, 2, 3)")
        .unwrap();
}

#[test]
fn test_new_rejects_a_begin_end_block() {
    // sql-parser's StartTransaction::Begin grammar has no body at all
    // (just the bare BEGIN [TRANSACTION] keyword(s)) — a BEGIN...END
    // block fails to parse rather than parsing and being rejected
    // semantically, but either way the SQL is refused.
    let c = conn();
    let err = c
        .clone()
        .create_statement("begin select 1; end")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_rollback_to_savepoint() {
    // sql-parser's Rollback grammar is just the bare ROLLBACK keyword —
    // TO SAVEPOINT has no equivalent, so this fails to parse.
    let c = conn();
    let err = c
        .clone()
        .create_statement("rollback to savepoint sp1")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

fn result_string(r: &ResultType) -> &str {
    match r {
        ResultType::ResultString(s) => s,
        _ => panic!("expected a ResultString, got a different ResultType variant"),
    }
}

#[test]
fn test_get_results_returns_the_first_result_then_takes_it() {
    let c = conn();
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    stmt.execute().unwrap();

    let first = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't' created");

    // Calling it again without advancing no longer returns a second copy
    // of the same result — a StreamingResult can't be cloned to produce
    // one (see get_results' own doc comment), so every result, streaming
    // or not, is retrievable exactly once. The slot at this position is
    // already empty.
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_results_returns_none_when_there_are_no_results() {
    let c = conn();
    let mut stmt = c.create_statement("drop table t").unwrap();
    stmt.execute().unwrap();
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_nextresult_walks_through_multiple_statements() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t1 (id integer); create table t2 (id integer)")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 2);

    let first = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't1' created");

    let second = stmt.get_nextresult().unwrap().unwrap();
    assert_eq!(result_string(&second), "Table 't2' created");

    assert!(stmt.get_nextresult().unwrap().is_none());
    // Cursor position didn't move past the end (still points at index
    // 1), but the result *at* that position was already taken by the
    // get_nextresult() call above — a second read finds the slot empty,
    // same as test_get_results_returns_the_first_result_then_takes_it.
    assert!(stmt.get_results().unwrap().is_none());
}

#[test]
fn test_get_nextresult_before_get_results_starts_from_the_first_result() {
    let c = conn();
    let mut stmt = c
        .create_statement("create table t1 (id integer); create table t2 (id integer)")
        .unwrap();
    stmt.execute().unwrap();

    // get_nextresult() with no prior get_results() call must return the
    // *first* result (index 0), not skip it.
    let first = stmt.get_nextresult().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't1' created");
}

#[test]
fn test_execute_alter_table_add_column_records_a_result_string() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement("alter table t add column plan varchar(10) default 'free'")
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(result_string(nth_result(&stmt, 0)), "Table \"t\" altered");
}

#[test]
fn test_execute_alter_table_drop_column_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("alter table nope drop column x")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_execute_alter_table_rename_column_fails_without_a_selected_schema() {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db_no_schema_alter").unwrap();
    let mut stmt = c
        .create_statement("alter table t rename column a to b")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::NoSchemaSelected));
}

#[test]
fn test_new_rejects_alter_table_with_multiple_operations() {
    // sql-parser's AlterTable grammar has exactly one `operation`, not a
    // list — a second operation fails to parse rather than parsing and
    // being rejected semantically.
    let c = conn();
    let err = c
        .create_statement("alter table t add column x integer, add column y integer")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_drop_column_if_exists() {
    // sql-parser's DropColumn grammar has no IF EXISTS.
    let c = conn();
    let err = c
        .create_statement("alter table t drop column if exists x")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_dropping_multiple_columns() {
    // sql-parser's DropColumn grammar takes exactly one Ident, not a list.
    let c = conn();
    let err = c
        .create_statement("alter table t drop column x, y")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_rename_table() {
    let c = conn();
    let err = c.create_statement("alter table t rename to u").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_alter_table_add_foreign_key_records_a_result_string() {
    let c = conn();
    run(
        &c,
        "create table customers (id integer not null, primary key(id))",
    )
    .unwrap();
    run(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id))",
    )
    .unwrap();
    let mut stmt = c
        .create_statement(
            "alter table orders add foreign key (customer_id) references customers(id)",
        )
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(
        result_string(nth_result(&stmt, 0)),
        "Table \"orders\" altered"
    );
}

#[test]
fn test_execute_alter_table_drop_foreign_key_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("alter table nope drop constraint fk_cust")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_add_constraint_not_valid() {
    // sql-parser's AddConstraint grammar has no NOT VALID.
    let c = conn();
    let err = c
        .create_statement(
            "alter table t add constraint fk_x foreign key (x) references y(id) not valid",
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_alter_table_drop_constraint_if_exists() {
    // sql-parser's DropConstraint grammar has no IF EXISTS.
    let c = conn();
    let err = c
        .create_statement("alter table t drop constraint if exists fk_x")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_a_composite_foreign_key() {
    let c = conn();
    let err = c
        .create_statement(
            "create table t (a integer, b integer, foreign key(a, b) references u(x, y))",
        )
        .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_new_tolerates_a_placeholder_in_ordinary_sql() {
    // The actual ask: semantic_validate must not reject "?" outright —
    // Statement::new (which PreparedStatement::new itself calls) has to
    // succeed for SQL containing a placeholder, the same way it would
    // for any other well-formed INSERT.
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    c.create_statement("insert into t values (?)").unwrap();
}

#[test]
fn test_prepared_insert_executes_with_bound_values() {
    let c = conn();
    run(
        &c,
        "create table t (id integer not null, name varchar(10), primary key(id))",
    )
    .unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?, ?)")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 2);
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.set_field(1, ValueItem::Str(("alice".into(), 10)))
        .unwrap();
    let result = stmt.execute().unwrap();
    assert!(matches!(result, ResultType::Count(1)), "got {result:?}");

    let mut check = c.create_statement("select * from t").unwrap();
    check.execute().unwrap();
    let (_, rows) = take_streaming_result(&mut check, 0);
    assert_eq!(
        rows,
        vec![vec![
            ValueItem::Integer(1),
            ValueItem::Str(("alice".into(), 10))
        ]]
    );
}

#[test]
fn test_prepared_insert_can_be_reused_with_different_bound_values() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();

    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.execute().unwrap();
    stmt.set_field(0, ValueItem::Integer(2)).unwrap();
    stmt.execute().unwrap();

    let mut check = c.create_statement("select * from t").unwrap();
    check.execute().unwrap();
    let (_, mut rows) = take_streaming_result(&mut check, 0);
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
    );
}

#[test]
fn test_prepared_insert_type_checks_bound_values() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    stmt.set_field(0, ValueItem::Str(("nope".into(), 10)))
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_insert_enforces_not_null() {
    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    stmt.set_field(0, ValueItem::Null).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_execute_fails_when_a_parameter_is_unbound() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_set_field_rejects_an_out_of_range_index() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("insert into t values (?)")
        .unwrap();
    let err = stmt.set_field(1, ValueItem::Integer(1)).unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_statement_rejects_multiple_sql_statements() {
    let c = conn();
    let err = c
        .create_prepared_statement("insert into t values (?); insert into u values (?)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::TooManyPreparedStatement));
}

#[test]
fn test_prepared_statement_rejects_ddl() {
    let c = conn();
    let err = c
        .create_prepared_statement("create table t (id integer)")
        .unwrap_err();
    assert!(
        matches!(err, SchemaError::BadPreparedStatement(_)),
        "got {err:?}"
    );
}

#[test]
fn test_prepared_update_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("update t set id = ? where id = ?")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 2);
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    stmt.set_field(1, ValueItem::Integer(2)).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_delete_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("delete from t where id = ?")
        .unwrap();
    stmt.set_field(0, ValueItem::Integer(1)).unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_prepared_select_execute_errors_not_implemented() {
    let c = conn();
    run(&c, "create table t (id integer)").unwrap();
    let mut stmt = c
        .clone()
        .create_prepared_statement("select * from t")
        .unwrap();
    assert_eq!(stmt.parameter_count(), 0);
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_execute_copy_into_records_load_counts() {
    let path = std::env::temp_dir().join(format!(
        "squeal_sql_stmt_copy_test_{}.csv",
        std::process::id()
    ));
    std::fs::write(&path, "id\n1\n2\n").unwrap();

    let c = conn();
    run(&c, "create table t (id integer not null, primary key(id))").unwrap();
    let mut stmt = c
        .create_statement(&format!("copy into t from @{}", path.to_str().unwrap()))
        .unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert_eq!(
        result_string(nth_result(&stmt, 0)),
        "2 row(s) loaded, 0 row(s) failed"
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_execute_copy_into_fails_for_an_unknown_table() {
    let c = conn();
    let mut stmt = c
        .create_statement("copy into nope from @/tmp/x.csv")
        .unwrap();
    let err = stmt.execute().unwrap_err();
    assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_copy_into_with_a_file_format_clause() {
    // sql-parser's CopyInto grammar is exactly "COPY INTO <table> FROM
    // @<path>" — a trailing FILE_FORMAT clause is leftover, unparsed
    // input after a complete statement, so this fails to parse rather
    // than parsing and being rejected semantically.
    let c = conn();
    let err = c
        .create_statement("copy into t from @stage file_format = (type = csv)")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}

#[test]
fn test_new_rejects_copy_into_with_a_pattern_clause() {
    let c = conn();
    let err = c
        .create_statement("copy into t from @stage pattern = '.*.csv'")
        .unwrap_err();
    assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
}
