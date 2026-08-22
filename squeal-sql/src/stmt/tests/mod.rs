use std::sync::Arc;

use store::memfile::MemFile;

use super::*;
use crate::conn::connection::{ConMgr, ConnectionManager};
use crate::constant::DEFAULT_SCHEMA_NAME;
use crate::rslt::resultset::ResultType;

fn conn() -> Arc<Connection<MemFile>> {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_db").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn run(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<(), SchemaError> {
    c.clone().create_statement(sql)?.execute()
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
    let ResultType::ResultString(s) = &stmt.results[0] else {
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
    let mut stmt = c.clone().create_statement("select 1").unwrap();
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
    let mut stmt = c.clone().create_statement("insert into t values (1)").unwrap();
    stmt.execute().unwrap();
    assert_eq!(stmt.results.len(), 1);
    assert!(matches!(stmt.results[0], ResultType::Count(1)));
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
    assert!(matches!(stmt.results[0], ResultType::Count(3)));
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

fn result_string(r: &ResultType<MemFile>) -> &str {
    match r {
        ResultType::ResultString(s) => s,
        _ => panic!("expected a ResultString, got a different ResultType variant"),
    }
}

#[test]
fn test_get_results_returns_the_first_result_and_is_idempotent() {
    let c = conn();
    let mut stmt = c.create_statement("create table t (id integer)").unwrap();
    stmt.execute().unwrap();

    let first = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&first), "Table 't' created");

    // Calling it again without advancing returns the same result.
    let again = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&again), "Table 't' created");
}

#[test]
fn test_get_results_returns_none_when_there_are_no_results() {
    let c = conn();
    let mut stmt = c.create_statement("select 1").unwrap();
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
    // Cursor didn't move past the end — get_results() still returns the
    // last valid result, not None.
    let still_second = stmt.get_results().unwrap().unwrap();
    assert_eq!(result_string(&still_second), "Table 't2' created");
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
