// Two deliberately separate concerns, split into their own files:
//
// - `mapping`: does valid SQL text turn into the Schema/SqlTable/Field/
//   SqlIndex shape we promise? These never assert on sqlparser's own
//   grammar or error messages — that's sqlparser's job to test, not
//   ours.
// - `contract`: does the Schema API (execute/create/open/close/
//   table_exists/get_table) uphold its behavioral guarantees — rejecting
//   invalid input with the right error, persisting across close/reopen,
//   not leaking partial state on failure?
// - `dml`: Schema::insert_rows' own storage mechanics — row keying
//   (PRIMARY KEY vs auto-id), secondary index population/enforcement,
//   and multi-row/multi-index atomicity on failure. Statement::execute's
//   INSERT *dispatch* (parsing, result reporting) is tested in stmt.rs
//   instead, same split as CREATE TABLE.
// - `alter`: Schema::add_column/drop_column/rename_column and
//   SqlTable::reproject — schema versioning, default backfill for rows
//   written before a column existed, and the "column used by an index"
//   guard. ALTER TABLE *dispatch* (parsing, result reporting) is tested
//   in stmt.rs, same split as CREATE TABLE/INSERT.
// - `foreign_key`: SqlForeignKey validation at CREATE TABLE and ALTER
//   TABLE ADD/DROP time, and enforcement at INSERT time. Statement
//   dispatch for the ALTER TABLE ADD/DROP FOREIGN KEY forms is tested
//   in stmt.rs, same split as everything else ALTER-shaped.
// - `copy_into`: Schema::copy_csv_into's own loading mechanics —
//   positional mapping, header-row skipping, NULL-on-empty-field, and
//   permissive (continue-on-error, not atomic) row handling. COPY INTO
//   *dispatch* (parsing, result reporting) is tested in stmt.rs, same
//   split as everything else.
mod alter;
mod contract;
mod copy_into;
mod dml;
mod foreign_key;
mod mapping;

use std::sync::Arc;

use store::memfile::MemFile;

use super::*;
use crate::conn::connection::{ConMgr, Connection, ConnectionManager};
use crate::constant::DEFAULT_SCHEMA_NAME;
use crate::schema_ops::database::Database;

// A fresh in-memory database + connection, pointed at its "default"
// schema — statement execution goes through Statement (see stmt.rs) now,
// which needs a Connection with a current schema, not a bare Schema.
fn conn() -> Arc<Connection<MemFile>> {
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::new());
    let c = mgr.create_and_connect("test_schema").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    c
}

fn schema() -> Arc<Schema<MemFile>> {
    conn().current_schema().unwrap()
}

// Parses and runs `sql` as a single statement against `c`'s current
// schema — the Statement::execute path exercised by real callers,
// instead of poking Schema::create_table directly.
fn execute(c: &Arc<Connection<MemFile>>, sql: &str) -> Result<(), SchemaError> {
    c.clone().create_statement(sql)?.execute()
}

// Runs `sql` against a fresh in-memory schema and returns the table
// named `table_name` afterward — the create_table/from_sql helper
// methods used before this was wired up returned the built SqlTable
// directly; now that execute() persists (writes to the system table,
// commits, and only then updates the in-memory map) and returns just
// `()`, tests have to go back through the schema to see what actually
// landed.
fn create_and_fetch(sql: &str, table_name: &str) -> Arc<SqlTable> {
    let c = conn();
    execute(&c, sql).unwrap();
    let s = c.current_schema().unwrap();
    s.get_table(table_name)
        .unwrap_or_else(|| panic!("table {table_name:?} missing after successful execute()"))
}

fn field<'a>(t: &'a SqlTable, name: &str) -> &'a crate::table::Field {
    t.fields()
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field named {name:?} in {t:#?}"))
}

// Only the close/reopen persistence tests need this (contract's own
// and alter's): they construct their own file-backed Schema directly
// (not through the shared conn()/execute() helpers, which are
// MemFile-only) and are testing Schema/Database's own persistence
// guarantees specifically — independent of whichever higher-level
// dispatcher (Statement) calls into create_table.
fn try_create_table_directly(
    s: &Arc<Schema<store::named_memfile::NamedMemFile>>,
    sql: &str,
) -> Result<(), SchemaError> {
    let stmt = sql_parser::parse_one(sql).unwrap();
    let sql_parser::Statement::CreateTable(c) = &stmt else {
        panic!("expected a CREATE TABLE statement, got: {stmt:?}");
    };
    s.create_table(SqlTable::from_sql(s, c.clone())?)
}

fn create_table_directly(s: &Arc<Schema<store::named_memfile::NamedMemFile>>, sql: &str) {
    try_create_table_directly(s, sql).unwrap();
}

fn temp_schema_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("squeal_sql_test_{tag}_{}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}
