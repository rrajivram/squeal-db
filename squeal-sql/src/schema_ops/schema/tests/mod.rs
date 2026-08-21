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
mod contract;
mod mapping;

use std::sync::Arc;

use store::memfile::MemFile;

use super::*;

fn schema() -> Arc<Schema<MemFile>> {
    Schema::create_database("test_schema".to_string()).unwrap()
}

// Runs `sql` against a fresh in-memory schema and returns the table
// named `table_name` afterward — the create_table/from_sql helper
// methods used before this was wired up returned the built SqlTable
// directly; now that execute() persists (writes to the system table,
// commits, and only then updates the in-memory map) and returns just
// `()`, tests have to go back through the schema to see what actually
// landed.
fn create_and_fetch(sql: &str, table_name: &str) -> SqlTable {
    let s = schema();
    s.execute(sql.to_string()).unwrap();
    s.get_table(table_name)
        .unwrap_or_else(|| panic!("table {table_name:?} missing after successful execute()"))
}

fn field<'a>(t: &'a SqlTable, name: &str) -> &'a crate::table::Field {
    t.fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field named {name:?} in {t:#?}"))
}

fn temp_schema_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("squeal_sql_test_{tag}_{}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}
