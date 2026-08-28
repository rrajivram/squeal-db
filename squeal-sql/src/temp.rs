use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use sql_parser::ddl::CreateTable;
use store::{
    db::{DBFile, Db},
    run::{Run, RunCursor},
    valueitem::{IndexKey, ValueItem},
};

use crate::{error::SchemaError, table::Field};

// Reserved name for connection-scoped temporary tables, addressed as
// `temp.<table>` — recognized specially at table-reference resolution
// time (see temp_table_name, and its callers in stmt.rs/plan/logical.rs)
// rather than being a real Schema/Database object. A real Schema lives
// in Database.schemas, shared by every connection to that database, and
// is backed by a durable, database-wide system catalog — neither of
// which fits "private to one connection, gone when it closes." Making
// `temp` behave like a schema only at the SQL-addressing layer, while
// backing it with a plain per-Connection map instead, gets the same
// `temp.<table>` ergonomics without needing Schema itself to support two
// different backing models. See Connection::use_schema/create_schema for
// where a real schema literally named `temp` is rejected, so this name
// can never collide with one.
pub(crate) const TEMP_SCHEMA_NAME: &str = "temp";

// If `parts` is exactly `[TEMP_SCHEMA_NAME, table]` (case-insensitive),
// returns the lowercased table name — the one shape `temp.<table>`
// addressing recognizes. `temp` alone, or qualified any other way,
// isn't a temp-table reference at all (and, for the bare `temp` case,
// use_schema/create_schema's own reserved-name guard is what actually
// rejects it — this function only ever answers "is this specifically a
// two-part temp.<table> reference").
pub(crate) fn temp_table_name(parts: &[&str]) -> Option<String> {
    match parts {
        [schema, table] if schema.eq_ignore_ascii_case(TEMP_SCHEMA_NAME) => {
            Some(table.to_lowercase())
        }
        _ => None,
    }
}

// Column list for a `CREATE TABLE temp.<table> (...)` — deliberately
// just Field::try_from per column, the same conversion SqlTable::from_sql
// uses, with none of its constraint/index/foreign-key handling: a temp
// table has no indices at all (see TempTable's own doc comment), so
// PRIMARY KEY/UNIQUE/FOREIGN KEY/CHECK have nothing to attach to.
// stmt.rs's validate_create_table rejects a temp CREATE TABLE with any
// constraint clause before this ever runs, so there's nothing left here
// to reject — every column becomes a plain field, ids assigned by
// declaration order same as TableBuilder::build's fresh-table case.
pub(crate) fn fields_from_create_table(c: &CreateTable) -> Result<Vec<Arc<Field>>, SchemaError> {
    c.columns()
        .enumerate()
        .map(|(i, col)| Ok(Arc::new(Field::try_from(col)?.with_id(i as u32))))
        .collect()
}

// A connection-scoped temporary table: a column list (for INSERT
// type-checking and a Source's own fields()) plus a store::run::Run for
// storage — append-only, unindexed, no undo/redo, freed only when this
// TempTable is dropped (see Run's own doc comment). No versions, no
// indices, no foreign keys, no db_table_id: unlike SqlTable, a temp
// table was never meant to support ALTER TABLE, constraints, or being
// looked up by anything other than a straight sequential scan.
pub(crate) struct TempTable<F: DBFile + 'static> {
    fields: Arc<[Arc<Field>]>,
    run: Run<F>,
}

impl<F> TempTable<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn fields(&self) -> Arc<[Arc<Field>]> {
        self.fields.clone()
    }

    pub(crate) fn insert_rows(&mut self, rows: Vec<Vec<ValueItem>>) -> Result<usize, SchemaError> {
        let mut count = 0;
        for row in rows {
            let ik = IndexKey::new_from(&row)?;
            self.run.append(&ik.to_bytes())?;
            count += 1;
        }
        Ok(count)
    }

    pub(crate) fn cursor(&self) -> Result<RunCursor<F>, SchemaError> {
        Ok(self.run.cursor()?)
    }
}

// Per-connection registry of temp tables, keyed by (already-lowercased)
// name — see TEMP_SCHEMA_NAME's own comment for why this exists
// alongside Schema rather than as one. Arc<RwLock<TempTable<F>>> per
// entry, not Arc<TempTable<F>> with copy-on-write replacement (the
// pattern Schema.tables uses): a Run's pages are real, identity-bearing
// allocations (see Run::append's own &mut self) — appending has to
// mutate the *same* Run in place, not build a fresh copy to swap in.
pub(crate) struct TempTables<F: DBFile + 'static> {
    tables: RwLock<HashMap<String, Arc<RwLock<TempTable<F>>>>>,
}

impl<F> TempTables<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<RwLock<TempTable<F>>>> {
        self.tables.read().get(name).cloned()
    }

    pub(crate) fn create(
        &self,
        db: &Arc<Db<F>>,
        name: String,
        fields: Vec<Arc<Field>>,
    ) -> Result<(), SchemaError> {
        let mut tables = self.tables.write();
        if tables.contains_key(&name) {
            return Err(SchemaError::UserError(format!(
                "temp table {name:?} already exists"
            )));
        }
        let run = db.create_run()?;
        tables.insert(
            name,
            Arc::new(RwLock::new(TempTable {
                fields: fields.into(),
                run,
            })),
        );
        Ok(())
    }

    // Drops every temp table this connection owns — called when the
    // owning Connection repoints at a different database (see
    // Connection::use_database/create_database). A TempTable's Run holds
    // pages allocated from a specific database's own PageBuffer (see
    // Db::create_run), so it can't mean anything once the connection is
    // pointed elsewhere; there's no reasonable way to "carry it over."
    pub(crate) fn clear(&self) {
        self.tables.write().clear();
    }
}
