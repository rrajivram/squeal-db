use std::{fmt::Debug, sync::Arc};

use postcard::from_bytes;
use store::{
    cursor::{Cursor, TableCursor},
    db::{DBFile, Db},
    txn::Transaction,
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    source::Source,
    table::{Field, SqlTable, VersionedRow},
};

pub struct TableSource<F: DBFile> {
    cursor: TableCursor<F>,
    table: Arc<SqlTable>,
}

impl<F> TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        txn: Option<&Transaction>,
    ) -> Result<Self, SchemaError> {
        let cursor = match txn {
            Some(txn) => db.table_scan_in_txn(table.db_table_id, txn)?,
            None => db.table_scan(table.db_table_id)?,
        };
        Ok(Self { cursor, table })
    }
}

impl<F> Source for TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if let Some(tuple) = self.cursor.next()? {
            let row = from_bytes::<VersionedRow>(tuple.data())?;
            Ok(Some(self.table.reproject(&row)?))
        } else {
            Ok(None)
        }
    }

    fn chain(&mut self, _depends: Option<Box<dyn Source>>) {
        // A table scan is always a leaf (nothing to pull from) — never
        // expects a real dependency to chain.
    }

    fn fields(&self) -> Arc<[Arc<Field>]> {
        self.table.fields_arc()
    }
}

impl<F> Debug for TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableScan")
            .field("table", &self.table.name)
            .finish()
    }
}
