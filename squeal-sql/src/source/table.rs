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
    source::{ProjectedField, Source},
    table::{SqlTable, VersionedRow},
};

pub struct TableSource<F: DBFile> {
    cursor: TableCursor<F>,
    table: Arc<SqlTable>,
    fields: Arc<[ProjectedField]>,
}

impl<F> TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // `txn` only needs to live for this call — table_scan_in_txn only
    // borrows it to read off its (cheap, owned) TransactionId, which is
    // all TableCursor itself ever keeps (see its own doc comment). Not
    // storing the borrow here is what lets a TableSource returned from
    // Connection::with_current_txn's closure outlive the closure itself;
    // reset() below doesn't need it back either, since TableCursor
    // already remembers its own transaction internally.
    pub(crate) fn new(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        txn: Option<&Transaction>,
    ) -> Result<Self, SchemaError> {
        let cursor = match txn {
            Some(txn) => db.table_scan_in_txn(table.db_table_id, txn)?,
            None => db.table_scan(table.db_table_id)?,
        };
        let fields = table
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectedField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>();
        let fields = Arc::from(fields);
        Ok(Self {
            cursor,
            table,
            fields,
        })
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

    fn fields(&self) -> Arc<[ProjectedField]> {
        self.fields.clone()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(self.cursor.reset()?)
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
