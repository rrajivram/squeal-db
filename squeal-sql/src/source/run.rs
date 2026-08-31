use std::fmt::Debug;

use store::{cursor::Cursor, db::DBFile, run::RunCursor, valueitem::IndexKey};

use crate::{
    error::SchemaError,
    source::{ProjectedField, Source},
};

// Streams a temp table's rows — the Run-backed equivalent of TableSource
// (see crate::temp::TempTable). Like TableSource, always a leaf: nothing
// upstream of a bare table/temp-table scan to chain from.
pub(crate) struct RunSource<F: DBFile + 'static> {
    cursor: RunCursor<F>,
    fields: Vec<ProjectedField>,
}

impl<F> RunSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(cursor: RunCursor<F>, fields: Vec<ProjectedField>) -> Self {
        Self { cursor, fields }
    }
}

impl<F> Source for RunSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        // A Run's own Tuple bytes ARE an IndexKey's to_bytes() encoding
        // (see TempTable::insert_rows) — no VersionedRow/reproject step
        // like TableSource's real-table case, since a temp table has no
        // ALTER TABLE, so there's only ever one schema version to decode
        // against.
        Ok(self
            .cursor
            .next()?
            .map(|tuple| IndexKey::from_bytes(tuple.data())))
    }

    fn chain(&mut self, _depends: Option<Box<dyn Source>>) {
        // A run scan is always a leaf (nothing to pull from) — same as
        // TableSource, never expects a real dependency to chain.
    }

    fn fields(&self) -> Vec<ProjectedField> {
        self.fields.clone()
    }
}

impl<F> Debug for RunSource<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunScan").finish_non_exhaustive()
    }
}
