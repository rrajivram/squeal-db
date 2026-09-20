use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Instant};

use store::{cursor::Cursor, db::DBFile, run::RunCursor, valueitem::IndexKey};

use crate::{
    error::SchemaError,
    source::{ProjectableField, QueryStats, Source},
};

// Streams a temp table's rows — the Run-backed equivalent of TableSource
// (see crate::temp::TempTable). Like TableSource, always a leaf: nothing
// upstream of a bare table/temp-table scan to chain from.
pub(crate) struct RunSource<F: DBFile + 'static> {
    cursor: RunCursor<F>,
    fields: Arc<[ProjectableField]>,
    next_time: u128,
}

impl<F> RunSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(cursor: RunCursor<F>, fields: &[ProjectableField]) -> Self {
        Self {
            cursor,
            fields: Arc::from(fields),
            next_time: 0,
        }
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
        let start = Instant::now();
        let out = self
            .cursor
            .next()?
            .map(|tuple| Ok(IndexKey::from_bytes(tuple.data())?))
            .transpose();
        self.next_time += start.elapsed().as_nanos();
        out
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(self.cursor.reset()?)
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        Some(vec![(
            "RunScan".to_string(),
            QueryStats {
                stats: HashMap::from([("next_ns".into(), self.next_time as f64)]),
                level: 0,
            },
        )])
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
