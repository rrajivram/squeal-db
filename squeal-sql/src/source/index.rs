use std::{collections::HashMap, fmt::Debug, sync::Arc};
use store::clock::Instant;
use crate::source::{planinfo::PlanNode};

use postcard::from_bytes;
use store::{
    cursor::{Cursor, RangeCursor},
    db::{DBFile, Db},
    txn::Transaction,
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    source::{ComputedTableStat, ProjectableField, QueryStats, Source},
    table::{SqlIndex, SqlTable},
};

#[allow(unused)]
pub struct IndexSource<F: DBFile + 'static> {
    name: String,
    cursor: RangeCursor<F>,
    fields: Arc<[ProjectableField]>,
    stats: Option<ComputedTableStat>,
    next_time: u128,
}

#[allow(unused)]
impl<F: DBFile + 'static> IndexSource<F> {
    pub fn new(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        index: &SqlIndex,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let cursor = match txn {
            Some(tx) => db.range_scan_bounds_in_txn(
                index.db_table_id,
                tx,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?,
            None => db.range_scan_bounds(
                index.db_table_id,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?,
        };
        let name = if let Some(name) = index.name.as_ref() {
            name.clone()
        } else {
            "noname".into()
        };
        let name = format!("IndexScan {}({})", table.name.clone(), name);
        let fields = Arc::from(
            index
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
                .collect::<Vec<_>>(),
        );
        Ok(Self {
            cursor,
            fields,
            stats,
            next_time: 0,
            name,
        })
    }
}

impl<F: DBFile + 'static> Source for IndexSource<F> {
    fn plan(&self) -> PlanNode {
        PlanNode::new(self.name.clone()).rows(self.stats.as_ref().map(|s| s.table_stat.row_count))
    }


    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        let start = Instant::now();
        if let Some(res) = self.cursor.next()? {
            let key = from_bytes::<IndexKey>(res.data())?;
            self.next_time += start.elapsed().as_nanos();
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        Some(vec![(
            self.name.clone(),
            QueryStats {
                stats: HashMap::from([("next_ns".into(), self.next_time as f64)]),
                level: 0,
            },
        )])
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(self.cursor.reset()?)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.stats.clone()
    }
}

impl<F: DBFile + 'static> Debug for IndexSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexScan")
            .field("index", &self.name)
            .finish()
    }
}
