use std::{collections::HashMap, fmt::Debug, sync::Arc};
use store::clock::Instant;
use crate::source::{planinfo::PlanNode};

use postcard::from_bytes;
use store::{
    cursor::{Cursor, TableCursor},
    db::{DBFile, Db},
    txn::Transaction,
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    source::{ComputedTableStat, ProjectableField, QueryStats, Source},
    table::{SqlTable, VersionedRow},
};

pub struct TableSource<F: DBFile> {
    cursor: TableCursor<F>,
    table: Arc<SqlTable>,
    fields: Arc<[ProjectableField]>,
    next_time: u128,
    stats: Option<ComputedTableStat>,
    // The most recently yielded tuple's own key — see Source::last_id's
    // own doc comment for why this exists at all (UPDATE/DELETE's own
    // use). None before the first next() call or after next() returns
    // None.
    last_id: Option<store::tuple::DBIdType>,
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
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let cursor = match txn {
            Some(txn) => db.table_scan_in_txn(table.db_table_id, txn)?,
            None => db.table_scan(table.db_table_id)?,
        };
        let fields = table
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>();
        let fields = Arc::from(fields);
        Ok(Self {
            cursor,
            table,
            fields,
            next_time: 0,
            stats,
            last_id: None,
        })
    }
}

impl<F> Source for TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        PlanNode::new("TableScan")
            .detail(self.table.name.clone())
            .rows(self.stats.as_ref().map(|s| s.table_stat.row_count))
    }


    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        if let Some(tuple) = self.cursor.next()? {
            let row = from_bytes::<VersionedRow>(tuple.data())?;
            let out = self.table.reproject(&row)?;
            self.last_id = Some(tuple.id().clone());
            self.next_time += start.elapsed().as_nanos();
            Ok(Some(out))
        } else {
            self.last_id = None;
            self.next_time += start.elapsed().as_nanos();
            Ok(None)
        }
    }

    fn last_id(&self) -> Option<store::tuple::DBIdType> {
        self.last_id.clone()
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.last_id = None;
        Ok(self.cursor.reset()?)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.stats.clone()
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        Some(vec![(
            format!("TableScan:{}", self.table.name),
            QueryStats {
                stats: HashMap::from([("next_ns".into(), self.next_time as f64)]),
                level: 0,
            },
        )])
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
