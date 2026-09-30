use crate::{optim::table_stats::ComputedTableStat, source::planinfo::PlanNode};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use store::clock::Instant;

use postcard::from_bytes;
use store::{
    cursor::{Cursor, KeyRange, RangeCursor, TableCursor},
    db::{DBFile, Db},
    txn::Transaction,
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    source::{ProjectableField, QueryStats, Source},
    table::{SqlTable, VersionedRow},
};

// Reads a table: every row (TableScan), or with a key range the rows
// within a range of the table's own key — its PRIMARY KEY, which the
// table's tree is keyed by (TableSeek). Either way rows come out whole, in
// the table's own layout.
pub struct TableSource<F: DBFile> {
    cursor: RowCursor<F>,
    // The seek's condition for EXPLAIN (see plan::sarg::describe_key_range)
    // and its estimated row count; None for a full scan.
    seek: Option<(String, Option<usize>)>,
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

enum RowCursor<F: DBFile + 'static> {
    Scan(TableCursor<F>),
    Seek(RangeCursor<F>),
}

impl<F> RowCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn next(&mut self) -> Result<Option<store::tuple::Tuple>, store::error::StoreError> {
        match self {
            RowCursor::Scan(c) => c.next(),
            RowCursor::Seek(c) => c.next(),
        }
    }

    fn reset(&mut self) -> Result<(), store::error::StoreError> {
        match self {
            RowCursor::Scan(c) => c.reset(),
            RowCursor::Seek(c) => c.reset(),
        }
    }
}

impl<F> TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    /// The rows of `table` within `range` of its PRIMARY KEY (see
    /// optim::picker::AccessPath::TableSeek), expected to be `rows` many.
    pub(crate) fn seek(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
        range: KeyRange,
        rows: Option<usize>,
    ) -> Result<Self, SchemaError> {
        let key_names: Vec<String> = table
            .primary_key()
            .map(|pk| pk.fields.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();
        let seek = crate::plan::sarg::describe_key_range(&range, &key_names);
        let cursor = db.key_range_scan(table.db_table_id, txn, range)?;
        let mut source = Self::new(db, table, txn, stats)?;
        source.cursor = RowCursor::Seek(cursor);
        source.seek = Some((seek, rows));
        Ok(source)
    }

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
            cursor: RowCursor::Scan(cursor),
            seek: None,
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
        match &self.seek {
            Some((seek, rows)) => PlanNode::new("TableSeek")
                .detail(format!("{} ({seek})", self.table.name))
                .rows(*rows),
            None => PlanNode::new("TableScan")
                .detail(self.table.name.clone())
                .rows(self.stats.as_ref().map(|s| s.table_stat.row_count)),
        }
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
            format!(
                "{}:{}",
                if self.seek.is_some() {
                    "TableSeek"
                } else {
                    "TableScan"
                },
                self.table.name
            ),
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
        f.debug_struct(if self.seek.is_some() {
            "TableSeek"
        } else {
            "TableScan"
        })
        .field("table", &self.table.name)
        .finish()
    }
}
