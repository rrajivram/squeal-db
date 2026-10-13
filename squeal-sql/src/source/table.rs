use crate::{optim::table_stats::ComputedTableStat, source::planinfo::PlanNode};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use store::clock::Instant;

use store::{
    cursor::{Cursor, KeyRange, RangeCursor, TableCursor},
    db::{DBFile, Db},
    txn::Transaction,
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    source::{ProjectableField, QueryStats, Source},
    table::{ColumnTest, SqlTable},
};

// Reads a table: every row (TableScan), or with a key range the rows
// within a range of the table's own key — its PRIMARY KEY, which the
// table's tree is keyed by (TableSeek). Either way rows come out in the
// table's own layout: whole, or with only the columns the query reads
// filled (see reading).
pub struct TableSource<F: DBFile> {
    timer: crate::source::timing::RowTimer,
    // None once released (see Source::release), until reopened.
    cursor: Option<RowCursor<F>>,
    db: Arc<Db<F>>,
    // The seek's ranges, described for EXPLAIN only when asked (see
    // plan::sarg::describe_key_range), and its estimated row count; None
    // for a full scan.
    seek: Option<(Vec<KeyRange>, Option<usize>)>,
    table: Arc<SqlTable>,
    // Which of the table's partitions this reads.
    part: usize,
    fields: Arc<[ProjectableField]>,
    // Per table column, whether the query reads it (see reading); None:
    // every column.
    wanted: Option<Vec<bool>>,
    next_time: u128,
    stats: Option<ComputedTableStat>,
    // The most recently yielded tuple's own key — see Source::last_id's
    // own doc comment for why this exists at all (UPDATE/DELETE's own
    // use). None before the first next() call or after next() returns
    // None, and always None without `keep_ids`.
    last_id: Option<store::tuple::DBIdType>,
    // Whether to keep last_id: a copy of every row's key, which only
    // UPDATE/DELETE read (see without_row_ids).
    keep_ids: bool,
    // Comparisons from WHERE a row is checked against before it is built
    // (see filtering): one that fails is passed over here.
    tests: Vec<ColumnTest>,
    // The current row, lent (see Source::row): refilled by each advance.
    row: Vec<store::valueitem::ValueItem>,
    // The current batch, lent (see Source::batch): refilled by each
    // advance_batch. Made on the first.
    batch: Option<crate::batch::Batch>,
}

enum RowCursor<F: DBFile + 'static> {
    Scan(TableCursor<F>),
    Seek(RangeCursor<F>),
}

impl<F> RowCursor<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn next_ref(
        &mut self,
    ) -> Result<Option<store::tuple::TupleRef<'_>>, store::error::StoreError> {
        match self {
            RowCursor::Scan(c) => c.next_ref(),
            RowCursor::Seek(c) => c.next_ref(),
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
    /// The rows of `table`'s partition `part` within `ranges` of its PRIMARY
    /// KEY (see optim::picker::AccessPath::TableSeek), expected to be `rows`
    /// many.
    pub(crate) fn seek(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        part: usize,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
        ranges: Vec<KeyRange>,
        rows: Option<usize>,
    ) -> Result<Self, SchemaError> {
        let seek = ranges.clone();
        let cursor =
            db.key_ranges_scan(table.partitions[part].rows(), txn.map(|t| t.id()), ranges)?;
        // Built around the seek's own cursor: opening a scan only to replace
        // it cost every primary key lookup a copy of the table's first data
        // page (TableCursor reads a page whole when it starts).
        let mut source = Self::over(db, RowCursor::Seek(cursor), table, part, stats);
        source.seek = Some((seek, rows));
        Ok(source)
    }

    fn key_names(&self) -> String {
        self.table
            .primary_key()
            .map(|pk| {
                pk.fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    }

    // `txn` only needs to live for this call — table_scan_in_txn only
    // borrows it to read off its (cheap, owned) TransactionId, which is
    // all TableCursor itself ever keeps (see its own doc comment). Not
    // storing the borrow here is what lets a TableSource returned from
    // Connection::with_current_txn's closure outlive the closure itself;
    // reset() below doesn't need it back either, since TableCursor
    // already remembers its own transaction internally.
    //
    // Reads every row of the table's partition `part`.
    pub(crate) fn new(
        db: Arc<Db<F>>,
        table: Arc<SqlTable>,
        part: usize,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let rows = table.partitions[part].rows();
        let cursor = match txn {
            Some(txn) => db.table_scan_in_txn(rows, txn)?,
            None => db.table_scan(rows)?,
        };
        Ok(Self::over(db, RowCursor::Scan(cursor), table, part, stats))
    }

    /// Fills only `columns` (positions in the table's row) — the ones the
    /// query reads; every other column comes out NULL.
    pub(crate) fn reading(mut self, columns: &std::collections::BTreeSet<usize>) -> Self {
        self.wanted = Some(
            (0..self.fields.len())
                .map(|c| columns.contains(&c))
                .collect(),
        );
        self
    }

    /// Passes over the rows that fail what of `filters` (WHERE's terms on
    /// this table alone, by position in its row) can be checked on a row
    /// before it is built — see ColumnTest. WHERE still runs above this.
    pub(crate) fn filtering(mut self, filters: &[crate::plan::eval::EvalExpr]) -> Self {
        self.tests = filters
            .iter()
            .filter_map(|f| ColumnTest::of(self.table.fields(), f))
            .collect();
        self
    }

    /// Doesn't keep each row's key for last_id: for a reader that never
    /// asks (a query, not an UPDATE or DELETE), which then copies no key
    /// out of the row it is lent.
    pub(crate) fn without_row_ids(mut self) -> Self {
        self.keep_ids = false;
        self
    }

    // What EXPLAIN calls what this reads: the table, and which partition of
    // it when it has several.
    fn label(&self) -> String {
        partition_label(&self.table, self.part)
    }

    // Row estimates are for the whole table: shown on the step reading all
    // of its partitions (AppendSource), not on each partition's.
    fn shown_rows(&self, rows: Option<usize>) -> Option<usize> {
        rows.filter(|_| !self.table.is_partitioned())
    }

    fn over(
        db: Arc<Db<F>>,
        cursor: RowCursor<F>,
        table: Arc<SqlTable>,
        part: usize,
        stats: Option<ComputedTableStat>,
    ) -> Self {
        let fields = table
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>();
        let fields = Arc::from(fields);
        Self {
            timer: Default::default(),
            cursor: Some(cursor),
            db,
            seek: None,
            table,
            part,
            fields,
            wanted: None,
            keep_ids: true,
            tests: vec![],
            row: Vec::new(),
            batch: None,
            next_time: 0,
            stats,
            last_id: None,
        }
    }
}

fn released() -> SchemaError {
    SchemaError::InternalSchemaError("a kept source was read before it was reopened".into())
}

// The table's name, and its partition's when the table is partitioned.
pub(crate) fn partition_label(table: &SqlTable, part: usize) -> String {
    if table.is_partitioned() {
        format!(
            "{} partition {}",
            table.name,
            table.partitions[part].describe()
        )
    } else {
        table.name.clone()
    }
}

impl<F> Source for TableSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        let key_names: Vec<String> = self
            .table
            .primary_key()
            .map(|pk| pk.fields.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();
        let seek = self
            .seek
            .as_ref()
            .map(|(ranges, rows)| (crate::plan::sarg::describe_key_ranges(ranges, &key_names), rows));
        match &seek {
            // All of it, read through its key for the order.
            Some((seek, rows)) if seek.is_empty() => PlanNode::new("TableScan")
                .detail(format!("{} (in {} order)", self.label(), self.key_names()))
                .rows(self.shown_rows(**rows)),
            Some((seek, rows)) => PlanNode::new("TableSeek")
                .detail(format!("{} ({seek})", self.label()))
                .rows(self.shown_rows(**rows)),
            None => PlanNode::new("TableScan")
                .detail(self.label())
                .rows(self.shown_rows(self.stats.as_ref().map(|s| s.table_stat.row_count))),
        }
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        Ok(match self.advance()? {
            true => Some(IndexKey::new_from_buffer(&mut self.row)?),
            false => None,
        })
    }

    fn lends(&self) -> bool {
        true
    }

    fn advance(&mut self) -> Result<bool, SchemaError> {
        let start = self.timer.start();
        // Loops past the rows `tests` rule out (see filtering).
        let cursor = self.cursor.as_mut().ok_or_else(released)?;
        while let Some(tuple) = cursor.next_ref()? {
            if !self.table.decode_row_into(
                tuple.data(),
                self.wanted.as_deref(),
                &self.tests,
                &mut self.row,
            )? {
                continue;
            }
            if self.keep_ids {
                self.last_id = Some(tuple.id().to_owned());
            }
            crate::source::timing::add(&mut self.next_time, start);
            return Ok(true);
        }
        self.last_id = None;
        crate::source::timing::add(&mut self.next_time, start);
        Ok(false)
    }

    fn row(&self) -> &[store::valueitem::ValueItem] {
        &self.row
    }

    // Not for a reader of row keys (UPDATE, DELETE): a batch keeps none.
    fn batches(&self) -> bool {
        !self.keep_ids
    }

    fn advance_batch(&mut self) -> Result<bool, SchemaError> {
        let start = self.timer.start();
        let batch = self.batch.get_or_insert_with(|| {
            crate::batch::Batch::new(
                self.table.fields().iter().map(|f| f.datatype),
                self.wanted.as_deref(),
            )
        });
        batch.clear();
        while !batch.is_full() {
            let Some(tuple) = self.cursor.as_mut().ok_or_else(released)?.next_ref()? else {
                break;
            };
            self.table
                .decode_row_to_batch(tuple.data(), &self.tests, batch)?;
        }
        batch.finish()?;
        let any = batch.len() > 0;
        crate::source::timing::add(&mut self.next_time, start);
        Ok(any)
    }

    fn batch(&self) -> Option<&crate::batch::Batch> {
        self.batch.as_ref()
    }

    fn last_id(&self) -> Option<store::tuple::DBIdType> {
        self.last_id.clone()
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.last_id = None;
        Ok(self.cursor.as_mut().ok_or_else(released)?.reset()?)
    }

    // A seek, kept: the same table read over new ranges. (A full scan has
    // no key to change and is never kept.)
    fn reopen(&mut self, reopen: &crate::source::Reopen<'_>) -> Result<bool, SchemaError> {
        let Some((ranges, rows)) = &mut self.seek else {
            return Ok(false);
        };
        ranges.clear();
        ranges.extend_from_slice(reopen.ranges);
        *rows = Some(reopen.ranges.len());
        let cursor = self.db.key_ranges_scan(
            self.table.partitions[self.part].rows(),
            reopen.txn.map(|t| t.id()),
            reopen.ranges.to_vec(),
        )?;
        self.cursor = Some(RowCursor::Seek(cursor));
        // The key's conditions held the last execution's values; the seek
        // reads exactly what they allow, so they need no test.
        self.tests.retain(|t| !reopen.key.contains(&t.column()));
        self.last_id = None;
        Ok(true)
    }

    fn release(&mut self) {
        self.cursor = None;
        self.last_id = None;
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
