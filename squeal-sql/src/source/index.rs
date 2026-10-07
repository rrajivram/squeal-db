use crate::{conn::connection::Connection, source::planinfo::PlanNode};
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use postcard::from_bytes;
use store::{
    cursor::KeyRange,
    cursor::{Cursor, RangeCursor},
    db::{DBFile, Db},
    tuple::{DBIdType, Tuple},
    txn::Transaction,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    optim::table_stats::ComputedTableStat,
    source::{ProjectableField, QueryStats, Source},
    table::{SqlIndex, SqlTable},
};

// Reads one of a table's indexes — all of it (IndexScan) or a key range
// (IndexSeek) — producing rows in the TABLE's own layout: every column in
// table order, NULL wherever the index holds no value. Same layout as
// TableSource, so the planner can swap one for the other without
// renumbering a single column position (see optim::picker — it only picks
// such an index when it holds every column the query reads). With a row
// lookup (IndexLookup) each entry's whole row is fetched from the table
// instead, at the scan's own snapshot, so any index will do.
//
// An index entry's key is the indexed columns' values (plus the row's
// identity, for a non-unique index); its data is the row's identity — for
// a PRIMARY KEY table, the key columns' values, which fill those columns
// too (see Schema's index maintenance).
pub struct IndexSource<F: DBFile + 'static> {
    timer: crate::source::timing::RowTimer,
    table: Arc<SqlTable>,
    // Which of the table's partitions this reads the index of.
    part: usize,
    index: usize,
    cursor: RangeCursor<F>,
    fields: Arc<[ProjectableField]>,
    // Table column position of each indexed column, in key order.
    key_positions: Vec<usize>,
    // Table column positions of the PRIMARY KEY columns, in key order —
    // None when the table has no PRIMARY KEY (its identity is a row id,
    // not a column).
    pk_positions: Option<Vec<usize>>,
    // The seek's condition for EXPLAIN and its estimated row count; None
    // for a full scan.
    seek: Option<(Vec<KeyRange>, Option<usize>)>,
    // Some: fetch each entry's row from the table (IndexLookup).
    lookup: Option<Arc<Db<F>>>,
    // Per table column, whether the query reads it (see reading): the
    // others are left NULL, and an entry's row identity is not decoded
    // unless a column it fills is read.
    wanted: Option<Vec<bool>>,
    last_id: Option<DBIdType>,
    stats: Option<ComputedTableStat>,
    next_time: u128,
}

impl<F> IndexSource<F>
where
    F: DBFile<Item = F> + 'static,
{
    /// The entries of `index` in the table's partition `part` within
    /// `ranges` (see optim::picker's IndexSeek/IndexLookup), expected to be
    /// `rows` many; `lookup` fetches each entry's row from the partition.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seek(
        conn: &Arc<Connection<F>>,
        table: &Arc<SqlTable>,
        part: usize,
        index: usize,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
        ranges: Vec<KeyRange>,
        lookup: bool,
        rows: Option<usize>,
    ) -> Result<Self, SchemaError> {
        let db = conn.database.read().db.clone();
        let seek = ranges.clone();
        let cursor = db.key_ranges_scan(
            table.partitions[part].index(index),
            txn.map(|t| t.id()),
            ranges,
        )?;
        let mut source = Self::with_cursor(table, part, index, cursor, stats)?;
        source.seek = Some((seek, rows));
        source.lookup = lookup.then_some(db);
        Ok(source)
    }

    pub fn new(
        db: &Arc<Connection<F>>,
        table: &Arc<SqlTable>,
        part: usize,
        index: usize,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let db = &db.database.read().db;
        let tree = table.partitions[part].index(index);
        let cursor = match txn {
            Some(tx) => db.range_scan_bounds_in_txn(
                tree,
                tx,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?,
            None => {
                db.range_scan_bounds(tree, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?
            }
        };
        Self::with_cursor(table, part, index, cursor, stats)
    }

    fn with_cursor(
        table: &Arc<SqlTable>,
        part: usize,
        index: usize,
        cursor: RangeCursor<F>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let table_fields = table.fields();
        let positions = |index: &SqlIndex| -> Result<Vec<usize>, SchemaError> {
            index
                .fields
                .iter()
                .map(|f| {
                    table_fields
                        .iter()
                        .position(|t| t.name.eq_ignore_ascii_case(&f.name))
                        .ok_or_else(|| {
                            SchemaError::InternalSchemaError(format!(
                                "index column {} is not a column of {}",
                                f.name, table.name
                            ))
                        })
                })
                .collect()
        };
        let key_positions = positions(&table.indices[index])?;
        let pk_positions = match table.indices.iter().find(|i| i.is_primary) {
            Some(pk) => Some(positions(pk)?),
            None => None,
        };
        let fields = table_fields
            .iter()
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>();
        Ok(Self {
            timer: Default::default(),
            table: table.clone(),
            part,
            index,
            cursor,
            fields: Arc::from(fields),
            key_positions,
            pk_positions,
            seek: None,
            lookup: None,
            wanted: None,
            last_id: None,
            stats,
            next_time: 0,
        })
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

    // The table row an entry points to, at the scan's snapshot. None if it
    // isn't visible there, which the entry being visible should rule out.
    fn fetch_row(
        &mut self,
        db: &Arc<Db<F>>,
        entry: &Tuple,
    ) -> Result<Option<IndexKey>, SchemaError> {
        let identity = from_bytes::<IndexKey>(entry.data())?;
        let id = if self.pk_positions.is_some() {
            DBIdType::Rec(identity)
        } else {
            match identity.values() {
                [ValueItem::Integer(n)] => DBIdType::Int(*n as u64),
                other => {
                    return Err(SchemaError::InternalSchemaError(format!(
                        "index {} points to a row id that isn't one integer: {other:?}",
                        self.index_name()
                    )));
                }
            }
        };
        let rows = self.table.partitions[self.part].rows();
        let Some(tuple) = db.find_as(rows, id.clone(), self.cursor.reader())? else {
            return Ok(None);
        };
        let row = self.table.decode_row(tuple.data(), self.wanted.as_deref())?;
        self.last_id = Some(id);
        Ok(Some(row))
    }

    fn index_name(&self) -> String {
        let index = &self.table.indices[self.index];
        match &index.name {
            Some(name) => name.clone(),
            None if index.is_primary => "primary key".into(),
            None => format!("index #{}", self.index),
        }
    }
}

impl<F> Source for IndexSource<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn plan(&self) -> PlanNode {
        let using = format!(
            "{} using {}",
            crate::source::table::partition_label(&self.table, self.part),
            self.index_name()
        );
        // Row estimates are for the whole table: shown on the step reading
        // all of its partitions (AppendSource), not on each partition's.
        let shown = |rows: Option<usize>| rows.filter(|_| !self.table.is_partitioned());
        let key_names: Vec<String> = self.table.indices[self.index]
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect();
        let seek = self.seek.as_ref().map(|(ranges, rows)| {
            (
                crate::plan::sarg::describe_key_ranges(ranges, &key_names),
                *rows,
            )
        });
        match &seek {
            Some((seek, rows)) => PlanNode::new(if self.lookup.is_some() {
                "IndexLookup"
            } else {
                "IndexSeek"
            })
            .detail(if seek.is_empty() {
                // All of it, read for its order.
                format!("{using} (in index order)")
            } else {
                format!("{using} ({seek})")
            })
            .rows(shown(*rows)),
            None => PlanNode::new("IndexScan")
                .detail(using)
                .rows(shown(self.stats.as_ref().map(|s| s.table_stat.row_count))),
        }
    }

    fn last_id(&self) -> Option<DBIdType> {
        self.last_id.clone()
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = self.timer.start();
        if let Some(db) = self.lookup.clone() {
            loop {
                let Some(entry) = self.cursor.next()? else {
                    self.last_id = None;
                    crate::source::timing::add(&mut self.next_time, start);
                    return Ok(None);
                };
                if let Some(row) = self.fetch_row(&db, &entry)? {
                    crate::source::timing::add(&mut self.next_time, start);
                    return Ok(Some(row));
                }
            }
        }
        let Some(entry) = self.cursor.next()? else {
            crate::source::timing::add(&mut self.next_time, start);
            return Ok(None);
        };
        let DBIdType::Rec(key) = entry.id() else {
            return Err(SchemaError::InternalSchemaError(format!(
                "index {} has an entry without a column key",
                self.index_name()
            )));
        };
        let wanted = |pos: usize| self.wanted.as_ref().is_none_or(|w| w[pos]);
        let mut row = vec![ValueItem::Null; self.fields.len()];
        // zip stops at the indexed columns: a non-unique index's key goes
        // on to carry the row identity, which the data holds as well.
        for (value, &pos) in key.values().iter().zip(&self.key_positions) {
            if wanted(pos) {
                row[pos] = value.clone();
            }
        }
        if let Some(pk) = &self.pk_positions
            && pk
                .iter()
                .any(|p| wanted(*p) && !self.key_positions.contains(p))
        {
            let identity = from_bytes::<IndexKey>(entry.data())?;
            for (value, &pos) in identity.values().iter().zip(pk) {
                if wanted(pos) {
                    row[pos] = value.clone();
                }
            }
        }
        crate::source::timing::add(&mut self.next_time, start);
        Ok(Some(IndexKey::new_from_owned(row)?))
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        Some(vec![(
            format!("IndexScan {}", self.table.name),
            QueryStats {
                stats: HashMap::from([("next_ns".into(), self.next_time as f64)]),
                level: 0,
            },
        )])
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.last_id = None;
        Ok(self.cursor.reset()?)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.stats.clone()
    }
}

impl<F> Debug for IndexSource<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexScan")
            .field("table", &self.table.name)
            .field("index", &self.index_name())
            .finish()
    }
}
