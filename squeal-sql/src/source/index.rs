use crate::{conn::connection::Connection, source::planinfo::PlanNode};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use store::clock::Instant;

use postcard::from_bytes;
use store::{
    cursor::{Cursor, RangeCursor},
    db::DBFile,
    tuple::DBIdType,
    txn::Transaction,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    optim::table_stats::ComputedTableStat,
    source::{ProjectableField, QueryStats, Source},
    table::{SqlIndex, SqlTable},
};

// A full scan of one of a table's indexes, producing rows in the TABLE's
// own layout: every column in table order, NULL wherever the index holds
// no value. Same layout as TableSource, so the planner can swap one for the
// other without renumbering a single column position (see optim::picker —
// it only picks an index that holds every column the query reads).
//
// An index entry's key is the indexed columns' values (plus the row's
// identity, for a non-unique index); its data is the row's identity — for
// a PRIMARY KEY table, the key columns' values, which fill those columns
// too (see Schema's index maintenance).
pub struct IndexSource<F: DBFile + 'static> {
    table: Arc<SqlTable>,
    index: usize,
    cursor: RangeCursor<F>,
    fields: Arc<[ProjectableField]>,
    // Table column position of each indexed column, in key order.
    key_positions: Vec<usize>,
    // Table column positions of the PRIMARY KEY columns, in key order —
    // None when the table has no PRIMARY KEY (its identity is a row id,
    // not a column).
    pk_positions: Option<Vec<usize>>,
    stats: Option<ComputedTableStat>,
    next_time: u128,
}

impl<F: DBFile + 'static> IndexSource<F> {
    pub fn new(
        db: &Arc<Connection<F>>,
        table: &Arc<SqlTable>,
        index: usize,
        txn: Option<&Transaction>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Self, SchemaError> {
        let db = &db.database.read().db;
        let cursor = match txn {
            Some(tx) => db.range_scan_bounds_in_txn(
                table.indices[index].db_table_id,
                tx,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?,
            None => db.range_scan_bounds(
                table.indices[index].db_table_id,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            )?,
        };
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
            table: table.clone(),
            index,
            cursor,
            fields: Arc::from(fields),
            key_positions,
            pk_positions,
            stats,
            next_time: 0,
        })
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

impl<F: DBFile + 'static> Source for IndexSource<F> {
    fn plan(&self) -> PlanNode {
        PlanNode::new("IndexScan")
            .detail(format!("{} using {}", self.table.name, self.index_name()))
            .rows(self.stats.as_ref().map(|s| s.table_stat.row_count))
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        let Some(entry) = self.cursor.next()? else {
            self.next_time += start.elapsed().as_nanos();
            return Ok(None);
        };
        let DBIdType::Rec(key) = entry.id() else {
            return Err(SchemaError::InternalSchemaError(format!(
                "index {} has an entry without a column key",
                self.index_name()
            )));
        };
        let mut row = vec![ValueItem::Null; self.fields.len()];
        // zip stops at the indexed columns: a non-unique index's key goes
        // on to carry the row identity, which the data holds as well.
        for (value, &pos) in key.values().iter().zip(&self.key_positions) {
            row[pos] = value.clone();
        }
        if let Some(pk) = &self.pk_positions {
            let identity = from_bytes::<IndexKey>(entry.data())?;
            for (value, &pos) in identity.values().iter().zip(pk) {
                row[pos] = value.clone();
            }
        }
        self.next_time += start.elapsed().as_nanos();
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
        Ok(self.cursor.reset()?)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        self.stats.clone()
    }
}

impl<F: DBFile + 'static> Debug for IndexSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexScan")
            .field("table", &self.table.name)
            .field("index", &self.index_name())
            .finish()
    }
}
