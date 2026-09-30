//! Index nested-loop join: for each row of the outer (left) side, seek the
//! inner table by the join key — its primary key (the table's own tree), or
//! an index whose entries then fetch their rows — instead of reading the
//! whole inner table into a hash table. Wins when few outer rows meet a big
//! inner table (see optim::picker::pick_join_seek).
//!
//! Output rows are exactly JoinSource's: outer columns, then the inner
//! table's columns in its own layout. INNER or LEFT only: the outer side is
//! the one whose rows drive the loop, so only its unmatched rows can be
//! kept. A NULL join key matches nothing (see JoinMatcher::keys_match), and
//! the whole ON condition is checked on every candidate pair.

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use postcard::from_bytes;
use store::{
    clock::Instant,
    cursor::{Cursor, KeyRange, RangeCursor},
    db::{DBFile, Db},
    tuple::{DBIdType, Tuple},
    txn::TransactionId,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    datatype::DataType,
    error::SchemaError,
    plan::{eval::EvalExpr, sarg::equal_points},
    source::{
        ProjectableField, QueryStats, Source, column_names, join::JoinType, planinfo::PlanNode,
    },
    table::{SqlTable, VersionedRow},
};

/// Which key of the inner table a nested-loop join seeks.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSeek {
    /// None: the table's primary key (its own tree). Some(i): index i.
    pub index: Option<usize>,
    /// (outer position, inner column), one per leading key column used, in
    /// key order.
    pub keys: Vec<(usize, usize)>,
    /// Expected inner rows per outer row.
    pub rows_per_key: usize,
}

pub(crate) struct NestedLoopJoin<F: DBFile + 'static> {
    outer: Box<dyn Source>,
    db: Arc<Db<F>>,
    reader: TransactionId,
    table: Arc<SqlTable>,
    seek: JoinSeek,
    key_types: Vec<DataType>,
    // Positions of the PRIMARY KEY columns in the inner row, when it has
    // one: an index entry's row identity is then those values.
    has_pk: bool,
    on_expr: EvalExpr,
    join_type: JoinType,
    fields: Arc<[ProjectableField]>,
    inner_width: usize,
    // The outer row being matched, its inner cursor, and whether anything
    // matched it yet.
    current: Option<(IndexKey, RangeCursor<F>, bool)>,
    lookups: usize,
    time_spent: u128,
}

impl<F> NestedLoopJoin<F>
where
    F: DBFile<Item = F> + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        outer: Box<dyn Source>,
        db: Arc<Db<F>>,
        reader: TransactionId,
        table: Arc<SqlTable>,
        seek: JoinSeek,
        on_expr: EvalExpr,
        join_type: JoinType,
    ) -> Result<Self, SchemaError> {
        if !matches!(join_type, JoinType::Inner | JoinType::Left) {
            return Err(SchemaError::InternalSchemaError(format!(
                "a nested-loop join keeps only the outer side's rows, not {join_type:?}"
            )));
        }
        let inner_fields = table.fields();
        let key_types = seek
            .keys
            .iter()
            .map(|(_, c)| inner_fields[*c].datatype)
            .collect();
        let fields: Vec<ProjectableField> = outer
            .fields()
            .iter()
            .cloned()
            .chain(
                inner_fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i)),
            )
            .collect();
        Ok(Self {
            outer,
            db,
            reader,
            has_pk: table.primary_key().is_some(),
            inner_width: inner_fields.len(),
            table,
            seek,
            key_types,
            on_expr,
            join_type,
            fields: Arc::from(fields),
            current: None,
            lookups: 0,
            time_spent: 0,
        })
    }

    // The inner key ranges an outer row's join key selects: every
    // combination of the stored values equal to each key value — none if
    // any is NULL.
    fn ranges_for(&self, outer: &IndexKey) -> Vec<KeyRange> {
        let mut prefixes: Vec<Vec<ValueItem>> = vec![vec![]];
        for ((pos, _), datatype) in self.seek.keys.iter().zip(&self.key_types) {
            let points = equal_points(*datatype, &outer[*pos]).unwrap_or_default();
            prefixes = prefixes
                .iter()
                .flat_map(|p| {
                    points.iter().map(move |v| {
                        let mut p = p.clone();
                        p.push(v.clone());
                        p
                    })
                })
                .collect();
        }
        prefixes.into_iter().map(KeyRange::prefix).collect()
    }

    fn tree(&self) -> store::table::TableIdType {
        match self.seek.index {
            None => self.table.db_table_id,
            Some(i) => self.table.indices[i].db_table_id,
        }
    }

    // The inner row an entry of the sought tree gives: the row itself for
    // the table's own tree, or the row its identity points to for an index.
    fn inner_row(&self, entry: &Tuple) -> Result<Option<IndexKey>, SchemaError> {
        let tuple = match self.seek.index {
            None => entry.clone(),
            Some(_) => {
                let identity = from_bytes::<IndexKey>(entry.data())?;
                let id = if self.has_pk {
                    DBIdType::Rec(identity)
                } else {
                    match identity.values() {
                        [ValueItem::Integer(n)] => DBIdType::Int(*n as u64),
                        other => {
                            return Err(SchemaError::InternalSchemaError(format!(
                                "an index of {} points to a row id that isn't one integer: \
                                 {other:?}",
                                self.table.name
                            )));
                        }
                    }
                };
                match self.db.find_as(self.table.db_table_id, id, self.reader)? {
                    Some(t) => t,
                    None => return Ok(None),
                }
            }
        };
        let row = from_bytes::<VersionedRow>(tuple.data())?;
        Ok(Some(self.table.reproject(&row)?))
    }

    fn combine(outer: &IndexKey, inner: &[ValueItem]) -> Result<IndexKey, SchemaError> {
        let mut values = outer.values().to_vec();
        values.extend_from_slice(inner);
        Ok(IndexKey::new_from_owned(values)?)
    }

    fn step(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        loop {
            if let Some((outer, cursor, _)) = &mut self.current {
                match cursor.next()? {
                    Some(entry) => {
                        let outer = outer.clone();
                        let Some(inner) = self.inner_row(&entry)? else {
                            continue;
                        };
                        let row = Self::combine(&outer, inner.values())?;
                        let on = self.on_expr.eval(std::slice::from_ref(&row), 0)?;
                        if on == ValueItem::Boolean(true) {
                            if let Some((_, _, matched)) = &mut self.current {
                                *matched = true;
                            }
                            return Ok(Some(row));
                        }
                        continue;
                    }
                    None => {
                        let (outer, _, matched) = self.current.take().expect("current");
                        if !matched && matches!(self.join_type, JoinType::Left) {
                            let nulls = vec![ValueItem::Null; self.inner_width];
                            return Ok(Some(Self::combine(&outer, &nulls)?));
                        }
                    }
                }
            }
            let Some(outer) = self.outer.next()? else {
                return Ok(None);
            };
            let ranges = self.ranges_for(&outer);
            self.lookups += 1;
            let cursor = self
                .db
                .key_ranges_scan(self.tree(), Some(self.reader), ranges)?;
            self.current = Some((outer, cursor, false));
        }
    }

    fn key_names(&self) -> (Vec<String>, Vec<String>) {
        let outer = column_names(&self.outer.fields());
        let inner = self.table.fields();
        self.seek
            .keys
            .iter()
            .map(|(o, i)| {
                (
                    outer.get(*o).cloned().unwrap_or_else(|| format!("#{o}")),
                    inner[*i].name.clone(),
                )
            })
            .unzip()
    }
}

impl<F> Source for NestedLoopJoin<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn plan(&self) -> PlanNode {
        let (outer, inner) = self.key_names();
        let keys = outer
            .iter()
            .zip(&inner)
            .map(|(o, i)| format!("outer({o}) = inner({i})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let condition = inner
            .iter()
            .zip(&outer)
            .map(|(i, o)| format!("{i} = outer {o}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let inner_plan = match self.seek.index {
            None => PlanNode::new("TableSeek").detail(format!("{} ({condition})", self.table.name)),
            Some(i) => {
                let index = &self.table.indices[i];
                let name = index.name.clone().unwrap_or_else(|| format!("index #{i}"));
                PlanNode::new("IndexLookup")
                    .detail(format!("{} using {name} ({condition})", self.table.name))
            }
        };
        PlanNode::new("NestedLoopJoin")
            .detail(format!("{:?} on {keys}", self.join_type))
            .child(self.outer.plan().with_role("outer"))
            .child(
                inner_plan
                    .rows(Some(self.seek.rows_per_key))
                    .with_role("each outer row"),
            )
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        let row = self.step();
        self.time_spent += start.elapsed().as_nanos();
        row
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.current = None;
        self.outer.reset()
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let own = (
            format!("NestedLoopJoin:{}", self.table.name),
            QueryStats {
                stats: HashMap::from([
                    ("time_ns".into(), self.time_spent as f64),
                    ("lookups".into(), self.lookups as f64),
                ]),
                level: 0,
            },
        );
        Some(super::merge_stats(vec![own], self.outer.query_stats()))
    }
}

impl<F> Debug for NestedLoopJoin<F>
where
    F: DBFile<Item = F> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NestedLoopJoin")
            .field("inner", &self.table.name)
            .field("join_type", &self.join_type)
            .finish()
    }
}
