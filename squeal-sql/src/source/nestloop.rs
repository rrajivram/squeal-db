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

use sql_parser::expr::BinaryOp;

use postcard::from_bytes;
use store::{
    cursor::{Cursor, KeyRange, RangeCursor},
    db::{DBFile, Db},
    tuple::{DBIdType, Tuple},
    txn::TransactionId,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    datatype::DataType,
    error::SchemaError,
    plan::{
        eval::EvalExpr,
        sarg::{condition_range, equal_points},
    },
    source::{
        ProjectableField, QueryStats, Source, column_names, join::JoinType, planinfo::PlanNode,
    },
    table::SqlTable,
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
    /// A range on the key column after `keys`: (outer position, op), the
    /// inner column `op` the outer value (`inner > outer.x`).
    pub range: Option<(usize, BinaryOp)>,
}

pub(crate) struct NestedLoopJoin<F: DBFile + 'static> {
    timer: crate::source::timing::RowTimer,
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
    // The inner table's partitions that may hold a match (positions in its
    // `partitions`): each is sought in turn.
    parts: Vec<usize>,
    // When the join key includes the inner table's partition column: the
    // outer row's position holding it, and its type. Each outer row then
    // seeks only the partition its value routes to.
    route_by: Option<(usize, DataType)>,
    current: Option<Current<F>>,
    lookups: usize,
    time_spent: u128,
}

// The outer row being matched: the partition being sought and its cursor,
// the partitions still to seek, and whether anything matched it yet.
struct Current<F: DBFile + 'static> {
    outer: IndexKey,
    part: usize,
    cursor: RangeCursor<F>,
    todo: Vec<usize>,
    matched: bool,
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
        parts: Vec<usize>,
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
        let route_by = table.partition_column().and_then(|(column, field)| {
            seek.keys
                .iter()
                .find(|(_, c)| *c == column)
                .map(|(o, _)| (*o, field.datatype))
        });
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
            timer: Default::default(),
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
            parts,
            route_by,
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
        let range = self.seek.range.and_then(|(pos, op)| {
            let column = self.key_column(self.seek.keys.len())?;
            let datatype = self.table.fields()[column].datatype;
            condition_range(datatype, op, &outer[pos])
        });
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
        match range {
            None if self.seek.range.is_some() => vec![],
            None => prefixes.into_iter().map(KeyRange::prefix).collect(),
            Some(r) => prefixes
                .into_iter()
                .map(|prefix| KeyRange {
                    prefix,
                    lower: r.lower.clone(),
                    upper: r.upper.clone(),
                })
                .filter(|k| !crate::plan::sarg::is_empty(k))
                .collect(),
        }
    }

    // The inner table column at position `n` of the sought key.
    fn key_column(&self, n: usize) -> Option<usize> {
        let fields = self.table.fields();
        let key = match self.seek.index {
            None => self.table.primary_key()?,
            Some(i) => &self.table.indices[i],
        };
        let name = &key.fields.get(n)?.name;
        fields.iter().position(|f| &f.name == name)
    }

    // The tree sought in partition `part`.
    fn tree(&self, part: usize) -> store::table::TableIdType {
        let partition = &self.table.partitions[part];
        match self.seek.index {
            None => partition.rows(),
            Some(i) => partition.index(i),
        }
    }

    // The partitions an outer row's match may be in, in reverse (popped
    // from the end): the one its partition-column value routes to, when
    // the key has it — none for a NULL, which matches nothing.
    fn parts_for(&self, outer: &IndexKey) -> Vec<usize> {
        let mut parts = match (self.route_by, &self.table.partitioning) {
            (Some((pos, datatype)), Some(by)) => {
                let mut routed: Vec<usize> = equal_points(datatype, &outer[pos])
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|v| by.route(&self.table.partitions, v))
                    .filter(|p| self.parts.contains(p))
                    .collect();
                routed.sort();
                routed.dedup();
                routed
            }
            _ => self.parts.clone(),
        };
        parts.reverse();
        parts
    }

    // The inner row an entry of the sought tree gives: the row itself for
    // the table's own tree, or the row its identity points to for an index.
    fn inner_row(&self, part: usize, entry: &Tuple) -> Result<Option<IndexKey>, SchemaError> {
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
                match self
                    .db
                    .find_as(self.table.partitions[part].rows(), id, self.reader)?
                {
                    Some(t) => t,
                    None => return Ok(None),
                }
            }
        };
        Ok(Some(self.table.decode_row(tuple.data(), None)?))
    }

    fn combine(outer: &IndexKey, inner: &[ValueItem]) -> Result<IndexKey, SchemaError> {
        let mut values = outer.values().to_vec();
        values.extend_from_slice(inner);
        Ok(IndexKey::new_from_owned(values)?)
    }

    fn step(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        loop {
            if let Some(cur) = &mut self.current {
                match cur.cursor.next()? {
                    Some(entry) => {
                        let (outer, part) = (cur.outer.clone(), cur.part);
                        let Some(inner) = self.inner_row(part, &entry)? else {
                            continue;
                        };
                        let row = Self::combine(&outer, inner.values())?;
                        let on = self.on_expr.eval(std::slice::from_ref(&row), 0)?;
                        if on == ValueItem::Boolean(true) {
                            if let Some(cur) = &mut self.current {
                                cur.matched = true;
                            }
                            return Ok(Some(row));
                        }
                        continue;
                    }
                    // This partition is done: on to the next, if any.
                    None => {
                        if let Some(part) = cur.todo.pop() {
                            let outer = cur.outer.clone();
                            let ranges = self.ranges_for(&outer);
                            let cursor = self.db.key_ranges_scan(
                                self.tree(part),
                                Some(self.reader),
                                ranges,
                            )?;
                            let cur = self.current.as_mut().expect("current");
                            cur.part = part;
                            cur.cursor = cursor;
                            self.lookups += 1;
                            continue;
                        }
                        let cur = self.current.take().expect("current");
                        if !cur.matched && matches!(self.join_type, JoinType::Left) {
                            let nulls = vec![ValueItem::Null; self.inner_width];
                            return Ok(Some(Self::combine(&cur.outer, &nulls)?));
                        }
                    }
                }
            }
            let Some(outer) = self.outer.next()? else {
                return Ok(None);
            };
            let mut todo = self.parts_for(&outer);
            let Some(part) = todo.pop() else {
                // No partition can hold a match.
                if matches!(self.join_type, JoinType::Left) {
                    let nulls = vec![ValueItem::Null; self.inner_width];
                    return Ok(Some(Self::combine(&outer, &nulls)?));
                }
                continue;
            };
            let ranges = self.ranges_for(&outer);
            self.lookups += 1;
            let cursor = self
                .db
                .key_ranges_scan(self.tree(part), Some(self.reader), ranges)?;
            self.current = Some(Current {
                outer,
                part,
                cursor,
                todo,
                matched: false,
            });
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
        let mut condition: Vec<String> = inner
            .iter()
            .zip(&outer)
            .map(|(i, o)| format!("{i} = outer {o}"))
            .collect();
        let mut keys = keys;
        if let Some((pos, op)) = self.seek.range
            && let Some(column) = self.key_column(self.seek.keys.len())
        {
            let sym = match op {
                BinaryOp::Lt => "<",
                BinaryOp::LtEq => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::GtEq => ">=",
                _ => "?",
            };
            let o = column_names(&self.outer.fields())
                .get(pos)
                .cloned()
                .unwrap_or_else(|| format!("#{pos}"));
            let i = self.table.fields()[column].name.clone();
            condition.push(format!("{i} {sym} outer {o}"));
            let range = format!("inner({i}) {sym} outer({o})");
            keys = if keys.is_empty() {
                range
            } else {
                format!("{keys} AND {range}")
            };
        }
        let condition = condition.join(" AND ");
        // Which of a partitioned table's partitions each outer row seeks.
        let table = if !self.table.is_partitioned() {
            self.table.name.clone()
        } else {
            let total = self.table.partitions.len();
            let of = if self.parts.len() == total {
                format!("{total} partitions")
            } else {
                format!("{} of {total} partitions", self.parts.len())
            };
            if self.route_by.is_some() {
                format!("{} (the one of {of} its key routes to)", self.table.name)
            } else {
                format!("{} (each of {of})", self.table.name)
            }
        };
        let inner_plan = match self.seek.index {
            None => PlanNode::new("TableSeek").detail(format!("{table} ({condition})")),
            Some(i) => {
                let index = &self.table.indices[i];
                let name = index.name.clone().unwrap_or_else(|| format!("index #{i}"));
                PlanNode::new("IndexLookup").detail(format!("{table} using {name} ({condition})"))
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
        let start = self.timer.start();
        let row = self.step();
        crate::source::timing::add(&mut self.time_spent, start);
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
