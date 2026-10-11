// GROUP BY by hashing: one accumulator set per group, kept in a hash
// table, instead of sorting every input row so that groups arrive one
// after another (SortSource + GroupSource). Memory is then proportional to
// the number of groups, not the number of rows: `GROUP BY item` over a
// million orders holds ten groups, where the sort held a million rows.
//
// Each group's memory is reserved against the query's budget. Should the
// groups outgrow it, the input is rewound and the query falls back to the
// sort-based plan, which spills to scratch pages — so a GROUP BY with
// more groups than memory still runs, as it always did.
//
// Groups are output in key order (ascending, NULLs first), as the
// sort-based plan outputs them: nothing a caller saw before changes.
use std::{collections::HashMap, sync::Arc};

use store::{
    db::{DBFile, Db},
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, funcs::FuncTrait, memory::QueryMemory},
    source::{
        ProjectableField, QueryStats, Source, column_names,
        group::GroupSource,
        merge_stats, output_label,
        planinfo::PlanNode,
        sort::{SortField, SortSource, cmp_by_fields},
    },
};

// What one group is charged against the query's budget, beyond its key:
// its copy of the SELECT list's expressions (accumulators included) and
// the values last computed. An estimate — an expression tree is a few
// hundred bytes, and a COUNT(DISTINCT)'s set grows past it, as it does
// in the sort-based plan too.
const GROUP_BYTES: usize = 512;
const PER_FIELD_BYTES: usize = 128;

struct Group {
    key: IndexKey,
    fields: Vec<ProjectableField>,
    having: Option<EvalExpr>,
    values: Vec<ValueItem>,
    keep: bool,
    _reserved: crate::plan::memory::MemReservation,
}

enum State {
    // Not read yet.
    Start,
    // Grouped by hashing: the output rows, in key order.
    Hashed(std::vec::IntoIter<IndexKey>),
    // Too many groups for the budget: the sort-based plan.
    Sorted(Box<GroupSource>),
}

pub(crate) struct HashGroupSource<F: DBFile + 'static> {
    // Taken by the sort-based plan if it falls back.
    source: Option<Box<dyn Source>>,
    fields: Vec<ProjectableField>,
    key_positions: Vec<usize>,
    having: Option<EvalExpr>,
    db: Arc<Db<F>>,
    mem: Arc<QueryMemory>,
    state: State,
    groups: usize,
    fell_back: bool,
}

impl<F: DBFile + 'static> std::fmt::Debug for HashGroupSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashGroupSource")
            .field("key_positions", &self.key_positions)
            .field("groups", &self.groups)
            .field("fell_back", &self.fell_back)
            .finish()
    }
}

impl<F> HashGroupSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(
        source: Box<dyn Source>,
        fields: Vec<ProjectableField>,
        key_positions: Vec<usize>,
        having: Option<EvalExpr>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
    ) -> Self {
        Self {
            source: Some(source),
            fields,
            key_positions,
            having,
            db,
            mem,
            state: State::Start,
            groups: 0,
            fell_back: false,
        }
    }

    fn reset_aggregates(fields: &mut [ProjectableField], having: &mut Option<EvalExpr>) -> Result<(), SchemaError> {
        for f in fields.iter_mut() {
            for func in &mut f.expr.get_funcs() {
                func.reset()?;
            }
        }
        if let Some(having) = having {
            for func in &mut having.get_funcs() {
                func.reset()?;
            }
        }
        Ok(())
    }

    fn is_true(v: ValueItem) -> Result<bool, SchemaError> {
        match v {
            ValueItem::Boolean(b) => Ok(b),
            ValueItem::Null => Ok(false),
            _ => Err(SchemaError::InternalSchemaError(
                "Output of having is not boolean.".into(),
            )),
        }
    }

    // Reads the whole input into groups; None if they outgrew the budget.
    fn hash(&mut self) -> Result<Option<Vec<IndexKey>>, SchemaError> {
        let source = self.source.as_mut().expect("the input, until a fallback takes it");
        let mut groups: HashMap<Vec<ValueItem>, Group> = HashMap::new();
        while let Some(row) = source.next()? {
            let key: Vec<ValueItem> = self.key_positions.iter().map(|&i| row[i].clone()).collect();
            let group = match groups.get_mut(&key) {
                Some(g) => g,
                None => {
                    let bytes = GROUP_BYTES
                        + PER_FIELD_BYTES * self.fields.len()
                        + key.iter().map(|v| v.size()).sum::<usize>();
                    let reserved = match self.mem.try_reserve(bytes) {
                        Ok(r) => r,
                        Err(SchemaError::QueryMemoryExceeded { .. }) => return Ok(None),
                        Err(e) => return Err(e),
                    };
                    let mut fields = self.fields.clone();
                    let mut having = self.having.clone();
                    Self::reset_aggregates(&mut fields, &mut having)?;
                    let group = Group {
                        key: IndexKey::new_from(&key)?,
                        fields,
                        having,
                        values: Vec::with_capacity(self.fields.len()),
                        keep: true,
                        _reserved: reserved,
                    };
                    groups.entry(key).or_insert(group)
                }
            };
            // Feed the row to the group's own accumulators: its SELECT list
            // (only the last row's values are the group's output) and HAVING.
            let data = [row];
            group.values.clear();
            for (i, f) in group.fields.iter_mut().enumerate() {
                group.values.push(f.expr.eval(&data, i)?);
            }
            group.keep = match &mut group.having {
                Some(having) => Self::is_true(having.eval(&data, 0)?)?,
                None => true,
            };
        }
        self.groups = groups.len();
        let order: Vec<SortField> = (0..self.key_positions.len())
            .map(|index| SortField {
                asc: true,
                null_first: true,
                index,
            })
            .collect();
        let mut kept: Vec<Group> = groups.into_values().filter(|g| g.keep).collect();
        kept.sort_by(|a, b| cmp_by_fields(&a.key, &b.key, &order));
        let rows = kept
            .into_iter()
            .map(|g| IndexKey::new_from(&g.values))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(rows))
    }

    // Rewinds the input and groups it the sort-based way.
    fn fall_back(&mut self) -> Result<GroupSource, SchemaError> {
        self.fell_back = true;
        let mut source = self.source.take().expect("the input");
        source.reset()?;
        let sorted = SortSource::with_fields(
            source,
            self.db.clone(),
            self.mem.clone(),
            &self.key_positions,
        )?;
        let group = GroupSource::new(Box::new(sorted), self.fields.clone(), self.key_positions.clone());
        Ok(match self.having.clone() {
            Some(h) => group.with_having(h),
            None => group,
        })
    }
}

impl<F> Source for HashGroupSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        if let State::Sorted(g) = &self.state {
            return g.plan();
        }
        let input = self.source.as_ref().map(|s| s.fields()).unwrap_or_default();
        let names = column_names(&input);
        let outputs = self
            .fields
            .iter()
            .map(|f| output_label(f, &names))
            .collect::<Vec<_>>()
            .join(", ");
        let outputs = match &self.having {
            Some(having) => format!("{outputs} having {}", having.describe(&names)),
            None => outputs,
        };
        let keys = self
            .key_positions
            .iter()
            .map(|k| names.get(*k).cloned().unwrap_or_else(|| format!("#{k}")))
            .collect::<Vec<_>>()
            .join(", ");
        let node = PlanNode::new("HashAggregate").detail(format!("by {keys}: {outputs}"));
        match &self.source {
            Some(s) => node.child(s.plan()),
            None => node,
        }
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if let State::Start = self.state {
            self.state = match self.hash()? {
                Some(rows) => State::Hashed(rows.into_iter()),
                None => State::Sorted(Box::new(self.fall_back()?)),
            };
        }
        match &mut self.state {
            State::Hashed(rows) => Ok(rows.next()),
            State::Sorted(g) => g.next(),
            State::Start => unreachable!("set above"),
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        match std::mem::replace(&mut self.state, State::Start) {
            State::Sorted(mut g) => {
                // The input now lives in the sort-based plan: keep using it.
                g.reset()?;
                self.state = State::Sorted(g);
            }
            _ => {
                if let Some(s) = &mut self.source {
                    s.reset()?;
                }
            }
        }
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        if let State::Sorted(g) = &self.state {
            return g.query_stats();
        }
        let stats = QueryStats {
            stats: HashMap::from([("groups".to_string(), self.groups as f64)]),
            level: 0,
        };
        Some(merge_stats(
            vec![("HashAggregate".into(), stats)],
            self.source.as_ref().and_then(|s| s.query_stats()),
        ))
    }
}
