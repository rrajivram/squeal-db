use std::{collections::HashMap, fmt::Debug, sync::Arc};

use store::{db::DBFile, valueitem::IndexKey};

use crate::{
    conn::connection::Connection,
    error::SchemaError,
    optim::table_stats::TableStat,
    plan::eval::EvalExpr,
    table::{Field, SqlTable},
};

pub mod aggr;
pub(crate) mod group;
pub mod hash;
mod index;
pub(crate) mod join;
mod joinmatch;
pub mod planinfo;
pub mod limit;
pub mod proj;
pub(crate) mod run;
pub mod sort;
mod sortjoin;
pub mod table;
#[cfg(test)]
mod tests;
pub mod where_source;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ProjectableField {
    pub(crate) field: Arc<Field>,
    pub(crate) display_name: String,
    pub(crate) source_id: usize,
    pub(crate) field_id: usize,
    pub(crate) expr: EvalExpr,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ComputedTableStat {
    pub table_stat: TableStat,
    pub indices: Option<Vec<IndexStat>>,
    pub self_index: Option<IndexStat>,
}

#[derive(Debug, Clone)]
pub struct IndexStat {
    pub levels: usize,
    pub nodes_per_page: usize,
    pub unique: bool,
}

#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    stats: HashMap<String, f64>,
    // How deeply nested this entry is in the Source pipeline that
    // produced it — 0 for whichever Source's own stats() call is the
    // root of a given Vec<(String, QueryStats)> (every stats() impl
    // below constructs its OWN entry at level 0; merge_stats is what
    // bumps a child's entries by one level as they're folded into a
    // parent's list, on the way back up). A pretty-printer (see
    // squeal-cli) indents by this to show the pipeline's actual
    // wrapping structure — a flat Vec on its own has no way to tell
    // "this ran inside that" from "this ran alongside that."
    level: usize,
}

impl QueryStats {
    pub fn level(&self) -> usize {
        self.level
    }

    /// This entry's own stat values, keyed by name — e.g. `"probe_ns"`.
    /// A `_ns` suffix (by convention, not enforced) means nanoseconds,
    /// for callers (like squeal-cli's pretty-printer) that want to
    /// render times more readably than a raw f64 count.
    pub fn stats(&self) -> &HashMap<String, f64> {
        &self.stats
    }
}

pub trait Source: Debug + Send {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError>;
    fn fields(&self) -> Arc<[ProjectableField]>;
    fn reset(&mut self) -> Result<(), SchemaError>;
    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        None
    }
    fn table_stats(&self) -> Option<ComputedTableStat> {
        None
    }
    // What EXPLAIN shows for this step and (through its own children) the
    // steps feeding it. The default is a bare leaf named by Debug; every
    // real operator overrides it.
    fn plan(&self) -> planinfo::PlanNode {
        planinfo::PlanNode::new(format!("{self:?}"))
    }
}

// Column names of a source's output row, in order — what a `Value(i)` in an
// expression reading that row refers to (see EvalExpr::describe).
pub(crate) fn column_names(fields: &[ProjectableField]) -> Vec<String> {
    fields.iter().map(|f| f.display_name.clone()).collect()
}

// How a Projection/GroupAggregate step shows one of its output columns:
// the expression, plus `AS name` when the name adds information (it does not
// for a bare column, or an unnamed expression).
pub(crate) fn output_label(f: &ProjectableField, names: &[String]) -> String {
    let expr = f.expr.describe(names);
    let bare_column = expr.split('#').next() == Some(f.display_name.as_str());
    if f.display_name == expr || f.display_name == "none" || bare_column {
        expr
    } else {
        format!("{expr} AS {}", f.display_name)
    }
}

pub(crate) fn compute_table_stats<F: DBFile + 'static>(
    conn: &Arc<Connection<F>>,
    schema: &str,
    table: &Arc<SqlTable>,
) -> Result<Option<ComputedTableStat>, SchemaError> {
    if let Some(table_stat) = conn.schema(schema)?.get_table_stats(table.db_table_id)? {
        let (levels, nodes_per_page) = conn
            .database
            .read()
            .db
            .btree_range_params(table.db_table_id)?;
        let mut indices = vec![];
        for index in &table.indices {
            let unique = index.is_primary || index.is_unique;
            let (levels, nodes_per_page) = conn
                .database
                .read()
                .db
                .btree_range_params(index.db_table_id)?;
            indices.push(IndexStat {
                levels,
                nodes_per_page,
                unique,
            })
        }
        return Ok(Some(ComputedTableStat {
            table_stat,
            indices: Some(indices),
            self_index: Some(IndexStat {
                levels,
                nodes_per_page,
                unique: true,
            }),
        }));
    }

    Ok(None)
}

// Folds a child Source's own stats() result into `this_stats` (the
// caller's own entry/entries so far), bumping every one of the child's
// levels by exactly 1 relative to that child's OWN root (level 0) —
// not relative to whatever's already accumulated in `this_stats`. That
// distinction matters for a Source with more than one child (e.g.
// HashedSource merging its left AND right sources in turn, via two
// separate merge_stats calls against the same growing `res`): each
// child's root must land at the SAME level (a sibling of the other
// child), not progressively deeper just because it was merged in
// second. Bumping strictly off the incoming Vec's own levels — never
// off `this_stats`'s current contents — gives exactly that.
fn merge_stats(
    this_stats: Vec<(String, QueryStats)>,
    that: Option<Vec<(String, QueryStats)>>,
) -> Vec<(String, QueryStats)> {
    let mut this_stats = this_stats;
    if let Some(that) = that {
        this_stats.extend(that.into_iter().map(|(name, mut stats)| {
            stats.level += 1;
            (name, stats)
        }));
    }
    this_stats
}

// Shared by every other Source's own test module (join, limit, proj,
// where_source, ...) — a minimal in-memory Source backed by a plain
// Vec<Vec<ValueItem>>, so those tests can exercise combining/filtering/
// limiting logic in isolation without a real Db/SqlTable/Connection.
#[cfg(test)]
pub(crate) mod test_support {
    use store::valueitem::{IndexKey, ValueItem};

    use super::{Arc, Debug, ProjectableField, SchemaError, Source};
    use crate::table::Field;

    #[derive(Debug)]
    pub(crate) struct VecSource {
        rows: Vec<Vec<ValueItem>>,
        pos: usize,
        fields: Arc<[ProjectableField]>,
    }

    impl VecSource {
        pub(crate) fn new(field_names: &[&str], rows: Vec<Vec<ValueItem>>) -> Self {
            let fields = field_names
                .iter()
                .enumerate()
                .map(|(i, name)| ProjectableField::from_field(Arc::new(Field::from(*name)), 0, i))
                .collect::<Vec<_>>();
            Self {
                rows,
                pos: 0,
                fields: Arc::from(fields),
            }
        }
    }

    impl Source for VecSource {
        fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
            if self.pos >= self.rows.len() {
                return Ok(None);
            }
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Ok(Some(IndexKey::new_from_owned(row)?))
        }

        fn fields(&self) -> Arc<[ProjectableField]> {
            self.fields.clone()
        }

        fn reset(&mut self) -> Result<(), SchemaError> {
            self.pos = 0;
            Ok(())
        }
    }

    // Test-only convenience: drain every remaining row as plain
    // Vec<ValueItem>, so a test can assert on values directly instead of
    // destructuring IndexKey each time.
    pub(crate) fn drain(source: &mut dyn Source) -> Vec<Vec<ValueItem>> {
        let mut out = vec![];
        while let Some(row) = source.next().unwrap() {
            out.push(row.values().to_vec());
        }
        out
    }
}
