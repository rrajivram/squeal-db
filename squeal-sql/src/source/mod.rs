use std::{collections::HashMap, fmt::Debug, sync::Arc};

use store::valueitem::IndexKey;

use crate::{error::SchemaError, plan::eval::EvalExpr, table::Field};

pub mod aggr;
pub(crate) mod group;
pub mod hash;
pub(crate) mod join;
pub mod limit;
pub mod proj;
pub(crate) mod run;
pub mod sort;
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

pub trait Source: Debug {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError>;
    fn fields(&self) -> Arc<[ProjectableField]>;
    fn reset(&mut self) -> Result<(), SchemaError>;
    fn stats(&self) -> Option<Vec<(String, QueryStats)>> {
        None
    }
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
