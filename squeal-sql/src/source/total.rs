//! A whole-table aggregate (no GROUP BY) read in batches (see crate::batch).
//!
//! Each aggregate takes the batch's column a whole column at a time
//! (FuncObj::feed_batch), never a row of values. The one output row is then
//! what an empty group's would be over those aggregates' totals
//! (EvalExpr::eval_empty_group): each reads its current value, and
//! everything around them is computed from those.

use std::{collections::HashMap, sync::Arc};

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    plan::funcs::FuncTrait,
    source::{
        ProjectableField, QueryStats, Source, column_names, merge_stats, output_label,
        planinfo::PlanNode,
    },
};

// Totals computed on this thread: what tests check the batch path ran by.
#[cfg(test)]
thread_local! {
    pub(crate) static TOTALS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
pub(crate) struct BatchTotal {
    timer: crate::source::timing::RowTimer,
    source: Box<dyn Source>,
    fields: Vec<ProjectableField>,
    done: bool,
    time_spent: u128,
}

impl BatchTotal {
    /// Whether the aggregate of `fields` over `source` can be read in
    /// batches: `source` gives them, and every aggregate can take one.
    pub(crate) fn fits(source: &dyn Source, fields: &[ProjectableField]) -> bool {
        source.batches()
            && fields
                .iter()
                .all(|f| f.expr.every_aggregate(&|a| a.batch_input().is_some()))
    }

    /// The aggregate of `fields` over `source` (see `fits`).
    pub(crate) fn new(source: Box<dyn Source>, fields: Vec<ProjectableField>) -> Self {
        BatchTotal {
            timer: Default::default(),
            source,
            fields,
            done: false,
            time_spent: 0,
        }
    }

    fn total(&mut self) -> Result<IndexKey, SchemaError> {
        #[cfg(test)]
        TOTALS.with(|t| t.set(t.get() + 1));
        for f in &mut self.fields {
            f.expr.for_each_aggregate(&mut |a| a.reset())?;
        }
        while self.source.advance_batch()? {
            let batch = self.source.batch().ok_or_else(|| {
                SchemaError::InternalSchemaError("a source moved to a batch it doesn't lend".into())
            })?;
            for f in &mut self.fields {
                f.expr.for_each_aggregate(&mut |a| a.feed_batch(batch))?;
            }
        }
        let row = self
            .fields
            .iter()
            .map(|f| f.expr.eval_empty_group())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(IndexKey::new_from_owned(row)?)
    }
}

impl Source for BatchTotal {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if self.done {
            return Ok(None);
        }
        let start = self.timer.start();
        let row = self.total();
        crate::source::timing::add(&mut self.time_spent, start);
        self.done = true;
        row.map(Some)
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        Arc::from(self.fields.clone())
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        self.done = false;
        Ok(())
    }

    // As GroupSource's whole-table aggregate shows itself: the same step,
    // read another way.
    fn plan(&self) -> PlanNode {
        let names = column_names(&self.source.fields());
        let outputs = self
            .fields
            .iter()
            .map(|f| output_label(f, &names))
            .collect::<Vec<_>>()
            .join(", ");
        PlanNode::new("Aggregate")
            .detail(outputs)
            .child(self.source.plan())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let stats = QueryStats {
            stats: HashMap::from([("time_ns".to_string(), self.time_spent as f64)]),
            level: 0,
        };
        Some(merge_stats(
            vec![("BatchTotal".into(), stats)],
            self.source.query_stats(),
        ))
    }
}
