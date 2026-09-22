use crate::source::{planinfo::PlanNode};
use std::{collections::HashMap, time::Instant};

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    source::{QueryStats, Source, merge_stats},
};

#[derive(Debug)]
pub(crate) struct AggregatingSource {
    source: Box<dyn Source>,
    next_emit: Option<IndexKey>,
    time_spent: u128,
}

impl AggregatingSource {
    pub(crate) fn new(source: Box<dyn Source>) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            next_emit: None,
            time_spent: 0,
        })
    }
}

impl Source for AggregatingSource {
    fn plan(&self) -> PlanNode {
        PlanNode::new("Distinct").child(self.source.plan())
    }


    fn fields(&self) -> std::sync::Arc<[super::ProjectableField]> {
        self.source.fields()
    }
    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        let start = Instant::now();
        //if next emit is some, continue till next() is not = to next_emit

        let next_emit = if let Some(s) = self.next_emit.take() {
            Some(s)
        } else {
            self.source.next()?
        };
        if let Some(this) = next_emit {
            loop {
                if let Some(next) = self.source.next()? {
                    if this != next {
                        self.next_emit = Some(next);
                        self.time_spent += start.elapsed().as_nanos();
                        return Ok(Some(this));
                    }
                } else {
                    self.time_spent += start.elapsed().as_nanos();
                    return Ok(Some(this));
                }
            }
        }
        self.time_spent += start.elapsed().as_nanos();
        Ok(None)
    }
    // Was a no-op: DISTINCT (this wraps a sort so equal rows are adjacent,
    // then dedups by comparing each to the last) could never be correctly
    // re-scanned, since neither the sort behind it nor `next_emit` (the
    // one row already pulled past the last one emitted) were ever
    // rewound — a re-scanned DISTINCT silently returned zero rows on the
    // second pass. See GroupSource::reset's own fix for the same bug on
    // the GROUP BY side.
    fn reset(&mut self) -> Result<(), SchemaError> {
        self.next_emit = None;
        self.source.reset()
    }

    fn query_stats(&self) -> Option<Vec<(String, super::QueryStats)>> {
        let time_spent = self.time_spent as f64;
        let this_query = QueryStats {
            stats: HashMap::from([("time_ns".into(), time_spent)]),
            level: 0,
        };
        let this_stats = vec![("AggegatingSource".to_string(), this_query)];
        Some(merge_stats(this_stats, self.source.query_stats()))
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::{
        plan::memory::QueryMemory,
        source::{
            sort::{SortField, SortSource},
            test_support::{VecSource, drain},
        },
    };

    // Regression: AggregatingSource::reset() used to be a no-op — see the
    // fix's own doc comment. Wires it over a real SortSource, the same
    // shape plan::logical actually builds for DISTINCT (SortSource::
    // with_fields, then AggregatingSource::new over it), not a pre-sorted
    // VecSource directly — the bug was specifically in the interaction
    // between the two (neither the sort's state nor next_emit was ever
    // rewound).
    #[test]
    fn test_distinct_over_a_real_sort_can_be_reset_and_rescanned() {
        let raw: Box<dyn Source> = Box::new(VecSource::new(
            &["cat"],
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(0)],
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(0)],
                vec![ValueItem::Integer(2)],
            ],
        ));
        let db = store::db::Db::<store::memfile::MemFile>::create("distinct_reset_test.db").unwrap();
        let sorted = SortSource::new(
            raw,
            &[SortField { asc: true, null_first: true, index: 0 }],
            None,
            db,
            QueryMemory::new(1024 * 1024),
        )
        .unwrap();
        let mut agg = AggregatingSource::new(Box::new(sorted)).unwrap();

        let first = drain(&mut agg);
        assert_eq!(
            first,
            vec![
                vec![ValueItem::Integer(0)],
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
            ]
        );

        agg.reset().unwrap();
        let second = drain(&mut agg);
        assert_eq!(second, first, "a reset DISTINCT over a real sort must reproduce the same rows");
    }
}
