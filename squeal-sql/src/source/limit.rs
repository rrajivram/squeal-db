use std::collections::HashMap;
use store::clock::Instant;
use crate::source::{planinfo::PlanNode};

use crate::source::{QueryStats, Source, merge_stats};

#[derive(Debug)]
pub(crate) struct Limit {
    timer: crate::source::timing::RowTimer,
    source: Box<dyn Source>,
    limit: usize,
    // OFFSET: rows read and dropped before the first one yielded.
    offset: usize,
    skipped: usize,
    yielded: usize,
    time_spent: u128,
}

impl Limit {
    pub(crate) fn new(source: Box<dyn Source>, limit: usize) -> Self {
        Self {
            timer: Default::default(),
            source,
            limit,
            offset: 0,
            skipped: 0,
            yielded: 0,
            time_spent: 0,
        }
    }

    // LIMIT `limit` (None: no limit) OFFSET `offset`.
    pub(crate) fn paged(source: Box<dyn Source>, offset: usize, limit: Option<usize>) -> Self {
        Self {
            offset,
            ..Self::new(source, limit.unwrap_or(usize::MAX))
        }
    }
}

impl Source for Limit {
    fn plan(&self) -> PlanNode {
        let detail = match (self.limit, self.offset) {
            (usize::MAX, o) => format!("offset {o}"),
            (l, 0) => l.to_string(),
            (l, o) => format!("{l} offset {o}"),
        };
        PlanNode::new("Limit").detail(detail).child(self.source.plan())
    }


    fn fields(&self) -> std::sync::Arc<[super::ProjectableField]> {
        self.source.as_ref().fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        let start = self.timer.start();
        while self.skipped < self.offset {
            self.skipped += 1;
            if self.source.next()?.is_none() {
                crate::source::timing::add(&mut self.time_spent, start);
                return Ok(None);
            }
        }
        let result = if self.yielded < self.limit {
            self.yielded += 1;
            self.source.as_mut().next()
        } else {
            Ok(None)
        };
        crate::source::timing::add(&mut self.time_spent, start);
        result
    }

    fn reset(&mut self) -> Result<(), crate::error::SchemaError> {
        self.source.reset()?;
        self.yielded = 0;
        self.skipped = 0;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let this_stats = vec![(
            "Limit".to_string(),
            QueryStats {
                stats: HashMap::from([("time_ns".into(), self.time_spent as f64)]),
                level: 0,
            },
        )];
        Some(merge_stats(this_stats, self.source.query_stats()))
    }
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn src(rows: &[i64]) -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["v"],
            rows.iter().map(|v| vec![ValueItem::Integer(*v)]).collect(),
        ))
    }

    #[test]
    fn test_limit_caps_output_when_the_source_has_more_rows() {
        let mut l = Limit::new(src(&[1, 2, 3, 4, 5]), 3);
        assert_eq!(
            drain(&mut l),
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ]
        );
    }

    #[test]
    fn test_limit_is_a_no_op_when_the_source_has_fewer_rows() {
        let mut l = Limit::new(src(&[1, 2]), 10);
        assert_eq!(
            drain(&mut l),
            vec![vec![ValueItem::Integer(1)], vec![ValueItem::Integer(2)]]
        );
    }

    #[test]
    fn test_limit_zero_yields_nothing() {
        let mut l = Limit::new(src(&[1, 2, 3]), 0);
        assert_eq!(drain(&mut l), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_reset_restarts_both_the_count_and_the_underlying_source() {
        let mut l = Limit::new(src(&[1, 2, 3, 4, 5]), 2);
        let first_pass = drain(&mut l);
        assert_eq!(first_pass.len(), 2);

        l.reset().unwrap();
        let second_pass = drain(&mut l);
        assert_eq!(
            second_pass, first_pass,
            "reset must re-apply the same limit from the start"
        );
    }
}
