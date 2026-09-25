use std::collections::HashMap;
use store::clock::Instant;
use crate::source::{column_names, planinfo::PlanNode};

use store::valueitem::ValueItem;

use crate::{
    error::SchemaError,
    plan::eval::EvalExpr,
    source::{ProjectableField, QueryStats, Source, merge_stats},
};

#[derive(Debug)]
pub(crate) struct WhereSource {
    source: Box<dyn Source>,
    expr: EvalExpr,
    time_spent: u128,
}

impl WhereSource {
    pub(crate) fn new(source: Box<dyn Source>, expr: EvalExpr) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            expr,
            time_spent: 0,
        })
    }
}

impl Source for WhereSource {
    fn plan(&self) -> PlanNode {
        let names = column_names(&self.source.fields());
        PlanNode::new("Filter")
            .detail(self.expr.describe(&names))
            .child(self.source.plan())
    }


    fn fields(&self) -> std::sync::Arc<[ProjectableField]> {
        // A filter passes its input's rows through unchanged, so its row
        // layout IS its input's (it used to report one made-up boolean
        // "where" column, which nothing could resolve a real column
        // position against).
        self.source.fields()
    }

    // A filter doesn't change which row is "current" — see Source::
    // last_id's own doc comment for why this exists at all.
    fn last_id(&self) -> Option<store::tuple::DBIdType> {
        self.source.last_id()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        let start = Instant::now();
        while let Some(res) = self.source.next()? {
            let mut slice = vec![res];
            let should_output = self.expr.eval(&slice, 0)?;
            match should_output {
                ValueItem::Boolean(b) => {
                    if b {
                        self.time_spent += start.elapsed().as_nanos();
                        return Ok(Some(slice.remove(0)));
                    } else {
                        continue;
                    }
                }
                // A NULL predicate (`k = 5` where k is NULL) is "not true":
                // the row is filtered out, it is not an error.
                ValueItem::Null => continue,
                _ => {
                    return Err(SchemaError::InternalSchemaError(
                        "Output of where is not boolean.".into(),
                    ));
                }
            }
        }
        self.time_spent += start.elapsed().as_nanos();
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let this_stats = vec![(
            "WhereSource".to_string(),
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
    use sql_parser::expr::BinaryOp;
    use store::valueitem::ValueItem;

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    fn src() -> Box<dyn Source> {
        Box::new(VecSource::new(
            &["v"],
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ],
        ))
    }

    fn gt_one() -> EvalExpr {
        EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Value(0)),
            op: BinaryOp::Gt,
            rhs: Box::new(EvalExpr::Literal(ValueItem::Integer(1))),
        }
    }

    #[test]
    fn test_where_source_keeps_only_matching_rows() {
        let mut w = WhereSource::new(src(), gt_one()).unwrap();
        assert_eq!(
            drain(&mut w),
            vec![vec![ValueItem::Integer(2)], vec![ValueItem::Integer(3)]]
        );
    }

    #[test]
    fn test_where_source_yields_nothing_when_no_row_matches() {
        let always_false = EvalExpr::Literal(ValueItem::Boolean(false));
        let mut w = WhereSource::new(src(), always_false).unwrap();
        assert_eq!(drain(&mut w), Vec::<Vec<ValueItem>>::new());
    }

    #[test]
    fn test_where_source_yields_everything_when_always_true() {
        let always_true = EvalExpr::Literal(ValueItem::Boolean(true));
        let mut w = WhereSource::new(src(), always_true).unwrap();
        assert_eq!(drain(&mut w).len(), 3);
    }

    #[test]
    fn test_where_source_errors_when_the_predicate_is_not_boolean() {
        // A predicate that evaluates to something other than a boolean or
        // NULL is a planning bug and surfaces as an error rather than
        // silently filtering the row out. (NULL is different: see the next
        // test.)
        let not_boolean = EvalExpr::Literal(ValueItem::Integer(1));
        let mut w = WhereSource::new(src(), not_boolean).unwrap();
        assert!(w.next().is_err());
    }

    #[test]
    fn test_where_source_filters_out_rows_whose_predicate_is_null() {
        // `k = 5` with a NULL k is NULL, which is "not true": no row passes,
        // and it is not an error.
        let null_result = EvalExpr::Literal(ValueItem::Null);
        let mut w = WhereSource::new(src(), null_result).unwrap();
        assert!(w.next().unwrap().is_none());
    }

    #[test]
    fn test_reset_lets_the_same_where_source_rescan_from_the_start() {
        let mut w = WhereSource::new(src(), gt_one()).unwrap();
        let first_pass = drain(&mut w);
        assert_eq!(first_pass.len(), 2);

        w.reset().unwrap();
        let second_pass = drain(&mut w);
        assert_eq!(second_pass, first_pass);
    }
}
