use std::sync::Arc;

use store::valueitem::ValueItem;

use crate::{
    error::SchemaError,
    plan::eval::EvalExpr,
    source::{ProjectableField, Source},
    table::Field,
};

#[derive(Debug)]
pub(crate) struct WhereSource {
    source: Box<dyn Source>,
    expr: EvalExpr,
    field: Arc<Field>,
}

impl WhereSource {
    pub(crate) fn new(source: Box<dyn Source>, expr: EvalExpr) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            expr,
            field: Arc::new(Field::new(
                "where".into(),
                crate::datatype::DataType::Boolean,
                false,
                None,
            )?),
        })
    }
}

impl Source for WhereSource {
    fn fields(&self) -> std::sync::Arc<[ProjectableField]> {
        Arc::from([ProjectableField::from_field(self.field.clone(), 0, 0)])
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        while let Some(res) = self.source.next()? {
            let mut slice = vec![res];
            let should_output = self.expr.eval(&slice, 0)?;
            match should_output {
                ValueItem::Boolean(b) => {
                    if b {
                        return Ok(Some(slice.remove(0)));
                    } else {
                        continue;
                    }
                }
                _ => {
                    return Err(SchemaError::InternalSchemaError(
                        "Output of where is not boolean.".into(),
                    ));
                }
            }
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        Ok(())
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
        // Current, documented limitation: a predicate that evaluates to
        // something other than a boolean (including NULL — there's no
        // three-valued WHERE logic yet, see CrateValueItem::binary's own
        // doc comment) surfaces as an error rather than silently
        // filtering the row out.
        let not_boolean = EvalExpr::Literal(ValueItem::Integer(1));
        let mut w = WhereSource::new(src(), not_boolean).unwrap();
        assert!(w.next().is_err());
    }

    #[test]
    fn test_where_source_errors_on_a_null_predicate_result() {
        let null_result = EvalExpr::Literal(ValueItem::Null);
        let mut w = WhereSource::new(src(), null_result).unwrap();
        assert!(w.next().is_err());
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
