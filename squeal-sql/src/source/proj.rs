use std::{collections::HashMap, sync::Arc, time::Instant};

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    plan::eval::EvalExpr,
    source::{ProjectableField, QueryStats, Source, merge_stats},
    table::Field,
};

// supported projections:
//  *

#[derive(Debug)]
pub(crate) struct Projection {
    source: Box<dyn Source>,
    fields: Vec<ProjectableField>,
    time_spent: u128,
    eval_time: u128,
}

impl Projection {
    pub(crate) fn new(source: Box<dyn Source>, fields: Vec<ProjectableField>) -> Self {
        Self {
            source,
            fields,
            time_spent: 0,
            eval_time: 0,
        }
    }
}

impl Source for Projection {
    fn fields(&self) -> Arc<[ProjectableField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        let start = Instant::now();
        if let Some(res) = self.source.next()? {
            self.time_spent += start.elapsed().as_nanos();
            let mut out = vec![];
            let res = &[res];
            let eval_start = Instant::now();
            for (i, f) in self.fields.iter_mut().enumerate() {
                out.push(f.expr.eval(res, i)?);
            }
            self.eval_time += eval_start.elapsed().as_nanos();
            return Ok(Some(IndexKey::new_from_owned(out)?));
        }
        self.time_spent += start.elapsed().as_nanos();
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()
    }

    // Sits at the top of nearly every SELECT's Source tree — without
    // its own entry here, the trait default (`None`) would silently
    // swallow every child's stats for any query with an explicit
    // column list, and eval() cost (the one thing Projection actually
    // does per row) would go unmeasured entirely.
    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let this_stats = vec![(
            "Projection".to_string(),
            QueryStats {
                stats: HashMap::from([
                    ("time_ns".into(), self.time_spent as f64),
                    ("eval_time_ns".into(), self.eval_time as f64),
                ]),
                level: 0,
            },
        )];
        Some(merge_stats(this_stats, self.source.query_stats()))
    }
}

impl ProjectableField {
    pub(crate) fn new_with_field(
        display_name: String,
        field: Arc<Field>,
        source_id: usize,
        field_id: usize,
        expr: EvalExpr,
    ) -> Self {
        Self {
            display_name,
            field,
            expr,
            source_id,
            field_id,
        }
    }

    // Every call site describes one source's own fields before UnionJoin
    // ever combines anything (source_id is always 0 here — see
    // TableSource/RunSource/WhereSource's own construction) — so the flat
    // position within that source's own not-yet-combined row is just
    // field_id, not something flat_position needs to compute.
    pub(crate) fn from_field(field: Arc<Field>, source_id: usize, field_id: usize) -> Self {
        Self {
            display_name: field.name.clone(),
            field: field.clone(),
            expr: EvalExpr::Value(field_id),
            source_id,
            field_id,
        }
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
            &["a", "b"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Integer(10)],
                vec![ValueItem::Integer(2), ValueItem::Integer(20)],
            ],
        ))
    }

    fn field(name: &str, expr: EvalExpr) -> ProjectableField {
        ProjectableField::new_with_field(name.to_string(), Arc::new(Field::from(name)), 0, 0, expr)
    }

    #[test]
    fn test_projection_reorders_and_subsets_fields() {
        // Project just "b" then "a" — the reverse of the source's own
        // column order — to confirm the output follows the projection
        // list, not the source's own layout.
        let fields = vec![
            field("b", EvalExpr::Value(1)),
            field("a", EvalExpr::Value(0)),
        ];
        let mut p = Projection::new(src(), fields);
        assert_eq!(
            drain(&mut p),
            vec![
                vec![ValueItem::Integer(10), ValueItem::Integer(1)],
                vec![ValueItem::Integer(20), ValueItem::Integer(2)],
            ]
        );
    }

    #[test]
    fn test_projection_evaluates_a_computed_expression() {
        let sum = EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Value(0)),
            op: BinaryOp::Plus,
            rhs: Box::new(EvalExpr::Value(1)),
        };
        let mut p = Projection::new(src(), vec![field("a+b", sum)]);
        assert_eq!(
            drain(&mut p),
            vec![vec![ValueItem::Integer(11)], vec![ValueItem::Integer(22)]]
        );
    }

    #[test]
    fn test_projection_fields_reports_the_projection_list_not_the_source() {
        let fields = vec![field("only_this", EvalExpr::Value(0))];
        let p = Projection::new(src(), fields);
        let names = p
            .fields()
            .iter()
            .map(|f| f.display_name.clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["only_this".to_string()]);
    }

    #[test]
    fn test_reset_delegates_to_the_underlying_source() {
        let mut p = Projection::new(src(), vec![field("a", EvalExpr::Value(0))]);
        let first_pass = drain(&mut p);
        assert_eq!(first_pass.len(), 2);

        p.reset().unwrap();
        let second_pass = drain(&mut p);
        assert_eq!(second_pass, first_pass);
    }
}
