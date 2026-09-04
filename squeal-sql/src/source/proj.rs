use std::sync::Arc;

use store::valueitem::IndexKey;

use crate::{
    error::SchemaError,
    plan::eval::EvalExpr,
    source::{ProjectedField, Source},
    table::Field,
};

// supported projections:
//  *

#[derive(Debug)]
pub(crate) struct Projection {
    source: Box<dyn Source>,
    fields: Vec<ProjectedField>,
}

impl Projection {
    pub(crate) fn new(source: Box<dyn Source>, fields: Vec<ProjectedField>) -> Self {
        Self { source, fields }
    }
}

impl Source for Projection {
    fn fields(&self) -> Arc<[ProjectedField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        if let Some(res) = self.source.next()? {
            let mut out = vec![];
            let res = &[res];
            for (i, f) in self.fields.iter().enumerate() {
                out.push(f.expr.eval(res, i)?);
            }
            return Ok(Some(IndexKey::new_from_owned(out)?));
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()
    }
}

impl ProjectedField {
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
