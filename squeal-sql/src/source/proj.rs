use std::sync::Arc;

use store::valueitem::{IndexKey, ValueItem};

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
    sources: Vec<Box<dyn Source>>,
    fields: Vec<ProjectedField>,
}

impl Projection {
    pub(crate) fn new(sources: Vec<Box<dyn Source>>, fields: Vec<ProjectedField>) -> Self {
        Self { sources, fields }
    }
}

impl Source for Projection {
    fn fields(&self) -> Arc<[ProjectedField]> {
        Arc::from(self.fields.clone())
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        let mut res = vec![];
        for s in &mut self.sources {
            res.push(s.next()?);
        }
        if res.iter().all(|f| f.is_none()) {
            return Ok(None);
        }
        if res.iter().any(|f| f.is_none()) {
            return Err(SchemaError::InternalSchemaError(
                "One of sources did not yield results.".into(),
            ));
        }
        let res = res.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();
        let mut out = vec![];
        for (i, f) in self.fields.iter().enumerate() {
            out.push(f.expr.eval(&res, i)?);
        }
        Ok(Some(IndexKey::new_from_owned(out)?))
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

    pub(crate) fn from_field(field: Arc<Field>, source_id: usize, field_id: usize) -> Self {
        Self {
            display_name: field.name.clone(),
            field: field.clone(),
            expr: EvalExpr::Value(source_id, field_id),
            source_id,
            field_id,
        }
    }
}
