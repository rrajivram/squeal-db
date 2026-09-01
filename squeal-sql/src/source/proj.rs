use std::sync::Arc;

use store::valueitem::{IndexKey, ValueItem};

use crate::{
    error::SchemaError,
    source::{self, ProjectedField, Source},
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
        let mut out = vec![];
        for f in &self.fields {
            out.push(
                res[f.source_id]
                    .as_ref()
                    .map_or(ValueItem::Null, |r| r[f.field_id].clone()),
            )
        }
        Ok(Some(IndexKey::new_from_owned(out)?))
    }
}

impl From<String> for ProjectedField {
    fn from(value: String) -> Self {
        Self {
            field: Arc::new(Field::from(value.clone())),
            display_name: value,
            source_id: 0,
            field_id: 0,
        }
    }
}

impl From<Arc<Field>> for ProjectedField {
    fn from(value: Arc<Field>) -> Self {
        Self {
            field: value.clone(),
            display_name: value.name.clone(),
            source_id: 0,
            field_id: 0,
        }
    }
}

impl From<Field> for ProjectedField {
    fn from(value: Field) -> Self {
        Self {
            display_name: value.name.clone(),
            field: Arc::new(value),
            source_id: 0,
            field_id: 0,
        }
    }
}

impl ProjectedField {
    pub(crate) fn new(name: String, source_id: usize, field_id: usize) -> Self {
        Self {
            display_name: name.clone(),
            field: Arc::new(Field::from(name)),
            source_id,
            field_id,
        }
    }

    pub(crate) fn new_with_field(
        display_name: String,
        field: Arc<Field>,
        source_id: usize,
        field_id: usize,
    ) -> Self {
        Self {
            display_name,
            field,
            source_id,
            field_id,
        }
    }
}
