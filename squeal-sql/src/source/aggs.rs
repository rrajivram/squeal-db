use std::sync::Arc;

use crate::{
    error::SchemaError,
    source::{self, ProjectedField, Source},
};

#[derive(Debug)]
pub(crate) struct AggregatingSouce {
    source: Box<dyn Source>,
    fields: Arc<[ProjectedField]>,
}

impl AggregatingSouce {
    pub(crate) fn new(
        source: Box<dyn Source>,
        fields: Arc<[ProjectedField]>,
    ) -> Result<Self, SchemaError> {
        Ok(Self { source, fields })
    }
}

impl Source for AggregatingSouce {
    fn fields(&self) -> Arc<[ProjectedField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        todo!()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()
    }
}
