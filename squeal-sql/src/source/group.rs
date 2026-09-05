use std::sync::Arc;

use crate::{
    error::SchemaError,
    source::{self, ProjectableField, Source},
};

#[derive(Debug, Clone)]
pub(crate) struct GroupedSource {
    source: Arc<dyn Source>,
    fields: Arc<[ProjectableField]>,
}

impl GroupedSource {
    pub(crate) fn new(
        source: Arc<dyn Source>,
        fields: Vec<ProjectableField>,
    ) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            fields: Arc::from(fields),
        })
    }
}

impl Source for GroupedSource {
    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        todo!()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }
}
