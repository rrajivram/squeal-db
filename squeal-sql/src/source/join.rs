use std::sync::Arc;

use crate::source::{ProjectedField, Source};

#[derive(Debug)]
pub(crate) struct UnionJoin {
    sources: Vec<Box<dyn Source>>,
}

impl Source for UnionJoin {
    fn fields(&self) -> Arc<[ProjectedField]> {
        todo!()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        todo!()
    }
}
