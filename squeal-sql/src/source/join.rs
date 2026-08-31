use crate::source::{ProjectedField, Source};

#[derive(Debug)]
pub(crate) struct UnionJoin {
    sources: Vec<Box<dyn Source>>,
}

impl Source for UnionJoin {
    fn chain(&mut self, _depends: Option<Box<dyn Source>>) {}

    fn fields(&self) -> Vec<ProjectedField> {
        todo!()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        todo!()
    }
}
