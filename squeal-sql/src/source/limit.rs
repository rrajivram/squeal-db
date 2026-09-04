use crate::source::Source;

#[derive(Debug)]
pub(crate) struct Limit {
    source: Box<dyn Source>,
    limit: usize,
    yielded: usize,
}

impl Limit {
    pub(crate) fn new(source: Box<dyn Source>, limit: usize) -> Self {
        Self {
            source,
            limit,
            yielded: 0,
        }
    }
}

impl Source for Limit {
    fn fields(&self) -> std::sync::Arc<[super::ProjectedField]> {
        self.source.as_ref().fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        if self.yielded < self.limit {
            self.yielded += 1;
            self.source.as_mut().next()
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<(), crate::error::SchemaError> {
        self.source.reset()?;
        self.yielded = 0;
        Ok(())
    }
}
