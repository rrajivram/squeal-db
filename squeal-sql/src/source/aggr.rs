use store::valueitem::IndexKey;

use crate::{error::SchemaError, source::Source};

#[derive(Debug)]
pub(crate) struct AggregatingSource {
    source: Box<dyn Source>,
    next_emit: Option<IndexKey>,
}

impl AggregatingSource {
    pub(crate) fn new(source: Box<dyn Source>) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            next_emit: None,
        })
    }
}

impl Source for AggregatingSource {
    fn fields(&self) -> std::sync::Arc<[super::ProjectableField]> {
        self.source.fields()
    }
    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        //if next emit is some, continue till next() is not = to next_emit

        let next_emit = if let Some(s) = self.next_emit.take() {
            Some(s)
        } else {
            self.source.next()?
        };
        if let Some(this) = next_emit {
            loop {
                if let Some(next) = self.source.next()? {
                    if this != next {
                        self.next_emit = Some(next);
                        return Ok(Some(this));
                    }
                } else {
                    return Ok(Some(this));
                }
            }
        }

        Ok(None)
    }
    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }
}
