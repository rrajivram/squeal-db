use std::sync::Arc;

use sql_parser::query::SelectItem;

use crate::source::Source;

#[derive(Debug)]
pub(crate) struct Proj {
    proj: SelectItem,
    child: Option<Box<dyn Source>>,
}

impl Proj {
    pub(crate) fn new(proj: SelectItem) -> Self {
        Self { proj, child: None }
    }
}

impl Source for Proj {
    fn chain(&mut self, depends: Option<Box<dyn Source>>) {
        self.child = depends;
    }

    fn fields(&self) -> std::sync::Arc<[std::sync::Arc<crate::table::Field>]> {
        if let Some(child) = &self.child {
            child.fields()
        } else {
            Arc::from([])
        }
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        if let Some(child) = &mut self.child {
            child.next()
        } else {
            Err(crate::error::SchemaError::UnknownError(
                "No child ser".into(),
            ))
        }
    }
}
