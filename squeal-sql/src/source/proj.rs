use std::sync::Arc;

use crate::{
    source::{ProjectedField, Source},
    table::Field,
};

#[derive(Debug)]
pub(crate) struct WildcardProj {
    proj: Vec<Vec<ProjectedField>>,
    child: Option<Box<dyn Source>>,
}

#[allow(unused)]
#[derive(Debug)]
pub(crate) struct ExprProj {
    proj: Vec<Vec<Arc<Field>>>,
    child: Option<Box<dyn Source>>,
}

impl WildcardProj {
    pub(crate) fn new(proj: &[Vec<ProjectedField>]) -> Self {
        Self {
            proj: proj.to_vec(),
            child: None,
        }
    }
}

impl Source for WildcardProj {
    fn chain(&mut self, depends: Option<Box<dyn Source>>) {
        self.child = depends;
    }

    fn fields(&self) -> Vec<ProjectedField> {
        self.proj.iter().flatten().cloned().collect::<Vec<_>>()
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

impl From<String> for ProjectedField {
    fn from(value: String) -> Self {
        Self {
            field: Arc::new(Field::from(value.clone())),
            display_name: value,
        }
    }
}

impl From<Arc<Field>> for ProjectedField {
    fn from(value: Arc<Field>) -> Self {
        Self {
            field: value.clone(),
            display_name: value.name.clone(),
        }
    }
}

impl From<Field> for ProjectedField {
    fn from(value: Field) -> Self {
        Self {
            display_name: value.name.clone(),
            field: Arc::new(value),
        }
    }
}
