use std::sync::Arc;

use store::valueitem::ValueItem;

use crate::{
    error::SchemaError,
    plan::eval::EvalExpr,
    source::{ProjectedField, Source},
    table::Field,
};

#[derive(Debug)]
pub(crate) struct WhereSource {
    source: Box<dyn Source>,
    expr: EvalExpr,
    field: Arc<Field>,
}

impl WhereSource {
    pub(crate) fn new(source: Box<dyn Source>, expr: EvalExpr) -> Result<Self, SchemaError> {
        Ok(Self {
            source,
            expr,
            field: Arc::new(Field::new(
                "where".into(),
                crate::datatype::DataType::Boolean,
                false,
                None,
            )?),
        })
    }
}

impl Source for WhereSource {
    fn fields(&self) -> std::sync::Arc<[ProjectedField]> {
        Arc::from([ProjectedField::from_field(self.field.clone(), 0, 0)])
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        while let Some(res) = self.source.next()? {
            let mut slice = vec![res];
            let should_output = self.expr.eval(&slice, 0)?;
            match should_output {
                ValueItem::Boolean(b) => {
                    if b {
                        return Ok(Some(slice.remove(0)));
                    } else {
                        continue;
                    }
                }
                _ => {
                    return Err(SchemaError::InternalSchemaError(
                        "Output of where is not boolean.".into(),
                    ));
                }
            }
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        Ok(())
    }
}
