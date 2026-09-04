use std::{fmt::Debug, sync::Arc};

use store::valueitem::IndexKey;

use crate::{error::SchemaError, plan::eval::EvalExpr, table::Field};

pub mod aggs;
pub(crate) mod join;
pub mod limit;
pub mod proj;
pub(crate) mod run;
pub mod table;
pub mod where_source;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ProjectedField {
    pub(crate) field: Arc<Field>,
    pub(crate) display_name: String,
    pub(crate) source_id: usize,
    pub(crate) field_id: usize,
    pub(crate) expr: EvalExpr,
}

pub trait Source: Debug {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError>;
    fn fields(&self) -> Arc<[ProjectedField]>;
    fn reset(&mut self) -> Result<(), SchemaError>;
}
