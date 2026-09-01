use std::{fmt::Debug, slice, sync::Arc};

use store::valueitem::{IndexKey, ValueItem};

use crate::{
    datatype::DataType,
    source::{ProjectedField, Source},
    table::Field,
};

#[derive(Clone)]
pub struct ConstValue {
    pub name: String,
    item: IndexKey,
    field: ProjectedField,
}

impl ConstValue {
    pub fn new(name: String, item: ValueItem) -> Self {
        assert!(!matches!(item, ValueItem::Null));
        Self {
            item: IndexKey::new_from(slice::from_ref(&item)).unwrap(),
            name: name.clone(),
            field: ProjectedField::from(Field {
                id: 0,
                name,
                datatype: DataType::of(&item).unwrap(), // ok to unwrap here as this is not expected to be null
                nullable: false,
                default: None,
            }),
        }
    }
}

impl Source for ConstValue {
    fn fields(&self) -> Arc<[ProjectedField]> {
        Arc::from([self.field.clone()])
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, crate::error::SchemaError> {
        Ok(Some(self.item.clone()))
    }
}

impl Debug for ConstValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Const")
            .field("name", &self.name)
            .field("value", &self.item)
            .finish()
    }
}
