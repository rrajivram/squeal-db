use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, TableType},
    error::StoreError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableIdType(DBSizeType);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub(crate) id: TableIdType,
    pub(crate) name: String,
    pub(crate) table_type: TableType,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) first_page: DBSizeType,
}

impl Table {
    pub fn new_with_id(
        id: DBSizeType,
        name: String,
        table_type: TableType,
        first_page: DBSizeType,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            id: TableIdType(id),
            name,
            table_type,
            first_page,
        })
    }
}
