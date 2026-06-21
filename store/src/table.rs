use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, error::StoreError, page::PageId};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TableType {
    Table,
    Index,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableIdType(DBSizeType);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub(crate) id: TableIdType,
    pub(crate) name: String,
    pub(crate) table_type: TableType,
    pub(crate) first_page: Option<PageId>,
}

impl Table {
    pub fn new_with_id(
        id: DBSizeType,
        name: String,
        table_type: TableType,
        first_page: Option<PageId>,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            id: TableIdType(id),
            name,
            table_type,
            first_page,
        })
    }
}

impl From<DBSizeType> for TableIdType {
    fn from(value: DBSizeType) -> Self {
        TableIdType(value)
    }
}
