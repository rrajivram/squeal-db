use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, page::PageId};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TableType {
    BtreeTable,
    Index,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, Hash, PartialEq)]
pub struct TableIdType(DBSizeType);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub(crate) id: TableIdType,
    pub(crate) name: String,
    pub(crate) table_type: TableType,
    pub(crate) first_index_page: PageId,
    pub(crate) first_data_page: PageId,
    pub(crate) nodes_per_page: usize,
}

impl From<DBSizeType> for TableIdType {
    fn from(value: DBSizeType) -> Self {
        TableIdType(value)
    }
}

impl TableIdType {
    pub(crate) fn to_string(&self) -> String {
        self.0.to_string()
    }
}
