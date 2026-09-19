use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, page::PageId};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TableType {
    BtreeTable,
    Index,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct TableIdType(DBSizeType);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub id: TableIdType,
    pub name: String,
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

impl Display for TableIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TableIdType {
    pub fn none() -> Self {
        Self(0)
    }

    // For a caller (e.g. squeal-sql's SchemaStats) that needs to key a
    // store-level row by table id — DBIdType::Int only takes a raw u64,
    // and DBSizeType itself is pub(crate), so this is the narrowest way
    // to hand the underlying value out.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}
