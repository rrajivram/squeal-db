use std::fmt::Display;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};

use crate::{db::DBSizeType, page::PageId};

/// Hand-rolled `Serialize`/`Deserialize`, not derived: a derived enum's wire
/// tag is its *declaration index*, which silently rotates every already-
/// persisted `Table` row (this is stored inside `Table::table_type`) if a
/// variant is ever inserted anywhere but the end. Explicit tag bytes below
/// fix the two existing variants at their historical index values forever;
/// a future variant must pick an unused tag (10+ is free) rather than 0/1.
#[derive(Debug, Clone)]
pub enum TableType {
    BtreeTable,
    Index,
}

impl TableType {
    const TAG_BTREE_TABLE: u8 = 0;
    const TAG_INDEX: u8 = 1;
}

impl Serialize for TableType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let tag = match self {
            TableType::BtreeTable => Self::TAG_BTREE_TABLE,
            TableType::Index => Self::TAG_INDEX,
        };
        serializer.serialize_u8(tag)
    }
}

impl<'de> Deserialize<'de> for TableType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let tag = u8::deserialize(deserializer)?;
        match tag {
            Self::TAG_BTREE_TABLE => Ok(TableType::BtreeTable),
            Self::TAG_INDEX => Ok(TableType::Index),
            other => Err(DeError::custom(format!("unknown TableType tag {other}"))),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::TableType;

    #[test]
    fn test_round_trip() {
        for v in [TableType::BtreeTable, TableType::Index] {
            let bytes = postcard::to_allocvec(&v).unwrap();
            let back: TableType = postcard::from_bytes(&bytes).unwrap();
            assert!(matches!((v, back), (TableType::BtreeTable, TableType::BtreeTable) | (TableType::Index, TableType::Index)));
        }
    }

    #[test]
    fn test_unknown_tag_errors() {
        assert!(postcard::from_bytes::<TableType>(&[99]).is_err());
    }

    // Fixture captured from the pre-Stage-1 `#[derive(Serialize,
    // Deserialize)]` encoding (commit bfbc240), before TableType grew a
    // hand-rolled codec. Proves the new decoder still reads bytes written
    // by every release up to this one.
    #[test]
    fn test_decodes_pre_stage1_derived_fixture() {
        const BTREE_BYTES: &[u8] = &[0];
        const INDEX_BYTES: &[u8] = &[1];
        assert!(matches!(
            postcard::from_bytes::<TableType>(BTREE_BYTES).unwrap(),
            TableType::BtreeTable
        ));
        assert!(matches!(
            postcard::from_bytes::<TableType>(INDEX_BYTES).unwrap(),
            TableType::Index
        ));
    }
}
