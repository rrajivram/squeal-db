use std::fmt::Display;
use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, db_hash},
    error::StoreError,
    logger::UndoId,
    txn::TransactionId,
};

const NONE: u8 = 0;
const INDEXED: u8 = 1;
const TOMBSTONED: u8 = 2;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash, Eq)]
pub enum DBIdType {
    Int(u64),
    Vec(Vec<u8>),
}

// Ordered by `hashed()` rather than structurally, so that comparisons here
// agree with `AnyTuplePage`'s hash-bucketed iteration/storage order — the
// B+ tree's navigation and split logic rely on `<`/`>` matching iteration
// order, and for `Vec` ids the derived (lexical) order would not.
//
// RISK: this intentionally diverges from `Eq`/`PartialEq`, which stay exact.
// Two distinct ids whose hashes collide will compare as `Ordering::Equal`
// here while still being `!=` under `PartialEq`/`Hash`. That's fine for the
// current use (the B+ tree only ever compares hashed keys), but it means
// `DBIdType` must NOT be used directly as a key in a `BTreeMap`/`BTreeSet`
// — colliding-but-distinct ids would silently collapse into one slot.
// Key by `.hashed()` (a plain `u64`) for that purpose instead, as
// `AnyTuplePage` already does.
impl PartialOrd for DBIdType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DBIdType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hashed().cmp(&other.hashed())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default, Hash)]
pub struct Tuple {
    pub(crate) id: DBIdType,
    pub(crate) txn_id: Option<TransactionId>,
    pub(crate) undo_id: Option<UndoId>,
    // Reference-counted so cloning a Tuple (page scans, undo records, find/get)
    // is an O(1) refcount bump instead of copying the whole payload. Serializes
    // identically to `Vec<u8>` in postcard (a seq of u8), so on-disk format is
    // unchanged. Mutation replaces the whole Arc (see `set_data`).
    pub(crate) data: Arc<[u8]>,
    flags: u8,
    // Cached serialized size — not persisted. Zero means not yet computed (e.g. after serde
    // deserialization, which skips this field); size() computes it on demand in that case.
    #[serde(skip)]
    serialized_size: DBSizeType,
}

impl Tuple {
    pub fn new(id: DBSizeType, data: &[u8]) -> Self {
        Self::new_with(DBIdType::Int(id), data, None, None)
    }

    pub fn new_indexed(id: DBIdType, data: &[u8], txn_id: Option<TransactionId>) -> Self {
        let mut s = Self::new_with(id, data, txn_id, None);
        s.flags |= INDEXED;
        s
    }

    pub fn new_with(
        id: DBIdType,
        data: &[u8],
        txn_id: Option<TransactionId>,
        undo_id: Option<UndoId>,
    ) -> Self {
        let mut s = Self {
            id,
            data: Arc::from(data),
            txn_id,
            undo_id,
            ..Default::default()
        };
        s.serialized_size = to_allocvec(&s).map(|v| v.len()).unwrap_or(0) as DBSizeType;
        s
    }

    pub fn is_index(&self) -> bool {
        self.flags & INDEXED == INDEXED
    }

    pub fn set_txn_id(&mut self, id: TransactionId) {
        self.txn_id = Some(id);
        // txn_id is serialized, so its length changed — invalidate the cache
        // (0 == "unknown", recompute on next size()). Page capacity accounting
        // (add_tuple/remove_tuple/replace_tuple) depends on size() being exact
        // after every mutation, not the pre-mutation value.
        self.serialized_size = 0;
    }

    pub fn is_same_txn(&self, tx_id: TransactionId) -> bool {
        self.txn_id.as_ref().map(|t| *t == tx_id).unwrap_or(false)
    }

    pub fn set_undo_id(&mut self, id: UndoId) {
        self.undo_id = Some(id);
        self.serialized_size = 0; // see set_txn_id
    }

    pub fn tombstone(&mut self) {
        self.flags |= 1 << TOMBSTONED
    }

    pub fn is_tombstoned(&self) -> bool {
        self.flags & 1 << TOMBSTONED != 0
    }

    pub fn from(bytes: &[u8]) -> Result<Self, StoreError> {
        let t: Tuple = from_bytes(bytes)?;
        Ok(t)
    }

    pub fn set_data(&mut self, data: &[u8]) {
        self.data = Arc::from(data);
        self.serialized_size = 0; // see set_txn_id
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn size(&self) -> DBSizeType {
        if self.serialized_size > 0 {
            self.serialized_size
        } else {
            // Deserialized tuples have serialized_size=0 (serde skips it); compute on demand.
            to_allocvec(self).map(|v| v.len()).unwrap_or(0) as DBSizeType
        }
    }

    pub fn to(&self) -> Vec<u8> {
        to_allocvec(&self).unwrap()
    }
}

impl Default for DBIdType {
    fn default() -> Self {
        Self::Int(0)
    }
}

impl Display for DBIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            DBIdType::Int(i) => write!(f, "{}", i),
            DBIdType::Vec(v) => write!(f, "{:?}", v),
        }
    }
}

impl From<String> for DBIdType {
    fn from(value: String) -> Self {
        Self::Vec(value.as_bytes().to_vec())
    }
}

impl From<DBIdType> for String {
    fn from(value: DBIdType) -> Self {
        match value {
            DBIdType::Int(i) => i.to_string(),
            DBIdType::Vec(v) => String::from_utf8(v).unwrap_or_default(),
        }
    }
}

impl From<u64> for DBIdType {
    fn from(value: u64) -> Self {
        Self::Int(value)
    }
}

impl DBIdType {
    pub(crate) fn hashed(&self) -> u64 {
        match self {
            Self::Int(i) => *i,
            Self::Vec(v) => db_hash(v),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::tuple::{DBIdType, Tuple};

    #[test]
    fn test_tuple() {
        let t = Tuple {
            id: DBIdType::Int(0),
            data: vec![b'h', b'e', b'l', b'l', b'o'].into(),
            txn_id: None,
            undo_id: None,
            ..Default::default()
        };
        let b = t.to();
        let t1 = Tuple::from(&b).unwrap();
        assert_eq!(t1.id, DBIdType::Int(0));
        assert_eq!(t1.data.to_vec(), vec![b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_tuple_vec_id() {
        let id = DBIdType::Vec(b"my_key".to_vec());
        let t = Tuple {
            id: id.clone(),
            data: b"value".to_vec().into(),
            txn_id: None,
            undo_id: None,
            ..Default::default()
        };
        let b = t.to();
        let t1 = Tuple::from(&b).unwrap();
        assert_eq!(t1.id, id);
        assert_eq!(t1.data.to_vec(), b"value");
    }

    #[test]
    fn test_tuple_set_txn_id() {
        use crate::txn::TransactionId;
        let mut t = Tuple::new(5, b"hello");
        assert!(t.txn_id.is_none());
        assert!(t.undo_id.is_none());
        t.set_txn_id(TransactionId::from(99));
        assert_eq!(t.txn_id, Some(TransactionId::from(99)));
        assert!(t.undo_id.is_none());
    }

    #[test]
    fn test_tuple_size_includes_overhead() {
        let data = b"hello world";
        let t = Tuple::new(1, data);
        // size must be at least the data length
        assert!(t.size() >= data.len() as u64);
        // and larger due to struct overhead
        assert!(t.size() > data.len() as u64);
    }

    #[test]
    fn test_dbid_ordering() {
        // Int variant ordering
        assert!(DBIdType::Int(1) < DBIdType::Int(2));
        assert!(DBIdType::Int(5) > DBIdType::Int(3));
        assert_eq!(DBIdType::Int(7), DBIdType::Int(7));
    }

    #[test]
    fn test_tuple_roundtrip_with_txn_id() {
        use crate::txn::TransactionId;
        let mut t = Tuple::new(10, b"payload");
        t.set_txn_id(TransactionId::from(1));
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.id, DBIdType::Int(10));
        assert_eq!(t2.data.to_vec(), b"payload");
        assert_eq!(t2.txn_id, Some(TransactionId::from(1)));
    }
}
