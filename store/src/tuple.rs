use std::fmt::Display;
use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    db::DBSizeType,
    error::StoreError,
    logger::UndoId,
    txn::TransactionId,
    valueitem::{IndexKey, ValueItem},
};

const NONE: u8 = 0;
const INDEXED: u8 = 1;
const TOMBSTONED: u8 = 2;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum DBIdType {
    Int(u64),
    Rec(IndexKey),
}

// Int orders by `hashed()` rather than structurally, so that comparisons
// here agree with `AnyTuplePage`'s (now id-keyed, see its own comment)
// storage/iteration order — the B+ tree's navigation and split logic rely
// on `<`/`>` matching iteration order.
//
// Rec is the one exception: it orders structurally (field-by-field via
// IndexKey::partial_cmp) instead. A hashed comparison — by design — loses
// any relationship to the actual field values, which defeats the entire
// point of a multi-key index: a range query over (customer_id, order_date)
// needs "close values" to sort "close together" in the tree, and no hash
// function can preserve that (it has to scramble to avoid collisions,
// which is the opposite of what ordering needs). Confirmed as a real,
// reachable problem, not just theoretical, in cursor.rs's range_scan
// tests: a range meant to select a contiguous span of small integer keys
// returned almost none of them once the ids were wrapped in a
// single-field IndexKey and ordered by hash.
//
// RISK: for Int, this still intentionally diverges from `Eq`/`PartialEq`,
// which stay exact — two distinct ids whose hashes collide compare as
// `Ordering::Equal` here while still being `!=` under `PartialEq`/`Hash`.
// For Rec, the analogous (much narrower) risk is IndexKey::partial_cmp's
// own documented quirks: a Str/Blob field whose reserved capacity differs
// compares Equal despite being `!=` under PartialEq, and a key that is a
// strict field-wise prefix of a longer key compares Equal to it despite
// having a different field count. Either way, `Ordering::Equal` here does
// NOT imply true equality — `AnyTuplePage` already has to tolerate that
// (see its own doc comment on why it buckets by full `DBIdType`, not just
// `.hashed()`).
impl PartialOrd for DBIdType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DBIdType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Rec(a), Self::Rec(b)) => {
                // IndexKey::partial_cmp only ever returns None if a field's
                // own PartialOrd does (see ValueItem — it panics instead of
                // returning None for any comparison it can't make sense of,
                // e.g. Blob), so this never actually falls back in practice;
                // it's here so Ord's contract (a total order) still holds
                // if that ever changes.
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            _ => self.hashed().cmp(&other.hashed()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
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
        // serialized_size() over to_allocvec().len(): every Tuple construction
        // needs only the byte count, not the bytes themselves — to_allocvec
        // allocated and immediately discarded a full copy of the encoded
        // tuple just to measure it. postcard's Size flavor computes the same
        // exact count by walking the same Serialize impl without writing
        // anything, so this is zero-allocation on what's likely the hottest
        // single call site in the whole write path (every insert/update/
        // remove constructs at least one Tuple).
        s.serialized_size = postcard::experimental::serialized_size(&s).unwrap_or(0) as DBSizeType;
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
            // Deserialized tuples have serialized_size=0 (serde skips it); compute
            // on demand. See new_with's comment on why serialized_size (not
            // to_allocvec) is used here too.
            postcard::experimental::serialized_size(self).unwrap_or(0) as DBSizeType
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
            DBIdType::Rec(r) => write!(f, "{:?}", r),
        }
    }
}

// A plain string id is now a single-field Rec(IndexKey) instead of its own
// variant — Rec already covers this shape (see the enum's own comment), so
// there's no need for a second, narrower byte-vector id kind. Capacity is
// set to exactly the content's length: `ValueItem::validate` requires
// content.len() <= capacity, and a from-String conversion has no other
// capacity to declare, so this can never fail validation.
impl From<String> for DBIdType {
    fn from(value: String) -> Self {
        let len = value.len() as u32;
        Self::Rec(
            IndexKey::new_from(&[ValueItem::Str((value, len))])
                .expect("capacity == content length can never fail ValueItem::validate"),
        )
    }
}

impl From<DBIdType> for String {
    fn from(value: DBIdType) -> Self {
        match value {
            DBIdType::Int(i) => i.to_string(),
            DBIdType::Rec(r) => r.as_single_str().unwrap_or_else(|| format!("{:?}", r)),
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
            Self::Rec(r) => r.hash(),
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
    fn test_tuple_string_id() {
        let id = DBIdType::from("my_key".to_string());
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
        // TransactionId::from(u64) mints a fresh timestamp on every call, so
        // two separately-constructed instances for the same numeric id are
        // not equal (identity includes ts, not just id) — reuse the same
        // instance instead of deriving a second one to compare against.
        let txn_id = TransactionId::from(99);
        t.set_txn_id(txn_id.clone());
        assert_eq!(t.txn_id, Some(txn_id));
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
        // See test_tuple_set_txn_id: reuse the same instance rather than
        // minting a second one, since identity now includes ts.
        let txn_id = TransactionId::from(1);
        t.set_txn_id(txn_id.clone());
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.id, DBIdType::Int(10));
        assert_eq!(t2.data.to_vec(), b"payload");
        assert_eq!(t2.txn_id, Some(txn_id));
    }
}
