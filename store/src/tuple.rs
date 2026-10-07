use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;

use postcard::{from_bytes, to_allocvec};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, SeqAccess, Visitor},
};

use crate::{
    db::DBSizeType,
    error::StoreError,
    logger::LsnId,
    txn::TransactionId,
    valueitem::{IndexKey, ValueItem, ValueRef},
    wire::{WireId, WireTuple, WireValues},
};

const NONE: u8 = 0;
const INDEXED: u8 = 1;
const TOMBSTONED: u8 = 2;

fn is_index(flags: u8) -> bool {
    flags & INDEXED == INDEXED
}

fn is_tombstoned(flags: u8) -> bool {
    flags & 1 << TOMBSTONED != 0
}

// Hand-rolled codec, not derived: see table.rs's TableType for why. DBIdType
// is embedded directly in every persisted Tuple, so its wire tag must never
// depend on Rust declaration order. Int=0/Rec=1 are fixed forever; a future
// variant picks an unused tag rather than reordering these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DBIdType {
    Int(u64),
    Rec(IndexKey),
}

impl Serialize for DBIdType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DBIdType::Int(v) => (0u8, v).serialize(serializer),
            DBIdType::Rec(v) => (1u8, v).serialize(serializer),
        }
    }
}

struct DBIdTypeVisitor;

impl<'de> Visitor<'de> for DBIdTypeVisitor {
    type Value = DBIdType;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a tagged DBIdType")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DBIdType, A::Error> {
        let tag: u8 = seq
            .next_element()?
            .ok_or_else(|| DeError::custom("missing DBIdType tag"))?;
        match tag {
            0 => Ok(DBIdType::Int(
                seq.next_element()?
                    .ok_or_else(|| DeError::custom("DBIdType::Int: missing value"))?,
            )),
            1 => Ok(DBIdType::Rec(
                seq.next_element()?
                    .ok_or_else(|| DeError::custom("DBIdType::Rec: missing value"))?,
            )),
            other => Err(DeError::custom(format!("unknown DBIdType tag {other}"))),
        }
    }
}

impl<'de> Deserialize<'de> for DBIdType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_tuple(2, DBIdTypeVisitor)
    }
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
    // Back-pointer to the WAL record that carries this tuple's pre-image —
    // None means "no ancestor" (only true of a fresh INSERT). Was a
    // per-transaction positional UndoId (STORE_AUDIT.md T10: minted as
    // `id.len() as u16`, wrapping at 65,536 ops); is now the record's own
    // globally-unique LsnId, which can never wrap or alias a different
    // transaction's/row's entry (T4_S2_WAL_DESIGN.md §7).
    pub(crate) pre_lsn: Option<LsnId>,
    // Reference-counted so cloning a Tuple (page scans, undo records, find/get)
    // is an O(1) refcount bump instead of copying the whole payload. Serializes
    // identically to `Vec<u8>` in postcard (a seq of u8), so on-disk format is
    // unchanged. Mutation replaces the whole Arc (see `set_data`).
    #[serde(deserialize_with = "arc_bytes")]
    pub(crate) data: Arc<[u8]>,
    pub(crate) flags: u8,
    // Cached serialized size — not persisted. Zero means not yet computed (e.g. after serde
    // deserialization, which skips this field); size() computes it on demand in that case.
    #[serde(skip)]
    serialized_size: DBSizeType,
}

// Tuple::data, decoded as bytes: postcard lays a seq of u8 out exactly as
// it does bytes (a length, then the bytes), and reading it as bytes is one
// copy into the Arc. Read as a seq — serde's own way for Arc<[u8]> — it was
// visited a byte at a time into a Box, then copied again into the Arc:
// about half of decoding a page.
fn arc_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<[u8]>, D::Error> {
    struct ArcBytes;

    impl<'de> Visitor<'de> for ArcBytes {
        type Value = Arc<[u8]>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a tuple's bytes")
        }

        fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<Arc<[u8]>, E> {
            Ok(Arc::from(v))
        }

        // A format that writes bytes as a seq and reads them back that way.
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Arc<[u8]>, A::Error> {
            let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(b) = seq.next_element()? {
                v.push(b);
            }
            Ok(Arc::from(v))
        }
    }

    deserializer.deserialize_bytes(ArcBytes)
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
        pre_lsn: Option<LsnId>,
    ) -> Self {
        let mut s = Self {
            id,
            data: Arc::from(data),
            txn_id,
            pre_lsn,
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
        is_index(self.flags)
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

    pub fn set_pre_lsn(&mut self, lsn: LsnId) {
        self.pre_lsn = Some(lsn);
        self.serialized_size = 0; // see set_txn_id
    }

    /// A fresh insert has no ancestor.
    pub fn set_pre_lsn_none(&mut self) {
        self.pre_lsn = None;
        self.serialized_size = 0;
    }

    pub fn tombstone(&mut self) {
        self.flags |= 1 << TOMBSTONED
    }

    pub fn is_tombstoned(&self) -> bool {
        is_tombstoned(self.flags)
    }

    /// The opposite of `tombstone()`: a fresh insert over a visible tombstone
    /// reuses the row slot and must come back to life.
    pub fn clear_tombstone(&mut self) {
        self.flags &= !(1 << TOMBSTONED)
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

    pub fn id(&self) -> &DBIdType {
        &self.id
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

// A tuple a reader is handed without it being copied out: what a page or
// cursor lends (see PageTuple::at_ref, TableCursor::next_ref). Opaque on
// purpose: behind it is a borrowed Tuple (AnyTuplePage), an owned one (a
// version walked back to, or a page that decodes on demand), or a tuple's
// encoded bytes read in place (SlottedPage, see wire::WireTuple).
#[derive(Debug, Clone)]
pub struct TupleRef<'a>(Lent<'a>);

#[derive(Debug, Clone)]
enum Lent<'a> {
    Tuple(Cow<'a, Tuple>),
    Wire(WireTuple<'a>),
}

impl<'a> TupleRef<'a> {
    pub(crate) fn borrowed(tuple: &'a Tuple) -> Self {
        Self(Lent::Tuple(Cow::Borrowed(tuple)))
    }

    pub(crate) fn owned(tuple: Tuple) -> Self {
        Self(Lent::Tuple(Cow::Owned(tuple)))
    }

    pub(crate) fn wire(tuple: WireTuple<'a>) -> Self {
        Self(Lent::Wire(tuple))
    }

    pub fn id(&self) -> IdRef<'_> {
        match &self.0 {
            Lent::Tuple(t) => IdRef::of(&t.id),
            Lent::Wire(w) => IdRef(Id::Wire(w.id)),
        }
    }

    pub fn data(&self) -> &[u8] {
        match &self.0 {
            Lent::Tuple(t) => &t.data,
            Lent::Wire(w) => w.data,
        }
    }

    fn flags(&self) -> u8 {
        match &self.0 {
            Lent::Tuple(t) => t.flags,
            Lent::Wire(w) => w.flags,
        }
    }

    pub fn is_index(&self) -> bool {
        is_index(self.flags())
    }

    pub fn is_tombstoned(&self) -> bool {
        is_tombstoned(self.flags())
    }

    pub(crate) fn txn_id(&self) -> Option<TransactionId> {
        match &self.0 {
            Lent::Tuple(t) => t.txn_id,
            Lent::Wire(w) => w.txn_id,
        }
    }

    pub(crate) fn pre_lsn(&self) -> Option<LsnId> {
        match &self.0 {
            Lent::Tuple(t) => t.pre_lsn,
            Lent::Wire(w) => w.pre_lsn,
        }
    }

    pub fn to_owned(&self) -> Tuple {
        self.clone().into_owned()
    }

    pub fn into_owned(self) -> Tuple {
        match self.0 {
            Lent::Tuple(t) => t.into_owned(),
            Lent::Wire(w) => Tuple {
                id: IdRef(Id::Wire(w.id)).to_owned(),
                txn_id: w.txn_id,
                pre_lsn: w.pre_lsn,
                data: Arc::from(w.data),
                flags: w.flags,
                serialized_size: 0,
            },
        }
    }
}

// A tuple's id, lent with it (see TupleRef): an owned id borrowed, or one
// read in place from a tuple's bytes. Compares, hashes and equals exactly
// as DBIdType does.
#[derive(Debug, Clone, Copy)]
pub struct IdRef<'a>(Id<'a>);

#[derive(Debug, Clone, Copy)]
enum Id<'a> {
    Owned(&'a DBIdType),
    Wire(WireId<'a>),
}

/// See IdRef::key_values.
pub struct KeyValues<'a>(Values<'a>);

enum Values<'a> {
    Owned(std::slice::Iter<'a, ValueItem>),
    Wire(WireValues<'a>),
}

impl<'a> Iterator for KeyValues<'a> {
    type Item = ValueRef<'a>;

    fn next(&mut self) -> Option<ValueRef<'a>> {
        match &mut self.0 {
            Values::Owned(i) => i.next().map(ValueItem::as_ref),
            Values::Wire(i) => i.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            Values::Owned(i) => i.size_hint(),
            Values::Wire(i) => i.size_hint(),
        }
    }
}

impl ExactSizeIterator for KeyValues<'_> {}

impl<'a> IdRef<'a> {
    pub(crate) fn of(id: &'a DBIdType) -> Self {
        IdRef(Id::Owned(id))
    }

    pub(crate) fn wire(id: WireId<'a>) -> Self {
        IdRef(Id::Wire(id))
    }

    /// A row-id key's number; None for a column key.
    pub fn as_int(&self) -> Option<u64> {
        match self.0 {
            Id::Owned(DBIdType::Int(i)) => Some(*i),
            Id::Wire(WireId::Int(i)) => Some(i),
            _ => None,
        }
    }

    /// A column key's fields, in order; None for a row-id key.
    pub fn key_values(&self) -> Option<KeyValues<'a>> {
        match self.0 {
            Id::Owned(DBIdType::Rec(k)) => Some(KeyValues(Values::Owned(k.values().iter()))),
            Id::Wire(WireId::Rec(k)) => Some(KeyValues(Values::Wire(k.values()))),
            _ => None,
        }
    }

    pub fn to_owned(&self) -> DBIdType {
        match self.0 {
            Id::Owned(id) => id.clone(),
            Id::Wire(WireId::Int(i)) => DBIdType::Int(i),
            Id::Wire(WireId::Rec(k)) => {
                DBIdType::Rec(IndexKey::from_stored(k.values().map(|v| v.to_owned()).collect()))
            }
        }
    }

    // `f` of this id as a DBIdType, for what looks ids up by one.
    pub(crate) fn with_owned<R>(&self, f: impl FnOnce(&DBIdType) -> R) -> R {
        match self.0 {
            Id::Owned(id) => f(id),
            Id::Wire(_) => f(&self.to_owned()),
        }
    }

    // See DBIdType::hashed.
    pub(crate) fn hashed(&self) -> u64 {
        match self.key_values() {
            Some(values) => IndexKey::hash_refs(values),
            None => self.as_int().unwrap_or_default(),
        }
    }

    // DBIdType's order (see its Ord): field by field between two column
    // keys — a key that runs out first ties — and by hashed() otherwise.
    pub(crate) fn cmp(&self, other: &IdRef) -> std::cmp::Ordering {
        if let (Id::Owned(a), Id::Owned(b)) = (self.0, other.0) {
            return a.cmp(b);
        }
        match (self.key_values(), other.key_values()) {
            (Some(a), Some(b)) => a
                .zip(b)
                .map(|(a, b)| a.cmp(&b))
                .find(|o| o.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => self.hashed().cmp(&other.hashed()),
        }
    }

    // DBIdType's order against an owned id.
    pub(crate) fn cmp_owned(&self, other: &DBIdType) -> std::cmp::Ordering {
        self.cmp(&IdRef::of(other))
    }

    // DBIdType's `==` (exact: the same kind, and for column keys the same
    // number of fields, each equal) against an owned id.
    pub(crate) fn eq_owned(&self, other: &DBIdType) -> bool {
        self.eq(&IdRef::of(other))
    }

    // DBIdType's `==` between two lent ids. Two keys read from bytes that
    // are the same bytes are equal without a value being read — an index
    // entry and the row it points to, as a range scan checks them.
    pub(crate) fn eq(&self, other: &IdRef) -> bool {
        match (self.0, other.0) {
            (Id::Owned(a), Id::Owned(b)) => return a == b,
            (Id::Wire(WireId::Rec(a)), Id::Wire(WireId::Rec(b))) if a.same_bytes(&b) => {
                return true;
            }
            _ => {}
        }
        match (self.key_values(), other.key_values()) {
            (Some(a), Some(b)) => a.len() == b.len() && a.zip(b).all(|(a, b)| a == b),
            (None, None) => self.as_int() == other.as_int(),
            _ => false,
        }
    }

    // Where this id lies against `range` (see KeyRange::position); None
    // for a row-id key, which no key range applies to.
    pub(crate) fn range_position(
        &self,
        range: &crate::cursor::KeyRange,
    ) -> Option<std::cmp::Ordering> {
        Some(range.position(self.key_values()?))
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

    use postcard::{from_bytes, to_allocvec};

    use crate::{
        tuple::{DBIdType, Tuple},
        valueitem::{IndexKey, ValueItem},
    };

    #[test]
    fn test_dbidtype_round_trip() {
        let rec = DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(7)]).unwrap());
        for v in [DBIdType::Int(42), rec] {
            let bytes = to_allocvec(&v).unwrap();
            let back: DBIdType = from_bytes(&bytes).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn test_dbidtype_unknown_tag_errors() {
        assert!(from_bytes::<DBIdType>(&[99, 42]).is_err());
    }

    // Fixture captured from the pre-Stage-1 `#[derive(Serialize,
    // Deserialize)]` encoding (commit bfbc240), before DBIdType grew a
    // hand-rolled codec.
    #[test]
    fn test_dbidtype_decodes_pre_stage1_derived_fixture() {
        const INT_BYTES: &[u8] = &[0, 42];
        const REC_BYTES: &[u8] = &[1, 1, 1, 14];
        assert_eq!(from_bytes::<DBIdType>(INT_BYTES).unwrap(), DBIdType::Int(42));
        assert_eq!(
            from_bytes::<DBIdType>(REC_BYTES).unwrap(),
            DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(7)]).unwrap())
        );
    }

    #[test]
    fn test_tuple() {
        let t = Tuple {
            id: DBIdType::Int(0),
            data: vec![b'h', b'e', b'l', b'l', b'o'].into(),
            txn_id: None,
            pre_lsn: None,
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
            pre_lsn: None,
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
        assert!(t.pre_lsn.is_none());
        // TransactionId::from(u64) mints a fresh timestamp on every call, so
        // two separately-constructed instances for the same numeric id are
        // not equal (identity includes ts, not just id) — reuse the same
        // instance instead of deriving a second one to compare against.
        let txn_id = TransactionId::from(99);
        t.set_txn_id(txn_id);
        assert_eq!(t.txn_id, Some(txn_id));
        assert!(t.pre_lsn.is_none());
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
        t.set_txn_id(txn_id);
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.id, DBIdType::Int(10));
        assert_eq!(t2.data.to_vec(), b"payload");
        assert_eq!(t2.txn_id, Some(txn_id));
    }

    // An id read in place from a tuple's bytes (IdRef over wire::WireId)
    // compares, equals, hashes and copies out exactly as the DBIdType it
    // was written from — over every pair from a set built to hit the
    // edges: NaN and -0.0, a string's capacity (not part of its value),
    // a key that runs out before another, mixed types, and row ids.
    #[test]
    fn test_an_id_read_from_bytes_behaves_as_the_id() {
        use crate::tuple::{IdRef, TupleRef};
        use crate::wire::WireTuple;
        use std::sync::Arc;

        let s = |v: &str, cap: u32| ValueItem::Str((v.to_owned(), cap));
        let rec = |vs: &[ValueItem]| DBIdType::Rec(IndexKey::new_from(vs).unwrap());
        let ids = vec![
            DBIdType::Int(0),
            DBIdType::Int(7),
            DBIdType::Int(u64::MAX),
            rec(&[]),
            rec(&[ValueItem::Null]),
            rec(&[ValueItem::Integer(-1)]),
            rec(&[ValueItem::Integer(7)]),
            rec(&[ValueItem::Double(0.0)]),
            rec(&[ValueItem::Double(-0.0)]),
            rec(&[ValueItem::Double(f64::NAN)]),
            rec(&[ValueItem::Double(f64::NEG_INFINITY)]),
            rec(&[ValueItem::Datetime(5)]),
            rec(&[ValueItem::Boolean(true)]),
            rec(&[s("abc", 3)]),
            rec(&[s("abc", 12)]),
            rec(&[s("abd", 12)]),
            rec(&[s("", 4)]),
            rec(&[ValueItem::Blob((Arc::from(&b"ab"[..]), 2))]),
            rec(&[s("abc", 12), ValueItem::Integer(1)]),
            rec(&[s("abc", 12), ValueItem::Integer(2)]),
            rec(&[s("abc", 12), ValueItem::Null]),
            rec(&[ValueItem::Integer(7), s("x", 1), ValueItem::Double(1.5)]),
        ];
        let bytes: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| postcard::to_allocvec(&Tuple::new_with(id.clone(), b"", None, None)).unwrap())
            .collect();
        let lent: Vec<TupleRef> = bytes
            .iter()
            .map(|b| TupleRef::wire(WireTuple::read(b).unwrap()))
            .collect();
        for (a, la) in ids.iter().zip(&lent) {
            let wa = la.id();
            assert_eq!(&wa.to_owned(), a);
            assert_eq!(wa.hashed(), a.hashed(), "{a:?}");
            for (b, lb) in ids.iter().zip(&lent) {
                let wb = lb.id();
                assert_eq!(wa.cmp_owned(b), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(wa.cmp(&wb), a.cmp(b), "{a:?} vs {b:?}, both read");
                assert_eq!(IdRef::of(a).cmp(&wb), a.cmp(b), "{a:?} vs read {b:?}");
                assert_eq!(wa.eq_owned(b), a == b, "{a:?} == {b:?}");
                assert_eq!(wa.eq(&wb), a == b, "{a:?} == {b:?}, both read");
                assert_eq!(IdRef::of(a).eq(&wb), a == b, "{a:?} == read {b:?}");
            }
        }
    }
}
