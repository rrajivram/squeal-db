use std::{cmp::Ordering, fmt::Display, hash::Hash, ops::Index, sync::Arc};

use log::error;
use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, db_hash},
    error::StoreError,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum ValueItem {
    #[default]
    Null = 0,
    Integer(i64) = 5,
    Double(f64) = 10,
    Datetime(u64) = 15,
    Str((String, u32)) = 20,
    Blob((Arc<[u8]>, u32)) = 25,
    Boolean(bool) = 30,
}

// Hand-written, not derived, for exactly one reason: Double. `impl Hash
// for ValueItem` (below) already hashes a Double by its bit pattern
// (`f.to_bits()`), not by IEEE-754 `==` — derived PartialEq would use
// `==` instead, which disagrees with that on -0.0 vs 0.0 (`-0.0 == 0.0`
// is true, but their bit patterns differ), a real violation of Hash's
// own contract (`a == b` must imply `hash(a) == hash(b)`). Bit-pattern
// equality fixes that, and is also exactly what makes `Ord`'s own
// `total_cmp`-based ordering agree with `==` (two f64s compare `Equal`
// under `total_cmp` iff their bits match) — same reasoning `ordered-
// float`-style crates use. The one deliberate, known consequence: NaN
// == NaN becomes true and -0.0 == 0.0 becomes false, the opposite of
// plain IEEE-754 `==` (see test_double_equality_is_bit_pattern_based).
// Every other variant just does what derive would already do.
impl PartialEq for ValueItem {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueItem::Null, ValueItem::Null) => true,
            (ValueItem::Integer(a), ValueItem::Integer(b)) => a == b,
            (ValueItem::Double(a), ValueItem::Double(b)) => a.to_bits() == b.to_bits(),
            (ValueItem::Datetime(a), ValueItem::Datetime(b)) => a == b,
            (ValueItem::Str(a), ValueItem::Str(b)) => a == b,
            (ValueItem::Blob(a), ValueItem::Blob(b)) => a == b,
            (ValueItem::Boolean(a), ValueItem::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize, Hash)]
pub struct IndexKey {
    data: Arc<[ValueItem]>,
}

impl IndexKey {
    pub fn new_from(data: &[ValueItem]) -> Result<Self, StoreError> {
        for d in data {
            d.validate()?;
        }
        Ok(Self {
            data: Arc::from(data),
        })
    }

    // Like new_from, but takes ownership of `data` instead of borrowing
    // it. `Arc::from(&[ValueItem])` (new_from's own `Arc::from`) has to
    // clone every element to fill a new allocation it doesn't own yet —
    // for a ValueItem::Str, that's a fresh heap copy of the whole
    // string. `Arc::from(Vec<ValueItem>)` instead moves each element in
    // (a plain struct-field copy — for a String, just its ptr/len/cap,
    // not its character data) since it already owns them; the character
    // data itself is never touched, let alone reallocated. Worth the
    // separate entry point specifically for callers building a fresh,
    // otherwise-about-to-be-dropped Vec<ValueItem> just to hand it to an
    // IndexKey (e.g. Schema::insert_rows_in_txn's own row storage) —
    // confirmed via allocation profiling as the single largest source
    // of small (<10 byte) allocations in a bulk load.
    pub fn new_from_owned(data: Vec<ValueItem>) -> Result<Self, StoreError> {
        for d in &data {
            d.validate()?;
        }
        Ok(Self {
            data: Arc::from(data),
        })
    }

    pub fn size(&self) -> usize {
        size_of::<u64>() + self.data.iter().map(|d| d.size()).sum::<usize>()
    }

    pub fn values(&self) -> &[ValueItem] {
        &self.data
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(&self.data.len().to_le_bytes());
        for d in self.data.iter() {
            bytes.extend_from_slice(&d.to_bytes());
        }
        bytes
    }

    // STORE_AUDIT.md S3: bounds-checked instead of a raw slice index /
    // `try_into().unwrap()` on the leading count prefix — this hand-rolled
    // format decodes bytes read straight off disk, so truncated/malformed
    // input must surface as an `Err`, not a panic.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        let count_bytes = bytes.get(0..size_of::<u64>()).ok_or_else(|| {
            StoreError::TruncatedValueItem(format!(
                "need {} byte(s) for the field-count prefix, buffer is {} byte(s)",
                size_of::<u64>(),
                bytes.len()
            ))
        })?;
        let count = u64::from_le_bytes(count_bytes.try_into().unwrap()) as usize;
        let mut index = size_of::<u64>();
        let mut data = vec![];
        for _ in 0..count {
            // from_bytes_many's returned index is relative to the sub-slice
            // it was handed (bytes[index..]), not absolute — it must be
            // added to the running offset, not replace it, or every field
            // after the first is read starting mid-way through the
            // previous one instead of where it actually begins. `index` is
            // fetched via `get` (not a raw `bytes[index..]` slice), since a
            // prior field's declared-but-truncated length can otherwise
            // push it clean past `bytes.len()`, which would itself panic
            // (slice start index out of range) before from_bytes_many ever
            // gets a chance to report it as an ordinary Err.
            let rest = bytes.get(index..).ok_or_else(|| {
                StoreError::TruncatedValueItem(format!(
                    "offset {index} past end of buffer ({} byte(s))",
                    bytes.len()
                ))
            })?;
            let (v, i) = ValueItem::from_bytes_many(rest)?;
            index = index.saturating_add(i);
            data.push(v);
        }
        Ok(Self::new_from(&data).unwrap_or(Self::new_from(&[ValueItem::Null]).unwrap()))
    }

    /// Combines each field's own hash with an FNV-1a-style XOR+multiply
    /// mixer — field order and each field's own value both affect the
    /// result (see `test_hash_mixes_field_order_not_just_bitwise_union`).
    /// Takes any `&ValueItem` iterator rather than requiring a real
    /// IndexKey, so a caller that only wants a hash over a subset of a
    /// row's fields (e.g. a hash index's key columns) can hash straight
    /// off references into that row — building an IndexKey first would
    /// mean validating every field and allocating an `Arc<[ValueItem]>`
    /// just to hash it once and immediately discard it.
    pub fn hash_fields<'a>(fields: impl IntoIterator<Item = &'a ValueItem>) -> u64 {
        let mut h = 0x811C9DC5;
        for d in fields {
            h ^= d.hash();
            h = (h * 0x01000193) & 0xFFFFFFFF;
        }
        h
    }

    pub fn hash(&self) -> u64 {
        Self::hash_fields(self.data.iter())
    }

    // Extracts the content back out of a single-field, Str-only key — the
    // shape `DBIdType::From<String>` builds. `None` for anything else (a
    // composite key, or a lone field of a different type), so callers can
    // fall back to a generic representation instead of misreporting one.
    pub(crate) fn as_single_str(&self) -> Option<String> {
        match self.data.as_ref() {
            [ValueItem::Str((s, _))] => Some(s.clone()),
            _ => None,
        }
    }
}

impl Index<usize> for IndexKey {
    type Output = ValueItem;
    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        for (l, r) in self.data.iter().zip(other.data.iter()) {
            let cmp = l.partial_cmp(r);
            if let Some(Ordering::Equal) = cmp {
                continue;
            }
            return cmp;
        }
        Some(Ordering::Equal)
    }
}

impl Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for d in self.data.iter() {
            let _ = writeln!(f, "{:?}", d);
        }
        Ok(())
    }
}

impl From<&[ValueItem]> for IndexKey {
    fn from(value: &[ValueItem]) -> Self {
        Self::new_from(value).unwrap_or(Self::new_from(&[ValueItem::Null]).unwrap())
    }
}

impl Eq for IndexKey {}

impl ValueItem {
    pub(super) fn validate(&self) -> Result<(), StoreError> {
        match self {
            ValueItem::Integer(_) => Ok(()),
            ValueItem::Double(_) => Ok(()),
            ValueItem::Datetime(_) => Ok(()),
            // TupleTooLarge(actual, max) — every other call site in this
            // crate (page.rs, fixedtuple.rs, bplustree.rs) passes this
            // same (actual, max) order, matching the variant's own
            // Display format; this one had it backwards (reserved cap
            // first, actual length second), so the printed message
            // reported the two numbers swapped.
            ValueItem::Str(s) => {
                if s.0.len() as u32 > s.1 {
                    Err(StoreError::TupleTooLarge(
                        s.0.len() as DBSizeType,
                        s.1 as usize,
                    ))
                } else {
                    Ok(())
                }
            }
            ValueItem::Blob(s) => {
                if s.0.len() as u32 > s.1 {
                    Err(StoreError::TupleTooLarge(
                        s.0.len() as DBSizeType,
                        s.1 as usize,
                    ))
                } else {
                    Ok(())
                }
            }
            ValueItem::Boolean(_) => Ok(()),
            ValueItem::Null => Ok(()),
        }
    }

    pub fn size(&self) -> usize {
        let sz = match self {
            ValueItem::Integer(_) => size_of::<i64>(),
            ValueItem::Double(_) => size_of::<f64>(),
            ValueItem::Datetime(_) => size_of::<u64>(),
            ValueItem::Str(s) => s.1 as usize + size_of::<u32>() * 2,
            ValueItem::Blob(b) => b.1 as usize + size_of::<u32>() * 2,
            ValueItem::Boolean(_) => size_of::<u8>(),
            ValueItem::Null => 0,
        };
        sz + 1
    }

    pub(super) fn size_of_empty(&self) -> usize {
        let sz = match self {
            ValueItem::Integer(_) => size_of::<i64>(),
            ValueItem::Double(_) => size_of::<f64>(),
            ValueItem::Datetime(_) => size_of::<u64>(),
            ValueItem::Str(_) => size_of::<u32>() * 2,
            ValueItem::Blob(_) => size_of::<u32>() * 2,
            ValueItem::Boolean(_) => size_of::<u8>(),
            ValueItem::Null => 0,
        };
        sz + 1
    }

    pub(super) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let value = match self {
            ValueItem::Null => 0u8,
            ValueItem::Integer(_) => 5,
            ValueItem::Double(_) => 10,
            ValueItem::Datetime(_) => 15,
            ValueItem::Str(_) => 20,
            ValueItem::Blob(_) => 25,
            ValueItem::Boolean(_) => 30,
        };
        bytes.push(value);
        match self {
            ValueItem::Integer(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            ValueItem::Double(f) => bytes.extend_from_slice(&f.to_le_bytes()),
            ValueItem::Datetime(d) => bytes.extend_from_slice(&d.to_le_bytes()),
            ValueItem::Str(s) => {
                bytes.extend_from_slice(&s.1.to_le_bytes());
                bytes.extend_from_slice(&(s.0.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s.0.as_bytes());
                if s.0.len() < s.1 as usize {
                    bytes.extend_from_slice(&vec![0u8; s.1 as usize - s.0.len()]);
                }
            }
            ValueItem::Blob(b) => {
                bytes.extend_from_slice(&b.1.to_le_bytes());
                bytes.extend_from_slice(&(b.0.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&b.0);
                if b.0.len() < b.1 as usize {
                    bytes.extend_from_slice(&vec![0u8; b.1 as usize - b.0.len()]);
                }
            }
            ValueItem::Boolean(b) => bytes.push(*b as u8),
            ValueItem::Null => {}
        }
        bytes
    }

    pub(super) fn hash(&self) -> u64 {
        match self {
            ValueItem::Null => 0,
            ValueItem::Integer(i) => *i as u64,
            ValueItem::Double(f) => f.to_bits(),
            ValueItem::Datetime(d) => *d,
            ValueItem::Str((s, _l)) => db_hash(s.as_bytes()),
            ValueItem::Blob((b, _)) => db_hash(b),
            ValueItem::Boolean(b) => *b as u64,
        }
    }

    pub(super) fn from_bytes_single(bytes: &[u8]) -> Result<ValueItem, StoreError> {
        Self::from_bytes_many(bytes).map(|(v, _)| v)
    }

    // STORE_AUDIT.md S3: every extraction below is bounds-checked (`take`)
    // instead of a raw slice index / `try_into().unwrap()` — this hand-
    // rolled format decodes bytes read straight off disk, so a truncated
    // or corrupted buffer must surface as an `Err`, not a panic (which,
    // per S8, would poison the page lock this runs under on the hot read
    // path).
    pub(super) fn from_bytes_many(bytes: &[u8]) -> Result<(ValueItem, usize), StoreError> {
        fn take<'a>(bytes: &'a [u8], index: usize, len: usize) -> Result<&'a [u8], StoreError> {
            let end = index.checked_add(len).ok_or_else(|| {
                StoreError::TruncatedValueItem(format!("offset {index} + {len} overflows"))
            })?;
            bytes.get(index..end).ok_or_else(|| {
                StoreError::TruncatedValueItem(format!(
                    "need {len} byte(s) at offset {index}, buffer is {} byte(s)",
                    bytes.len()
                ))
            })
        }
        let mut index = 0usize;
        let vtype = *bytes
            .first()
            .ok_or_else(|| StoreError::TruncatedValueItem("empty buffer".into()))?;
        index += 1;
        let val = match vtype {
            0 => ValueItem::Null,
            5 => {
                let v = i64::from_le_bytes(take(bytes, index, size_of::<i64>())?.try_into().unwrap());
                index += size_of::<i64>();
                ValueItem::Integer(v)
            }
            10 => {
                let v = f64::from_le_bytes(take(bytes, index, size_of::<f64>())?.try_into().unwrap());
                index += size_of::<f64>();
                ValueItem::Double(v)
            }
            15 => {
                let v = u64::from_le_bytes(take(bytes, index, size_of::<u64>())?.try_into().unwrap());
                index += size_of::<u64>();
                ValueItem::Datetime(v)
            }
            20 => {
                let len =
                    u32::from_le_bytes(take(bytes, index, size_of::<u32>())?.try_into().unwrap());
                index += size_of::<u32>();
                let real_len = u32::from_le_bytes(
                    take(bytes, index, size_of::<u32>())?.try_into().unwrap(),
                ) as usize;
                index += size_of::<u32>();
                let str = String::from_utf8(take(bytes, index, real_len)?.to_vec())
                    .unwrap_or_default();
                // to_bytes() pads the content out to `len` bytes when the
                // real content is shorter than the reserved capacity — skip
                // that padding too, not just the real content, or the next
                // value in the buffer is misread starting mid-padding.
                index = index.saturating_add(real_len.max(len as usize));
                ValueItem::Str((str, len))
            }
            25 => {
                let len =
                    u32::from_le_bytes(take(bytes, index, size_of::<u32>())?.try_into().unwrap());
                index += size_of::<u32>();
                let real_len = u32::from_le_bytes(
                    take(bytes, index, size_of::<u32>())?.try_into().unwrap(),
                ) as usize;
                index += size_of::<u32>();
                let arc = Arc::from(take(bytes, index, real_len)?);
                // See the Str case above: skip trailing padding too.
                index = index.saturating_add(real_len.max(len as usize));
                ValueItem::Blob((arc, len))
            }
            30 => {
                let v = take(bytes, index, size_of::<u8>())?[0] != 0;
                index += size_of::<u8>();
                ValueItem::Boolean(v)
            }
            i => {
                error!("Unknown value item : {i}");
                ValueItem::Null
            }
        };
        Ok((val, index))
    }

    fn discriminant(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }
}

impl Hash for ValueItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ValueItem::Double(f) => {
                f.to_bits().hash(state);
            }
            ValueItem::Blob(b) => b.hash(state),
            ValueItem::Datetime(d) => d.hash(state),
            ValueItem::Integer(i) => i.hash(state),
            ValueItem::Str(s) => s.hash(state),
            ValueItem::Boolean(b) => b.hash(state),
            ValueItem::Null => {}
        }
    }
}

impl Display for ValueItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueItem::Integer(i) => write!(f, "{}", i),
            ValueItem::Double(d) => write!(f, "{}", d),
            ValueItem::Datetime(dt) => write!(f, "{}", dt),
            ValueItem::Str(s) => write!(f, "{}", s.0),
            ValueItem::Blob(_) => write!(f, "(blob)"),
            ValueItem::Boolean(b) => write!(f, "{}", b),
            ValueItem::Null => write!(f, "(null)"),
        }
    }
}

impl PartialOrd for ValueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ValueItem {
    // STORE_AUDIT.md S7: fixed, total (if otherwise arbitrary) rank across
    // variants, used by Ord::cmp only when comparing two DIFFERENT
    // variants. Store has no schema to prevent a Rec key's fields from
    // differing in type between rows (or a range scan over mixed-type
    // data), so Ord must be a genuine total order over every possible
    // pair — not "every same-type pair, plus a panic for anything else".
    // Null ranks lowest (unchanged from the old special-casing); the rest
    // is an arbitrary but fixed and documented order.
    fn type_rank(&self) -> u8 {
        match self {
            ValueItem::Null => 0,
            ValueItem::Boolean(_) => 1,
            ValueItem::Integer(_) => 2,
            ValueItem::Double(_) => 3,
            ValueItem::Datetime(_) => 4,
            ValueItem::Str(_) => 5,
            ValueItem::Blob(_) => 6,
        }
    }
}

impl Ord for ValueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ValueItem::Integer(a), ValueItem::Integer(b)) => a.cmp(b),
            (ValueItem::Double(a), ValueItem::Double(b)) => a.total_cmp(b),
            (ValueItem::Datetime(a), ValueItem::Datetime(b)) => a.cmp(b),
            (ValueItem::Str(a), ValueItem::Str(b)) => a.0.cmp(&b.0),
            (ValueItem::Boolean(a), ValueItem::Boolean(b)) => a.cmp(b),
            // Content-based, like Str above — the u32 alongside the bytes
            // is reserved on-disk capacity, not part of the logical value
            // (see PartialEq's own handling of this same distinction).
            (ValueItem::Blob(a), ValueItem::Blob(b)) => a.0.cmp(&b.0),
            (ValueItem::Null, ValueItem::Null) => Ordering::Equal,
            // Different variants: fall back to the fixed type rank instead
            // of panicking — see type_rank's own comment. Subsumes the old
            // Null-vs-anything special cases (Null's rank is lowest) and
            // makes every pair, in either order, agree with each other
            // (a.cmp(&b) == b.cmp(&a).reverse()) — the old code's Null
            // handling didn't (Blob.cmp(&Null) panicked, but
            // Null.cmp(&Blob) didn't).
            _ => self.type_rank().cmp(&other.type_rank()),
        }
    }
}

impl TryFrom<String> for ValueItem {
    type Error = StoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let len = value.len() as u32;
        Ok(ValueItem::Str((value, len)))
    }
}
impl TryFrom<&String> for ValueItem {
    type Error = StoreError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let len = value.len() as u32;
        Ok(ValueItem::Str((value.clone(), len)))
    }
}

impl Eq for ValueItem {}

#[cfg(test)]
mod valueitem_tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use crate::valueitem::ValueItem;

    #[test]
    fn test_default_is_null() {
        assert_eq!(ValueItem::default(), ValueItem::Null);
    }

    #[test]
    fn test_cmp() {
        assert!(ValueItem::Integer(0) < ValueItem::Integer(1));
        assert!(ValueItem::Integer(-1) < ValueItem::Integer(1));
        assert!(ValueItem::Integer(1) <= ValueItem::Integer(1));
        assert!(ValueItem::Integer(1) == ValueItem::Integer(1));
        assert!(ValueItem::Integer(2) >= ValueItem::Integer(1));
        assert!(ValueItem::Integer(2) > ValueItem::Integer(1));

        assert!(ValueItem::Null < ValueItem::Integer(0));
        assert!(ValueItem::Null < ValueItem::Str(("".to_owned(), 1)));
        assert!(ValueItem::Null == ValueItem::Null);
        assert!(ValueItem::Integer(-1234) > ValueItem::Null);
        assert!(ValueItem::Str(("".to_owned(), 1)) > ValueItem::Null);
    }

    #[test]
    fn test_cmp_integer_double_datetime() {
        assert!(ValueItem::Integer(0) < ValueItem::Integer(1));
        assert!(ValueItem::Integer(i64::MAX) > ValueItem::Integer(0));
        assert!(ValueItem::Integer(5) == ValueItem::Integer(5));

        assert!(ValueItem::Double(1.5) < ValueItem::Double(2.5));
        assert!(ValueItem::Double(-1.5) < ValueItem::Double(0.0));
        assert!(ValueItem::Double(1.0) == ValueItem::Double(1.0));
        assert!(ValueItem::Double(f64::NEG_INFINITY) < ValueItem::Double(0.0));
        assert!(ValueItem::Double(f64::INFINITY) > ValueItem::Double(f64::MAX));

        assert!(ValueItem::Datetime(100) < ValueItem::Datetime(200));
        assert!(ValueItem::Datetime(0) == ValueItem::Datetime(0));
    }

    // Double's PartialEq is bit-pattern equality (a.to_bits() ==
    // b.to_bits()), not plain IEEE-754 `==` — see the impl's own doc
    // comment for why (it's what keeps Hash, PartialEq, and Ord all
    // agreeing with each other). The direct, intentional consequence:
    // this disagrees with `==` on regular f64 in exactly the two cases
    // where IEEE-754 equality and bit-pattern equality diverge.
    // std::hash::Hash's own trait method, not ValueItem's unrelated
    // inherent hash(&self) -> u64 (a separate, coarser FNV-style hash
    // IndexKey uses — calling `.hash()` directly would resolve to that
    // one instead, which isn't what's being verified here).
    fn std_hash(v: &ValueItem) -> u64 {
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(v, &mut h);
        h.finish()
    }

    #[test]
    fn test_double_equality_is_bit_pattern_based_not_ieee754() {
        // NaN == NaN is true here, unlike plain f64 (NaN != NaN always).
        assert_eq!(ValueItem::Double(f64::NAN), ValueItem::Double(f64::NAN));
        // -0.0 == 0.0 is false here, unlike plain f64 (-0.0 == 0.0 is true).
        assert_ne!(ValueItem::Double(-0.0), ValueItem::Double(0.0));

        // And consistent with std::hash::Hash: equal ValueItems must hash
        // equally, which plain IEEE-754 equality would have broken for
        // -0.0/0.0 (see this impl's own doc comment) since Hash already
        // hashes by bit pattern.
        assert_eq!(
            std_hash(&ValueItem::Double(0.0)),
            std_hash(&ValueItem::Double(0.0))
        );
        assert_ne!(
            std_hash(&ValueItem::Double(-0.0)),
            std_hash(&ValueItem::Double(0.0))
        );

        // And consistent with Ord: equal under PartialEq iff equal under
        // partial_cmp, which was the whole point.
        assert_eq!(
            ValueItem::Double(f64::NAN).partial_cmp(&ValueItem::Double(f64::NAN)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_cmp_boolean() {
        assert!(ValueItem::Boolean(false) < ValueItem::Boolean(true));
        assert!(ValueItem::Boolean(true) == ValueItem::Boolean(true));
        assert!(ValueItem::Null < ValueItem::Boolean(false));
        assert!(ValueItem::Boolean(true) > ValueItem::Null);
    }

    // STORE_AUDIT.md S7: mixed-type comparisons used to panic; now they
    // fall back to a fixed type rank (see ValueItem::type_rank) instead,
    // and agree with each other regardless of argument order.
    #[test]
    fn test_partial_ord_boolean_vs_integer_uses_type_rank() {
        assert_eq!(
            ValueItem::Boolean(true).partial_cmp(&ValueItem::Integer(1)),
            Some(Ordering::Less)
        );
        assert_eq!(
            ValueItem::Integer(1).partial_cmp(&ValueItem::Boolean(true)),
            Some(Ordering::Greater)
        );
    }

    // The u32 alongside the String/blob data is a reserved on-disk capacity
    // (see to_bytes' padding logic), not part of the logical value — so
    // ordering must compare content only and ignore it.
    #[test]
    fn test_str_ordering_ignores_reserved_capacity() {
        assert!(ValueItem::Str(("apple".into(), 5)) < ValueItem::Str(("banana".into(), 500)));
        assert_eq!(
            ValueItem::Str(("apple".into(), 500)).partial_cmp(&ValueItem::Str(("apple".into(), 5))),
            Some(std::cmp::Ordering::Equal),
            "same content with different reserved capacity must compare Equal"
        );
    }

    // Surfaces a real inconsistency: derived PartialEq (used by `==`)
    // compares the *whole* (String, u32) tuple including reserved capacity,
    // but partial_cmp (used by `<`/`>`/sort) only compares the string
    // content. Two Str values can therefore be `!=` while also comparing
    // as `Equal` under partial_cmp — violating the usual expectation that
    // `a == b` iff `a.partial_cmp(&b) == Some(Equal)`. Documented here so
    // it's not accidentally relied upon either way (e.g. a BTree that
    // dedupes by ordering-equality could conflate two structurally
    // distinct values).
    #[test]
    fn test_str_eq_and_partial_cmp_disagree_on_reserved_capacity() {
        let a = ValueItem::Str(("apple".into(), 5));
        let b = ValueItem::Str(("apple".into(), 500));
        assert_ne!(a, b, "derived PartialEq compares the reserved capacity too");
        assert_eq!(
            a.partial_cmp(&b),
            Some(std::cmp::Ordering::Equal),
            "but partial_cmp only compares content, so it disagrees with =="
        );
    }

    #[test]
    fn test_blob_partial_eq_compares_content_not_identity() {
        let a = ValueItem::Blob((Arc::from(&b"same"[..]), 10));
        let b = ValueItem::Blob((Arc::from(&b"same"[..]), 10));
        assert_eq!(a, b, "distinct Arcs with equal content must be ==");
        let c = ValueItem::Blob((Arc::from(&b"same"[..]), 999));
        assert_ne!(a, c, "differing reserved capacity makes them !=, like Str");
    }

    // STORE_AUDIT.md S7: Blob is now genuinely comparable — same-variant
    // Blob-vs-Blob compares content (like Str), and Blob-vs-anything-else
    // falls back to the fixed type rank, same as any other mixed-type pair.
    #[test]
    fn test_partial_ord_blob_vs_blob_compares_content() {
        let a = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
        let c = ValueItem::Blob((Arc::from(&b"y"[..]), 1));
        assert_eq!(a.partial_cmp(&c), Some(Ordering::Less));
    }

    #[test]
    fn test_partial_ord_blob_vs_integer_uses_type_rank() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        assert_eq!(
            b.partial_cmp(&ValueItem::Integer(1)),
            Some(Ordering::Greater)
        );
        assert_eq!(ValueItem::Integer(1).partial_cmp(&b), Some(Ordering::Less));
    }

    // Previously a real asymmetry: Blob-vs-Null panicked from the Blob
    // side but not from the Null side (Null had its own special-cased
    // catch-all checked before any Blob-specific arm). Both directions
    // now agree, like every other mixed-type pair.
    #[test]
    fn test_partial_ord_null_vs_blob_is_symmetric() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        assert_eq!(ValueItem::Null.partial_cmp(&b), Some(Ordering::Less));
        assert_eq!(b.partial_cmp(&ValueItem::Null), Some(Ordering::Greater));
    }

    #[test]
    fn test_partial_ord_integer_vs_str_uses_type_rank() {
        assert_eq!(
            ValueItem::Integer(1).partial_cmp(&ValueItem::Str(("x".into(), 1))),
            Some(Ordering::Less)
        );
        assert_eq!(
            ValueItem::Str(("x".into(), 1)).partial_cmp(&ValueItem::Integer(1)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn test_size_and_size_of_empty() {
        // +1 everywhere for the discriminant byte.
        assert_eq!(ValueItem::Null.size(), 1);
        assert_eq!(ValueItem::Null.size_of_empty(), 1);

        assert_eq!(ValueItem::Integer(42).size(), 1 + size_of::<i64>());
        assert_eq!(ValueItem::Integer(42).size(), 1 + size_of::<u64>());
        assert_eq!(ValueItem::Double(1.0).size(), 1 + size_of::<f64>());
        assert_eq!(ValueItem::Datetime(1).size(), 1 + size_of::<u64>());
        assert_eq!(ValueItem::Boolean(true).size(), 1 + size_of::<u8>());
        assert_eq!(
            ValueItem::Boolean(true).size(),
            ValueItem::Boolean(true).size_of_empty(),
            "fixed-width variants have no empty/full distinction"
        );
        assert_eq!(
            ValueItem::Integer(42).size(),
            ValueItem::Integer(42).size_of_empty(),
            "fixed-width variants have no empty/full distinction"
        );

        // Str/Blob: size() is driven by the *reserved* capacity (the u32),
        // not the actual content length — size_of_empty() is the fixed
        // overhead alone (two u32 length fields + discriminant).
        let s = ValueItem::Str(("hi".into(), 20));
        assert_eq!(s.size(), 1 + 20 + size_of::<u32>() * 2);
        assert_eq!(s.size_of_empty(), 1 + size_of::<u32>() * 2);

        let b = ValueItem::Blob((Arc::from(&b"hi"[..]), 20));
        assert_eq!(b.size(), 1 + 20 + size_of::<u32>() * 2);
        assert_eq!(b.size_of_empty(), 1 + size_of::<u32>() * 2);
    }

    // size() for Str/Blob is driven entirely by the reserved-capacity u32,
    // not the actual byte length of the content. When the content is
    // LONGER than the declared capacity, to_bytes() (see below) writes the
    // full content anyway with no padding — so size() silently
    // under-reports the real serialized length in that case. Documented as
    // a real discrepancy, not asserted as "correct": callers that size a
    // page slot from `.size()` alone (matching the pattern used elsewhere
    // in this codebase, e.g. Tuple::size()) would under-allocate.
    #[test]
    fn test_size_can_under_report_when_content_exceeds_reserved_capacity() {
        let s = ValueItem::Str(("this is way more than five bytes".into(), 5));
        let reported = s.size();
        let actual = s.to_bytes().len();
        assert!(
            actual > reported,
            "expected actual serialized size ({actual}) to exceed size() ({reported}) \
             when content overruns the declared capacity"
        );
    }

    #[test]
    fn test_valueitem_serialize() {
        let ivalue = ValueItem::Integer(123456);
        let ibytes = ivalue.to_bytes();
        assert_eq!(ivalue, ValueItem::from_bytes_single(&ibytes).unwrap());
        let fvalue = ValueItem::Double(123456.5678);
        let fbytes = fvalue.to_bytes();
        assert_eq!(fvalue, ValueItem::from_bytes_single(&fbytes).unwrap());
        let dvalue = ValueItem::Datetime(1234567);
        let dbytes = dvalue.to_bytes();
        assert_eq!(dvalue, ValueItem::from_bytes_single(&dbytes).unwrap());
        let svalue = ValueItem::Str(("Hello, World".to_owned(), 20));
        let sbytes = svalue.to_bytes();
        assert_eq!(svalue, ValueItem::from_bytes_single(&sbytes).unwrap());
        let bvalue = ValueItem::Blob((Arc::new([b'A'; 545]), 545));
        let bbytes = bvalue.to_bytes();
        assert_eq!(bvalue, ValueItem::from_bytes_single(&bbytes).unwrap());
        let nvalue = ValueItem::Null;
        let nbytes = nvalue.to_bytes();
        assert_eq!(nvalue, ValueItem::from_bytes_single(&nbytes).unwrap());
        for bvalue in [ValueItem::Boolean(true), ValueItem::Boolean(false)] {
            let bbytes = bvalue.to_bytes();
            assert_eq!(bvalue, ValueItem::from_bytes_single(&bbytes).unwrap());
        }

        let junk = vec![b'A'; 10];
        assert_eq!(ValueItem::Null, ValueItem::from_bytes_single(&junk).unwrap());
    }

    #[test]
    fn test_serialize_edge_values() {
        for v in [
            ValueItem::Integer(i64::MIN),
            ValueItem::Integer(i64::MAX),
            ValueItem::Integer(0),
            ValueItem::Integer(0),
            ValueItem::Integer(i64::MAX),
            ValueItem::Datetime(0),
            ValueItem::Datetime(u64::MAX),
            ValueItem::Str(("".into(), 0)),
            ValueItem::Blob((Arc::from(&[][..]), 0)),
        ] {
            let bytes = v.to_bytes();
            assert_eq!(
                v,
                ValueItem::from_bytes_single(&bytes).unwrap(),
                "roundtrip failed for {v:?}"
            );
        }

        // NaN and signed zero need bit-level comparison since NaN != NaN
        // and derived PartialEq on f64 follows IEEE 754 semantics.
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0] {
            let v = ValueItem::Double(f);
            let bytes = v.to_bytes();
            match ValueItem::from_bytes_single(&bytes).unwrap() {
                ValueItem::Double(got) => {
                    assert_eq!(
                        got.to_bits(),
                        f.to_bits(),
                        "bit pattern must round-trip exactly for {f}"
                    );
                }
                other => panic!("expected Double, got {other:?}"),
            }
        }
    }

    // Content longer than the declared reserved capacity (s.1 < s.0.len())
    // must still round-trip correctly for a single value: to_bytes()
    // writes the real length either way, and from_bytes_single only cares
    // about the returned value, not the index into the buffer.
    #[test]
    fn test_serialize_content_longer_than_reserved_capacity_single_value() {
        let s = ValueItem::Str(("this is way more than five bytes".into(), 5));
        let bytes = s.to_bytes();
        assert_eq!(s, ValueItem::from_bytes_single(&bytes).unwrap());

        let b = ValueItem::Blob((Arc::from(&b"this is way more than five bytes"[..]), 5));
        let bytes = b.to_bytes();
        assert_eq!(b, ValueItem::from_bytes_single(&bytes).unwrap());
    }

    #[test]
    fn test_from_bytes_many_sequential_values_no_padding() {
        // Reserved capacity equals actual length, so to_bytes() writes no
        // padding — the simplest case, and it works today.
        let a = ValueItem::Str(("hi".into(), 2));
        let b = ValueItem::Integer(42);
        let mut bytes = a.to_bytes();
        bytes.extend(b.to_bytes());

        let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes).unwrap();
        assert_eq!(parsed_a, a);
        let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]).unwrap();
        assert_eq!(
            parsed_b, b,
            "second value must be recovered after the first's real index"
        );
    }

    // BUG (now fixed): the fixed-width branches (Integer/Integer/Double/
    // Datetime) never advanced `index` past the discriminant byte, so the
    // returned index was always short by the value's own width whenever a
    // fixed-width value was anything but the last one read from a buffer.
    #[test]
    fn test_from_bytes_many_index_advances_past_fixed_width_values() {
        for a in [
            ValueItem::Integer(-7),
            ValueItem::Integer(7),
            ValueItem::Double(1.25),
            ValueItem::Datetime(99),
        ] {
            let b = ValueItem::Str(("tail".into(), 4));
            let mut bytes = a.to_bytes();
            bytes.extend(b.to_bytes());

            let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes).unwrap();
            assert_eq!(parsed_a, a);
            let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]).unwrap();
            assert_eq!(
                parsed_b, b,
                "index after reading {a:?} must skip its full width, not just the tag byte"
            );
        }
    }

    // BUG: from_bytes_many's returned index only accounts for the *real*
    // content length it read, never the padding to_bytes() adds when the
    // reserved capacity (s.1/b.1) exceeds the actual content length. So
    // parsing a value with padding, then continuing to parse from the
    // returned index, misreads the padding bytes as the start of the next
    // value instead of skipping them. This directly affects IndexKey's
    // multi-field to_bytes/from_bytes round trip whenever a non-last Str
    // or Blob field reserves more capacity than it currently uses.
    #[test]
    fn test_from_bytes_many_sequential_values_with_padding() {
        let a = ValueItem::Str(("hi".into(), 20)); // reserves 20, uses 2: 18 padding bytes
        let b = ValueItem::Integer(42);
        let mut bytes = a.to_bytes();
        bytes.extend(b.to_bytes());

        let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes).unwrap();
        assert_eq!(parsed_a, a);
        let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]).unwrap();
        assert_eq!(
            parsed_b, b,
            "index returned by from_bytes_many must skip padding bytes, not just the real \
             content — otherwise the next value in the buffer is misparsed"
        );
    }

    #[test]
    fn test_hash_is_deterministic() {
        for v in [
            ValueItem::Null,
            ValueItem::Integer(-42),
            ValueItem::Integer(42),
            ValueItem::Double(1.5),
            ValueItem::Datetime(123),
            ValueItem::Str(("hello".into(), 10)),
            ValueItem::Blob((Arc::from(&b"hello"[..]), 10)),
            ValueItem::Boolean(true),
            ValueItem::Boolean(false),
        ] {
            assert_eq!(
                v.hash(),
                v.hash(),
                "hash must be stable across calls for {v:?}"
            );
        }
    }

    // hash() uses Double(f).to_bits(), not a lossy `f as u64` truncating
    // cast — the latter would silently collide any two doubles with the
    // same integer part (e.g. 1.5 and 1.9 both truncate to 1), and more
    // importantly would violate Hash's own contract against this type's
    // hand-written PartialEq (see its own doc comment), which is already
    // bit-pattern equality (`a.to_bits() == b.to_bits()`), not `==`.
    #[test]
    fn test_double_hash_is_bit_pattern_based_not_a_lossy_cast() {
        // A naive `f as u64` cast would collide every value in this group
        // (all truncate to 1) despite having distinct bit patterns.
        let a = ValueItem::Double(1.1).hash();
        let b = ValueItem::Double(1.5).hash();
        let c = ValueItem::Double(1.9).hash();
        assert_ne!(a, b, "1.1 and 1.5 must not collide under a bit-pattern hash");
        assert_ne!(b, c, "1.5 and 1.9 must not collide under a bit-pattern hash");
        assert_ne!(a, c, "1.1 and 1.9 must not collide under a bit-pattern hash");

        // Hash/Eq consistency (Hash's own contract: a == b must imply
        // hash(a) == hash(b)) for the two cases where this type's
        // bit-pattern PartialEq deliberately disagrees with plain
        // IEEE-754 `==` — see test_double_equality_is_bit_pattern_based.
        let nan1 = ValueItem::Double(f64::NAN);
        let nan2 = ValueItem::Double(f64::NAN);
        assert_eq!(nan1, nan2, "sanity: bit-identical NaNs are equal under this type's PartialEq");
        assert_eq!(
            nan1.hash(),
            nan2.hash(),
            "equal (per this type's own PartialEq) values must hash equal"
        );

        let pos_zero = ValueItem::Double(0.0);
        let neg_zero = ValueItem::Double(-0.0);
        assert_ne!(
            pos_zero, neg_zero,
            "sanity: 0.0 and -0.0 are NOT equal under this type's bit-pattern PartialEq"
        );
        // Not required by the Hash contract (unequal values are allowed
        // to collide), but a to_bits()-based hash will in fact differ
        // here, since 0.0 and -0.0 have different bit patterns — pinning
        // that down as the concrete, expected behavior of this hash.
        assert_ne!(
            pos_zero.hash(),
            neg_zero.hash(),
            "0.0 and -0.0 have different bit patterns, so a bit-pattern hash distinguishes them"
        );
    }

    #[test]
    fn test_hash_str_and_blob_ignore_reserved_capacity() {
        // hash() delegates Str/Blob to db_hash() of the content only.
        let a = ValueItem::Str(("hello".into(), 5));
        let b = ValueItem::Str(("hello".into(), 500));
        assert_eq!(a.hash(), b.hash());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", ValueItem::Integer(-5)), "-5");
        assert_eq!(format!("{}", ValueItem::Integer(5)), "5");
        assert_eq!(format!("{}", ValueItem::Double(1.5)), "1.5");
        assert_eq!(format!("{}", ValueItem::Datetime(100)), "100");
        assert_eq!(format!("{}", ValueItem::Str(("hi".into(), 10))), "hi");
        assert_eq!(format!("{}", ValueItem::Boolean(true)), "true");
        assert_eq!(format!("{}", ValueItem::Boolean(false)), "false");
        assert_eq!(format!("{}", ValueItem::Null), "(null)");
        assert_eq!(
            format!("{}", ValueItem::Blob((Arc::from(&b"x"[..]), 1))),
            "(blob)"
        );
    }

    #[test]
    fn test_discriminant_matches_wire_tag() {
        assert_eq!(ValueItem::Null.discriminant(), 0);
        assert_eq!(ValueItem::Integer(0).discriminant(), 5);
        assert_eq!(ValueItem::Double(0.0).discriminant(), 10);
        assert_eq!(ValueItem::Datetime(0).discriminant(), 15);
        assert_eq!(ValueItem::Str(("".into(), 0)).discriminant(), 20);
        assert_eq!(ValueItem::Blob((Arc::from(&[][..]), 0)).discriminant(), 25);
        assert_eq!(ValueItem::Boolean(true).discriminant(), 30);
    }

    #[test]
    fn test_unknown_tag_decodes_as_null() {
        // Tag byte 65 ('A') matches none of the known variants (0/5/6/10/
        // 15/20/25); from_bytes_many's fallback logs and returns Null
        // instead of panicking on malformed/corrupt input.
        let junk = vec![b'A'; 10];
        assert_eq!(ValueItem::Null, ValueItem::from_bytes_single(&junk).unwrap());
    }
}

#[cfg(test)]
mod indexkey_tests {
    use std::sync::Arc;

    use crate::valueitem::{IndexKey, ValueItem};

    #[test]
    fn test_default_is_empty() {
        let k = IndexKey::default();
        assert_eq!(
            k.size(),
            size_of::<u64>(),
            "just the field-count prefix, no fields"
        );
        assert_eq!(k, IndexKey::new_from(&[]).unwrap());
    }

    #[test]
    fn test_new_from_and_size_sums_fields_plus_count_prefix() {
        let k = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        assert_eq!(
            k.size(),
            size_of::<u64>() + ValueItem::Integer(1).size() + ValueItem::Integer(2).size()
        );
    }

    #[test]
    fn test_from_slice_matches_new_from() {
        let fields = [ValueItem::Integer(1), ValueItem::Str(("x".into(), 1))];
        assert_eq!(
            IndexKey::from(&fields[..]),
            IndexKey::new_from(&fields).unwrap()
        );
    }

    #[test]
    fn test_eq_reflexive_and_field_sensitive() {
        let a = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        let c = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(3)]).unwrap();
        assert_eq!(a, a.clone());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_display_writes_one_line_per_field() {
        let k = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        let s = format!("{k}");
        assert_eq!(s.lines().count(), 2);
    }

    // --- to_bytes / from_bytes round trip ---
    //
    // BUG: IndexKey::from_bytes reads the leading field count via
    // `bytes.try_into().unwrap()`, converting the ENTIRE input slice into a
    // `[u8; 8]` array rather than slicing just the first 8 bytes
    // (`bytes[0..8]`). `<[u8; 8]>::try_from(&[u8])` only succeeds when the
    // slice's length is exactly 8. to_bytes() always emits the 8-byte
    // count prefix followed by every field's own bytes, so the total
    // length is 8 only for a key with zero fields — any non-empty
    // IndexKey's serialized bytes are longer than 8, and from_bytes panics
    // on the `.unwrap()` instead of decoding it.

    #[test]
    fn test_roundtrip_empty_key() {
        let k = IndexKey::new_from(&[]).unwrap();
        let bytes = k.to_bytes();
        assert_eq!(bytes.len(), 8, "empty key is just the 8-byte zero count");
        assert_eq!(k, IndexKey::from_bytes(&bytes).unwrap());
    }

    #[test]
    fn test_roundtrip_single_field_key() {
        let k = IndexKey::new_from(&[ValueItem::Integer(42)]).unwrap();
        let bytes = k.to_bytes();
        assert_eq!(
            k,
            IndexKey::from_bytes(&bytes).unwrap(),
            "a single-field (non-empty) key must round-trip through to_bytes/from_bytes"
        );
    }

    #[test]
    fn test_roundtrip_multi_field_key_mixed_types() {
        let k = IndexKey::new_from(&[
            ValueItem::Integer(-7),
            ValueItem::Str(("hello".into(), 5)), // no padding: capacity == len
            ValueItem::Double(1.25),
            ValueItem::Integer(9),
        ])
        .unwrap();
        let bytes = k.to_bytes();
        assert_eq!(k, IndexKey::from_bytes(&bytes).unwrap());
    }

    // Same as above, but the Str field reserves more capacity than it
    // uses and is NOT the last field — this is the case that additionally
    // depends on ValueItem::from_bytes_many correctly skipping padding
    // bytes (see test_from_bytes_many_sequential_values_with_padding in
    // valueitem_tests) on top of IndexKey::from_bytes's own count-parsing
    // bug above.
    #[test]
    fn test_roundtrip_multi_field_key_with_padded_non_last_field() {
        let k = IndexKey::new_from(&[
            ValueItem::Str(("hi".into(), 20)), // reserves 20, uses 2
            ValueItem::Integer(42),
        ])
        .unwrap();
        let bytes = k.to_bytes();
        assert_eq!(k, IndexKey::from_bytes(&bytes).unwrap());
    }

    // IndexKey::size() sums each field's own size() but never accounts for
    // the 8-byte field-count prefix to_bytes() always writes — so it
    // under-reports the true serialized length by a fixed 8 bytes for
    // every key. Matters if size() is used (as the analogous Tuple::size()
    // is elsewhere in this codebase) to decide whether a key fits in a
    // page slot: an 8-byte-too-small estimate would let a key that
    // actually doesn't fit look like it does.
    #[test]
    fn test_size_matches_actual_serialized_length() {
        let k = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        assert_eq!(
            k.size(),
            k.to_bytes().len(),
            "size() must match to_bytes().len() (it's short by the 8-byte count prefix)"
        );
    }

    // --- ordering ---

    #[test]
    fn test_partial_ord_first_field_decides() {
        let a = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(99)]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Integer(2), ValueItem::Integer(0)]).unwrap();
        assert!(a < b);
    }

    #[test]
    fn test_partial_ord_ties_broken_by_next_field() {
        let a = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(1)]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        assert!(a < b);
    }

    #[test]
    fn test_partial_ord_equal_keys() {
        let a =
            IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Str(("x".into(), 1))]).unwrap();
        let b =
            IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Str(("x".into(), 1))]).unwrap();
        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Equal));
    }

    // BUG (or at least an easy-to-miss quirk): partial_cmp zips the two
    // keys' fields together, so comparison stops at the length of the
    // SHORTER key. A key that is a strict field-wise prefix of a longer
    // key compares as Equal, even though the two keys have a different
    // number of columns. This only matters if IndexKey is ever compared
    // across differing field counts (e.g. a partial-column range-query
    // probe key vs a full index key) — for a fixed-shape multi-column
    // index compared against itself, lengths always match and this never
    // triggers.
    #[test]
    fn test_partial_ord_shorter_key_that_is_a_prefix_compares_equal() {
        let short = IndexKey::new_from(&[ValueItem::Integer(1)]).unwrap();
        let long = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        assert_eq!(
            short.partial_cmp(&long),
            Some(std::cmp::Ordering::Equal),
            "current behavior: comparison stops at the shorter key's length instead of \
             treating a strict prefix as less-than"
        );
    }

    #[test]
    fn test_hash_is_deterministic() {
        let k =
            IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Str(("a".into(), 1))]).unwrap();
        assert_eq!(k.hash(), k.hash());
    }

    #[test]
    fn test_hash_fields_matches_hash_of_an_equivalent_index_key() {
        let values = [ValueItem::Integer(1), ValueItem::Str(("a".into(), 1))];
        let k = IndexKey::new_from(&values).unwrap();
        assert_eq!(
            IndexKey::hash_fields(values.iter()),
            k.hash(),
            "hashing field references directly must agree with hashing a real IndexKey \
             built from the same fields"
        );
    }

    #[test]
    fn test_hash_fields_works_over_a_subset_of_a_wider_row() {
        // The actual motivating use case: hashing just the key columns of
        // a wider row (e.g. a hash index's chosen columns), without first
        // collecting them into a fresh Vec<ValueItem>/IndexKey.
        let row = [
            ValueItem::Integer(1),
            ValueItem::Str(("ignored".into(), 10)),
            ValueItem::Integer(2),
        ];
        let key_columns = [0usize, 2usize];
        let subset_hash = IndexKey::hash_fields(key_columns.iter().map(|&i| &row[i]));

        let expected = IndexKey::new_from(&[row[0].clone(), row[2].clone()])
            .unwrap()
            .hash();
        assert_eq!(subset_hash, expected);
    }

    // hash() mixes per-field hashes with an FNV-1a-style XOR+multiply
    // combiner, unlike the plain bitwise-OR it used to use — field order
    // and each field's own value both affect the result, so two
    // structurally different keys built from the same field values in a
    // different arrangement should (not guaranteed, but expected in
    // practice) land on different hashes, unlike the old OR-combiner where
    // e.g. [Integer(1), Integer(2)] and [Integer(3)] collided outright
    // (1|2 == 3).
    #[test]
    fn test_hash_mixes_field_order_not_just_bitwise_union() {
        let a = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Integer(2), ValueItem::Integer(1)]).unwrap();
        assert_ne!(a, b, "structurally different keys (order matters)");
        assert_ne!(
            a.hash(),
            b.hash(),
            "a real mixing function shouldn't collide on such a simple reordering"
        );
    }

    // --- Blob fields and ordering ---

    // STORE_AUDIT.md S7: ValueItem::Blob's PartialOrd used to always
    // panic, so any IndexKey containing a Blob field could never be
    // ordered against another key, even one holding an identical Blob.
    // Now compares content, like Str.
    #[test]
    fn test_partial_ord_compares_content_when_a_field_is_blob() {
        let a = IndexKey::new_from(&[ValueItem::Blob((Arc::from(&b"x"[..]), 1))]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Blob((Arc::from(&b"x"[..]), 1))]).unwrap();
        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Equal));
        let c = IndexKey::new_from(&[ValueItem::Blob((Arc::from(&b"y"[..]), 1))]).unwrap();
        assert_eq!(a.partial_cmp(&c), Some(std::cmp::Ordering::Less));
    }

    // STORE_AUDIT.md S3: the hand-rolled key parser panics on truncated/
    // malformed input instead of returning an error — a real risk since
    // these bytes come straight off disk. Wrapped in catch_unwind rather
    // than asserted directly: the eventual fix will very likely change
    // these functions' signatures to return Result, and this assertion
    // (no panic occurs) stays meaningful and compiling either way, unlike
    // a call site that assumes one particular future signature.
    //
    // This is exactly the kind of panic S8 warns about happening under a
    // page lock (poisoning it); IndexKey::from_bytes/from_bytes_many are
    // called while decoding page content on the hot read path.
    #[test]
    fn test_audit_s3_from_bytes_does_not_panic_on_a_truncated_length_prefix() {
        // Not even enough bytes for the leading u64 field-count prefix.
        let result = std::panic::catch_unwind(|| IndexKey::from_bytes(&[1, 2, 3]));
        assert!(
            result.is_ok(),
            "IndexKey::from_bytes must not panic on truncated input, only report an error"
        );
    }

    #[test]
    fn test_audit_s3_from_bytes_many_does_not_panic_on_a_truncated_string_field() {
        // vtype=20 (Str), followed by a `len`/`real_len` pair claiming 100
        // bytes of content that were never actually written.
        let mut bytes = vec![20u8];
        bytes.extend_from_slice(&100u32.to_le_bytes()); // len
        bytes.extend_from_slice(&100u32.to_le_bytes()); // real_len
        // No content bytes at all follow.
        let result = std::panic::catch_unwind(|| ValueItem::from_bytes_many(&bytes));
        assert!(
            result.is_ok(),
            "ValueItem::from_bytes_many must not panic on a length prefix that overruns the \
             buffer, only report an error"
        );
    }
}
