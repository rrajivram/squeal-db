use std::{cmp::Ordering, fmt::Display, sync::Arc};

use log::error;
use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, db_hash},
    error::StoreError,
};

#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum ValueItem {
    #[default]
    Null = 0,
    Integer(i64) = 5,
    UInteger(u64) = 6,
    Double(f64) = 10,
    Datetime(u64) = 15,
    Str((String, u32)) = 20,
    Blob((Arc<[u8]>, u32)) = 25,
}

#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize)]
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

    pub fn size(&self) -> usize {
        size_of::<u64>() + self.data.iter().map(|d| d.size()).sum::<usize>()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend_from_slice(&self.data.len().to_le_bytes());
        for d in self.data.iter() {
            bytes.extend_from_slice(&d.to_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let count = u64::from_le_bytes(bytes[0..size_of::<u64>()].try_into().unwrap()) as usize;
        let mut index = size_of::<u64>();
        let mut data = vec![];
        for _ in 0..count {
            // from_bytes_many's returned index is relative to the sub-slice
            // it was handed (bytes[index..]), not absolute — it must be
            // added to the running offset, not replace it, or every field
            // after the first is read starting mid-way through the
            // previous one instead of where it actually begins.
            let (v, i) = ValueItem::from_bytes_many(&bytes[index..]);
            index += i;
            data.push(v);
        }
        Self::new_from(&data).unwrap_or(Self::new_from(&[ValueItem::Null]).unwrap())
    }

    pub fn hash(&self) -> u64 {
        let mut h = 0x811C9DC5;
        for d in self.data.iter() {
            h ^= d.hash();
            h = (h * 0x01000193) & 0xFFFFFFFF;
        }
        h
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
            ValueItem::UInteger(_) => Ok(()),
            ValueItem::Double(_) => Ok(()),
            ValueItem::Datetime(_) => Ok(()),
            ValueItem::Str(s) => {
                if s.0.len() as u32 > s.1 {
                    Err(StoreError::TupleTooLarge(s.1 as DBSizeType, s.0.len()))
                } else {
                    Ok(())
                }
            }
            ValueItem::Blob(s) => {
                if s.0.len() as u32 > s.1 {
                    Err(StoreError::TupleTooLarge(s.1 as DBSizeType, s.0.len()))
                } else {
                    Ok(())
                }
            }
            ValueItem::Null => Ok(()),
        }
    }

    pub(super) fn size(&self) -> usize {
        let sz = match self {
            ValueItem::Integer(_) => size_of::<i64>(),
            ValueItem::UInteger(_) => size_of::<u64>(),
            ValueItem::Double(_) => size_of::<f64>(),
            ValueItem::Datetime(_) => size_of::<u64>(),
            ValueItem::Str(s) => s.1 as usize + size_of::<u32>() * 2,
            ValueItem::Blob(b) => b.1 as usize + size_of::<u32>() * 2,
            ValueItem::Null => 0,
        };
        sz + 1
    }

    pub(super) fn size_of_empty(&self) -> usize {
        let sz = match self {
            ValueItem::Integer(_) => size_of::<i64>(),
            ValueItem::UInteger(_) => size_of::<u64>(),
            ValueItem::Double(_) => size_of::<f64>(),
            ValueItem::Datetime(_) => size_of::<u64>(),
            ValueItem::Str(_) => size_of::<u32>() * 2,
            ValueItem::Blob(_) => size_of::<u32>() * 2,
            ValueItem::Null => 0,
        };
        sz + 1
    }

    pub(super) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let value = match self {
            ValueItem::Null => 0u8,
            ValueItem::Integer(_) => 5,
            ValueItem::UInteger(_) => 6,
            ValueItem::Double(_) => 10,
            ValueItem::Datetime(_) => 15,
            ValueItem::Str(_) => 20,
            ValueItem::Blob(_) => 25,
        };
        bytes.push(value);
        match self {
            ValueItem::Integer(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            ValueItem::UInteger(u) => bytes.extend_from_slice(&u.to_le_bytes()),
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
            ValueItem::Null => {}
        }
        bytes
    }

    pub(super) fn hash(&self) -> u64 {
        match self {
            ValueItem::Null => 0,
            ValueItem::Integer(i) => *i as u64,
            ValueItem::UInteger(i) => *i,
            ValueItem::Double(f) => f.abs() as u64,
            ValueItem::Datetime(d) => *d,
            ValueItem::Str((s, _l)) => db_hash(s.as_bytes()),
            ValueItem::Blob((b, _)) => db_hash(b),
        }
    }

    pub(super) fn from_bytes_single(bytes: &[u8]) -> ValueItem {
        Self::from_bytes_many(bytes).0
    }

    pub(super) fn from_bytes_many(bytes: &[u8]) -> (ValueItem, usize) {
        let mut index = 0usize;
        let vtype = bytes[index];
        index += 1;
        let val = match vtype {
            0 => ValueItem::Null,
            5 => {
                let v =
                    i64::from_le_bytes(bytes[index..index + size_of::<i64>()].try_into().unwrap());
                index += size_of::<i64>();
                ValueItem::Integer(v)
            }
            6 => {
                let v =
                    u64::from_le_bytes(bytes[index..index + size_of::<i64>()].try_into().unwrap());
                index += size_of::<u64>();
                ValueItem::UInteger(v)
            }
            10 => {
                let v =
                    f64::from_le_bytes(bytes[index..index + size_of::<f64>()].try_into().unwrap());
                index += size_of::<f64>();
                ValueItem::Double(v)
            }
            15 => {
                let v =
                    u64::from_le_bytes(bytes[index..index + size_of::<i64>()].try_into().unwrap());
                index += size_of::<u64>();
                ValueItem::Datetime(v)
            }
            20 => {
                let len =
                    u32::from_le_bytes(bytes[index..index + size_of::<u32>()].try_into().unwrap());
                index += size_of::<u32>();
                let real_len =
                    u32::from_le_bytes(bytes[index..index + size_of::<u32>()].try_into().unwrap())
                        as usize;
                index += size_of::<u32>();
                let str =
                    String::from_utf8(bytes[index..index + real_len].to_vec()).unwrap_or_default();
                // to_bytes() pads the content out to `len` bytes when the
                // real content is shorter than the reserved capacity — skip
                // that padding too, not just the real content, or the next
                // value in the buffer is misread starting mid-padding.
                index += real_len.max(len as usize);
                ValueItem::Str((str, len))
            }
            25 => {
                let len =
                    u32::from_le_bytes(bytes[index..index + size_of::<u32>()].try_into().unwrap());
                index += size_of::<u32>();
                let real_len =
                    u32::from_le_bytes(bytes[index..index + size_of::<u32>()].try_into().unwrap())
                        as usize;
                index += size_of::<u32>();
                let arc = Arc::from(&bytes[index..index + real_len]);
                // See the Str case above: skip trailing padding too.
                index += real_len.max(len as usize);
                ValueItem::Blob((arc, len))
            }
            i => {
                error!("Unknown value item : {i}");
                ValueItem::Null
            }
        };
        (val, index)
    }

    fn discriminant(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }
}

impl Display for ValueItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueItem::Integer(i) => write!(f, "{}", i),
            ValueItem::UInteger(i) => write!(f, "{}", i),
            ValueItem::Double(d) => write!(f, "{}", d),
            ValueItem::Datetime(dt) => write!(f, "{}", dt),
            ValueItem::Str(s) => write!(f, "{}", s.0),
            ValueItem::Blob(_) => write!(f, "(blob)"),
            ValueItem::Null => write!(f, "(null)"),
        }
    }
}

impl PartialOrd for ValueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if *self == ValueItem::Null && *other == ValueItem::Null {
            return Some(std::cmp::Ordering::Equal);
        }
        match (self, other) {
            (ValueItem::Integer(a), ValueItem::Integer(b)) => a.partial_cmp(b),
            (ValueItem::UInteger(a), ValueItem::UInteger(b)) => a.partial_cmp(b),
            (ValueItem::Double(a), ValueItem::Double(b)) => a.partial_cmp(b),
            (ValueItem::Datetime(a), ValueItem::Datetime(b)) => a.partial_cmp(b),
            (ValueItem::Str(a), ValueItem::Str(b)) => a.0.partial_cmp(&b.0),
            (ValueItem::Blob(_), _) => panic!("Blobs cannot be compared."),

            (_, ValueItem::Null) => Some(std::cmp::Ordering::Greater),
            (ValueItem::Integer(_), _) => panic!("Invalid comparison. I"),
            (ValueItem::UInteger(_), _) => panic!("Invalid comparison. I"),
            (ValueItem::Double(_), _) => panic!("Invalid comparison. F "),
            (ValueItem::Datetime(_), _) => panic!("Invalid comparison. D"),
            (ValueItem::Str(_), _) => panic!("Invalid comparison. S"),

            (ValueItem::Null, _) => Some(std::cmp::Ordering::Less),
        }
    }
}

impl Eq for ValueItem {}

#[cfg(test)]
mod valueitem_tests {
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
    fn test_cmp_uinteger_double_datetime() {
        assert!(ValueItem::UInteger(0) < ValueItem::UInteger(1));
        assert!(ValueItem::UInteger(u64::MAX) > ValueItem::UInteger(0));
        assert!(ValueItem::UInteger(5) == ValueItem::UInteger(5));

        assert!(ValueItem::Double(1.5) < ValueItem::Double(2.5));
        assert!(ValueItem::Double(-1.5) < ValueItem::Double(0.0));
        assert!(ValueItem::Double(1.0) == ValueItem::Double(1.0));
        assert!(ValueItem::Double(f64::NEG_INFINITY) < ValueItem::Double(0.0));
        assert!(ValueItem::Double(f64::INFINITY) > ValueItem::Double(f64::MAX));

        assert!(ValueItem::Datetime(100) < ValueItem::Datetime(200));
        assert!(ValueItem::Datetime(0) == ValueItem::Datetime(0));
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

    #[test]
    #[should_panic(expected = "Blobs cannot be compared")]
    fn test_partial_ord_blob_vs_blob_panics() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        let _ = b.partial_cmp(&b.clone());
    }

    #[test]
    #[should_panic(expected = "Blobs cannot be compared")]
    fn test_partial_ord_blob_vs_integer_panics() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        let _ = b.partial_cmp(&ValueItem::Integer(1));
    }

    // Blob is checked before the `(_, Null)` catch-all, so Blob-vs-Null
    // panics from the Blob side...
    #[test]
    #[should_panic(expected = "Blobs cannot be compared")]
    fn test_partial_ord_blob_vs_null_panics() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        let _ = b.partial_cmp(&ValueItem::Null);
    }

    // ...but the same comparison with the operands swapped does NOT panic:
    // Null (as the left side) falls through to the final `(Null, _) =>
    // Less` arm before a Blob-specific case is ever checked on that side.
    // A real asymmetry — `a.partial_cmp(&b)` and `b.partial_cmp(&a)` are
    // not mirror images of each other for (Null, Blob) — documented so a
    // future caller doesn't assume partial_cmp is order-independent here.
    #[test]
    fn test_partial_ord_null_vs_blob_does_not_panic_but_blob_vs_null_does() {
        let b = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        assert_eq!(
            ValueItem::Null.partial_cmp(&b),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    #[should_panic(expected = "Invalid comparison. I")]
    fn test_partial_ord_integer_vs_str_panics() {
        let _ = ValueItem::Integer(1).partial_cmp(&ValueItem::Str(("x".into(), 1)));
    }

    #[test]
    #[should_panic(expected = "Invalid comparison. S")]
    fn test_partial_ord_str_vs_integer_panics() {
        let _ = ValueItem::Str(("x".into(), 1)).partial_cmp(&ValueItem::Integer(1));
    }

    #[test]
    fn test_size_and_size_of_empty() {
        // +1 everywhere for the discriminant byte.
        assert_eq!(ValueItem::Null.size(), 1);
        assert_eq!(ValueItem::Null.size_of_empty(), 1);

        assert_eq!(ValueItem::Integer(42).size(), 1 + size_of::<i64>());
        assert_eq!(ValueItem::UInteger(42).size(), 1 + size_of::<u64>());
        assert_eq!(ValueItem::Double(1.0).size(), 1 + size_of::<f64>());
        assert_eq!(ValueItem::Datetime(1).size(), 1 + size_of::<u64>());
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
        assert_eq!(ivalue, ValueItem::from_bytes_single(&ibytes));
        let fvalue = ValueItem::Double(123456.5678);
        let fbytes = fvalue.to_bytes();
        assert_eq!(fvalue, ValueItem::from_bytes_single(&fbytes));
        let dvalue = ValueItem::Datetime(1234567);
        let dbytes = dvalue.to_bytes();
        assert_eq!(dvalue, ValueItem::from_bytes_single(&dbytes));
        let svalue = ValueItem::Str(("Hello, World".to_owned(), 20));
        let sbytes = svalue.to_bytes();
        assert_eq!(svalue, ValueItem::from_bytes_single(&sbytes));
        let bvalue = ValueItem::Blob((Arc::new([b'A'; 545]), 545));
        let bbytes = bvalue.to_bytes();
        assert_eq!(bvalue, ValueItem::from_bytes_single(&bbytes));
        let nvalue = ValueItem::Null;
        let nbytes = nvalue.to_bytes();
        assert_eq!(nvalue, ValueItem::from_bytes_single(&nbytes));

        let junk = vec![b'A'; 10];
        assert_eq!(ValueItem::Null, ValueItem::from_bytes_single(&junk));
    }

    #[test]
    fn test_serialize_edge_values() {
        for v in [
            ValueItem::Integer(i64::MIN),
            ValueItem::Integer(i64::MAX),
            ValueItem::Integer(0),
            ValueItem::UInteger(0),
            ValueItem::UInteger(u64::MAX),
            ValueItem::Datetime(0),
            ValueItem::Datetime(u64::MAX),
            ValueItem::Str(("".into(), 0)),
            ValueItem::Blob((Arc::from(&[][..]), 0)),
        ] {
            let bytes = v.to_bytes();
            assert_eq!(
                v,
                ValueItem::from_bytes_single(&bytes),
                "roundtrip failed for {v:?}"
            );
        }

        // NaN and signed zero need bit-level comparison since NaN != NaN
        // and derived PartialEq on f64 follows IEEE 754 semantics.
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0] {
            let v = ValueItem::Double(f);
            let bytes = v.to_bytes();
            match ValueItem::from_bytes_single(&bytes) {
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
        assert_eq!(s, ValueItem::from_bytes_single(&bytes));

        let b = ValueItem::Blob((Arc::from(&b"this is way more than five bytes"[..]), 5));
        let bytes = b.to_bytes();
        assert_eq!(b, ValueItem::from_bytes_single(&bytes));
    }

    #[test]
    fn test_from_bytes_many_sequential_values_no_padding() {
        // Reserved capacity equals actual length, so to_bytes() writes no
        // padding — the simplest case, and it works today.
        let a = ValueItem::Str(("hi".into(), 2));
        let b = ValueItem::Integer(42);
        let mut bytes = a.to_bytes();
        bytes.extend(b.to_bytes());

        let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes);
        assert_eq!(parsed_a, a);
        let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]);
        assert_eq!(
            parsed_b, b,
            "second value must be recovered after the first's real index"
        );
    }

    // BUG (now fixed): the fixed-width branches (Integer/UInteger/Double/
    // Datetime) never advanced `index` past the discriminant byte, so the
    // returned index was always short by the value's own width whenever a
    // fixed-width value was anything but the last one read from a buffer.
    #[test]
    fn test_from_bytes_many_index_advances_past_fixed_width_values() {
        for a in [
            ValueItem::Integer(-7),
            ValueItem::UInteger(7),
            ValueItem::Double(1.25),
            ValueItem::Datetime(99),
        ] {
            let b = ValueItem::Str(("tail".into(), 4));
            let mut bytes = a.to_bytes();
            bytes.extend(b.to_bytes());

            let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes);
            assert_eq!(parsed_a, a);
            let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]);
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

        let (parsed_a, idx) = ValueItem::from_bytes_many(&bytes);
        assert_eq!(parsed_a, a);
        let (parsed_b, _) = ValueItem::from_bytes_many(&bytes[idx..]);
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
            ValueItem::UInteger(42),
            ValueItem::Double(1.5),
            ValueItem::Datetime(123),
            ValueItem::Str(("hello".into(), 10)),
            ValueItem::Blob((Arc::from(&b"hello"[..]), 10)),
        ] {
            assert_eq!(
                v.hash(),
                v.hash(),
                "hash must be stable across calls for {v:?}"
            );
        }
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
        assert_eq!(format!("{}", ValueItem::UInteger(5)), "5");
        assert_eq!(format!("{}", ValueItem::Double(1.5)), "1.5");
        assert_eq!(format!("{}", ValueItem::Datetime(100)), "100");
        assert_eq!(format!("{}", ValueItem::Str(("hi".into(), 10))), "hi");
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
        assert_eq!(ValueItem::UInteger(0).discriminant(), 6);
        assert_eq!(ValueItem::Double(0.0).discriminant(), 10);
        assert_eq!(ValueItem::Datetime(0).discriminant(), 15);
        assert_eq!(ValueItem::Str(("".into(), 0)).discriminant(), 20);
        assert_eq!(ValueItem::Blob((Arc::from(&[][..]), 0)).discriminant(), 25);
    }

    #[test]
    fn test_unknown_tag_decodes_as_null() {
        // Tag byte 65 ('A') matches none of the known variants (0/5/6/10/
        // 15/20/25); from_bytes_many's fallback logs and returns Null
        // instead of panicking on malformed/corrupt input.
        let junk = vec![b'A'; 10];
        assert_eq!(ValueItem::Null, ValueItem::from_bytes_single(&junk));
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
        let k = IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::UInteger(2)]).unwrap();
        assert_eq!(
            k.size(),
            size_of::<u64>() + ValueItem::Integer(1).size() + ValueItem::UInteger(2).size()
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
        assert_eq!(k, IndexKey::from_bytes(&bytes));
    }

    #[test]
    fn test_roundtrip_single_field_key() {
        let k = IndexKey::new_from(&[ValueItem::Integer(42)]).unwrap();
        let bytes = k.to_bytes();
        assert_eq!(
            k,
            IndexKey::from_bytes(&bytes),
            "a single-field (non-empty) key must round-trip through to_bytes/from_bytes"
        );
    }

    #[test]
    fn test_roundtrip_multi_field_key_mixed_types() {
        let k = IndexKey::new_from(&[
            ValueItem::Integer(-7),
            ValueItem::Str(("hello".into(), 5)), // no padding: capacity == len
            ValueItem::Double(1.25),
            ValueItem::UInteger(9),
        ])
        .unwrap();
        let bytes = k.to_bytes();
        assert_eq!(k, IndexKey::from_bytes(&bytes));
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
        assert_eq!(k, IndexKey::from_bytes(&bytes));
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

    // ValueItem::Blob's PartialOrd always panics (see valueitem_tests), so
    // any IndexKey containing a Blob field can never be ordered against
    // another key, even one holding an identical Blob. Worth knowing
    // before allowing Blob-typed columns into a multi-key index that will
    // ever need range queries or B+-tree ordering.
    #[test]
    #[should_panic(expected = "Blobs cannot be compared")]
    fn test_partial_ord_panics_when_a_field_is_blob() {
        let a = IndexKey::new_from(&[ValueItem::Blob((Arc::from(&b"x"[..]), 1))]).unwrap();
        let b = IndexKey::new_from(&[ValueItem::Blob((Arc::from(&b"x"[..]), 1))]).unwrap();
        let _ = a.partial_cmp(&b);
    }
}
