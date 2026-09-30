//! Index keys: values encoded into the store's `IndexKey`s so that key
//! order is MongoDB's value order within each type.
//!
//! Each indexed value becomes two key fields: its type bracket
//! (value::type_order) and then the value itself. The bracket first means a
//! range over one type is one contiguous run of keys — `{qty: {$gt: 5}}`
//! seeks the numbers above 5 and nothing else, as MongoDB's comparisons
//! are type-bracketed. Within a bracket:
//! - numbers are an order-preserving byte string over ints and doubles
//!   together, exact (5 and 5.0 encode the same; 2^53 + 1 stays above
//!   2^53), NaN below every other number;
//! - strings are the store's strings (bytewise order, as MongoDB's default
//!   collation); ObjectIds their 12 bytes; dates their milliseconds;
//! - documents and arrays (a whole embedded value as the key) are their
//!   serialized bytes: equal values give equal keys, but their key order is
//!   not MongoDB's, so ranges over them aren't sought through an index.

use std::sync::Arc;

use store::valueitem::ValueItem;

use crate::error::{Error, Result};
use crate::value::{Value, type_order};

/// The two key fields for one value.
pub(crate) fn encode(v: &Value) -> [ValueItem; 2] {
    let payload = match v {
        Value::Null => ValueItem::Null,
        Value::Bool(b) => ValueItem::Boolean(*b),
        Value::Int(_) | Value::Double(_) => blob(&number_bytes(v)),
        Value::String(s) => ValueItem::Str((s.clone(), s.len() as u32)),
        Value::ObjectId(o) => blob(&o.0),
        Value::Date(ms) => ValueItem::Integer(*ms),
        Value::Document(_) | Value::Array(_) => {
            blob(&postcard::to_allocvec(v).expect("values serialize"))
        }
    };
    [ValueItem::Integer(type_order(v) as i64), payload]
}

/// The bracket field alone (for a range bounded within one type).
pub(crate) fn bracket(v: &Value) -> ValueItem {
    ValueItem::Integer(type_order(v) as i64)
}

/// Whether ranges over values of this kind can be sought through an index
/// (key order agrees with MongoDB's).
pub(crate) fn orders_like_mongo(v: &Value) -> bool {
    !matches!(v, Value::Document(_) | Value::Array(_))
}

fn blob(bytes: &[u8]) -> ValueItem {
    ValueItem::Blob((Arc::from(bytes), bytes.len() as u32))
}

// 16 bytes: the number's f64 approximation in an order-preserving form,
// then how far the exact value lies from that approximation (only ever
// nonzero for an integer beyond 2^53). Rounding to f64 never reverses an
// order, and numbers equal after rounding differ exactly by that offset.
fn number_bytes(v: &Value) -> [u8; 16] {
    let (approx, offset) = match v {
        Value::Int(i) => {
            let f = *i as f64;
            (f, (*i as i128 - f as i128) as i64)
        }
        Value::Double(d) => (*d, 0),
        _ => unreachable!("numbers only"),
    };
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&sortable_f64(approx));
    out[8..].copy_from_slice(&((offset as u64) ^ (1 << 63)).to_be_bytes());
    out
}

fn sortable_f64(f: f64) -> [u8; 8] {
    if f.is_nan() {
        return [0; 8];
    }
    // -0.0 and 0.0 are the same number.
    let bits = if f == 0.0 { 0f64.to_bits() } else { f.to_bits() };
    let ordered = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    ordered.to_be_bytes()
}

/// The largest key an index or collection accepts, in serialized bytes —
/// every tree is created to hold keys up to this (see Collection).
pub(crate) const KEY_BUDGET: usize = 256;

/// Fails if `key` won't fit the trees it goes into (MongoDB, too, limits
/// index keys; an `_id` or indexed value this long is refused).
pub(crate) fn check_size(key: &store::valueitem::IndexKey) -> Result<()> {
    let size = postcard::experimental::serialized_size(&store::tuple::DBIdType::Rec(key.clone()))
        .unwrap_or(usize::MAX);
    if size > KEY_BUDGET {
        return Err(Error::KeyTooLarge(size, KEY_BUDGET));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{compare, from_json};

    fn key(v: &Value) -> store::valueitem::IndexKey {
        store::valueitem::IndexKey::new_from(&encode(v)).unwrap()
    }

    #[test]
    fn test_key_order_is_mongo_order_for_scalars() {
        let values: Vec<Value> = [
            "null", "-1e300", "-5", "-0.5", "0", "-0.0", "1", "1.0", "1.5", "9007199254740992",
            "9007199254740993", "1e300", r#""""#, r#""a""#, r#""b""#, r#""ba""#,
            r#"{"$oid":"000000000000000000000001"}"#, "false", "true", r#"{"$date":-5}"#, r#"{"$date":7}"#,
        ]
        .iter()
        .map(|j| from_json(j).unwrap())
        .collect();
        for a in &values {
            for b in &values {
                assert_eq!(
                    key(a).partial_cmp(&key(b)).unwrap(),
                    compare(a, b),
                    "{a} vs {b}"
                );
            }
        }
        assert!(key(&Value::Double(f64::NAN)) < key(&Value::Double(f64::NEG_INFINITY)));
    }

    #[test]
    fn test_oversized_keys_are_refused() {
        let long = Value::String("x".repeat(KEY_BUDGET));
        assert!(check_size(&key(&long)).is_err());
        assert!(check_size(&key(&Value::String("x".repeat(100)))).is_ok());
    }
}
