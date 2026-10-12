//! Keys as bytes that order as the keys do: comparing two encoded keys
//! with a plain byte comparison gives the order of the keys themselves, so
//! a page search, a sort or a merge never has to decode one.
//!
//! A key is its kind, then (for a record key) its fields one after
//! another. Every field ends where its own bytes say it does — no count in
//! front, no terminator behind — so the bytes of a key begin every longer
//! key that begins with the same fields, and a shorter key sorts before
//! its extensions. The keys that begin with a key `p`'s fields are exactly
//! the bytes from `p` up to (not including) `p` then FF (see `has_prefix`,
//! `prefix_end`): what follows `p` in such a key is a field's tag, and FF
//! is no tag.
//!
//! ```text
//! record key   01, then its fields:
//!   NULL       02
//!   false      03
//!   true       04
//!   integer    10..=20   18 is zero; 18+n, then n bytes: a positive of n
//!                        significant bytes, big-endian; 18-n, then n
//!                        bytes: a negative whose magnitude has n, as
//!                        (2^8n - 1 - magnitude)
//!   double     21, then 8 bytes: the bits, big-endian, sign flipped for a
//!              non-negative and all flipped for a negative (the order
//!              f64::total_cmp gives, -0.0 below +0.0, NaNs at the ends)
//!   datetime   30..=38   30+n, then its n significant bytes, big-endian
//!   string     40, the bytes with every 00 written 00 FF, then 00
//!   blob       41, likewise
//! row id       E0..=E8   E0+n, then its n significant bytes, big-endian
//! ```
//!
//! The tags rise as ValueItem's type ranks do (Null, Boolean, Integer,
//! Double, Datetime, Str, Blob), so values of different types in one
//! position order as ValueItem::cmp orders them. A string's or blob's
//! reserved capacity is not part of its key (it never took part in
//! comparing or equality): one decoded comes back with a capacity of its
//! length. FF is no tag, which is what lets a string end in a single 00:
//! after it comes a tag or nothing, both below the FF of an escaped 00 —
//! and why "starts with `p`'s bytes" alone is not "begins with `p`'s
//! fields": the empty string's bytes begin those of a string that starts
//! with a 00.
//!
//! This is DBIdType's own order (see its Ord): a key that is a prefix of
//! another sorts before it, and every record key sorts below every row id
//! — below Int(MAX) above all, the last entry of an inner page.
use std::borrow::Cow;

use crate::{
    error::StoreError,
    tuple::DBIdType,
    valueitem::{IndexKey, ValueItem},
};

const REC: u8 = 0x01;
const NULL: u8 = 0x02;
const FALSE: u8 = 0x03;
const TRUE: u8 = 0x04;
const INT_ZERO: u8 = 0x18;
const DOUBLE: u8 = 0x21;
const DATETIME: u8 = 0x30;
const STR: u8 = 0x40;
const BLOB: u8 = 0x41;
const ROW_ID: u8 = 0xE0;

const END: u8 = 0x00;
const ESCAPED: u8 = 0xFF;

/// Appends `id`'s bytes to `out`.
pub fn encode(id: &DBIdType, out: &mut Vec<u8>) {
    match id {
        DBIdType::Int(n) => put_unsigned(out, ROW_ID, *n),
        DBIdType::Rec(key) => encode_record(key.values(), out),
    }
}

/// Appends a record key of `fields` to `out`: also the bytes every key
/// beginning with those fields begins with.
pub fn encode_record(fields: &[ValueItem], out: &mut Vec<u8>) {
    out.push(REC);
    for field in fields {
        encode_field(field, out);
    }
}

/// Appends one more field to a record key in `out`.
pub fn encode_field(field: &ValueItem, out: &mut Vec<u8>) {
    match field {
        ValueItem::Null => out.push(NULL),
        ValueItem::Boolean(b) => out.push(if *b { TRUE } else { FALSE }),
        ValueItem::Integer(i) => put_integer(out, *i),
        ValueItem::Double(d) => {
            out.push(DOUBLE);
            let bits = d.to_bits();
            let ordered = if bits >> 63 == 0 { bits ^ (1 << 63) } else { !bits };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        ValueItem::Datetime(t) => put_unsigned(out, DATETIME, *t),
        ValueItem::Str((s, _)) => put_bytes(out, STR, s.as_bytes()),
        ValueItem::Blob((b, _)) => put_bytes(out, BLOB, b),
    }
}

// `v`'s significant bytes, big-endian, after a tag of `base` plus how many.
fn put_unsigned(out: &mut Vec<u8>, base: u8, v: u64) {
    let n = 8 - (v.leading_zeros() / 8) as usize;
    out.push(base + n as u8);
    out.extend_from_slice(&v.to_be_bytes()[8 - n..]);
}

fn put_integer(out: &mut Vec<u8>, v: i64) {
    if v >= 0 {
        return put_unsigned(out, INT_ZERO, v as u64);
    }
    let magnitude = v.unsigned_abs();
    let n = 8 - (magnitude.leading_zeros() / 8) as usize;
    out.push(INT_ZERO - n as u8);
    out.extend_from_slice(&(!magnitude).to_be_bytes()[8 - n..]);
}

fn put_bytes(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    let mut rest = bytes;
    while let Some(zero) = rest.iter().position(|b| *b == END) {
        out.extend_from_slice(&rest[..=zero]);
        out.push(ESCAPED);
        rest = &rest[zero + 1..];
    }
    out.extend_from_slice(rest);
    out.push(END);
}

/// Whether the key in `key` begins with the fields of the record key in
/// `prefix` (itself included).
pub fn has_prefix(key: &[u8], prefix: &[u8]) -> bool {
    key.starts_with(prefix) && key.get(prefix.len()) != Some(&ESCAPED)
}

/// Appends to `out` the first bytes past every key that begins with the
/// fields of the record key in `prefix`: those keys are the ones from
/// `prefix` up to, not including, this.
pub fn prefix_end(prefix: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(prefix);
    out.push(ESCAPED);
}

/// One field of a record key, read from its bytes. A string or blob is
/// lent from them unless it holds a 00 (which they hold escaped).
#[derive(Debug, Clone, PartialEq)]
pub enum Field<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Datetime(u64),
    Str(Cow<'a, str>),
    Blob(Cow<'a, [u8]>),
}

impl Field<'_> {
    /// The value, a string or blob with a capacity of its length.
    pub fn to_owned(&self) -> ValueItem {
        match self {
            Field::Null => ValueItem::Null,
            Field::Boolean(b) => ValueItem::Boolean(*b),
            Field::Integer(i) => ValueItem::Integer(*i),
            Field::Double(d) => ValueItem::Double(*d),
            Field::Datetime(t) => ValueItem::Datetime(*t),
            Field::Str(s) => ValueItem::Str((s.to_string(), s.len() as u32)),
            Field::Blob(b) => ValueItem::Blob((b.as_ref().into(), b.len() as u32)),
        }
    }
}

/// What a key's bytes hold.
#[derive(Debug)]
pub enum Key<'a> {
    RowId(u64),
    Record(Fields<'a>),
}

/// The key in `bytes`, its fields (if a record's) still to be read.
pub fn read(bytes: &[u8]) -> Result<Key<'_>, StoreError> {
    match bytes.split_first() {
        Some((&REC, rest)) => Ok(Key::Record(Fields { rest })),
        Some((&tag, rest)) if (ROW_ID..=ROW_ID + 8).contains(&tag) => {
            let (n, rest) = take_unsigned(rest, (tag - ROW_ID) as usize)?;
            if !rest.is_empty() {
                return Err(damaged("bytes after a row id"));
            }
            Ok(Key::RowId(n))
        }
        _ => Err(damaged("no key kind")),
    }
}

/// The key in `bytes`, owned.
pub fn decode(bytes: &[u8]) -> Result<DBIdType, StoreError> {
    match read(bytes)? {
        Key::RowId(n) => Ok(DBIdType::Int(n)),
        Key::Record(fields) => {
            let values = fields
                .map(|f| f.map(|f| f.to_owned()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DBIdType::Rec(IndexKey::new_from_owned(values)?))
        }
    }
}

/// A record key's fields, in order, read from its bytes.
#[derive(Debug, Clone)]
pub struct Fields<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<Field<'a>, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (&tag, rest) = self.rest.split_first()?;
        let field = read_field(tag, rest);
        match field {
            Ok((field, rest)) => {
                self.rest = rest;
                Some(Ok(field))
            }
            Err(e) => {
                self.rest = &[];
                Some(Err(e))
            }
        }
    }
}

fn read_field(tag: u8, rest: &[u8]) -> Result<(Field<'_>, &[u8]), StoreError> {
    Ok(match tag {
        NULL => (Field::Null, rest),
        FALSE => (Field::Boolean(false), rest),
        TRUE => (Field::Boolean(true), rest),
        DOUBLE => {
            let (ordered, rest) = take_unsigned(rest, 8)?;
            let bits = if ordered >> 63 == 1 { ordered ^ (1 << 63) } else { !ordered };
            (Field::Double(f64::from_bits(bits)), rest)
        }
        STR => {
            let (bytes, rest) = take_bytes(rest)?;
            let s = match bytes {
                Cow::Borrowed(b) => Cow::Borrowed(std::str::from_utf8(b).map_err(|_| damaged("a string not UTF-8"))?),
                Cow::Owned(b) => Cow::Owned(String::from_utf8(b).map_err(|_| damaged("a string not UTF-8"))?),
            };
            (Field::Str(s), rest)
        }
        BLOB => {
            let (bytes, rest) = take_bytes(rest)?;
            (Field::Blob(bytes), rest)
        }
        _ if (INT_ZERO..=INT_ZERO + 8).contains(&tag) => {
            let (v, rest) = take_unsigned(rest, (tag - INT_ZERO) as usize)?;
            let v = i64::try_from(v).map_err(|_| damaged("an integer past i64"))?;
            (Field::Integer(v), rest)
        }
        _ if (INT_ZERO - 8..INT_ZERO).contains(&tag) => {
            let n = (INT_ZERO - tag) as usize;
            let (flipped, rest) = take_unsigned(rest, n)?;
            // The n bytes hold 2^8n - 1 - magnitude.
            let mask = if n == 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
            let magnitude = !flipped & mask;
            if magnitude > 1 << 63 {
                return Err(damaged("an integer below i64"));
            }
            (Field::Integer((magnitude as i64).wrapping_neg()), rest)
        }
        _ if (DATETIME..=DATETIME + 8).contains(&tag) => {
            let (t, rest) = take_unsigned(rest, (tag - DATETIME) as usize)?;
            (Field::Datetime(t), rest)
        }
        _ => return Err(damaged("an unknown field tag")),
    })
}

// A number of `n` big-endian bytes, and what follows it.
fn take_unsigned(bytes: &[u8], n: usize) -> Result<(u64, &[u8]), StoreError> {
    let (head, rest) = bytes.split_at_checked(n).ok_or_else(|| damaged("a number cut short"))?;
    let mut word = [0u8; 8];
    word[8 - n..].copy_from_slice(head);
    Ok((u64::from_be_bytes(word), rest))
}

// A string's or blob's bytes up to its END, and what follows that.
fn take_bytes(bytes: &[u8]) -> Result<(Cow<'_, [u8]>, &[u8]), StoreError> {
    let cut_short = || damaged("a string or blob with no end");
    let first = bytes.iter().position(|b| *b == END).ok_or_else(cut_short)?;
    if bytes.get(first + 1) != Some(&ESCAPED) {
        return Ok((Cow::Borrowed(&bytes[..first]), &bytes[first + 1..]));
    }
    // It holds a 00: copied out, each 00 FF back to 00.
    let mut out = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        let zero = rest.iter().position(|b| *b == END).ok_or_else(cut_short)?;
        out.extend_from_slice(&rest[..zero]);
        if rest.get(zero + 1) != Some(&ESCAPED) {
            return Ok((Cow::Owned(out), &rest[zero + 1..]));
        }
        out.push(END);
        rest = &rest[zero + 2..];
    }
}

fn damaged(what: &str) -> StoreError {
    StoreError::Corruption(format!("key bytes: {what}"))
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use super::*;

    fn s(v: &str) -> ValueItem {
        ValueItem::Str((v.to_string(), 64))
    }

    fn b(v: &[u8]) -> ValueItem {
        ValueItem::Blob((Arc::from(v), 64))
    }

    // Values of every type, with the ones an encoding gets wrong: the
    // extremes, each byte-length boundary, both zeros, NaNs of both signs,
    // strings that are prefixes of each other and that hold 00 and FF.
    fn pool() -> Vec<ValueItem> {
        let mut v = vec![ValueItem::Null, ValueItem::Boolean(false), ValueItem::Boolean(true)];
        let mut ints = vec![0i64, 1, -1, i64::MAX, i64::MIN, i64::MIN + 1, 42, -42];
        for bytes in 1..8 {
            let edge = 1i64 << (8 * bytes);
            ints.extend([edge - 1, edge, edge + 1, -edge + 1, -edge, -edge - 1]);
        }
        v.extend(ints.into_iter().map(ValueItem::Integer));
        v.extend(
            [
                0.0,
                -0.0,
                1.0,
                -1.0,
                1.5,
                -1.5,
                f64::MIN_POSITIVE,
                -f64::MIN_POSITIVE,
                f64::MAX,
                f64::MIN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
                -f64::NAN,
                f64::EPSILON,
                1e300,
                -1e300,
            ]
            .map(ValueItem::Double),
        );
        let mut times = vec![0u64, 1, u64::MAX];
        for bytes in 1..8 {
            let edge = 1u64 << (8 * bytes);
            times.extend([edge - 1, edge, edge + 1]);
        }
        v.extend(times.into_iter().map(ValueItem::Datetime));
        for text in ["", "a", "aa", "ab", "b", "a\0", "a\0\0", "a\0b", "a\u{1}", "\0", "\0\0", "é", "zz\u{10FFFF}"] {
            v.push(s(text));
        }
        for bytes in [&[][..], &[0], &[0, 0], &[0, 255], &[0, 255, 0], &[1], &[255], &[255, 255], &[97], &[97, 0], &[97, 0, 255]] {
            v.push(b(bytes));
        }
        v
    }

    fn rec(fields: &[ValueItem]) -> DBIdType {
        DBIdType::Rec(IndexKey::new_from(fields).unwrap())
    }

    fn bytes_of(id: &DBIdType) -> Vec<u8> {
        let mut out = vec![];
        encode(id, &mut out);
        out
    }

    // Bit-for-bit: a NaN is not == itself, and the two zeros are.
    fn same(a: &DBIdType, b: &DBIdType) -> bool {
        match (a, b) {
            (DBIdType::Int(a), DBIdType::Int(b)) => a == b,
            (DBIdType::Rec(a), DBIdType::Rec(b)) => {
                a.values().len() == b.values().len()
                    && a.values().iter().zip(b.values()).all(|pair| match pair {
                        (ValueItem::Double(a), ValueItem::Double(b)) => a.to_bits() == b.to_bits(),
                        (ValueItem::Str((a, _)), ValueItem::Str((b, _))) => a == b,
                        (ValueItem::Blob((a, _)), ValueItem::Blob((b, _))) => a == b,
                        (a, b) => a == b,
                    })
            }
            _ => false,
        }
    }

    #[test]
    fn test_every_key_reads_back() {
        let pool = pool();
        for a in &pool {
            let id = rec(std::slice::from_ref(a));
            assert!(same(&decode(&bytes_of(&id)).unwrap(), &id), "{a:?}");
            for b in &pool {
                let id = rec(&[a.clone(), b.clone(), a.clone()]);
                assert!(same(&decode(&bytes_of(&id)).unwrap(), &id), "{a:?}, {b:?}");
            }
        }
        for n in [0u64, 1, 255, 256, u64::MAX - 1, u64::MAX] {
            assert!(same(&decode(&bytes_of(&DBIdType::Int(n))).unwrap(), &DBIdType::Int(n)));
        }
        assert!(same(&decode(&bytes_of(&rec(&[]))).unwrap(), &rec(&[])));
    }

    // A string or blob comes back with a capacity of its length, whatever
    // it was given: the capacity is not in the key.
    #[test]
    fn test_capacity_is_not_part_of_a_key() {
        let wide = rec(&[ValueItem::Str(("ab".into(), 200))]);
        let narrow = rec(&[ValueItem::Str(("ab".into(), 2))]);
        assert_eq!(bytes_of(&wide), bytes_of(&narrow));
        let DBIdType::Rec(key) = decode(&bytes_of(&wide)).unwrap() else {
            panic!("a record key");
        };
        assert_eq!(key.values()[0], ValueItem::Str(("ab".into(), 2)));
        assert!(matches!(&key.values()[0], ValueItem::Str((_, 2))));
    }

    // The point of it: the bytes of two keys compare as the keys do —
    // every pair of one field, of two with either leading, of different
    // lengths, and with a row id on either side.
    #[test]
    fn test_bytes_order_as_the_keys_do() {
        let pool = pool();
        let check = |x: &DBIdType, y: &DBIdType| {
            assert_eq!(bytes_of(x).cmp(&bytes_of(y)), x.cmp(y), "{x:?} against {y:?}");
            // And equal bytes are equal keys, bit for bit.
            assert_eq!(bytes_of(x) == bytes_of(y), same(x, y), "{x:?} against {y:?}");
        };
        let ids = [0u64, 1, 300, u64::MAX];
        for a in &pool {
            for b in &pool {
                check(&rec(std::slice::from_ref(a)), &rec(std::slice::from_ref(b)));
                // A key against a longer one, beginning with it or not.
                check(&rec(std::slice::from_ref(a)), &rec(&[b.clone(), a.clone()]));
                check(&rec(&[a.clone(), b.clone()]), &rec(std::slice::from_ref(b)));
                for c in pool.iter().step_by(7) {
                    check(&rec(&[a.clone(), c.clone()]), &rec(&[b.clone(), c.clone()]));
                    check(&rec(&[c.clone(), a.clone()]), &rec(&[c.clone(), b.clone()]));
                    // The first field decides before the second is looked at.
                    check(&rec(&[a.clone(), c.clone()]), &rec(&[b.clone(), a.clone()]));
                }
            }
            for n in ids {
                check(&rec(std::slice::from_ref(a)), &DBIdType::Int(n));
                check(&DBIdType::Int(n), &rec(&[a.clone(), a.clone()]));
            }
            check(&rec(&[]), &rec(std::slice::from_ref(a)));
        }
        for a in ids {
            for b in ids {
                check(&DBIdType::Int(a), &DBIdType::Int(b));
            }
        }
    }

    #[test]
    fn test_row_ids_order_by_value_above_every_record_key() {
        let ids = [0u64, 1, 2, 255, 256, 257, 65_535, 65_536, 1 << 40, u64::MAX - 1, u64::MAX];
        for a in ids {
            for b in ids {
                assert_eq!(
                    bytes_of(&DBIdType::Int(a)).cmp(&bytes_of(&DBIdType::Int(b))),
                    a.cmp(&b),
                    "{a} against {b}"
                );
            }
        }
        // Int(MAX) is the last entry of an inner page: above everything.
        let top = bytes_of(&DBIdType::Int(u64::MAX));
        for a in &pool() {
            assert!(bytes_of(&rec(&[a.clone(), a.clone()])) < bytes_of(&DBIdType::Int(0)));
            assert!(bytes_of(&rec(std::slice::from_ref(a))) < top);
        }
    }

    // A key's bytes begin every longer key that begins with its fields, and
    // sort before them; has_prefix says which keys those are, and they are
    // the ones from the key up to its prefix_end.
    #[test]
    fn test_a_key_is_a_prefix_of_exactly_its_extensions() {
        let pool = pool();
        for a in &pool {
            let short = bytes_of(&rec(std::slice::from_ref(a)));
            let mut end = vec![];
            prefix_end(&short, &mut end);
            assert!(has_prefix(&short, &short));
            for b in &pool {
                let long = bytes_of(&rec(&[a.clone(), b.clone()]));
                assert!(long.starts_with(&short), "{a:?} then {b:?}");
                assert!(has_prefix(&long, &short));
                assert!(short < long && long < end);
                let other = bytes_of(&rec(&[b.clone(), a.clone()]));
                let same_first =
                    rec(std::slice::from_ref(a)).cmp(&rec(std::slice::from_ref(b))) == Ordering::Equal;
                assert_eq!(has_prefix(&other, &short), same_first, "{b:?} is not {a:?}");
                assert_eq!(short <= other && other < end, same_first, "{b:?} is not {a:?}");
            }
        }
        assert!(bytes_of(&rec(&[])) < bytes_of(&rec(&[ValueItem::Null])));
        // Every record key begins with the empty one.
        assert!(has_prefix(&bytes_of(&rec(&[ValueItem::Null])), &bytes_of(&rec(&[]))));
        // The empty string's bytes begin those of "\0", which doesn't begin
        // with its fields.
        assert!(bytes_of(&rec(&[s("\0")])).starts_with(&bytes_of(&rec(&[s("")]))));
    }

    // What makes strings hard: a prefix sorts first, a 00 inside is not an
    // end, and what follows a string can't be taken for more of it.
    #[test]
    fn test_strings_that_hold_zeros_and_prefixes() {
        let order = ["", "\0", "\0\0", "\0a", "a", "a\0", "a\0\0", "a\0b", "a\u{1}", "aa", "b"];
        for pair in order.windows(2) {
            assert!(bytes_of(&rec(&[s(pair[0])])) < bytes_of(&rec(&[s(pair[1])])), "{pair:?}");
            // With a field after: the string's end still decides.
            let after = ValueItem::Integer(i64::MAX);
            assert!(
                bytes_of(&rec(&[s(pair[0]), after.clone()])) < bytes_of(&rec(&[s(pair[1]), ValueItem::Null])),
                "{pair:?}"
            );
        }
        // A string without a 00 is lent, not copied.
        let bytes = bytes_of(&rec(&[s("plain"), s("with\0zero")]));
        let Key::Record(mut fields) = read(&bytes).unwrap() else {
            panic!("a record key");
        };
        assert!(matches!(fields.next().unwrap().unwrap(), Field::Str(Cow::Borrowed("plain"))));
        assert!(matches!(fields.next().unwrap().unwrap(), Field::Str(Cow::Owned(s)) if s == "with\0zero"));
        assert!(fields.next().is_none());
    }

    #[test]
    fn test_small_numbers_take_few_bytes() {
        let len = |v: ValueItem| bytes_of(&rec(&[v])).len() - 1;
        assert_eq!(len(ValueItem::Integer(0)), 1);
        assert_eq!(len(ValueItem::Integer(200)), 2);
        assert_eq!(len(ValueItem::Integer(-200)), 2);
        assert_eq!(len(ValueItem::Integer(100_000)), 4);
        assert_eq!(len(ValueItem::Integer(i64::MIN)), 9);
        assert_eq!(len(ValueItem::Datetime(1_700_000_000_000)), 7);
        assert_eq!(len(ValueItem::Boolean(true)), 1);
        assert_eq!(len(ValueItem::Double(1.0)), 9);
        assert_eq!(len(s("abc")), 5);
        assert_eq!(bytes_of(&DBIdType::Int(500_000)).len(), 4);
    }

    // Cut short anywhere, or of bytes that are no key: an error, never a
    // panic.
    #[test]
    fn test_damaged_bytes_are_an_error() {
        let pool = pool();
        for a in pool.iter().step_by(3) {
            let bytes = bytes_of(&rec(&[a.clone(), s("tail\0"), a.clone()]));
            for cut in 0..bytes.len() {
                let _ = decode(&bytes[..cut]);
            }
        }
        assert!(decode(&[]).is_err());
        assert!(decode(&[0x00]).is_err());
        assert!(decode(&[0xFF]).is_err());
        assert!(decode(&[REC, 0x05]).is_err(), "an unknown tag");
        assert!(decode(&[REC, STR, b'a']).is_err(), "a string with no end");
        assert!(decode(&[REC, STR, 0xC3, END]).is_err(), "not UTF-8");
        assert!(decode(&[REC, DOUBLE, 1, 2, 3]).is_err());
        assert!(decode(&[ROW_ID + 2, 1]).is_err());
        assert!(decode(&[ROW_ID + 1, 1, 9]).is_err(), "bytes after a row id");
        assert!(decode(&[REC, INT_ZERO + 8, 0xFF, 0, 0, 0, 0, 0, 0, 0]).is_err(), "past i64");
    }
}
