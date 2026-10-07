// Reading the postcard encoding of the store's own types in place, without
// decoding them into owned values: a page that keeps its tuples as their
// postcard bytes (SlottedPage) compares, finds and lends them straight from
// those bytes, allocating nothing.
//
// The layouts read here are the types' serde impls (Tuple's derive;
// DBIdType's, IndexKey's and ValueItem's hand-written ones) under postcard's
// rules: a u8 or bool is one byte; any other integer is a LEB128 varint,
// zigzagged if signed; an f64 is its eight little-endian bytes; a string,
// byte string or sequence is a varint length then its contents; an Option
// is one byte, 0 or 1, then the value if 1; a tuple or struct is its fields
// back to back. The tests pin each against postcard itself.

use crate::{error::StoreError, logger::LsnId, txn::TransactionId, valueitem::ValueRef};

fn malformed(what: &str) -> StoreError {
    StoreError::UnknownError(format!("malformed tuple bytes: {what}"))
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], StoreError> {
        let end = self.pos.checked_add(n).ok_or_else(|| malformed("length overflows"))?;
        let out = self.bytes.get(self.pos..end).ok_or_else(|| malformed("truncated"))?;
        self.pos = end;
        Ok(out)
    }

    fn byte(&mut self) -> Result<u8, StoreError> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64, StoreError> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            out |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err(malformed("varint too long"))
    }

    fn signed(&mut self) -> Result<i64, StoreError> {
        let n = self.varint()?;
        Ok((n >> 1) as i64 ^ -((n & 1) as i64))
    }

    fn sized(&mut self) -> Result<&'a [u8], StoreError> {
        let n = self.varint()? as usize;
        self.take(n)
    }

    fn option(&mut self) -> Result<Option<u64>, StoreError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.varint()?)),
            _ => Err(malformed("option tag")),
        }
    }

    // One ValueItem (see its Serialize: a tag, then the value). `checked`:
    // its strings are known to be UTF-8 (see WireKey::checked), so aren't
    // checked again.
    fn value(&mut self, checked: bool) -> Result<ValueRef<'a>, StoreError> {
        Ok(match self.byte()? {
            0 => ValueRef::Null,
            1 => ValueRef::Integer(self.signed()?),
            2 => ValueRef::Double(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            3 => ValueRef::Datetime(self.varint()?),
            4 => {
                let bytes = self.sized()?;
                let s = if checked {
                    // SAFETY: see WireKey::assume_checked.
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                } else {
                    std::str::from_utf8(bytes).map_err(|_| malformed("string"))?
                };
                ValueRef::Str(s, self.varint()? as u32)
            }
            5 => {
                let b = self.sized()?;
                ValueRef::Blob(b, self.varint()? as u32)
            }
            6 => match self.byte()? {
                0 => ValueRef::Boolean(false),
                1 => ValueRef::Boolean(true),
                _ => return Err(malformed("bool")),
            },
            _ => return Err(malformed("value tag")),
        })
    }

    // Steps over one ValueItem as `value` reads it, without checking a
    // string is UTF-8 (see WireKey::check).
    fn skip_value(&mut self) -> Result<(), StoreError> {
        match self.byte()? {
            0 => {}
            1 | 3 => {
                self.varint()?;
            }
            2 => {
                self.take(8)?;
            }
            4 | 5 => {
                self.sized()?;
                self.varint()?;
            }
            6 => {
                if self.byte()? > 1 {
                    return Err(malformed("bool"));
                }
            }
            _ => return Err(malformed("value tag")),
        }
        Ok(())
    }

    // A DBIdType (see its Serialize): tag 0 and a number, or tag 1 and an
    // IndexKey — a count, then that many values, stepped over (see
    // skip_value): a tuple's key is located without being read.
    fn id(&mut self) -> Result<WireId<'a>, StoreError> {
        match self.byte()? {
            0 => Ok(WireId::Int(self.varint()?)),
            1 => {
                let count = self.varint()? as usize;
                let start = self.pos;
                for _ in 0..count {
                    self.skip_value()?;
                }
                Ok(WireId::Rec(WireKey {
                    count,
                    bytes: &self.bytes[start..self.pos],
                    checked: false,
                }))
            }
            _ => Err(malformed("id tag")),
        }
    }
}

/// A tuple's id as it is encoded, checked well-formed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WireId<'a> {
    Int(u64),
    Rec(WireKey<'a>),
}

impl WireId<'_> {
    /// See WireKey::assume_checked.
    ///
    /// # Safety
    /// As WireKey::assume_checked.
    pub(crate) unsafe fn assume_checked(self) -> Self {
        match self {
            WireId::Rec(k) => WireId::Rec(unsafe { k.assume_checked() }),
            id => id,
        }
    }
}

/// An IndexKey's values as they are encoded, checked well-formed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WireKey<'a> {
    count: usize,
    bytes: &'a [u8],
    // Its strings are known to be UTF-8 (see assume_checked): reading a
    // value then doesn't check again.
    checked: bool,
}

impl<'a> WireKey<'a> {
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    /// Whether every value reads (`values` assumes so): the strings are
    /// UTF-8. Locating a key (see Reader::id) doesn't check that.
    pub(crate) fn check(&self) -> Result<(), StoreError> {
        let mut r = Reader::new(self.bytes);
        for _ in 0..self.count {
            r.value(false)?;
        }
        Ok(())
    }

    /// This key, its strings taken to be UTF-8 without checking each time
    /// a value is read — checking was a seventh of a range scan whose
    /// keys are read from bytes.
    ///
    /// # Safety
    /// Every string in the key is UTF-8: `check` said so of these bytes,
    /// or they were written from a Tuple (whose strings are Strings). A
    /// SlottedPage holds only such bytes.
    pub(crate) unsafe fn assume_checked(self) -> Self {
        Self {
            checked: true,
            ..self
        }
    }

    pub(crate) fn values(&self) -> WireValues<'a> {
        WireValues {
            reader: Reader::new(self.bytes),
            left: self.count,
            checked: self.checked,
        }
    }
}

/// See WireKey::values.
pub(crate) struct WireValues<'a> {
    reader: Reader<'a>,
    left: usize,
    checked: bool,
}

impl<'a> Iterator for WireValues<'a> {
    type Item = ValueRef<'a>;

    fn next(&mut self) -> Option<ValueRef<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        // A page checks its keys when it reads them in (WireKey::check),
        // so this can't fail there; if it somehow did, the key just ends.
        match self.reader.value(self.checked) {
            Ok(v) => Some(v),
            Err(_) => {
                self.left = 0;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for WireValues<'_> {}

/// A tuple as it is encoded (Tuple's derived Serialize: id, txn_id,
/// pre_lsn, data, flags), every field located and checked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WireTuple<'a> {
    pub(crate) id: WireId<'a>,
    pub(crate) txn_id: Option<TransactionId>,
    pub(crate) pre_lsn: Option<LsnId>,
    pub(crate) data: &'a [u8],
    pub(crate) flags: u8,
}

impl<'a> WireTuple<'a> {
    /// See WireKey::assume_checked.
    ///
    /// # Safety
    /// As WireKey::assume_checked, of this tuple's id.
    pub(crate) unsafe fn assume_checked(self) -> Self {
        Self {
            id: unsafe { self.id.assume_checked() },
            ..self
        }
    }

    pub(crate) fn read(bytes: &'a [u8]) -> Result<Self, StoreError> {
        Self::read_after(bytes, id_end(bytes)?)
    }

    /// `read`, told where the id ends (see id_end) — it then doesn't step
    /// over the id's key to find the rest.
    pub(crate) fn read_after(bytes: &'a [u8], id_end: usize) -> Result<Self, StoreError> {
        let id = tuple_id_ending(bytes, id_end)?;
        let mut r = Reader::new(bytes);
        r.pos = id_end;
        Ok(Self {
            id,
            txn_id: r.option()?.map(TransactionId),
            pre_lsn: r.option()?.map(LsnId),
            data: r.sized()?,
            flags: r.byte()?,
        })
    }
}

/// Just the id of an encoded tuple — its first field.
pub(crate) fn tuple_id(bytes: &[u8]) -> Result<WireId<'_>, StoreError> {
    Reader::new(bytes).id()
}

/// Where an encoded tuple's id ends: what a reader that keeps it can
/// hand tuple_id_ending / WireTuple::read_after.
pub(crate) fn id_end(bytes: &[u8]) -> Result<usize, StoreError> {
    let mut r = Reader::new(bytes);
    r.id()?;
    Ok(r.pos)
}

/// tuple_id, told where the id ends (see id_end): a key's values are
/// then not stepped over.
pub(crate) fn tuple_id_ending(bytes: &[u8], id_end: usize) -> Result<WireId<'_>, StoreError> {
    let mut r = Reader::new(bytes);
    match r.byte()? {
        0 => Ok(WireId::Int(r.varint()?)),
        1 => {
            let count = r.varint()? as usize;
            let bytes = bytes.get(r.pos..id_end).ok_or_else(|| malformed("id end"))?;
            Ok(WireId::Rec(WireKey {
                count,
                bytes,
                checked: false,
            }))
        }
        _ => Err(malformed("id tag")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        tuple::{DBIdType, Tuple},
        valueitem::{IndexKey, ValueItem},
    };

    fn one_of_each() -> Vec<ValueItem> {
        vec![
            ValueItem::Null,
            ValueItem::Integer(-7),
            ValueItem::Integer(i64::MIN),
            ValueItem::Integer(300),
            ValueItem::Double(-2.5),
            ValueItem::Datetime(u64::MAX),
            ValueItem::Str(("héllo".to_owned(), 20)),
            ValueItem::Blob((Arc::from(&b"\x00\x01"[..]), 4)),
            ValueItem::Boolean(true),
            ValueItem::Boolean(false),
        ]
    }

    #[test]
    fn test_reads_what_postcard_writes() {
        let key = IndexKey::new_from(&one_of_each()).unwrap();
        let mut t = Tuple::new_with(DBIdType::Rec(key), b"payload", None, None);
        t.set_txn_id(TransactionId(1 << 40));
        t.set_pre_lsn(LsnId(300));
        t.tombstone();
        let bytes = postcard::to_allocvec(&t).unwrap();
        let w = WireTuple::read(&bytes).unwrap();
        let WireId::Rec(k) = w.id else {
            panic!("{:?}", w.id)
        };
        assert_eq!(k.len(), one_of_each().len());
        let values: Vec<ValueItem> = k.values().map(|v| v.to_owned()).collect();
        assert_eq!(values, one_of_each());
        // Capacity comes through too, not just content.
        assert!(matches!(k.values().nth(6), Some(ValueRef::Str("héllo", 20))));
        assert_eq!(w.txn_id, Some(TransactionId(1 << 40)));
        assert_eq!(w.pre_lsn, Some(LsnId(300)));
        assert_eq!(w.data, b"payload");
        assert_eq!(w.flags, t.flags);

        let end = id_end(&bytes).unwrap();
        let after = WireTuple::read_after(&bytes, end).unwrap();
        assert_eq!((after.data, after.flags, after.txn_id), (w.data, w.flags, w.txn_id));
        let WireId::Rec(k2) = tuple_id_ending(&bytes, end).unwrap() else {
            panic!()
        };
        assert_eq!(k2.values().map(|v| v.to_owned()).collect::<Vec<_>>(), one_of_each());

        let t = Tuple::new(u64::MAX, b"");
        let bytes = postcard::to_allocvec(&t).unwrap();
        let w = WireTuple::read(&bytes).unwrap();
        assert!(matches!(w.id, WireId::Int(u64::MAX)));
        assert_eq!((w.txn_id, w.pre_lsn, w.data), (None, None, &b""[..]));
        assert!(matches!(tuple_id(&bytes).unwrap(), WireId::Int(u64::MAX)));
    }

    // Cut short anywhere: an error, never a panic.
    // A key whose string isn't UTF-8 is located all the same, and its
    // check says so.
    #[test]
    fn test_a_key_is_located_without_being_checked() {
        let key = IndexKey::new_from(&[ValueItem::Str(("ab".to_owned(), 2))]).unwrap();
        let mut bytes =
            postcard::to_allocvec(&Tuple::new_with(DBIdType::Rec(key), b"", None, None)).unwrap();
        let at = bytes.windows(2).position(|w| w == b"ab").unwrap();
        bytes[at] = 0xFF;
        let w = WireTuple::read(&bytes).unwrap();
        let WireId::Rec(k) = w.id else { panic!() };
        assert!(k.check().is_err());
    }

    #[test]
    fn test_a_truncated_tuple_is_an_error() {
        let key = IndexKey::new_from(&one_of_each()).unwrap();
        let bytes =
            postcard::to_allocvec(&Tuple::new_with(DBIdType::Rec(key), b"xy", None, None)).unwrap();
        for cut in 0..bytes.len() {
            assert!(WireTuple::read(&bytes[..cut]).is_err(), "cut at {cut}");
        }
    }
}
