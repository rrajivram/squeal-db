use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::Bound;

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

// A page's tuples, sorted by id (DBIdType's own Ord), in one Vec: a lookup
// is a binary search over contiguous entries, and a caller reading entries
// in order can say where it expects the next one (`get_hinted`) and skip
// the search. Was a BTreeMap<DBIdType, Vec<Tuple>>, whose lookups compared
// about twice as many keys and chased a pointer per node — and for a row
// table keyed by IndexKey, each comparison is a memcmp: that lookup was
// over half of a range scan's time in store.
//
// The order is what the B+ tree's navigation/split logic treats as "sorted
// by DBIdType::cmp" (see find_page/insert_recursive in bplustree.rs), so
// keying by the id itself, not a separately-derived hash, keeps the two in
// agreement by construction. DBIdType::cmp can say `Equal` for ids that
// are `PartialEq`-distinct (hash collisions for Int; IndexKey's own
// documented ties — same content, different reserved capacity, or a
// strict field-wise prefix — for Rec): such ties sit next to each other in
// arrival order, and every lookup picks among them by `PartialEq`.
//
// A binary search jumps around the page, and comparing an IndexKey
// follows two pointers (the key's fields, then a string's bytes) — a cache
// miss or three per probe once the page isn't hot, which made it slower
// than the BTreeMap's walk over neighbouring keys. So each entry also has
// its key's leading bits inline (`prefixes`, see `order_prefix`): most
// probes compare those and never touch the key itself.
//
// The wire form is the tuples in this order, exactly as the BTreeMap's
// flattened values were, so pages already on disk read back unchanged.
#[derive(Debug, Default, Clone)]
pub struct AnyTuplePage {
    data: Vec<Tuple>,
    // order_prefix of each entry's id, in step with `data`.
    prefixes: Vec<u128>,
}

impl PartialEq for AnyTuplePage {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Serialize for AnyTuplePage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AnyTuplePage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_sorted(Vec::<Tuple>::deserialize(deserializer)?))
    }
}

// Leading bits of an id that order the same way it does, wherever they
// differ: if two ids' prefixes are both known (non-zero tag), of the same
// kind, and unequal, the ids compare the same way their prefixes do. Equal
// prefixes say nothing — compare the ids. Bits 127..126 are the kind: 0 no
// prefix (always compare the ids), 1 Int, 2 Rec; an Int and a Rec order by
// hash (DBIdType::cmp), so they compare by id too.
//
// Int: its own value, which is what it orders by. Rec: its first field
// only — IndexKey compares field by field, so a difference there decides
// the order; later fields would not be safe (a shorter key ties with any
// longer one it starts). The field is its type rank (3 bits), then the
// value, left-aligned in the remaining 120 bits: an integer or timestamp
// as an unsigned big-endian number (sign bit flipped), a double by
// total_cmp's bit trick, a string or blob as its first 15 bytes, zero
// padded — a shorter string padded with zeros is never above a longer one
// it starts, so truncating and padding can only turn a difference into a
// tie, never reverse it. A key with no fields ties with every key: no
// prefix.
pub(crate) fn order_prefix(id: &DBIdType) -> u128 {
    use crate::valueitem::ValueItem;
    const INT: u128 = 1 << 126;
    const REC: u128 = 2 << 126;
    let rec = match id {
        DBIdType::Int(i) => return INT | *i as u128,
        DBIdType::Rec(k) => k,
    };
    let Some(first) = rec.values().first() else {
        return 0;
    };
    let ordered = |n: u64| (n as u128) << 56;
    let bytes = |b: &[u8]| {
        let mut out = [0u8; 16];
        let n = b.len().min(15);
        out[1..1 + n].copy_from_slice(&b[..n]);
        u128::from_be_bytes(out)
    };
    let value = match first {
        ValueItem::Null => 0,
        ValueItem::Boolean(b) => ordered(*b as u64),
        ValueItem::Integer(i) => ordered(*i as u64 ^ (1 << 63)),
        ValueItem::Double(d) => {
            let bits = d.to_bits() as i64;
            let total = bits ^ (((bits >> 63) as u64) >> 1) as i64;
            ordered(total as u64 ^ (1 << 63))
        }
        ValueItem::Datetime(t) => ordered(*t),
        ValueItem::Str((s, _)) => bytes(s.as_bytes()),
        ValueItem::Blob((b, _)) => bytes(b),
    };
    REC | (first.type_rank() as u128) << 123 | value
}

impl AnyTuplePage {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<AnyTuplePage, StoreError> {
        Ok(Self::from_sorted(from_bytes(bytes)?))
    }

    fn from_sorted(mut data: Vec<Tuple>) -> Self {
        // Written in key order; sorted anyway (stable, so ties keep their
        // order) rather than trusting a page whose order is wrong, which a
        // binary search would silently misread.
        if !data.is_sorted_by(|a, b| a.id <= b.id) {
            data.sort_by(|a, b| a.id.cmp(&b.id));
        }
        let prefixes = data.iter().map(|t| order_prefix(&t.id)).collect();
        Self { data, prefixes }
    }

    // Entry `i` against `id`, whose order_prefix is `prefix`.
    #[inline(always)]
    fn cmp_at(&self, i: usize, id: &DBIdType, prefix: u128) -> Ordering {
        let mine = self.prefixes[i];
        if mine != prefix && mine >> 126 == prefix >> 126 && prefix >> 126 != 0 {
            return mine.cmp(&prefix);
        }
        self.data[i].id.cmp(id)
    }

    // The first entry for which `past` holds; it holds for every one after.
    #[inline(always)]
    fn search(&self, id: &DBIdType, past: impl Fn(Ordering) -> bool) -> usize {
        let prefix = order_prefix(id);
        let (mut lo, mut hi) = (0, self.data.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if past(self.cmp_at(mid, id, prefix)) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo
    }

    // The first entry not below `id`.
    fn lower_bound(&self, id: &DBIdType) -> usize {
        self.search(id, |o| o != Ordering::Less)
    }

    // The first entry above `id`.
    fn upper_bound(&self, id: &DBIdType) -> usize {
        self.search(id, |o| o == Ordering::Greater)
    }

    // Where the entry with exactly this id is.
    fn position(&self, id: &DBIdType) -> Option<usize> {
        let from = self.lower_bound(id);
        self.data[from..]
            .iter()
            .take_while(|t| t.id.cmp(id) == Ordering::Equal)
            .position(|t| t.id == *id)
            .map(|i| from + i)
    }
}

impl PageTuple for AnyTuplePage {
    fn deep_clone(&self) -> Box<dyn PageTuple> {
        Box::new(self.clone())
    }

    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.len())
    }

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if self.position(&tuple.id).is_some() {
            return Err(StoreError::DuplicateKey(tuple.id));
        }
        // After any ties: they stay in arrival order.
        let at = self.upper_bound(&tuple.id);
        self.prefixes.insert(at, order_prefix(&tuple.id));
        self.data.insert(at, tuple);
        Ok(())
    }

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        Ok(self.position(id).is_some())
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self.position(id).map(|i| self.data[i].clone()))
    }

    fn get_hinted(&self, id: &DBIdType, hint: usize) -> Result<Option<(Tuple, usize)>, StoreError> {
        if let Some(t) = self.data.get(hint)
            && t.id == *id
        {
            return Ok(Some((t.clone(), hint)));
        }
        Ok(self.position(id).map(|i| (self.data[i].clone(), i)))
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        // Same id (the callers' contract), so the order holds.
        match self.position(id) {
            Some(i) => {
                self.prefixes[i] = order_prefix(&tuple.id);
                Ok(std::mem::replace(&mut self.data[i], tuple))
            }
            None => Err(StoreError::KeyNotFound(id.clone())),
        }
    }

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        match self.position(&id) {
            Some(i) => {
                self.prefixes.remove(i);
                Ok(self.data.remove(i))
            }
            None => Err(StoreError::KeyNotFound(id)),
        }
    }

    fn values(&self) -> Result<Vec<Tuple>, StoreError> {
        Ok(self.data.clone())
    }

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
        // Unused externally (no caller outside this trait's own impls as of
        // this writing) — kept returning the hashed u64 rather than
        // widening the trait's signature to DBIdType for a method nothing
        // reads.
        Ok(self.data.iter().map(|t| t.id.hashed()).collect())
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(&self.data)?)
    }

    fn clear(&mut self) -> Result<(), StoreError> {
        self.data.clear();
        self.prefixes.clear();
        Ok(())
    }

    fn first(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.first().cloned())
    }

    fn last(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.last().cloned())
    }

    fn values_in(&self, lower: Bound<&DBIdType>, max: usize) -> Result<Vec<Tuple>, StoreError> {
        let from = match lower {
            Bound::Included(k) => self.lower_bound(k),
            Bound::Excluded(k) => self.upper_bound(k),
            Bound::Unbounded => 0,
        };
        let rest = &self.data[from..];
        // `max` of them, then the rest of the last one's ties: never part
        // of a run of equal ids, which the next chunk (starting past that
        // id) would skip.
        let mut end = max.min(rest.len());
        if let Some(last) = end.checked_sub(1).map(|i| &rest[i].id) {
            end += rest[end..]
                .iter()
                .take_while(|t| t.id.cmp(last) == Ordering::Equal)
                .count();
        }
        Ok(rest[..end].to_vec())
    }

    // STORE_AUDIT.md P5: see PageTuple::successor's own comment.
    fn successor(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.get(self.upper_bound(id)).cloned())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::StoreError,
        pages::{PageTuple, anytuple::AnyTuplePage},
        tuple::{DBIdType, Tuple},
    };

    fn make_page() -> AnyTuplePage {
        AnyTuplePage::default()
    }

    // values_in reads a chunk from a bound on, in key order.
    #[test]
    fn test_values_in_reads_a_chunk_from_a_bound() {
        use std::ops::Bound::*;
        let mut page = make_page();
        for i in (1u64..=20).rev() {
            page.add(Tuple::new(i, b"v")).unwrap();
        }
        let ids = |ts: Vec<Tuple>| {
            ts.into_iter()
                .map(|t| match t.id {
                    DBIdType::Int(i) => i,
                    other => panic!("{other:?}"),
                })
                .collect::<Vec<_>>()
        };
        let five = DBIdType::Int(5);
        assert_eq!(ids(page.values_in(Unbounded, 3).unwrap()), vec![1, 2, 3]);
        assert_eq!(ids(page.values_in(Included(&five), 3).unwrap()), vec![5, 6, 7]);
        assert_eq!(ids(page.values_in(Excluded(&five), 2).unwrap()), vec![6, 7]);
        let twenty = DBIdType::Int(20);
        assert!(page.values_in(Excluded(&twenty), 4).unwrap().is_empty());
        assert_eq!(ids(page.values_in(Included(&twenty), 4).unwrap()), vec![20]);
    }

    // A hint is only a place to look first: right, wrong, or past the end,
    // the answer (and where it was) is the same.
    #[test]
    fn test_get_hinted_finds_the_entry_whatever_the_hint() {
        let mut page = make_page();
        for i in 0u64..10 {
            page.add(Tuple::new(i * 2, b"v")).unwrap();
        }
        let at = |id: u64, hint: usize| {
            page.get_hinted(&DBIdType::Int(id), hint)
                .unwrap()
                .map(|(t, at)| (t.id, at))
        };
        for hint in [3, 0, 9, 10, usize::MAX] {
            assert_eq!(at(6, hint), Some((DBIdType::Int(6), 3)), "hint {hint}");
            assert_eq!(at(7, hint), None, "hint {hint}");
        }
    }

    #[test]
    fn test_add_and_count() {
        let mut p = make_page();
        assert_eq!(p.count().unwrap(), 0);
        p.add(Tuple::new(1, b"hello")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        p.add(Tuple::new(2, b"world")).unwrap();
        assert_eq!(p.count().unwrap(), 2);
    }

    #[test]
    fn test_add_duplicate_returns_err() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        assert!(matches!(
            p.add(Tuple::new(1, b"b")),
            Err(StoreError::DuplicateKey(_))
        ));
    }

    #[test]
    fn test_contains() {
        let mut p = make_page();
        p.add(Tuple::new(5, b"data")).unwrap();
        assert!(p.contains(&DBIdType::Int(5)).unwrap());
        assert!(!p.contains(&DBIdType::Int(99)).unwrap());
    }

    #[test]
    fn test_get_hit_and_miss() {
        let mut p = make_page();
        p.add(Tuple::new(10, b"value")).unwrap();
        let found = p.get(&DBIdType::Int(10)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().data.to_vec(), b"value");
        assert!(p.get(&DBIdType::Int(999)).unwrap().is_none());
    }

    // STORE_AUDIT.md P5: successor() is the whole point of this fix —
    // route_to_leaf/remove_index_entry/update_index_entry/insert_recursive
    // all now depend on it returning exactly "the smallest key strictly
    // greater than id" by a binary search, not a linear scan.
    #[test]
    fn test_successor_returns_first_key_greater_than_id() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(5, b"b")).unwrap();
        p.add(Tuple::new(10, b"c")).unwrap();
        let s = p.successor(&DBIdType::Int(3)).unwrap().unwrap();
        assert_eq!(s.data.to_vec(), b"b", "successor of 3 must be the entry keyed 5");
    }

    #[test]
    fn test_successor_of_an_existing_key_skips_past_it_not_returns_it() {
        let mut p = make_page();
        p.add(Tuple::new(5, b"exact")).unwrap();
        p.add(Tuple::new(10, b"next")).unwrap();
        let s = p.successor(&DBIdType::Int(5)).unwrap().unwrap();
        assert_eq!(
            s.data.to_vec(),
            b"next",
            "successor must be strictly greater, never the exact match itself"
        );
    }

    #[test]
    fn test_successor_returns_none_when_id_is_at_or_past_every_key() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(5, b"b")).unwrap();
        assert!(p.successor(&DBIdType::Int(5)).unwrap().is_none());
        assert!(p.successor(&DBIdType::Int(99)).unwrap().is_none());
    }

    #[test]
    fn test_successor_on_an_empty_page_returns_none() {
        let p = make_page();
        assert!(p.successor(&DBIdType::Int(1)).unwrap().is_none());
    }

    #[test]
    fn test_set_updates_existing() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"old")).unwrap();
        let updated = Tuple::new(1, b"new");
        p.replace(&DBIdType::Int(1), updated).unwrap();
        let got = p.get(&DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(got.data.to_vec(), b"new");
    }

    #[test]
    fn test_set_missing_returns_err() {
        let mut p = make_page();
        assert!(matches!(
            p.replace(&42.into(), Tuple::new(42, b"x")),
            Err(StoreError::KeyNotFound(_))
        ));
    }

    #[test]
    fn test_remove_existing() {
        let mut p = make_page();
        p.add(Tuple::new(3, b"bye")).unwrap();
        let removed = p.remove(DBIdType::Int(3));
        assert!(removed.is_ok());
        assert_eq!(removed.unwrap().data.to_vec(), b"bye");
        assert!(!p.contains(&DBIdType::Int(3)).unwrap());
    }

    #[test]
    fn test_remove_missing_returns_err() {
        let mut p = make_page();
        assert!(matches!(
            p.remove(DBIdType::Int(7)),
            Err(StoreError::KeyNotFound(_))
        ));
    }

    #[test]
    fn test_values_returns_all() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(2, b"b")).unwrap();
        p.add(Tuple::new(3, b"c")).unwrap();
        let vals = p.values().unwrap();
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"foo")).unwrap();
        p.add(Tuple::new(2, b"bar")).unwrap();
        let bytes = p.to_bytes().unwrap();
        let p2 = AnyTuplePage::from_bytes(&bytes).unwrap();
        assert_eq!(p2.count().unwrap(), 2);
        assert_eq!(
            p2.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"foo"
        );
        assert_eq!(
            p2.get(&DBIdType::Int(2)).unwrap().unwrap().data.to_vec(),
            b"bar"
        );
    }

    #[test]
    fn test_clone_is_independent() {
        let mut p = make_page();
        p.add(Tuple::new(1, b"original")).unwrap();
        let mut q = p.clone();
        // Adding to q does not affect p
        q.add(Tuple::new(2, b"extra")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        assert_eq!(q.count().unwrap(), 2);
    }

    #[test]
    fn test_partial_eq() {
        let mut p = make_page();
        let mut q = make_page();
        assert_eq!(p, q);
        p.add(Tuple::new(1, b"x")).unwrap();
        assert_ne!(p, q);
        q.add(Tuple::new(1, b"x")).unwrap();
        assert_eq!(p, q);
    }

    #[test]
    fn test_string_id() {
        let mut p = make_page();
        let id = DBIdType::from("my_key".to_string());
        p.add(Tuple::new_with(id.clone(), b"payload", None, None))
            .unwrap();
        assert!(p.contains(&id.clone()).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data.to_vec(), b"payload");
    }

    fn rec_id(a: i64, b: i64) -> DBIdType {
        DBIdType::Rec(
            crate::valueitem::IndexKey::new_from(&[
                crate::valueitem::ValueItem::Integer(a),
                crate::valueitem::ValueItem::Integer(b),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn test_rec_id_add_get_remove() {
        let mut p = make_page();
        let id = rec_id(1, 2);
        p.add(Tuple::new_with(id.clone(), b"payload", None, None))
            .unwrap();
        assert!(p.contains(&id).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data.to_vec(), b"payload");
        let removed = p.remove(id.clone()).unwrap();
        assert_eq!(removed.data.to_vec(), b"payload");
        assert!(!p.contains(&id).unwrap());
    }

    #[test]
    fn test_rec_id_iterates_in_structural_order() {
        // Entries are sorted by DBIdType directly (see this file's own
        // doc comment on `data`), so their order IS DBIdType::cmp's
        // order — for Rec, that's structural. Confirms
        // values()/iteration produce ids in ascending field order, not
        // insertion order or hash order.
        let mut p = make_page();
        for (a, b) in [(3, 1), (1, 2), (2, 1), (1, 1)] {
            p.add(Tuple::new_with(
                rec_id(a, b),
                format!("{a}-{b}").as_bytes(),
                None,
                None,
            ))
            .unwrap();
        }
        let vals: Vec<String> = p
            .values()
            .unwrap()
            .into_iter()
            .map(|t| String::from_utf8(t.data.to_vec()).unwrap())
            .collect();
        // values() returns the entries in key order (ascending), so this
        // should come out already sorted structurally.
        assert_eq!(vals, vec!["1-1", "1-2", "2-1", "3-1"]);
    }

    // DBIdType::cmp for Rec can say Equal for ids that are `!=` under
    // PartialEq (see IndexKey::partial_cmp's own documented ties: same
    // content with a different Str/Blob reserved capacity, or a key that's
    // a strict field-wise prefix of a longer one). Keying the map by
    // DBIdType (Ord) rather than a separately-hashed u64 makes this the
    // SAME kind of "collision" the old hash-keyed scheme already had to
    // tolerate — confirm the Vec-bucketing + PartialEq disambiguation
    // still correctly keeps both entries distinct and independently
    // reachable, rather than one silently shadowing the other.
    #[test]
    fn test_rec_ids_that_tie_under_ord_but_differ_under_partial_eq_both_survive() {
        use crate::valueitem::{IndexKey, ValueItem};

        let mut p = make_page();
        let short = DBIdType::Rec(IndexKey::new_from(&[ValueItem::Integer(1)]).unwrap());
        let long = DBIdType::Rec(
            IndexKey::new_from(&[ValueItem::Integer(1), ValueItem::Integer(2)]).unwrap(),
        );
        assert_eq!(
            short.cmp(&long),
            std::cmp::Ordering::Equal,
            "sanity: a prefix key ties under Ord with the longer key it prefixes"
        );
        assert_ne!(short, long, "but they are NOT the same id under PartialEq");

        p.add(Tuple::new_with(short.clone(), b"short", None, None))
            .unwrap();
        p.add(Tuple::new_with(long.clone(), b"long", None, None))
            .unwrap();

        assert_eq!(p.count().unwrap(), 2, "both are kept, side by side");
        assert_eq!(p.get(&short).unwrap().unwrap().data.to_vec(), b"short");
        assert_eq!(p.get(&long).unwrap().unwrap().data.to_vec(), b"long");

        let removed_short = p.remove(short.clone()).unwrap();
        assert_eq!(removed_short.data.to_vec(), b"short");
        assert!(!p.contains(&short).unwrap());
        assert!(
            p.contains(&long).unwrap(),
            "removing one tied id must not remove the other"
        );
    }

    // order_prefix's promise (see its own comment): where two ids'
    // prefixes are both known, of one kind, and unequal, they order the
    // ids. Checked over every pair of a mix of values of each type,
    // including the edges: shared string prefixes past 15 bytes, embedded
    // zeros, negative zero, NaN, the integer extremes, keys of different
    // lengths, and Int ids next to Rec ones.
    #[test]
    fn test_order_prefix_never_contradicts_the_id_order() {
        use super::order_prefix;
        use crate::valueitem::{IndexKey, ValueItem};
        use std::sync::Arc;

        let s = |v: &str| ValueItem::Str((v.to_string(), 32));
        let firsts = vec![
            ValueItem::Null,
            ValueItem::Boolean(false),
            ValueItem::Boolean(true),
            ValueItem::Integer(i64::MIN),
            ValueItem::Integer(-1),
            ValueItem::Integer(0),
            ValueItem::Integer(1),
            ValueItem::Integer(i64::MAX),
            ValueItem::Double(f64::NEG_INFINITY),
            ValueItem::Double(-1.5),
            ValueItem::Double(-0.0),
            ValueItem::Double(0.0),
            ValueItem::Double(2.5),
            ValueItem::Double(f64::NAN),
            ValueItem::Datetime(0),
            ValueItem::Datetime(u64::MAX),
            s(""),
            s("\0"),
            s("a"),
            s("a\0"),
            s("a\0b"),
            s("ab"),
            s("ORD0100000"),
            s("ORD0100001"),
            s("abcdefghijklmno"),
            s("abcdefghijklmnoA"),
            s("abcdefghijklmnoB"),
            s("abcdefghijklmn"),
            s("\u{ff}"),
            ValueItem::Blob((Arc::from(&b""[..]), 8)),
            ValueItem::Blob((Arc::from(&b"\x00\x01"[..]), 8)),
            ValueItem::Blob((Arc::from(&b"\x01"[..]), 8)),
        ];
        let mut ids: Vec<DBIdType> = vec![
            DBIdType::Int(0),
            DBIdType::Int(7),
            DBIdType::Int(u64::MAX),
            DBIdType::Rec(IndexKey::new_from(&[]).unwrap()),
        ];
        for f in &firsts {
            ids.push(DBIdType::Rec(
                IndexKey::new_from(std::slice::from_ref(f)).unwrap(),
            ));
            for second in [ValueItem::Integer(-3), s("z")] {
                ids.push(DBIdType::Rec(
                    IndexKey::new_from(&[f.clone(), second]).unwrap(),
                ));
            }
        }
        let mut checked = 0;
        for a in &ids {
            for b in &ids {
                let (pa, pb) = (order_prefix(a), order_prefix(b));
                if pa == pb || pa >> 126 != pb >> 126 || pa >> 126 == 0 {
                    continue;
                }
                assert_eq!(pa.cmp(&pb), a.cmp(b), "{a:?} vs {b:?}");
                checked += 1;
            }
        }
        assert!(checked > 1000, "{checked}");
    }

    // STORE_AUDIT.md P6 — see slotted.rs's matching bench and its own doc
    // comment on why SlottedPage is NOT the default despite existing:
    // repeated in-memory access to an already-loaded page is what this
    // measures (not load/flush cost), and it's the axis SlottedPage loses
    // badly on. Throwaway (not a committed criterion bench), #[ignore]d.
    #[test]
    #[ignore]
    fn bench_repeated_get_on_an_already_loaded_page() {
        let mut p = AnyTuplePage::new();
        for i in 0..200u64 {
            p.add(Tuple::new(i, b"0123456789012345678901234567890123456789")).unwrap();
        }
        let mut state: u64 = 0x243F_6A88_85A3_08D3;
        const ITERS: u64 = 2_000_000;
        let start = crate::clock::Instant::now();
        for _ in 0..ITERS {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let id = DBIdType::Int(state % 200);
            std::hint::black_box(p.get(&id).unwrap());
        }
        let elapsed = start.elapsed();
        eprintln!(
            "AnyTuplePage bench_repeated_get_on_an_already_loaded_page: {ITERS} gets in \
             {elapsed:?} ({:.0} gets/s)",
            ITERS as f64 / elapsed.as_secs_f64()
        );
    }

    // Why a sorted Vec with inline key prefixes and not the BTreeMap it
    // replaced: random gets of composite string keys over many pages (the
    // working set out of cache, as a table's leaves are), against a
    // BTreeMap<DBIdType, Vec<Tuple>> built the old way. Measured: 210 ns
    // a get vs 440 (BTreeMap); with no prefixes, a plain binary search was
    // 800-930 — two pointer chases per probe. Throwaway, #[ignore]d.
    #[test]
    #[ignore]
    fn bench_get_over_many_pages_vs_btreemap() {
        use crate::valueitem::{IndexKey, ValueItem};
        use std::collections::BTreeMap;

        let key = |i: usize| {
            DBIdType::Rec(
                IndexKey::new_from(&[ValueItem::Str((format!("ORD{i:07}"), 12))]).unwrap(),
            )
        };
        for (pages, n) in [(1usize, 600usize), (400, 600), (400, 150)] {
            let mut maps = vec![];
            let mut vecs = vec![];
            for p in 0..pages {
                let ts: Vec<Tuple> = (0..n)
                    .map(|i| Tuple::new_with(key(p * n + i), &7u64.to_le_bytes(), None, None))
                    .collect();
                let bytes = postcard::to_allocvec(&ts).unwrap();
                let mut map: BTreeMap<DBIdType, Vec<Tuple>> = BTreeMap::new();
                for t in postcard::from_bytes::<Vec<Tuple>>(&bytes).unwrap() {
                    map.entry(t.id.clone()).or_default().push(t);
                }
                maps.push(map);
                vecs.push(AnyTuplePage::from_bytes(&bytes).unwrap());
            }
            let mut x: u64 = 0x2545_F491_4F6C_DD1D;
            let probes: Vec<(usize, DBIdType)> = (0..200_000)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let i = (x % (pages * n) as u64) as usize;
                    (i / n, key(i))
                })
                .collect();
            let start = std::time::Instant::now();
            for (p, k) in &probes {
                std::hint::black_box(
                    maps[*p]
                        .get(k)
                        .and_then(|v| v.iter().find(|t| t.id == *k))
                        .cloned(),
                );
            }
            let map_ns = start.elapsed().as_nanos() as f64 / probes.len() as f64;
            let start = std::time::Instant::now();
            for (p, k) in &probes {
                std::hint::black_box(vecs[*p].get(k).unwrap());
            }
            let vec_ns = start.elapsed().as_nanos() as f64 / probes.len() as f64;
            eprintln!("{pages} pages of {n}: BTreeMap {map_ns:.0} ns, AnyTuplePage {vec_ns:.0} ns");
        }
    }
}
