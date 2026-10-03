use crate::{
    db::DBSizeType,
    error::StoreError,
    tuple::{DBIdType, Tuple},
};

pub mod anytuple;
pub mod content;
pub mod fixedtuple;
pub mod run;
pub mod slotted;

pub type TupleType = Tuple;

pub trait PageTuple {
    fn count(&self) -> Result<usize, StoreError>;

    /// Deep-copy into a fresh allocation. `Page::clone` must use this (not a
    /// shared pointer) so a cloned Page owns an independent tuple store —
    /// `Page` guards its own copy behind a lock (see `PageInner`), so
    /// ordinary mutation (`add`/`remove`/`replace`/`clear`) happens in place
    /// under that lock rather than via this method; `deep_clone` exists for
    /// the rarer case of needing a genuinely separate copy (`Page::clone`).
    fn deep_clone(&self) -> Box<dyn PageTuple>;

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError>;

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError>;

    fn get(&self, id: &DBIdType) -> Result<Option<TupleType>, StoreError>;

    /// `get`, told where the entry probably is — a caller reading a page's
    /// entries in order passes one past the last position it was given —
    /// and saying where it was found. A wrong hint costs nothing but the
    /// one check; the default ignores it.
    fn get_hinted(
        &self,
        id: &DBIdType,
        _hint: usize,
    ) -> Result<Option<(TupleType, usize)>, StoreError> {
        Ok(self.get(id)?.map(|t| (t, 0)))
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError>;

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError>;

    fn values(&self) -> Result<Vec<TupleType>, StoreError>;

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError>;

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError>;

    fn clear(&mut self) -> Result<(), StoreError>;

    //fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError>;

    fn first(&self) -> Result<Option<TupleType>, StoreError>;

    /// Tuples at or after `lower`, in key order: about `max` of them —
    /// whole buckets of ids that compare equal, never part of one — so a
    /// caller can walk a page a chunk at a time instead of copying all of
    /// it (`values`) to read a few entries.
    fn values_in(
        &self,
        lower: std::ops::Bound<&DBIdType>,
        max: usize,
    ) -> Result<Vec<TupleType>, StoreError> {
        use std::ops::Bound::*;
        let mut out: Vec<TupleType> = vec![];
        for t in self.values()? {
            let wanted = match lower {
                Included(k) => t.id >= *k,
                Excluded(k) => t.id > *k,
                Unbounded => true,
            };
            if !wanted {
                continue;
            }
            if out.len() >= max && out.last().is_some_and(|l| l.id < t.id) {
                break;
            }
            out.push(t);
        }
        Ok(out)
    }

    fn last(&self) -> Result<Option<TupleType>, StoreError>;

    // STORE_AUDIT.md P5: the smallest-keyed tuple whose id is strictly
    // greater than `id`, or `None` if `id` is >= every key present —
    // callers combine this with `last()` for that fallthrough case (see
    // bplustree.rs's route_to_leaf/remove_index_entry/update_index_entry/
    // insert_recursive, all of which used to answer this exact question
    // via `values()` — a full clone of every tuple on the page into a
    // fresh `Vec` — followed by a linear scan decoding each one in turn
    // until the first match. For an inner-routing page with N entries,
    // that's an O(N) clone plus up to O(N) `postcard` decodes for what is
    // structurally a single B-tree range query. `AnyTuplePage` backs this
    // with a binary search, an O(log N) lookup with no clone of anything
    // but the one matched entry.
    fn successor(&self, id: &DBIdType) -> Result<Option<TupleType>, StoreError>;
}
