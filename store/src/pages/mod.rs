use crate::{
    db::DBSizeType,
    error::StoreError,
    tuple::{DBIdType, Tuple, TupleRef},
};

pub mod anytuple;
pub mod content;
pub mod fixedtuple;
pub mod run;
pub mod slotted;

pub type TupleType = Tuple;

// Send + Sync: a page's content is shared, read-only, with the readers
// holding a snapshot of it (see Page::iter), on whatever thread they run.
pub trait PageTuple: Send + Sync {
    fn count(&self) -> Result<usize, StoreError>;

    /// The `i`th tuple in the page's own order, if there are that many.
    fn at(&self, i: usize) -> Option<TupleType>;

    /// `at`, lent rather than copied (see TupleRef) where the page holds
    /// tuples it can lend; the default copies.
    fn at_ref(&self, i: usize) -> Option<TupleRef<'_>> {
        self.at(i).map(TupleRef::owned)
    }

    /// Where `id` is in the page's own order (`at`), told where it probably
    /// is as `get_hinted` is. The default looks at every tuple.
    fn find_hinted(&self, id: &DBIdType, _hint: usize) -> Result<Option<usize>, StoreError> {
        Ok((0..self.count()?).find(|&i| self.at(i).is_some_and(|t| t.id == *id)))
    }

    /// Where the first tuple at or after `lower` is, in the page's own
    /// order (`at`) — `count` if none.
    fn seek(&self, lower: std::ops::Bound<&DBIdType>) -> Result<usize, StoreError> {
        use std::ops::Bound::*;
        let values = self.values()?;
        Ok(values
            .iter()
            .position(|t| match lower {
                Included(k) => t.id >= *k,
                Excluded(k) => t.id > *k,
                Unbounded => true,
            })
            .unwrap_or(values.len()))
    }

    /// Deep-copy into a fresh allocation: for `Page::clone`, which must own
    /// an independent tuple store, and for a change to content a reader
    /// still holds a snapshot of (see `PageInner::data_mut`). Otherwise
    /// mutation (`add`/`remove`/`replace`/`clear`) happens in place under
    /// `Page`'s lock.
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
