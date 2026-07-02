use std::sync::Arc;

use crate::{
    db::DBSizeType,
    error::StoreError,
    tuple::{DBIdType, Tuple},
};

pub mod anytuple;
pub mod fixedtuple;

pub type TupleType = Tuple;

pub trait PageTuple {
    fn count(&self) -> Result<usize, StoreError>;

    /// Deep-copy into a fresh allocation. `Page::clone` must use this (not
    /// `Arc::clone`) so a cloned Page owns an independent tuple store. Mutation
    /// happens through `&mut self` and is only reachable after
    /// `Arc::make_mut(&mut Arc<Page>)` has made the Page uniquely owned, so a
    /// shared Arc would otherwise let a copy-on-write clone leak mutations back
    /// into the cached page or a page mid-serialization on the writer thread.
    fn deep_clone(&self) -> Arc<dyn PageTuple>;

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError>;

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError>;

    fn get(&self, id: &DBIdType) -> Result<Option<TupleType>, StoreError>;

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError>;

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError>;

    fn values(&self) -> Result<Vec<TupleType>, StoreError>;

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError>;

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError>;

    fn clear(&mut self) -> Result<(), StoreError>;

    //fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError>;

    fn first(&self) -> Result<Option<TupleType>, StoreError>;

    fn last(&self) -> Result<Option<TupleType>, StoreError>;
}
