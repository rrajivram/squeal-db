use std::sync::Arc;

use crate::{
    error::StoreError,
    tuple::{DBIdType, Tuple},
};

pub mod anytuple;
pub mod fixedtuple;

pub trait PageTuple {
    type Item;
    fn count(&self) -> Result<usize, StoreError>;

    fn add(&self, tuple: Tuple) -> Result<(), StoreError>;

    fn contains(&self, id: DBIdType) -> Result<bool, StoreError>;

    fn get(&self, id: DBIdType) -> Result<Option<Arc<Tuple>>, StoreError>;

    fn set(&self, tuple: Tuple) -> Result<(), StoreError>;

    fn remove(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError>;

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError>;

    fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError>;
}
