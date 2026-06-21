use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    error::StoreError,
    tuple::{DBIdType, Tuple},
};

pub mod anytuple;
pub mod fixedtuple;

pub trait PageTuple {
    fn count(&self) -> Result<usize, StoreError>;

    fn add(&self, tuple: Tuple) -> Result<(), StoreError>;

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError>;

    fn get(&self, id: &DBIdType) -> Result<Option<Arc<Tuple>>, StoreError>;

    fn replace(&self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError>;

    fn remove(&self, id: DBIdType) -> Result<Tuple, StoreError>;

    fn values(&self) -> Result<Vec<Arc<Tuple>>, StoreError>;

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError>;

    //fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError>;

    fn first(&self) -> Result<Option<Arc<Tuple>>, StoreError>;

    fn last(&self) -> Result<Option<Arc<Tuple>>, StoreError>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TupleType {
    Fixed,
    AnySize,
}
