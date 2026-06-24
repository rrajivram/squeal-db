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

    fn add(&self, tuple: Tuple) -> Result<(), StoreError>;

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError>;

    fn get(&self, id: &DBIdType) -> Result<Option<TupleType>, StoreError>;

    fn replace(&self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError>;

    fn remove(&self, id: DBIdType) -> Result<Tuple, StoreError>;

    fn values(&self) -> Result<Vec<TupleType>, StoreError>;

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError>;

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError>;

    fn clear(&self) -> Result<(), StoreError>;

    //fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError>;

    fn first(&self) -> Result<Option<TupleType>, StoreError>;

    fn last(&self) -> Result<Option<TupleType>, StoreError>;
}
