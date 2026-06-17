use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::{PageTuple, anytuple::AnyTuplePage},
    tuple::{DBIdType, Tuple},
};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FixedTuplePage {
    tuple_size: usize,
    data: AnyTuplePage,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct FixedTupleDto<'a> {
    size: usize,
    data: &'a [u8],
}

impl FixedTuplePage {
    pub fn new(size: usize) -> Self {
        Self {
            tuple_size: size,
            ..Default::default()
        }
    }
}

impl PageTuple for FixedTuplePage {
    type Item = FixedTuplePage;

    fn count(&self) -> Result<usize, StoreError> {
        self.data.count()
    }

    fn add(&self, tuple: Tuple) -> Result<(), StoreError> {
        if tuple.size() > self.tuple_size as DBSizeType {
            return Err(StoreError::TupleTooLarge(tuple.size(), self.tuple_size));
        }
        self.data.add(tuple)
    }

    fn contains(&self, id: DBIdType) -> Result<bool, StoreError> {
        self.data.contains(id)
    }

    fn get(&self, id: DBIdType) -> Result<Option<std::sync::Arc<Tuple>>, StoreError> {
        self.data.get(id)
    }

    fn set(&self, tuple: Tuple) -> Result<(), StoreError> {
        if tuple.size() > self.tuple_size as DBSizeType {
            return Err(StoreError::TupleTooLarge(tuple.size(), self.tuple_size));
        }
        self.data.set(tuple)
    }

    fn remove(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        self.data.remove(id)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        let bytes = self.data.to_bytes()?;
        let dto = FixedTupleDto {
            size: self.tuple_size,
            data: &bytes,
        };
        Ok(to_allocvec(&dto)?)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Item, StoreError> {
        let dto = from_bytes::<FixedTupleDto>(bytes)?;
        Ok(Self {
            tuple_size: dto.size,
            data: AnyTuplePage::from_bytes(dto.data)?,
        })
    }
}
