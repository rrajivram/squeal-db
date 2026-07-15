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

impl Clone for FixedTuplePage {
    fn clone(&self) -> Self {
        Self {
            tuple_size: self.tuple_size,
            data: self.data.clone(),
        }
    }
}

impl PartialEq for FixedTuplePage {
    fn eq(&self, other: &Self) -> bool {
        self.tuple_size == other.tuple_size && self.data == other.data
    }
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

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        let dto = from_bytes::<FixedTupleDto>(bytes)?;
        Ok(Self {
            tuple_size: dto.size,
            data: AnyTuplePage::from_bytes(dto.data)?,
        })
    }
}

impl PageTuple for FixedTuplePage {
    fn deep_clone(&self) -> Box<dyn PageTuple> {
        Box::new(self.clone())
    }

    fn count(&self) -> Result<usize, StoreError> {
        self.data.count()
    }

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if tuple.size() > self.tuple_size as DBSizeType {
            return Err(StoreError::TupleTooLarge(tuple.size(), self.tuple_size));
        }
        self.data.add(tuple)
    }

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        self.data.contains(id)
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        self.data.get(id)
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        if tuple.size() > self.tuple_size as DBSizeType {
            return Err(StoreError::TupleTooLarge(tuple.size(), self.tuple_size));
        }
        self.data.replace(id, tuple)
    }

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        self.data.remove(id)
    }

    fn values(&self) -> Result<Vec<Tuple>, StoreError> {
        self.data.values()
    }

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
        self.data.keys()
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        let bytes = self.data.to_bytes()?;
        let dto = FixedTupleDto {
            size: self.tuple_size,
            data: &bytes,
        };
        Ok(to_allocvec(&dto)?)
    }
    fn clear(&mut self) -> Result<(), StoreError> {
        self.data.clear()
    }

    fn first(&self) -> Result<Option<Tuple>, StoreError> {
        self.data.first()
    }

    fn last(&self) -> Result<Option<Tuple>, StoreError> {
        self.data.last()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::StoreError,
        pages::{PageTuple, fixedtuple::FixedTuplePage},
        tuple::{DBIdType, Tuple},
    };

    fn make_page(tuple_size: usize) -> FixedTuplePage {
        FixedTuplePage::new(tuple_size)
    }

    // Returns the serialized size of a Tuple with the given data payload.
    fn tuple_size(data: &[u8]) -> usize {
        Tuple::new(0, data).size() as usize
    }

    #[test]
    fn test_add_within_limit() {
        let sz = tuple_size(b"hello");
        let mut p = make_page(sz);
        assert!(p.add(Tuple::new(1, b"hello")).is_ok());
        assert_eq!(p.count().unwrap(), 1);
    }

    #[test]
    fn test_add_exceeds_limit_returns_err() {
        let sz = tuple_size(b"hi");
        let mut p = make_page(sz);
        // "toolong" is bigger than "hi" in tuple size
        assert!(matches!(
            p.add(Tuple::new(1, b"toolong_payload")),
            Err(StoreError::TupleTooLarge(_, _))
        ));
    }

    #[test]
    fn test_set_exceeds_limit_returns_err() {
        let sz = tuple_size(b"hello");
        let mut p = make_page(sz);
        p.add(Tuple::new(1, b"hello")).unwrap();
        assert!(matches!(
            p.replace(&1.into(), Tuple::new(1, b"way_too_long_payload")),
            Err(StoreError::TupleTooLarge(_, _))
        ));
    }

    #[test]
    fn test_contains_and_get() {
        let sz = tuple_size(b"abc");
        let mut p = make_page(sz);
        p.add(Tuple::new(7, b"abc")).unwrap();
        assert!(p.contains(&DBIdType::Int(7)).unwrap());
        assert!(!p.contains(&DBIdType::Int(8)).unwrap());
        assert_eq!(p.get(&DBIdType::Int(7)).unwrap().unwrap().data.to_vec(), b"abc");
    }

    #[test]
    fn test_remove() {
        let sz = tuple_size(b"x");
        let mut p = make_page(sz);
        p.add(Tuple::new(1, b"x")).unwrap();
        let removed = p.remove(DBIdType::Int(1));
        assert!(removed.is_ok());
        assert_eq!(removed.unwrap().data.to_vec(), b"x");
        assert!(!p.contains(&DBIdType::Int(1)).unwrap());
    }

    #[test]
    fn test_values() {
        let sz = tuple_size(b"v");
        let mut p = make_page(sz);
        p.add(Tuple::new(1, b"v")).unwrap();
        p.add(Tuple::new(2, b"v")).unwrap();
        assert_eq!(p.values().unwrap().len(), 2);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let sz = tuple_size(b"data");
        let mut p = make_page(sz);
        p.add(Tuple::new(1, b"data")).unwrap();
        p.add(Tuple::new(2, b"data")).unwrap();
        let bytes = p.to_bytes().unwrap();
        let p2 = FixedTuplePage::from_bytes(&bytes).unwrap();
        assert_eq!(p2.count().unwrap(), 2);
        assert_eq!(p2.get(&DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), b"data");
    }

    #[test]
    fn test_clone_independence() {
        let sz = tuple_size(b"hi");
        let mut p = make_page(sz);
        p.add(Tuple::new(1, b"hi")).unwrap();
        let mut q = p.clone();
        q.add(Tuple::new(2, b"hi")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        assert_eq!(q.count().unwrap(), 2);
    }

    #[test]
    fn test_partial_eq() {
        let sz = tuple_size(b"a");
        let mut p = make_page(sz);
        let mut q = make_page(sz);
        assert_eq!(p, q);
        p.add(Tuple::new(1, b"a")).unwrap();
        assert_ne!(p, q);
        q.add(Tuple::new(1, b"a")).unwrap();
        assert_eq!(p, q);
    }

    #[test]
    fn test_different_sizes_not_equal() {
        let p = make_page(100);
        let q = make_page(200);
        assert_ne!(p, q);
    }
}
