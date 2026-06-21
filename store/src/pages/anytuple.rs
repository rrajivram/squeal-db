use std::{
    collections::{BTreeMap, hash_map::Iter},
    sync::{Arc, RwLock},
};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AnyTuplePage {
    data: RwLock<BTreeMap<DBSizeType, Vec<Arc<Tuple>>>>,
}

impl Clone for AnyTuplePage {
    fn clone(&self) -> Self {
        Self {
            data: RwLock::new(self.data.read().unwrap().clone()),
            ..Default::default()
        }
    }
}

impl PartialEq for AnyTuplePage {
    fn eq(&self, other: &Self) -> bool {
        *self.data.read().unwrap() == *other.data.read().unwrap()
    }
}

pub struct TupleIter<'a> {
    iter: Iter<'a, DBIdType, Arc<Tuple>>,
}

impl AnyTuplePage {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<AnyTuplePage, StoreError> {
        let mut vec: Vec<Arc<Tuple>> = from_bytes(bytes)?;
        let data = vec
            .drain(..)
            .map(|t| (t.id.hashed(), t))
            .collect::<Vec<_>>();
        let mut map: BTreeMap<u64, Vec<Arc<Tuple>>> = BTreeMap::new();
        for (id, t) in data {
            map.entry(id)
                .and_modify(|f| f.push(t.clone()))
                .or_insert(vec![t]);
        }

        Ok(Self {
            data: RwLock::new(map),
        })
    }
}

impl PageTuple for AnyTuplePage {
    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.read()?.len())
    }

    fn add(&self, tuple: Tuple) -> Result<(), StoreError> {
        if self.contains(&tuple.id)? {
            return Err(StoreError::DuplicateKey(tuple.id));
        }
        let tuple = Arc::new(tuple);
        self.data
            .write()?
            .entry(tuple.id.hashed())
            .and_modify(|v| v.push(tuple.clone()))
            .or_insert(vec![tuple]);
        Ok(())
    }

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        Ok(self
            .data
            .read()?
            .get(&id.hashed())
            .map(|v| is_present(v, id))
            .unwrap_or_default())
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Arc<Tuple>>, StoreError> {
        Ok(self
            .data
            .read()?
            .get(&id.hashed())
            .map(|t| extract(t, id))
            .flatten())
    }

    fn replace(&self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        Ok(self
            .data
            .write()?
            .get_mut(&id.hashed())
            .map(|v| replace(v, id, tuple))
            .flatten()
            .ok_or(StoreError::KeyNotFound(id.clone()))?)
    }

    fn remove(&self, id: DBIdType) -> Result<Tuple, StoreError> {
        Ok(self
            .data
            .write()?
            .get_mut(&id.hashed())
            .map(|t| remove(t, &id))
            .flatten()
            .map(|t| t.as_ref().clone())
            .ok_or(StoreError::KeyNotFound(id))?)
    }

    fn values(&self) -> Result<Vec<Arc<Tuple>>, StoreError> {
        Ok(self.data.read()?.values().flatten().cloned().collect())
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(&self.values()?)?)
    }

    fn first(&self) -> Result<Option<Arc<Tuple>>, StoreError> {
        Ok(self
            .data
            .read()?
            .first_key_value()
            .map(|(_k, v)| v.first())
            .flatten()
            .cloned())
    }

    fn last(&self) -> Result<Option<Arc<Tuple>>, StoreError> {
        Ok(self
            .data
            .read()?
            .last_key_value()
            .map(|(_k, v)| v.last())
            .flatten()
            .cloned())
    }
}

#[inline(always)]
fn is_present(items: &Vec<Arc<Tuple>>, id: &DBIdType) -> bool {
    items.iter().any(|i| i.id == *id)
}

#[inline(always)]
fn extract(items: &Vec<Arc<Tuple>>, id: &DBIdType) -> Option<Arc<Tuple>> {
    items.iter().find(|i| i.id == *id).map(|v| v.clone())
}

#[inline(always)]
fn remove(items: &mut Vec<Arc<Tuple>>, id: &DBIdType) -> Option<Arc<Tuple>> {
    items.extract_if(.., |f| f.id == *id).next()
}

#[inline(always)]
fn replace(items: &mut Vec<Arc<Tuple>>, id: &DBIdType, tuple: Tuple) -> Option<Tuple> {
    items
        .iter_mut()
        .try_for_each(|t| {
            if t.id == *id {
                *t = Arc::new(tuple.clone());
                return std::ops::ControlFlow::Break(Some(t.as_ref().clone()));
            }
            std::ops::ControlFlow::Continue(())
        })
        .break_value()
        .flatten()
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

    #[test]
    fn test_add_and_count() {
        let p = make_page();
        assert_eq!(p.count().unwrap(), 0);
        p.add(Tuple::new(1, b"hello")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        p.add(Tuple::new(2, b"world")).unwrap();
        assert_eq!(p.count().unwrap(), 2);
    }

    #[test]
    fn test_add_duplicate_returns_err() {
        let p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        assert!(matches!(
            p.add(Tuple::new(1, b"b")),
            Err(StoreError::DuplicateKey(_))
        ));
    }

    #[test]
    fn test_contains() {
        let p = make_page();
        p.add(Tuple::new(5, b"data")).unwrap();
        assert!(p.contains(&DBIdType::Int(5)).unwrap());
        assert!(!p.contains(&DBIdType::Int(99)).unwrap());
    }

    #[test]
    fn test_get_hit_and_miss() {
        let p = make_page();
        p.add(Tuple::new(10, b"value")).unwrap();
        let found = p.get(&DBIdType::Int(10)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().data, b"value");
        assert!(p.get(&DBIdType::Int(999)).unwrap().is_none());
    }

    #[test]
    fn test_set_updates_existing() {
        let p = make_page();
        p.add(Tuple::new(1, b"old")).unwrap();
        let updated = Tuple::new(1, b"new");
        p.replace(&DBIdType::Int(1), updated).unwrap();
        let got = p.get(&DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(got.data, b"new");
    }

    #[test]
    fn test_set_missing_returns_err() {
        let p = make_page();
        assert!(matches!(
            p.replace(&42.into(), Tuple::new(42, b"x")),
            Err(StoreError::KeyNotFound(_))
        ));
    }

    #[test]
    fn test_remove_existing() {
        let p = make_page();
        p.add(Tuple::new(3, b"bye")).unwrap();
        let removed = p.remove(DBIdType::Int(3));
        assert!(removed.is_ok());
        assert_eq!(removed.unwrap().data, b"bye");
        assert!(!p.contains(&DBIdType::Int(3)).unwrap());
    }

    #[test]
    fn test_remove_missing_returns_err() {
        let p = make_page();
        assert!(matches!(
            p.remove(DBIdType::Int(7)),
            Err(StoreError::KeyNotFound(_))
        ));
    }

    #[test]
    fn test_values_returns_all() {
        let p = make_page();
        p.add(Tuple::new(1, b"a")).unwrap();
        p.add(Tuple::new(2, b"b")).unwrap();
        p.add(Tuple::new(3, b"c")).unwrap();
        let vals = p.values().unwrap();
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let p = make_page();
        p.add(Tuple::new(1, b"foo")).unwrap();
        p.add(Tuple::new(2, b"bar")).unwrap();
        let bytes = p.to_bytes().unwrap();
        let p2 = AnyTuplePage::from_bytes(&bytes).unwrap();
        assert_eq!(p2.count().unwrap(), 2);
        assert_eq!(p2.get(&DBIdType::Int(1)).unwrap().unwrap().data, b"foo");
        assert_eq!(p2.get(&DBIdType::Int(2)).unwrap().unwrap().data, b"bar");
    }

    #[test]
    fn test_clone_is_independent() {
        let p = make_page();
        p.add(Tuple::new(1, b"original")).unwrap();
        let q = p.clone();
        // Adding to q does not affect p
        q.add(Tuple::new(2, b"extra")).unwrap();
        assert_eq!(p.count().unwrap(), 1);
        assert_eq!(q.count().unwrap(), 2);
    }

    #[test]
    fn test_partial_eq() {
        let p = make_page();
        let q = make_page();
        assert_eq!(p, q);
        p.add(Tuple::new(1, b"x")).unwrap();
        assert_ne!(p, q);
        q.add(Tuple::new(1, b"x")).unwrap();
        assert_eq!(p, q);
    }

    #[test]
    fn test_vec_id() {
        let p = make_page();
        let id = DBIdType::Vec(b"my_key".to_vec());
        p.add(Tuple::new_with(id.clone(), b"payload", None, None))
            .unwrap();
        assert!(p.contains(&id.clone()).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data, b"payload");
    }
}
