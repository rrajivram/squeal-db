use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

// No interior lock: the store is only mutated through `&mut self`, which is only
// reachable after `Arc::make_mut(&mut Arc<Page>)` has made the owning Page
// uniquely owned (see `PageTuple::deep_clone`). Concurrent readers only ever see
// a different, immutable Page snapshot, so a lock here would guard nothing.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AnyTuplePage {
    data: BTreeMap<DBSizeType, Vec<Tuple>>,
}

impl Clone for AnyTuplePage {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl PartialEq for AnyTuplePage {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl AnyTuplePage {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<AnyTuplePage, StoreError> {
        let mut vec: Vec<Tuple> = from_bytes(bytes)?;
        let data = vec
            .drain(..)
            .map(|t| (t.id.hashed(), t))
            .collect::<Vec<_>>();
        let mut map: BTreeMap<u64, Vec<Tuple>> = BTreeMap::new();
        for (id, t) in data {
            map.entry(id)
                .and_modify(|f| f.push(t.clone()))
                .or_insert(vec![t]);
        }

        Ok(Self { data: map })
    }
}

impl PageTuple for AnyTuplePage {
    fn deep_clone(&self) -> std::sync::Arc<dyn PageTuple> {
        std::sync::Arc::new(self.clone())
    }

    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.len())
    }

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if self.contains(&tuple.id)? {
            return Err(StoreError::DuplicateKey(tuple.id));
        }
        self.data
            .entry(tuple.id.hashed())
            .and_modify(|v| v.push(tuple.clone()))
            .or_insert(vec![tuple]);
        Ok(())
    }

    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        Ok(self
            .data
            .get(&id.hashed())
            .map(|v| is_present(v, id))
            .unwrap_or_default())
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.get(&id.hashed()).and_then(|t| extract(t, id)))
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        self.data
            .get_mut(&id.hashed())
            .and_then(|v| replace(v, id, tuple))
            .ok_or(StoreError::KeyNotFound(id.clone()))
    }

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        self.data
            .get_mut(&id.hashed())
            .and_then(|t| remove(t, &id))
            .ok_or(StoreError::KeyNotFound(id))
    }

    fn values(&self) -> Result<Vec<Tuple>, StoreError> {
        Ok(self.data.values().flatten().cloned().collect())
    }

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
        Ok(self.data.keys().copied().collect())
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        // Serialize borrowed tuples: no clone / refcount bump of the payloads on
        // the writer-thread path. `Vec<&Tuple>` serializes identically to
        // `Vec<Tuple>` (serde forwards `&T` to `T`), so the wire format — and
        // thus `from_bytes` — is unchanged.
        let refs: Vec<&Tuple> = self.data.values().flatten().collect();
        Ok(to_allocvec(&refs)?)
    }

    fn clear(&mut self) -> Result<(), StoreError> {
        self.data.clear();
        Ok(())
    }

    fn first(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self
            .data
            .first_key_value()
            .and_then(|(_k, v)| v.first())
            .cloned())
    }

    fn last(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self
            .data
            .last_key_value()
            .and_then(|(_k, v)| v.last())
            .cloned())
    }
}

#[inline(always)]
fn is_present(items: &[Tuple], id: &DBIdType) -> bool {
    items.iter().any(|i| i.id == *id)
}

#[inline(always)]
fn extract(items: &[Tuple], id: &DBIdType) -> Option<Tuple> {
    items.iter().find(|i| i.id == *id).cloned()
}

#[inline(always)]
fn remove(items: &mut Vec<Tuple>, id: &DBIdType) -> Option<Tuple> {
    items.extract_if(.., |f| f.id == *id).next()
}

#[inline(always)]
fn replace(items: &mut [Tuple], id: &DBIdType, tuple: Tuple) -> Option<Tuple> {
    items
        .iter_mut()
        .try_for_each(|t| {
            if t.id == *id {
                let ret = t.clone();
                *t = tuple.clone();
                return std::ops::ControlFlow::Break(Some(ret));
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
    fn test_vec_id() {
        let mut p = make_page();
        let id = DBIdType::Vec(b"my_key".to_vec());
        p.add(Tuple::new_with(id.clone(), b"payload", None, None))
            .unwrap();
        assert!(p.contains(&id.clone()).unwrap());
        assert_eq!(p.get(&id).unwrap().unwrap().data.to_vec(), b"payload");
    }
}
