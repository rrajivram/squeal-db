use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    db::DBSizeType,
    error::StoreError,
    pages::PageTuple,
    tuple::{DBIdType, Tuple},
};

// Insertion order, not id-sorted like AnyTuplePage's BTreeMap — a Run
// (crate::run::Run) needs its tuples read back in exactly the order they
// were appended (a sort run must stay sorted; nothing else here has any
// other ordering to preserve), which a keyed/sorted map can't give:
// AnyTuplePage in particular orders DBIdType::Int by a hash of the id,
// not the id's own value, specifically so B+Tree keys spread out for
// balance — exactly the opposite of what a Run needs. A plain Vec, kept
// in push order, is the whole fix.
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct RunPage {
    data: Vec<Tuple>,
}

impl RunPage {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Ok(Self {
            data: from_bytes(bytes)?,
        })
    }
}

impl PageTuple for RunPage {
    fn deep_clone(&self) -> Box<dyn PageTuple> {
        Box::new(self.clone())
    }

    fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.len())
    }

    fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        self.data.push(tuple);
        Ok(())
    }

    // A Run has no real per-tuple key (see this module's own doc
    // comment) — get/contains/replace/remove exist only to satisfy
    // PageTuple; crate::run::Run itself never calls them. Implemented as
    // honest linear scans over whatever id each tuple happens to carry,
    // rather than panicking, so they still behave sanely if something
    // generic (debug tooling, a future caller) ever does call them.
    fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
        Ok(self.data.iter().any(|t| t.id == *id))
    }

    fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.iter().find(|t| t.id == *id).cloned())
    }

    fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        let slot = self
            .data
            .iter_mut()
            .find(|t| t.id == *id)
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        Ok(std::mem::replace(slot, tuple))
    }

    fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        let pos = self
            .data
            .iter()
            .position(|t| t.id == id)
            .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
        Ok(self.data.remove(pos))
    }

    fn values(&self) -> Result<Vec<Tuple>, StoreError> {
        Ok(self.data.clone())
    }

    fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
        // Unused externally, same as AnyTuplePage's own keys() — kept
        // returning the hashed u64 rather than widening PageTuple's
        // signature for a method nothing reads.
        Ok(self.data.iter().map(|t| t.id.hashed()).collect())
    }

    fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(&self.data)?)
    }

    fn clear(&mut self) -> Result<(), StoreError> {
        self.data.clear();
        Ok(())
    }

    fn first(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.first().cloned())
    }

    fn last(&self) -> Result<Option<Tuple>, StoreError> {
        Ok(self.data.last().cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_preserves_insertion_order() {
        let mut p = RunPage::new();
        p.add(Tuple::new(0, b"c")).unwrap();
        p.add(Tuple::new(0, b"a")).unwrap();
        p.add(Tuple::new(0, b"b")).unwrap();
        let vals: Vec<Vec<u8>> = p.values().unwrap().into_iter().map(|t| t.data().to_vec()).collect();
        assert_eq!(vals, vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn test_add_allows_duplicate_ids() {
        // Unlike AnyTuplePage, a RunPage has no key uniqueness constraint
        // at all — every tuple in a Run carries the same placeholder id.
        let mut p = RunPage::new();
        p.add(Tuple::new(0, b"first")).unwrap();
        p.add(Tuple::new(0, b"second")).unwrap();
        assert_eq!(p.count().unwrap(), 2);
    }

    #[test]
    fn test_roundtrip_serialization_preserves_order() {
        let mut p = RunPage::new();
        p.add(Tuple::new(0, b"x")).unwrap();
        p.add(Tuple::new(0, b"y")).unwrap();
        let bytes = p.to_bytes().unwrap();
        let p2 = RunPage::from_bytes(&bytes).unwrap();
        let vals: Vec<Vec<u8>> = p2.values().unwrap().into_iter().map(|t| t.data().to_vec()).collect();
        assert_eq!(vals, vec![b"x".to_vec(), b"y".to_vec()]);
    }

    #[test]
    fn test_clear_empties_the_page() {
        let mut p = RunPage::new();
        p.add(Tuple::new(0, b"x")).unwrap();
        p.clear().unwrap();
        assert_eq!(p.count().unwrap(), 0);
        assert!(p.first().unwrap().is_none());
    }
}
