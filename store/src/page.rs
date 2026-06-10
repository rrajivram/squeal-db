use std::{
    collections::{BTreeMap, btree_map::Values},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use bitflags::bitflags;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{db::DBSizeType, error::StoreError, tuple::Tuple};

#[derive(Debug, Serialize, Deserialize)]
struct PageDto {
    data: Arc<BTreeMap<DBSizeType, Arc<Tuple>>>,
    #[serde(with = "postcard::fixint::le")]
    next_page: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    data_size: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    capacity: DBSizeType,
    flags: PageFlags,
}

bitflags! {
    #[derive(Debug,Serialize,Deserialize,Clone, Copy,PartialEq)]
    struct PageFlags: u8 {
        const NONE=0;
        const PINNED = 1;
    }
}

const PAGE_OVERHEAD: usize = 24;
#[derive(Debug, Serialize, Deserialize)]
#[serde(into = "PageDto", from = "PageDto")]
pub(crate) struct Page {
    data: Arc<BTreeMap<DBSizeType, Arc<Tuple>>>,
    next_page: AtomicU64,
    dirty: AtomicBool,
    data_size: DBSizeType,
    capacity: DBSizeType,
    flags: PageFlags,
}

#[derive(Debug)]
pub(crate) struct PageIterator<'a> {
    iter: Values<'a, DBSizeType, Arc<Tuple>>,
}

impl Page {
    pub(crate) fn new(size: DBSizeType) -> Self {
        let ds = size - PAGE_OVERHEAD as DBSizeType;
        Self {
            data: Arc::new(BTreeMap::new()),
            next_page: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            data_size: ds,
            capacity: ds,
            flags: PageFlags::NONE,
        }
    }

    pub(crate) fn get_next_page(&self) -> DBSizeType {
        self.next_page.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_next_page(&self, next_page: DBSizeType) {
        self.next_page
            .store(next_page, std::sync::atomic::Ordering::Relaxed);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn set_dirty(&self, dirty: bool) {
        self.dirty
            .store(dirty, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn iter<'a>(&'a self) -> PageIterator<'a> {
        PageIterator {
            iter: self.data.values(),
        }
    }

    pub(crate) fn can_store(&self, tuple: &Tuple) -> bool {
        tuple.size() < self.capacity
    }

    pub(crate) fn add_tuple(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if !self.can_store(&tuple) {
            return Err(StoreError::PageCapacityError);
        }
        if let Some(data) = Arc::get_mut(&mut self.data) {
            if data.contains_key(&tuple.id) {
                return Err(StoreError::DuplicateKey(tuple.id));
            }
            let sz = tuple.size();
            data.insert(tuple.id, Arc::new(tuple));
            self.capacity -= sz;
            self.set_dirty(true);
            Ok(())
        } else {
            return Err(StoreError::LockContentionError);
        }
    }

    pub(crate) fn contains(&self, id: DBSizeType) -> bool {
        self.data.contains_key(&id)
    }

    pub(crate) fn get(&self, id: DBSizeType) -> Option<Arc<Tuple>> {
        if let Some(v) = self.data.get(&id) {
            Some(v.clone())
        } else {
            None
        }
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut v = to_allocvec(&self).unwrap_or_default();
        if v.len() < self.data_size as usize + PAGE_OVERHEAD {
            v.append(&mut vec![
                0u8;
                (self.data_size as usize + PAGE_OVERHEAD) - v.len()
            ]);
        }
        return v;
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Page, StoreError> {
        let p: Page = from_bytes(bytes)?;
        Ok(p)
    }
}

impl<'a> Iterator for PageIterator<'a> {
    type Item = &'a Arc<Tuple>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl From<PageDto> for Page {
    fn from(value: PageDto) -> Self {
        Self {
            data: value.data.clone(),
            next_page: AtomicU64::new(value.next_page),
            dirty: AtomicBool::new(false),
            data_size: value.data_size,
            capacity: value.capacity,
            flags: value.flags,
        }
    }
}

impl From<Page> for PageDto {
    fn from(value: Page) -> Self {
        Self {
            data: value.data.clone(),
            next_page: value.next_page.load(std::sync::atomic::Ordering::Relaxed),
            data_size: value.data_size,
            capacity: value.capacity,
            flags: value.flags,
        }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            next_page: AtomicU64::new(self.next_page.load(std::sync::atomic::Ordering::Relaxed)),
            dirty: AtomicBool::new(self.dirty.load(std::sync::atomic::Ordering::Relaxed)),
            data_size: self.data_size,
            capacity: self.capacity,
            flags: self.flags,
        }
    }
}

impl PartialEq for Page {
    fn eq(&self, rhs: &Self) -> bool {
        if self.capacity != rhs.capacity
            || self.data_size != rhs.data_size
            || self.next_page.load(std::sync::atomic::Ordering::Relaxed)
                != rhs.next_page.load(std::sync::atomic::Ordering::Relaxed)
            || self.flags != rhs.flags
            || self.data != rhs.data
        {
            return false;
        }
        return true;
    }
}

unsafe impl Sync for Page {}
unsafe impl Send for Page {}

#[cfg(test)]
mod tests {
    use crate::{error::StoreError, page::Page, tuple::Tuple};

    #[test]
    fn page_test_1() {
        let mut p = Page::new(200);
        assert!(p.add_tuple(Tuple::new(1, b"abcdefabcd")).is_ok());
        assert!(p.add_tuple(Tuple::new(2, b"abcdefabcd")).is_ok());
        assert!(matches!(
            p.add_tuple(Tuple::new(2, b"aa")),
            Err(StoreError::DuplicateKey(_))
        ));
        assert_eq!(p.is_dirty(), true);
        assert_eq!(p.can_store(&Tuple::new(3, b"abcd")), true);
    }

    #[test]
    fn page_test_2() {
        let mut p = Page::new(77);
        assert!(p.add_tuple(Tuple::new(1, b"abcdefabcd")).is_ok());
        assert!(p.add_tuple(Tuple::new(2, b"abcdefabcd")).is_ok());
        assert_eq!(p.capacity, 1);
        assert!(matches!(
            p.add_tuple(Tuple::new(3, b"abcd")),
            Err(StoreError::PageCapacityError)
        ));
    }

    #[test]
    fn page_test_3() {
        let mut p = Page::new(1000);
        // 1000 - 24 = 976 avaolable - at 22 b/tuple  = 44 max
        for i in 0..44 {
            assert!(
                p.add_tuple(Tuple::new(i, b"abcdef")).is_ok(),
                "Failed at i={i}"
            );
        }
        assert!(matches!(
            p.add_tuple(Tuple::new(100, b"abcdef")),
            Err(StoreError::PageCapacityError)
        ));
        let b = p.to_bytes();
        assert!(b.len() == 1000);
        let p1 = Page::from_bytes(&b);
        assert!(p1.is_ok());
        let p1 = p1.unwrap();
        assert_eq!(p1, p);
    }
}
