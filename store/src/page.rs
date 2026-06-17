use std::{
    collections::{BTreeMap, btree_map::Values},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use bitflags::bitflags;
use portable_atomic::AtomicU128;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp,
    db::DBSizeType,
    error::StoreError,
    logger::LsnId,
    tuple::{DBIdType, Tuple},
};

// Note: LSN is not an option here as it needs to be set to write a page
#[derive(Debug, Serialize, Deserialize)]
struct PageDto {
    data: Arc<BTreeMap<DBIdType, Arc<Tuple>>>,
    #[serde(with = "postcard::fixint::le")]
    next_page: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    data_size: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    capacity: DBSizeType,
    lsn: Option<LsnId>,
    flags: PageFlags,
}

bitflags! {
    #[derive(Debug,Serialize,Deserialize,Clone, Copy,PartialEq,Default)]
    struct PageFlags: u8 {
        const NONE=0;
        const PINNED = 1;
        const INDEX_PAGE = 0b10;
    }
}

const PAGE_OVERHEAD: usize = size_of::<PageDto>() - size_of::<Arc<u64>>();

///Page Invariants
/// when written lsn = non-zero
/// Rows added must have txn id and undo id set
#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(into = "PageDto", from = "PageDto")]
pub(crate) struct Page {
    data: Arc<BTreeMap<DBIdType, Arc<Tuple>>>,
    next_page: AtomicU64,
    dirty: AtomicBool,
    data_size: DBSizeType,
    capacity: DBSizeType,
    lsn: Option<LsnId>,
    flags: PageFlags,
    accessed: AtomicU128,
    saved: AtomicU128,
    written: AtomicU128,
}

#[derive(Debug)]
pub(crate) struct PageIterator<'a> {
    iter: Values<'a, DBIdType, Arc<Tuple>>,
}

impl Page {
    pub(crate) fn new_data(size: DBSizeType) -> Self {
        Self::new(size, None, PageFlags::NONE)
    }

    pub(crate) fn new_pinned(size: DBSizeType) -> Self {
        Self::new(size, Some(LsnId(0)), PageFlags::PINNED)
    }

    pub(crate) fn new_indexed(size: DBSizeType) -> Self {
        Self::new(size, None, PageFlags::INDEX_PAGE)
    }

    fn new(size: DBSizeType, lsn_id: Option<LsnId>, flags: PageFlags) -> Self {
        let ds = size - PAGE_OVERHEAD as DBSizeType;
        Self {
            data: Arc::new(BTreeMap::new()),
            next_page: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            data_size: ds,
            capacity: ds,
            lsn: lsn_id, //pinned  pages are for system use, so written immediate;y
            flags: flags,
            ..Default::default()
        }
    }

    pub(crate) fn lsn_id(&self) -> Option<LsnId> {
        self.lsn
    }

    pub(crate) fn is_pinned(&self) -> bool {
        self.flags.contains(PageFlags::PINNED)
    }

    pub(crate) fn is_index_page(&self) -> bool {
        self.flags.contains(PageFlags::INDEX_PAGE)
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
            data.insert(tuple.id.clone(), Arc::new(tuple));
            self.capacity -= sz;
            self.set_dirty(true);
            Ok(())
        } else {
            return Err(StoreError::LockContentionError);
        }
    }

    pub(crate) fn written(&self) {
        self.written
            .store(timestamp(), std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn accessed(&self) {
        self.accessed
            .store(timestamp(), std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn saved(&self) {
        self.saved
            .store(timestamp(), std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn contains(&self, id: DBIdType) -> bool {
        self.data.contains_key(&id)
    }

    pub(crate) fn get(&self, id: DBIdType) -> Option<Arc<Tuple>> {
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
            lsn: value.lsn,
            accessed: AtomicU128::new(timestamp()),
            written: AtomicU128::new(timestamp()),
            saved: AtomicU128::new(timestamp()),
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
            lsn: value.lsn.or(None),
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
            lsn: self.lsn,
            accessed: AtomicU128::new(self.accessed.load(std::sync::atomic::Ordering::Relaxed)),
            written: AtomicU128::new(self.written.load(std::sync::atomic::Ordering::Relaxed)),
            saved: AtomicU128::new(self.saved.load(std::sync::atomic::Ordering::Relaxed)),
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
    use crate::{
        error::StoreError,
        page::{PAGE_OVERHEAD, Page},
        tuple::Tuple,
    };

    #[test]
    fn page_test_unique_id() {
        let mut p = Page::new_data(2000);
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
    fn page_test_capacity() {
        let size = Tuple::new(1, b"abcdefabcd").size() * 2 + PAGE_OVERHEAD as u64;
        let mut p = Page::new_data(size + 1);
        assert!(p.add_tuple(Tuple::new(1, b"abcdefabcd")).is_ok());
        assert!(p.add_tuple(Tuple::new(2, b"abcdefabcd")).is_ok());
        assert_eq!(p.capacity, 1);
        assert!(matches!(
            p.add_tuple(Tuple::new(3, b"abcd")),
            Err(StoreError::PageCapacityError)
        ));
    }

    #[test]
    fn page_test_accurate_page_bytes() {
        let mut p = Page::new_data(1000);
        // 1000 - 24 = 976 avaolable - at 46 b/tuple  = 21 max
        for i in 0..10 {
            assert!(
                p.add_tuple(Tuple::new(i, b"abcdef")).is_ok(),
                "Failed at i={i}"
            );
        }
        let b = p.to_bytes();
        assert!(b.len() == 1000);
        let p1 = Page::from_bytes(&b);
        assert!(p1.is_ok());
        let p1 = p1.unwrap();
        assert_eq!(p1, p);
    }

    #[test]
    fn page_test_4() {
        let p = Page::new_data(1024);
        let b = p.to_bytes();
        assert_eq!(b.len(), 1024);
        let p1 = Page::from_bytes(&b).unwrap();
        assert_eq!(p, p1);
    }

    #[test]
    fn page_test_get_existing_and_missing() {
        use crate::tuple::DBIdType;
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(7, b"payload")).unwrap();
        let found = p.get(DBIdType::Int(7));
        assert!(found.is_some());
        assert_eq!(found.unwrap().data, b"payload");
        let missing = p.get(DBIdType::Int(99));
        assert!(missing.is_none());
    }

    #[test]
    fn page_test_contains() {
        use crate::tuple::DBIdType;
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(3, b"x")).unwrap();
        assert!(p.contains(DBIdType::Int(3)));
        assert!(!p.contains(DBIdType::Int(4)));
    }

    #[test]
    fn page_test_iter_yields_all_tuples() {
        let mut p = Page::new_data(4000);
        for i in 0..5u64 {
            p.add_tuple(Tuple::new(i, b"data")).unwrap();
        }
        let count = p.iter().count();
        assert_eq!(count, 5);
    }

    #[test]
    fn page_test_next_page() {
        let p = Page::new_data(1024);
        assert_eq!(p.get_next_page(), 0);
        p.set_next_page(42);
        assert_eq!(p.get_next_page(), 42);
    }

    #[test]
    fn page_test_pinned_flag() {
        let p = Page::new_data(1024);
        assert!(!p.is_pinned());
        let p_pinned = Page::new_pinned(1024);
        assert!(p_pinned.is_pinned());
    }

    #[test]
    fn page_test_lsn_initially_none() {
        let p = Page::new_data(1024);
        assert!(p.lsn_id().is_none());
    }

    #[test]
    fn page_test_dirty_state() {
        let p = Page::new_data(1024);
        assert!(p.is_dirty());
        p.set_dirty(false);
        assert!(!p.is_dirty());
        p.set_dirty(true);
        assert!(p.is_dirty());
    }

    #[test]
    fn page_test_roundtrip_preserves_tuples() {
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(1, b"first")).unwrap();
        p.add_tuple(Tuple::new(2, b"second")).unwrap();
        let bytes = p.to_bytes();
        let p2 = Page::from_bytes(&bytes).unwrap();
        use crate::tuple::DBIdType;
        assert!(p2.contains(DBIdType::Int(1)));
        assert!(p2.contains(DBIdType::Int(2)));
        assert_eq!(p2.get(DBIdType::Int(1)).unwrap().data, b"first");
        assert_eq!(p2.get(DBIdType::Int(2)).unwrap().data, b"second");
    }
}
