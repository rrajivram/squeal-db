use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU16, AtomicU64},
};

use portable_atomic::AtomicU128;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp,
    db::DBSizeType,
    error::StoreError,
    logger::{Logger, LsnId},
    pages::{PageTuple, anytuple::AnyTuplePage, fixedtuple::FixedTuplePage},
    tuple::{DBIdType, Tuple},
};
use atomic_bitfield::AtomicBitField as _;
#[derive(Debug, Serialize, Deserialize)]
struct PageDto {
    data: Vec<u8>,
    #[serde(with = "postcard::fixint::le")]
    next_page: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    data_size: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    capacity: DBSizeType,
    record_size: Option<usize>,
    lsn: LsnId,
    flags: u16,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct PageId(DBSizeType);

const NONE: u16 = 0;
const PINNED: u16 = 1;
const INDEX_PAGE: u16 = 2;
const RESERVED_FLAGS: u16 = 0x0f;

const PAGE_OVERHEAD: usize = size_of::<PageDto>() - size_of::<Vec<u8>>();

// PageType bundles all the constraints Page<PT> needs on its data field.
// Baking `Item = Self` in here means every impl block just needs `PT: PageType`.
pub(crate) trait PageType: PageTuple + Clone + PartialEq + std::fmt::Debug {}
impl<T> PageType for T where T: PageTuple + Clone + PartialEq + std::fmt::Debug {}

impl Eq for PageId {}

///Page Invariants
/// when written lsn = non-zero
/// Rows added must have txn id and undo id set
#[derive(Serialize, Deserialize, Debug)]
#[serde(into = "PageDto", from = "PageDto")]
// PT doesn't need Serialize/Deserialize — PageDto handles that via to_bytes/from_bytes.
//#[serde(bound = "PT: PageType")]
pub(crate) struct Page {
    data: Arc<dyn PageTuple>,
    next_page: AtomicU64,
    dirty: AtomicBool,
    data_size: DBSizeType,
    capacity: DBSizeType,
    record_size: Option<usize>,
    lsn: RwLock<LsnId>,
    flags: AtomicU16,
    accessed: AtomicU128,
    saved: AtomicU128,
    written: AtomicU128,
}

pub(crate) struct PageTupleIterator {
    data: std::vec::IntoIter<Tuple>,
}

impl Page {
    pub(crate) fn new_data(size: DBSizeType) -> Self {
        Self::new(size, NONE, None)
    }

    pub(crate) fn new_pinned(size: DBSizeType) -> Self {
        Self::new(size, PINNED, None)
    }

    pub(crate) fn new_indexed(size: DBSizeType, record_size: usize) -> Self {
        Self::new(size, INDEX_PAGE, Some(record_size))
    }

    fn new(size: DBSizeType, flags: u16, record_size: Option<usize>) -> Self {
        let ds = size - PAGE_OVERHEAD as DBSizeType;
        let ts = timestamp();
        let pt: Arc<dyn PageTuple> = if let Some(record_size) = record_size {
            Arc::new(FixedTuplePage::new(record_size))
        } else {
            Arc::new(AnyTuplePage::new())
        };
        Self {
            data: pt,
            next_page: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            data_size: ds,
            capacity: ds,
            record_size,
            lsn: RwLock::new(LsnId(0)),
            flags: AtomicU16::new(flags),
            accessed: AtomicU128::new(ts),
            saved: AtomicU128::new(ts),
            written: AtomicU128::new(ts),
        }
    }

    pub(crate) fn get_data_size(&self) -> DBSizeType {
        self.data_size
    }

    pub(crate) fn lsn_id(&self) -> Result<LsnId, StoreError> {
        Ok(self.lsn.read()?.clone())
    }

    pub(crate) fn is_pinned(&self) -> bool {
        self.flags.load(std::sync::atomic::Ordering::Relaxed) & PINNED != 0
    }

    pub(crate) fn is_index_page(&self) -> bool {
        self.flags.load(std::sync::atomic::Ordering::Relaxed) & INDEX_PAGE != 0
    }

    pub(crate) fn get_next_page(&self) -> PageId {
        PageId(self.next_page.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_next_page(&self, next_page: PageId) -> Result<(), StoreError> {
        self.next_page
            .store(next_page.0, std::sync::atomic::Ordering::Relaxed);
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn set_dirty(&self, dirty: bool) -> Result<(), StoreError> {
        self.dirty
            .store(dirty, std::sync::atomic::Ordering::Relaxed);
        if dirty {
            *self.lsn.write()? = Logger::last_lsn();
        }
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<(), StoreError> {
        self.data.clear()?;
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn iter(&self) -> PageTupleIterator {
        PageTupleIterator {
            data: self.data.values().unwrap_or_default().into_iter(),
        }
    }

    pub(crate) fn can_store(&self, tuple: &Tuple) -> bool {
        tuple.size() < self.capacity
    }

    pub(crate) fn add_tuple(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if !self.can_store(&tuple) {
            return Err(StoreError::PageCapacityError);
        }
        let sz = tuple.size();
        self.data.add(tuple)?;
        self.capacity -= sz;
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn count(&self) -> Result<usize, StoreError> {
        Ok(self.data.count()?)
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

    pub(crate) fn contains(&self, id: DBIdType) -> Result<bool, StoreError> {
        self.data.contains(&id)
    }

    pub(crate) fn get(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        self.data.get(&id)
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut v = to_allocvec(&self).unwrap_or_default();
        if v.len() < self.data_size as usize + PAGE_OVERHEAD {
            v.append(&mut vec![
                0u8;
                (self.data_size as usize + PAGE_OVERHEAD) - v.len()
            ]);
        }
        v
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Ok(from_bytes(bytes)?)
    }

    pub(crate) fn set_page_flags(&self, flag: usize) -> Result<(), StoreError> {
        if flag & RESERVED_FLAGS as usize != 0 {
            panic!("Reserved bits cannot be set : {flag}");
        }
        self.flags
            .set_bit(flag, std::sync::atomic::Ordering::Relaxed);
        Ok(self.set_dirty(true)?)
    }

    pub(crate) fn clear_page_flag(&self, flag: usize) -> Result<(), StoreError> {
        if flag & RESERVED_FLAGS as usize != 0 {
            panic!("Reserved bits cannot be set: {flag}");
        }
        self.flags
            .clear_bit(flag, std::sync::atomic::Ordering::Relaxed);
        Ok(self.set_dirty(true)?)
    }

    pub(crate) fn is_flag_set(&self, flag: usize) -> bool {
        self.flags
            .get_bit(flag, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Iterator for PageTupleIterator {
    type Item = Tuple;
    fn next(&mut self) -> Option<Self::Item> {
        self.data.next()
    }
}

impl From<PageDto> for Page {
    fn from(value: PageDto) -> Self {
        let pt: Arc<dyn PageTuple> = if let Some(_record_size) = value.record_size {
            Arc::new(FixedTuplePage::from_bytes(&value.data).unwrap())
        } else {
            Arc::new(AnyTuplePage::from_bytes(&value.data).unwrap())
        };
        Self {
            data: pt,
            next_page: AtomicU64::new(value.next_page),
            dirty: AtomicBool::new(false),
            data_size: value.data_size,
            capacity: value.capacity,
            record_size: value.record_size,
            flags: AtomicU16::new(value.flags),
            lsn: RwLock::new(value.lsn),
            accessed: AtomicU128::new(timestamp()),
            written: AtomicU128::new(timestamp()),
            saved: AtomicU128::new(timestamp()),
        }
    }
}

impl From<Page> for PageDto {
    fn from(value: Page) -> Self {
        Self {
            data: value.data.to_bytes().unwrap(),
            next_page: value.next_page.load(std::sync::atomic::Ordering::Relaxed),
            data_size: value.data_size,
            capacity: value.capacity,
            flags: value.flags.load(std::sync::atomic::Ordering::Relaxed),
            lsn: value.lsn.read().unwrap().clone(),
            record_size: value.record_size,
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
            record_size: self.record_size,
            flags: AtomicU16::new(self.flags.load(std::sync::atomic::Ordering::Relaxed)),
            lsn: RwLock::new(self.lsn.read().unwrap().clone()),
            accessed: AtomicU128::new(self.accessed.load(std::sync::atomic::Ordering::Relaxed)),
            written: AtomicU128::new(self.written.load(std::sync::atomic::Ordering::Relaxed)),
            saved: AtomicU128::new(self.saved.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

impl PartialEq for Page {
    fn eq(&self, rhs: &Self) -> bool {
        self.capacity == rhs.capacity
            && self.data_size == rhs.data_size
            && self.next_page.load(std::sync::atomic::Ordering::Relaxed)
                == rhs.next_page.load(std::sync::atomic::Ordering::Relaxed)
            && self.flags.load(std::sync::atomic::Ordering::Relaxed)
                == rhs.flags.load(std::sync::atomic::Ordering::Relaxed)
            && self.record_size == rhs.record_size
    }
}

impl From<DBSizeType> for PageId {
    fn from(value: DBSizeType) -> Self {
        Self(value)
    }
}

impl From<PageId> for DBSizeType {
    fn from(value: PageId) -> Self {
        value.0
    }
}

impl std::fmt::Debug for dyn PageTuple + 'static {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl PageId {
    pub(crate) fn is_valid_next_page(&self) -> bool {
        self.0 != 0
    }

    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(&self)?)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Ok(from_bytes::<PageId>(bytes)?)
    }
}

unsafe impl Sync for Page {}
unsafe impl Send for Page {}

#[cfg(test)]
mod tests {

    use crate::{
        error::StoreError,
        page::{PAGE_OVERHEAD, Page},
        tuple::{DBIdType, Tuple},
    };

    type FixedPage = Page;

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
        let tuple_sz = Tuple::new(0, b"abcdef").size();
        let page_size = tuple_sz * 10 + PAGE_OVERHEAD as u64 + 1;
        let mut p = Page::new_data(page_size);
        for i in 0..10 {
            assert!(
                p.add_tuple(Tuple::new(i, b"abcdef")).is_ok(),
                "Failed at i={i}"
            );
        }
        let b = p.to_bytes();
        assert_eq!(b.len(), page_size as usize);
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
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(7, b"payload")).unwrap();
        let found = p.get(DBIdType::Int(7)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().data, b"payload");
        let missing = p.get(DBIdType::Int(99)).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn page_test_contains() {
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(3, b"x")).unwrap();
        assert!(p.contains(DBIdType::Int(3)).unwrap());
        assert!(!p.contains(DBIdType::Int(4)).unwrap());
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
        assert_eq!(p.get_next_page(), 0.into());
        p.set_next_page(42.into()).unwrap();
        assert_eq!(p.get_next_page(), 42.into());
    }

    #[test]
    fn page_test_pinned_flag() {
        let p = Page::new_data(1024);
        assert!(!p.is_pinned());
        let p_pinned = Page::new_pinned(1024);
        assert!(p_pinned.is_pinned());
    }

    #[test]
    fn page_test_dirty_state() {
        let p = Page::new_data(1024);
        assert!(p.is_dirty());
        p.set_dirty(false).unwrap();
        assert!(!p.is_dirty());
        p.set_dirty(true).unwrap();
        assert!(p.is_dirty());
    }

    #[test]
    fn page_test_roundtrip_preserves_tuples() {
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(1, b"first")).unwrap();
        p.add_tuple(Tuple::new(2, b"second")).unwrap();
        let bytes = p.to_bytes();
        let p2 = Page::from_bytes(&bytes).unwrap();
        assert!(p2.contains(DBIdType::Int(1)).unwrap());
        assert!(p2.contains(DBIdType::Int(2)).unwrap());
        assert_eq!(p2.get(DBIdType::Int(1)).unwrap().unwrap().data, b"first");
        assert_eq!(p2.get(DBIdType::Int(2)).unwrap().unwrap().data, b"second");
    }

    // --- PageIterator: AnyTuplePage ---

    #[test]
    fn page_iter_any_empty() {
        let p = Page::new_data(1024);
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn page_iter_any_correct_data() {
        let mut p = Page::new_data(4000);
        p.add_tuple(Tuple::new(1, b"alpha")).unwrap();
        p.add_tuple(Tuple::new(2, b"beta")).unwrap();
        p.add_tuple(Tuple::new(3, b"gamma")).unwrap();
        assert_eq!(p.iter().count(), 3);
        assert!(p.iter().any(|t| t.data == b"alpha"));
        assert!(p.iter().any(|t| t.data == b"beta"));
        assert!(p.iter().any(|t| t.data == b"gamma"));
    }

    #[test]
    fn page_iter_any_after_roundtrip() {
        let mut p = Page::new_data(2000);
        p.add_tuple(Tuple::new(10, b"x")).unwrap();
        p.add_tuple(Tuple::new(20, b"y")).unwrap();
        let p2 = Page::from_bytes(&p.to_bytes()).unwrap();
        assert_eq!(p2.iter().count(), 2);
    }

    // --- PageIterator: FixedTuplePage (new_indexed) ---

    #[test]
    fn page_iter_fixed_empty() {
        let record_size = Tuple::new(0, b"xxxx").size() as usize;
        let p = FixedPage::new_indexed(1024, record_size);
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn page_iter_fixed_correct_count() {
        let record_size = Tuple::new(0, b"data").size() as usize;
        let mut p = FixedPage::new_indexed(4000, record_size);
        for i in 0..4u64 {
            p.add_tuple(Tuple::new(i, b"data")).unwrap();
        }
        assert_eq!(p.iter().count(), 4);
    }

    #[test]
    fn page_iter_fixed_correct_data() {
        let record_size = Tuple::new(0, b"hello").size() as usize;
        let mut p = FixedPage::new_indexed(4000, record_size);
        p.add_tuple(Tuple::new(10, b"hello")).unwrap();
        p.add_tuple(Tuple::new(20, b"hello")).unwrap();
        assert!(p.iter().all(|t| t.data == b"hello"));
        let mut ids: Vec<u64> = p
            .iter()
            .map(|t| match t.id {
                DBIdType::Int(n) => n,
                _ => panic!("unexpected id type"),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec![10, 20]);
    }

    #[test]
    fn page_iter_fixed_oversized_rejected_iter_stays_empty() {
        let record_size = Tuple::new(0, b"hi").size() as usize;
        let mut p = FixedPage::new_indexed(4000, record_size);
        assert!(p.add_tuple(Tuple::new(1, b"way_too_long_payload")).is_err());
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn page_iter_fixed_after_roundtrip() {
        let record_size = Tuple::new(0, b"abc").size() as usize;
        let mut p = FixedPage::new_indexed(2000, record_size);
        p.add_tuple(Tuple::new(1, b"abc")).unwrap();
        p.add_tuple(Tuple::new(2, b"abc")).unwrap();
        let p2 = FixedPage::from_bytes(&p.to_bytes()).unwrap();
        assert_eq!(p2.iter().count(), 2);
        assert!(p2.iter().all(|t| t.data == b"abc"));
    }
}
