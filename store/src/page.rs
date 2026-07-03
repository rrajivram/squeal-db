/*
 * Page is a logical construct. It does nbot care about actual disk page size ,  though it is bound by it. i.e. capacity =0
 * if HAS_Overflow is set, next_page will point to continuation. This contunation logic is fully handled by PageBuffer
 */
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
    logger::{LsnClock, LsnId},
    pages::{PageTuple, anytuple::AnyTuplePage, fixedtuple::FixedTuplePage},
    tuple::{DBIdType, Tuple},
};
use atomic_bitfield::AtomicBitField as _;
// Header fields are serialized before the data payload so the header can be
// read from the start of a page's bytes without needing to know the data length.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct PageHeader {
    #[serde(with = "postcard::fixint::le")]
    pub(crate) next_page: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) page_data_size: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) page_used_size: DBSizeType,
    pub(crate) record_size: Option<usize>,
    pub(crate) lsn: LsnId,
    pub(crate) flags: u16,
}

impl PageHeader {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(self)?)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Ok(from_bytes(bytes)?)
    }
}

// Internal serde shim — header first, data last.
// postcard does not support #[serde(flatten)] so PageHeader fields are
// mirrored here in the same order; the From impls below keep them in sync.
#[derive(Debug, Serialize, Deserialize)]
struct PageDto {
    #[serde(with = "postcard::fixint::le")]
    next_page: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_data_size: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_used_size: DBSizeType,
    record_size: Option<usize>,
    lsn: LsnId,
    flags: u16,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct PageId(pub(crate) DBSizeType);

const NONE: u16 = 0;
const PINNED: u16 = 1;
const INDEX_PAGE: u16 = 1 << 1;
const HAS_OVERFLOW: u16 = 1 << 2;
const IS_OVERFLOW: u16 = 1 << 3;
const RESERVED_FLAGS: u16 = 0x0f;

pub(crate) const PAGE_OVERHEAD: usize = size_of::<PageDto>();

// Bytes reserved below the physical data-region size when deciding whether a
// page is "full" (can_store / handle_large_page_size's overflow trigger).
// `page_used_size` is a sum of individual `Tuple::size()` values, but the
// actual bytes written for a page's tuple store carry additional framing those
// sums never include:
//   - AnyTuplePage::to_bytes serializes `Vec<&Tuple>`, prefixed by a single
//     top-level varint(tuple_count).
//   - FixedTuplePage::to_bytes (index pages) wraps that in
//     `FixedTupleDto { size: usize, data: &[u8] }`, adding a varint(record_size)
//     and a varint(data.len()) around it — up to three stacked length varints
//     per page, none accounted by any individual tuple's size().
// NOTE: this must shrink the *fullness threshold* (usable_data_size), not
// `page_data_size` itself. `page_data_size = page_size - PAGE_OVERHEAD` is the
// true physical data-region size (used to know where the on-disk data region
// ends); page_used_size is allowed to fill right up to whatever ceiling
// can_store checks against. Folding this margin into PAGE_OVERHEAD instead
// just reduces page_data_size by the same amount as the threshold — a zero-sum
// relabeling that leaves no actual slack, since the header always pads out to
// exactly PAGE_OVERHEAD regardless of its value. The margin only creates real
// headroom by making the fullness ceiling strictly less than the true physical
// capacity — see `PageHeader::usable_data_size` / `Page::usable_data_size`.
// Postcard's varint is 7 bits/byte, so even a 3-byte varint covers values up to
// ~2M; this is deliberately oversized rather than tightly computed per page.
pub(crate) const USABLE_DATA_MARGIN: DBSizeType = 16;

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
    page_data_size: DBSizeType,
    page_used_size: DBSizeType,
    record_size: Option<usize>,
    lsn: RwLock<LsnId>,
    // This database's WAL clock, injected by the PageBuffer that owns the page.
    // `set_dirty` stamps `lsn` from it (was a process-global before). `None` for
    // a page not yet adopted by a buffer (freshly deserialized, or a standalone
    // test page); such a page keeps its lsn and, being low, writes promptly.
    lsn_clock: Option<Arc<LsnClock>>,
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
            page_data_size: ds,
            page_used_size: 0,
            record_size,
            lsn: RwLock::new(LsnId(0)),
            lsn_clock: None,
            flags: AtomicU16::new(flags),
            accessed: AtomicU128::new(ts),
            saved: AtomicU128::new(ts),
            written: AtomicU128::new(ts),
        }
    }

    pub(crate) fn header(&self) -> PageHeader {
        PageHeader {
            next_page: self.next_page.load(std::sync::atomic::Ordering::Relaxed),
            page_data_size: self.page_data_size,
            page_used_size: self.page_used_size,
            record_size: self.record_size,
            lsn: self.lsn.read().unwrap().clone(),
            flags: self.flags.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub(crate) fn get_data_size(&self) -> DBSizeType {
        self.page_data_size
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

    pub(crate) fn has_overflow(&self) -> bool {
        self.flags.load(std::sync::atomic::Ordering::Relaxed) & HAS_OVERFLOW != 0
    }

    pub(crate) fn set_overflow(&self, of: bool) {
        let mut flags = self.flags.load(std::sync::atomic::Ordering::Relaxed);
        if of {
            flags |= HAS_OVERFLOW;
        } else {
            flags &= !HAS_OVERFLOW;
        }
        self.flags
            .store(flags, std::sync::atomic::Ordering::Relaxed);
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
            // Stamp with this database's flush watermark, so the writer defers
            // this page until the watermark advances past it (WAL: the change's
            // redo record becomes durable first). `None` only for a page a buffer
            // hasn't adopted yet; it keeps its existing (low) lsn and writes
            // promptly. Was a process-global read; now per-database.
            if let Some(clock) = &self.lsn_clock {
                let w = clock.last_written();
                // Cold start: last_written is the u64::MAX sentinel ("nothing
                // durable yet; treat as fully durable"). Stamping u64::MAX would
                // defer this page FOREVER — the watermark only ever moves to real
                // (smaller) redo lsns, so `page.lsn < last_written` can never
                // become true, and the write lingers in the writer's `pending`
                // until shutdown. If the page is freed and reused in the
                // meantime, that stale write then clobbers the new occupant on
                // the shutdown flush. Stamp low so a cold-dirtied page is written
                // promptly instead of forever-deferred.
                *self.lsn.write()? = if w.0 == u64::MAX { LsnId(0) } else { w };
            }
        }
        Ok(())
    }

    /// Adopt a page into a database's WAL clock. Called by the PageBuffer for
    /// every page it creates or loads, before the page is mutated; `set_dirty`
    /// then stamps from this clock, and copy-on-write clones inherit it.
    pub(crate) fn set_clock(&mut self, clock: Arc<LsnClock>) {
        self.lsn_clock = Some(clock);
    }

    /// Mutable access to the tuple store. Sound because every mutating Page
    /// method takes `&mut self`, only reachable after `Arc::make_mut(&mut
    /// Arc<Page>)` has made this Page uniquely owned — so its `data` Arc has a
    /// single strong ref and `get_mut` succeeds. A `None` here would mean that
    /// copy-on-write invariant was violated, so it surfaces as an error rather
    /// than silently corrupting a shared page.
    fn data_mut(&mut self) -> Result<&mut (dyn PageTuple + 'static), StoreError> {
        Arc::get_mut(&mut self.data).ok_or_else(|| {
            StoreError::UnknownError(
                "page tuple store aliased while mutating through &mut Page (COW invariant violated)"
                    .into(),
            )
        })
    }

    pub(crate) fn clear(&mut self) -> Result<(), StoreError> {
        self.data_mut()?.clear()?;
        self.page_used_size = 0;
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn iter(&self) -> PageTupleIterator {
        PageTupleIterator {
            data: self.data.values().unwrap_or_default().into_iter(),
        }
    }

    // Threshold used for the fullness decision — strictly less than the true
    // physical data-region size (page_data_size), reserving room for the
    // page-serialization framing (e.g. the tuple-count varint) that no
    // individual tuple's size() accounts for. See USABLE_DATA_MARGIN.
    fn usable_data_size(&self) -> DBSizeType {
        self.page_data_size.saturating_sub(USABLE_DATA_MARGIN)
    }

    pub(crate) fn can_store(&self, tuple: &Tuple) -> bool {
        // Accept a tuple only if it actually fits. Exception: an empty page
        // always accepts, so a single tuple larger than a whole page still has
        // a home — PageBuffer then spills that one tuple across an overflow
        // chain. This keeps overflow off the common path: ordinary pages fill to
        // capacity and link to the next data page instead of every near-full
        // page spilling into (and rewriting) an overflow chain on each write.
        self.page_used_size == 0 || self.page_used_size + tuple.size() <= self.usable_data_size()
    }

    pub(crate) fn add_tuple(&mut self, tuple: Tuple) -> Result<(), StoreError> {
        if !self.can_store(&tuple) {
            return Err(StoreError::PageCapacityError);
        }
        let sz = tuple.size();
        self.data_mut()?.add(tuple)?;
        self.page_used_size += sz;
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn remove_tuple(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
        let old = self.data_mut()?.remove(id)?;
        // checked_sub: an underflow here would wrap page_used_size to ~u64::MAX,
        // which then drives handle_large_page_size to allocate a giant overflow
        // chain (observed: 21 GB file / OOM). Surface it as an error instead.
        self.page_used_size = self.page_used_size.checked_sub(old.size()).ok_or_else(|| {
            StoreError::UnknownError(format!(
                "remove_tuple used_size underflow: used={} old={}",
                self.page_used_size,
                old.size()
            ))
        })?;
        self.set_dirty(true)?;
        Ok(old)
    }

    pub(crate) fn replace_tuple(
        &mut self,
        id: &DBIdType,
        tuple: Tuple,
    ) -> Result<Tuple, StoreError> {
        let new_size = tuple.size();
        let old = self.data_mut()?.replace(id, tuple)?;
        let old_size = old.size();
        self.page_used_size = self.page_used_size.checked_sub(old_size).ok_or_else(|| {
            StoreError::UnknownError(format!(
                "replace_tuple used_size underflow: used={} old={}",
                self.page_used_size, old_size
            ))
        })?;
        self.page_used_size += new_size;
        self.set_dirty(true)?;
        Ok(old)
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
        /*         let mut v = to_allocvec(&self).unwrap_or_default();
               if v.len() < self.data_size as usize + PAGE_OVERHEAD {
                   v.append(&mut vec![
                       0u8;
                       (self.data_size as usize + PAGE_OVERHEAD) - v.len()
                   ]);
               }
               v
        */
        let mut v = to_allocvec(&self.header()).unwrap_or_default();
        if v.len() < PAGE_OVERHEAD {
            v.append(&mut vec![0u8; PAGE_OVERHEAD - v.len()]);
        }
        v.extend_from_slice(&self.data.to_bytes().unwrap_or_default());
        if v.len() < self.page_data_size as usize + PAGE_OVERHEAD {
            v.append(&mut vec![
                0u8;
                (self.page_data_size as usize + PAGE_OVERHEAD)
                    - v.len()
            ]);
        }
        v
    }

    pub(crate) fn to_data_bytes(&self) -> Vec<u8> {
        let mut v = self.data.to_bytes().unwrap_or_default();
        if v.len() < self.page_data_size as usize {
            v.append(&mut vec![0u8; self.page_data_size as usize - v.len()]);
        }
        v
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        let header = &bytes[..PAGE_OVERHEAD];
        let header = from_bytes::<PageHeader>(&header)?;
        let data = &bytes[PAGE_OVERHEAD..];
        // Deserialize the tuple payload with `?` rather than routing through the
        // `From<PageDto>` impl, which `.unwrap()`s and would turn a torn/partial
        // read into a panic instead of a recoverable StoreError.
        let pt: Arc<dyn PageTuple> = if header.record_size.is_some() {
            Arc::new(FixedTuplePage::from_bytes(data)?)
        } else {
            Arc::new(AnyTuplePage::from_bytes(data)?)
        };
        Ok(Self {
            data: pt,
            next_page: AtomicU64::new(header.next_page),
            dirty: AtomicBool::new(false),
            page_data_size: header.page_data_size,
            page_used_size: header.page_used_size,
            record_size: header.record_size,
            flags: AtomicU16::new(header.flags),
            lsn: RwLock::new(header.lsn),
            lsn_clock: None,
            accessed: AtomicU128::new(timestamp()),
            written: AtomicU128::new(timestamp()),
            saved: AtomicU128::new(timestamp()),
        })
    }

    pub(crate) fn set_page_flags(&self, flag: usize) -> Result<(), StoreError> {
        if (1usize << flag) & RESERVED_FLAGS as usize != 0 {
            panic!("Reserved bits cannot be set : {flag}");
        }
        self.flags
            .set_bit(flag, std::sync::atomic::Ordering::Relaxed);
        Ok(self.set_dirty(true)?)
    }

    pub(crate) fn clear_page_flag(&self, flag: usize) -> Result<(), StoreError> {
        if (1usize << flag) & RESERVED_FLAGS as usize != 0 {
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

impl PageHeader {
    pub(crate) fn next_page(&self) -> PageId {
        self.next_page.into()
    }

    pub(crate) fn set_next_page(&mut self, id: PageId) {
        self.next_page = id.into();
    }

    pub(crate) fn is_overflow(&self) -> bool {
        self.flags & IS_OVERFLOW == IS_OVERFLOW
    }

    pub(crate) fn set_is_overflow(&mut self) {
        self.flags |= IS_OVERFLOW;
        self.clear_has_overflow();
    }

    pub(crate) fn clear_is_overflow(&mut self) {
        self.flags &= !IS_OVERFLOW
    }

    pub(crate) fn has_overflow(&self) -> bool {
        self.flags & HAS_OVERFLOW == HAS_OVERFLOW
    }

    pub(crate) fn set_has_overflow(&mut self) {
        self.flags |= HAS_OVERFLOW;
        self.clear_is_overflow();
    }

    pub(crate) fn clear_has_overflow(&mut self) {
        self.flags &= !HAS_OVERFLOW;
    }

    pub(crate) fn header_size() -> usize {
        PAGE_OVERHEAD
    }

    pub(crate) fn used_size(&self) -> DBSizeType {
        self.page_used_size
    }

    pub(crate) fn usable_data_size(&self) -> DBSizeType {
        self.page_data_size.saturating_sub(USABLE_DATA_MARGIN)
    }

    pub(crate) fn total_page_size(&self) -> usize {
        self.page_used_size as usize + Self::header_size()
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
            page_data_size: value.page_data_size,
            page_used_size: value.page_used_size,
            record_size: value.record_size,
            flags: AtomicU16::new(value.flags),
            lsn: RwLock::new(value.lsn),
            lsn_clock: None,
            accessed: AtomicU128::new(timestamp()),
            written: AtomicU128::new(timestamp()),
            saved: AtomicU128::new(timestamp()),
        }
    }
}

impl From<Page> for PageDto {
    fn from(value: Page) -> Self {
        Self {
            next_page: value.next_page.load(std::sync::atomic::Ordering::Relaxed),
            page_data_size: value.page_data_size,
            page_used_size: value.page_used_size,
            record_size: value.record_size,
            lsn: value.lsn.read().unwrap().clone(),
            flags: value.flags.load(std::sync::atomic::Ordering::Relaxed),
            data: value.data.to_bytes().unwrap(),
        }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Self {
            // Deep-copy, not Arc::clone: a Page is mutated through `&mut Page`
            // obtained via Arc::make_mut, which calls this clone when the page is
            // shared. Sharing the inner Arc instead would let the freshly-cloned
            // (now "unique") page still alias the original's store, leaking
            // mutations back into the cached page or a page mid-serialization on
            // the writer thread — corrupting used_size/data and losing updates.
            data: self.data.deep_clone(),
            next_page: AtomicU64::new(self.next_page.load(std::sync::atomic::Ordering::Relaxed)),
            dirty: AtomicBool::new(self.dirty.load(std::sync::atomic::Ordering::Relaxed)),
            page_data_size: self.page_data_size,
            page_used_size: self.page_used_size,
            record_size: self.record_size,
            flags: AtomicU16::new(self.flags.load(std::sync::atomic::Ordering::Relaxed)),
            lsn: RwLock::new(self.lsn.read().unwrap().clone()),
            lsn_clock: self.lsn_clock.clone(),
            accessed: AtomicU128::new(self.accessed.load(std::sync::atomic::Ordering::Relaxed)),
            written: AtomicU128::new(self.written.load(std::sync::atomic::Ordering::Relaxed)),
            saved: AtomicU128::new(self.saved.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

impl PartialEq for Page {
    fn eq(&self, rhs: &Self) -> bool {
        self.page_used_size == rhs.page_used_size
            && self.page_data_size == rhs.page_data_size
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

impl From<usize> for PageId {
    fn from(value: usize) -> Self {
        Self { 0: value as u64 }
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

// SAFETY: needed only because `data: Arc<dyn PageTuple>` is a trait object
// without Send+Sync bounds. The concrete stores (AnyTuplePage/FixedTuplePage)
// are Send+Sync plain data. Sharing `&Page` across threads is sound: `&self`
// methods only read the store (concurrent BTreeMap reads are fine) and the
// genuinely-shared mutable fields are atomics / a lock. Mutation requires
// `&mut Page`, which the copy-on-write model (Arc::make_mut) hands out only for
// a uniquely-owned page, so no thread can observe a page while it is written.
unsafe impl Sync for Page {}
unsafe impl Send for Page {}

#[cfg(test)]
mod tests {

    use crate::{
        error::StoreError,
        page::{PAGE_OVERHEAD, Page, USABLE_DATA_MARGIN},
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
    fn page_test_accurate_page_bytes() {
        let tuple_sz = Tuple::new(0, b"abcdef").size();
        // can_store's fullness ceiling is page_data_size - USABLE_DATA_MARGIN
        // (room reserved for page-serialization framing the per-tuple size sum
        // doesn't see — see USABLE_DATA_MARGIN), so fitting 10 tuples with 1
        // byte to spare needs that margin folded into the requested page size.
        let page_size = tuple_sz * 10 + USABLE_DATA_MARGIN + PAGE_OVERHEAD as u64 + 1;
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
        assert_eq!(found.unwrap().data.to_vec(), b"payload");
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
        assert_eq!(p.get_next_page(), 0usize.into());
        p.set_next_page(42usize.into()).unwrap();
        assert_eq!(p.get_next_page(), 42usize.into());
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
        assert_eq!(p2.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(), b"first");
        assert_eq!(p2.get(DBIdType::Int(2)).unwrap().unwrap().data.to_vec(), b"second");
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
        assert!(p.iter().any(|t| t.data.to_vec() == b"alpha"));
        assert!(p.iter().any(|t| t.data.to_vec() == b"beta"));
        assert!(p.iter().any(|t| t.data.to_vec() == b"gamma"));
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
        assert!(p.iter().all(|t| t.data.to_vec() == b"hello"));
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
        assert!(p2.iter().all(|t| t.data.to_vec() == b"abc"));
    }

    #[test]
    fn test_can_store_true_for_fresh_page() {
        let p = Page::new_data(1000);
        assert!(p.can_store(&Tuple::new(1, b"anything")));
    }

    #[test]
    fn test_any_size_tuple_accepted_while_page_has_capacity() {
        let mut p = Page::new_data(1000);
        let big_data = vec![0u8; 2000]; // much larger than page_data_size
        assert!(p.can_store(&Tuple::new(1, &big_data)));
        assert!(p.add_tuple(Tuple::new(1, &big_data)).is_ok());
    }

    #[test]
    fn test_used_size_tracks_tuple_size() {
        let mut p = Page::new_data(1000);
        let t = Tuple::new(1, b"hello");
        let expected = t.size();
        p.add_tuple(t).unwrap();
        assert_eq!(p.header().page_used_size, expected);
    }

    #[test]
    fn test_can_store_false_after_oversized_tuple() {
        let mut p = Page::new_data(1000);
        let big_data = vec![0u8; 2000]; // page_used_size will exceed page_data_size
        p.add_tuple(Tuple::new(1, &big_data)).unwrap();
        assert!(!p.can_store(&Tuple::new(2, b"x")));
    }

    #[test]
    fn test_second_tuple_rejected_when_page_full() {
        let mut p = Page::new_data(1000);
        let big_data = vec![0u8; 2000];
        p.add_tuple(Tuple::new(1, &big_data)).unwrap();
        assert!(matches!(
            p.add_tuple(Tuple::new(2, b"tiny")),
            Err(StoreError::PageCapacityError)
        ));
    }
}
