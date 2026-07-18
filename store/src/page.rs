/*
 * Page is a logical construct. It does nbot care about actual disk page size ,  though it is bound by it. i.e. capacity =0
 * if HAS_Overflow is set, next_page will point to continuation. This contunation logic is fully handled by PageBuffer
 */
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU16},
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
    // B-link tree high key: the exclusive upper bound of keys this index
    // page's subtree currently covers. `None` means unbounded — this page
    // has never had its upper range split away (the common case for a
    // never-split page, or the rightmost page at its level). See
    // PageInner's own comment for why this is bundled with next_page rather
    // than tracked separately.
    pub(crate) high_key: Option<DBIdType>,
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
    high_key: Option<DBIdType>,
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

// The tuple store and its byte-accounting must change together — locking
// them separately would let a reader observe a page_used_size that doesn't
// match the data it just read (or vice versa). See Page's own doc comment
// on why this is a lock, not an Arc<dyn PageTuple> mutated via
// Arc::make_mut: a single-record insert/update/remove only ever touches one
// key, so mutating in place under a write lock (what BTreeMap::insert/
// remove already do internally, no cloning involved) is strictly cheaper
// than cloning the whole store first just to get unique ownership.
//
// has_overflow and next_page live here too, not in the `flags`/`next_page`
// atomics alongside the other independent bits (PINNED, LEAF/INNER_NODE,
// ...) — they have the exact same must-change-together coupling with
// page_used_size/data as those two have with each other: HAS_OVERFLOW=true
// means "the tuple store's real content is bigger than one page, split
// across a chain starting at next_page," which is a statement *about*
// page_used_size/data, not an independent fact — and next_page's meaning
// (overflow-chain head vs. data-chain sibling) flips on has_overflow, so
// the two can never be read or written correctly one at a time. Tracking
// either as a separately-atomic field let a reader observe e.g.
// has_overflow=false (just cleared by a newer write) alongside
// page_used_size/data — or next_page — still holding an older, stale
// snapshot (not yet updated by that same newer write) — a real, confirmed
// bug: the writer thread's write_page reads a page's state *after* its
// per-page write lock has already been released (that's the whole point of
// writing asynchronously), so by the time it runs, a second, unrelated
// write to the same page can already be interleaving its own
// has_overflow/next_page/content update. Before Page became interior-mutable
// (see above), this was impossible — Arc::make_mut cloned the whole page on
// any concurrent access, so the writer thread's queued Arc was always a
// frozen, independent snapshot. Bundling has_overflow and next_page into
// this same lock restores that "one coherent snapshot" guarantee for the
// pieces of state that specifically need it. See
// handle_large_page_size in buffer.rs, which sets these two in either
// order (next_page then has_overflow, or has_overflow then next_page)
// across two separate calls — exactly the shape that produced the
// has_overflow bug and, until this field moved here too, still could for
// next_page.
//
// high_key (an index page's B-link tree upper bound — see PageHeader's own
// comment) lives here for the same reason: it changes in lockstep with
// next_page during a split (see BPlusTree::split_non_root_page), and a
// reader combining a stale high_key with a fresher next_page (or vice
// versa) would misnavigate exactly the way the has_overflow/next_page pair
// used to.
#[derive(Debug)]
struct PageInner {
    data: Box<dyn PageTuple>,
    page_used_size: DBSizeType,
    has_overflow: bool,
    next_page: DBSizeType,
    high_key: Option<DBIdType>,
}

///Page Invariants
/// when written lsn = non-zero
/// Rows added must have txn id and undo id set
#[derive(Serialize, Deserialize, Debug)]
#[serde(into = "PageDto", from = "PageDto")]
// PT doesn't need Serialize/Deserialize — PageDto handles that via to_bytes/from_bytes.
//#[serde(bound = "PT: PageType")]
pub(crate) struct Page {
    // See PageInner's own comment for why data, page_used_size, has_overflow,
    // and next_page are bundled behind one lock instead of separate fields.
    inner: RwLock<PageInner>,
    dirty: AtomicBool,
    page_data_size: DBSizeType,
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
        let pt: Box<dyn PageTuple> = if let Some(record_size) = record_size {
            Box::new(FixedTuplePage::new(record_size))
        } else {
            Box::new(AnyTuplePage::new())
        };
        Self {
            inner: RwLock::new(PageInner {
                data: pt,
                page_used_size: 0,
                has_overflow: false,
                next_page: 0,
                high_key: None,
            }),
            dirty: AtomicBool::new(true),
            page_data_size: ds,
            record_size,
            lsn: RwLock::new(LsnId(0)),
            lsn_clock: None,
            flags: AtomicU16::new(flags),
            accessed: AtomicU128::new(ts),
            saved: AtomicU128::new(ts),
            written: AtomicU128::new(ts),
        }
    }

    // Builds a PageHeader from an already-held `inner` read guard, folding
    // has_overflow (tracked in `inner`, see PageInner's own comment) into
    // the other, still-atomic flag bits into the one combined `u16` the
    // on-disk format expects, and reading next_page from `inner` too.
    // Shared by header()/to_bytes_snapshot() so both — and anyone combining
    // a header with `inner`'s data, like buffer.rs's write_page — read
    // has_overflow, next_page, and page_used_size/data from the exact same
    // locked view, never several separate ones.
    fn header_from_inner(&self, inner: &PageInner) -> PageHeader {
        let mut flags = self.flags.load(std::sync::atomic::Ordering::Relaxed);
        if inner.has_overflow {
            flags |= HAS_OVERFLOW;
        } else {
            flags &= !HAS_OVERFLOW;
        }
        PageHeader {
            next_page: inner.next_page,
            page_data_size: self.page_data_size,
            page_used_size: inner.page_used_size,
            record_size: self.record_size,
            lsn: *self.lsn.read().unwrap(),
            flags,
            high_key: inner.high_key.clone(),
        }
    }

    pub(crate) fn header(&self) -> PageHeader {
        let inner = self.inner.read().unwrap();
        self.header_from_inner(&inner)
    }

    pub(crate) fn get_data_size(&self) -> DBSizeType {
        self.page_data_size
    }

    pub(crate) fn lsn_id(&self) -> Result<LsnId, StoreError> {
        Ok(*self.lsn.read()?)
    }

    pub(crate) fn is_pinned(&self) -> bool {
        self.flags.load(std::sync::atomic::Ordering::Relaxed) & PINNED != 0
    }

    pub(crate) fn is_index_page(&self) -> bool {
        self.flags.load(std::sync::atomic::Ordering::Relaxed) & INDEX_PAGE != 0
    }

    pub(crate) fn has_overflow(&self) -> bool {
        self.inner.read().unwrap().has_overflow
    }

    pub(crate) fn set_overflow(&self, of: bool) {
        self.inner.write().unwrap().has_overflow = of;
    }

    pub(crate) fn get_next_page(&self) -> PageId {
        PageId(self.inner.read().unwrap().next_page)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_next_page(&self, next_page: PageId) -> Result<(), StoreError> {
        self.inner.write().unwrap().next_page = next_page.0;
        self.set_dirty(true)?;
        Ok(())
    }

    // B-link tree high key — see PageHeader's own comment. `None` means
    // unbounded.
    pub(crate) fn high_key(&self) -> Option<DBIdType> {
        self.inner.read().unwrap().high_key.clone()
    }

    pub(crate) fn set_high_key(&self, high_key: Option<DBIdType>) -> Result<(), StoreError> {
        self.inner.write().unwrap().high_key = high_key;
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

    pub(crate) fn clear(&self) -> Result<(), StoreError> {
        {
            let mut inner = self.inner.write()?;
            inner.data.clear()?;
            inner.page_used_size = 0;
        }
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn iter(&self) -> PageTupleIterator {
        PageTupleIterator {
            data: self
                .inner
                .read()
                .unwrap()
                .data
                .values()
                .unwrap_or_default()
                .into_iter(),
        }
    }

    // Threshold used for the fullness decision — strictly less than the true
    // physical data-region size (page_data_size), reserving room for the
    // page-serialization framing (e.g. the tuple-count varint) that no
    // individual tuple's size() accounts for. See USABLE_DATA_MARGIN.
    fn usable_data_size(&self) -> DBSizeType {
        self.page_data_size.saturating_sub(USABLE_DATA_MARGIN)
    }

    // Some(n) for a fixed-record page (FixedTuplePage, record_size = n at
    // construction — see new_indexed); None for a variable-size page
    // (AnyTuplePage). Exposed so callers that pre-check a tuple's fate
    // before descending into a page (e.g. BPlusTree's leaf-insert routing)
    // can distinguish "will never fit here" from "no room right now"
    // without duplicating record_size's own bookkeeping.
    pub(crate) fn record_size(&self) -> Option<usize> {
        self.record_size
    }

    pub(crate) fn can_store(&self, tuple: &Tuple) -> bool {
        // Accept a tuple only if it actually fits. Exception: an empty page
        // always accepts, so a single tuple larger than a whole page still has
        // a home — PageBuffer then spills that one tuple across an overflow
        // chain. This keeps overflow off the common path: ordinary pages fill to
        // capacity and link to the next data page instead of every near-full
        // page spilling into (and rewriting) an overflow chain on each write.
        let used = self.inner.read().unwrap().page_used_size;
        used == 0 || used + tuple.size() <= self.usable_data_size()
    }

    // &self, not &mut self: single-record mutation only needs a write lock on
    // `inner`, not unique ownership of the whole Page. This is what lets a
    // caller mutate a shared Arc<Page> directly (see WritePageHandle) instead
    // of paying for Arc::make_mut's whole-store clone just to change one
    // record — the per-page write lock callers already hold (see
    // PageBuffer::get_page_mut) still serializes writers the same as before;
    // this only removes the *extra* unique-ownership requirement layered on
    // top of that.
    pub(crate) fn add_tuple(&self, tuple: Tuple) -> Result<(), StoreError> {
        // Checked ahead of can_store(): can_store's fullness check is about
        // aggregate bytes used on this page, not any one tuple's own budget,
        // so once a fixed-record page (record_size = Some(n), i.e. backed by
        // FixedTuplePage) is holding several entries, an oversized tuple can
        // trip the aggregate check first — surfacing a generic
        // PageCapacityError ("no room right now") instead of the real,
        // permanent reason it can never fit here (TupleTooLarge). Checking
        // record_size first means the more specific, actionable error always
        // wins when both would apply.
        if let Some(max) = self.record_size
            && tuple.size() as usize > max
        {
            return Err(StoreError::TupleTooLarge(tuple.size(), max));
        }
        if !self.can_store(&tuple) {
            return Err(StoreError::PageCapacityError);
        }
        let sz = tuple.size();
        {
            let mut inner = self.inner.write()?;
            inner.data.add(tuple)?;
            inner.page_used_size += sz;
        }
        self.set_dirty(true)?;
        Ok(())
    }

    pub(crate) fn remove_tuple(&self, id: DBIdType) -> Result<Tuple, StoreError> {
        let old = {
            let mut inner = self.inner.write()?;
            let old = inner.data.remove(id)?;
            // checked_sub: an underflow here would wrap page_used_size to
            // ~u64::MAX, which then drives handle_large_page_size to allocate
            // a giant overflow chain (observed: 21 GB file / OOM). Surface it
            // as an error instead.
            inner.page_used_size =
                inner.page_used_size.checked_sub(old.size()).ok_or_else(|| {
                    StoreError::UnknownError(format!(
                        "remove_tuple used_size underflow: used={} old={}",
                        inner.page_used_size,
                        old.size()
                    ))
                })?;
            old
        };
        self.set_dirty(true)?;
        Ok(old)
    }

    pub(crate) fn replace_tuple(&self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
        let new_size = tuple.size();
        let old = {
            let mut inner = self.inner.write()?;
            let old = inner.data.replace(id, tuple)?;
            let old_size = old.size();
            inner.page_used_size =
                inner
                    .page_used_size
                    .checked_sub(old_size)
                    .ok_or_else(|| {
                        StoreError::UnknownError(format!(
                            "replace_tuple used_size underflow: used={} old={}",
                            inner.page_used_size, old_size
                        ))
                    })?;
            inner.page_used_size += new_size;
            old
        };
        self.set_dirty(true)?;
        Ok(old)
    }

    pub(crate) fn count(&self) -> Result<usize, StoreError> {
        self.inner.read()?.data.count()
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
        self.inner.read()?.data.contains(&id)
    }

    pub(crate) fn get(&self, id: DBIdType) -> Result<Option<Tuple>, StoreError> {
        self.inner.read()?.data.get(&id)
    }

    // The one atomic read anything that needs *both* a header and the raw
    // data bytes together should go through — including buffer.rs's
    // write_page, which used to call header()/to_data_bytes()/to_bytes() as
    // three *separate* top-level calls. Each was internally consistent on
    // its own, but nothing held `inner` locked across all three, so a
    // concurrent mutation between them (exactly what the async writer
    // thread is exposed to — see PageInner's own comment) could still make
    // write_page act on a header from one moment and data from another.
    // One lock acquisition here closes that the rest of the way.
    pub(crate) fn to_bytes_snapshot(&self) -> (PageHeader, Vec<u8>) {
        let inner = self.inner.read().unwrap();
        let header = self.header_from_inner(&inner);
        let mut data = inner.data.to_bytes().unwrap_or_default();
        if data.len() < self.page_data_size as usize {
            data.append(&mut vec![0u8; self.page_data_size as usize - data.len()]);
        }
        (header, data)
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let (header, data) = self.to_bytes_snapshot();
        let mut v = to_allocvec(&header).unwrap_or_default();
        if v.len() < PAGE_OVERHEAD {
            v.append(&mut vec![0u8; PAGE_OVERHEAD - v.len()]);
        }
        v.extend_from_slice(&data);
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
        self.to_bytes_snapshot().1
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        let header = &bytes[..PAGE_OVERHEAD];
        let header = from_bytes::<PageHeader>(header)?;
        let data = &bytes[PAGE_OVERHEAD..];
        // Deserialize the tuple payload with `?` rather than routing through the
        // `From<PageDto>` impl, which `.unwrap()`s and would turn a torn/partial
        // read into a panic instead of a recoverable StoreError.
        let pt: Box<dyn PageTuple> = if header.record_size.is_some() {
            Box::new(FixedTuplePage::from_bytes(data)?)
        } else {
            Box::new(AnyTuplePage::from_bytes(data)?)
        };
        // has_overflow is decoded out of the on-disk flags bit and into
        // PageInner (see its own comment); masked out of what's stored in
        // the atomic so there's exactly one live source of truth for it,
        // not a stale second copy nothing ever reads. next_page moves into
        // PageInner too, for the same reason.
        let has_overflow = header.flags & HAS_OVERFLOW != 0;
        Ok(Self {
            inner: RwLock::new(PageInner {
                data: pt,
                page_used_size: header.page_used_size,
                has_overflow,
                next_page: header.next_page,
                high_key: header.high_key,
            }),
            dirty: AtomicBool::new(false),
            page_data_size: header.page_data_size,
            record_size: header.record_size,
            flags: AtomicU16::new(header.flags & !HAS_OVERFLOW),
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
        self.set_dirty(true)
    }

    pub(crate) fn clear_page_flag(&self, flag: usize) -> Result<(), StoreError> {
        if (1usize << flag) & RESERVED_FLAGS as usize != 0 {
            panic!("Reserved bits cannot be set: {flag}");
        }
        self.flags
            .clear_bit(flag, std::sync::atomic::Ordering::Relaxed);
        self.set_dirty(true)
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
        let pt: Box<dyn PageTuple> = if let Some(_record_size) = value.record_size {
            Box::new(FixedTuplePage::from_bytes(&value.data).unwrap())
        } else {
            Box::new(AnyTuplePage::from_bytes(&value.data).unwrap())
        };
        // See Page::from_bytes's matching comment: has_overflow decodes out
        // of the wire flags and into PageInner, masked out of the atomic.
        // next_page moves into PageInner too, for the same reason.
        let has_overflow = value.flags & HAS_OVERFLOW != 0;
        Self {
            inner: RwLock::new(PageInner {
                data: pt,
                page_used_size: value.page_used_size,
                has_overflow,
                next_page: value.next_page,
                high_key: value.high_key,
            }),
            dirty: AtomicBool::new(false),
            page_data_size: value.page_data_size,
            record_size: value.record_size,
            flags: AtomicU16::new(value.flags & !HAS_OVERFLOW),
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
        // into_inner(), not read(): value is owned here, so there's no other
        // referent that could still hold the lock — this is a plain field
        // access, not a real lock acquisition.
        let inner = value.inner.into_inner().unwrap();
        // Recombine has_overflow (from `inner`) with the other, still-atomic
        // flag bits into the one on-disk `u16` — the inverse of from_bytes/
        // From<PageDto>'s split.
        let mut flags = value.flags.load(std::sync::atomic::Ordering::Relaxed);
        if inner.has_overflow {
            flags |= HAS_OVERFLOW;
        } else {
            flags &= !HAS_OVERFLOW;
        }
        Self {
            next_page: inner.next_page,
            page_data_size: value.page_data_size,
            page_used_size: inner.page_used_size,
            record_size: value.record_size,
            lsn: *value.lsn.read().unwrap(),
            flags,
            high_key: inner.high_key,
            data: inner.data.to_bytes().unwrap(),
        }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        // Deep-copy, not a shared pointer: independent Page values (e.g. a
        // standalone test fixture, or PageBuffer::write_page's &Page ->
        // owned-Page conversion) must not alias each other's tuple store.
        // Ordinary in-place mutation of an already-shared Arc<Page> goes
        // through add_tuple/remove_tuple/replace_tuple's own locking instead
        // of this Clone impl — see their own comments.
        let inner = self.inner.read().unwrap();
        Self {
            inner: RwLock::new(PageInner {
                data: inner.data.deep_clone(),
                page_used_size: inner.page_used_size,
                has_overflow: inner.has_overflow,
                next_page: inner.next_page,
                high_key: inner.high_key.clone(),
            }),
            dirty: AtomicBool::new(self.dirty.load(std::sync::atomic::Ordering::Relaxed)),
            page_data_size: self.page_data_size,
            record_size: self.record_size,
            flags: AtomicU16::new(self.flags.load(std::sync::atomic::Ordering::Relaxed)),
            lsn: RwLock::new(*self.lsn.read().unwrap()),
            lsn_clock: self.lsn_clock.clone(),
            accessed: AtomicU128::new(self.accessed.load(std::sync::atomic::Ordering::Relaxed)),
            written: AtomicU128::new(self.written.load(std::sync::atomic::Ordering::Relaxed)),
            saved: AtomicU128::new(self.saved.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

impl PartialEq for Page {
    fn eq(&self, rhs: &Self) -> bool {
        let inner = self.inner.read().unwrap();
        let rhs_inner = rhs.inner.read().unwrap();
        inner.page_used_size == rhs_inner.page_used_size
            && inner.has_overflow == rhs_inner.has_overflow
            && inner.next_page == rhs_inner.next_page
            && inner.high_key == rhs_inner.high_key
            && self.page_data_size == rhs.page_data_size
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
        Self(value as u64)
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

    pub(crate) fn to_bytes(self) -> Result<Vec<u8>, StoreError> {
        Ok(to_allocvec(&self)?)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Ok(from_bytes::<PageId>(bytes)?)
    }
}

// SAFETY: needed only because `data: Box<dyn PageTuple>` (inside PageInner)
// is a trait object without Send+Sync bounds. The concrete stores
// (AnyTuplePage/FixedTuplePage) are Send+Sync plain data. Sharing `&Page`
// across threads is sound: every mutating method (add_tuple/remove_tuple/
// replace_tuple/clear) takes `&self` and mutates through `inner`'s own
// RwLock, and the other genuinely-shared mutable fields are atomics / a
// lock — there is no field left that's mutated without synchronization.
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
        let p = Page::new_data(2000);
        assert!(p.add_tuple(Tuple::new(1, b"abcdefabcd")).is_ok());
        assert!(p.add_tuple(Tuple::new(2, b"abcdefabcd")).is_ok());
        assert!(matches!(
            p.add_tuple(Tuple::new(2, b"aa")),
            Err(StoreError::DuplicateKey(_))
        ));
        assert!(p.is_dirty());
        assert!(p.can_store(&Tuple::new(3, b"abcd")));
    }

    #[test]
    fn page_test_accurate_page_bytes() {
        let tuple_sz = Tuple::new(0, b"abcdef").size();
        // can_store's fullness ceiling is page_data_size - USABLE_DATA_MARGIN
        // (room reserved for page-serialization framing the per-tuple size sum
        // doesn't see — see USABLE_DATA_MARGIN), so fitting 10 tuples with 1
        // byte to spare needs that margin folded into the requested page size.
        let page_size = tuple_sz * 10 + USABLE_DATA_MARGIN + PAGE_OVERHEAD as u64 + 1;
        let p = Page::new_data(page_size);
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
        let p = Page::new_data(2000);
        p.add_tuple(Tuple::new(7, b"payload")).unwrap();
        let found = p.get(DBIdType::Int(7)).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().data.to_vec(), b"payload");
        let missing = p.get(DBIdType::Int(99)).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn page_test_contains() {
        let p = Page::new_data(2000);
        p.add_tuple(Tuple::new(3, b"x")).unwrap();
        assert!(p.contains(DBIdType::Int(3)).unwrap());
        assert!(!p.contains(DBIdType::Int(4)).unwrap());
    }

    #[test]
    fn page_test_iter_yields_all_tuples() {
        let p = Page::new_data(4000);
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
        let p = Page::new_data(2000);
        p.add_tuple(Tuple::new(1, b"first")).unwrap();
        p.add_tuple(Tuple::new(2, b"second")).unwrap();
        let bytes = p.to_bytes();
        let p2 = Page::from_bytes(&bytes).unwrap();
        assert!(p2.contains(DBIdType::Int(1)).unwrap());
        assert!(p2.contains(DBIdType::Int(2)).unwrap());
        assert_eq!(
            p2.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"first"
        );
        assert_eq!(
            p2.get(DBIdType::Int(2)).unwrap().unwrap().data.to_vec(),
            b"second"
        );
    }

    // --- PageIterator: AnyTuplePage ---

    #[test]
    fn page_iter_any_empty() {
        let p = Page::new_data(1024);
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn page_iter_any_correct_data() {
        let p = Page::new_data(4000);
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
        let p = Page::new_data(2000);
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
        let p = FixedPage::new_indexed(4000, record_size);
        for i in 0..4u64 {
            p.add_tuple(Tuple::new(i, b"data")).unwrap();
        }
        assert_eq!(p.iter().count(), 4);
    }

    #[test]
    fn page_iter_fixed_correct_data() {
        let record_size = Tuple::new(0, b"hello").size() as usize;
        let p = FixedPage::new_indexed(4000, record_size);
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
        let p = FixedPage::new_indexed(4000, record_size);
        assert!(p.add_tuple(Tuple::new(1, b"way_too_long_payload")).is_err());
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn page_iter_fixed_after_roundtrip() {
        let record_size = Tuple::new(0, b"abc").size() as usize;
        let p = FixedPage::new_indexed(2000, record_size);
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
        let p = Page::new_data(1000);
        let big_data = vec![0u8; 2000]; // much larger than page_data_size
        assert!(p.can_store(&Tuple::new(1, &big_data)));
        assert!(p.add_tuple(Tuple::new(1, &big_data)).is_ok());
    }

    #[test]
    fn test_used_size_tracks_tuple_size() {
        let p = Page::new_data(1000);
        let t = Tuple::new(1, b"hello");
        let expected = t.size();
        p.add_tuple(t).unwrap();
        assert_eq!(p.header().page_used_size, expected);
    }

    #[test]
    fn test_can_store_false_after_oversized_tuple() {
        let p = Page::new_data(1000);
        let big_data = vec![0u8; 2000]; // page_used_size will exceed page_data_size
        p.add_tuple(Tuple::new(1, &big_data)).unwrap();
        assert!(!p.can_store(&Tuple::new(2, b"x")));
    }

    #[test]
    fn test_second_tuple_rejected_when_page_full() {
        let p = Page::new_data(1000);
        let big_data = vec![0u8; 2000];
        p.add_tuple(Tuple::new(1, &big_data)).unwrap();
        assert!(matches!(
            p.add_tuple(Tuple::new(2, b"tiny")),
            Err(StoreError::PageCapacityError)
        ));
    }

    #[test]
    fn test_add_tuple_prefers_tuple_too_large_over_capacity_error_on_fixed_page() {
        // record_size deliberately sized for "small"-shaped tuples only,
        // mimicking a too-small index_entry_size chosen for a BPlusTree's
        // index page.
        let record_size = Tuple::new(0, b"small").size() as usize;
        let p = FixedPage::new_indexed(300, record_size);
        // Fill the page's aggregate byte budget with tuples that individually
        // stay within record_size, so page_used_size ends up close to
        // usable_data_size (can_store's own threshold) — the generic
        // fullness check will now say no to *any* further tuple, whether or
        // not that tuple would also violate record_size.
        let mut i = 0u64;
        while p.can_store(&Tuple::new(i, b"small")) {
            p.add_tuple(Tuple::new(i, b"small")).unwrap();
            i += 1;
        }
        assert!(!p.can_store(&Tuple::new(i, b"small")));
        // A tuple that ALSO violates record_size must be reported as
        // TupleTooLarge — the specific, permanent reason — not the generic
        // PageCapacityError that can_store's aggregate check would otherwise
        // produce first.
        let oversized = Tuple::new(i, b"this payload is far larger than record_size");
        match p.add_tuple(oversized.clone()) {
            Err(StoreError::TupleTooLarge(actual, budget)) => {
                assert_eq!(budget, record_size);
                assert_eq!(actual, oversized.size());
            }
            other => panic!("expected TupleTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn test_add_tuple_still_returns_capacity_error_for_within_budget_tuple_on_full_page() {
        // Regression guard: the new record_size check must not swallow the
        // legitimate "no room right now" case for a tuple that fits its own
        // per-entry budget just fine.
        let record_size = Tuple::new(0, b"small").size() as usize;
        let p = FixedPage::new_indexed(300, record_size);
        let mut i = 0u64;
        while p.can_store(&Tuple::new(i, b"small")) {
            p.add_tuple(Tuple::new(i, b"small")).unwrap();
            i += 1;
        }
        assert!(matches!(
            p.add_tuple(Tuple::new(i, b"small")),
            Err(StoreError::PageCapacityError)
        ));
    }

    // Runs a mutator thread doing realistic, production-ordered overflow
    // transitions (content settles before the flag does, on both the grow
    // and shrink side — exactly how buffer.rs's handle_large_page_size
    // sequences it: it only flips has_overflow after the content it
    // describes has already landed). Returns the join handle and a stop
    // flag so callers can sample concurrently and then shut it down.
    fn spawn_overflow_transition_mutator(
        page: std::sync::Arc<Page>,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let big_data = vec![0u8; 2000]; // exceeds page_data_size, see other tests above
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    page.add_tuple(Tuple::new(i, &big_data)).unwrap();
                    page.set_overflow(true);
                    page.clear().unwrap();
                    page.set_overflow(false);
                    i += 1;
                }
            })
        };
        (handle, stop)
    }

    #[test]
    fn test_separate_header_and_data_calls_can_observe_a_mismatched_pair() {
        // Characterizes the actual historical bug: buffer.rs's write_page
        // used to call header() and to_data_bytes() as two independent
        // top-level calls, each acquiring `inner` on its own. Even with
        // has_overflow bundled into `inner` (this session's fix — each call
        // is individually coherent), a full content+overflow transition
        // landing *between* the two separate calls still produces a pair
        // that never co-existed at any single instant. This is exactly why
        // write_page was rewritten to call to_bytes_snapshot() once instead
        // — see the next test for the corresponding positive case.
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let page = Arc::new(Page::new_data(1000));
        let (mutator, stop) = spawn_overflow_transition_mutator(Arc::clone(&page));

        let mut saw_mismatch = false;
        for _ in 0..50_000 {
            let header = page.header();
            std::thread::yield_now();
            let data = page.to_data_bytes();
            if !header.has_overflow() && data.len() > header.page_data_size as usize {
                saw_mismatch = true;
                break;
            }
        }
        stop.store(true, Ordering::Relaxed);
        mutator.join().unwrap();

        assert!(
            saw_mismatch,
            "expected header()+to_data_bytes() called as two separate acquisitions \
             to observe at least one mismatched pair across 50,000 samples"
        );
    }

    #[test]
    fn test_to_bytes_snapshot_never_observes_a_mismatched_pair() {
        // The fix: to_bytes_snapshot's single `inner.read()` acquisition
        // returns page_used_size (in `header`) and `data` from the exact
        // same instant, so they can never disagree about whether content is
        // present — unlike the previous test's two-call pattern, which can
        // pair page_used_size from one generation with data from another
        // arbitrarily later one.
        //
        // Note this does *not* assert has_overflow always agrees with
        // content size: add_tuple/clear and set_overflow are still separate
        // calls (Page has no combined "replace content and overflow flag
        // atomically" method), so a snapshot legitimately landing between
        // them — e.g. right after content has grown oversized but before
        // set_overflow(true) has run — will truthfully report has_overflow
        // still false. That's an accurate read of a real, if momentary,
        // state, not a torn one — buffer.rs's handle_large_page_size runs
        // that whole sequence under the page's ArcLock so no *other
        // foreground* caller can observe it. A concurrent reader unbound by
        // that lock (the pre-existing async writer-thread design) still
        // can, which to_bytes_snapshot alone cannot close — see the report
        // to the user for that residual, narrower concern.
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let page = Arc::new(Page::new_data(1000));
        let (mutator, stop) = spawn_overflow_transition_mutator(Arc::clone(&page));

        for _ in 0..50_000 {
            let (header, data) = page.to_bytes_snapshot();
            let content_present = data.len() > header.page_data_size as usize;
            assert_eq!(
                header.page_used_size > 0,
                content_present,
                "torn snapshot observed via to_bytes_snapshot: page_used_size={} but \
                 data.len()={} (page_data_size={})",
                header.page_used_size,
                data.len(),
                header.page_data_size
            );
        }

        stop.store(true, Ordering::Relaxed);
        mutator.join().unwrap();
    }
}
