use std::{
    collections::HashMap,
    io::SeekFrom,
    ops::Rem,
    sync::{Arc, Weak, atomic::AtomicU64, atomic::AtomicUsize},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::{error, info};
use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};

use crate::{
    arclock::{ArcLock, ArcLockGuard},
    constant::timestamp,
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::Logger,
    page::{PAGE_OVERHEAD, Page, PageHeader, PageId},
    utils::shardedpq::ShardedPQ,
};

#[derive(Debug, Clone)]
enum BufMsg {
    WritePage(WriteMsg),
    WriteHeader(Header),
    Shutdowm,
}

#[derive(Debug, Clone)]
struct WriteMsg {
    page_num: PageId,
    page: Arc<Page>,
}

#[derive(Debug, Clone)]
pub(crate) struct WritePageHandle {
    pub(crate) page_num: PageId,
    lock: ArcLockGuard<PageId>,
    pub(crate) page: Arc<Page>,
}

// A cached page is either live (Strong) or has been evicted to make room
// (Weak). Evicting never actually drops the page's data — it just gives up
// the cache's own claim on it. If a write is still in flight when a page
// gets evicted, the writer thread's queued WriteMsg holds its own Arc clone,
// so the page stays alive and upgrade() still succeeds; a get_page() for it
// reuses that exact in-flight copy instead of racing the writer thread to
// read a backing file that may not reflect it yet. upgrade() only fails once
// the writer thread has dropped its copy, which only happens after the
// actual file write completes — so a failed upgrade is the signal that it's
// now safe to read from disk.
#[derive(Debug, Clone)]
enum PageEntry {
    Strong(Arc<Page>),
    Weak(Weak<Page>),
}

static WRITE_PAGE_COUNTER: AtomicU64 = AtomicU64::new(0);
static WRITE_LOCKED_PAGE_COUNTER: AtomicU64 = AtomicU64::new(0);
static WRITE_LARGE_PAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct PageBuffer<F: DBFile + 'static> {
    buffer: RwLock<HashMap<PageId, PageEntry>>,
    header: Arc<Header>,
    page_size: DBSizeType,
    page_count: Arc<AtomicU64>,
    max_entries: usize,
    // Count of currently-Strong residents — what max_entries actually bounds.
    // The buffer map itself can grow past max_entries with Weak tombstones
    // for pages that were evicted but not yet dropped (and gets pruned
    // lazily, on the next failed upgrade() for that page in get_page).
    strong_count: AtomicUsize,
    write_tx: Sender<BufMsg>,
    // None once shutdown() has taken it to join the thread — Drop uses that
    // to tell "shut down properly" apart from "dropped without shutdown".
    write_handle: Option<JoinHandle<Result<(), StoreError>>>,
    self_file: RwLock<F>,
    access_map: ShardedPQ<PageId, u128>,
    locks: ArcLock<PageId>,
    free_pages: RwLock<Vec<PageId>>,
}

impl<F: DBFile> PageBuffer<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub(crate) fn new(
        page_size: DBSizeType,
        page_counter: Arc<AtomicU64>,
        db_file: F,
        header: Arc<Header>,
        max_entries: usize,
    ) -> Result<Self, StoreError> {
        let read_file = db_file.do_clone()?;
        let writer_file = db_file.do_clone()?;
        let (write_tx, write_rx) = unbounded();
        let w_header = header.clone();
        let write_handle = thread::spawn(move || writer(writer_file, w_header, write_rx));
        Ok(Self {
            page_size,
            max_entries,
            strong_count: AtomicUsize::new(0),
            buffer: RwLock::new(HashMap::new()),
            write_tx,
            self_file: RwLock::new(read_file),
            write_handle: Some(write_handle),
            access_map: ShardedPQ::new(max_entries / 10),
            page_count: page_counter,
            header,
            locks: ArcLock::new(),
            free_pages: RwLock::new(vec![]),
        })
    }

    pub(crate) fn shutdown(mut self) -> Result<(), StoreError> {
        println!(
            "WP: {}, WLP: {}, WLPC: {}",
            WRITE_PAGE_COUNTER.load(std::sync::atomic::Ordering::Relaxed),
            WRITE_LOCKED_PAGE_COUNTER.load(std::sync::atomic::Ordering::Relaxed),
            WRITE_LARGE_PAGE_COUNTER.load(std::sync::atomic::Ordering::Relaxed)
        );
        self.write_tx.send(BufMsg::Shutdowm)?;
        if let Some(handle) = self.write_handle.take() {
            let res = handle.join();
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining redo.Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn page_size(&self) -> DBSizeType {
        self.page_size
    }

    pub(crate) fn page_count_val(&self) -> u64 {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn write_header(&self, header: Header) -> Result<(), StoreError> {
        Ok(self.write_tx.send(BufMsg::WriteHeader(header))?)
    }

    pub(crate) fn write_page(&self, page_num: PageId, page: &Page) -> Result<(), StoreError> {
        let page = Arc::new(page.clone());
        page.written();
        WRITE_PAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.handle_large_page_size(page_num, &page)?;
        self.cache_strong(page_num, page.clone())?;
        // Sending an Arc clone (not an owned copy) is what makes the Weak
        // eviction scheme correct: as long as this message is in flight (or
        // sitting in the writer thread's LSN-deferred queue), the strong
        // count never drops to zero, so a concurrent get_page() for this
        // page after eviction will see it via upgrade() instead of racing
        // the writer thread to the backing file.
        Ok(self
            .write_tx
            .send(BufMsg::WritePage(WriteMsg { page_num, page }))?)
    }

    pub(crate) fn write_locked_page(&self, handle: WritePageHandle) -> Result<(), StoreError> {
        // Use the handle's existing Arc directly rather than converting to &Page
        // and back. The Arc identity must be preserved: the same allocation goes
        // into the cache (Strong) and the writer channel, so a Weak evicted from
        // the cache can be upgraded as long as the write is in flight. Creating a
        // new Arc here (as write_page does for &Page callers) would break that
        // chain — and more critically, if Arc::make_mut gave the caller in-place
        // mutation it simultaneously dissociates Weaks on that allocation; the only
        // way to close the resulting stale-disk-read window is to get the new
        // Strong back into the cache as fast as possible using the same Arc.
        let WritePageHandle {
            page_num,
            page,
            lock: _lock,
        } = handle;
        page.written();
        WRITE_LOCKED_PAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.handle_large_page_size(page_num, &page)?;
        self.cache_strong(page_num, page.clone())?;
        // _lock drops here, releasing the per-page lock after cache is updated.
        Ok(self
            .write_tx
            .send(BufMsg::WritePage(WriteMsg { page_num, page }))?)
    }

    fn handle_large_page_size(&self, page_id: PageId, page: &Arc<Page>) -> Result<(), StoreError> {
        if page.has_overflow() {
            WRITE_LARGE_PAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(next_page) = self.free_overflow_pages(page_id, page.header())? {
                page.set_next_page(next_page)?;
            }
            // Always clear the in-memory overflow flag after freeing the chain. If
            // used_size still exceeds data_size below, we re-set it. Leaving it true
            // when the page no longer needs overflow would cause subsequent
            // write_locked_page calls to re-enter free_overflow_pages with a stale
            // next_page pointer (now pointing at a data page, not an overflow page),
            // corrupting the data chain and eventually producing invalid page IDs.
            page.set_overflow(false);
            // Patch the disk header: free_overflow_pages left next_page pointing at
            // the freed overflow chain start. Rewrite with the restored value so no
            // stale overflow pointer remains on disk.
            self.write_page_header(page_id, &page.header())?;
        }
        let mut header = page.header();
        if header.used_size() > header.usable_data_size() {
            let orig_next_page = page.get_next_page();
            // Number of overflow pages needed beyond the primary: ceil((used-1)/data_size).
            // This is (used_size - 1) / data_size in integer division.
            let overflow_pages = (header.used_size() - 1) / header.usable_data_size();
            assert!(overflow_pages > 0);
            // Sanity cap: a corrupt used_size (e.g. an underflow to ~u64::MAX)
            // would otherwise make us allocate millions of overflow pages,
            // ballooning the file to tens of GB and hanging. A single logical
            // page's payload can't legitimately span a huge chain.
            const MAX_OVERFLOW_PAGES: DBSizeType = 1024;
            if overflow_pages > MAX_OVERFLOW_PAGES {
                return Err(StoreError::UnknownError(format!(
                    "handle_large_page_size: absurd overflow_pages={} (used_size={} corrupt?) for {:?}",
                    overflow_pages, header.used_size(), page_id
                )));
            }
            // Use alloc_overflow_page (no init_page write) so the IS_OVERFLOW headers we writeim
            // synchronously below are not overwritten by an async init_page from alloc_page.
            let first_page = self.alloc_overflow_page()?;
            header.set_has_overflow();
            header.set_next_page(first_page);
            self.write_page_header(page_id, &header)?;
            let mut write_page_id = first_page;
            // Write intermediate overflow pages (IS_OVERFLOW). The loop runs overflow_pages-1
            // times; the last iteration writes the terminator (not IS_OVERFLOW) below.
            for _ in 1..overflow_pages {
                let new_page_id = self.alloc_overflow_page()?;
                header.next_page = new_page_id.into();
                header.set_is_overflow();
                self.write_page_header(write_page_id, &header)?;
                write_page_id = new_page_id;
            }
            // Terminator: restore original next_page and clear IS_OVERFLOW so the read loop stops.
            header.next_page = orig_next_page.into();
            header.clear_is_overflow();
            self.write_page_header(write_page_id, &header)?;
            // Reflect the overflow state on the in-memory Arc so the writer thread's write_page
            // call uses the overflow path and distributes data across the chain.
            page.set_overflow(true);
            page.set_next_page(first_page)?;
            return Ok(());
        }
        Ok(())
    }

    // Allocates a page slot for use as an overflow continuation without queueing an init_page
    // write. handle_large_page_size writes IS_OVERFLOW headers synchronously; if we used
    // alloc_page here, the queued async init_page write would overwrite those headers before
    // the main page's overflow write executes.
    fn alloc_overflow_page(&self) -> Result<PageId, StoreError> {
        if let Some(page) = self.free_pages.write().pop() {
            Ok(page)
        } else {
            let next_page = self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(next_page.into())
        }
    }

    fn free_overflow_pages(
        &self,
        page_id: PageId,
        page: PageHeader,
    ) -> Result<Option<PageId>, StoreError> {
        let mut header = page;
        if header.has_overflow() {
            header.clear_has_overflow();
            self.write_page_header(page_id, &header)?;
            let mut next_page = header.next_page();
            while next_page.is_valid_next_page() {
                header = self.read_page_header(next_page)?;
                if !header.is_overflow() {
                    return Ok(Some(header.next_page()));
                } else {
                    header.clear_is_overflow();
                    self.write_page_header(next_page, &header)?;
                }
                self.free_page(next_page)?;
                next_page = header.next_page();
            }
        }
        Ok(None)
    }

    fn read_page_header(&self, page_num: PageId) -> Result<PageHeader, StoreError> {
        let num = u64::from(page_num);
        let page_size = self.header.page_size;
        let page_count = self.page_count.load(std::sync::atomic::Ordering::Relaxed);
        if num >= page_count {
            return Err(StoreError::UnknownError(format!(
                "read_page_header: page_num {num} >= page_count {page_count}"
            )));
        }
        let offset = self.header.first_page_offset + page_size * num;
        let mut file = self.self_file.write();
        file.seek(SeekFrom::Start(offset))?;
        // TODO - Need some cleaner refactoring here.
        let mut bytes = vec![0u8; PageHeader::header_size()];
        file.read_exact(&mut bytes)?;
        Ok(from_bytes::<PageHeader>(&bytes)?)
    }

    fn write_page_header(&self, page_num: PageId, header: &PageHeader) -> Result<(), StoreError> {
        let num = u64::from(page_num);
        let page_count = self.page_count.load(std::sync::atomic::Ordering::Relaxed);
        if num >= page_count {
            // A write past the high-water mark means a corrupt page id (e.g. a
            // bad overflow-chain pointer). Without this guard MemFile::write
            // resizes its Vec to num*page_size — instantly allocating gigabytes.
            return Err(StoreError::UnknownError(format!(
                "write_page_header: page_num {num} >= page_count {page_count} (corrupt page id?)"
            )));
        }
        let offset = self.header.first_page_offset + self.header.page_size * num;
        let mut file = self.self_file.write();
        file.seek(SeekFrom::Start(offset))?;
        // TODO - Need some cleaner refactoring here.
        let mut bytes = to_allocvec(&header)?;
        if bytes.len() < PageHeader::header_size() {
            bytes.append(&mut vec![0u8; PageHeader::header_size() - bytes.len()]);
        }
        file.write(&bytes)?;
        Ok(())
    }

    pub(crate) fn free_page(&self, page: PageId) -> Result<(), StoreError> {
        Ok(self.free_pages.write().push(page))
    }

    // Follow the overflow chain from page_id to its terminator (first non-IS_OVERFLOW page).
    fn overflow_terminator(&self, page_id: PageId) -> Result<PageId, StoreError> {
        let primary = self.read_page_header(page_id)?;
        let mut cur = primary.next_page();
        loop {
            let h = self.read_page_header(cur)?;
            if !h.is_overflow() {
                return Ok(cur);
            }
            cur = h.next_page();
        }
    }

    /// Return the next DATA page in the chain. If `page` has overflow, follows
    /// the overflow chain to its terminator and returns that page's next_page
    /// (the preserved data-chain link). Otherwise returns page.next_page directly.
    pub(crate) fn data_chain_next(
        &self,
        page: &Page,
        page_id: PageId,
    ) -> Result<PageId, StoreError> {
        if page.has_overflow() {
            let term = self.overflow_terminator(page_id)?;
            Ok(self.read_page_header(term)?.next_page())
        } else {
            Ok(page.get_next_page())
        }
    }

    /// Link `from_id → to_id` in the data page chain. If `from_id` already has
    /// overflow, writes `to_id` into the overflow terminator's next_page so the
    /// link survives overflow re-setup. Otherwise updates the page normally.
    pub(crate) fn set_data_chain_next(
        &self,
        from_id: PageId,
        to_id: PageId,
    ) -> Result<(), StoreError> {
        // Use the in-memory page to check overflow state: freshly-allocated pages
        // may not have been written to disk yet, and disk/memory can diverge briefly
        // during concurrent overflow setup. The in-memory state is always current.
        let handle = self.get_page_mut(from_id)?;
        if handle.page.has_overflow() {
            drop(handle);
            let term = self.overflow_terminator(from_id)?;
            let mut h = self.read_page_header(term)?;
            h.set_next_page(to_id);
            self.write_page_header(term, &h)?;
        } else {
            handle.page.set_next_page(to_id)?;
            self.write_locked_page(handle)?;
        }
        Ok(())
    }

    pub(crate) fn alloc_page(&self, should_pin: bool) -> Result<PageId, StoreError> {
        if let Some(page) = self.free_pages.write().pop() {
            Ok(page)
        } else {
            let next_page = self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let pg: PageId = next_page.into();
            self.init_page(pg.clone(), should_pin)?;
            Ok(next_page.into())
        }
    }

    pub(crate) fn get_page(&self, page_num: PageId) -> Result<Arc<Page>, StoreError> {
        let valid = page_num
            < self
                .page_count
                .load(std::sync::atomic::Ordering::Relaxed)
                .into();
        if !valid {
            return Err(StoreError::UnknownError(format!(
                "Invalid page number : {:?}",
                page_num
            )));
        }
        // Bound to a let, not matched on directly: a match scrutinee's
        // temporaries stay alive for the whole arm body, so matching
        // straight on `self.buffer.read()?...` would keep this read guard
        // held while the arms below try to re-acquire the same lock via
        // cache_strong/write() — a self-deadlock with no other thread
        // involved.
        let existing = self.buffer.read().get(&page_num).cloned();
        match existing {
            Some(PageEntry::Strong(arc)) => {
                // Pure LRU timestamp refresh — page is already resident and
                // counted; no eviction or count change needed here.
                self.update_page_access(page_num)?;
                return Ok(arc);
            }
            Some(PageEntry::Weak(weak)) => {
                if let Some(arc) = weak.upgrade() {
                    // Still alive — reuse it, but via get_or_install so we don't
                    // clobber a concurrent writer's newer Strong with our
                    // upgraded (possibly stale) copy.
                    return Ok(self.get_or_install(page_num, arc));
                }
                // Dead: the writer already dropped its copy, which only
                // happens after the file write completed, so the backing
                // file is now guaranteed current. Prune the stale tombstone
                // while we're here rather than leaving it around forever.
                self.buffer.write().remove(&page_num);
            }
            None => {}
        }
        /*         let mut bytes = vec![0u8; self.page_size as usize];
               let offset = self.header.first_page_offset + self.header.page_size * u64::from(page_num);
               self.self_file.write()?.seek(SeekFrom::Start(offset))?;
               self.self_file.write()?.read(&mut bytes)?;
               let page = Arc::new(Page::from_bytes(&bytes)?);
        */
        // cache_strong handles both the access_map update and the buffer
        // insert under one write lock — the old two-step was racy.
        let mut file = self.self_file.write();
        let page = Arc::new(read_page(
            page_num,
            &mut *file,
            self.header.page_size,
            self.header.first_page_offset,
        )?);
        drop(file);
        // get_or_install, not cache_strong: a concurrent writer may have installed
        // a newer Strong while we were reading from disk; don't overwrite it with
        // the older on-disk copy.
        let page = self.get_or_install(page_num, page);
        page.accessed();
        Ok(page)
    }

    pub(crate) fn get_page_mut(&self, page_num: PageId) -> Result<WritePageHandle, StoreError> {
        // Acquire the per-page lock *before* reading: a writer holds this lock
        // for its entire read-modify-write cycle (see write_locked_page), so
        // reading only after we hold it guarantees we see the latest committed
        // write rather than a snapshot from before some other writer's update.
        let lock = self
            .locks
            .lock(page_num, 500)
            .ok_or(StoreError::LockContentionError)?;
        let page = self.get_page(page_num)?;
        page.accessed();
        let handle = WritePageHandle {
            lock,
            page_num,
            page,
        };
        Ok(handle)
    }

    // Inserts/refreshes page_num as the live (Strong) resident. Holds the
    // buffer write lock for the entire operation so that:
    //   1. The already_strong check and the insert are atomic — two threads
    //      cannot both see Weak/None for the same page and both increment
    //      strong_count (which was the source of the "access map empty" panic).
    //   2. The access_map update happens under the same lock, so evict_lru
    //      cannot pop a page that is in access_map but not yet in the buffer.
    fn cache_strong(&self, page_num: PageId, page: Arc<Page>) -> Result<(), StoreError> {
        let mut buffer = self.buffer.write();
        self.cache_strong_locked(&mut buffer, page_num, page);
        Ok(())
    }

    // Install `page` as the Strong resident, evicting/counting/LRU-updating under
    // the caller's held buffer write lock. This UNCONDITIONALLY overwrites — it
    // is for WRITERS (write_page/write_locked_page), which hold the per-page lock
    // and are the authority on the page's latest contents.
    fn cache_strong_locked(
        &self,
        buffer: &mut HashMap<PageId, PageEntry>,
        page_num: PageId,
        page: Arc<Page>,
    ) {
        let already_strong = matches!(buffer.get(&page_num), Some(PageEntry::Strong(_)));
        if !already_strong {
            if self.strong_count.load(std::sync::atomic::Ordering::Relaxed) >= self.max_entries {
                self.evict_lru_locked(buffer);
            }
            self.strong_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let priority = u128::MAX - timestamp();
        if !self.access_map.contains(&page_num) {
            self.access_map.push(page_num, priority);
        } else {
            self.access_map.change_priority(&page_num, priority);
        }
        buffer.insert(page_num, PageEntry::Strong(page));
    }

    // Reader-side cache fill. If a Strong resident already exists (a writer's
    // current version), return THAT and never overwrite it — otherwise a slow
    // reader that upgraded a stale Weak (or read an older copy from disk) would
    // clobber a concurrent writer's fresh version, silently losing that write.
    // Only when there is no Strong do we install our `page`.
    fn get_or_install(&self, page_num: PageId, page: Arc<Page>) -> Arc<Page> {
        let mut buffer = self.buffer.write();
        if let Some(PageEntry::Strong(arc)) = buffer.get(&page_num) {
            let arc = arc.clone();
            let priority = u128::MAX - timestamp();
            if !self.access_map.contains(&page_num) {
                self.access_map.push(page_num, priority);
            } else {
                self.access_map.change_priority(&page_num, priority);
            }
            return arc;
        }
        self.cache_strong_locked(&mut buffer, page_num, page.clone());
        page
    }

    // Downgrades the LRU resident to Weak. Takes the caller's already-held
    // write lock to avoid the evict/insert race (see cache_strong). Loops
    // past stale access_map entries (those whose victims are no longer Strong
    // in the buffer — can happen if update_page_access ran for a page that
    // was concurrently evicted by another thread's LRU-refresh call).
    fn evict_lru_locked(&self, buffer: &mut HashMap<PageId, PageEntry>) {
        loop {
            match self.access_map.pop() {
                None => {
                    // access_map is empty. Under correct accounting this
                    // shouldn't happen, but if strong_count drifted (e.g. due
                    // to a crash recovery path) don't panic — just allow the
                    // buffer to temporarily exceed max_entries.
                    return;
                }
                Some((victim, _)) => {
                    if let Some(PageEntry::Strong(arc)) = buffer.get(&victim) {
                        let weak = Arc::downgrade(arc);
                        buffer.insert(victim, PageEntry::Weak(weak));
                        self.strong_count
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                    // Stale entry: victim is already Weak or absent in the
                    // buffer (e.g. update_page_access ran after eviction).
                    // Keep looping to find the next LRU Strong candidate.
                }
            }
        }
    }

    // LRU timestamp refresh for pages that are already Strong residents.
    // Called on the hot read path (Strong hit in get_page) without acquiring
    // the buffer lock — it only touches access_map, which has its own
    // fine-grained locks via ShardedPQ.
    fn update_page_access(&self, page_num: PageId) -> Result<(), StoreError> {
        // pop() is max-first; invert the timestamp so the least-recently
        // touched page (smallest raw timestamp) ends up with the largest
        // priority and gets evicted first.
        let priority = u128::MAX - timestamp();
        if !self.access_map.contains(&page_num) {
            self.access_map.push(page_num, priority);
        } else {
            self.access_map.change_priority(&page_num, priority);
        }
        Ok(())
    }

    fn init_page(&self, page_num: PageId, should_pin: bool) -> Result<(), StoreError> {
        let p = if should_pin {
            Page::new_pinned(self.header.page_size)
        } else {
            Page::new_data(self.header.page_size)
        };
        self.write_page(page_num, &p)?;
        Ok(())
    }
}

impl<F: DBFile + 'static> Drop for PageBuffer<F> {
    fn drop(&mut self) {
        // shutdown() always takes write_handle before self is dropped, leaving
        // None. If it's still Some here, this PageBuffer was dropped without
        // an explicit shutdown() — flag it, since that's the one case we
        // actually want to know about (as opposed to the writer thread's own
        // channel disconnecting, which is just a normal consequence of this
        // and not worth a panic on its own).
        if self.write_handle.is_some() {
            error!(
                "PageBuffer dropped without calling shutdown() first \
                 (page_size={:?}) — the writer thread is being abandoned \
                 uncleanly instead of flushed and joined.",
                self.page_size
            );
        }
    }
}

impl From<crossbeam::channel::SendError<BufMsg>> for StoreError {
    fn from(value: crossbeam::channel::SendError<BufMsg>) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}

impl Rem<usize> for PageId {
    type Output = usize;
    fn rem(self, rhs: usize) -> Self::Output {
        self.0 as usize % rhs
    }
}

impl Rem<PageId> for usize {
    type Output = usize;
    fn rem(self, rhs: PageId) -> Self::Output {
        self % rhs.0 as usize
    }
}

impl Rem<PageId> for PageId {
    type Output = usize;
    fn rem(self, rhs: PageId) -> Self::Output {
        (self.0 % rhs.0) as usize
    }
}

fn writer<F: DBFile>(
    file: F,
    header: Arc<Header>,
    recv: Receiver<BufMsg>,
) -> Result<(), StoreError> {
    let mut file = file;
    let mut pending: Vec<WriteMsg> = vec![];
    loop {
        // Drain deferred pages EVERY iteration, not just when idle. Db::insert
        // queues a page write *before* it logs the page's redo record, so a page
        // almost always arrives with lsn >= last_lsn and gets deferred here.
        // Draining only on an Empty channel (the old behavior) meant that under
        // sustained load — when the channel never drains empty — `pending` grew
        // without bound (observed: 12 GB RSS, then thrash). Retrying on every
        // pass keeps it bounded to just the pages whose redo isn't durable yet.
        let mut i = 0;
        while i < pending.len() {
            if pending[i].page.is_pinned() || pending[i].page.lsn_id()? < Logger::last_lsn() {
                let m = pending.swap_remove(i);
                write_page(
                    m.page_num,
                    &m.page,
                    &mut file,
                    header.page_size,
                    header.first_page_offset,
                )?;
            } else {
                i += 1;
            }
        }
        // Block up to 1ms for the next message instead of busy-spinning: lets
        // this thread idle cheaply while still waking promptly to re-drain
        // pending as last_lsn advances.
        match recv.recv_timeout(Duration::from_millis(1)) {
            Ok(BufMsg::Shutdowm) => {
                // Flush everything still waiting before exit — all committed
                // operations' redo records are already durable by now.
                for m in pending.drain(..) {
                    write_page(
                        m.page_num,
                        &m.page,
                        &mut file,
                        header.page_size,
                        header.first_page_offset,
                    )?;
                }
                break;
            }
            Ok(BufMsg::WritePage(msg)) => {
                if msg.page.is_pinned() || msg.page.lsn_id()? < Logger::last_lsn() {
                    write_page(
                        msg.page_num,
                        &msg.page,
                        &mut file,
                        header.page_size,
                        header.first_page_offset,
                    )?;
                } else {
                    pending.push(msg);
                }
            }
            Ok(BufMsg::WriteHeader(header)) => {
                file.seek(SeekFrom::Start(0))?;
                let bytes = to_allocvec(&header)?;
                file.write(&bytes)?;
                if bytes.len() < size_of::<Header>() {
                    let b = vec![0u8; size_of::<Header>() - bytes.len()];
                    file.write(&b)?;
                }
            }
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                // The sending PageBuffer was dropped without an explicit
                // shutdown() (flagged separately by PageBuffer's Drop impl).
                info!("Writer exiting: channel disconnected");
                break;
            }
        }
    }
    Ok(())
}

fn write_page(
    page_id: PageId,
    page: &Arc<Page>,
    file: &mut impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<(), StoreError> {
    let header = page.header();
    if header.has_overflow() {
        let bytes = page.to_data_bytes();
        // Seek to the start of the primary page slot and re-write the HAS_OVERFLOW header.
        // An async init_page write (queued by alloc_page before handle_large_page_size ran)
        // may arrive in the writer thread before this message and clobber the header that
        // handle_large_page_size already wrote synchronously.  Writing it here — inside the
        // writer thread with the data — eliminates the race window.
        seek_to_page(page_id.into(), file, page_size, first_offset)?;
        let page_bytes = page.to_bytes();
        file.write(&page_bytes[..PAGE_OVERHEAD])?; // header with HAS_OVERFLOW
        // File is now positioned at the primary page's data area.
        let first_end = (header.page_data_size as usize).min(bytes.len());
        file.write(&bytes[..first_end])?;
        let mut start = first_end;
        if start < bytes.len() {
            let mut cur_header =
                read_page_header(header.next_page(), file, page_size, first_offset)?;
            loop {
                let end = (start + cur_header.page_data_size as usize).min(bytes.len());
                file.write(&bytes[start..end])?;
                start = end;
                // Stop when all data is written or when we've written to the terminator page.
                if !cur_header.is_overflow() || start >= bytes.len() {
                    break;
                }
                cur_header =
                    read_page_header(cur_header.next_page(), file, page_size, first_offset)?;
            }
        }
    } else {
        seek_to_page(page_id.into(), file, page_size, first_offset)?;
        let bytes = page.to_bytes();
        file.write(&bytes)?;
    }

    Ok(())
}

fn read_page(
    page_id: PageId,
    file: &mut impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<Page, StoreError> {
    let header = read_page_header(page_id, file, page_size, first_offset)?;
    if header.has_overflow() {
        // After read_page_header, file is positioned at primary page data area.
        // Use read() not read_exact(): the last overflow page (terminator) may hold fewer
        // bytes than page_data_size when the data doesn't exactly fill the page.
        // The zero-initialized buffers act as natural zero-padding; postcard ignores trailing
        // zeros when deserializing since from_bytes does not check that all input is consumed.
        if header.page_data_size > page_size {
            return Err(StoreError::UnknownError(format!(
                "read_page: corrupt primary page_data_size {} > page_size {} for {:?}",
                header.page_data_size, page_size, page_id
            )));
        }
        let mut all_data = vec![0u8; header.page_data_size as usize];
        file.read(&mut all_data)?;
        let primary_header = header;
        let mut cur_header = primary_header.clone();
        // Bound the walk: a cyclic/corrupt overflow chain would otherwise
        // extend all_data forever, reallocating it up to many GB (observed).
        let mut guard = 0u64;
        loop {
            guard += 1;
            if guard > page_size {
                return Err(StoreError::UnknownError(format!(
                    "read_page: runaway/cyclic overflow chain from {:?}",
                    page_id
                )));
            }
            cur_header = read_page_header(cur_header.next_page(), file, page_size, first_offset)?;
            if cur_header.page_data_size > page_size {
                return Err(StoreError::UnknownError(format!(
                    "read_page: corrupt overflow page_data_size {} > page_size {}",
                    cur_header.page_data_size, page_size
                )));
            }
            let mut chunk = vec![0u8; cur_header.page_data_size as usize];
            file.read(&mut chunk)?;
            all_data.extend_from_slice(&chunk);
            if !cur_header.is_overflow() {
                break;
            }
        }
        // Reconstruct as full header+data bytes so Page::from_bytes can deserialize correctly.
        let mut full_bytes = primary_header.to_bytes()?;
        full_bytes.resize(PAGE_OVERHEAD, 0);
        full_bytes.extend_from_slice(&all_data);
        Ok(Page::from_bytes(&full_bytes)?)
    } else {
        // Read the full page slot so Page::from_bytes gets the complete serialized data.
        // Use read() not read_exact(): if the async writer hasn't flushed yet the file
        // may be shorter than page_size; the zero-initialized buffer acts as padding.
        seek_to_page(page_id, file, page_size, first_offset)?;
        let mut bytes = vec![0u8; page_size as usize];
        file.read(&mut bytes)?;
        Ok(Page::from_bytes(&bytes)?)
    }
}

fn read_page_header(
    page: PageId,
    file: &mut impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<PageHeader, StoreError> {
    seek_to_page(page, file, page_size, first_offset)?;
    let mut bytes = vec![0u8; PageHeader::header_size()];
    file.read_exact(&mut bytes)?;
    Ok(from_bytes(&bytes)?)
}

fn seek_to_page(
    page: PageId,
    file: &mut impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<(), StoreError> {
    let pos = first_offset + page.0 * page_size;
    file.seek(SeekFrom::Start(pos))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};

    use postcard::from_bytes;

    use crate::page::Page;
    use crate::tuple::{DBIdType, Tuple};
    use crate::{buffer::PageBuffer, db::Header, memfile::MemFile};

    const PAGE_SIZE: u64 = 1000;

    // Construct a Header by deserializing raw bytes (same path as Db::open).
    // Layout: 2-byte magic, then three little-endian u64s (first_page_offset, page_count, page_size).
    fn make_header_bytes(first_page_offset: u64, page_count: u64, page_size: u64) -> Vec<u8> {
        let mut v = vec![0x53u8, 0x65]; // MAGIC
        v.extend_from_slice(&first_page_offset.to_le_bytes());
        v.extend_from_slice(&page_count.to_le_bytes());
        v.extend_from_slice(&page_size.to_le_bytes());
        v
    }

    fn make_header() -> Arc<Header> {
        let bytes = make_header_bytes(0, 0, PAGE_SIZE);
        Arc::new(from_bytes::<Header>(&bytes).unwrap())
    }

    // Builds a MemFile pre-populated with `num_pages` serialized pages starting at offset 0,
    // then resets the seek position so the buffer's read_file clone starts at 0.
    fn make_buffer(num_pages: u64, max_entries: usize) -> (PageBuffer<MemFile>, Arc<AtomicU64>) {
        let mut mem = MemFile::new();
        for _ in 0..num_pages {
            let page = Page::new_data(PAGE_SIZE);
            mem.write_all(&page.to_bytes()).unwrap();
        }
        mem.seek(SeekFrom::Start(0)).unwrap();
        let page_counter = Arc::new(AtomicU64::new(num_pages));
        let buf = PageBuffer::new(
            PAGE_SIZE,
            page_counter.clone(),
            mem,
            make_header(),
            max_entries,
        )
        .unwrap();
        (buf, page_counter)
    }

    // Like make_buffer but with a configurable page_size and a MemFile clone that shares
    // the same backing store — useful for disk-roundtrip tests (shutdown first buffer,
    // then open a second buffer with the clone to verify what was persisted).
    fn make_buffer_ps(
        page_size: u64,
        num_pages: u64,
        max_entries: usize,
    ) -> (PageBuffer<MemFile>, Arc<AtomicU64>, MemFile) {
        let mut mem = MemFile::new();
        for _ in 0..num_pages {
            let page = Page::new_data(page_size);
            mem.write_all(&page.to_bytes()).unwrap();
        }
        let file_clone = mem.clone(); // shares Arc<RwLock<Vec<u8>>> with mem
        mem.seek(SeekFrom::Start(0)).unwrap();
        let page_counter = Arc::new(AtomicU64::new(num_pages));
        let header =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, num_pages, page_size)).unwrap());
        let buf =
            PageBuffer::new(page_size, page_counter.clone(), mem, header, max_entries).unwrap();
        (buf, page_counter, file_clone)
    }

    #[test]
    fn test_write_and_read_normal_page() {
        let (buf, _, _) = make_buffer_ps(PAGE_SIZE, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let mut page = Page::new_data(PAGE_SIZE);
        page.add_tuple(Tuple::new(1, b"hello")).unwrap();
        buf.write_page(page_id, &page).unwrap();
        let cached = buf.get_page(page_id).unwrap();
        assert_eq!(cached.count().unwrap(), 1);
        assert_eq!(
            cached.get(DBIdType::Int(1)).unwrap().unwrap().data,
            b"hello"
        );
        let _ = buf.shutdown();
    }

    #[test]
    fn test_write_oversized_page_allocates_overflow_pages() {
        let page_size = 300u64;
        let (buf, page_counter, _) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let count_after_alloc = page_counter.load(Ordering::Relaxed);

        let big_data = vec![42u8; page_size as usize]; // definitely larger than page_data_size
        let mut page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();

        let count_after_write = page_counter.load(Ordering::Relaxed);
        assert!(
            count_after_write > count_after_alloc,
            "expected overflow pages to be allocated: page_count {} -> {}",
            count_after_alloc,
            count_after_write
        );
        let _ = buf.shutdown();
    }

    #[test]
    fn test_oversized_page_readable_from_cache() {
        let page_size = 300u64;
        let (buf, _, _) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();

        let big_data = vec![7u8; page_size as usize];
        let mut page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();

        // get_page returns the cached Arc directly — the full tuple must be intact
        let cached = buf.get_page(page_id).unwrap();
        assert_eq!(cached.count().unwrap(), 1);
        let tuple = cached.get(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(tuple.data.as_slice(), big_data.as_slice());
        let _ = buf.shutdown();
    }

    #[test]
    fn test_oversized_page_disk_roundtrip() {
        let page_size = 300u64;
        let (buf, page_counter, file_clone) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();

        let big_data = vec![9u8; page_size as usize];
        let mut page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();
        // shutdown flushes the writer thread, persisting all writes to the shared MemFile
        buf.shutdown().unwrap();

        // Re-open with the clone — it shares backing storage so sees all flushed writes
        let page_count = page_counter.load(Ordering::Relaxed);
        let page_counter2 = Arc::new(AtomicU64::new(page_count));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, page_count, page_size)).unwrap());
        let buf2 = PageBuffer::new(page_size, page_counter2, file_clone, header2, 10).unwrap();

        let retrieved = buf2.get_page(page_id).unwrap();
        assert_eq!(retrieved.count().unwrap(), 1);
        let tuple = retrieved.get(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(tuple.data.as_slice(), big_data.as_slice());
        let _ = buf2.shutdown();
    }

    #[test]
    fn test_buffer_new_and_shutdown() {
        let (buf, _) = make_buffer(0, 10);
        assert!(buf.shutdown().is_ok());
    }

    #[test]
    fn test_get_page_invalid_num_returns_err() {
        let (buf, _) = make_buffer(0, 10);
        let r = buf.get_page(0usize.into());
        assert!(r.is_err());
        buf.shutdown().unwrap();
    }

    #[test]
    fn test_get_page_reads_from_file() {
        let (buf, _) = make_buffer(1, 10);
        // page 0 is valid (page_counter = 1) and its bytes are at offset 0 in MemFile
        let r = buf.get_page(0usize.into());
        assert!(r.is_ok());
        let _ = buf.shutdown();
    }

    #[test]
    fn test_get_page_cached_after_first_read() {
        let (buf, _) = make_buffer(1, 10);
        let p1 = buf.get_page(0usize.into()).unwrap();
        let p2 = buf.get_page(0usize.into()).unwrap();
        assert_eq!(*p1, *p2);
        let _ = buf.shutdown();
    }

    #[test]
    fn test_write_page_updates_in_memory_cache() {
        let (buf, _) = make_buffer(1, 10);
        // Populate cache with the initial (non-pinned) page
        let p = buf.get_page(0usize.into()).unwrap();
        assert!(!p.is_pinned());
        // Write a pinned page into the cache slot for page 0
        let new_page = Page::new_pinned(PAGE_SIZE);
        assert!(buf.write_page(0usize.into(), &new_page).is_ok());
        // Cache must now hold the updated page
        let p2 = buf.get_page(0usize.into()).unwrap();
        assert!(p2.is_pinned());
        // Note: shutdown may fail if the writer exited due to the page having no LSN —
        // that is expected here since we only test cache behaviour.
        let _ = buf.shutdown();
    }

    #[test]
    fn test_write_header_sends_without_error() {
        let (buf, _) = make_buffer(0, 10);
        // Create an updated header via the same deserialization path
        let header = from_bytes::<Header>(&make_header_bytes(0, 5, PAGE_SIZE)).unwrap();
        assert!(buf.write_header(header).is_ok());
        assert!(buf.shutdown().is_ok());
    }
}
