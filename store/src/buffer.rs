use std::{
    collections::HashMap,
    ops::Rem,
    sync::{Arc, Weak, atomic::AtomicU64, atomic::AtomicUsize},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, bounded};
use log::{error, info};
use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};

use crate::{
    arclock::{ArcLock, ArcLockGuard},
    constant::timestamp,
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::LsnClock,
    page::{PAGE_OVERHEAD, Page, PageHeader, PageId},
    pages::content::PageContentRegistry,
    utils::shardedpq::ShardedPQ,
};

#[derive(Debug, Clone)]
enum BufMsg {
    WritePage(WriteMsg),
    WriteHeader(Header),
    // Drop any deferred (pending) writes for this page: it has just been freed,
    // so an as-yet-unflushed write of its old contents must not survive to
    // clobber the next occupant after the slot is reallocated.
    DiscardPending(PageId),
    Shutdowm,
    Checkpoint(Sender<Result<(), StoreError>>),
}

#[derive(Debug, Clone)]
struct WriteMsg {
    page_num: PageId,
    page: Arc<Page>,
    // Counts retries specifically due to StoreError::PageTransientlyInconsistent
    // (see write_page's own comment) — bounds how long the writer thread will
    // wait out an in-progress overflow transition on this exact message
    // before treating it as a genuine, un-retryable bug. Not touched for the
    // ordinary "LSN not durable yet" deferral, which isn't bounded the same
    // way since it's driven by an always-progressing external watermark.
    transient_retries: u32,
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
    locks: Arc<ArcLock<PageId>>,
    free_pages: RwLock<Vec<PageId>>,
    // This database's WAL clock, shared with its Logger. Read to stamp a page's
    // LSN when submitting it for writing, and by the writer thread to decide
    // flush-now vs defer. Per-Db, not a process global.
    clock: Arc<LsnClock>,
    // Per-Db-instance (see its own doc comment for why not a process
    // global), used to reconstruct a page's content from raw bytes on a
    // cache-miss read without Page/PageBuffer needing to know about
    // specific content kinds.
    content_registry: Arc<PageContentRegistry>,
}

impl<F: DBFile> PageBuffer<F>
where
    F: DBFile<Item = F> + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        page_size: DBSizeType,
        page_counter: Arc<AtomicU64>,
        db_file: F,
        header: Arc<Header>,
        max_entries: usize,
        clock: Arc<LsnClock>,
        max_pending_writes: usize,
        content_registry: Arc<PageContentRegistry>,
    ) -> Result<Self, StoreError> {
        let read_file = db_file.do_clone()?;
        let writer_file = db_file.do_clone()?;
        // Bounded so a full `pending` in the writer thread (see writer's own
        // comment) turns into real backpressure on senders, not an
        // ever-growing in-memory backlog. The channel's own bound is a small
        // fixed constant, not max_pending_writes itself: the writer's gate
        // (pending.len() + recv.len() >= max_pending_writes) already governs
        // the real cap, and messages can keep arriving in the channel after
        // the gate trips (right up until the channel's own bound) before any
        // send() actually blocks — so a channel bound equal to
        // max_pending_writes would let the effective total run to roughly
        // 2x the configured cap. Keeping the channel small caps that slop to
        // a fixed, negligible amount instead of one that scales with it.
        // Never larger than max_pending_writes itself, so a small configured
        // cap (e.g. in tests) isn't silently widened back out by this.
        const WRITE_CHANNEL_CAPACITY: usize = 64;
        let (write_tx, write_rx) = bounded(max_pending_writes.clamp(1, WRITE_CHANNEL_CAPACITY));
        let w_header = header.clone();
        let writer_clock = clock.clone();
        let write_handle = thread::spawn(move || {
            writer(
                writer_file,
                w_header,
                write_rx,
                writer_clock,
                max_pending_writes.max(1),
            )
        });
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
            clock,
            content_registry,
        })
    }

    pub(crate) fn shutdown(mut self) -> Result<(), StoreError> {
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
        self.handle_large_page_size(page_num, &page)?;
        self.cache_strong(page_num, page.clone())?;
        // Sending an Arc clone (not an owned copy) is what makes the Weak
        // eviction scheme correct: as long as this message is in flight (or
        // sitting in the writer thread's LSN-deferred queue), the strong
        // count never drops to zero, so a concurrent get_page() for this
        // page after eviction will see it via upgrade() instead of racing
        // the writer thread to the backing file.
        /*         Ok(self
                   .write_tx
                   .send(BufMsg::WritePage(WriteMsg { page_num, page }))?)

        */
        write_page(
            page_num,
            &page,
            &*(self.self_file.read()),
            self.header.page_size,
            self.header.first_page_offset,
        )
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
        self.handle_large_page_size(page_num, &page)?;
        self.cache_strong(page_num, page.clone())?;
        // _lock drops here, releasing the per-page lock after cache is updated.
        Ok(self.write_tx.send(BufMsg::WritePage(WriteMsg {
            page_num,
            page,
            transient_retries: 0,
        }))?)
    }

    fn handle_large_page_size(&self, page_id: PageId, page: &Arc<Page>) -> Result<(), StoreError> {
        if page.has_overflow() {
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
            // The overflow chain this branch builds is only valid for a
            // genuinely oversized SINGLE tuple: Page::can_store's own
            // "empty page always accepts" exception is the only way a page
            // is meant to end up needing one, and a page in that state
            // holds exactly that one tuple (count() == 1). A page holding
            // more than one tuple must never reach here — BPlusTree::update
            // guards against letting an ordinary multi-tuple page's
            // used_size exceed capacity via in-place replace (see its own
            // comment). If it ever does anyway (a bug elsewhere), building
            // an overflow chain here would clobber this page's next_page —
            // which points at the next SIBLING data page, not an overflow
            // page — silently corrupting table_scan's walk instead of
            // surfacing the bug. Fail loudly instead.
            if page.count()? > 1 {
                return Err(StoreError::UnknownError(format!(
                    "handle_large_page_size: {:?} holds {} tuples (used={} > usable={}) — \
                     a multi-tuple page must never need an overflow chain; refusing to \
                     avoid corrupting its next_page (the data-chain link to the next \
                     sibling page)",
                    page_id,
                    page.count()?,
                    header.used_size(),
                    header.usable_data_size()
                )));
            }
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
                    overflow_pages,
                    header.used_size(),
                    page_id
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
                    let record_size = header.record_size;
                    let following = header.next_page();
                    // Reset to a genuinely empty page before it goes on the
                    // free list — see reset_freed_page's doc comment for why
                    // the old clear_is_overflow-then-write_page_header (header
                    // only) left the page unsafe to reuse.
                    self.reset_freed_page(next_page, record_size)?;
                    self.free_page(next_page)?;
                    next_page = following;
                }
            }
        }
        Ok(None)
    }

    /// Resets `page_id`'s on-disk content to a genuinely empty page before
    /// it's handed back to the free list. Without this, a freed overflow
    /// continuation page kept the page_used_size, next_page, and tuple-store
    /// bytes from whatever chunk of the overflow object it used to hold —
    /// alloc_page() hands a popped free-list id straight to the caller with
    /// no re-init of its own, so a page reused this way could spuriously
    /// report itself full (page_used_size left over from its old life,
    /// observed at ~8x its real capacity), chained to a stale next_page, or
    /// in the worst case fail to deserialize at all (the data region held a
    /// raw slice of a larger blob, not a standalone serialized tuple store).
    /// Writing a fresh, empty Page through the normal write path — the same
    /// one init_page uses for a brand-new page — sidesteps all of that.
    fn reset_freed_page(
        &self,
        page_id: PageId,
        record_size: Option<usize>,
    ) -> Result<(), StoreError> {
        let mut p = match record_size {
            Some(rs) => Page::new_indexed(self.header.page_size, rs),
            None => Page::new_data(self.header.page_size),
        };
        p.set_clock(self.clock.clone());
        self.write_page(page_id, &p)
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
        let file = self.self_file.read();
        // TODO - Need some cleaner refactoring here.
        let mut bytes = vec![0u8; PageHeader::header_size()];
        pread_exact(&*file, &mut bytes, offset)?;
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
        let file = self.self_file.read();
        // TODO - Need some cleaner refactoring here.
        let mut bytes = to_allocvec(&header)?;
        if bytes.len() < PageHeader::header_size() {
            bytes.append(&mut vec![0u8; PageHeader::header_size() - bytes.len()]);
        }
        pwrite_all(&*file, &bytes, offset)?;
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> Result<(), StoreError> {
        let (tx, rx) = bounded(1);
        self.write_tx.send(BufMsg::Checkpoint(tx.clone()))?;
        rx.recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?
    }

    pub(crate) fn free_page(&self, page: PageId) -> Result<(), StoreError> {
        // Tell the writer to drop any deferred write still queued for this slot
        // before it can be reallocated. Ordering is safe: this send precedes any
        // reuse's write on the same (FIFO) channel, and the writer processes an
        // earlier queued old write into `pending` before it sees this discard.
        self.write_tx.send(BufMsg::DiscardPending(page))?;
        self.free_pages.write().push(page);
        Ok(())
    }

    // reset_freed_page + free_page, exposed together: a page must be reset
    // to a genuinely blank state *before* it goes on the free list (see
    // reset_freed_page's own doc comment) — this is the general-purpose
    // pairing for any caller freeing a page outright (e.g. Db::drop_table),
    // as opposed to free_overflow_pages' narrower case of collapsing one
    // page's own overflow chain while the page itself survives.
    pub(crate) fn reset_and_free_page(
        &self,
        page_id: PageId,
        record_size: Option<usize>,
    ) -> Result<(), StoreError> {
        self.reset_freed_page(page_id, record_size)?;
        self.free_page(page_id)
    }

    // Frees every page in a raw next_page-linked chain starting at `head`,
    // resetting each one first (see reset_and_free_page). Deliberately does
    // not use data_chain_next/overflow_terminator's "skip over an overflow
    // detour to find the real next sibling" logic: those exist to let a
    // page's *content* survive a shrink while its overflow chain collapses,
    // which doesn't apply here — dropping a table needs every page in the
    // chain gone, including overflow continuation pages, so a plain
    // follow-next_page-until-invalid walk already visits (and frees)
    // exactly the right set: overflow continuation pages are linked via
    // this same next_page field, just with IS_OVERFLOW set, so nothing
    // about them needs special-casing when everything gets freed anyway.
    //
    // Reads through get_page (the cache), not read_page_header (a raw disk
    // read) — confirmed the hard way: page writes go through the async
    // writer thread, and cache_strong() updates the in-memory cache
    // synchronously before that write is even queued (see
    // write_locked_page), so the cache is always current but the on-disk
    // file can briefly lag behind it. Db::drop_table calling this right
    // after a burst of inserts hit exactly that window — read_page_header
    // saw a page's stale, pre-split content and the chain walk ended one
    // page short of the table's real last page, leaking it.
    pub(crate) fn free_page_chain(&self, head: PageId) -> Result<(), StoreError> {
        let mut cur = head;
        loop {
            let page = self.get_page(cur)?;
            let next = page.get_next_page();
            let record_size = page.record_size();
            self.reset_and_free_page(cur, record_size)?;
            if !next.is_valid_next_page() {
                break;
            }
            cur = next;
        }
        Ok(())
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

    pub(crate) fn get_free_pages(&self) -> Vec<PageId> {
        self.free_pages.read().clone()
    }

    pub(crate) fn set_free_pages(&self, free_pages: Vec<PageId>) {
        let mut fp = self.free_pages.write();
        fp.clear();
        fp.extend_from_slice(&free_pages);
    }

    pub(crate) fn alloc_page(&self, should_pin: bool) -> Result<PageId, StoreError> {
        if let Some(page) = self.free_pages.write().pop() {
            Ok(page)
        } else {
            let next_page = self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let pg: PageId = next_page.into();
            self.init_page(pg, should_pin)?;
            Ok(next_page.into())
        }
    }

    /// Like `alloc_page`, but for a fixed-record-size (index) page: always
    /// writes a fresh `FixedTuplePage` with this exact `record_size`,
    /// whether the slot is a brand-new page or one popped from the free
    /// list. Unlike `alloc_page`'s reuse branch — which hands back a
    /// popped id as-is, trusting that whoever freed it already reset it to
    /// its target shape (see `reset_freed_page`) — a page's record_size
    /// must match *this* caller's requirement exactly, not whatever the
    /// slot happened to hold in a past life, so this always (re)writes it
    /// rather than trusting the free list's prior content.
    pub(crate) fn alloc_indexed_page(&self, record_size: usize) -> Result<PageId, StoreError> {
        let page_num = match self.free_pages.write().pop() {
            Some(page) => page,
            None => self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                .into(),
        };
        let mut p = Page::new_indexed(self.header.page_size, record_size);
        p.set_clock(self.clock.clone());
        self.write_page(page_num, &p)?;
        Ok(page_num)
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
        // cache_strong handles both the access_map update and the buffer
        // insert under one write lock — the old two-step was racy.
        let file = self.self_file.read();
        let mut page = read_page(
            page_num,
            &*file,
            self.header.page_size,
            self.header.first_page_offset,
            &self.content_registry,
        )?;
        drop(file);
        // Adopt the freshly-loaded page into this database's WAL clock before it
        // can be mutated (set_dirty stamps from it; clones inherit it).
        page.set_clock(self.clock.clone());
        let page = Arc::new(page);
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
        // 5ms, not the ~500us this used to be: a page's critical section
        // (read-modify-write a single page) normally finishes in low
        // microseconds, but the *lock holder* can be preempted by the OS
        // scheduler for a full quantum (easily 1-15ms+ on a busy machine)
        // while still holding it. A timeout close to the actual work time
        // gives no margin for that and times out on ordinary scheduling
        // jitter, not just genuine contention — confirmed as a real
        // contributor to a hard-to-reproduce flake (see the callers of
        // retry_on_contention in db.rs). 5ms is still imperceptible for a
        // caller that has to wait it out, but gives an order of magnitude
        // more room before concluding the lock is genuinely contended.
        let lock = self
            .locks
            .lock(page_num, 5000)
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
        let mut p = if should_pin {
            Page::new_pinned(self.header.page_size)
        } else {
            Page::new_data(self.header.page_size)
        };
        // Adopt the page into this database's WAL clock so later mutations stamp
        // their lsn from it (and copy-on-write clones inherit it).
        p.set_clock(self.clock.clone());
        self.write_page(page_num, &p)?;
        Ok(())
    }

    /// Shared handle to this database's WAL clock, for callers (e.g. BPlusTree)
    /// that create a Page outside the buffer and must adopt it before use.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
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

// Past this many retries, a StoreError::PageTransientlyInconsistent hit on
// the same message is no longer "caught mid-overflow-transition" (a window a
// couple of lock acquisitions wide, microseconds) — it's a genuine
// page_used_size accounting bug and must surface loudly instead of retrying
// forever. At the writer loop's ~1ms outer cadence this is still well under
// a second, nowhere near long enough to look like a hang.
const MAX_TRANSIENT_RETRIES: u32 = 100;

// Increments `msg`'s transient-retry counter (see WriteMsg's own comment) and
// turns it into a hard failure once MAX_TRANSIENT_RETRIES is exceeded.
fn bump_transient_retries(msg: &mut WriteMsg, page_id: PageId) -> Result<(), StoreError> {
    msg.transient_retries += 1;
    if msg.transient_retries > MAX_TRANSIENT_RETRIES {
        return Err(StoreError::PageTransientlyInconsistent(page_id));
    }
    Ok(())
}

// Bounded retry-with-sleep variant of write_page for Checkpoint/Shutdown's
// drain loops: unlike the main pending-drain loop (which just leaves a
// transiently-inconsistent message in `pending` for the next ~1ms outer
// pass), these two loops consume `pending` via drain(..) and must finish
// this one pass before returning/exiting, so they retry inline instead.
fn write_page_with_bounded_retry<F: DBFile>(
    page_id: PageId,
    page: &Arc<Page>,
    file: &F,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<(), StoreError> {
    let mut attempt = 0u32;
    loop {
        match write_page(page_id, page, file, page_size, first_offset) {
            Err(StoreError::PageTransientlyInconsistent(_)) if attempt < MAX_TRANSIENT_RETRIES => {
                attempt += 1;
                thread::sleep(Duration::from_micros(200 * attempt as u64));
            }
            other => return other,
        }
    }
}

fn writer<F: DBFile>(
    mut file: F,
    header: Arc<Header>,
    recv: Receiver<BufMsg>,
    clock: Arc<LsnClock>,
    max_pending: usize,
) -> Result<(), StoreError> {
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
            if pending[i].page.is_pinned() || pending[i].page.lsn_id()? < clock.last_written() {
                match write_page(
                    pending[i].page_num,
                    &pending[i].page,
                    &file,
                    header.page_size,
                    header.first_page_offset,
                ) {
                    Ok(()) => {
                        let m = pending.swap_remove(i);
                        m.page.set_dirty(false)?;
                        // Don't advance i: swap_remove pulled a new element
                        // into this position.
                    }
                    Err(StoreError::PageTransientlyInconsistent(pid)) => {
                        bump_transient_retries(&mut pending[i], pid)?;
                        // Leave it in place — retried on the next outer pass
                        // (~1ms later, see recv_timeout below), which gives
                        // the in-progress foreground transition plenty of
                        // time to finish without this thread busy-spinning.
                        i += 1;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                i += 1;
            }
        }
        // The drain above bounds `pending` to pages whose redo genuinely isn't
        // durable yet — but under sustained write load with a small page size
        // (many more distinct pages touched per row than a large page size),
        // even that "not yet durable" set can grow unboundedly, since nothing
        // upstream throttles how fast new WritePage messages arrive relative to
        // how fast the redo watermark advances (confirmed: 13+ GB RSS for 2M
        // rows at the default page size before this fix). Once `pending` hits
        // the cap, stop pulling new messages off `write_tx` entirely instead of
        // draining into an ever-growing Vec: since `write_tx` is now bounded to
        // the same capacity, senders (get_page_mut/write_locked_page callers)
        // block on send() once it fills, applying real backpressure all the way
        // back to whatever's inserting.
        //
        // This can't deadlock: the redo watermark (clock.last_written(), which
        // is what the drain above is waiting on) advances via the Logger's own
        // independent redo-writer thread, not through this channel — so pending
        // keeps draining, and thus this gate keeps re-opening, even while this
        // thread isn't receiving anything new.
        //
        // Staying on one channel (not splitting control messages like
        // Checkpoint/DiscardPending onto a separate one to dodge this) is
        // deliberate: DiscardPending's and Checkpoint's correctness both rely
        // on FIFO order relative to WritePage on this exact channel (see their
        // own comments) — pausing intake entirely delays everything equally
        // and preserves that order; splitting channels would not.
        //
        // Gate on `pending` alone, deliberately NOT `pending.len() +
        // recv.len()`: counting the channel's own backlog too seems tighter,
        // but it isn't safe — once the channel fills, the only way its
        // backlog ever shrinks is for this thread to recv() from it, which a
        // recv.len()-inclusive gate would itself be blocking. That's a
        // deadlock: pending drains to 0, but pending.len() + recv.len() stays
        // at the cap forever, since nothing is popping recv. Gating on
        // pending alone reopens unconditionally once pending drains, which
        // is what actually lets the channel drain too. The channel's own
        // bound (see PageBuffer::new — a small fixed constant, not
        // max_pending_writes) already keeps the resulting slop small.
        if pending.len() >= max_pending {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        // Block up to 1ms for the next message instead of busy-spinning: lets
        // this thread idle cheaply while still waking promptly to re-drain
        // pending as last_lsn advances.
        match recv.recv_timeout(Duration::from_millis(1)) {
            Ok(BufMsg::Checkpoint(tx)) => {
                let res = (|| {
                    for m in pending.drain(..) {
                        write_page_with_bounded_retry(
                            m.page_num,
                            &m.page,
                            &file,
                            header.page_size,
                            header.first_page_offset,
                        )?;
                        m.page.set_dirty(false)?;
                    }
                    // A checkpoint's entire point is "everything up to here is
                    // durable" — the redo/undo logs get truncated right after
                    // this returns (Db::checkpoint), on the assumption that
                    // whatever they'd replay is already safely on disk. Without
                    // an actual fsync, "on disk" only ever meant "handed to the
                    // OS via write()" — recoverable across a process crash via
                    // WAL replay, but not across a real power loss, since the
                    // OS's own page cache might not have been flushed yet. This
                    // closes that gap for the specific point where it matters
                    // most: right before we discard the only other record of
                    // this data.
                    file.do_sync()?;
                    Ok(())
                })();
                let _ = tx.send(res);
            }
            Ok(BufMsg::Shutdowm) => {
                // Flush everything still waiting before exit — all committed
                // operations' redo records are already durable by now.
                for m in pending.drain(..) {
                    write_page_with_bounded_retry(
                        m.page_num,
                        &m.page,
                        &file,
                        header.page_size,
                        header.first_page_offset,
                    )?;
                    m.page.set_dirty(false)?;
                }
                // Same rationale as Checkpoint above: close() truncates the
                // WAL right after this returns, so this is the last point
                // page data can be made durable before that happens.
                file.do_sync()?;
                break;
            }
            Ok(BufMsg::WritePage(mut msg)) => {
                // This write supersedes any still-deferred write of the same
                // slot: an older snapshot must never reach disk after this one.
                // `pending` is drained with swap_remove (out of order) and at
                // shutdown in vector order, so without this a stale entry for a
                // repeatedly-mutated page could flush last and clobber it.
                pending.retain(|m| m.page_num != msg.page_num);
                if msg.page.is_pinned() || msg.page.lsn_id()? < clock.last_written() {
                    match write_page(
                        msg.page_num,
                        &msg.page,
                        &file,
                        header.page_size,
                        header.first_page_offset,
                    ) {
                        Ok(()) => msg.page.set_dirty(false)?,
                        Err(StoreError::PageTransientlyInconsistent(pid)) => {
                            bump_transient_retries(&mut msg, pid)?;
                            // Defer instead of retrying inline — same
                            // treatment as "LSN not durable yet" below.
                            pending.push(msg);
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    pending.push(msg);
                }
            }
            Ok(BufMsg::WriteHeader(header)) => {
                let mut bytes = to_allocvec(&header)?;
                if bytes.len() < size_of::<Header>() {
                    bytes.append(&mut vec![0u8; size_of::<Header>() - bytes.len()]);
                }
                pwrite_all(&file, &bytes, 0)?;
            }
            Ok(BufMsg::DiscardPending(page_id)) => {
                // The slot was freed; drop its deferred write so it can't clobber
                // the next occupant on a later flush. Any earlier queued write of
                // this slot has already been moved into `pending` above (FIFO),
                // and a reuse's write arrives after this message, so this removes
                // exactly the stale old-contents write.
                pending.retain(|m| m.page_num != page_id);
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

// Positioned I/O only below this point — no `seek`. `file` here may be one of
// several independently do_clone()'d handles to the same underlying OS file
// (PageBuffer::self_file, the writer thread's own handle, ...); those clones
// SHARE the OS-level seek cursor (confirmed: std::fs::File::try_clone dups the
// file description, not just the Rust handle), so a `seek` on one silently
// moves the position under a concurrent `seek`+read/write on another —
// producing exactly the kind of byte-shifted, partially-valid header corruption
// this replaced (some fields decode correctly, others land on the wrong bytes).
// pread/pwrite take an explicit offset and never touch a shared cursor, so
// concurrent calls on independent clones are safe.

fn page_offset(page: PageId, page_size: DBSizeType, first_offset: DBSizeType) -> u64 {
    first_offset + page.0 * page_size
}

fn pread_exact(file: &impl DBFile, buf: &mut [u8], offset: u64) -> Result<(), StoreError> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = file.pread(&mut buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(StoreError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "pread_exact: failed to fill whole buffer",
            )));
        }
        total += n;
    }
    Ok(())
}

fn pwrite_all(file: &impl DBFile, buf: &[u8], offset: u64) -> Result<(), StoreError> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = file.pwrite(&buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(StoreError::IoError(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "pwrite_all: failed to write whole buffer",
            )));
        }
        total += n;
    }
    Ok(())
}

fn write_page(
    page_id: PageId,
    page: &Arc<Page>,
    file: &impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<(), StoreError> {
    // One atomic read of header+data together (see Page::to_bytes_snapshot's
    // own comment), not three separate top-level calls (header(),
    // to_data_bytes(), to_bytes()) the way this used to be written. Each of
    // those was individually consistent, but nothing held `inner` locked
    // across all three — so a concurrent mutation landing between them
    // (exactly what the async writer thread is exposed to: it runs here
    // *after* the per-page write lock that produced this message has
    // already been released, so a second, unrelated write to the same page
    // can already be interleaving its own update) could make this function
    // act on a header from one moment and data from another. Confirmed as
    // the cause of a real "page_used_size has drifted" panic under load —
    // has_overflow read as false (from a newer write that had already
    // cleared it) alongside leftover oversized content an older write's
    // to_bytes() call captured moments later. One snapshot closes the gap.
    let (header, data) = page.to_bytes_snapshot();
    let start_offset = page_offset(page_id, page_size, first_offset);
    let mut header_bytes = to_allocvec(&header).unwrap_or_default();
    if header_bytes.len() < PAGE_OVERHEAD {
        header_bytes.append(&mut vec![0u8; PAGE_OVERHEAD - header_bytes.len()]);
    }
    if header.has_overflow() {
        // Re-write the HAS_OVERFLOW header at the primary page slot. An async
        // init_page write (queued by alloc_page before handle_large_page_size
        // ran) may arrive in the writer thread before this message and clobber
        // the header that handle_large_page_size already wrote synchronously.
        // Writing it here — inside the writer thread with the data —
        // eliminates the race window.
        pwrite_all(file, &header_bytes, start_offset)?; // header with HAS_OVERFLOW
        let first_end = (header.page_data_size as usize).min(data.len());
        pwrite_all(
            file,
            &data[..first_end],
            start_offset + PAGE_OVERHEAD as u64,
        )?;
        let mut start = first_end;
        if start < data.len() {
            let mut cur_page_id = header.next_page();
            loop {
                let cur_offset = page_offset(cur_page_id, page_size, first_offset);
                let cur_header = read_page_header(cur_page_id, file, page_size, first_offset)?;
                let end = (start + cur_header.page_data_size as usize).min(data.len());
                pwrite_all(file, &data[start..end], cur_offset + PAGE_OVERHEAD as u64)?;
                start = end;
                // Stop when all data is written or when we've written to the terminator page.
                if !cur_header.is_overflow() || start >= data.len() {
                    break;
                }
                cur_page_id = cur_header.next_page();
            }
        }
    } else {
        let mut bytes = header_bytes;
        bytes.extend_from_slice(&data);
        // This page isn't flagged has_overflow, so its slot is exactly
        // `page_size` bytes — writing more would silently spill into the next
        // page's slot. This is expected to be transient, not corruption: this
        // Arc<Page> is shared with the cache and any other write in flight
        // for the same page_num (see write_locked_page's comment on why Arc
        // identity is preserved), so this call can catch it mid-transition —
        // content already grown by a newer, concurrent write on the same
        // live page, has_overflow not yet flipped to match because that's a
        // separate, later lock acquisition inside handle_large_page_size.
        // The caller (writer's own retry loop) is responsible for treating
        // this as "not yet", not "corrupt" — see its own comment and the
        // bounded-retry counter that turns a genuinely stuck case (an actual
        // page_used_size accounting bug, not a transition) into a hard error
        // instead of retrying forever.
        if bytes.len() > page_size as usize {
            return Err(StoreError::PageTransientlyInconsistent(page_id));
        }
        pwrite_all(file, &bytes, start_offset)?;
    }

    Ok(())
}

fn read_page(
    page_id: PageId,
    file: &impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
    content_registry: &PageContentRegistry,
) -> Result<Page, StoreError> {
    let header = read_page_header(page_id, file, page_size, first_offset)?;
    if header.has_overflow() {
        // Use a single pread (like the old read()), not pread_exact: the last
        // overflow page (terminator) may hold fewer bytes than page_data_size
        // when the data doesn't exactly fill the page. The zero-initialized
        // buffers act as natural zero-padding; postcard ignores trailing zeros
        // when deserializing since from_bytes does not check that all input is
        // consumed.
        if header.page_data_size > page_size {
            return Err(StoreError::UnknownError(format!(
                "read_page: corrupt primary page_data_size {} > page_size {} for {:?}",
                header.page_data_size, page_size, page_id
            )));
        }
        let mut all_data = vec![0u8; header.page_data_size as usize];
        file.pread(
            &mut all_data,
            page_offset(page_id, page_size, first_offset) + PAGE_OVERHEAD as u64,
        )?;
        let primary_header = header;
        let mut cur_header;
        let mut cur_page_id = primary_header.next_page();
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
            cur_header = read_page_header(cur_page_id, file, page_size, first_offset)?;
            if cur_header.page_data_size > page_size {
                return Err(StoreError::UnknownError(format!(
                    "read_page: corrupt overflow page_data_size {} > page_size {}",
                    cur_header.page_data_size, page_size
                )));
            }
            let mut chunk = vec![0u8; cur_header.page_data_size as usize];
            file.pread(
                &mut chunk,
                page_offset(cur_page_id, page_size, first_offset) + PAGE_OVERHEAD as u64,
            )?;
            all_data.extend_from_slice(&chunk);
            if !cur_header.is_overflow() {
                break;
            }
            cur_page_id = cur_header.next_page();
        }
        // Reconstruct as full header+data bytes so Page::from_bytes can deserialize correctly.
        let mut full_bytes = primary_header.to_bytes()?;
        full_bytes.resize(PAGE_OVERHEAD, 0);
        full_bytes.extend_from_slice(&all_data);
        Ok(Page::from_bytes(&full_bytes, content_registry)?)
    } else {
        // Read the full page slot so Page::from_bytes gets the complete
        // serialized data. Single pread, not pread_exact: if the async writer
        // hasn't flushed yet the file may be shorter than page_size; the
        // zero-initialized buffer acts as padding.
        let mut bytes = vec![0u8; page_size as usize];
        file.pread(&mut bytes, page_offset(page_id, page_size, first_offset))?;
        Ok(Page::from_bytes(&bytes, content_registry)?)
    }
}

fn read_page_header(
    page: PageId,
    file: &impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<PageHeader, StoreError> {
    let mut bytes = vec![0u8; PageHeader::header_size()];
    pread_exact(file, &mut bytes, page_offset(page, page_size, first_offset))?;
    Ok(from_bytes(&bytes)?)
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};

    use postcard::from_bytes;

    use crate::db::DBSizeType;
    use crate::error::StoreError;
    use crate::page::Page;
    use crate::tuple::{DBIdType, Tuple};
    use crate::{buffer::PageBuffer, db::Header, memfile::MemFile};

    const PAGE_SIZE: u64 = 1000;

    // Construct a Header by deserializing raw bytes (same path as Db::open).
    // Layout: 2-byte magic, then three little-endian u64s (first_page_offset,
    // page_count, page_size), then last_checkpoint — a u128 with no fixint
    // annotation, so postcard varint-encodes it; append its own to_allocvec
    // output (postcard concatenates struct fields with no extra framing, so
    // this is byte-identical to what a full Header serialization produces).
    fn make_header_bytes(first_page_offset: u64, page_count: u64, page_size: u64) -> Vec<u8> {
        let mut v = vec![0x53u8, 0x65]; // MAGIC
        v.extend_from_slice(&first_page_offset.to_le_bytes());
        v.extend_from_slice(&page_count.to_le_bytes());
        v.extend_from_slice(&page_size.to_le_bytes());
        v.extend_from_slice(&postcard::to_allocvec(&0u128).unwrap()); // last_checkpoint
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
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
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
        let buf = PageBuffer::new(
            page_size,
            page_counter.clone(),
            mem,
            header,
            max_entries,
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();
        (buf, page_counter, file_clone)
    }

    #[test]
    fn test_write_and_read_normal_page() {
        let (buf, _, _) = make_buffer_ps(PAGE_SIZE, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let page = Page::new_data(PAGE_SIZE);
        page.add_tuple(Tuple::new(1, b"hello")).unwrap();
        buf.write_page(page_id, &page).unwrap();
        let cached = buf.get_page(page_id).unwrap();
        assert_eq!(cached.count().unwrap(), 1);
        assert_eq!(
            cached.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
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
        let page = Page::new_data(page_size);
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
        let page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();

        // get_page returns the cached Arc directly — the full tuple must be intact
        let cached = buf.get_page(page_id).unwrap();
        assert_eq!(cached.count().unwrap(), 1);
        let tuple = cached.get(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(tuple.data.to_vec(), big_data.as_slice());
        let _ = buf.shutdown();
    }

    #[test]
    fn test_oversized_page_disk_roundtrip() {
        let page_size = 300u64;
        let (buf, page_counter, file_clone) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();

        let big_data = vec![9u8; page_size as usize];
        let page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();
        // shutdown flushes the writer thread, persisting all writes to the shared MemFile
        buf.shutdown().unwrap();

        // Re-open with the clone — it shares backing storage so sees all flushed writes
        let page_count = page_counter.load(Ordering::Relaxed);
        let page_counter2 = Arc::new(AtomicU64::new(page_count));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, page_count, page_size)).unwrap());
        let buf2 = PageBuffer::new(
            page_size,
            page_counter2,
            file_clone,
            header2,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();

        let retrieved = buf2.get_page(page_id).unwrap();
        assert_eq!(retrieved.count().unwrap(), 1);
        let tuple = retrieved.get(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(tuple.data.to_vec(), big_data.as_slice());
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

    // Regression test for the writer's unbounded `pending` growth (see
    // writer's own comment: "observed 13+ GB RSS for 2M rows at the default
    // page size" during a bulk-load stress run). Proves two things at once:
    // sends beyond the cap actually block (backpressure is real, not a
    // no-op), and they unblock and complete once the redo watermark
    // advances (not a deadlock).
    #[test]
    fn test_pending_write_cap_blocks_then_drains_without_deadlock() {
        use std::sync::mpsc;

        use crate::logger::{LsnClock, LsnId};

        const MAX_PENDING: usize = 2;
        const NUM_PAGES: u64 = 6;

        let mut mem = MemFile::new();
        for _ in 0..NUM_PAGES {
            let page = Page::new_data(PAGE_SIZE);
            mem.write_all(&page.to_bytes()).unwrap();
        }
        mem.seek(SeekFrom::Start(0)).unwrap();
        let page_counter = Arc::new(AtomicU64::new(NUM_PAGES));
        let clock = Arc::new(LsnClock::default());
        let buf = Arc::new(
            PageBuffer::new(
                PAGE_SIZE,
                page_counter,
                mem,
                make_header(),
                10,
                clock.clone(),
                MAX_PENDING,
                Arc::new(crate::pages::content::PageContentRegistry::builtin()),
            )
            .unwrap(),
        );

        // Pull the watermark down to a real value: Page::set_dirty stamps a
        // page with the *current* watermark, and a cold u64::MAX watermark
        // stamps low (writes promptly) specifically to avoid deferring
        // forever — so without this, nothing here would defer into
        // `pending` at all.
        clock.mark_written(LsnId(100));

        fn dirty_and_send(buf: &PageBuffer<MemFile>, page_num: crate::page::PageId) {
            let mut handle = buf.get_page_mut(page_num).unwrap();
            std::sync::Arc::make_mut(&mut handle.page)
                .set_dirty(true)
                .unwrap();
            buf.write_locked_page(handle).unwrap();
        }

        // These fill `pending` exactly to capacity (both stamped at 100,
        // which never satisfies "< last_written" until it advances) —
        // sends here must not block.
        for i in 0..MAX_PENDING as u64 {
            dirty_and_send(&buf, i.into());
        }

        // Pending is now at the cap. Send the rest from another thread: if
        // the cap didn't apply real backpressure, this finishes immediately;
        // if the gating logic deadlocks instead of backing off, this hangs
        // forever instead of taking down the whole test process.
        let (done_tx, done_rx) = mpsc::channel();
        let buf2 = Arc::clone(&buf);
        let sender = std::thread::spawn(move || {
            for i in MAX_PENDING as u64..NUM_PAGES {
                dirty_and_send(&buf2, i.into());
            }
            let _ = done_tx.send(());
        });

        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "sends beyond the cap should block until pending drains, but finished immediately"
        );

        // Advance the watermark: everything stamped at 100 now satisfies
        // "100 < 101". The writer's per-iteration drain (unconditional, not
        // gated on receiving a new message) picks this up on its own,
        // draining `pending` and reopening the gate — unblocking the sender.
        clock.mark_written(LsnId(101));

        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("writes beyond the cap must complete once pending drains, not hang forever");
        sender.join().unwrap();

        let _ = Arc::try_unwrap(buf).unwrap().shutdown();
    }

    // A minimal third PageTuple kind, standing in for something like a
    // hash-join bucket: unordered, append-only, no notion of a positional
    // key. Exists only to prove PageContentRegistry can round-trip a kind
    // it didn't ship with, not to be a realistic implementation.
    #[derive(Debug, Clone, Default, PartialEq)]
    struct TestBucketPage {
        tuples: Vec<Tuple>,
    }

    impl crate::pages::PageTuple for TestBucketPage {
        fn count(&self) -> Result<usize, StoreError> {
            Ok(self.tuples.len())
        }

        fn deep_clone(&self) -> Box<dyn crate::pages::PageTuple> {
            Box::new(self.clone())
        }

        fn add(&mut self, tuple: Tuple) -> Result<(), StoreError> {
            self.tuples.push(tuple);
            Ok(())
        }

        fn contains(&self, id: &DBIdType) -> Result<bool, StoreError> {
            Ok(self.tuples.iter().any(|t| &t.id == id))
        }

        fn get(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
            Ok(self.tuples.iter().find(|t| &t.id == id).cloned())
        }

        fn replace(&mut self, id: &DBIdType, tuple: Tuple) -> Result<Tuple, StoreError> {
            let pos = self
                .tuples
                .iter()
                .position(|t| &t.id == id)
                .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
            Ok(std::mem::replace(&mut self.tuples[pos], tuple))
        }

        fn remove(&mut self, id: DBIdType) -> Result<Tuple, StoreError> {
            let pos = self
                .tuples
                .iter()
                .position(|t| t.id == id)
                .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
            Ok(self.tuples.remove(pos))
        }

        fn values(&self) -> Result<Vec<Tuple>, StoreError> {
            Ok(self.tuples.clone())
        }

        fn keys(&self) -> Result<Vec<DBSizeType>, StoreError> {
            // No positional key exists for an unordered bucket; an honest
            // empty answer rather than a fabricated one.
            Ok(vec![])
        }

        fn to_bytes(&self) -> Result<Vec<u8>, StoreError> {
            Ok(postcard::to_allocvec(&self.tuples)?)
        }

        fn clear(&mut self) -> Result<(), StoreError> {
            self.tuples.clear();
            Ok(())
        }

        fn first(&self) -> Result<Option<Tuple>, StoreError> {
            Ok(self.tuples.first().cloned())
        }

        fn last(&self) -> Result<Option<Tuple>, StoreError> {
            Ok(self.tuples.last().cloned())
        }
    }

    impl TestBucketPage {
        fn from_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
            Ok(Self {
                tuples: postcard::from_bytes(bytes)?,
            })
        }
    }

    const TEST_BUCKET_KIND: crate::pages::content::PageContentKind =
        crate::pages::content::PageContentKind(2);

    fn registry_with_test_bucket() -> crate::pages::content::PageContentRegistry {
        let mut registry = crate::pages::content::PageContentRegistry::builtin();
        registry
            .register(
                TEST_BUCKET_KIND,
                Arc::new(|bytes| {
                    Ok(Box::new(TestBucketPage::from_bytes(bytes)?) as Box<dyn crate::pages::PageTuple>)
                }),
            )
            .unwrap();
        registry
    }

    #[test]
    fn test_custom_content_kind_roundtrips_through_disk() {
        let page_size = PAGE_SIZE;
        let mut mem = MemFile::new();
        let file_clone = mem.clone();
        mem.seek(SeekFrom::Start(0)).unwrap();
        let page_counter = Arc::new(AtomicU64::new(0));
        let header =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, 0, page_size)).unwrap());

        let buf = PageBuffer::new(
            page_size,
            page_counter.clone(),
            mem,
            header,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(registry_with_test_bucket()),
        )
        .unwrap();

        let page_id = buf.alloc_page(false).unwrap();
        let page = Page::new_with_content(
            page_size,
            0,
            None,
            Box::new(TestBucketPage::default()),
            TEST_BUCKET_KIND,
        );
        page.add_tuple(Tuple::new(1, b"bucket-entry")).unwrap();
        buf.write_page(page_id, &page).unwrap();
        // shutdown flushes the writer thread, persisting all writes to the shared MemFile
        buf.shutdown().unwrap();

        // Re-open with a fresh buffer over the shared backing storage and a
        // fresh registry (not the same Arc the first buffer used) — this is
        // what actually forces Page::from_bytes -> PageContentRegistry::resolve
        // to run the custom factory, rather than reading a cached Arc<Page>
        // the first buffer already had in memory.
        let page_count = page_counter.load(Ordering::Relaxed);
        let page_counter2 = Arc::new(AtomicU64::new(page_count));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, page_count, page_size)).unwrap());
        let buf2 = PageBuffer::new(
            page_size,
            page_counter2,
            file_clone,
            header2,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(registry_with_test_bucket()),
        )
        .unwrap();

        let retrieved = buf2.get_page(page_id).unwrap();
        assert_eq!(retrieved.count().unwrap(), 1);
        let tuple = retrieved.get(DBIdType::Int(1)).unwrap().unwrap();
        assert_eq!(tuple.data.to_vec(), b"bucket-entry");
        let _ = buf2.shutdown();
    }

    #[test]
    fn test_custom_content_kind_unresolvable_without_registration() {
        // Same write as above, but reading it back with a registry that never
        // learned about kind 2 must fail loudly (UnknownPageContentKind)
        // rather than silently misdecoding the bytes as a built-in kind.
        let page_size = PAGE_SIZE;
        let mut mem = MemFile::new();
        mem.seek(SeekFrom::Start(0)).unwrap();
        let page_counter = Arc::new(AtomicU64::new(0));
        let header =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, 0, page_size)).unwrap());
        let buf = PageBuffer::new(
            page_size,
            page_counter.clone(),
            mem,
            header,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            1024,
            Arc::new(registry_with_test_bucket()),
        )
        .unwrap();

        buf.alloc_page(false).unwrap();
        let page = Page::new_with_content(
            page_size,
            0,
            None,
            Box::new(TestBucketPage::default()),
            TEST_BUCKET_KIND,
        );
        page.add_tuple(Tuple::new(1, b"bucket-entry")).unwrap();
        let raw_bytes = page.to_bytes();

        let unregistered = crate::pages::content::PageContentRegistry::builtin();
        let err = Page::from_bytes(&raw_bytes, &unregistered).unwrap_err();
        assert!(matches!(err, StoreError::UnknownPageContentKind(2)));
        let _ = buf.shutdown();
    }
}
