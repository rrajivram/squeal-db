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
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::{LsnClock, LsnId},
    page::{PAGE_MAGIC, PAGE_OVERHEAD, Page, PageHeader, PageId, fnv1a_32},
    pages::content::PageContentRegistry,
    utils::shardedpq::ShardedPQ,
};

// Phase 6/7: modified pages reach disk only through a checkpoint capture
// (see capture_dirty_pages), written by the checkpointing thread itself.
// The writer thread is left with the header and shutdown.
#[derive(Debug, Clone)]
enum BufMsg {
    WriteHeader(Header),
    // STORE_AUDIT.md T5: like WriteHeader, but with a reply channel the
    // caller blocks on — the writer thread pwrite's AND fsyncs the header
    // before replying, so the caller (Db::checkpoint) knows the header is
    // actually durable before it lets anything truncate the log. Plain
    // WriteHeader stays fire-and-forget for callers that don't need that
    // guarantee (Db::close, whose own buffer.shutdown() call right after
    // already flushes and syncs the whole file — same-channel FIFO order
    // already guarantees the header write is processed first).
    WriteHeaderSynced(Header, Sender<Result<(), StoreError>>),
    Shutdowm,
}

// STORE_AUDIT.md P2: no longer Clone — ArcLockGuard now wraps a real
// parking_lot::ArcReentrantMutexGuard, which is deliberately !Send (a
// reentrant guard's whole correctness model depends on the OS thread that
// acquired it being the one that releases/re-enters it — moving it to
// another thread would let a different thread masquerade as the owner).
// Confirmed via grep before removing: nothing actually cloned a whole
// WritePageHandle (only its `.page: Arc<Page>` field, which stays Clone).
/// TXN_SIMPLIFICATION_PLAN.md phase 5: the page-lock order, checked at every
/// acquisition BEFORE waiting. Index pages (any level of the tree) come
/// before data pages; among index pages, acquiring another while holding one
/// is allowed (crabbing top-down, a split's new sibling); among data pages
/// (and Run pages, which share the level), only the same page may be
/// re-entered. Requesting an Index page while holding a Data page, or a
/// second Data page, is `LockOrderViolation` — immediately, so a mistake can
/// never become a hang.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LockLevel {
    Index = 1,
    Data = 2,
}

thread_local! {
    static HELD: std::cell::RefCell<Vec<(LockLevel, PageId)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Pops this thread's held-lock stack entry when dropped.
#[derive(Debug)]
struct LevelToken;

impl Drop for LevelToken {
    fn drop(&mut self) {
        HELD.with(|h| {
            h.borrow_mut().pop();
        });
    }
}

/// Phase 5 (debug builds): panics if this thread holds any page lock — used
/// at every point that can block on something other than a page lock (the
/// writer channel, the durability condvar), so "never hold a page lock across
/// anything that blocks" is checked, not assumed.
pub(crate) fn debug_assert_no_page_locks_held(what: &str) {
    if cfg!(debug_assertions) {
        HELD.with(|h| {
            let h = h.borrow();
            debug_assert!(
                h.is_empty(),
                "{what} while holding page locks {:?} — a page lock must never be held across a blocking wait",
                *h
            );
        });
    }
}

fn check_lock_order(level: LockLevel, page: PageId) -> Result<(), StoreError> {
    HELD.with(|h| {
        let h = h.borrow();
        if let Some(&(top_level, top_page)) = h.last() {
            let ok = match (top_level, level) {
                (LockLevel::Index, LockLevel::Index) => true,
                (LockLevel::Index, LockLevel::Data) => true,
                (LockLevel::Data, LockLevel::Data) => top_page == page,
                (LockLevel::Data, LockLevel::Index) => false,
            };
            if !ok {
                return Err(StoreError::LockOrderViolation(format!(
                    "requested {level:?} page {page:?} while holding {:?}",
                    *h
                )));
            }
        }
        Ok(())
    })
}

#[derive(Debug)]
pub(crate) struct WritePageHandle {
    pub(crate) page_num: PageId,
    lock: ArcLockGuard<PageId>,
    pub(crate) page: Arc<Page>,
    _level: LevelToken,
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

// STORE_AUDIT.md P2 survey follow-up: fixed regardless of max_entries —
// unlike max_entries itself (a real capacity bound), shard count has no
// correctness meaning, only a concurrency-spreading one. A small
// max_entries (common in tests) just means most shards stay empty, which
// is harmless.
const BUFFER_SHARD_COUNT: usize = 16;

/// Phase 5: see `PageBuffer::set_lock_timeout`.
pub(crate) const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

// The outcome of trying to free exactly one Strong slot. Distinct from a
// bare `Option<(PageId, Arc<Page>)>` because "evicted a clean page" (no
// flush needed) and "access_map had nothing left to offer" must not be
// conflated — a caller that can't tell them apart would treat a
// just-freed clean slot the same as total exhaustion and wrongly fall
// back to tolerating a capacity overflow it didn't actually need to.
enum Evicted {
    /// A (clean) slot was freed.
    Yes,
    /// access_map is empty — nothing left to evict.
    Exhausted,
}

// Distinguishes install()'s two callers, which need genuinely different
// "what if page_num is already Strong" semantics — see install's own
// comment for why one lock-per-page-num critical section can't just pick
// one behavior for both.
#[derive(Clone, Copy)]
enum InstallMode {
    /// Writer path (write_page/write_locked_page via cache_strong): the
    /// caller holds page_num's per-page lock and is the authority on its
    /// latest contents — always overwrite whatever is currently cached.
    Overwrite,
    /// Reader path (get_or_install, called from get_page): our `page` may
    /// be a stale disk read or a just-upgraded Weak, and a concurrent
    /// writer's Strong entry — if one is already there — is always at
    /// least as fresh. Return that instead of clobbering it.
    ReuseIfPresent,
}

#[derive(Debug)]
pub(crate) struct PageBuffer<F: DBFile + 'static> {
    // STORE_AUDIT.md P2 survey follow-up: sharded (Vec of independent
    // RwLocks, picked by PageId's own hash — same scheme as ArcLock/
    // ShardedMap), not a single RwLock<HashMap<..>>. This is the map every
    // get_page/get_page_mut call touches, so it's the highest-traffic lock
    // in the whole system — see PageBuffer::shard_for and install's own
    // comments for how eviction (a GLOBAL decision via access_map, which
    // stays unsharded-in-spirit — it already shards itself) stays correct
    // without ever needing to hold two shards' locks at once.
    buffer: Vec<RwLock<HashMap<PageId, PageEntry>>>,
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
    // Phase 6: every tree write holds the read side for its whole
    // duration; a checkpoint holds the write side while it captures the
    // dirty pages, so the captured image is one instant of the tree — no
    // parent on disk can point at a child that is not. Held for
    // microseconds by writers and for one memcpy of the dirty set by the
    // checkpoint; never across I/O, never across a wait on a transaction.
    write_gate: RwLock<()>,
    // Dirty pages evict_one has set aside: not evictable until a checkpoint
    // captures them, and not worth re-scanning until then. Re-enqueued as
    // eviction candidates by capture_dirty_pages.
    parked_dirty: parking_lot::Mutex<Vec<PageId>>,
    // STORE_AUDIT.md P3: still a ShardedPQ (its own sharded locking already
    // handles concurrent eviction-candidate tracking fine) but priorities
    // are now insertion-sequence numbers (next_seq()), not timestamps —
    // see evict_lru_locked's own comment for the full CLOCK/second-chance
    // scheme this backs.
    access_map: ShardedPQ<PageId, u64>,
    insertion_seq: AtomicU64,
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
    // TXN_SIMPLIFICATION_PLAN.md phase 0: how many page writes the writer
    // thread is currently holding back (not yet durable, or queued). Kept
    // as a shared atomic so Db::stats() can report it without a channel
    // round trip.
    // Phase 5: how long get_page_mut waits before reporting LockTimeout, in
    // microseconds. A legitimate hold is microseconds; the default (1 s) is
    // a bug detector with a thousandfold margin, never a tunable for
    // contention.
    lock_timeout_us: AtomicU64,
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
        content_registry: Arc<PageContentRegistry>,
    ) -> Result<Self, StoreError> {
        let read_file = db_file.do_clone()?;
        let writer_file = db_file.do_clone()?;
        let (write_tx, write_rx) = bounded(64);
        let w_header = header.clone();
        let write_handle = thread::spawn(move || writer(writer_file, w_header, write_rx));
        Ok(Self {
            page_size,
            max_entries,
            strong_count: AtomicUsize::new(0),
            buffer: (0..BUFFER_SHARD_COUNT)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
            write_tx,
            self_file: RwLock::new(read_file),
            write_gate: RwLock::new(()),
            parked_dirty: parking_lot::Mutex::new(Vec::new()),
            write_handle: Some(write_handle),
            access_map: ShardedPQ::new(max_entries / 10),
            insertion_seq: AtomicU64::new(0),
            page_count: page_counter,
            header,
            locks: ArcLock::new(),
            free_pages: RwLock::new(vec![]),
            clock,
            content_registry,
            lock_timeout_us: AtomicU64::new(DEFAULT_LOCK_TIMEOUT.as_micros() as u64),
        })
    }

    /// Phase 5: how long a page-lock wait may take before it is reported as
    /// a bug (`LockTimeout`).
    pub(crate) fn set_lock_timeout(&self, timeout: Duration) {
        self.lock_timeout_us.store(
            timeout.as_micros().max(1) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Pages currently cache-resident (Strong).
    pub(crate) fn cached_pages(&self) -> usize {
        self.strong_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn shard_for(&self, page_num: &PageId) -> &RwLock<HashMap<PageId, PageEntry>> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        page_num.hash(&mut hasher);
        &self.buffer[(hasher.finish() as usize) % self.buffer.len()]
    }

    pub(crate) fn shutdown(mut self) -> Result<(), StoreError> {
        let captured = {
            let _excl = self.exclude_writers();
            self.capture_dirty_pages()?
        };
        self.write_captured(captured)?;
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

    // STORE_AUDIT.md T5: see WriteHeaderSynced's own comment — blocks until
    // the header is physically written AND fsynced, not just queued.
    pub(crate) fn write_header_synced(&self, header: Header) -> Result<(), StoreError> {
        let (tx, rx) = bounded(1);
        self.write_tx.send(BufMsg::WriteHeaderSynced(header, tx))?;
        rx.recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?
    }

    // Unlike write_locked_page, this one stays synchronous (eager) rather
    // than deferring to eviction/checkpoint/shutdown. It's what
    // init_page/alloc_indexed_page/alloc_run_page/reset_freed_page use to
    // write a fresh page — and handle_large_page_size (called just below)
    // can, within that SAME call, immediately reuse the very page_num just
    // reset here as a new overflow-chain link and patch its on-disk header
    // directly via write_page_header (a raw, synchronous, cache-bypassing
    // write — see its own comment). Writing here synchronously, before
    // handle_large_page_size can touch the same page_num again, keeps that
    // reuse safe.
    //
    // Must clear dirty once the synchronous write below completes: this
    // used to be a harmless no-op (the old eager design never consulted
    // is_dirty() outside the writer thread's own send/receive cycle), but
    // flush_dirty_cached_pages (checkpoint/shutdown) now does too — an
    // Arc left dirty=true here would look like it still needs flushing
    // even though it's already durable, and if this exact page_num then
    // gets reused as an overflow-chain link (patched via the raw
    // write_page_header above), that later flush would resurrect this
    // stale content and clobber the patch — confirmed via
    // test_freed_overflow_pages_persist_across_close_reopen.
    pub(crate) fn write_page(&self, page_num: PageId, page: &Page) -> Result<(), StoreError> {
        let page = Arc::new(page.clone());
        self.handle_large_page_size(page_num, &page)?;
        self.cache_strong(page_num, page.clone())?;
        write_page(
            page_num,
            &page,
            &*(self.self_file.read()),
            self.header.page_size,
            self.header.first_page_offset,
        )
        // write_page itself marks the page flushed (conditionally — see
        // Page::mark_flushed_up_to) on success now.
    }

    // Does NOT write `page` to disk itself, despite the name (kept for the
    // caller-facing symmetry with write_page, and because that's still
    // its net effect eventually) — it updates the cache (which is the
    // only thing any reader ever consults; see get_page) and leaves the
    // page marked dirty (already true by the time this runs: every
    // mutator — add_tuple/replace_tuple/remove_tuple/set_next_page/etc.
    // — calls set_dirty(true) itself). The actual disk write is deferred
    // until this page leaves the cache: eviction (see evict_lru_locked/
    // flush_evicted), or an explicit checkpoint/shutdown (see
    // flush_dirty_cached_pages). This is what lets a page absorb many
    // mutations — e.g. a table's own tail page during a bulk load —
    // for the cost of one eventual write instead of one write per
    // mutation; the previous eager-send-on-every-write behavior was
    // confirmed (via temporary instrumentation, not kept) to rewrite
    // some pages 200+ times for what a single flush would have covered.
    // Deferring is safe because page writes were never this system's
    // durability boundary to begin with — they're not fsynced except at
    // checkpoint/shutdown; the redo log already is, on every commit, and
    // is what a page write is recoverable *from* on an unclean reopen.
    // STORE_AUDIT.md T2: use this instead of write_locked_page whenever the
    // write is part of a logged Db-level operation (insert/update/remove) —
    // i.e. whenever the caller minted an lsn via Logger::next_lsn for this
    // operation before mutating. Stamps the page with that lsn (see
    // Page::stamp_lsn_at_least's own comment for why this must override,
    // not follow, set_dirty's watermark-based stamp) while still under the
    // page's exclusive lock, then publishes it exactly like
    // write_locked_page. Plain write_locked_page stays correct as-is for
    // mutations with no operation-level lsn to give it (e.g. page
    // allocation/formatting during table creation).
    pub(crate) fn write_locked_page_with_lsn(
        &self,
        handle: WritePageHandle,
        lsn: LsnId,
    ) -> Result<(), StoreError> {
        handle.page.stamp_lsn_at_least(lsn)?;
        self.write_locked_page(handle)
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
            lock,
            _level,
        } = handle;
        self.handle_large_page_size(page_num, &page)?;
        self.install(page_num, page, InstallMode::Overwrite);
        drop(lock);
        drop(_level);
        Ok(())
    }

    fn handle_large_page_size(&self, page_id: PageId, page: &Arc<Page>) -> Result<(), StoreError> {
        // STORE_AUDIT.md P9: if the page already has an overflow chain of
        // EXACTLY the length the new size still needs, leave it alone
        // entirely — no free, no realloc, no synchronous header writes at
        // all. The actual byte CONTENT gets rewritten regardless by the
        // async writer thread's own write_page (it walks whatever chain
        // linkage is already on disk and overwrites content+checksum per
        // physical page — see its own comment), so an unchanged-length
        // chain needs nothing further from this function. Before this
        // existed, every single write to an already-oversized page tore
        // down its whole chain and rebuilt a brand new one (fresh page
        // ids, N synchronous pwrites) even when nothing about its shape
        // had changed — documented in ARCHITECTURE.md as the reason
        // large-value writes ran at ~600-1000/s.
        //
        // page.overflow_page_count() lives inside Page's own `inner` lock
        // (see PageInner's own comment) so it can never be read torn
        // against has_overflow/next_page. It's only ever set by the
        // rebuild branch below succeeding — a Page freshly reconstructed
        // from disk bytes (from_bytes/From<PageDto>) always starts at 0,
        // which can never equal a real chain length (always >= 1, see the
        // assert below), so a cold Page's first oversized write always
        // conservatively falls through to a full rebuild, exactly like
        // before this field existed, then tracks accurately afterward for
        // as long as this same Arc stays cached.
        if page.has_overflow() {
            let header = page.header();
            if header.used_size() > header.usable_data_size() {
                let overflow_pages = (header.used_size() - 1) / header.usable_data_size();
                if page.overflow_page_count() == overflow_pages {
                    return Ok(());
                }
            }
        }
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
            page.set_overflow_page_count(0)?;
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
            // alloc_overflow_page never writes data, so every continuation
            // page's own bytes are genuinely all-zero at this point (a
            // brand-new page from extending page_count is zero-filled; a
            // reused free-list one was zeroed by reset_freed_page before
            // being freed) — a checksum matching that, not the 0
            // placeholder header_from_inner defaults to, so a read landing
            // in the window before write_page's later real data flush (see
            // its own comment: continuation pages are never cached, so
            // get_page always reads them raw off disk) sees a checksum
            // that actually matches what's on disk right now. write_page
            // overwrites this again with the true data checksum once it
            // runs — this is only about the header being honest about
            // disk contents in the meantime.
            let zero_checksum = fnv1a_32(&vec![0u8; header.page_data_size as usize]);
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
                header.checksum = zero_checksum;
                self.write_page_header(write_page_id, &header)?;
                write_page_id = new_page_id;
            }
            // Terminator: restore original next_page and clear IS_OVERFLOW so the read loop stops.
            header.next_page = orig_next_page.into();
            header.clear_is_overflow();
            header.checksum = zero_checksum;
            self.write_page_header(write_page_id, &header)?;
            // Reflect the overflow state on the in-memory Arc so the writer thread's write_page
            // call uses the overflow path and distributes data across the chain.
            page.set_overflow(true);
            page.set_next_page(first_page)?;
            // STORE_AUDIT.md P9: record the freshly-built chain's length so
            // the NEXT write to this same (still-cached) page can skip
            // straight past this whole rebuild if nothing about its shape
            // has changed — see this function's own opening comment.
            page.set_overflow_page_count(overflow_pages)?;
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
        let p = match record_size {
            Some(rs) => Page::new_indexed(self.header.page_size, rs),
            None => Page::new_data(self.header.page_size),
        };
        self.write_page(page_id, &p)
    }

    pub(crate) fn read_page_header(&self, page_num: PageId) -> Result<PageHeader, StoreError> {
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

    // Scans the cache for every still-Strong, still-dirty resident and
    // sends a flush for each. Needed because write_locked_page no longer
    // flushes on every mutation (see its own doc comment) — a page that's
    // mutated but never evicted would otherwise stay dirty in the cache
    // forever, with flush_evicted (eviction-triggered) never once seeing
    // it. checkpoint() and shutdown() both need every dirty page durable
    // before they proceed, so both call this first.
    // ---- Phase 6: checkpoint-only page flushing ----
    //
    // Modified pages reach the data file in exactly one way: a checkpoint
    // (or shutdown) captures every dirty page at one instant, with writers
    // excluded, and writes the copies. Nothing else writes a modified page
    // — eviction never picks a dirty page (see evict_one) — so the data
    // file is always some past instant of the whole tree plus whatever the
    // log replays on top. That is what makes "page on disk points at a
    // page that is not" impossible, and it is the only structural
    // guarantee recovery needs, since splits and chain links are not
    // logged.

    /// Excludes tree writers (see `write_gate`). Held only around
    /// `capture_dirty_pages` and the system-page writes that go with it.
    pub(crate) fn exclude_writers(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.write_gate.write()
    }

    /// Taken by every tree write for its whole duration (BPlusTree::
    /// write_version, table creation/drop). Cheap and never held across a
    /// wait on another transaction, so a checkpoint's exclusion is a
    /// microsecond stall, never a hang.
    pub(crate) fn writer_permit(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.write_gate.read()
    }

    /// Copies every dirty cached page and marks the original clean. Must be
    /// called with writers excluded; the copies are one consistent instant
    /// of the tree and are written by `write_captured` after the log that
    /// explains them is durable.
    pub(crate) fn capture_dirty_pages(&self) -> Result<Vec<(PageId, Arc<Page>)>, StoreError> {
        let mut out = Vec::new();
        for shard in self.buffer.iter() {
            let shard = shard.read();
            for (page_num, entry) in shard.iter() {
                if let PageEntry::Strong(arc) = entry
                    && arc.is_dirty()
                {
                    let version = arc.dirty_version();
                    out.push((*page_num, Arc::new((**arc).clone())));
                    arc.mark_flushed_up_to(version);
                }
            }
        }
        // Clean again: back on the eviction candidate list.
        let parked: Vec<PageId> = std::mem::take(&mut *self.parked_dirty.lock());
        for page_num in parked {
            self.access_map.push(page_num, self.next_seq());
        }
        Ok(out)
    }

    /// Writes captured pages to the data file and fsyncs it.
    pub(crate) fn write_captured(&self, pages: Vec<(PageId, Arc<Page>)>) -> Result<(), StoreError> {
        {
            let file = self.self_file.read();
            for (page_num, page) in &pages {
                write_page_with_bounded_retry(
                    *page_num,
                    page,
                    &*file,
                    self.header.page_size,
                    self.header.first_page_offset,
                )?;
            }
        }
        // Past the OS page cache: the log segments that could redo these
        // pages are deleted right after the checkpoint returns.
        self.self_file.write().do_sync()?;
        Ok(())
    }

    /// Capture + write in one step, for callers with no log to sync (tests,
    /// and code paths that own the buffer alone).
    pub(crate) fn checkpoint(&self) -> Result<(), StoreError> {
        let captured = {
            let _excl = self.exclude_writers();
            self.capture_dirty_pages()?
        };
        self.write_captured(captured)
    }

    /// Dirty pages held in the cache (bounded only by checkpoints).
    pub(crate) fn dirty_pages(&self) -> usize {
        self.buffer
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .values()
                    .filter(|e| matches!(e, PageEntry::Strong(arc) if arc.is_dirty()))
                    .count()
            })
            .sum()
    }

    pub(crate) fn free_page(&self, page: PageId) -> Result<(), StoreError> {
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
        let handle = self.get_page_mut(from_id, LockLevel::Data)?;
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
        let p = Page::new_indexed(self.header.page_size, record_size);
        self.write_page(page_num, &p)?;
        Ok(page_num)
    }

    /// Like `alloc_page`, but for a Run page: always writes a fresh
    /// `RunPage`, whether the slot is brand-new or popped from the free
    /// list. Needed for the same reason `alloc_indexed_page` doesn't
    /// trust the free list's prior content: a freed page always resets
    /// to plain `AnyTuplePage` content (see `reset_freed_page`, which
    /// only distinguishes indexed vs. non-indexed, not which kind of
    /// non-indexed content a page held before) — reusing `alloc_page`
    /// here would silently hand back id-sorted `AnyTuplePage` content
    /// instead of the arrival-order `RunPage` a Run requires.
    pub(crate) fn alloc_run_page(&self) -> Result<PageId, StoreError> {
        let page_num = match self.free_pages.write().pop() {
            Some(page) => page,
            None => self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                .into(),
        };
        let p = Page::new_run(self.header.page_size);
        self.write_page(page_num, &p)?;
        Ok(page_num)
    }

    // STORE_AUDIT.md P6 — mirrors alloc_run_page exactly, but for a
    // SlottedPage-backed page (see Page::new_slotted's own comment).
    pub(crate) fn alloc_slotted_page(&self) -> Result<PageId, StoreError> {
        let page_num = match self.free_pages.write().pop() {
            Some(page) => page,
            None => self
                .page_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                .into(),
        };
        let p = Page::new_slotted(self.header.page_size);
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
        // straight on `self.shard_for(&page_num).read()?...` would keep
        // this read guard held while the arms below try to re-acquire the
        // same shard's lock via cache_strong/write() — a self-deadlock
        // with no other thread involved.
        let existing = self.shard_for(&page_num).read().get(&page_num).cloned();
        match existing {
            Some(PageEntry::Strong(arc)) => {
                // STORE_AUDIT.md P3: a plain relaxed bool store — no lock,
                // no timestamp, no heap/priority-queue update. See Page::
                // mark_referenced's own comment.
                arc.mark_referenced();
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
                self.shard_for(&page_num).write().remove(&page_num);
            }
            None => {}
        }
        // cache_strong handles both the access_map update and the buffer
        // insert under one write lock — the old two-step was racy.
        let file = self.self_file.read();
        let page = read_page(
            page_num,
            &*file,
            self.header.page_size,
            self.header.first_page_offset,
            &self.content_registry,
        )?;
        drop(file);
        // Adopt the freshly-loaded page into this database's WAL clock before it
        // can be mutated (set_dirty stamps from it; clones inherit it).
        let page = Arc::new(page);
        // get_or_install, not cache_strong: a concurrent writer may have installed
        // a newer Strong while we were reading from disk; don't overwrite it with
        // the older on-disk copy.
        let page = self.get_or_install(page_num, page);
        Ok(page)
    }

    /// Lock `page_num` for a read-modify-write. `level` is what the caller
    /// is about to treat the page as; the order check (see `LockLevel`)
    /// runs before any waiting. A wait longer than the configured
    /// `lock_timeout` is `LockTimeout`, naming the holder — never retried by
    /// anything in this crate.
    pub(crate) fn get_page_mut(
        &self,
        page_num: PageId,
        level: LockLevel,
    ) -> Result<WritePageHandle, StoreError> {
        check_lock_order(level, page_num)?;
        // Warm the cache before locking: a miss evicts, and an eviction's
        // flush should not run with this page's lock held (see
        // get_or_install for the rare case where it still does).
        let _ = self.get_page(page_num)?;
        let timeout = Duration::from_micros(
            self.lock_timeout_us
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let started = std::time::Instant::now();
        let lock = match self.locks.lock_for(page_num, timeout) {
            crate::arclock::LockAttempt::Acquired(g) => g,
            crate::arclock::LockAttempt::TimedOut(holder) => {
                let msg = format!(
                    "{level:?} page {page_num:?}: waited {:?} (limit {timeout:?}); {holder}; this thread holds {:?}",
                    started.elapsed(),
                    HELD.with(|h| h.borrow().clone())
                );
                log::error!("page lock timeout: {msg}");
                return Err(StoreError::LockTimeout(msg));
            }
        };
        HELD.with(|h| h.borrow_mut().push((level, page_num)));
        let page = self.get_page(page_num)?;
        Ok(WritePageHandle {
            lock,
            page_num,
            page,
            _level: LevelToken,
        })
    }

    // Inserts/refreshes page_num as the live (Strong) resident. Thin
    // wrapper over install() for WRITERS (write_page/write_locked_page),
    // which hold the per-page lock and are the authority on the page's
    // latest contents — always overwrites.
    fn cache_strong(&self, page_num: PageId, page: Arc<Page>) -> Result<(), StoreError> {
        self.install(page_num, page, InstallMode::Overwrite);
        Ok(())
    }

    // Reader-side cache fill. Thin wrapper over install() that returns the
    // WINNING Arc — a Strong entry already there (a concurrent writer's
    // fresher version) beats our freshly-loaded/upgraded one, since a slow
    // reader must never clobber a concurrent writer's newer write.
    fn get_or_install(&self, page_num: PageId, page: Arc<Page>) -> Arc<Page> {
        self.install(page_num, page, InstallMode::ReuseIfPresent)
    }

    // The shared engine behind cache_strong/get_or_install. Installs `page`
    // as page_num's Strong resident, evicting elsewhere first if the cache
    // is already at max_entries, and returns (the Arc now cached for
    // page_num, every dirty victim evicted along the way that still needs
    // flushing).
    //
    // STORE_AUDIT.md P2 survey follow-up: with `buffer` sharded, the old
    // single-RwLock design's easy invariant — "the already-Strong check and
    // the insert happen atomically, under the one lock the whole cache
    // shares" — no longer holds for free. This never holds two shards'
    // locks at once (no lock-ordering discipline needed, so no deadlock
    // risk): checking/inserting into page_num's own target shard, and
    // evicting a victim from whatever (possibly different) shard it lives
    // in, are two fully independent, sequential critical sections,
    // coordinated only through the lock-free global strong_count/access_map.
    // A retry loop handles the case where eviction was needed: drop the
    // target shard's lock, evict one victim via evict_one(), then loop back
    // and re-check the target shard (capacity may now be available, or a
    // concurrent installer may have raced us to page_num in the meantime).
    //
    // `mode` exists because the two callers need genuinely different
    // "page_num is already Strong" behavior — see InstallMode's own comment.
    fn install(&self, page_num: PageId, page: Arc<Page>, mode: InstallMode) -> Arc<Page> {
        // Set once evict_one() reports the access_map genuinely has nothing
        // left to offer — forces the next pass to insert past max_entries
        // rather than retrying eviction forever. See Evicted::Exhausted.
        let mut force_insert = false;
        loop {
            let shard = self.shard_for(&page_num);
            let mut guard = shard.write();
            let existing_strong = match guard.get(&page_num) {
                Some(PageEntry::Strong(arc)) => Some(arc.clone()),
                _ => None,
            };
            if let Some(arc) = existing_strong {
                if matches!(mode, InstallMode::ReuseIfPresent) {
                    arc.mark_referenced();
                    return arc;
                }
                page.mark_referenced();
                guard.insert(page_num, PageEntry::Strong(page.clone()));
                return page;
            }
            // page_num isn't Strong yet — this is a genuinely new resident,
            // so it needs an access_map entry and, if the cache is already
            // full, a victim evicted first.
            if !force_insert
                && self.strong_count.load(std::sync::atomic::Ordering::Relaxed) >= self.max_entries
            {
                drop(guard);
                match self.evict_one() {
                    Evicted::Yes => continue,
                    Evicted::Exhausted => {
                        // Under correct accounting this shouldn't happen,
                        // but if strong_count drifted (e.g. a crash
                        // recovery path) don't loop forever — tolerate a
                        // capacity overflow instead.
                        force_insert = true;
                        continue;
                    }
                }
            }
            self.strong_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.access_map.push(page_num, self.next_seq());
            page.mark_referenced();
            guard.insert(page_num, PageEntry::Strong(page.clone()));
            return page;
        }
    }

    // Frees exactly one Strong slot, chosen by access_map (see access_map's
    // own doc comment on the CLOCK/second-chance scheme). Takes only the
    // victim's own shard's write lock — never install()'s target shard's,
    // by construction (see install's own comment) — so this never risks a
    // two-shard deadlock. Loops past stale access_map entries (victims no
    // longer Strong in their shard — can happen if a page was concurrently
    // evicted some other way) and past referenced victims (given a second
    // chance instead of evicted — see below).
    //
    // STORE_AUDIT.md P3: CLOCK / second-chance eviction. access_map still
    // gives the same thing it always did — the oldest-tracked candidate,
    // across its own shards — but candidates are ordered by insertion
    // sequence (next_seq(), a plain AtomicU64 fetch_add), not wall-clock
    // time, and a candidate found to have been accessed since it was last
    // considered (Page::take_referenced()) isn't evicted: it's cleared and
    // re-pushed with a fresh sequence number (a "second chance"), moving
    // it to the back of the queue, and the sweep continues. This is what
    // makes the hot path (PageBuffer::get_page's Strong-hit branch) able
    // to skip touching access_map at all — only eviction, not every
    // access, ever reorders anything.
    fn evict_one(&self) -> Evicted {
        // Phase 6: a dirty page is not evictable (it reaches disk only via a
        // checkpoint capture); it is parked until then, so a cache that is
        // all dirty drains the candidate list once and reports Exhausted
        // instead of re-scanning every page on every install.
        loop {
            match self.access_map.pop() {
                None => {
                    // access_map is empty. Under correct accounting this
                    // shouldn't happen, but if strong_count drifted (e.g. due
                    // to a crash recovery path) don't panic — let the caller
                    // decide how to tolerate it.
                    return Evicted::Exhausted;
                }
                Some((victim, _)) => {
                    let mut guard = self.shard_for(&victim).write();
                    if let Some(PageEntry::Strong(arc)) = guard.get(&victim) {
                        if arc.take_referenced() {
                            // Second chance: accessed since it was last
                            // swept (or since insertion) — give it another
                            // lap instead of evicting it now.
                            drop(guard);
                            self.access_map.push(victim, self.next_seq());
                            continue;
                        }
                        if arc.is_dirty() {
                            drop(guard);
                            self.parked_dirty.lock().push(victim);
                            continue;
                        }
                        let arc = arc.clone();
                        let weak = Arc::downgrade(&arc);
                        guard.insert(victim, PageEntry::Weak(weak));
                        self.strong_count
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        return Evicted::Yes;
                    }
                    // Stale entry: victim is already Weak or absent in its
                    // shard (evicted some other way, or never actually
                    // installed). Keep looping to find the next candidate.
                }
            }
        }
    }

    // Monotonic insertion-order counter backing access_map's priority —
    // inverted (u64::MAX - seq) so the OLDEST sequence number, i.e. the
    // earliest-inserted-or-last-given-a-second-chance page, is the largest
    // priority and pop()s first (ShardedPQ::pop is max-first). Plain
    // fetch_add, no timestamp: this only needs a total order among pages
    // this PageBuffer has itself ever tracked, never anything comparable
    // across a reopen or another Db instance.
    fn next_seq(&self) -> u64 {
        u64::MAX
            - self
                .insertion_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn init_page(&self, page_num: PageId, should_pin: bool) -> Result<(), StoreError> {
        let p = if should_pin {
            Page::new_pinned(self.header.page_size)
        } else {
            Page::new_data(self.header.page_size)
        };
        // Adopt the page into this database's WAL clock so later mutations stamp
        // their lsn from it (and copy-on-write clones inherit it).
        self.write_page(page_num, &p)?;
        Ok(())
    }

    /// Shared handle to this database's WAL clock, for callers (e.g. BPlusTree)
    /// that create a Page outside the buffer and must adopt it before use.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
    }

    pub(crate) fn page_data_size(&self) -> usize {
        self.get_page(PageId(0)).unwrap().get_data_size() as usize
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

// Bounded retry-with-sleep variant of write_page, used by the checkpoint's
// write of captured pages: a page caught mid-overflow-transition is retried
// briefly rather than treated as corruption.
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
    file: F,
    _header: Arc<Header>,
    recv: Receiver<BufMsg>,
) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        match recv.recv() {
            Ok(BufMsg::Shutdowm) => {
                // Everything the checkpoint/shutdown capture wrote is synced
                // by the writer of those pages; this is the header's turn.
                file.do_sync()?;
                break;
            }
            Ok(BufMsg::WriteHeader(header)) => {
                let mut bytes = to_allocvec(&header)?;
                if bytes.len() < size_of::<Header>() {
                    bytes.append(&mut vec![0u8; size_of::<Header>() - bytes.len()]);
                }
                pwrite_all(&file, &bytes, 0)?;
            }
            Ok(BufMsg::WriteHeaderSynced(header, tx)) => {
                let res = (|| {
                    let mut bytes = to_allocvec(&header)?;
                    if bytes.len() < size_of::<Header>() {
                        bytes.append(&mut vec![0u8; size_of::<Header>() - bytes.len()]);
                    }
                    pwrite_all(&file, &bytes, 0)?;
                    // STORE_AUDIT.md T5: the whole point of this variant —
                    // the caller (Db::checkpoint) must not delete log
                    // segments until the header is confirmed durable, not
                    // just handed to the OS via write().
                    file.do_sync()?;
                    Ok(())
                })();
                let _ = tx.send(res);
            }
            Err(_) => {
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
    // Captured *before* the byte snapshot below, and handed to
    // mark_flushed_up_to at the end instead of an unconditional
    // set_dirty(false) — see that method's own comment for the data-loss
    // race this closes (a concurrent mutation of this same shared Arc
    // landing between the snapshot taken here and dirty being cleared).
    let observed_version = page.dirty_version();
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
    let (mut header, data) = page.to_bytes_snapshot();
    let start_offset = page_offset(page_id, page_size, first_offset);
    if header.has_overflow() {
        // Re-write the HAS_OVERFLOW header at the primary page slot. An async
        // init_page write (queued by alloc_page before handle_large_page_size
        // ran) may arrive in the writer thread before this message and clobber
        // the header that handle_large_page_size already wrote synchronously.
        // Writing it here — inside the writer thread with the data —
        // eliminates the race window.
        //
        // Each physical page's checksum is computed over exactly the
        // page_data_size-sized, zero-padded chunk written to its own slot —
        // not to_bytes_snapshot's whole, not-yet-split `data` — so it
        // matches what read_page reads back for that one slot (a fixed-size
        // pread, zero-padding included). Overwriting the header here, right
        // before writing that chunk, costs no extra I/O: this slot's header
        // is already being (re)written in this same pass regardless.
        let first_len = (header.page_data_size as usize).min(data.len());
        let mut first_chunk = vec![0u8; header.page_data_size as usize];
        first_chunk[..first_len].copy_from_slice(&data[..first_len]);
        header.checksum = fnv1a_32(&first_chunk);
        let mut header_bytes = to_allocvec(&header).unwrap_or_default();
        if header_bytes.len() < PAGE_OVERHEAD {
            header_bytes.append(&mut vec![0u8; PAGE_OVERHEAD - header_bytes.len()]);
        }
        pwrite_all(file, &header_bytes, start_offset)?; // header with HAS_OVERFLOW
        pwrite_all(file, &first_chunk, start_offset + PAGE_OVERHEAD as u64)?;
        let mut start = first_len;
        let mut cur_page_id = header.next_page();
        loop {
            let cur_offset = page_offset(cur_page_id, page_size, first_offset);
            let mut cur_header = read_page_header(cur_page_id, file, page_size, first_offset)?;
            let chunk_len =
                (cur_header.page_data_size as usize).min(data.len().saturating_sub(start));
            let mut chunk = vec![0u8; cur_header.page_data_size as usize];
            if chunk_len > 0 {
                chunk[..chunk_len].copy_from_slice(&data[start..start + chunk_len]);
            }
            cur_header.checksum = fnv1a_32(&chunk);
            let mut cur_header_bytes = to_allocvec(&cur_header)?;
            if cur_header_bytes.len() < PAGE_OVERHEAD {
                cur_header_bytes.append(&mut vec![0u8; PAGE_OVERHEAD - cur_header_bytes.len()]);
            }
            pwrite_all(file, &cur_header_bytes, cur_offset)?;
            pwrite_all(file, &chunk, cur_offset + PAGE_OVERHEAD as u64)?;
            start += chunk_len;
            // Walk all the way to the physical terminator, not just until
            // the real data runs out: handle_large_page_size sizes the
            // chain conservatively (via usable_data_size's margin) while
            // this loop writes at the full page_data_size rate, so it can
            // finish consuming `data` a page or two before the chain's
            // actual last page. Any such trailing, logically-unused page
            // is still a real physical link a later reader (e.g.
            // drop_table's free_page_chain) will walk to and checksum —
            // it needs its own correct (all-zero-data) checksum written
            // here too, not the checksum:0 placeholder
            // handle_large_page_size left it with at chain-construction
            // time.
            if !cur_header.is_overflow() {
                break;
            }
            cur_page_id = cur_header.next_page();
        }
    } else {
        let mut header_bytes = to_allocvec(&header).unwrap_or_default();
        if header_bytes.len() < PAGE_OVERHEAD {
            header_bytes.append(&mut vec![0u8; PAGE_OVERHEAD - header_bytes.len()]);
        }
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

    page.mark_flushed_up_to(observed_version);
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
        if header.checksum != fnv1a_32(&all_data) {
            return Err(StoreError::PageChecksumMismatch(page_id));
        }
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
            // Verified per physical page, against its own header — not
            // against some checksum over the whole reassembled object — the
            // same granularity write_page computed it at.
            if cur_header.checksum != fnv1a_32(&chunk) {
                return Err(StoreError::PageChecksumMismatch(cur_page_id));
            }
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
        if header.checksum != fnv1a_32(&bytes[PAGE_OVERHEAD..]) {
            return Err(StoreError::PageChecksumMismatch(page_id));
        }
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
    let header: PageHeader = from_bytes(&bytes)?;
    // Cheap, header-only sanity check — catches a garbage/zeroed/wrong-offset
    // slot before any data byte is even read. A checksum failure (see
    // read_page) means the header itself decoded fine but the data attached
    // to it didn't; this catches the case where the header didn't either.
    if header.magic != PAGE_MAGIC {
        return Err(StoreError::InvalidPageMagic(page));
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};

    use postcard::from_bytes;

    use super::{Evicted, PageEntry};
    use crate::cursor::Cursor;
    use crate::db::{DBSizeType, Opener};
    use crate::error::StoreError;
    use crate::page::{PAGE_OVERHEAD, Page, PageId};
    use crate::run::Run;
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
        v.extend_from_slice(&3u32.to_le_bytes()); // format_version
        v.extend_from_slice(&first_page_offset.to_le_bytes());
        v.extend_from_slice(&page_count.to_le_bytes());
        v.extend_from_slice(&page_size.to_le_bytes());
        v.extend_from_slice(&postcard::to_allocvec(&0u128).unwrap()); // last_checkpoint
        v.extend_from_slice(&1u64.to_le_bytes()); // counter (phase 1)
        v.extend_from_slice(&0u64.to_le_bytes()); // checkpoint_lsn (phase 6)
        // header_checksum (STORE_AUDIT.md S1) — this path never runs
        // Header::validate, so a placeholder is fine.
        v.extend_from_slice(&0u32.to_le_bytes());
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
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();
        (buf, page_counter)
    }

    // Isolation test for a squeal-sql-level bug (HashedSource's
    // bug_repro_rehash_past_272384_rows_loses_entries, in squeal-sql's
    // source/hash.rs): building a large hash table loses entries, and
    // both observed failure boundaries were exact multiples of 1024 —
    // suspicious, since `max_entries` (this buffer's resident-page cap
    // before eviction) is hardcoded to 1024 in Db::create_core_db. This
    // forces the same "page count exceeds max_entries" condition at a
    // tiny, fast scale (4 resident slots, 50 pages) to check whether
    // eviction itself is where a write goes missing.
    #[test]
    fn test_run_survives_eviction_when_page_count_exceeds_max_entries() {
        use crate::cursor::Cursor;
        let (buf, _counter) = make_buffer(0, 20);
        let mut run = crate::run::Run::create_slotted(Arc::new(buf)).unwrap();
        const NUM_PAGES: usize = 50;
        for page in 0..NUM_PAGES {
            if page > 0 {
                run.new_slotted_page().unwrap();
            }
            let value = page as u64;
            run.set_slot_at(page, 0, &value.to_le_bytes()).unwrap();
        }

        let mut cursor = run.cursor().unwrap();
        let mut seen = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen.push(u64::from_le_bytes(bytes));
        }
        assert_eq!(
            seen.len(),
            NUM_PAGES,
            "expected {NUM_PAGES} tuples, cursor yielded {}",
            seen.len()
        );
        assert_eq!(seen, (0..NUM_PAGES as u64).collect::<Vec<_>>());
    }

    // Closer to HashedSource::rehash's actual access pattern than the
    // single-run test above: TWO runs alive at once, sharing the same
    // small buffer pool — a cursor over run A stays live and gets read
    // from (`cursor.next()`) WHILE run B is separately allocated and
    // written to, exactly like rehash() keeps `run_cursor` (over the
    // old run) alive across the entire replay loop that builds the new
    // run. Twice the page count competing for the same max_entries
    // resident slots means twice the eviction pressure, and read+write
    // interleaving that the single-run test never exercises.
    #[test]
    fn test_run_to_run_copy_survives_eviction_when_both_runs_share_a_small_buffer() {
        use crate::cursor::Cursor;
        let (buf, _counter) = make_buffer(0, 20);
        let buf = Arc::new(buf);
        let mut run_a = crate::run::Run::create_slotted(buf.clone()).unwrap();
        const NUM_PAGES: usize = 50;
        for page in 0..NUM_PAGES {
            if page > 0 {
                run_a.new_slotted_page().unwrap();
            }
            let value = page as u64;
            run_a.set_slot_at(page, 0, &value.to_le_bytes()).unwrap();
        }

        // run_a's cursor is created and stays alive across the ENTIRE
        // copy into run_b below — mirrors rehash()'s
        // `let mut run_cursor = self.run.cursor()?;` followed by
        // building the new run while still reading through it.
        let mut cursor_a = run_a.cursor().unwrap();
        let mut run_b = crate::run::Run::create_slotted(buf).unwrap();
        let mut copied = Vec::new();
        let mut page_b = 0usize;
        while let Some(t) = cursor_a.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            if page_b > 0 {
                run_b.new_slotted_page().unwrap();
            }
            run_b.set_slot_at(page_b, 0, &bytes).unwrap();
            copied.push(u64::from_le_bytes(bytes));
            page_b += 1;
        }
        assert_eq!(
            copied.len(),
            NUM_PAGES,
            "expected to copy {NUM_PAGES} tuples out of run_a, got {}",
            copied.len()
        );
        assert_eq!(copied, (0..NUM_PAGES as u64).collect::<Vec<_>>());

        // Independently verify run_b itself holds everything that was
        // just written to it, via a fresh cursor.
        let mut cursor_b = run_b.cursor().unwrap();
        let mut seen_b = Vec::new();
        while let Some(t) = cursor_b.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen_b.push(u64::from_le_bytes(bytes));
        }
        assert_eq!(
            seen_b.len(),
            NUM_PAGES,
            "expected {NUM_PAGES} tuples in run_b, cursor yielded {}",
            seen_b.len()
        );
        assert_eq!(seen_b, (0..NUM_PAGES as u64).collect::<Vec<_>>());
    }

    // One more step closer to the real bug than the single A->B copy
    // above: a CHAIN of doubling generations (10 -> 20 -> 40 -> ... ->
    // 320 pages), each one copying the previous generation's content
    // into a fresh, bigger run while the old run's cursor stays live —
    // exactly HashedSource's own repeated-rehash-and-grow pattern
    // (insert_left doubles capacity every time the table fills, and the
    // real bug needed AT LEAST two such doublings — crossing 1024
    // pages, then 2048 — to manifest, not just one).
    #[test]
    fn test_chained_doubling_generations_survive_repeated_eviction() {
        use crate::cursor::Cursor;
        // 1, not 0: reserves PageId(0) the same way a real Db does (see
        // Db::create_system_tables — page 0 is the permanent "system"
        // table, never freed). PageId::is_valid_next_page() treats 0 as
        // the sentinel for "no next page" (self.0 != 0) — if a Run page
        // ever legitimately points at page 0 as its NEXT page (not just
        // as the sentinel value happening to equal a real page's id),
        // the chain walk can't tell the two apart and silently truncates
        // right there. A from-0 counter (the bug in this test's own
        // setup, not real Db behavior) lets page 0 get freed and reused
        // as an ordinary mid-chain Run page — exactly triggering that
        // ambiguity.
        let (buf, _counter) = make_buffer(1, 20);
        let buf = Arc::new(buf);

        let mut run = crate::run::Run::create_slotted(buf.clone()).unwrap();
        let mut page_count = 1usize;
        run.set_slot_at(0, 0, &0u64.to_le_bytes()).unwrap();

        for _gen in 0..6 {
            let next_page_count = page_count * 2;
            let mut cursor = run.cursor().unwrap();
            let mut next_run = crate::run::Run::create_slotted(buf.clone()).unwrap();
            let mut copied = Vec::new();
            let mut page = 0usize;
            while let Some(t) = cursor.next().unwrap() {
                let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
                if page > 0 {
                    next_run.new_slotted_page().unwrap();
                }
                next_run.set_slot_at(page, 0, &bytes).unwrap();
                copied.push(u64::from_le_bytes(bytes));
                page += 1;
            }
            assert_eq!(
                copied,
                (0..page_count as u64).collect::<Vec<_>>(),
                "generation growing to {next_page_count} pages lost entries during replay \
                 (started with {page_count})"
            );
            // Pad the new (bigger) run out to next_page_count pages,
            // continuing the value sequence — matches how a real
            // rehash's new capacity is bigger than what it just
            // replayed, with more real inserts landing after it.
            for extra in page_count..next_page_count {
                next_run.new_slotted_page().unwrap();
                next_run
                    .set_slot_at(extra, 0, &(extra as u64).to_le_bytes())
                    .unwrap();
            }
            run = next_run;
            page_count = next_page_count;
        }

        let mut final_cursor = run.cursor().unwrap();
        let mut seen = Vec::new();
        while let Some(t) = final_cursor.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen.push(u64::from_le_bytes(bytes));
        }
        assert_eq!(
            seen.len(),
            page_count,
            "expected {page_count} tuples in the final generation, cursor yielded {}",
            seen.len()
        );
        assert_eq!(seen, (0..page_count as u64).collect::<Vec<_>>());
    }

    // Same chain as above, except every prior generation's Run is kept
    // alive (pushed into `_kept`, never dropped) instead of being
    // replaced by `run = next_run`. If dropping an old generation frees
    // its pages back for a LATER generation's alloc_slotted_page() to
    // reuse, and a stale still-pending write for the freed PageId later
    // fires and clobbers whatever the new occupant wrote — a PageId-
    // reuse race with the async writer's pending queue — keeping every
    // generation alive (nothing ever freed, no PageId ever reused)
    // should make the loss disappear even though everything else about
    // the access pattern is identical.
    #[test]
    fn test_chained_doubling_generations_survive_repeated_eviction_when_old_runs_kept_alive() {
        use crate::cursor::Cursor;
        // Scaled up to match the real-world repro's own confirmed
        // parameters (HashedSource's rehash chain, max_entries=128 —
        // "fails after the 8th rehash every time" — vs. max_entries=4096,
        // where the same chain "works every time"). 1, not 0: reserves
        // PageId(0) the way a real Db does (see the other chained test's
        // own comment on why).
        let (buf, _counter) = make_buffer(1, 128);
        let buf = Arc::new(buf);

        let mut run = crate::run::Run::create_slotted(buf.clone()).unwrap();
        let mut page_count = 1usize;
        run.set_slot_at(0, 0, &0u64.to_le_bytes()).unwrap();
        let mut kept = vec![];

        for _gen in 0..12 {
            let next_page_count = page_count * 2;
            let mut cursor = run.cursor().unwrap();
            let mut next_run = crate::run::Run::create_slotted(buf.clone()).unwrap();
            let mut copied = Vec::new();
            let mut page = 0usize;
            while let Some(t) = cursor.next().unwrap() {
                let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
                if page > 0 {
                    next_run.new_slotted_page().unwrap();
                }
                next_run.set_slot_at(page, 0, &bytes).unwrap();
                copied.push(u64::from_le_bytes(bytes));
                page += 1;
            }
            assert_eq!(
                copied,
                (0..page_count as u64).collect::<Vec<_>>(),
                "generation growing to {next_page_count} pages lost entries during replay \
                 (started with {page_count}) EVEN WITH every prior generation kept alive"
            );
            for extra in page_count..next_page_count {
                next_run.new_slotted_page().unwrap();
                next_run
                    .set_slot_at(extra, 0, &(extra as u64).to_le_bytes())
                    .unwrap();
            }
            kept.push(run);
            run = next_run;
            page_count = next_page_count;
        }

        let mut final_cursor = run.cursor().unwrap();
        let mut seen = Vec::new();
        while let Some(t) = final_cursor.next().unwrap() {
            let bytes: [u8; 8] = t.data().try_into().expect("8-byte u64 tuple");
            seen.push(u64::from_le_bytes(bytes));
        }
        assert_eq!(
            seen.len(),
            page_count,
            "expected {page_count} tuples in the final generation, cursor yielded {}",
            seen.len()
        );
        assert_eq!(seen, (0..page_count as u64).collect::<Vec<_>>());
    }

    // A minimal open-addressing hash table built directly on Run, using
    // the SAME mechanics as squeal-sql's HashedSource (hash.rs):
    // multiple slots per page (every earlier test in this file used
    // exactly 1 slot/page — untested territory), real hash%capacity
    // placement with linear-probe collision resolution (not sequential
    // or an arbitrary permutation), and rehash-on-full doubling
    // capacity, replaying the old table's contents into a new one via a
    // live cursor over the old run while the new run is built — this is
    // the closest store-level analog yet to the real repro
    // (bug_repro_rehash_past_272384_rows_loses_entries in squeal-sql's
    // source/hash.rs), scaled to match its confirmed failing
    // parameters (max_entries=128, "fails after the 8th rehash").
    #[derive(Debug, Clone, Copy)]
    struct InsertRecord {
        generation: usize,
        page_id: PageId,
        page_idx: usize,
        slot: u64,
        // How many slots on THIS specific page_id had already been
        // filled before this one, within the current generation (0 =
        // first slot written to that page). Reset per generation since
        // page_id itself is fresh each rehash.
        fill_order_on_page: usize,
        pages_in_run_at_insert: usize,
    }

    struct MiniHashTable {
        run: Run<MemFile>,
        buf: Arc<PageBuffer<MemFile>>,
        records_per_page: usize,
        capacity: usize,
        occupied: Vec<bool>,
        count: usize,
        generation: usize,
        // Per-page_id fill counter, reset at the start of each
        // generation (new pages always start this history fresh, since
        // no PageId is ever reused across generations while reuse-
        // related mechanics remain in their normal, non-patched state).
        page_fill_counts: std::collections::HashMap<PageId, usize>,
        // Latest known insert site for every key ever inserted —
        // overwritten on replay, so a key present in the final table
        // shows its CURRENT location, and a key that went missing shows
        // the LAST location it was ever actually written to before
        // vanishing.
        history: std::collections::HashMap<u64, InsertRecord>,
    }

    impl MiniHashTable {
        fn new(buf: Arc<PageBuffer<MemFile>>, records_per_page: usize) -> Self {
            let run = crate::run::Run::create_slotted(buf.clone()).unwrap();
            Self {
                run,
                buf,
                records_per_page,
                capacity: records_per_page,
                occupied: vec![false; records_per_page],
                count: 0,
                generation: 0,
                page_fill_counts: std::collections::HashMap::new(),
                history: std::collections::HashMap::new(),
            }
        }

        // FNV-1a-ish — cheap, decent distribution, deterministic.
        fn hash(key: u64) -> u64 {
            let mut h = 0xcbf29ce484222325u64;
            for byte in key.to_le_bytes() {
                h ^= byte as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            h
        }

        fn insert(&mut self, key: u64) {
            if self.count == self.capacity {
                self.rehash(self.capacity * 2);
            }
            let mut idx = (Self::hash(key) % self.capacity as u64) as usize;
            while self.occupied[idx] {
                idx = (idx + 1) % self.capacity;
            }
            self.occupied[idx] = true;
            self.count += 1;
            let page_idx = idx / self.records_per_page;
            let slot = (idx % self.records_per_page) as u64;
            let page_id = self.run.page_ids()[page_idx];
            let fill_order_on_page = *self.page_fill_counts.get(&page_id).unwrap_or(&0);
            self.page_fill_counts
                .insert(page_id, fill_order_on_page + 1);
            self.history.insert(
                key,
                InsertRecord {
                    generation: self.generation,
                    page_id,
                    page_idx,
                    slot,
                    fill_order_on_page,
                    pages_in_run_at_insert: self.run.page_ids().len(),
                },
            );
            self.run
                .set_slot_at(page_idx, slot, &key.to_le_bytes())
                .unwrap();
        }

        fn rehash(&mut self, new_capacity: usize) {
            self.generation += 1;
            self.page_fill_counts.clear();
            let count = self.count;
            let mut cursor = self.run.cursor().unwrap();
            let new_pages = new_capacity.div_ceil(self.records_per_page);
            let mut new_run = crate::run::Run::create_slotted(self.buf.clone()).unwrap();
            for _ in 1..new_pages {
                new_run.new_slotted_page().unwrap();
            }
            self.run = new_run;
            self.capacity = new_capacity;
            self.occupied = vec![false; new_capacity];
            self.count = 0;
            let mut added = 0;
            // Drains whatever the old run's cursor actually has, rather
            // than enforcing `count` strictly and panicking on a short
            // read (the earlier version of this test) — lets the WHOLE
            // run continue to completion even if entries go missing
            // somewhere, so the final all_keys()/dedup check at the end
            // can report exactly which key(s) are gone, instead of
            // stopping at the first symptom.
            while let Some(t) = cursor.next().unwrap() {
                let bytes: [u8; 8] = t.data().try_into().unwrap();
                self.insert(u64::from_le_bytes(bytes));
                added += 1;
            }
            if added != count {
                eprintln!(
                    "rehash({new_capacity}): old run yielded {added} tuples, expected {count} \
                     ({} missing)",
                    count as i64 - added as i64
                );
            }
        }

        fn all_keys(&self) -> Vec<u64> {
            let mut cursor = self.run.cursor().unwrap();
            let mut out = vec![];
            while let Some(t) = cursor.next().unwrap() {
                let bytes: [u8; 8] = t.data().try_into().unwrap();
                out.push(u64::from_le_bytes(bytes));
            }
            out
        }
    }

    // Regression test for a real, previously-mysterious data-loss bug: the
    // async writer thread used to clear a page's dirty flag unconditionally
    // after flushing it, racing a concurrent mutation of that same shared
    // page (via a Weak-upgrade) landing between the flush's byte snapshot
    // and the dirty-clear — the mutation was then silently lost the next
    // time that page was evicted while wrongly believed clean. Fixed via
    // Page::dirty_version/mark_flushed_up_to (page.rs, two independent
    // monotonic counters — see mark_flushed_up_to's own comment for why a
    // single boolean, even guarded by a version check, still wasn't safe)
    // and write_page (this file). Before the fix this failed reliably
    // (13-18 of 50000 keys missing) at max_entries=128; passes reliably now.
    #[test]
    fn test_mini_hash_table_survives_repeated_rehash_under_eviction_pressure() {
        // Matches the real repro's confirmed parameters: works reliably
        // at max_entries=4096, failed after the 8th rehash at
        // max_entries=128 (records_per_page=133 there; 8 here, since
        // this test's own record size is fixed/tiny — the page COUNT
        // trajectory across rehashes is what needs to match, not the
        // exact records_per_page value).
        const MAX_ENTRIES: usize = 128;
        const RECORDS_PER_PAGE: usize = 8;
        const TOTAL_KEYS: u64 = 50_000; // comfortably past rehash 8 (2048+ capacity)

        let (buf, _counter) = make_buffer(1, MAX_ENTRIES);
        let buf = Arc::new(buf);
        let mut table = MiniHashTable::new(buf, RECORDS_PER_PAGE);
        for key in 0..TOTAL_KEYS {
            table.insert(key);
        }
        if table.count as u64 != TOTAL_KEYS {
            eprintln!(
                "table.count={} (self-corrects after each rehash — see its own replay loop) \
                 vs {TOTAL_KEYS} keys actually inserted",
                table.count
            );
        }

        let mut seen = table.all_keys();
        if seen.len() as u64 != TOTAL_KEYS {
            let mut present: Vec<u64> = seen.clone();
            present.sort_unstable();
            present.dedup();
            let present_set: std::collections::HashSet<u64> = present.iter().copied().collect();
            let missing: Vec<u64> = (0..TOTAL_KEYS)
                .filter(|k| !present_set.contains(k))
                .collect();
            eprintln!(
                "=== {} missing keys — last recorded insert site for each ===",
                missing.len()
            );
            for key in &missing {
                match table.history.get(key) {
                    Some(r) => eprintln!(
                        "key={key} gen={} page_id={:?} page_idx={} slot={} \
                         fill_order_on_page={} pages_in_run_at_insert={}",
                        r.generation,
                        r.page_id,
                        r.page_idx,
                        r.slot,
                        r.fill_order_on_page,
                        r.pages_in_run_at_insert
                    ),
                    None => eprintln!("key={key} — NO history entry at all (never inserted?!)"),
                }
            }
            // Cross-check: do missing keys cluster on the same page_id
            // (several slots on ONE physical page all silently lost) or
            // spread across many different, unrelated pages?
            let mut by_page: std::collections::HashMap<PageId, Vec<u64>> =
                std::collections::HashMap::new();
            for key in &missing {
                if let Some(r) = table.history.get(key) {
                    by_page.entry(r.page_id).or_default().push(*key);
                }
            }
            eprintln!("=== missing keys grouped by page_id ===");
            for (page_id, keys) in &by_page {
                eprintln!(
                    "  page_id={page_id:?}: {} missing key(s): {keys:?}",
                    keys.len()
                );
            }
        }
        assert_eq!(
            seen.len() as u64,
            TOTAL_KEYS,
            "expected {TOTAL_KEYS} keys, found {} — {} missing",
            seen.len(),
            TOTAL_KEYS as i64 - seen.len() as i64
        );
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            seen.len() as u64,
            TOTAL_KEYS,
            "duplicates present — {} unique keys out of {TOTAL_KEYS}",
            seen.len()
        );
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

    fn overflow_chain_ids(buf: &PageBuffer<MemFile>, primary: PageId) -> Vec<PageId> {
        let mut ids = vec![];
        let primary_header = buf.read_page_header(primary).unwrap();
        let mut cur = primary_header.next_page();
        loop {
            ids.push(cur);
            let h = buf.read_page_header(cur).unwrap();
            if !h.is_overflow() {
                break;
            }
            cur = h.next_page();
        }
        ids
    }

    // STORE_AUDIT.md P9: a same-size rewrite of an already-oversized page
    // must reuse the exact same physical overflow chain, not tear it down
    // and allocate a fresh one — see handle_large_page_size's own comment.
    // Goes through get_page_mut/write_locked_page (not write_page, which
    // always wraps a brand-new Arc — see write_page's own doc comment) to
    // exercise the same cached Arc a real BPlusTree::update() call reuses
    // across successive writes to the same row, since that persistence
    // (via Page's own overflow_page_count field) is exactly what the fix
    // relies on.
    #[test]
    fn test_repeated_same_size_overflow_write_reuses_the_existing_chain() {
        let page_size = 300u64;
        let (buf, page_counter, _) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();

        let big_data = vec![1u8; page_size as usize];
        let page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();

        let chain_before = overflow_chain_ids(&buf, page_id);
        assert!(
            !chain_before.is_empty(),
            "sanity: this write must have needed overflow pages"
        );
        let count_after_first_write = page_counter.load(Ordering::Relaxed);

        let handle = buf
            .get_page_mut(page_id, crate::buffer::LockLevel::Data)
            .unwrap();
        let different_big_data = vec![2u8; page_size as usize];
        handle
            .page
            .replace_tuple(&DBIdType::Int(1), Tuple::new(1, &different_big_data))
            .unwrap();
        buf.write_locked_page(handle).unwrap();

        let chain_after = overflow_chain_ids(&buf, page_id);
        let count_after_second_write = page_counter.load(Ordering::Relaxed);

        assert_eq!(
            chain_before, chain_after,
            "a same-size rewrite must reuse the exact same overflow chain page ids, \
             not tear down and reallocate a fresh one"
        );
        assert_eq!(
            count_after_first_write, count_after_second_write,
            "no new pages should have been allocated for a same-size rewrite"
        );

        let reread = buf.get_page(page_id).unwrap();
        assert_eq!(
            reread.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            different_big_data,
            "chain reuse must not leave stale bytes behind"
        );

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

    // write_locked_page no longer sends a write on every mutation — it only
    // updates the cache and leaves the disk write to eviction, checkpoint,
    // or shutdown (see its own doc comment). This exercises the eviction
    // side: a dirty page pushed out of the cache under memory pressure
    // must (a) still read back correctly in-process (the Weak-upgrade
    // path, or the flush having already landed) and (b) actually be
    // durable, not just cache-consistent — checked here by reopening a
    // second, independent buffer over the same backing bytes with an
    // empty cache, forcing a genuine disk read.
    #[test]
    fn test_evicting_a_dirty_page_flushes_it_before_a_fresh_buffer_can_see_it() {
        // ShardedPQ::new(max_entries / 10) needs max_entries >= 10 to avoid
        // a zero-shard-count divide, so eviction pressure here comes from
        // page *count* (11 pages, max_entries 10) rather than a tiny cap.
        const MAX_ENTRIES: usize = 10;
        let (buf, _, file_clone) = make_buffer_ps(PAGE_SIZE, MAX_ENTRIES as u64 + 1, MAX_ENTRIES);
        let page0: crate::page::PageId = 0u64.into();

        let handle = buf
            .get_page_mut(page0, crate::buffer::LockLevel::Data)
            .unwrap();
        handle.page.add_tuple(Tuple::new(1, b"hello")).unwrap();
        buf.write_locked_page(handle).unwrap();

        // Touching MAX_ENTRIES more pages fills the cache to capacity and
        // past it, forcing page0 (now the LRU resident) out. It's still
        // dirty at that point, so eviction must flush it (see
        // evict_lru_locked/flush_evicted) instead of silently dropping the
        // mutation.
        for i in 1..=MAX_ENTRIES as u64 {
            let _ = buf.get_page(i.into()).unwrap();
        }

        // Reading page0 again right away must still see the write — either
        // via the Weak-upgrade path (the writer thread's own in-flight Arc
        // clone keeps the evicted page alive) or, if the flush already
        // landed, from disk. Never a stale pre-mutation copy.
        let reread = buf.get_page(page0).unwrap();
        assert_eq!(
            reread.get(DBIdType::Int(1)).unwrap().unwrap().data.to_vec(),
            b"hello"
        );

        buf.checkpoint().unwrap();

        // A brand-new buffer over the same bytes has no cache at all, so
        // this read can only be satisfied from disk — proving the eviction
        // actually made it durable, not just cache-visible.
        let page_counter2 = Arc::new(AtomicU64::new(MAX_ENTRIES as u64 + 1));
        let buf2 = PageBuffer::new(
            PAGE_SIZE,
            page_counter2,
            file_clone,
            make_header(),
            MAX_ENTRIES,
            Arc::new(crate::logger::LsnClock::default()),
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();
        let from_disk = buf2.get_page(page0).unwrap();
        assert_eq!(
            from_disk
                .get(DBIdType::Int(1))
                .unwrap()
                .unwrap()
                .data
                .to_vec(),
            b"hello"
        );

        let _ = buf.shutdown();
        let _ = buf2.shutdown();
    }

    // STORE_AUDIT.md P3: direct, white-box test of evict_one's actual
    // decision — not an end-to-end test through get_page. An earlier
    // version of this test drove it through get_page instead, repeatedly
    // re-accessing "page 0" while pushing pages through a full cache; it
    // passed even with the second-chance check deleted entirely, because
    // re-fetching an EVICTED page transparently reinstalls it with a fresh
    // insertion sequence, which looks identical to "never evicted" by
    // Strong/Weak state alone — and checking Arc identity across
    // iterations didn't help either, since holding any external reference
    // to a page keeps its Weak entry upgradeable, masking a real eviction
    // as a same-object "reuse". Constructing the cache state directly
    // (bypassing get_page/install entirely) is what actually isolates the
    // mechanism: two Strong pages, one marked referenced, one not, both
    // already registered in access_map in insertion order — evict_one must
    // skip the referenced one (a real behavioral difference from plain
    // FIFO, which would target it as the oldest) and evict the other one
    // instead. Inserted directly into their own shards (via shard_for)
    // rather than through install/cache_strong, since P2's follow-up
    // sharded `buffer` itself — see PageBuffer's own doc comment on why
    // that's still safe to read back afterward without ever holding two
    // shards' locks at once.
    #[test]
    fn test_evict_one_gives_a_referenced_page_a_second_chance() {
        let (buf, _) = make_buffer(2, 10);
        let page_a = Arc::new(Page::new_data(PAGE_SIZE));
        let page_b = Arc::new(Page::new_data(PAGE_SIZE));
        // Phase 6: only clean pages are evictable (a dirty one is parked
        // until a checkpoint captures it); these are "already on disk".
        page_a.set_dirty(false).unwrap();
        page_b.set_dirty(false).unwrap();
        page_a.mark_referenced(); // A: accessed since it was cached.
        // B is left un-referenced (fresh Page starts with referenced=false).

        let id_a: PageId = 0u64.into();
        let id_b: PageId = 1u64.into();
        buf.shard_for(&id_a)
            .write()
            .insert(id_a, PageEntry::Strong(page_a.clone()));
        buf.shard_for(&id_b)
            .write()
            .insert(id_b, PageEntry::Strong(page_b.clone()));
        // A inserted (logically) before B, so a plain FIFO/LRU-by-age
        // policy with no referenced check would target A first.
        buf.access_map.push(id_a, buf.next_seq());
        buf.access_map.push(id_b, buf.next_seq());

        match buf.evict_one() {
            Evicted::Yes => {}
            Evicted::Exhausted => panic!("expected a victim to be evicted"),
        }

        let is_strong = |id: PageId| {
            matches!(
                buf.shard_for(&id).read().get(&id),
                Some(PageEntry::Strong(_))
            )
        };
        assert!(
            is_strong(id_a),
            "the referenced page must be given a second chance, not evicted"
        );
        assert!(
            !is_strong(id_b),
            "the un-referenced page must be the one actually evicted"
        );
    }

    // The other half of the same deferral: a page that's mutated but never
    // evicted (cache well under capacity) must still become durable once
    // checkpoint() runs — flush_dirty_cached_pages exists specifically to
    // catch this case, since flush_evicted only fires on the page actually
    // leaving the cache.
    #[test]
    fn test_checkpoint_flushes_a_dirty_page_that_was_never_evicted() {
        let (buf, _, file_clone) = make_buffer_ps(PAGE_SIZE, 1, 10); // generous max_entries: no eviction
        let page0: crate::page::PageId = 0u64.into();

        let handle = buf
            .get_page_mut(page0, crate::buffer::LockLevel::Data)
            .unwrap();
        handle.page.add_tuple(Tuple::new(1, b"world")).unwrap();
        buf.write_locked_page(handle).unwrap();

        buf.checkpoint().unwrap();

        let page_counter2 = Arc::new(AtomicU64::new(1));
        let buf2 = PageBuffer::new(
            PAGE_SIZE,
            page_counter2,
            file_clone,
            make_header(),
            10,
            Arc::new(crate::logger::LsnClock::default()),
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();
        let from_disk = buf2.get_page(page0).unwrap();
        assert_eq!(
            from_disk
                .get(DBIdType::Int(1))
                .unwrap()
                .unwrap()
                .data
                .to_vec(),
            b"world"
        );

        let _ = buf.shutdown();
        let _ = buf2.shutdown();
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

        fn successor(&self, id: &DBIdType) -> Result<Option<Tuple>, StoreError> {
            Ok(self
                .tuples
                .iter()
                .filter(|t| t.id > *id)
                .min_by(|a, b| a.id.cmp(&b.id))
                .cloned())
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
        crate::pages::content::PageContentKind(3);

    fn registry_with_test_bucket() -> crate::pages::content::PageContentRegistry {
        let mut registry = crate::pages::content::PageContentRegistry::builtin();
        registry
            .register(
                TEST_BUCKET_KIND,
                Arc::new(|bytes| {
                    Ok(Box::new(TestBucketPage::from_bytes(bytes)?)
                        as Box<dyn crate::pages::PageTuple>)
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
        let header = Arc::new(from_bytes::<Header>(&make_header_bytes(0, 0, page_size)).unwrap());

        let buf = PageBuffer::new(
            page_size,
            page_counter.clone(),
            mem,
            header,
            10,
            Arc::new(crate::logger::LsnClock::default()),
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
        let header = Arc::new(from_bytes::<Header>(&make_header_bytes(0, 0, page_size)).unwrap());
        let buf = PageBuffer::new(
            page_size,
            page_counter.clone(),
            mem,
            header,
            10,
            Arc::new(crate::logger::LsnClock::default()),
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
        assert!(matches!(err, StoreError::UnknownPageContentKind(3)));
        let _ = buf.shutdown();
    }

    // ── page reliability: magic number + checksum ──────────────────────────

    #[test]
    fn test_get_page_detects_a_bitflipped_data_byte_via_checksum() {
        let (buf, page_counter, file_clone) = make_buffer_ps(PAGE_SIZE, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let page = Page::new_data(PAGE_SIZE);
        page.add_tuple(Tuple::new(1, b"hello")).unwrap();
        buf.write_page(page_id, &page).unwrap();
        // shutdown flushes the write to the shared MemFile before we corrupt it.
        buf.shutdown().unwrap();

        // Flip a byte inside the page's data region, well past the header, directly on disk.
        let corrupt_offset = crate::page::PageHeader::header_size() as u64 + 5;
        let mut byte = [0u8; 1];
        file_clone.pread(&mut byte, corrupt_offset).unwrap();
        byte[0] ^= 0xFF;
        file_clone.pwrite(&byte, corrupt_offset).unwrap();

        // Fresh buffer over the same (now-corrupted) backing storage: page_id
        // was never evicted from the first buffer's cache, so re-reading
        // through it would just return the cached (uncorrupted) copy without
        // ever touching disk.
        let page_count = page_counter.load(Ordering::Relaxed);
        let page_counter2 = Arc::new(AtomicU64::new(page_count));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, page_count, PAGE_SIZE)).unwrap());
        let buf2 = PageBuffer::new(
            PAGE_SIZE,
            page_counter2,
            file_clone,
            header2,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();

        let err = buf2.get_page(page_id).unwrap_err();
        assert!(matches!(err, StoreError::PageChecksumMismatch(id) if id == page_id));
        let _ = buf2.shutdown();
    }

    #[test]
    fn test_get_page_detects_a_missing_magic_number_on_a_garbage_page() {
        let (buf, page_counter, file_clone) = make_buffer_ps(PAGE_SIZE, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        buf.shutdown().unwrap();

        // Zero out the whole slot, as if it were never actually written
        // (a garbage/corrupt page id, or a read racing an allocation).
        let zeros = vec![0u8; PAGE_SIZE as usize];
        file_clone
            .pwrite(&zeros, super::page_offset(page_id, PAGE_SIZE, 0))
            .unwrap();

        let page_count = page_counter.load(Ordering::Relaxed);
        let page_counter2 = Arc::new(AtomicU64::new(page_count));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, page_count, PAGE_SIZE)).unwrap());
        let buf2 = PageBuffer::new(
            PAGE_SIZE,
            page_counter2,
            file_clone,
            header2,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();

        let err = buf2.get_page(page_id).unwrap_err();
        assert!(matches!(err, StoreError::InvalidPageMagic(id) if id == page_id));
        let _ = buf2.shutdown();
    }

    #[test]
    fn test_get_page_detects_corruption_in_an_overflow_continuation_page() {
        let page_size = 300u64;
        let (buf, page_counter, file_clone) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let count_before = page_counter.load(Ordering::Relaxed);

        // Large enough to need several overflow continuation pages, not just one.
        let big_data = vec![9u8; page_size as usize * 3];
        let page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();
        buf.shutdown().unwrap();

        let count_after = page_counter.load(Ordering::Relaxed);
        assert!(
            count_after > count_before + 1,
            "expected more than one overflow continuation page for this test to be meaningful"
        );
        // alloc_overflow_page hands out ids sequentially starting right after
        // the primary — this is the first continuation page in the chain.
        let continuation_id: PageId = count_before.into();

        // Flip a byte inside that continuation page's own data region — not
        // the primary's — to specifically exercise per-physical-page
        // verification of an overflow chain, not just the primary.
        let corrupt_offset =
            super::page_offset(continuation_id, page_size, 0) + PAGE_OVERHEAD as u64 + 2;
        let mut byte = [0u8; 1];
        file_clone.pread(&mut byte, corrupt_offset).unwrap();
        byte[0] ^= 0xFF;
        file_clone.pwrite(&byte, corrupt_offset).unwrap();

        let page_counter2 = Arc::new(AtomicU64::new(count_after));
        let header2 =
            Arc::new(from_bytes::<Header>(&make_header_bytes(0, count_after, page_size)).unwrap());
        let buf2 = PageBuffer::new(
            page_size,
            page_counter2,
            file_clone,
            header2,
            10,
            Arc::new(crate::logger::LsnClock::default()),
            Arc::new(crate::pages::content::PageContentRegistry::builtin()),
        )
        .unwrap();

        let err = buf2.get_page(page_id).unwrap_err();
        assert!(matches!(err, StoreError::PageChecksumMismatch(id) if id == continuation_id));
        let _ = buf2.shutdown();
    }

    // STORE_AUDIT.md P2 survey follow-up: throwaway (not a criterion bench,
    // same call as P3's own microbenchmark — see BASELINE.md) measurement
    // of get_page's Strong-hit path under concurrency, on disjoint pages
    // spread across 8 threads. Run with --ignored, alone, on both this
    // (sharded `buffer`) revision and the pre-sharding revision (a single
    // RwLock<HashMap<..>>) to compare — cache is pre-populated first so the
    // loop measures lock-acquisition contention, not disk I/O or eviction.
    #[test]
    #[ignore]
    fn bench_get_page_concurrent_disjoint_pages() {
        const NUM_PAGES: u64 = 8 * 64;
        const THREADS: u64 = 8;
        const ITERS_PER_THREAD: u64 = 200_000;
        let (buf, _) = make_buffer(NUM_PAGES, NUM_PAGES as usize + 1);
        for i in 0..NUM_PAGES {
            let _ = buf.get_page(i.into()).unwrap();
        }
        let buf = Arc::new(buf);
        let start = std::time::Instant::now();
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let buf = buf.clone();
                s.spawn(move || {
                    let base = t * (NUM_PAGES / THREADS);
                    let span = NUM_PAGES / THREADS;
                    for i in 0..ITERS_PER_THREAD {
                        let page_num: PageId = (base + (i % span)).into();
                        let _ = buf.get_page(page_num).unwrap();
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        eprintln!(
            "bench_get_page_concurrent_disjoint_pages: {THREADS} threads x {ITERS_PER_THREAD} \
             iters in {elapsed:?} ({:.0} ops/s)",
            (THREADS * ITERS_PER_THREAD) as f64 / elapsed.as_secs_f64()
        );
        let buf = Arc::try_unwrap(buf).unwrap_or_else(|_| panic!("buf still shared"));
        let _ = buf.shutdown();
    }

    // STORE_AUDIT.md P9 — throwaway (not a committed criterion bench, same
    // call as this session's other direct microbenchmarks) wall-clock
    // measurement of repeated same-size updates to a single overflow page —
    // the case handle_large_page_size's chain-reuse fast path targets. Run
    // the identical test text against this revision and against
    // `git show <pre-fix>:store/src/buffer.rs` (patched with this same fn,
    // dropping the now-nonexistent overflow_page_count() call in favor of
    // whatever the old code did — i.e. nothing, since the old code has no
    // such check) to get a before/after comparison — see BASELINE.md.
    #[test]
    #[ignore]
    fn bench_repeated_same_size_overflow_write() {
        const ITERS: u64 = 2_000;
        let page_size = 300u64;
        let (buf, _, _) = make_buffer_ps(page_size, 0, 10);
        let page_id = buf.alloc_page(false).unwrap();
        let big_data = vec![0u8; page_size as usize];
        let page = Page::new_data(page_size);
        page.add_tuple(Tuple::new(1, &big_data)).unwrap();
        buf.write_page(page_id, &page).unwrap();

        let start = std::time::Instant::now();
        for i in 0..ITERS {
            let handle = buf
                .get_page_mut(page_id, crate::buffer::LockLevel::Data)
                .unwrap();
            let data = vec![(i % 256) as u8; page_size as usize];
            handle
                .page
                .replace_tuple(&DBIdType::Int(1), Tuple::new(1, &data))
                .unwrap();
            buf.write_locked_page(handle).unwrap();
        }
        let elapsed = start.elapsed();
        eprintln!(
            "bench_repeated_same_size_overflow_write: {ITERS} writes in {elapsed:?} \
             ({:.0} writes/s)",
            ITERS as f64 / elapsed.as_secs_f64()
        );
        let _ = buf.shutdown();
    }

    // ---- Phase 5: enforced lock order, one generous timeout ----

    // Locks are ordered Index → Data; a request that would take them in
    // the other order (or a second Data page) is refused at once with the
    // full held-set in the message. It never waits: an out-of-order request
    // is the one thing that could deadlock, and a hang is not a diagnosis.
    #[test]
    fn test_lock_order_violation_is_refused_immediately() {
        use crate::buffer::LockLevel::{Data, Index};
        let (buf, _) = make_buffer(10, 100);
        let a = buf.alloc_page(false).unwrap();
        let b = buf.alloc_page(false).unwrap();

        let held = buf.get_page_mut(a, Data).unwrap();
        let start = std::time::Instant::now();
        match buf.get_page_mut(b, Index) {
            Err(StoreError::LockOrderViolation(msg)) => {
                assert!(msg.contains("Index"), "{msg}");
                assert!(
                    msg.contains(&format!("{a:?}")),
                    "must name what is held: {msg}"
                );
            }
            other => panic!("expected LockOrderViolation, got {other:?}"),
        }
        assert!(
            matches!(
                buf.get_page_mut(b, Data),
                Err(StoreError::LockOrderViolation(_))
            ),
            "a second, different Data page is out of order too"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_millis(100),
            "an order violation must not wait for the lock"
        );
        // Re-entering the same page is fine (ArcLock is reentrant per thread).
        drop(buf.get_page_mut(a, Data).unwrap());
        drop(held);

        // With nothing held, Index then Data is the sanctioned order, and
        // Index after Index (a descent) is fine.
        let root = buf.get_page_mut(b, Index).unwrap();
        let child = buf.get_page_mut(a, Index).unwrap();
        let leaf_data = buf.get_page_mut(a, Data).unwrap();
        drop(leaf_data);
        drop(child);
        drop(root);
        let _ = buf.shutdown();
    }

    // A lock wait past the timeout fails — it does not hang — and the error
    // says who holds the lock and for how long, plus what the waiter itself
    // holds, so a stuck production system reports the deadlock instead of
    // exhibiting it.
    #[test]
    fn test_lock_timeout_fails_fast_and_names_the_holder() {
        use crate::buffer::LockLevel::Data;
        let (buf, _) = make_buffer(10, 100);
        let buf = Arc::new(buf);
        buf.set_lock_timeout(std::time::Duration::from_millis(50));
        let a = buf.alloc_page(false).unwrap();

        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = {
            let buf = Arc::clone(&buf);
            std::thread::Builder::new()
                .name("lock-holder".into())
                .spawn(move || {
                    let h = buf.get_page_mut(a, Data).unwrap();
                    held_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    drop(h);
                })
                .unwrap()
        };
        held_rx.recv().unwrap();

        let start = std::time::Instant::now();
        let r = buf.get_page_mut(a, Data);
        let waited = start.elapsed();
        match r {
            Err(StoreError::LockTimeout(msg)) => {
                assert!(
                    msg.contains("lock-holder"),
                    "must name the holder thread: {msg}"
                );
                assert!(msg.contains("held by"), "{msg}");
                assert!(msg.contains(&format!("{a:?}")), "{msg}");
            }
            other => panic!("expected LockTimeout, got {other:?}"),
        }
        assert!(
            waited >= std::time::Duration::from_millis(50),
            "returned before the timeout: {waited:?}"
        );
        assert!(
            waited < std::time::Duration::from_secs(1),
            "did not honour the configured timeout: {waited:?}"
        );

        release_tx.send(()).unwrap();
        holder.join().unwrap();
        // The failed wait left no bookkeeping behind: the page is takeable.
        drop(buf.get_page_mut(a, Data).unwrap());
        if let Ok(buf) = Arc::try_unwrap(buf) {
            let _ = buf.shutdown();
        }
    }

    // The "no page lock across a blocking wait" rule is checked, not assumed.
    #[test]
    #[should_panic(expected = "while holding page locks")]
    fn test_blocking_wait_with_a_page_lock_held_is_caught() {
        let (buf, _) = make_buffer(10, 100);
        let a = buf.alloc_page(false).unwrap();
        let _held = buf.get_page_mut(a, crate::buffer::LockLevel::Data).unwrap();
        crate::buffer::debug_assert_no_page_locks_held("test wait");
    }

    #[test]
    fn test_no_page_locks_held_passes_once_handles_drop() {
        let (buf, _) = make_buffer(10, 100);
        let a = buf.alloc_page(false).unwrap();
        let held = buf.get_page_mut(a, crate::buffer::LockLevel::Data).unwrap();
        drop(held);
        crate::buffer::debug_assert_no_page_locks_held("test wait");
        let _ = buf.shutdown();
    }
}
