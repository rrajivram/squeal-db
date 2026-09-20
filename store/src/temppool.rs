//! A dedicated page pool for query-execution scratch space (`Run`s: sort
//! spills, hash-join tables, temp tables).
//!
//! Run pages used to live in the database's own `PageBuffer`, so they rode
//! through the WAL-gated checkpoint machinery, the persisted free list and
//! the (small) core cache. None of that applies to scratch data: it needs no
//! WAL, no LSN gate, no checkpoint, no recovery and no persistence. This pool
//! keeps only what a run needs:
//!
//! - an in-memory cache with CLOCK second-chance eviction, sized in pages;
//! - a *dirty* victim is written to a private temp file (`<db>.tmp`) at
//!   `page_id * page_size` and then dropped, a clean one is just dropped, so
//!   memory is bounded without any checkpoint;
//! - the file is created lazily on the first dirty eviction (a small query
//!   never touches the disk) and removed again when the last page is freed,
//!   giving the space back;
//! - the pool is wiped at database open, so a crash leaks nothing.
//!
//! A page held by a caller (an `Arc<Page>` outstanding) is never evicted:
//! mutations happen in place through that `Arc`, and evicting it mid-write
//! would lose them.

use std::{collections::HashMap, fs::OpenOptions, sync::Arc, sync::atomic::AtomicUsize};

use parking_lot::{Mutex, MutexGuard};

use crate::{
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{Page, PageId},
    pages::content::PageContentRegistry,
};

/// Bytes reserved at the start of every temp page for its header. Run pages
/// never carry a `high_key`, so only the fixed header fields (measured at 62
/// bytes worst case, see `page.rs`) are needed.
pub(crate) const TEMP_PAGE_OVERHEAD: usize = 64;

/// Default in-memory budget of a pool, in bytes.
pub const DEFAULT_TEMP_CACHE_BYTES: u64 = 64 * 1024 * 1024;

const STRIPES: usize = 16;

/// A locked-for-mutation page. The stripe lock is what keeps two threads'
/// read-modify-write of the same page atomic (see `Run::set_slot_at`).
pub(crate) struct TempWriteHandle<'a> {
    pub(crate) page: Arc<Page>,
    _guard: MutexGuard<'a, ()>,
}

/// A point-in-time view of a pool, for `Db::stats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TempStats {
    /// Pages currently allocated to live runs.
    pub live_pages: u64,
    /// Pages resident in the pool's cache.
    pub cached_pages: usize,
    /// Bytes in the temp file right now (0 when it does not exist).
    pub file_bytes: u64,
    /// Pages written to the temp file over the pool's life.
    pub spills: u64,
}

struct State<F> {
    /// Slab of cached pages; `None` slots are on `free_slots`.
    slots: Vec<Option<(PageId, Arc<Page>)>>,
    free_slots: Vec<usize>,
    index: HashMap<PageId, usize>,
    /// CLOCK hand: next slot the eviction sweep looks at.
    hand: usize,
    /// Page ids handed back by `free_pages`, reused before growing.
    free_ids: Vec<PageId>,
    /// Next never-used id. Starts at 1: id 0 means "no next page".
    next_id: u64,
    live: u64,
    file: Option<F>,
    spills: u64,
}

pub(crate) struct TempPool<F: DBFile + 'static> {
    page_size: DBSizeType,
    /// A handle in the database's namespace, used to open/remove the temp
    /// file as a sibling.
    base: F,
    path: String,
    registry: PageContentRegistry,
    cache_pages: AtomicUsize,
    state: Mutex<State<F>>,
    stripes: [Mutex<()>; STRIPES],
}

impl<F> TempPool<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    /// A pool for the database `name`, its file being `<name>.tmp`. Any
    /// leftover file from a previous run (crash) is removed.
    pub(crate) fn new(
        name: &str,
        base: F,
        page_size: DBSizeType,
        cache_bytes: u64,
    ) -> Result<Self, StoreError> {
        let path = format!("{name}.tmp");
        base.remove_sibling(&path)?;
        Ok(Self {
            page_size,
            base,
            path,
            registry: PageContentRegistry::builtin(),
            cache_pages: AtomicUsize::new(Self::pages_for(cache_bytes, page_size)),
            state: Mutex::new(State {
                slots: Vec::new(),
                free_slots: Vec::new(),
                index: HashMap::new(),
                hand: 0,
                free_ids: Vec::new(),
                next_id: 1,
                live: 0,
                file: None,
                spills: 0,
            }),
            stripes: std::array::from_fn(|_| Mutex::new(())),
        })
    }

    fn pages_for(bytes: u64, page_size: DBSizeType) -> usize {
        ((bytes / page_size.max(1)) as usize).max(1)
    }

    pub(crate) fn set_cache_bytes(&self, bytes: u64) {
        self.cache_pages.store(
            Self::pages_for(bytes, self.page_size),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn page_size(&self) -> DBSizeType {
        self.page_size
    }

    pub(crate) fn page_overhead(&self) -> usize {
        TEMP_PAGE_OVERHEAD
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn stats(&self) -> TempStats {
        let st = self.state.lock();
        TempStats {
            live_pages: st.live,
            cached_pages: st.index.len(),
            file_bytes: st
                .file
                .as_ref()
                .and_then(|f| f.get_metadata().ok())
                .map(|m| m.len)
                .unwrap_or(0),
            spills: st.spills,
        }
    }

    /// Removes the temp file. Called at close; the pool holds nothing worth
    /// keeping.
    pub(crate) fn remove_file(&self) -> Result<(), StoreError> {
        let mut st = self.state.lock();
        st.file = None;
        self.base.remove_sibling(&self.path)?;
        Ok(())
    }

    pub(crate) fn alloc_run_page(&self) -> Result<PageId, StoreError> {
        self.alloc(Page::new_run(self.page_size, TEMP_PAGE_OVERHEAD))
    }

    pub(crate) fn alloc_slotted_page(&self) -> Result<PageId, StoreError> {
        self.alloc(Page::new_slotted(self.page_size, TEMP_PAGE_OVERHEAD))
    }

    fn alloc(&self, page: Page) -> Result<PageId, StoreError> {
        let mut st = self.state.lock();
        let id = match st.free_ids.pop() {
            Some(id) => id,
            None => {
                let id = PageId(st.next_id);
                st.next_id += 1;
                id
            }
        };
        st.live += 1;
        self.insert(&mut st, id, Arc::new(page))?;
        Ok(id)
    }

    pub(crate) fn get_page(&self, id: PageId) -> Result<Arc<Page>, StoreError> {
        let mut st = self.state.lock();
        if let Some(&slot) = st.index.get(&id) {
            let page = &st.slots[slot].as_ref().expect("indexed slot is occupied").1;
            page.mark_referenced();
            return Ok(page.clone());
        }
        debug_assert!(!st.free_ids.contains(&id), "temp page {id:?} was freed");
        if id.0 == 0 || id.0 >= st.next_id {
            return Err(StoreError::UnknownError(format!(
                "temp page {id:?} is not allocated"
            )));
        }
        let page = Arc::new(self.read(&st, id)?);
        self.insert(&mut st, id, page.clone())?;
        Ok(page)
    }

    /// The page, plus the stripe lock that serializes read-modify-write on
    /// it. The page cannot be evicted while the handle is alive.
    pub(crate) fn get_page_mut(&self, id: PageId) -> Result<TempWriteHandle<'_>, StoreError> {
        let page = self.get_page(id)?;
        let guard = self.stripes[(id.0 as usize) % STRIPES].lock();
        Ok(TempWriteHandle {
            page,
            _guard: guard,
        })
    }

    /// Publishing a mutation is a no-op here — the `Page` tracks its own
    /// dirtiness and the cache holds the same `Arc` — kept so callers read
    /// like they did against `PageBuffer`. Dropping the handle releases the
    /// stripe.
    pub(crate) fn write_locked_page(&self, handle: TempWriteHandle<'_>) -> Result<(), StoreError> {
        drop(handle);
        Ok(())
    }

    pub(crate) fn set_data_chain_next(&self, from: PageId, to: PageId) -> Result<(), StoreError> {
        let handle = self.get_page_mut(from)?;
        handle.page.set_next_page(to)?;
        self.write_locked_page(handle)
    }

    /// Returns `ids` to the pool without writing anything. Pages need not be
    /// resident. When the last live page goes, the temp file is deleted.
    pub(crate) fn free_pages(&self, ids: &[PageId]) -> Result<(), StoreError> {
        let mut st = self.state.lock();
        for &id in ids {
            if let Some(slot) = st.index.remove(&id) {
                st.slots[slot] = None;
                st.free_slots.push(slot);
            }
            st.free_ids.push(id);
            st.live = st.live.saturating_sub(1);
        }
        if st.live == 0 {
            // Nothing references the file or any id any more: start over.
            st.slots.clear();
            st.free_slots.clear();
            st.index.clear();
            st.hand = 0;
            st.free_ids.clear();
            st.next_id = 1;
            if st.file.take().is_some() {
                self.base.remove_sibling(&self.path)?;
            }
        }
        Ok(())
    }

    fn offset(&self, id: PageId) -> u64 {
        id.0 * self.page_size
    }

    fn read(&self, st: &State<F>, id: PageId) -> Result<Page, StoreError> {
        let file = st.file.as_ref().ok_or_else(|| {
            StoreError::UnknownError(format!("temp page {id:?} was never spilled"))
        })?;
        let mut bytes = vec![0u8; self.page_size as usize];
        let mut done = 0;
        while done < bytes.len() {
            let n = file.pread(&mut bytes[done..], self.offset(id) + done as u64)?;
            if n == 0 {
                return Err(StoreError::UnknownError(format!(
                    "temp page {id:?} is past the end of the temp file"
                )));
            }
            done += n;
        }
        Page::from_bytes(&bytes, &self.registry, TEMP_PAGE_OVERHEAD)
    }

    fn write(&self, st: &mut State<F>, id: PageId, page: &Page) -> Result<(), StoreError> {
        if st.file.is_none() {
            let opts = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .clone();
            st.file = Some(self.base.open_sibling(&self.path, opts)?);
        }
        let file = st.file.as_ref().expect("just opened");
        let bytes = page.to_bytes();
        let mut done = 0;
        while done < bytes.len() {
            let n = file.pwrite(&bytes[done..], self.offset(id) + done as u64)?;
            if n == 0 {
                return Err(StoreError::UnknownError("temp file write made no progress".into()));
            }
            done += n;
        }
        st.spills += 1;
        Ok(())
    }

    /// Makes `page` resident as `id`, evicting first if the cache is full.
    fn insert(&self, st: &mut State<F>, id: PageId, page: Arc<Page>) -> Result<(), StoreError> {
        let cap = self.cache_pages.load(std::sync::atomic::Ordering::Relaxed);
        while st.index.len() >= cap {
            if !self.evict_one(st)? {
                break; // everything pinned: exceed the budget rather than fail
            }
        }
        page.mark_referenced();
        let slot = match st.free_slots.pop() {
            Some(s) => {
                st.slots[s] = Some((id, page));
                s
            }
            None => {
                st.slots.push(Some((id, page)));
                st.slots.len() - 1
            }
        };
        st.index.insert(id, slot);
        Ok(())
    }

    /// One CLOCK sweep: evicts the first page that is neither held by a
    /// caller nor recently referenced. `false` if every page is pinned.
    fn evict_one(&self, st: &mut State<F>) -> Result<bool, StoreError> {
        let n = st.slots.len();
        for _ in 0..n * 2 {
            let i = st.hand;
            st.hand = (st.hand + 1) % n;
            let Some((id, page)) = st.slots[i].as_ref() else {
                continue;
            };
            if Arc::strong_count(page) > 1 || page.take_referenced() {
                continue;
            }
            let (id, page) = (*id, page.clone());
            if page.is_dirty() {
                // The flush stays under the pool lock so a concurrent miss
                // on this id can never read the file before it is written.
                self.write(st, id, &page)?;
            }
            st.slots[i] = None;
            st.free_slots.push(i);
            st.index.remove(&id);
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::Opener, memfile::MemFile, tuple::Tuple};

    const PS: DBSizeType = 1024;

    fn make_pool(cache_pages: u64) -> TempPool<MemFile> {
        pool(cache_pages)
    }

    fn pool(cache_pages: u64) -> TempPool<MemFile> {
        TempPool::new(
            "temppool_test.db",
            MemFile::new(),
            PS,
            cache_pages * PS,
        )
        .unwrap()
    }

    fn put<F: DBFile<Item = F> + 'static>(pool: &TempPool<F>, id: PageId, data: &[u8]) {
        let h = pool.get_page_mut(id).unwrap();
        h.page.add_tuple(Tuple::new(0, data)).unwrap();
        pool.write_locked_page(h).unwrap();
    }

    fn contents<F: DBFile<Item = F> + 'static>(pool: &TempPool<F>, id: PageId) -> Vec<Vec<u8>> {
        pool.get_page(id)
            .unwrap()
            .iter()
            .map(|t| t.data().to_vec())
            .collect()
    }

    #[test]
    fn test_alloc_free_reuse_and_ids_start_at_one() {
        let p = pool(8);
        let a = p.alloc_run_page().unwrap();
        let b = p.alloc_run_page().unwrap();
        assert_eq!((a.0, b.0), (1, 2));
        assert_eq!(p.stats().live_pages, 2);
        p.free_pages(&[a]).unwrap();
        assert_eq!(p.stats().live_pages, 1);
        assert_eq!(p.alloc_run_page().unwrap(), a, "a freed id is reused");
        assert!(p.get_page(PageId(9)).is_err());
    }

    #[test]
    fn test_file_is_created_lazily_and_only_on_dirty_eviction() {
        let base = MemFile::new();
        let p = TempPool::new("lazy.db", base.clone(), PS, 4 * PS).unwrap();
        let ids: Vec<_> = (0..4).map(|_| p.alloc_run_page().unwrap()).collect();
        assert!(base.list_siblings("lazy.db.tmp").unwrap().is_empty());
        assert_eq!(p.stats().spills, 0);
        // One more page than the cache holds forces a dirty eviction.
        let _extra = p.alloc_run_page().unwrap();
        assert_eq!(base.list_siblings("lazy.db.tmp").unwrap().len(), 1);
        assert_eq!(p.stats().spills, 1);
        assert!(p.stats().file_bytes > 0);
        assert_eq!(p.stats().cached_pages, 4);
        drop(ids);
    }

    #[test]
    fn test_dirty_eviction_round_trip_under_a_tiny_cache() {
        let p = pool(2);
        let ids: Vec<_> = (0..10).map(|_| p.alloc_run_page().unwrap()).collect();
        for (i, id) in ids.iter().enumerate() {
            put(&p, *id, format!("page-{i}").as_bytes());
        }
        assert!(p.stats().cached_pages <= 2);
        // Twice, so a page read back (clean) and evicted again still survives.
        for _ in 0..2 {
            for (i, id) in ids.iter().enumerate() {
                assert_eq!(contents(&p, *id), vec![format!("page-{i}").into_bytes()]);
            }
        }
    }

    #[test]
    fn test_modifying_a_reloaded_page_is_written_back_again() {
        let p = pool(1);
        let a = p.alloc_run_page().unwrap();
        put(&p, a, b"one");
        let b = p.alloc_run_page().unwrap(); // evicts a
        put(&p, b, b"other");
        put(&p, a, b"two"); // reload a, mutate, evicts b
        let _ = p.get_page(b).unwrap(); // evicts a again
        assert_eq!(contents(&p, a), vec![b"one".to_vec(), b"two".to_vec()]);
    }

    #[test]
    fn test_a_held_page_is_never_evicted() {
        let p = pool(1);
        let a = p.alloc_run_page().unwrap();
        let held = p.get_page_mut(a).unwrap();
        // Everything else churns through a one-page cache while `a` is held.
        let others: Vec<_> = (0..5).map(|_| p.alloc_run_page().unwrap()).collect();
        held.page.add_tuple(Tuple::new(0, b"kept")).unwrap();
        p.write_locked_page(held).unwrap();
        drop(others);
        assert_eq!(contents(&p, a), vec![b"kept".to_vec()]);
    }

    #[test]
    fn test_freeing_writes_nothing_and_the_last_free_removes_the_file() {
        let base = MemFile::new();
        let p = TempPool::new("reclaim.db", base.clone(), PS, 2 * PS).unwrap();
        let ids: Vec<_> = (0..6).map(|_| p.alloc_run_page().unwrap()).collect();
        let spills = p.stats().spills;
        assert!(spills > 0);
        p.free_pages(&ids[..3]).unwrap();
        assert_eq!(p.stats().spills, spills, "freeing must not write pages");
        assert_eq!(base.list_siblings("reclaim.db.tmp").unwrap().len(), 1);
        p.free_pages(&ids[3..]).unwrap();
        assert!(base.list_siblings("reclaim.db.tmp").unwrap().is_empty());
        let s = p.stats();
        assert_eq!((s.live_pages, s.cached_pages, s.file_bytes), (0, 0, 0));
        // And the pool is fully usable afterwards, ids starting over.
        let a = p.alloc_run_page().unwrap();
        assert_eq!(a.0, 1);
    }

    #[test]
    fn test_leftover_file_is_removed_at_creation_and_on_remove_file() {
        let base = MemFile::new();
        base.open_sibling("left.db.tmp", OpenOptions::new().create(true).clone())
            .unwrap();
        let p = TempPool::new("left.db", base.clone(), PS, 2 * PS).unwrap();
        assert!(base.list_siblings("left.db.tmp").unwrap().is_empty());
        for _ in 0..4 {
            p.alloc_run_page().unwrap();
        }
        assert_eq!(base.list_siblings("left.db.tmp").unwrap().len(), 1);
        p.remove_file().unwrap();
        assert!(base.list_siblings("left.db.tmp").unwrap().is_empty());
    }

    #[test]
    fn test_slotted_pages_survive_eviction() {
        let p = pool(2);
        let ids: Vec<_> = (0..6).map(|_| p.alloc_slotted_page().unwrap()).collect();
        for (i, id) in ids.iter().enumerate() {
            let h = p.get_page_mut(*id).unwrap();
            h.page
                .add_tuple(Tuple::new(i as u64, format!("s{i}").as_bytes()))
                .unwrap();
            p.write_locked_page(h).unwrap();
        }
        for (i, id) in ids.iter().enumerate() {
            let page = p.get_page(*id).unwrap();
            let t = page.get(crate::tuple::DBIdType::Int(i as u64)).unwrap().unwrap();
            assert_eq!(t.data(), format!("s{i}").as_bytes());
        }
    }

    #[test]
    fn test_chain_links_survive_eviction() {
        let p = pool(2);
        let ids: Vec<_> = (0..8).map(|_| p.alloc_run_page().unwrap()).collect();
        for w in ids.windows(2) {
            p.set_data_chain_next(w[0], w[1]).unwrap();
        }
        let mut cur = ids[0];
        let mut seen = vec![cur];
        loop {
            let next = p.get_page(cur).unwrap().get_next_page();
            if !next.is_valid_next_page() {
                break;
            }
            seen.push(next);
            cur = next;
        }
        assert_eq!(seen, ids);
    }

    #[test]
    fn test_concurrent_pools_users_do_not_corrupt_each_other() {
        let p = Arc::new(pool(4));
        let handles: Vec<_> = (0..4u8)
            .map(|t| {
                let p = p.clone();
                std::thread::spawn(move || {
                    let ids: Vec<_> = (0..20).map(|_| p.alloc_run_page().unwrap()).collect();
                    for id in &ids {
                        put(&p, *id, &[t; 8]);
                    }
                    for id in &ids {
                        assert_eq!(contents(&p, *id), vec![vec![t; 8]]);
                    }
                    p.free_pages(&ids).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(p.stats().live_pages, 0);
    }

    #[test]
    fn test_works_over_a_real_file() {
        let dir = std::env::temp_dir().join(format!("temppool_real_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let name = dir.join("real.db").to_string_lossy().into_owned();
        let base = std::fs::File::create(&name).unwrap();
        let p = TempPool::new(&name, base, PS, 2 * PS).unwrap();
        let ids: Vec<_> = (0..8).map(|_| p.alloc_run_page().unwrap()).collect();
        for (i, id) in ids.iter().enumerate() {
            put(&p, *id, format!("r{i}").as_bytes());
        }
        assert!(std::path::Path::new(p.path()).exists());
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(contents(&p, *id), vec![format!("r{i}").into_bytes()]);
        }
        p.free_pages(&ids).unwrap();
        assert!(!std::path::Path::new(p.path()).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---- Eviction-survival tests, ported from buffer.rs (they used to run
    // against the core PageBuffer when runs lived there). ----

    use crate::{cursor::Cursor, run::Run};

    // Isolation test for a squeal-sql-level bug (HashedSource's
    // bug_repro_rehash_past_272384_rows_loses_entries, in squeal-sql's
    // source/hash.rs): building a large hash table loses entries, and
    // both observed failure boundaries were exact multiples of 1024 —
    // suspicious, since `cache_pages` (this buffer's resident-page cap
    // before eviction) is hardcoded to 1024 in Db::create_core_db. This
    // forces the same "page count exceeds cache_pages" condition at a
    // tiny, fast scale (4 resident slots, 50 pages) to check whether
    // eviction itself is where a write goes missing.
    #[test]
    fn test_run_survives_eviction_when_page_count_exceeds_cache_pages() {
        use crate::cursor::Cursor;
        let buf = make_pool(20 as u64);
        let mut run = Run::create_slotted(Arc::new(buf)).unwrap();
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
    // run. Twice the page count competing for the same cache_pages
    // resident slots means twice the eviction pressure, and read+write
    // interleaving that the single-run test never exercises.
    #[test]
    fn test_run_to_run_copy_survives_eviction_when_both_runs_share_a_small_buffer() {
        use crate::cursor::Cursor;
        let buf = make_pool(20 as u64);
        let buf = Arc::new(buf);
        let mut run_a = Run::create_slotted(buf.clone()).unwrap();
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
        let mut run_b = Run::create_slotted(buf).unwrap();
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
        let buf = make_pool(20 as u64);
        let buf = Arc::new(buf);

        let mut run = Run::create_slotted(buf.clone()).unwrap();
        let mut page_count = 1usize;
        run.set_slot_at(0, 0, &0u64.to_le_bytes()).unwrap();

        for _gen in 0..6 {
            let next_page_count = page_count * 2;
            let mut cursor = run.cursor().unwrap();
            let mut next_run = Run::create_slotted(buf.clone()).unwrap();
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
        // parameters (HashedSource's rehash chain, cache_pages=128 —
        // "fails after the 8th rehash every time" — vs. cache_pages=4096,
        // where the same chain "works every time"). 1, not 0: reserves
        // PageId(0) the way a real Db does (see the other chained test's
        // own comment on why).
        let buf = make_pool(128 as u64);
        let buf = Arc::new(buf);

        let mut run = Run::create_slotted(buf.clone()).unwrap();
        let mut page_count = 1usize;
        run.set_slot_at(0, 0, &0u64.to_le_bytes()).unwrap();
        let mut kept = vec![];

        for _gen in 0..12 {
            let next_page_count = page_count * 2;
            let mut cursor = run.cursor().unwrap();
            let mut next_run = Run::create_slotted(buf.clone()).unwrap();
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
    // parameters (cache_pages=128, "fails after the 8th rehash").
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
        buf: Arc<TempPool<MemFile>>,
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
        fn new(buf: Arc<TempPool<MemFile>>, records_per_page: usize) -> Self {
            let run = Run::create_slotted(buf.clone()).unwrap();
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
            let mut new_run = Run::create_slotted(self.buf.clone()).unwrap();
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
    // (13-18 of 50000 keys missing) at cache_pages=128; passes reliably now.
    #[test]
    fn test_mini_hash_table_survives_repeated_rehash_under_eviction_pressure() {
        // Matches the real repro's confirmed parameters: works reliably
        // at cache_pages=4096, failed after the 8th rehash at
        // cache_pages=128 (records_per_page=133 there; 8 here, since
        // this test's own record size is fixed/tiny — the page COUNT
        // trajectory across rehashes is what needs to match, not the
        // exact records_per_page value).
        const MAX_ENTRIES: usize = 128;
        const RECORDS_PER_PAGE: usize = 8;
        const TOTAL_KEYS: u64 = 50_000; // comfortably past rehash 8 (2048+ capacity)

        let buf = make_pool(MAX_ENTRIES as u64);
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

}
