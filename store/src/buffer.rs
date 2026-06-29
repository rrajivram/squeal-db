use std::{
    collections::HashMap,
    io::SeekFrom,
    ops::Rem,
    sync::{Arc, RwLock, Weak, atomic::AtomicU64, atomic::AtomicUsize},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::{error, info};
use postcard::to_allocvec;

use crate::{
    arclock::{ArcLock, ArcLockGuard},
    constant::timestamp,
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::Logger,
    page::{Page, PageId},
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
    read_file: RwLock<F>,
    access_map: ShardedPQ<PageId, u128>,
    locks: ArcLock<PageId>,
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
        let write_file = db_file.do_clone()?;
        let (write_tx, write_rx) = unbounded();
        let w_header = header.clone();
        let write_handle = thread::spawn(move || writer(write_file, w_header, write_rx));
        Ok(Self {
            page_size,
            max_entries,
            strong_count: AtomicUsize::new(0),
            buffer: RwLock::new(HashMap::new()),
            write_tx,
            read_file: RwLock::new(read_file),
            write_handle: Some(write_handle),
            access_map: ShardedPQ::new(max_entries / 10),
            page_count: page_counter,
            header,
            locks: ArcLock::new(),
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

    pub(crate) fn write_header(&self, header: Header) -> Result<(), StoreError> {
        Ok(self.write_tx.send(BufMsg::WriteHeader(header))?)
    }

    pub(crate) fn write_page(&self, page_num: PageId, page: &Page) -> Result<(), StoreError> {
        let page = Arc::new(page.clone());
        page.written();
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
        let WritePageHandle { page_num, page, lock: _lock } = handle;
        page.written();
        self.cache_strong(page_num, page.clone())?;
        // _lock drops here, releasing the per-page lock after cache is updated.
        Ok(self.write_tx.send(BufMsg::WritePage(WriteMsg { page_num, page }))?)
    }

    pub(crate) fn alloc_page(&self, should_pin: bool) -> Result<PageId, StoreError> {
        let next_page = self
            .page_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let pg: PageId = next_page.into();
        self.init_page(pg.clone(), should_pin)?;
        Ok(next_page.into())
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
        let existing = self.buffer.read()?.get(&page_num).cloned();
        match existing {
            Some(PageEntry::Strong(arc)) => {
                // Pure LRU timestamp refresh — page is already resident and
                // counted; no eviction or count change needed here.
                self.update_page_access(page_num)?;
                return Ok(arc);
            }
            Some(PageEntry::Weak(weak)) => {
                if let Some(arc) = weak.upgrade() {
                    // Still alive — either the writer hasn't flushed the
                    // write that evicted it yet, or another reader is
                    // holding it. Either way, reuse it directly.
                    // cache_strong updates access_map atomically with the
                    // buffer insert, avoiding the race in the standalone
                    // update_page_access + cache_strong two-step.
                    self.cache_strong(page_num, arc.clone())?;
                    return Ok(arc);
                }
                // Dead: the writer already dropped its copy, which only
                // happens after the file write completed, so the backing
                // file is now guaranteed current. Prune the stale tombstone
                // while we're here rather than leaving it around forever.
                self.buffer.write()?.remove(&page_num);
            }
            None => {}
        }
        let mut bytes = vec![0u8; self.page_size as usize];
        let offset = self.header.first_page_offset + self.header.page_size * u64::from(page_num);
        self.read_file.write()?.seek(SeekFrom::Start(offset))?;
        self.read_file.write()?.read(&mut bytes)?;
        let page = Arc::new(Page::from_bytes(&bytes)?);
        // cache_strong handles both the access_map update and the buffer
        // insert under one write lock — the old two-step was racy.
        self.cache_strong(page_num, page.clone())?;
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
        let mut buffer = self.buffer.write()?;
        let already_strong = matches!(buffer.get(&page_num), Some(PageEntry::Strong(_)));
        if !already_strong {
            if self.strong_count.load(std::sync::atomic::Ordering::Relaxed) >= self.max_entries {
                self.evict_lru_locked(&mut buffer);
            }
            self.strong_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Update LRU position under the same write lock so evict_lru_locked
        // always sees a consistent (buffer entry, access_map entry) pair.
        let priority = u128::MAX - timestamp();
        if !self.access_map.contains(&page_num) {
            self.access_map.push(page_num, priority);
        } else {
            self.access_map.change_priority(&page_num, priority);
        }
        buffer.insert(page_num, PageEntry::Strong(page));
        Ok(())
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
    let mut pending = vec![];
    loop {
        let msg = recv.try_recv();
        if msg.is_err() {
            match msg.err().unwrap() {
                crossbeam::channel::TryRecvError::Disconnected => {
                    // The sending PageBuffer was dropped without an explicit
                    // shutdown() (flagged separately by PageBuffer's Drop
                    // impl). Either way, there's no one left to send us
                    // anything — exit the same way an explicit Shutdowm does.
                    info!("Writer exiting: channel disconnected");
                    break;
                }
                crossbeam::channel::TryRecvError::Empty => {
                    // Retry pages that were deferred because the redo log
                    // hadn't caught up yet. The else-branch below is
                    // unreachable (try_recv only returns Ok or Err), so this
                    // is the only place the pending queue can drain.
                    for i in (0..pending.len()).rev() {
                        let m: &WriteMsg = &pending[i];
                        if m.page.is_pinned() || m.page.lsn_id()? < Logger::last_lsn() {
                            seek_to_page(
                                m.page_num.into(),
                                &mut file,
                                header.page_size,
                                header.first_page_offset,
                            )?;
                            file.write(&m.page.to_bytes())?;
                            pending.swap_remove(i);
                        }
                    }
                    // Nothing arrived this pass — only now is it safe to idle.
                    // Sleeping unconditionally on every iteration (the old
                    // behavior) capped this thread at ~1000 messages/sec no
                    // matter how fast producers sent them: under load the
                    // channel backs up for the entire run, and a later
                    // Shutdowm message has to wait behind that whole backlog,
                    // making PageBuffer::shutdown() (and thus Db::close())
                    // hang for minutes. Only sleep when idle so a backlog
                    // drains as fast as the disk/memory can take it.
                    thread::sleep(Duration::from_millis(1));
                }
            }
        } else if msg.is_ok() {
            let msg = msg.unwrap();
            match msg {
                BufMsg::Shutdowm => {
                    // Flush any pages still waiting before exit — the redo log
                    // has already been written for all committed operations, so
                    // it's safe to write everything unconditionally here.
                    for m in pending.drain(..) {
                        seek_to_page(
                            m.page_num.into(),
                            &mut file,
                            header.page_size,
                            header.first_page_offset,
                        )?;
                        file.write(&m.page.to_bytes())?;
                    }
                    break;
                }
                BufMsg::WritePage(msg) => {
                    if msg.page.is_pinned() || msg.page.lsn_id()? < Logger::last_lsn() {
                        seek_to_page(
                            msg.page_num.into(),
                            &mut file,
                            header.page_size,
                            header.first_page_offset,
                        )?;
                        file.write(&msg.page.to_bytes())?;
                    } else {
                        info!(
                            "Waiting for lsn : page lsn: {:?}, last_lsn: {:?}",
                            msg.page.lsn_id(),
                            Logger::last_lsn()
                        );
                        pending.push(msg);
                    }
                }
                BufMsg::WriteHeader(header) => {
                    file.seek(SeekFrom::Start(0))?;
                    let bytes = to_allocvec(&header)?;
                    file.write(&to_allocvec(&header)?)?;
                    if bytes.len() < size_of::<Header>() {
                        let b = vec![0u8; size_of::<Header>() - bytes.len()];
                        file.write(&b)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn seek_to_page(
    page: DBSizeType,
    file: &mut impl DBFile,
    page_size: DBSizeType,
    first_offset: DBSizeType,
) -> Result<(), StoreError> {
    let pos = first_offset + page * page_size;
    file.seek(SeekFrom::Start(pos))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::{Arc, atomic::AtomicU64};

    use postcard::from_bytes;

    use crate::page::Page;
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
