use std::{
    collections::HashMap,
    io::SeekFrom,
    sync::{Arc, RwLock, atomic::AtomicU64},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::{error, info};
use postcard::to_allocvec;
use priority_queue::PriorityQueue;

use crate::{
    arclock::{ArcLock, ArcLockGuard},
    constant::timestamp,
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::Logger,
    page::{Page, PageId},
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
    page: Page,
}

#[derive(Debug, Clone)]
pub(crate) struct WritePageHandle {
    pub(crate) page_num: PageId,
    lock: ArcLockGuard<PageId>,
    pub(crate) page: Arc<Page>,
}

#[derive(Debug)]
pub(crate) struct PageBuffer<F: DBFile + 'static> {
    buffer: RwLock<HashMap<PageId, Arc<Page>>>,
    header: Arc<Header>,
    page_size: DBSizeType,
    page_count: Arc<AtomicU64>,
    max_entries: usize,
    write_tx: Sender<BufMsg>,
    // None once shutdown() has taken it to join the thread — Drop uses that
    // to tell "shut down properly" apart from "dropped without shutdown".
    write_handle: Option<JoinHandle<Result<(), StoreError>>>,
    read_file: RwLock<F>,
    access_map: RwLock<PriorityQueue<PageId, u128>>,
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
            buffer: RwLock::new(HashMap::new()),
            write_tx,
            read_file: RwLock::new(read_file),
            write_handle: Some(write_handle),
            access_map: RwLock::new(PriorityQueue::new()),
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
        let page_to_write = page.clone();
        let page = Arc::new(page.clone());
        self.update_page_access(page_num)?;
        let (contains, count) = {
            let buffer = self.buffer.read()?;
            (buffer.contains_key(&page_num), buffer.len())
        };
        if contains || count < self.max_entries {
            self.buffer.write()?.insert(page_num, page.clone());
        } else {
            self.replace_oldest(page_num, &page)?;
        }
        Ok(self.write_tx.send(BufMsg::WritePage(WriteMsg {
            page_num,
            page: page_to_write,
        }))?)
    }

    pub(crate) fn write_locked_page(&self, handle: WritePageHandle) -> Result<(), StoreError> {
        Ok(self.write_page(handle.page_num, handle.page.as_ref())?)
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
        if let Some(page) = self.buffer.read()?.get(&page_num) {
            self.update_page_access(page_num)?;
            return Ok(page.clone());
        }
        let mut bytes = vec![0u8; self.page_size as usize];
        let offset = self.header.first_page_offset + self.header.page_size * u64::from(page_num);
        self.read_file.write()?.seek(SeekFrom::Start(offset))?;
        self.read_file.write()?.read(&mut bytes)?;
        let page = Arc::new(Page::from_bytes(&bytes)?);
        let count = { self.buffer.read()?.len() };
        if count == self.max_entries {
            self.replace_oldest(page_num, &page)?;
        } else {
            self.buffer.write()?.insert(page_num, page.clone());
        }
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
        let handle = WritePageHandle {
            lock,
            page_num,
            page,
        };
        Ok(handle)
    }

    fn replace_oldest(&self, page_num: PageId, page: &Arc<Page>) -> Result<(), StoreError> {
        if let Some(last_used_page) = { self.access_map.write()?.pop() } {
            let mut buffer = self.buffer.write()?;
            buffer.remove(&last_used_page.0);
            buffer.insert(page_num, page.clone());
        } else {
            panic!("Could not find any pages in access mao!");
        }
        Ok(())
    }

    fn update_page_access(&self, page_num: PageId) -> Result<(), StoreError> {
        let mut pq = self.access_map.write()?;
        if !pq.contains(&page_num) {
            pq.push(page_num, timestamp());
        } else {
            pq.change_priority(&page_num, timestamp());
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
                crossbeam::channel::TryRecvError::Empty => {}
            }
        } else if msg.is_ok() {
            let msg = msg.unwrap();
            match msg {
                BufMsg::Shutdowm => {
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
        } else {
            for i in (0..pending.len()).rev() {
                let m = &pending[i];
                if m.page.lsn_id()? < Logger::last_lsn() {
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
        }
        thread::sleep(Duration::from_millis(1));
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
        let r = buf.get_page(0.into());
        assert!(r.is_err());
        buf.shutdown().unwrap();
    }

    #[test]
    fn test_get_page_reads_from_file() {
        let (buf, _) = make_buffer(1, 10);
        // page 0 is valid (page_counter = 1) and its bytes are at offset 0 in MemFile
        let r = buf.get_page(0.into());
        assert!(r.is_ok());
        let _ = buf.shutdown();
    }

    #[test]
    fn test_get_page_cached_after_first_read() {
        let (buf, _) = make_buffer(1, 10);
        let p1 = buf.get_page(0.into()).unwrap();
        let p2 = buf.get_page(0.into()).unwrap();
        assert_eq!(*p1, *p2);
        let _ = buf.shutdown();
    }

    #[test]
    fn test_write_page_updates_in_memory_cache() {
        let (buf, _) = make_buffer(1, 10);
        // Populate cache with the initial (non-pinned) page
        let p = buf.get_page(0.into()).unwrap();
        assert!(!p.is_pinned());
        // Write a pinned page into the cache slot for page 0
        let new_page = Page::new_pinned(PAGE_SIZE);
        assert!(buf.write_page(0.into(), &new_page).is_ok());
        // Cache must now hold the updated page
        let p2 = buf.get_page(0.into()).unwrap();
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
