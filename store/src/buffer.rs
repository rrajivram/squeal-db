use std::{
    collections::HashMap,
    io::SeekFrom,
    sync::{
        Arc, RwLock,
        atomic::AtomicU64,
        mpsc::{self, Receiver, SendError, Sender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use log::info;
use postcard::to_allocvec;
use priority_queue::PriorityQueue;

use crate::{
    constant::timestamp,
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::Logger,
    page::Page,
};

#[derive(Debug, Clone)]
enum BufMsg {
    WritePage(WriteMsg),
    WriteHeader(Header),
    Shutdowm,
}

#[derive(Debug, Default, Clone)]
struct WriteMsg {
    page_num: DBSizeType,
    page: Page,
}

#[derive(Debug)]
pub(crate) struct PageBuffer<F: DBFile + 'static> {
    buffer: RwLock<HashMap<DBSizeType, Arc<Page>>>,
    page_counter: Arc<AtomicU64>,
    page_size: DBSizeType,
    max_entries: usize,
    write_tx: Sender<BufMsg>,
    write_handle: JoinHandle<Result<(), StoreError>>,
    read_file: RwLock<F>,
    access_map: RwLock<PriorityQueue<DBSizeType, u128>>,
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
        let (write_tx, write_rx) = mpsc::channel();
        let write_handle = thread::spawn(move || writer(write_file, header.clone(), write_rx));
        Ok(Self {
            page_size,
            max_entries,
            buffer: RwLock::new(HashMap::new()),
            write_tx,
            read_file: RwLock::new(read_file),
            write_handle,
            access_map: RwLock::new(PriorityQueue::new()),
            page_counter,
        })
    }

    pub(crate) fn write_header(&self, header: Header) -> Result<(), StoreError> {
        Ok(self.write_tx.send(BufMsg::WriteHeader(header))?)
    }

    pub(crate) fn write_page(
        &self,
        page_num: DBSizeType,
        page: Arc<Page>,
    ) -> Result<(), StoreError> {
        {
            self.update_page_access(page_num)?;
            let (contains, count) = {
                let buffer = self.buffer.read()?;
                (buffer.contains_key(&page_num), buffer.len())
            };
            if contains {
                self.buffer.write()?.insert(page_num, page.clone());
            } else {
                if count == self.max_entries {
                    self.replace_oldest(page_num, &page)?;
                }
            }
        }
        Ok(self.write_tx.send(BufMsg::WritePage(WriteMsg {
            page_num,
            page: (*page).clone(),
        }))?)
    }

    pub(crate) fn get_page(&self, page_num: DBSizeType) -> Result<Arc<Page>, StoreError> {
        let valid = page_num < self.page_counter.load(std::sync::atomic::Ordering::Relaxed);
        if !valid {
            return Err(StoreError::UnknownError(format!(
                "Invalid page number : {}",
                page_num
            )));
        }
        if let Some(page) = self.buffer.read()?.get(&page_num) {
            self.update_page_access(page_num)?;
            return Ok(page.clone());
        }
        let mut bytes = vec![0u8; self.page_size as usize];
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

    fn replace_oldest(&self, page_num: DBSizeType, page: &Arc<Page>) -> Result<(), StoreError> {
        if let Some(last_used_page) = { self.access_map.write()?.pop() } {
            let mut buffer = self.buffer.write()?;
            buffer.remove(&last_used_page.0);
            buffer.insert(page_num, page.clone());
        } else {
            panic!("Could not find any pages in access mao!");
        }
        Ok(())
    }

    fn update_page_access(&self, page_num: DBSizeType) -> Result<(), StoreError> {
        let mut pq = self.access_map.write()?;
        if !pq.contains(&page_num) {
            pq.push(page_num, timestamp());
        } else {
            pq.change_priority(&page_num, timestamp());
        }
        Ok(())
    }
}

impl From<SendError<BufMsg>> for StoreError {
    fn from(value: SendError<BufMsg>) -> Self {
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
                mpsc::TryRecvError::Disconnected => {
                    panic!("Writer disconnected");
                }
                mpsc::TryRecvError::Empty => {}
            }
        } else if msg.is_ok() {
            let msg = msg.unwrap();
            match msg {
                BufMsg::Shutdowm => {
                    break;
                }
                BufMsg::WritePage(msg) => {
                    if let Some(lsn) = msg.page.lsn_id() {
                        if msg.page.is_pinned() || lsn < Logger::last_lsn() {
                            seek_to_page(
                                msg.page_num,
                                &mut file,
                                header.page_size,
                                header.first_page_offset,
                            )?;
                            file.write(&msg.page.to_bytes())?;
                        } else {
                            info!(
                                "Waiting for lsn : page lsn: {:?}, last_lsn: {:?}",
                                msg.page.lsn_id(),
                                lsn
                            );
                            pending.push(msg);
                        }
                    } else {
                        return Err(StoreError::UnknownError("Page does not have LSN".into()));
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
                if let Some(_) = m.page.lsn_id().filter(|m| *m < Logger::last_lsn()) {
                    seek_to_page(
                        m.page_num,
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
mod tests {}
