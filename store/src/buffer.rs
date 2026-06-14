use std::{
    collections::HashMap,
    io::SeekFrom,
    marker::PhantomData,
    sync::{
        Arc, RwLock,
        mpsc::{self, Receiver, SendError, Sender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use log::info;

use crate::{
    db::{DBFile, DBSizeType, Header},
    error::StoreError,
    logger::Logger,
    page::Page,
};

#[derive(Debug, Clone)]
enum BufMsg {
    Write(WriteMsg),
    Shutdowm,
}

#[derive(Debug, Default, Clone)]
struct WriteMsg {
    page_num: DBSizeType,
    page: Page,
}

#[derive(Debug)]
pub(crate) struct PageBuffer<F: DBFile> {
    buffer: RwLock<HashMap<DBSizeType, Page>>,
    page_size: DBSizeType,
    max_entries: usize,
    write_tx: Sender<BufMsg>,
    write_handle: JoinHandle<Result<(), StoreError>>,
    read_file: RwLock<F>,
}

impl<F: DBFile> PageBuffer<F>
where
    F: DBFile<Item = F> + 'static,
{
    pub(crate) fn new(
        page_size: DBSizeType,
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
        })
    }
}

impl From<SendError<WriteMsg>> for StoreError {
    fn from(value: SendError<WriteMsg>) -> Self {
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
                BufMsg::Write(msg) => {
                    if let Some(lsn) = msg.page.lsn_id() {
                        if lsn < Logger::last_lsn() {
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
