use crate::error::StoreError;
use log::LevelFilter;
use log::info;
use postcard::from_bytes;
use postcard::to_allocvec;
use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::atomic::AtomicU64;

const RDB_MAGIC: u16 = 0x5365;
const MAGIC: [u8; 2] = [0x53, 0x65];
const ZERO_PAGE_SIZE: DBSizeType = 8 * 1024;
const DEFAULT_PAGE_SIZE: DBSizeType = 16 * 1024;

trait DBFile: std::io::Write + std::io::Read + std::io::Seek {}
pub(crate) type DBSizeType = u64;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum TableType {
    Table,
    Index,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Header {
    magic: [u8; 2],
    #[serde(with = "postcard::fixint::le")]
    first_page_offset: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_count: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_size: DBSizeType,
}

#[derive(Debug)]
pub struct Db {
    name: String,
    header: Header,
    file: File,
    wal_file: File,
    page_count: AtomicU64,
}

impl Db {
    pub fn create<S: AsRef<str>>(name: S) -> Result<Self, StoreError> {
        Ok(Self::create_with_page_size(name, DEFAULT_PAGE_SIZE)?)
    }

    pub fn create_with_page_size<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
    ) -> Result<Self, StoreError> {
        let wal_name = name.as_ref().to_string() + ".wal";
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(name.as_ref())?;
        let wf = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(wal_name)?;
        let header = Header {
            magic: MAGIC,
            first_page_offset: ZERO_PAGE_SIZE,
            page_count: 0,
            page_size,
        };
        let bytes = to_allocvec(&header)?;
        f.write(&bytes)?;

        Ok(Self {
            name: name.as_ref().to_string(),
            header: header,
            file: f,
            wal_file: wf,
            page_count: AtomicU64::new(0),
        })
    }

    pub fn open<S: AsRef<str>>(name: S) -> Result<Self, StoreError> {
        let wal_name = name.as_ref().to_string() + ".wal";
        let mut f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .open(name.as_ref())?;
        let wf = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(wal_name)?;
        let mut bytes = vec![0u8; size_of::<Header>()];
        f.read_exact(&mut bytes)?;
        let header: Header = from_bytes(&bytes)?;
        if header.magic != MAGIC {
            return Err(StoreError::FileError);
        }
        Ok(Self {
            page_count: AtomicU64::new(header.page_count),
            header: header,
            file: f,
            wal_file: wf,
            name: name.as_ref().to_string(),
        })
    }

    pub fn close(mut self) -> Result<(), StoreError> {
        self.closeup()?;
        self.wal_file.sync_data()?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn page_count(&self) -> DBSizeType {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn closeup(&mut self) -> Result<(), StoreError> {
        self.write_header()?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<(), StoreError> {
        let n = self.file.seek(SeekFrom::Start(0))?;
        if n != 0 {
            return Err(StoreError::FileError);
        }
        let mut header = self.header.clone();
        header.page_count = self.page_count.load(std::sync::atomic::Ordering::Relaxed);
        let bytes = to_allocvec(&header)?;
        self.file.write(&bytes)?;
        if bytes.len() < size_of::<Header>() {
            let b = vec![0u8; size_of::<Header>() - bytes.len()];
            self.file.write(&b)?;
        }

        Ok(())
    }

    pub fn delete<S: AsRef<str>>(name: S) -> Result<(), StoreError> {
        let wal_file = name.as_ref().to_string() + ".wal";
        remove_file(name.as_ref())?;
        remove_file(wal_file)?;
        Ok(())
    }

    fn alloc_page(&self) -> Result<DBSizeType, StoreError> {
        let next_page = self
            .page_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.init_page(next_page)?;
        Ok(next_page)
    }

    fn init_page(&self, page_num: DBSizeType) -> Result<(), StoreError> {
        let offset = self.header.first_page_offset + page_num * self.header.page_size;
        let bytes = vec![0u8; self.header.page_size as usize];
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        file.write(&bytes)?;
        Ok(())
    }
}

fn init_logger() {
    let res = env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                //"{}:{} {} [{}] - {}",
                "{}:{}:{:?} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                std::thread::current().id(),
                //chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .is_test(true)
        .filter(None, LevelFilter::Debug)
        .try_init();
    if res.is_ok() {
        info!("Logging enabled.")
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{DEFAULT_PAGE_SIZE, Db, ZERO_PAGE_SIZE};

    #[test]
    fn test_create() {
        Db::delete("test.db").unwrap_or_default();
        let db = Db::create("test.db");
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.first_page_offset, ZERO_PAGE_SIZE);
        assert_eq!(db.header.page_count, 0);
        db.close().unwrap();
        let db = Db::open("test.db");
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.page_count, 0);
        assert_eq!(db.header.page_size, DEFAULT_PAGE_SIZE);
        Db::delete("test.db").unwrap_or_default();
    }

    #[test]
    fn test_simple_alloc_page() {
        Db::delete("test.db").unwrap_or_default();
        let db = Db::create("test.db").unwrap();
        let page = db.alloc_page();
        assert!(page.is_ok());
        let page = page.unwrap();
        assert_eq!(page, 0);
        let m = db.file.metadata().unwrap();
        assert_eq!(m.len(), DEFAULT_PAGE_SIZE + ZERO_PAGE_SIZE);
        let page = db.alloc_page().unwrap_or(0);
        assert_eq!(page, 1);
        let m = db.file.metadata().unwrap();
        assert_eq!(m.len(), ZERO_PAGE_SIZE + 2 * DEFAULT_PAGE_SIZE);
        assert_eq!(db.page_count(), 2);
        db.close().unwrap();
        let db = Db::open("test.db").unwrap();
        assert_eq!(db.page_count(), 2);
        Db::delete("test.db").unwrap_or_default();
    }
}
