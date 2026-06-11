use crate::constant::MAX_TABLE_NAME_LEN;
use crate::constant::SYSTEM_TABLE_NAME;
use crate::constant::SYSTEM_TABLE_PAGE;
use crate::error::StoreError;
use crate::page::Page;
use crate::table::Table;
use crate::tuple::Tuple;
use log::LevelFilter;
use log::info;
use postcard::from_bytes;
use postcard::to_allocvec;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;

const RDB_MAGIC: u16 = 0x5365;
const MAGIC: [u8; 2] = [0x53, 0x65];
const ZERO_PAGE_SIZE: DBSizeType = 8 * 1024;
const DEFAULT_PAGE_SIZE: DBSizeType = 16 * 1024;

trait DBFile: std::io::Write + std::io::Read + std::io::Seek {}
pub(crate) type DBSizeType = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TableType {
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
    tables: Arc<RwLock<HashMap<String, Table>>>,
}

impl Db {
    pub fn create<S: AsRef<str>>(name: S) -> Result<Self, StoreError> {
        Ok(Self::create_with_page_size(name, DEFAULT_PAGE_SIZE)?)
    }

    pub fn create_with_page_size<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
    ) -> Result<Self, StoreError> {
        let sf = Self::create_core_db(name.as_ref().to_string(), page_size)?;
        sf.create_system_tables()?;
        Ok(sf)
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
        let sf = Self {
            page_count: AtomicU64::new(header.page_count),
            header: header,
            file: f,
            wal_file: wf,
            name: name.as_ref().to_string(),
            tables: Arc::new(RwLock::new(HashMap::new())),
        };
        sf.load_system_tables()?;
        Ok(sf)
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

    pub fn create_table(&self, name: String) -> Result<Table, StoreError> {
        let table = {
            self.validate_table_name(&name)?;
            let mut tables = self
                .tables
                .write()
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            let table_page = self.alloc_page(false)?;
            let table = Table::new(name, TableType::Table, table_page)?;
            tables.insert(table.name.clone(), table.clone());
            table
        };
        self.write_system_table()?;
        Ok(table)
    }

    fn create_system_table(&self, name: String) -> Result<Table, StoreError> {
        let table_page = self.alloc_page(true)?;
        let table = Table::new(name, TableType::Table, table_page)?;
        Ok(table)
    }

    fn validate_table_name(&self, name: &String) -> Result<(), StoreError> {
        if name.len() > MAX_TABLE_NAME_LEN {
            return Err(StoreError::TableNameInvalid(MAX_TABLE_NAME_LEN, name.len()));
        }
        let tables = self
            .tables
            .read()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        if tables.contains_key(name) {
            return Err(StoreError::DuplicateTableName(name.to_string()));
        }
        Ok(())
    }

    fn write_system_table(&self) -> Result<(), StoreError> {
        let mut page = Page::new(self.header.page_size);
        let tables = self
            .tables
            .read()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        for (i, t) in tables.values().enumerate() {
            let bytes = to_allocvec(&t)?;
            // We dont care what the tables id is or if it is consistent across saves.
            page.add_tuple(Tuple::new(i as DBSizeType, &bytes))?;
        }
        let bytes = page.to_bytes();
        self.write_page(0, &bytes)?;
        Ok(())
    }

    fn load_system_tables(&self) -> Result<(), StoreError> {
        if self.header.page_count > 0 {
            let v = self.read_page(SYSTEM_TABLE_PAGE)?;
            let p = Page::from_bytes(&v)?;
            let mut tables = self
                .tables
                .write()
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            for t in p.iter() {
                let t: Table = from_bytes(&t.data)?;
                tables.insert(t.name.clone(), t);
            }
        }
        Ok(())
    }

    fn get_tables(&self) -> Result<Vec<Table>, StoreError> {
        Ok(self
            .tables
            .read()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?
            .values()
            .map(|t| t.clone())
            .collect::<Vec<_>>())
    }

    fn create_system_tables(&self) -> Result<(), StoreError> {
        self.create_system_table(SYSTEM_TABLE_NAME.to_string())?;

        Ok(())
    }

    fn create_core_db(name: String, page_size: DBSizeType) -> Result<Self, StoreError> {
        let wal_name = name.clone() + ".wal";
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&name)?;
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
            name: name,
            header: header,
            file: f,
            wal_file: wf,
            page_count: AtomicU64::new(0),
            tables: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn closeup(&mut self) -> Result<(), StoreError> {
        self.write_system_table()?;
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

    fn alloc_page(&self, should_pin: bool) -> Result<DBSizeType, StoreError> {
        let next_page = self
            .page_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.init_page(next_page, should_pin)?;
        Ok(next_page)
    }

    fn init_page(&self, page_num: DBSizeType, should_pin: bool) -> Result<(), StoreError> {
        let p = if should_pin {
            Page::new_pinned(self.header.page_size)
        } else {
            Page::new(self.header.page_size)
        };
        let bytes = p.to_bytes();
        self.write_page(page_num, &bytes)?;
        Ok(())
    }

    fn write_page(&self, page_num: DBSizeType, bytes: &[u8]) -> Result<(), StoreError> {
        let offset = self.header.first_page_offset + page_num * self.header.page_size;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        file.write(&bytes)?;
        Ok(())
    }

    fn read_page(&self, page_num: DBSizeType) -> Result<Vec<u8>, StoreError> {
        let offset = self.header.first_page_offset + page_num * self.header.page_size;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        let mut v = vec![0u8; self.header.page_size as usize];
        file.read_exact(&mut v)?;
        Ok(v)
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
    use crate::{
        db::{DEFAULT_PAGE_SIZE, Db, ZERO_PAGE_SIZE},
        error::StoreError,
    };

    #[test]
    fn test_create() {
        const DB_NAME: &str = "test1.db";
        Db::delete(DB_NAME).unwrap_or_default();
        let db = Db::create_core_db(DB_NAME.to_string(), DEFAULT_PAGE_SIZE);
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.first_page_offset, ZERO_PAGE_SIZE);
        assert_eq!(db.header.page_count, 0);
        db.close().unwrap();
        let db = Db::open(DB_NAME);
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.page_count, 0);
        assert_eq!(db.header.page_size, DEFAULT_PAGE_SIZE);
        Db::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_simple_alloc_page() {
        const DB_NAME: &str = "test2.db";
        Db::delete(DB_NAME).unwrap_or_default();
        let db = Db::create_core_db(DB_NAME.to_string(), DEFAULT_PAGE_SIZE).unwrap();
        let page = db.alloc_page(false);
        assert!(page.is_ok());
        let page = page.unwrap();
        assert_eq!(page, 0);
        let m = db.file.metadata().unwrap();
        assert_eq!(m.len(), DEFAULT_PAGE_SIZE + ZERO_PAGE_SIZE);
        let page = db.alloc_page(false).unwrap_or(0);
        assert_eq!(page, 1);
        let m = db.file.metadata().unwrap();
        assert_eq!(m.len(), ZERO_PAGE_SIZE + 2 * DEFAULT_PAGE_SIZE);
        assert_eq!(db.page_count(), 2);
        db.close().unwrap();
        let db = Db::open(DB_NAME).unwrap();
        assert_eq!(db.page_count(), 2);
        Db::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_create_table() {
        const DB_NAME: &str = "test3.db";
        Db::delete(DB_NAME).unwrap_or_default();
        let db = Db::create(DB_NAME.to_string());
        assert!(db.is_ok());
        let db = db.unwrap();
        let r = db.create_table("table_1".to_string());
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r.name, "table_1");
        assert_eq!(db.get_tables().unwrap().len(), 1);
        db.close().unwrap();
        let db = Db::open(DB_NAME.to_string()).unwrap();
        let t = db.get_tables().unwrap();
        assert!(t.len() == 1);
        assert_eq!(t[0].name, "table_1");
        let r = db.create_table("table_1".to_string());
        assert!(matches!(r, Err(StoreError::DuplicateTableName(_))));
        Db::delete(DB_NAME).unwrap_or_default()
    }
}
