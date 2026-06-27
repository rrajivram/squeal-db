#![allow(private_bounds)]
use crate::buffer::PageBuffer;
use crate::constant::FIRST_USER_PAGE;
use crate::constant::GENERATOR_TABLE_PAGE;
use crate::constant::MAX_TABLE_NAME_LEN;
use crate::constant::SYSTEM_TABLE_NAME;
use crate::constant::SYSTEM_TABLE_PAGE;
use crate::error::StoreError;
use crate::generator::Generator;
use crate::logger::Logger;
use crate::page::Page;
use crate::table::Table;
use crate::table::TableIdType;
use crate::tables::bplustree::BPlusTree;
use crate::tuple::DBIdType;
use crate::tuple::Tuple;
use crate::txn::Transaction;
use crate::txn::TransactionId;
use crate::txn::TransactionManager;
use log::LevelFilter;
use log::info;
use parking_lot::RwLock;
use postcard::from_bytes;
use postcard::to_allocvec;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::fs::remove_file;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

const RDB_MAGIC: u16 = 0x5365;
const MAGIC: [u8; 2] = [0x53, 0x65];
const ZERO_PAGE_SIZE: DBSizeType = 8 * 1024;
const DEFAULT_PAGE_SIZE: DBSizeType = 16 * 1024;

pub type FileDB = Db<File>;
pub(crate) struct Meta {
    pub(crate) len: u64,
}

pub(crate) trait Opener {
    type Item;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<Self::Item>;
    fn do_sync(&mut self) -> std::io::Result<()>;
    fn do_clone(&self) -> std::io::Result<Self::Item>;
    fn get_metadata(&self) -> std::io::Result<Meta>;
    fn do_lock(&self) -> Result<(), TryLockError>;
}

pub(crate) trait DBFile:
    std::io::Write + std::io::Read + std::io::Seek + std::marker::Send + Opener
{
}
pub(crate) type DBSizeType = u64;

impl<T> DBFile for T where
    T: std::io::Write + std::io::Read + std::io::Seek + std::marker::Send + Opener
{
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Header {
    magic: [u8; 2],
    #[serde(with = "postcard::fixint::le")]
    pub(crate) first_page_offset: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_count: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) page_size: DBSizeType,
}

pub struct Db<F: DBFile + 'static> {
    name: String,
    pub(crate) header: Arc<Header>,
    file: F,
    pub(crate) undo_file: F,
    pub(crate) redo_file: F,
    page_count: Arc<AtomicU64>,
    tables: Arc<RwLock<HashMap<TableIdType, Arc<BPlusTree<F>>>>>,
    generator: Arc<Generator>,
    logger: Arc<Logger>,
    tx_mgr: Arc<TransactionManager>,
    buffer: Arc<PageBuffer<F>>,
}

struct NeededObjects<F: DBFile + 'static> {
    logger: Arc<Logger>,
    txn_mgr: Arc<TransactionManager>,
    buffer: Arc<PageBuffer<F>>,
}

impl<F: DBFile + 'static> Db<F>
where
    F: DBFile<Item = F>,
{
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

    pub fn open_using<S: AsRef<str>>(
        name: S,
        file: F,
        undo_file: F,
        redo_file: F,
    ) -> Result<Self, StoreError> {
        let mut bytes = vec![0u8; size_of::<Header>()];
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut bytes)?;
        let header = Arc::new(from_bytes::<Header>(&bytes)?);
        if header.magic != MAGIC {
            return Err(StoreError::FileError);
        }
        file.do_lock()?;
        undo_file.do_lock()?;
        redo_file.do_lock()?;
        let gens = Arc::new(Generator::new());
        let page_count = Arc::new(AtomicU64::new(header.page_count));
        let nm = Self::setup_needed_modules(
            header.clone(),
            gens.clone(),
            page_count.clone(),
            file.do_clone()?,
            undo_file.do_clone()?,
            redo_file.do_clone()?,
        )?;
        let sf = Self {
            page_count: page_count,
            header: header,
            file: file,
            undo_file,
            redo_file,
            name: name.as_ref().to_string(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            generator: Arc::new(Generator::new()),
            logger: nm.logger,
            tx_mgr: nm.txn_mgr,
            buffer: nm.buffer,
        };
        sf.load_system_tables()?;
        Ok(sf)
    }

    pub fn open<S: AsRef<str>>(name: S) -> Result<Self, StoreError> {
        let uf_name = name.as_ref().to_string() + ".undo";
        let rf_name = name.as_ref().to_string() + ".redo";
        let f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .clone();
        let f = F::open(f, name.as_ref())?;
        let undo_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .clone();
        let undo_file = F::open(undo_file, uf_name)?;
        let redo_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .clone();
        let redo_file = F::open(redo_file, rf_name)?;
        Self::open_using(name, f, undo_file, redo_file)
    }

    /*
     * Close only return files used so this can be used with MemFile for testing,
     * as MemFile does not survive recreating. Similarly with open_using.
     */
    pub fn close(self) -> Result<(F, F, F), StoreError> {
        self.write_system_tables()?;
        let mut hdr = (*self.header).clone();
        hdr.page_count = self.page_count();
        // Each BPlusTree in tables holds Arc<PageBuffer>, Arc<Logger>, and
        // Arc<TransactionManager>. Drop them before Arc::into_inner so the
        // reference counts reach 1 and into_inner succeeds.
        let Db {
            buffer,
            logger,
            tables,
            file,
            undo_file,
            redo_file,
            ..
        } = self;
        drop(tables);
        let buffer = Arc::into_inner(buffer).unwrap();
        buffer.write_header(hdr)?;
        buffer.shutdown()?;
        // Unwrapping here as the expectation is there is only this thread accessing logger
        let logger = Arc::into_inner(logger).unwrap();
        logger.shutdown()?;
        Ok((file, undo_file, redo_file))
    }

    pub fn page_count(&self) -> DBSizeType {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn begin(&self) -> Result<Transaction, StoreError> {
        Ok(self.tx_mgr.begin()?)
    }

    pub fn commit(&self, txn: Transaction) -> Result<(), StoreError> {
        todo!()
    }

    pub fn rollback(&self, txn: Transaction) -> Result<(), StoreError> {
        todo!()
    }

    pub(crate) fn table_by_id(&self, id: TableIdType) -> Result<Arc<BPlusTree<F>>, StoreError> {
        Ok(self
            .tables
            .read()
            .get(&id)
            .map(|t| Arc::clone(t))
            .ok_or(StoreError::TableNotFound(id.to_string()))?)
    }

    pub(crate) fn insert(
        &self,
        id: TableIdType,
        tuple: Tuple,
        txn: Transaction,
    ) -> Result<(), StoreError> {
        let tx_id = txn.id();
        let mut tuple = tuple;
        tuple.set_txn_id(tx_id.clone());
        self.table_by_id(id)?.insert(tuple, tx_id.clone())?;
        Ok(())
    }

    pub(crate) fn find(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn: Transaction,
    ) -> Result<Option<Tuple>, StoreError> {
        let _txn_id = txn.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            let tuple = self.find_last_committed(&tuple).map(|t| t.into_owned());
            return Ok(tuple);
        } else {
            return Ok(None);
        }
    }

    pub(crate) fn update(
        &self,
        tid: TableIdType,
        new_tuple: Tuple,
        txn_id: Transaction,
    ) -> Result<(), StoreError> {
        let txn = txn_id.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(new_tuple.id.clone())?;
        if let Some(tuple) = tuple {
            let tuple = self
                .find_last_committed(&tuple)
                .ok_or(StoreError::KeyNotFound(new_tuple.id.clone()))?;
            let mut tuple = tuple.into_owned();
            tuple.set_txn_id(txn.clone());
            tuple.set_undo_id(self.logger.next_undo_id(txn.clone())?);
            tuple.set_data(&new_tuple.data);
            table.update(tuple.clone(), txn.clone())?;
            return Ok(());
        } else {
            return Err(StoreError::KeyNotFound(new_tuple.id));
        }
    }

    pub(crate) fn remove(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn_id: Transaction,
    ) -> Result<Tuple, StoreError> {
        let txn = txn_id.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            let tuple = self
                .find_last_committed(&tuple)
                .ok_or(StoreError::KeyNotFound(tuple.id.clone()))?;
            let mut tuple = tuple.into_owned();
            tuple.set_txn_id(txn.clone());
            tuple.tombstone();
            tuple.set_undo_id(self.logger.next_undo_id(txn.clone())?);
            table.update(tuple.clone(), txn.clone())?;
            return Ok(tuple);
        } else {
            return Err(StoreError::KeyNotFound(id));
        }
    }

    fn find_last_committed<'a>(&self, tuple: &'a Tuple) -> Option<Cow<'a, Tuple>> {
        if let Some(txn) = tuple.txn_id.clone() {
            if !self.tx_mgr.is_transaction_active(&txn) {
                Some(Cow::Borrowed(tuple))
            } else {
                let mut tuple = tuple.clone();
                let mut txn = txn;
                loop {
                    if tuple.undo_id.is_none() {
                        return None;
                    }
                    let undo_id = tuple.undo_id.unwrap();

                    let t = self.logger.find_undo_tuple(txn.clone(), undo_id);
                    let next_tuple = t.expect(
                        format!("Could not find undo record for {:?},{:?}", txn, undo_id).as_str(),
                    );
                    let next_txn = next_tuple
                        .txn_id
                        .clone()
                        .expect(format!("Could not find txn id for {}", tuple.id).as_str());
                    if !self.tx_mgr.is_transaction_active(&next_txn) {
                        return Some(Cow::Owned(tuple));
                    }
                    tuple = next_tuple;
                    txn = next_txn;
                }
            }
        } else {
            panic!("Tuple does NOT have txn! {:?}", tuple.id);
        }
    }

    pub fn create_table(&self, name: String) -> Result<TableIdType, StoreError> {
        let table_id = {
            self.validate_table_name(&name)?;
            let mut tables = self.tables.write();
            self.generator.create_generator(&name, None)?;
            //let table_page = self.buffer.alloc_page(false)?;
            let table = BPlusTree::new(
                self.generator.gen_key(SYSTEM_TABLE_NAME)?.into(),
                name.clone(),
                self.buffer.clone(),
                self.tx_mgr.clone(),
                self.logger.clone(),
            )?;
            let id = table.id();
            tables.insert(id, Arc::new(table));
            id
        };
        self.write_system_tables()?;
        Ok(table_id)
    }

    fn setup_needed_modules(
        header: Arc<Header>,
        gens: Arc<Generator>,
        page_counter: Arc<AtomicU64>,
        file: F,
        undo_file: F,
        redo_file: F,
    ) -> Result<NeededObjects<F>, StoreError> {
        let mut logger = Logger::new();
        logger.set_db(undo_file, redo_file)?;
        let nm = NeededObjects {
            buffer: Arc::new(PageBuffer::new(
                header.page_size,
                page_counter,
                file,
                header,
                1024,
            )?),
            logger: Arc::new(logger),
            txn_mgr: Arc::new(TransactionManager::new(gens, TransactionId::default())?),
        };
        Ok(nm)
    }

    fn create_core_db(name: String, page_size: DBSizeType) -> Result<Self, StoreError> {
        let uf_name = name.to_string() + ".undo";
        let rf_name = name.to_string() + ".redo";
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .clone();
        let mut f = F::open(f, &name)?;
        let undo_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .clone();
        let undo_file = F::open(undo_file, uf_name)?;
        let redo_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .clone();
        let redo_file = F::open(redo_file, rf_name)?;
        f.do_lock()?;
        undo_file.do_lock()?;
        redo_file.do_lock()?;
        let header = Header {
            magic: MAGIC,
            first_page_offset: ZERO_PAGE_SIZE,
            page_count: 0,
            page_size,
        };
        let bytes = to_allocvec(&header)?;
        f.write(&bytes)?;
        let header = Arc::new(header);
        let gens = Generator::new();
        gens.create_generator(&SYSTEM_TABLE_NAME.to_owned(), None)?;
        let gens = Arc::new(gens);
        let page_count = Arc::new(AtomicU64::new(0));
        let nm = Self::setup_needed_modules(
            header.clone(),
            gens.clone(),
            page_count.clone(),
            f.do_clone()?,
            undo_file.do_clone()?,
            redo_file.do_clone()?,
        )?;

        Ok(Self {
            name: name,
            header: header,
            file: f,
            undo_file,
            redo_file,
            page_count: page_count,
            tables: Arc::new(RwLock::new(HashMap::new())),
            generator: gens,
            logger: nm.logger,
            tx_mgr: nm.txn_mgr,
            buffer: nm.buffer,
        })
    }

    fn validate_table_name(&self, name: &String) -> Result<(), StoreError> {
        if name.len() > MAX_TABLE_NAME_LEN {
            return Err(StoreError::TableNameInvalid(MAX_TABLE_NAME_LEN, name.len()));
        }
        let tables = self.tables.read();
        let present = tables
            .values()
            .map(|t| &t.table.name)
            .position(|n| n == name);
        if present.is_some() {
            return Err(StoreError::DuplicateName(name.to_string()));
        }
        Ok(())
    }

    fn write_system_tables(&self) -> Result<(), StoreError> {
        let mut page = Page::new_pinned(self.header.page_size);
        let tables = self.tables.read();
        for (i, t) in tables.values().enumerate() {
            let bytes = to_allocvec(&t.table)?;
            // We dont care what the tables id is or if it is consistent across saves.
            page.add_tuple(Tuple::new(i as DBSizeType, &bytes))?;
        }
        self.buffer.write_page(0.into(), &page)?;
        let gens = self.generator.get_values()?;
        let mut page = Page::new_pinned(self.header.page_size);
        page.add_tuple(Tuple::new(0, &to_allocvec(&gens)?))?;
        self.buffer.write_page(1.into(), &page)?;
        // TODO : Handle page overflows correctly
        // TODO: Handle empty pages
        Ok(())
    }

    fn create_system_tables(&self) -> Result<(), StoreError> {
        let t = self.buffer.alloc_page(true)?; // system
        assert!(t == 0.into());
        let t = self.buffer.alloc_page(true)?; // generators
        assert!(t == 1.into());
        let t = self.buffer.alloc_page(true)?; // free pages
        assert!(t == 2.into());
        assert!(self.page_count() == 3);
        Ok(())
    }

    fn load_system_tables(&self) -> Result<(), StoreError> {
        if self.page_count() < FIRST_USER_PAGE {
            return Err(StoreError::UnknownError(
                "Unable to load system tables".into(),
            ));
        }
        let page = self.buffer.get_page(SYSTEM_TABLE_PAGE.into())?;
        let mut tables = self.tables.write();
        for t in page.iter() {
            let t: BPlusTree<F> = BPlusTree::from_bytes(
                &t.data,
                self.buffer.clone(),
                self.tx_mgr.clone(),
                self.logger.clone(),
            )?;
            tables.insert(t.table.id, Arc::new(t));
        }
        let page = self.buffer.get_page(GENERATOR_TABLE_PAGE.into())?;
        let tuple = page.get(DBIdType::Int(0))?.unwrap_or_default();
        let gens = from_bytes(&tuple.data)?;
        self.generator.set_values(gens)?;
        Ok(())
    }

    fn get_tables(&self) -> Result<Vec<Table>, StoreError> {
        Ok(self
            .tables
            .read()
            .values()
            .map(|t| t.table.clone())
            .collect::<Vec<_>>())
    }

    pub fn delete<S: AsRef<str>>(name: S) -> Result<(), StoreError> {
        let uf_name = name.as_ref().to_string() + ".undo";
        let rf_name = name.as_ref().to_string() + ".redo";
        remove_file(name.as_ref())?;
        remove_file(uf_name)?;
        remove_file(rf_name)?;
        Ok(())
    }
}

pub(crate) fn db_hash(bytes: &[u8]) -> u64 {
    let mut h = 0x811C9DC5;
    for b in bytes {
        h = h ^ *b as u64;
        h = (h * 0x01000193) & 0xFFFFFFFF;
    }
    h
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
    use std::{thread, time::Duration};

    use crate::{
        db::{DEFAULT_PAGE_SIZE, Db, Opener, ZERO_PAGE_SIZE},
        error::StoreError,
        memfile::MemFile,
    };
    type TestDB = Db<MemFile>;

    #[test]
    fn test_create() {
        const DB_NAME: &str = "test1.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME.to_string());
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.first_page_offset, ZERO_PAGE_SIZE);
        assert_eq!(db.page_count(), 3);
        let (f, u, r) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, u, r);
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.page_count, 3);
        assert_eq!(db.header.page_size, DEFAULT_PAGE_SIZE);
        //FileDB::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_simple_alloc_page() {
        const DB_NAME: &str = "test2.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME.to_string()).unwrap();
        let page = db.buffer.alloc_page(false);
        assert!(page.is_ok());
        let page = page.unwrap();
        assert_eq!(page, 3.into());
        thread::sleep(Duration::from_millis(100));
        let m = db.file.get_metadata().unwrap();
        assert_eq!(m.len, DEFAULT_PAGE_SIZE * 4 + ZERO_PAGE_SIZE);
        let page = db.buffer.alloc_page(false).unwrap_or(0.into());
        assert_eq!(page, 4.into());
        thread::sleep(Duration::from_millis(100));
        let m = db.file.get_metadata().unwrap();
        assert_eq!(m.len, ZERO_PAGE_SIZE + 5 * DEFAULT_PAGE_SIZE);
        assert_eq!(db.page_count(), 5);
        let (f, u, r) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, u, r).unwrap();
        assert_eq!(db.page_count(), 5);
        //FileDB::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_create_table() {
        const DB_NAME: &str = "test3.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME.to_string());
        assert!(db.is_ok());
        let db = db.unwrap();
        let r = db.create_table("table_1".to_string());
        assert!(r.is_ok());
        assert_eq!(db.get_tables().unwrap().len(), 1);
        let (f, u, r) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME.to_string(), f, u, r).unwrap();
        let t = db.get_tables().unwrap();
        assert!(t.len() == 1);
        assert_eq!(t[0].name, "table_1");
        let r = db.create_table("table_1".to_string());
        assert!(matches!(r, Err(StoreError::DuplicateName(_))));
        //FileDB::delete(DB_NAME).unwrap_or_default()
    }
}
