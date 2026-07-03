#![allow(private_bounds)]
use crate::buffer::PageBuffer;
use crate::constant::FIRST_USER_PAGE;
use crate::constant::GENERATOR_TABLE_PAGE;
use crate::constant::MAX_TABLE_NAME_LEN;
use crate::constant::SYSTEM_TABLE_NAME;
use crate::constant::SYSTEM_TABLE_PAGE;
use crate::constant::timestamp;
use crate::error::StoreError;
use crate::generator::Generator;
use crate::logger::Logger;
use crate::logger::Operation;
use crate::logger::Record;
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
pub struct Meta {
    pub len: u64,
}

pub trait Opener {
    type Item;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<Self::Item>;
    fn do_sync(&mut self) -> std::io::Result<()>;
    fn do_clone(&self) -> std::io::Result<Self::Item>;
    fn get_metadata(&self) -> std::io::Result<Meta>;
    fn do_lock(&self) -> Result<(), TryLockError>;

    /// Positioned read: fills as much of `buf` as available starting at
    /// `offset`, returning the number of bytes actually read (0 at EOF) —
    /// same partial-transfer contract as `Read::read`. Does not use or affect
    /// any shared seek cursor: `do_clone()`'d handles to the same underlying
    /// file (e.g. `std::fs::File::try_clone`) share their OS-level cursor, so
    /// a `seek` on one silently moves the position under a concurrent
    /// `seek`+`read`/`write` on another — this is what let `PageBuffer`'s
    /// `self_file` and its background writer thread's independently-cloned
    /// handle race each other into misaligned reads/writes of the wrong file
    /// offset. Positioned I/O (pread/pwrite) sidesteps that entirely: every
    /// call is self-contained and safe to run concurrently across clones.
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;

    /// Positioned write — see `pread`. Returns bytes actually written (same
    /// partial-transfer contract as `Write::write`).
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;
}

pub trait DBFile:
    std::io::Write + std::io::Read + std::io::Seek + std::marker::Send + std::marker::Sync + Opener
{
}
pub(crate) type DBSizeType = u64;

impl<T> DBFile for T where
    T: std::io::Write + std::io::Read + std::io::Seek + std::marker::Send + std::marker::Sync + Opener
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
            // Must be the same Arc<Generator> passed to setup_needed_modules:
            // TransactionManager holds its own clone of `gens` and calls
            // gen_key(TXN_GENERATOR_NANE) on it directly. If `generator` were a
            // separate instance, load_system_tables()'s restore below would
            // never reach the generator tx_mgr actually uses, so the txn id
            // sequence would silently restart at 0 on every reopen — colliding
            // with transaction ids from the prior session.
            generator: gens,
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
        // Revert any still-aborting transactions before persisting: the aborting
        // set is in-memory only, so an un-reverted aborted row would reappear as
        // committed after reopen (its txn no longer being in any set).
        self.drain_aborting();
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
        // Reclaim any transactions abandoned via Transaction::drop (parked in
        // `aborting`) before starting new work: they are already invisible, this
        // physically reverts them so their rows don't linger and a re-insert of
        // the same key finds it free. The reverts are conditional (see
        // revert_txn_writes), so this is safe even if another thread is making
        // forward progress on the same keys.
        self.drain_aborting();
        Ok(self.tx_mgr.begin()?)
    }

    pub fn commit(&self, txn: Transaction) -> Result<(), StoreError> {
        // Detach the id before doing any fallible work below. If that work
        // fails partway, returning the `?` here must NOT fall back to
        // `Transaction::drop`'s default rollback — see `Transaction::into_id`.
        // The transaction simply stays active (and correctly invisible) until
        // a retried commit completes successfully.
        let id = txn.into_id();
        // Capture the tombstoned rows to reclaim BEFORE writing the commit
        // marker: logging a Commit op discards this txn's undo records (see
        // Logger::log_undo), so we must read them first.
        let del_records: Vec<Record> = self
            .logger
            .get_undo_operations(id.clone())?
            .into_iter()
            .filter_map(|o| match o {
                Operation::Del(_, r) => Some(r),
                _ => None,
            })
            .collect();
        // Commit point FIRST — make the transaction atomically committed before
        // touching the tree. The physical reclamation of tombstoned rows below
        // is best-effort cleanup (find() already treats a committed tombstone as
        // absent), so it must not run *before* the commit: doing so let commit
        // remove rows and then return Err from a later step, leaving the caller
        // unable to tell "fully failed" from "partially applied" — which diverged
        // the caller's state from the DB's (a committed remove the caller thought
        // had failed).
        let op = Operation::Commit(id.clone(), timestamp());
        self.logger.log_redo(op.clone())?;
        self.logger.log_undo(op)?;
        self.tx_mgr.commit(id.clone())?;

        // Best-effort tombstone reclamation. Errors here do not un-commit the
        // transaction; the tombstone stays and is handled by find() until a
        // later pass reclaims it.
        for r in del_records {
            if let Ok(table) = self.table_by_id(r.table_id) {
                if let Ok(Some(tuple)) = table.find(r.tuple.id.clone()) {
                    if tuple.is_tombstoned() && tuple.is_same_txn(id.clone()) {
                        let _ = retry_on_contention(|| table.remove(tuple.id.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn rollback(&self, txn: Transaction) -> Result<(), StoreError> {
        // See the comment in `commit` above — `into_id` prevents Drop's default
        // rollback so this is the single place the txn changes state.
        let id = txn.into_id();
        // Revert while the txn is STILL ACTIVE. Active transactions are invisible
        // (is_committed == false) and are never scanned by drain_aborting, so the
        // owner reverts its own writes with zero cross-thread interference — no
        // other thread can observe or reclaim this txn mid-revert. Only once the
        // writes are physically undone do we retire it from the active set.
        // Revert BEFORE the Rollback marker, which discards the undo records.
        self.revert_txn_writes(&id)?;
        let op = Operation::Rollback(id.clone(), timestamp());
        self.logger.log_redo(op.clone())?;
        self.logger.log_undo(op)?;
        self.tx_mgr.finish_rolled_back(id);
        Ok(())
    }

    /// Physically revert `id`'s writes by replaying its undo log. Each op is
    /// applied *conditionally* — only if the row still belongs to `id` at the
    /// moment of the write, checked under the data-page lock
    /// (update_if_txn / remove_if_txn) — so a concurrent forward write to the
    /// same key by another transaction is never clobbered. Performs no
    /// transaction-set or undo-log bookkeeping; callers do that.
    fn revert_txn_writes(&self, id: &TransactionId) -> Result<(), StoreError> {
        let ops = self.logger.get_undo_operations(id.clone())?;
        for o in ops {
            match o {
                Operation::Add(_, r) => {
                    let table = self.table_by_id(r.table_id)?;
                    retry_on_contention(|| table.remove_if_txn(r.tuple.id.clone(), id))?;
                }
                Operation::Del(_, r) | Operation::Mod(_, r) => {
                    let table = self.table_by_id(r.table_id)?;
                    retry_on_contention(|| table.update_if_txn(r.tuple.clone(), id))?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Reclaim a transaction parked in `aborting` — one abandoned via
    /// Transaction::drop, which has no table access to revert itself. Reverts its
    /// writes, drops its undo records, then finishes the abort. Safe to run
    /// concurrently with the owner's forward progress (and with another drainer)
    /// thanks to the conditional reverts in revert_txn_writes.
    fn revert_aborted(&self, id: &TransactionId) -> Result<(), StoreError> {
        self.revert_txn_writes(id)?;
        self.logger.discard_undo(id);
        self.tx_mgr.abort_complete(id);
        Ok(())
    }

    /// Best-effort reclamation of transactions parked in `aborting` by
    /// Transaction::drop. Only *dropped* txns land there — a Db-level rollback
    /// reverts itself while active and never enters this set — so under healthy
    /// operation this is empty and the loop is a no-op. A revert that errors is
    /// simply retried on the next drain (the txn stays invisible meanwhile).
    pub(crate) fn drain_aborting(&self) {
        for id in self.tx_mgr.aborting_ids() {
            let _ = self.revert_aborted(&id);
        }
    }

    pub(crate) fn table_by_id(&self, id: TableIdType) -> Result<Arc<BPlusTree<F>>, StoreError> {
        Ok(self
            .tables
            .read()
            .get(&id)
            .map(|t| Arc::clone(t))
            .ok_or(StoreError::TableNotFound(id.to_string()))?)
    }

    pub fn insert(
        &self,
        id: TableIdType,
        tuple: Tuple,
        txn: &Transaction,
    ) -> Result<(), StoreError> {
        let tx_id = txn.id();
        let mut tuple = tuple;
        tuple.set_txn_id(tx_id.clone());
        self.table_by_id(id)?.insert(tuple.clone(), tx_id.clone())?;
        let op = Operation::Add(tx_id, Record::new(id, tuple));
        self.logger.log_undo(op.clone())?;
        self.logger.log_redo(op)?;
        Ok(())
    }

    pub fn find(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn: &Transaction,
    ) -> Result<Option<Tuple>, StoreError> {
        let _txn_id = txn.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            let committed = self.find_last_committed(&tuple).map(|t| t.into_owned());
            // A committed tombstone means the key was removed — it must be
            // invisible even if its physical row hasn't been reclaimed yet.
            // (commit reclaims tombstones best-effort AFTER its commit point, so
            // a committed-but-not-yet-reclaimed tombstone can legitimately still
            // be present in the tree.)
            match committed {
                Some(t) if t.is_tombstoned() => return Ok(None),
                other => return Ok(other),
            }
        } else {
            return Ok(None);
        }
    }

    pub fn update(
        &self,
        tid: TableIdType,
        new_tuple: Tuple,
        txn_id: &Transaction,
    ) -> Result<(), StoreError> {
        let txn = txn_id.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(new_tuple.id.clone())?;
        if let Some(tuple) = tuple {
            // old_tuple is the pre-update, already-committed version. It's kept
            // (with its original txn_id) as the undo record's content, so a
            // rollback restores the exact prior state and concurrent readers
            // can walk the undo chain back to a value that's actually visible.
            let old_tuple = self
                .find_last_committed(&tuple)
                .ok_or(StoreError::KeyNotFound(new_tuple.id.clone()))?
                .into_owned();
            let mut updated = old_tuple.clone();
            updated.set_txn_id(txn.clone());
            updated.set_undo_id(self.logger.next_undo_id(txn.clone())?);
            updated.set_data(&new_tuple.data);
            // Undo log must be written BEFORE the tree is mutated: once
            // `updated` (carrying undo_id) lands in the tree, a concurrent
            // reader on another thread can observe it immediately and try to
            // resolve that undo_id via find_last_committed. If the undo entry
            // doesn't exist yet, that lookup panics (find_undo_tuple returns
            // None where the code expects Some).
            let redo_op = Operation::Mod(txn.clone(), Record::new(tid, updated.clone()));
            let undo_op = Operation::Mod(txn.clone(), Record::new(tid, old_tuple));
            self.logger.log_redo(redo_op)?;
            self.logger.log_undo(undo_op)?;
            table.update(updated)?;
            return Ok(());
        } else {
            return Err(StoreError::KeyNotFound(new_tuple.id));
        }
    }

    pub fn remove(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn_id: &Transaction,
    ) -> Result<Tuple, StoreError> {
        let txn = txn_id.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            // old_tuple is the pre-remove, already-committed (non-tombstoned)
            // version, kept as the undo record's content so a rollback restores
            // the row exactly (including clearing the tombstone flag) and
            // concurrent readers see it instead of the in-flight tombstone.
            let old_tuple = self
                .find_last_committed(&tuple)
                .ok_or(StoreError::KeyNotFound(tuple.id.clone()))?
                .into_owned();
            let mut tombstoned = old_tuple.clone();
            tombstoned.set_txn_id(txn.clone());
            tombstoned.tombstone();
            tombstoned.set_undo_id(self.logger.next_undo_id(txn.clone())?);
            // Same ordering requirement as update(): log undo/redo before the
            // tombstoned tuple becomes visible in the tree, so a concurrent
            // reader can never observe an undo_id that doesn't resolve yet.
            let redo_op = Operation::Del(txn.clone(), Record::new(tid, tombstoned.clone()));
            let undo_op = Operation::Del(txn.clone(), Record::new(tid, old_tuple));
            self.logger.log_redo(redo_op)?;
            self.logger.log_undo(undo_op)?;
            table.update(tombstoned.clone())?;
            return Ok(tombstoned);
        } else {
            return Err(StoreError::KeyNotFound(id));
        }
    }

    fn find_last_committed<'a>(&self, tuple: &'a Tuple) -> Option<Cow<'a, Tuple>> {
        if let Some(txn) = tuple.txn_id.clone() {
            // Visible iff the writer COMMITTED — i.e. it is neither still active
            // nor aborting-with-unreverted-writes. A dropped/aborted txn stays
            // in `aborting` and is therefore correctly invisible here even
            // though it has left the active set.
            if self.tx_mgr.is_committed(&txn) {
                Some(Cow::Borrowed(tuple))
            } else {
                let mut tuple = tuple.clone();
                let mut txn = txn;
                loop {
                    if tuple.undo_id.is_none() {
                        return None;
                    }
                    let undo_id = tuple.undo_id.unwrap();

                    // Tolerate a missing undo record: an aborting txn's undo can
                    // be discarded concurrently once its rows are reverted. If we
                    // can't walk further, treat the row as not-yet-committed
                    // (invisible) rather than panicking.
                    let next_tuple = match self.logger.find_undo_tuple(txn.clone(), undo_id) {
                        Some(t) => t,
                        None => return None,
                    };
                    let next_txn = match next_tuple.txn_id.clone() {
                        Some(t) => t,
                        None => return None,
                    };
                    if self.tx_mgr.is_committed(&next_txn) {
                        // next_tuple is the committed ancestor we walked back
                        // to — return it, not the in-flight `tuple` we started
                        // from (which belongs to a non-committed txn and must
                        // stay invisible to other readers).
                        return Some(Cow::Owned(next_tuple));
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
        // Buffer shares the logger's WAL clock, so page-flush deferral and redo
        // LSNs are scoped to this one database (not a process global).
        let clock = logger.clock();
        let nm = NeededObjects {
            buffer: Arc::new(PageBuffer::new(
                header.page_size,
                page_counter,
                file,
                header,
                1024,
                clock,
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
        self.buffer.write_page(0usize.into(), &page)?;
        let gens = self.generator.get_values()?;
        let mut page = Page::new_pinned(self.header.page_size);
        page.add_tuple(Tuple::new(0, &to_allocvec(&gens)?))?;
        self.buffer.write_page(1usize.into(), &page)?;
        // TODO : Handle page overflows correctly
        // TODO: Handle empty pages
        Ok(())
    }

    fn create_system_tables(&self) -> Result<(), StoreError> {
        let t = self.buffer.alloc_page(true)?; // system
        assert!(t == 0usize.into());
        let t = self.buffer.alloc_page(true)?; // generators
        assert!(t == 1usize.into());
        let t = self.buffer.alloc_page(true)?; // free pages
        assert!(t == 2usize.into());
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

/// Retries `f` on `LockContentionError` with a short linear backoff. Used by
/// `Db::commit`/`Db::rollback`'s per-record cleanup loops: those calls race
/// against the same per-page locks every other concurrent operation uses, and
/// under load a single transient lock timeout shouldn't abort the whole
/// commit/rollback (see `Transaction::into_id`).
fn retry_on_contention<T>(mut f: impl FnMut() -> Result<T, StoreError>) -> Result<T, StoreError> {
    let mut attempt = 0u32;
    loop {
        match f() {
            Err(StoreError::LockContentionError) if attempt < 8 => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_micros(100 * attempt as u64));
            }
            other => return other,
        }
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
        table::TableIdType,
        tuple::{DBIdType, Tuple},
    };
    type TestDB = Db<MemFile>;

    fn make_db_with_table() -> (TestDB, TableIdType) {
        let db = TestDB::create("txn_test.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        (db, tid)
    }

    fn make_db_with_two_tables() -> (TestDB, TableIdType, TableIdType) {
        let db = TestDB::create("txn_test_multi.db").unwrap();
        let ta = db.create_table("table_a".to_string()).unwrap();
        let tb = db.create_table("table_b".to_string()).unwrap();
        (db, ta, tb)
    }

    fn row(id: u64, data: &[u8]) -> Tuple {
        Tuple::new(id, data)
    }

    fn id(n: u64) -> DBIdType {
        DBIdType::Int(n)
    }

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
        assert_eq!(page, 3usize.into());
        thread::sleep(Duration::from_millis(100));
        let m = db.file.get_metadata().unwrap();
        assert_eq!(m.len, DEFAULT_PAGE_SIZE * 4 + ZERO_PAGE_SIZE);
        let page = db.buffer.alloc_page(false).unwrap_or(0usize.into());
        assert_eq!(page, 4usize.into());
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

    // ── transactional insert / find ───────────────────────────────────────────

    #[test]
    fn test_txn_insert_commit_find() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(found.expect("row should be visible").data.to_vec(), b"hello");
    }

    #[test]
    fn test_txn_insert_rollback_not_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"gone"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(found.is_none(), "rolled-back insert must not be visible");
    }

    #[test]
    fn test_txn_dropped_guard_insert_not_visible() {
        // A transaction guard dropped WITHOUT commit/rollback (e.g. an uncaught
        // error) must not leak its writes as committed. Its write stays invisible
        // (parked in `aborting`) and is physically reverted on the next drain.
        let (db, tid) = make_db_with_table();
        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"dropped"), &txn).unwrap();
            // txn dropped here — no commit, no rollback.
        }
        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(
            found.is_none(),
            "a dropped transaction's insert must not be visible as committed"
        );
    }

    #[test]
    fn test_txn_dropped_guard_update_reverts_to_committed() {
        // A dropped guard that UPDATED a committed key must not leak the update;
        // the committed value stands, and is physically restored on the next
        // drain (Mod-revert path).
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t0).unwrap();
        db.commit(t0).unwrap();
        {
            let t1 = db.begin().unwrap();
            db.update(tid, row(1, b"v2"), &t1).unwrap();
            // t1 dropped here — no commit, no rollback.
        }
        let t2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &t2).unwrap();
        drop(t2);
        assert_eq!(
            found.expect("committed v1 must remain").data.to_vec(),
            b"v1",
            "a dropped update must not be visible; committed value stands"
        );
    }

    #[test]
    fn test_txn_dropped_guard_then_reinsert_succeeds() {
        // After a dropped transaction's insert is reverted, the same key can be
        // re-inserted cleanly (its orphaned row/index entry must be gone).
        let (db, tid) = make_db_with_table();
        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"dropped"), &txn).unwrap();
        }
        let txn2 = db.begin().unwrap();
        db.insert(tid, row(1, b"real"), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("re-insert must be visible").data(), b"real");
    }

    #[test]
    fn test_txn_multiple_inserts_commit_all_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn).unwrap();
        db.insert(tid, row(2, b"B"), &txn).unwrap();
        db.insert(tid, row(3, b"C"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(db.find(tid, id(1), &txn2).unwrap().unwrap().data.to_vec(), b"A");
        assert_eq!(db.find(tid, id(2), &txn2).unwrap().unwrap().data.to_vec(), b"B");
        assert_eq!(db.find(tid, id(3), &txn2).unwrap().unwrap().data.to_vec(), b"C");
        drop(txn2);
    }

    #[test]
    fn test_txn_multiple_inserts_rollback_none_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn).unwrap();
        db.insert(tid, row(2, b"B"), &txn).unwrap();
        db.insert(tid, row(3, b"C"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(db.find(tid, id(1), &txn2).unwrap().is_none());
        assert!(db.find(tid, id(2), &txn2).unwrap().is_none());
        assert!(db.find(tid, id(3), &txn2).unwrap().is_none());
        drop(txn2);
    }

    // ── read isolation (uncommitted writes are invisible) ─────────────────────

    #[test]
    fn test_txn_uncommitted_insert_not_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        // T1 inserts but doesn't commit yet
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(42, b"secret"), &txn1).unwrap();

        // T2 (concurrent) must not see T1's uncommitted row
        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn2).unwrap();
        drop(txn2);
        assert!(
            found.is_none(),
            "uncommitted insert must be invisible to other txns"
        );

        // After T1 commits, T3 should see it
        db.commit(txn1).unwrap();
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("committed row must be visible").data.to_vec(),
            b"secret"
        );
    }

    // ── update ────────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_update_commit_sees_new_data() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("updated row must exist").data.to_vec(), b"v2");
    }

    #[test]
    fn test_txn_uncommitted_update_not_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // T2 updates but doesn't commit
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();

        // T3 must still see the old committed value
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("original must still be visible").data.to_vec(), b"v1");

        db.commit(txn2).unwrap();
    }

    #[test]
    fn test_txn_update_nonexistent_returns_err() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        let r = db.update(tid, row(99, b"x"), &txn);
        assert!(
            matches!(r, Err(StoreError::KeyNotFound(_))),
            "updating missing row must return KeyNotFound, got {r:?}"
        );
        // No writes were logged for this txn; dropping the guard rolls it
        // back at the manager level, which is sufficient to remove it from
        // the active set.
        drop(txn);
    }

    // ── remove ────────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_remove_commit_not_findable() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"bye"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert!(found.is_none(), "committed remove must make row invisible");
    }

    #[test]
    fn test_txn_uncommitted_remove_row_still_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"alive"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // T2 removes but doesn't commit yet
        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();

        // T3 must still see the row (T2 is uncommitted)
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found
                .expect("row must still be visible before remove commits")
                .data.to_vec(),
            b"alive"
        );

        db.commit(txn2).unwrap();
    }

    #[test]
    fn test_txn_remove_nonexistent_returns_err() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        let r = db.remove(tid, id(999), &txn);
        assert!(
            matches!(r, Err(StoreError::KeyNotFound(_))),
            "removing missing row must return KeyNotFound, got {r:?}"
        );
        drop(txn); // no writes logged → drop cleans up the active set
    }

    // ── multiple operations in a single transaction ───────────────────────────

    #[test]
    fn test_txn_multiple_ops_in_one_txn_commit() {
        let (db, tid) = make_db_with_table();

        // Seed three rows
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn1).unwrap();
        db.insert(tid, row(2, b"B"), &txn1).unwrap();
        db.insert(tid, row(3, b"C"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // One txn: update row 1, leave row 2 alone, remove row 3
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"A_v2"), &txn2).unwrap();
        db.remove(tid, id(3), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        assert_eq!(db.find(tid, id(1), &txn3).unwrap().unwrap().data.to_vec(), b"A_v2");
        assert_eq!(db.find(tid, id(2), &txn3).unwrap().unwrap().data.to_vec(), b"B");
        assert!(
            db.find(tid, id(3), &txn3).unwrap().is_none(),
            "removed row must be gone"
        );
        drop(txn3);
    }

    #[test]
    fn test_txn_large_number_of_inserts_all_findable() {
        let (db, tid) = make_db_with_table();
        const N: u64 = 200;

        let txn = db.begin().unwrap();
        for i in 0..N {
            db.insert(tid, row(i, format!("val_{i}").as_bytes()), &txn)
                .unwrap();
        }
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        for i in 0..N {
            let found = db.find(tid, id(i), &txn2).unwrap();
            assert_eq!(
                found.unwrap_or_else(|| panic!("row {i} missing")).data.to_vec(),
                format!("val_{i}").as_bytes(),
                "row {i} has wrong data"
            );
        }
        drop(txn2);
    }

    // ── persistence (close + reopen) ─────────────────────────────────────────

    #[test]
    fn test_txn_close_reopen_data_persists() {
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"persistent"), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found.expect("data must survive close/reopen").data.to_vec(),
            b"persistent"
        );
    }

    #[test]
    fn test_txn_close_reopen_new_txn_id_does_not_collide_with_prior_session() {
        // Regression test for the generator-restoration bug: open_using() used to
        // assign a fresh Generator to self.generator instead of reusing the Arc
        // passed to TransactionManager, so the txn id sequence silently restarted
        // at 0 after every reopen and collided with ids from the prior session.
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        let first_txn_id = txn.id();
        db.insert(tid, row(1, b"v1"), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let txn2 = db2.begin().unwrap();
        assert_ne!(
            txn2.id(),
            first_txn_id,
            "txn id sequence must not restart after reopen"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_close_reopen_removed_row_stays_gone() {
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        db.insert(tid, row(7, b"temp"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn = db.begin().unwrap();
        db.remove(tid, id(7), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(7), &txn2).unwrap();
        drop(txn2);
        assert!(found.is_none(), "removed row must stay gone after reopen");
    }

    // ── error cases ───────────────────────────────────────────────────────────

    #[test]
    fn test_txn_duplicate_insert_returns_err() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"first"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        let r = db.insert(tid, row(1, b"second"), &txn2);
        assert!(
            matches!(r, Err(StoreError::DuplicateKey(_))),
            "duplicate insert must return DuplicateKey, got {r:?}"
        );
        drop(txn2); // insert failed before any writes were logged → drop cleans up active set
    }

    #[test]
    fn test_txn_insert_on_nonexistent_table_returns_err() {
        let (db, _) = make_db_with_table();
        let fake_tid: TableIdType = 9999u64.into();
        let txn = db.begin().unwrap();
        let r = db.insert(fake_tid, row(1, b"x"), &txn);
        assert!(
            matches!(r, Err(StoreError::TableNotFound(_))),
            "expected TableNotFound, got {r:?}"
        );
        drop(txn); // no writes logged → drop cleans up the active set
    }

    // ── RAII guard ────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_drop_without_explicit_commit_rolls_back_at_mgr_level() {
        // Transaction::Drop calls mgr.rollback (not Db::rollback), so it doesn't
        // replay undo ops, but it does remove the txn from the active set — which
        // means concurrent readers stop being blocked by it.
        let (db, tid) = make_db_with_table();

        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"ephemeral"), &txn).unwrap();
            // txn drops here → mgr.rollback fires but undo-log replay does NOT
        }

        // Because undo-log replay didn't fire, the row may or may not be
        // physically present — but the guard-level test is that the txn is no
        // longer active (so it can't block readers). Use Db::rollback for full
        // application-level undo.
        assert_eq!(
            db.tx_mgr.active_count(),
            0,
            "dropped txn must be removed from active set"
        );
    }

    // ── rollback of Mod/Del restores the pre-image ────────────────────────────

    #[test]
    fn test_txn_update_rollback_sees_original_data() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();
        db.rollback(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("original row must still exist").data.to_vec(), b"v1");
    }

    #[test]
    fn test_txn_remove_rollback_row_still_visible() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"alive"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();
        db.rollback(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("row must survive a rolled-back remove").data.to_vec(),
            b"alive"
        );
    }

    // ── multi-table transactions ───────────────────────────────────────────────

    #[test]
    fn test_txn_insert_across_two_tables_commit_both_visible() {
        let (db, ta, tb) = make_db_with_two_tables();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &txn).unwrap();
        db.insert(tb, row(1, b"b1"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(), b"a1");
        assert_eq!(db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(), b"b1");
        drop(txn2);
    }

    #[test]
    fn test_txn_insert_across_two_tables_rollback_neither_visible() {
        let (db, ta, tb) = make_db_with_two_tables();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &txn).unwrap();
        db.insert(tb, row(1, b"b1"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(db.find(ta, id(1), &txn2).unwrap().is_none());
        assert!(db.find(tb, id(1), &txn2).unwrap().is_none());
        drop(txn2);
    }

    #[test]
    fn test_txn_update_and_remove_across_two_tables_commit() {
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(ta, row(1, b"a_v1"), &setup).unwrap();
        db.insert(tb, row(1, b"b_v1"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.update(ta, row(1, b"a_v2"), &txn).unwrap();
        db.remove(tb, id(1), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(), b"a_v2");
        assert!(db.find(tb, id(1), &txn2).unwrap().is_none());
        drop(txn2);
    }

    #[test]
    fn test_txn_update_and_remove_across_two_tables_rollback_restores_both() {
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(ta, row(1, b"a_v1"), &setup).unwrap();
        db.insert(tb, row(1, b"b_v1"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.update(ta, row(1, b"a_v2"), &txn).unwrap();
        db.remove(tb, id(1), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a_v1",
            "table A update must be rolled back"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"b_v1",
            "table B remove must be rolled back"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_partial_failure_across_tables_rollback_undoes_successful_table() {
        // Table A's insert succeeds; table B's insert fails (duplicate key
        // already present, inserted by an earlier committed txn). Rolling back
        // the failed txn must undo table A's insert even though table B's
        // write never got logged in the first place.
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(tb, row(1, b"existing"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a_new"), &txn).unwrap();
        let r = db.insert(tb, row(1, b"dup"), &txn);
        assert!(
            matches!(r, Err(StoreError::DuplicateKey(_))),
            "expected DuplicateKey, got {r:?}"
        );
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(
            db.find(ta, id(1), &txn2).unwrap().is_none(),
            "table A's successful insert must be undone by the txn-wide rollback"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"existing",
            "table B must be unaffected by the failed duplicate insert"
        );
        drop(txn2);
    }

    // ── large-object (multi-page overflow) ───────────────────────────────────
    // DEFAULT_PAGE_SIZE=16384, page_data_size=16304.
    // A payload of N bytes → serialized tuple ≈ N+20 bytes.
    // Spans: 2 pages ≈ 20 KB payload, 4 pages ≈ 50 KB, 5 pages ≈ 70 KB.

    #[test]
    fn test_large_object_commit_is_findable() {
        let (db, tid) = make_db_with_table();
        // 50 KB payload → serialized ≈ 50 020 B → 4 overflow pages
        let big = vec![b'A'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(found.expect("large object must be visible after commit").data.to_vec(), big);
    }

    #[test]
    fn test_large_object_rollback_not_visible() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'B'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &big), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(found.is_none(), "rolled-back large object must not be visible");
    }

    #[test]
    fn test_very_large_object_five_pages_commit() {
        let (db, tid) = make_db_with_table();
        // 70 KB payload → serialized ≈ 70 020 B → 5 overflow pages
        let big = vec![b'C'; 70_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(42, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn2).unwrap();
        drop(txn2);
        assert_eq!(found.expect("5-page object must survive commit").data.to_vec(), big);
    }

    #[test]
    fn test_large_objects_persist_across_close_reopen() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'D'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(99, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        // Close and reopen — overflow pages must be readable from disk
        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(99), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found.expect("large object must survive close/reopen").data.to_vec(),
            big
        );
    }

    #[test]
    fn test_multiple_large_objects_same_txn_commit() {
        let (db, tid) = make_db_with_table();
        // Three objects that collectively span many overflow pages
        let small = vec![b'E'; 20_000];  // 2 pages each
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &small), &txn).unwrap();
        db.insert(tid, row(2, &small), &txn).unwrap();
        db.insert(tid, row(3, &small), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        for i in 1u64..=3 {
            let found = db.find(tid, id(i), &txn2).unwrap();
            assert_eq!(
                found.unwrap_or_else(|| panic!("row {i} missing")).data.to_vec(),
                small
            );
        }
        drop(txn2);
    }

    #[test]
    fn test_large_object_rollback_then_reinsert_succeeds() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'F'; 50_000];

        // Insert + rollback
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(7, &big), &txn1).unwrap();
        db.rollback(txn1).unwrap();

        // Re-insert the same key + commit
        let txn2 = db.begin().unwrap();
        db.insert(tid, row(7, &big), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(7), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("re-inserted large object must be visible").data.to_vec(), big);
    }

    // ── large-object: exercise EVERY public op at 4–8× the page size ──────────

    /// A payload of `len` bytes whose byte at each position varies with both the
    /// position and `seed`. A uniform `vec![b'X'; len]` would still verify equal
    /// even if the overflow-page chain were reassembled out of order or a page
    /// were duplicated; a position-dependent pattern catches those.
    fn big_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((i as u64).wrapping_mul(31).wrapping_add(seed as u64) & 0xFF) as u8)
            .collect()
    }

    #[test]
    fn test_large_object_full_lifecycle_all_ops() {
        // One object driven through every public operation while it spans 4–8
        // overflow pages: insert, find, update (grow), update (shrink), remove,
        // re-insert, commit, rollback, and close/reopen. DEFAULT_PAGE_SIZE=16 KB,
        // so 4×≈64 KB … 8×≈128 KB payloads.
        let page = DEFAULT_PAGE_SIZE as usize;
        assert!((4 * page..=8 * page).contains(&(6 * page)));
        let base = big_payload(1, 6 * page); // ~6× page
        let grown = big_payload(2, 8 * page); // ~8× page — more overflow pages
        let shrunk = big_payload(3, 4 * page); // ~4× page — fewer overflow pages
        let reins = big_payload(4, 5 * page); // ~5× page

        let (db, tid) = make_db_with_table();

        // insert + commit → find returns exactly what went in
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &base), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().expect("inserted").data.to_vec(),
            base
        );
        db.rollback(t).unwrap();

        // update that GROWS the object (allocates more overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().expect("grown").data.to_vec(),
            grown
        );
        db.rollback(t).unwrap();

        // update that SHRINKS the object (frees overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().expect("shrunk").data.to_vec(),
            shrunk
        );
        db.rollback(t).unwrap();

        // remove → gone
        let t = db.begin().unwrap();
        db.remove(tid, id(1), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert!(
            db.find(tid, id(1), &t).unwrap().is_none(),
            "removed large object must be gone"
        );
        db.rollback(t).unwrap();

        // re-insert, then close/reopen → overflow chain readable from storage
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &reins), &t).unwrap();
        db.commit(t).unwrap();
        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().expect("reopened").data.to_vec(),
            reins
        );
        db2.rollback(t).unwrap();
    }

    #[test]
    fn test_large_object_update_rollback_keeps_committed() {
        // Rolling back an update between two large values must restore the exact
        // committed overflow chain (Mod-revert across multiple pages).
        let page = DEFAULT_PAGE_SIZE as usize;
        let v1 = big_payload(10, 6 * page);
        let v2 = big_payload(20, 8 * page);
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(5, &v1), &t).unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        db.update(tid, row(5, &v2), &t).unwrap();
        db.rollback(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(5), &t).unwrap().expect("v1 must stand").data.to_vec(),
            v1,
            "rolled-back large update must leave the committed value intact"
        );
        db.rollback(t).unwrap();
    }

    #[test]
    fn test_large_object_remove_rollback_still_visible() {
        // Rolling back the removal of a large object must leave it fully readable.
        let page = DEFAULT_PAGE_SIZE as usize;
        let v = big_payload(30, 7 * page);
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(8, &v), &t).unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        db.remove(tid, id(8), &t).unwrap();
        db.rollback(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(8), &t)
                .unwrap()
                .expect("large object must survive a rolled-back remove")
                .data.to_vec(),
            v
        );
        db.rollback(t).unwrap();
    }
}
