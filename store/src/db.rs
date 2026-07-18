#![allow(private_bounds)]
use crate::buffer::PageBuffer;
use crate::constant::FIRST_USER_PAGE;
use crate::constant::FREE_PAGE_TABLE_PAGE;
use crate::constant::GENERATOR_TABLE_PAGE;
use crate::constant::MAX_TABLE_NAME_LEN;
use crate::constant::SYSTEM_TABLE_NAME;
use crate::constant::SYSTEM_TABLE_PAGE;
use crate::constant::timestamp;
use crate::cursor::RangeCursor;
use crate::cursor::TableCursor;
use crate::error::StoreError;
use crate::generator::Generator;
use crate::logger::Logger;
use crate::logger::MsgType;
use crate::logger::Operation;
use crate::logger::Record;
use crate::memfile::MemFile;
use crate::page::Page;
use crate::table::Table;
use crate::table::TableIdType;
use crate::tables::bplustree;
use crate::tables::bplustree::BPlusTree;
use crate::tuple::DBIdType;
use crate::tuple::Tuple;
use crate::txn::Transaction;
use crate::txn::TransactionId;
use crate::txn::TransactionManager;
use log::LevelFilter;
use log::info;
use memmap::MmapOptions;
use parking_lot::RwLock;
use portable_atomic::AtomicU128;
use postcard::from_bytes;
use postcard::take_from_bytes;
use postcard::to_allocvec;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
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
// Cap on pages the writer thread will hold in memory awaiting durable redo
// before applying backpressure to callers (see PageBuffer's writer). Not
// persisted — purely a runtime memory/throughput knob, safe to pick freshly
// on every open. Matches PageBuffer's existing page-cache size (max_entries)
// as a reasonable default order of magnitude.
const DEFAULT_MAX_PENDING_WRITES: usize = 1024;

pub type FileDB = Db<File>;
pub struct Meta {
    pub len: u64,
}

pub trait Opener: Any {
    type Item;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<Self::Item>;
    fn truncate(&mut self) -> std::io::Result<()>;
    fn do_sync(&mut self) -> std::io::Result<()>;
    fn do_clone(&self) -> std::io::Result<Self::Item>;
    fn get_metadata(&self) -> std::io::Result<Meta>;
    fn do_lock(&self) -> Result<(), TryLockError>;
    fn as_any(&self) -> &dyn Any;

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
    T: std::io::Write
        + std::io::Read
        + std::io::Seek
        + std::marker::Send
        + std::marker::Sync
        + Opener
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
    pub(crate) last_checkpoint: u128,
}

// Every constructor (create*/open*) returns Arc<Db<F>>, never a bare Db<F>:
// TableCursor needs to hold its own reference to the Db it's scanning (to
// resolve MVCC visibility per row via find_last_committed), so Db is meant
// to always be shared this way rather than owned outright by one caller.
// close() reflects this too — it takes Arc<Self> and unwraps it internally.
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
    last_checkpoint: AtomicU128, // Store the actual checkopint so it can be mutated
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
    pub fn create<S: AsRef<str>>(name: S) -> Result<Arc<Self>, StoreError> {
        Self::create_with_page_size(name, DEFAULT_PAGE_SIZE)
    }

    pub fn create_with_page_size<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
    ) -> Result<Arc<Self>, StoreError> {
        Self::create_with_limits(name, page_size, DEFAULT_MAX_PENDING_WRITES)
    }

    // Like create_with_page_size, but also controls how many dirty pages the
    // writer thread will hold in memory awaiting durable redo before blocking
    // callers (see PageBuffer's writer/DEFAULT_MAX_PENDING_WRITES). Lower this
    // for a small page_size on a memory-constrained machine; the default is
    // tuned for the normal (16 KiB) page size.
    pub fn create_with_limits<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
        max_pending_writes: usize,
    ) -> Result<Arc<Self>, StoreError> {
        let sf = Self::create_core_db(name.as_ref().to_string(), page_size, max_pending_writes)?;
        sf.create_system_tables()?;
        Ok(Arc::new(sf))
    }

    pub fn open_using<S: AsRef<str>>(
        name: S,
        file: F,
        undo_file: F,
        redo_file: F,
    ) -> Result<Arc<Self>, StoreError> {
        Self::open_using_with_limits(name, file, undo_file, redo_file, DEFAULT_MAX_PENDING_WRITES)
    }

    // Like open_using, but also controls the writer thread's pending-write
    // cap — see create_with_limits.
    pub fn open_using_with_limits<S: AsRef<str>>(
        name: S,
        file: F,
        undo_file: F,
        redo_file: F,
        max_pending_writes: usize,
    ) -> Result<Arc<Self>, StoreError> {
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
            max_pending_writes,
        )?;
        let sf = Self {
            last_checkpoint: AtomicU128::new(header.last_checkpoint),
            page_count,
            header,
            file,
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
        sf.load_logs()?;
        Ok(Arc::new(sf))
    }

    pub fn get_generator(self: &Arc<Self>) -> Arc<Generator> {
        self.generator.clone()
    }

    pub fn open<S: AsRef<str>>(name: S) -> Result<Arc<Self>, StoreError> {
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
    // Takes Arc<Self> (not a bare owned Db) because every constructor now
    // hands out Arc<Db<F>> — see the type's own doc comment. Closing still
    // needs unique ownership (to tear down the buffer/logger threads and
    // hand the underlying files back), so this unwraps the Arc first: it
    // errors instead of panicking if some other clone (e.g. a TableCursor,
    // or another thread) is still holding a reference.
    pub fn close(self: Arc<Self>) -> Result<(F, F, F), StoreError> {
        let db = Arc::try_unwrap(self).map_err(|_| {
            StoreError::UnknownError(
                "Db::close: other Arc<Db> references still exist (e.g. a live TableCursor or \
                 another thread) — drop them before closing"
                    .into(),
            )
        })?;
        // Revert any still-aborting transactions before persisting: the aborting
        // set is in-memory only, so an un-reverted aborted row would reappear as
        // committed after reopen (its txn no longer being in any set).
        db.drain_aborting();
        db.write_system_tables()?;
        let mut hdr = (*db.header).clone();
        hdr.page_count = db.page_count();
        let ts = timestamp();
        hdr.last_checkpoint = ts;
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
        } = db;
        drop(tables);
        let buffer = Arc::into_inner(buffer).unwrap();
        buffer.write_header(hdr)?;
        buffer.shutdown()?;
        // Unwrapping here as the expectation is there is only this thread accessing logger
        let logger = Arc::into_inner(logger).unwrap();
        // A clean close is, by definition, a point where everything is
        // durable (buffer.shutdown() just flushed every remaining pending
        // page write) — exactly the condition checkpoint() truncates the
        // logs under. Without this, close()+reopen never truncates at all
        // (only an explicit checkpoint() does), so every such cycle
        // accumulates the *entire* history of redo/undo records instead of
        // just what's new — replay then has to reprocess everything from
        // the very first write on every single reopen. That's wasteful on
        // its own, but for Mod operations specifically it's also
        // incorrect: replaying superseded intermediate updates (not just
        // the latest one) repeatedly tears down and rebuilds the
        // overflow-page chain for content that never needed to change,
        // and each such rebuild can land on a different set of pages than
        // the original run did — confirmed via
        // test_free_pages_do_not_accumulate_across_multiple_close_reopen_cycles.
        logger.checkpoint(ts)?;
        logger.shutdown()?;
        Ok((file, undo_file, redo_file))
    }

    pub fn page_count(&self) -> DBSizeType {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        // Must happen before the log truncate below, and really before
        // anything else here: write_system_tables() persists the
        // generator's current sequences (including the transaction-id
        // one) to GENERATOR_TABLE_PAGE. That's the *only* place this state
        // is durable — close() is the only other caller — so without this,
        // a reopen after checkpoint-but-no-close restores a stale
        // transaction-id sequence (whatever it was at table creation) and
        // can mint a "new" transaction that numerically collides with an
        // old, already-committed one (see TransactionId's PartialEq/Hash
        // and TransactionManager::advance_past). Called first so its own
        // page write is queued before, and thus flushed synchronously by,
        // buffer.checkpoint() below rather than left pending.
        self.write_system_tables()?;
        self.buffer.checkpoint()?;
        let mut hdr = (*self.header).clone();
        hdr.page_count = self.page_count();
        let ts = timestamp();
        hdr.last_checkpoint = ts;
        self.buffer.write_header(hdr)?;
        self.logger.checkpoint(ts)?;
        self.last_checkpoint
            .store(ts, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn load_logs(&self) -> Result<(usize, usize), StoreError> {
        let (mut redo_count, mut undo_count) = (0, 0);
        // mmap-ing a zero-length file errors ("memory map must have a
        // non-zero length") rather than yielding an empty mapping — and a
        // brand-new database's redo/undo files are exactly that (0 bytes,
        // nothing ever written yet), so this isn't an edge case, it's the
        // state of every single fresh Db::create::<File>() call. Skip the
        // mmap entirely when there's nothing to read.
        if let Some(redo_file) = self.redo_file.as_any().downcast_ref::<File>()
            && redo_file.metadata()?.len() > 0
        {
            let map = unsafe { MmapOptions::new().map(redo_file)? };
            let buf = &map[..];
            redo_count = self.process_redo(buf)?;
        }
        if let Some(undo_file) = self.undo_file.as_any().downcast_ref::<File>()
            && undo_file.metadata()?.len() > 0
        {
            let map = unsafe { MmapOptions::new().map(undo_file)? };
            let buf = &map[..];
            undo_count = self.process_undo(buf)?;
        }
        if let Some(redo_file) = self.redo_file.as_any().downcast_ref::<MemFile>() {
            let buf = &redo_file.data()[..];
            redo_count = self.process_redo(buf)?;
        }
        if let Some(undo_file) = self.undo_file.as_any().downcast_ref::<MemFile>() {
            let buf = &undo_file.data()[..];
            undo_count = self.process_undo(buf)?;
        }

        println!("{} redo found, {} undo found", redo_count, undo_count);

        Ok((redo_count, undo_count))
    }

    fn process_redo(&self, buffer: &[u8]) -> Result<usize, StoreError> {
        let mut count = 0;
        let mut buf = buffer;
        let (mut committed, mut inprogress, mut rollback) =
            (HashSet::new(), HashSet::new(), HashSet::new());
        let mut lsn_id = None;
        while !buf.is_empty() {
            let (redo, remaining) = take_from_bytes::<MsgType>(buf)?;
            buf = remaining;
            match redo {
                MsgType::Redo(redo) => {
                    let op = redo.operation;
                    lsn_id = Some(redo.lsn_id);
                    match op {
                        Operation::Add(t, _r) | Operation::Mod(t, _r) | Operation::Del(t, _r) => {
                            inprogress.insert(t);
                        }
                        Operation::Commit(t, _ts) => {
                            committed.insert(t);
                        }
                        Operation::Rollback(t, _ts) => {
                            rollback.insert(t);
                        }
                    }
                }
                _ => {
                    panic!("Unknown message in redo log: {:?}", redo)
                }
            }
            count += 1;
        }
        let mut buf = buffer;
        while !buf.is_empty() {
            let (redo, remaining) = take_from_bytes(buf)?;
            buf = remaining;
            if let MsgType::Redo(redo) = redo {
                let op = redo.operation;
                match op {
                    Operation::Add(t, r) if committed.contains(&t) => {
                        self.table_by_id(r.table_id)?
                            .insert_if_needed(&r.tuple, t)?;
                    }
                    Operation::Mod(t, r) if committed.contains(&t) => {
                        self.table_by_id(r.table_id)?.update_if_needed(r.tuple)?;
                    }
                    Operation::Del(t, r) if committed.contains(&t) => {
                        self.table_by_id(r.table_id)?.remove(r.tuple.id)?;
                    }
                    _ => {}
                }
            };
        }
        if let Some(lsn_id) = lsn_id {
            self.logger.clock().mark_written(lsn_id);
            // See LsnClock::advance_counter_past's own doc comment: without
            // this, the counter restarts at 0 on reopen, and the first new
            // write's redo record landing regresses the watermark this just
            // set right back down.
            self.logger.clock().advance_counter_past(lsn_id);
        }
        Ok(count)
    }

    fn process_undo(&self, buffer: &[u8]) -> Result<usize, StoreError> {
        let mut count = 0;
        let mut buf = buffer;
        let (mut committed, mut inprogress, mut rollback) =
            (HashSet::new(), HashSet::new(), HashSet::new());
        let mut operations = HashMap::new();
        while !buf.is_empty() {
            let (redo, remaining) = take_from_bytes::<MsgType>(buf)?;
            buf = remaining;
            match redo {
                MsgType::Undo(undo) => {
                    let op = undo.operation;
                    match &op {
                        Operation::Add(t, _r) | Operation::Mod(t, _r) | Operation::Del(t, _r) => {
                            inprogress.insert(t.clone());
                            operations
                                .entry(t.clone())
                                .and_modify(|v: &mut Vec<_>| v.push(op.clone()))
                                .or_insert(vec![op.clone()]);
                        }
                        Operation::Commit(t, _ts) => {
                            committed.insert(t.clone());
                        }
                        Operation::Rollback(t, _ts) => {
                            rollback.insert(t.clone());
                        }
                    }
                }
                _ => {
                    panic!("Unknown message in redo log: {:?}", redo)
                }
            }
            count += 1;
        }
        operations.retain(|k, _v| !committed.contains(k));
        operations
            .iter()
            .try_for_each(|(id, ops)| self.revert_undo_ops(ops, id))?;
        Ok(count)
    }

    pub fn begin(&self) -> Result<Transaction, StoreError> {
        // Reclaim any transactions abandoned via Transaction::drop (parked in
        // `aborting`) before starting new work: they are already invisible, this
        // physically reverts them so their rows don't linger and a re-insert of
        // the same key finds it free. The reverts are conditional (see
        // revert_txn_writes), so this is safe even if another thread is making
        // forward progress on the same keys.
        self.drain_aborting();
        // Same opportunistic-cleanup pattern for committed transactions whose
        // undo trail commit() had to defer (see Logger::discard_or_defer_undo)
        // because some reader's snapshot might still have needed it. Cheap
        // no-op when nothing is pending.
        self.logger
            .drain_ready_undo_discards(&self.tx_mgr.get_active_transactions()?);
        if self.redo_file.get_metadata()?.len > 16 * 1024 * 1024
            || self.undo_file.get_metadata()?.len > 16 * 1024 * 1024
        {
            self.checkpoint()?;
        }
        self.tx_mgr.begin()
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
        // Decide whether id's undo trail can be dropped now or must wait for
        // every currently-active transaction that might have it in its own
        // snapshot to finish first — see Logger::discard_or_defer_undo. Must
        // capture the active set (excluding id itself, which is about to
        // leave it on the very next line) before tx_mgr.commit changes it.
        let mut still_active = self.tx_mgr.get_active_transactions()?;
        still_active.remove(&id);
        self.logger.discard_or_defer_undo(id.clone(), still_active);
        self.tx_mgr.commit(id.clone())?;

        // Best-effort tombstone reclamation. Errors here do not un-commit the
        // transaction: find() already treats a committed tombstone as absent,
        // so a caller never sees the removed row regardless of whether this
        // physical cleanup below ever runs.
        //
        // But it's not purely cosmetic either: tombstoning (Db::remove) only
        // flips a flag via table.update — it never touches the index — so
        // until this reclaim actually completes, the index entry is still
        // live and a future insert of the same key hits a real, permanent
        // DuplicateKey (there is no other pass that ever revisits this row).
        // find() and remove() must therefore be retried TOGETHER as one unit,
        // not just remove() alone: table.find() below walks the index/data
        // pages the same way any other read does and can itself return
        // LockContentionError under write contention. The old code let that
        // specific failure through an `if let Ok(...)` unchecked, silently
        // skipping the reclaim entirely on the very first contention hit on
        // find() — permanently orphaning the index entry, since there's no
        // later pass to retry it. Retrying the whole find+remove sequence is
        // safe to repeat: find is a pure read, and remove (since the earlier
        // fix) tolerates the data already being gone from a prior attempt.
        #[allow(clippy::unnecessary_map_or, clippy::collapsible_if)]
        for r in del_records {
            if let Ok(table) = self.table_by_id(r.table_id) {
                let _ = retry_on_contention(|| {
                    if let Some(tuple) = table.find(r.tuple.id.clone())? {
                        if tuple.is_tombstoned() && tuple.is_same_txn(id.clone()) {
                            table.remove(tuple.id.clone())?;
                        }
                    }
                    Ok(())
                });
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
        self.revert_undo_ops(&ops, id)
    }

    fn revert_undo_ops(&self, ops: &Vec<Operation>, id: &TransactionId) -> Result<(), StoreError> {
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

    pub(crate) fn get_last_checkpoint(&self) -> u128 {
        self.last_checkpoint
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.tables
            .read()
            .get(&id)
            .map(Arc::clone)
            .ok_or(StoreError::TableNotFound(id.to_string()))
    }

    pub fn table_id_by_name<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Option<TableIdType>, StoreError> {
        Ok(self
            .tables
            .read()
            .values()
            .find(|t| t.table.name == name.as_ref())
            .map(|t| t.id()))
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
        let page_id = self.table_by_id(id)?.insert(tuple.clone(), tx_id.clone())?;
        let op = Operation::Add(tx_id, Record::new(id, tuple, Some(page_id)));
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
        let txn_id = txn.id();
        let table = self.table_by_id(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            let visible = self
                .find_visible_to(&tuple, &txn_id)
                .map(|t| t.into_owned());
            // A committed tombstone means the key was removed — it must be
            // invisible even if its physical row hasn't been reclaimed yet.
            // (commit reclaims tombstones best-effort AFTER its commit point, so
            // a committed-but-not-yet-reclaimed tombstone can legitimately still
            // be present in the tree.)
            match visible {
                Some(t) if t.is_tombstoned() => Ok(None),
                other => Ok(other),
            }
        } else {
            Ok(None)
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
            let redo_op = Operation::Mod(txn.clone(), Record::new(tid, updated.clone(), None));
            let undo_op = Operation::Mod(txn.clone(), Record::new(tid, old_tuple, None));
            self.logger.log_redo(redo_op)?;
            self.logger.log_undo(undo_op)?;
            table.update(updated)?;
            Ok(())
        } else {
            Err(StoreError::KeyNotFound(new_tuple.id))
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
            let redo_op = Operation::Del(txn.clone(), Record::new(tid, tombstoned.clone(), None));
            let undo_op = Operation::Del(txn.clone(), Record::new(tid, old_tuple, None));
            self.logger.log_redo(redo_op)?;
            self.logger.log_undo(undo_op)?;
            table.update(tombstoned.clone())?;
            Ok(tombstoned)
        } else {
            Err(StoreError::KeyNotFound(id))
        }
    }

    // Takes &Arc<Self>, not &self: the returned cursor holds its own
    // Arc<Db<F>> clone (it needs to call find_last_committed per row to
    // resolve MVCC visibility as it scans), which only a caller already
    // holding the Db as Arc<Db<F>> can provide.
    pub fn table_scan(self: &Arc<Self>, tid: TableIdType) -> Result<TableCursor<F>, StoreError> {
        TableCursor::new(Arc::clone(self), tid, None)
    }

    pub fn range_scan(
        self: &Arc<Self>,
        tid: TableIdType,
        start: DBIdType,
        end: DBIdType,
    ) -> Result<RangeCursor<F>, StoreError> {
        RangeCursor::new(Arc::clone(self), tid, None, start, end)
    }

    // Shared undo-chain walk: returns the first version (this tuple, or an
    // ancestor reached by walking the undo chain backward) for which
    // `is_visible` holds. `find_last_committed` and `find_visible_to` are
    // both this walk with a different visibility predicate — the former
    // ("is anybody's write here durable/committed") for write paths that
    // must act on the true latest state, the latter ("is this write visible
    // to *this specific reader's* snapshot") for read paths.
    fn resolve_visible<'a>(
        &self,
        tuple: &'a Tuple,
        is_visible: impl Fn(&TransactionId) -> bool,
    ) -> Option<Cow<'a, Tuple>> {
        if let Some(txn) = tuple.txn_id.clone() {
            if is_visible(&txn) {
                Some(Cow::Borrowed(tuple))
            } else {
                let mut tuple = tuple.clone();
                let mut txn = txn;
                loop {
                    tuple.undo_id?;
                    let undo_id = tuple.undo_id.unwrap();

                    // Tolerate a missing undo record: an aborting txn's undo can
                    // be discarded concurrently once its rows are reverted. If we
                    // can't walk further, treat the row as not-yet-visible
                    // (invisible) rather than panicking.
                    let next_tuple = self.logger.find_undo_tuple(txn.clone(), undo_id)?;
                    let next_txn = next_tuple.txn_id.clone()?;
                    if is_visible(&next_txn) {
                        // next_tuple is the visible ancestor we walked back
                        // to — return it, not the in-flight `tuple` we started
                        // from (which belongs to a not-yet-visible txn and must
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

    // Latest committed version, full stop — no snapshot filtering. Used
    // internally by update()/remove() to resolve the current pre-image for
    // undo-log construction: write paths must act against the true latest
    // committed state (conflicting writers are already serialized via
    // page-level locks — see ArcLock — so there's no lost-update risk to
    // guard against here), not a reader's potentially-stale snapshot.
    pub(crate) fn find_last_committed<'a>(&self, tuple: &'a Tuple) -> Option<Cow<'a, Tuple>> {
        // Visible iff the writer COMMITTED — i.e. it is neither still active
        // nor aborting-with-unreverted-writes. A dropped/aborted txn stays
        // in `aborting` and is therefore correctly invisible here even
        // though it has left the active set.
        self.resolve_visible(tuple, |txn| self.tx_mgr.is_committed(txn))
    }

    // Snapshot-isolated visibility for reads (Db::find, TableCursor,
    // RangeCursor): a version is visible to `reader` only if its writer
    // committed strictly *before* `reader` began — not just "is committed
    // right now". A writer that was still active when `reader` began (in
    // `reader`'s captured snapshot set) stays invisible for `reader`'s
    // entire lifetime even once it commits, and a writer that didn't even
    // exist yet when `reader` began is excluded the same way, since
    // `reader`'s snapshot — captured once, at begin() — couldn't have
    // recorded it either way. Together these two checks are what makes a
    // transaction's reads internally consistent (repeatable read):
    // re-reading the same row twice within one transaction can no longer
    // observe a concurrent commit that landed in between.
    //
    // This was previously dead scaffolding: TransactionManager captured a
    // snapshot at every begin() and exposed it via
    // TransactionManager::snapshot(), but nothing in the read path ever
    // consulted it — every read used find_last_committed's live, "as of
    // right now" check instead, regardless of when the reading transaction
    // itself began.
    //
    // Making this actually hold also needed Logger::discard_or_defer_undo:
    // without it, a commit discarded its entire undo trail immediately, so
    // the pre-image needed to keep honoring an older reader's snapshot was
    // gone by the time this function went looking for it — see the
    // fallback below, which is now a rare defensive backstop for a narrow
    // race (a new reader beginning in between this commit's active-set
    // snapshot and the commit actually taking effect) rather than the
    // common path.
    pub(crate) fn find_visible_to<'a>(
        &self,
        tuple: &'a Tuple,
        reader: &TransactionId,
    ) -> Option<Cow<'a, Tuple>> {
        // Cloned once up front rather than held as a lock guard for the
        // whole (potentially multi-hop) undo-chain walk below. Full
        // TransactionId (id + ts), not just the numeric id — see
        // TransactionInner's own PartialEq comment on why the numeric id
        // alone isn't a safe identity across a reopen.
        let reader_snapshot: HashSet<TransactionId> = self
            .tx_mgr
            .snapshot(reader)
            .map(|s| s.clone())
            .unwrap_or_default();
        let reader_ts = reader.ts();
        if let Some(t) = self.resolve_visible(tuple, |txn| {
            self.tx_mgr.is_committed(txn) && txn.ts() < reader_ts && !reader_snapshot.contains(txn)
        }) {
            return Some(t);
        }
        // No version satisfying `reader`'s exact snapshot survives in the
        // retained undo history — fall back to the latest committed
        // version instead of hiding a row that genuinely, currently
        // exists. In the common case discard_or_defer_undo already keeps
        // the needed pre-image around for exactly as long as `reader`
        // could still be asking, so this path is rarely taken — but it can
        // still be reached by a narrow race: Db::commit captures its
        // "who's still active" waiter set with a plain (non-atomic, w.r.t.
        // TransactionManager's own locks) read before actually committing,
        // so a brand new reader beginning in that exact window wouldn't be
        // counted as a waiter, and could see its undo trail discarded
        // immediately if no one else was active at that moment. Confirmed
        // the hard way that this fallback matters: an earlier version of
        // this function returned None here entirely (before
        // discard_or_defer_undo existed), which made Db::find incorrectly
        // report a real, committed, currently-existing row as missing after
        // a concurrent commit. The one guarantee that must hold
        // unconditionally, even in that race window: a row that exists and
        // is committed is never reported as absent.
        self.find_last_committed(tuple)
    }

    /// Convenience wrapper around `create_table_with_index_entry_size` using
    /// `crate::tables::bplustree::MAX_ENTRY_BYTES` as the index entry size —
    /// sized for a plain `Int`/`Vec` key (see that constant's own comment).
    /// If your table's primary key is a composite `Rec(IndexKey)` with
    /// `Str`/`Blob` fields, this default is very likely too small: call
    /// `create_table_with_index_entry_size` directly with a size you've
    /// computed for your actual key shape instead of hoping this guess
    /// covers it.
    pub fn create_table(&self, name: String) -> Result<TableIdType, StoreError> {
        self.create_table_with_index_entry_size(name, bplustree::MAX_ENTRY_BYTES)
    }

    /// Like `create_table`, but takes the fixed per-entry byte budget for
    /// this table's index pages explicitly instead of assuming a default —
    /// see `BPlusTree::new`'s own doc comment for what this bounds and how
    /// to size it for a given key shape.
    pub fn create_table_with_index_entry_size(
        &self,
        name: String,
        index_entry_size: DBSizeType,
    ) -> Result<TableIdType, StoreError> {
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
                index_entry_size,
            )?;
            let id = table.id();
            tables.insert(id, Arc::new(table));
            id
        };
        self.write_system_tables()?;
        Ok(table_id)
    }

    /// Drops a table and reclaims every page it owns (its whole index tree
    /// and its whole data chain, including any overflow continuation
    /// pages) back to the free list, so later `create_table` calls can
    /// reuse that space instead of the file only ever growing.
    ///
    /// Not transactional, like `create_table`/`create_table_with_index_
    /// entry_size` themselves — this is DDL, not a row-level operation
    /// tracked by a `Transaction`'s undo log, so there's no `&Transaction`
    /// parameter and nothing here can be rolled back once called.
    ///
    /// Not safe against a concurrent operation already in flight against
    /// this table: this only removes it from the name/id registry and
    /// frees its pages — anything that already holds its own reference
    /// (obtained before this call) keeps working against pages that are
    /// now back on the free list and may be reused underneath it. Callers
    /// are responsible for knowing nothing else is using the table (e.g.
    /// squeal-sql's own use — cleaning up a table's own just-created,
    /// not-yet-published indices after a failed CREATE TABLE — holds,
    /// since nothing else can have discovered them yet).
    pub fn drop_table<S: AsRef<str>>(&self, name: S) -> Result<(), StoreError> {
        let name = name.as_ref();
        let table = {
            let mut tables = self.tables.write();
            let id = tables
                .values()
                .find(|t| t.table.name == name)
                .map(|t| t.id())
                .ok_or_else(|| StoreError::TableNotFound(name.to_string()))?;
            // Always Some: id was just read from this same map under the
            // same held write lock.
            tables.remove(&id).unwrap()
        };
        for page_id in table.all_index_page_ids()? {
            let record_size = self.buffer.get_page(page_id)?.record_size();
            self.buffer.reset_and_free_page(page_id, record_size)?;
        }
        self.buffer.free_page_chain(table.table.first_data_page)?;
        self.generator.remove_generator(name)?;
        self.write_system_tables()?;
        Ok(())
    }

    fn setup_needed_modules(
        header: Arc<Header>,
        gens: Arc<Generator>,
        page_counter: Arc<AtomicU64>,
        file: F,
        undo_file: F,
        redo_file: F,
        max_pending_writes: usize,
    ) -> Result<NeededObjects<F>, StoreError> {
        let mut logger = Logger::new();
        logger.set_db(undo_file, redo_file)?;
        // Buffer shares the logger's WAL clock, so page-flush deferral and redo
        // LSNs are scoped to this one database (not a process global).
        let clock = logger.clock();
        let buffer = Arc::new(PageBuffer::new(
            header.page_size,
            page_counter,
            file,
            header,
            1024,
            clock,
            max_pending_writes,
        )?);
        let nm = NeededObjects {
            buffer,
            logger: Arc::new(logger),
            txn_mgr: Arc::new(TransactionManager::new(gens, TransactionId::default())?),
        };
        Ok(nm)
    }

    fn create_core_db(
        name: String,
        page_size: DBSizeType,
        max_pending_writes: usize,
    ) -> Result<Self, StoreError> {
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
            last_checkpoint: timestamp(),
        };
        let bytes = to_allocvec(&header)?;
        f.write_all(&bytes)?;
        let header = Arc::new(header);
        let gens = Generator::new();
        gens.create_generator(SYSTEM_TABLE_NAME, None)?;
        let gens = Arc::new(gens);
        let page_count = Arc::new(AtomicU64::new(0));
        let nm = Self::setup_needed_modules(
            header.clone(),
            gens.clone(),
            page_count.clone(),
            f.do_clone()?,
            undo_file.do_clone()?,
            redo_file.do_clone()?,
            max_pending_writes,
        )?;

        Ok(Self {
            last_checkpoint: AtomicU128::new(header.last_checkpoint),
            name,
            header,
            file: f,
            undo_file,
            redo_file,
            page_count,
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
        let page = Page::new_pinned(self.header.page_size);
        let tables = self.tables.read();
        for (i, t) in tables.values().enumerate() {
            let bytes = to_allocvec(&t.table)?;
            // We dont care what the tables id is or if it is consistent across saves.
            page.add_tuple(Tuple::new(i as DBSizeType, &bytes))?;
        }
        self.buffer.write_page(0usize.into(), &page)?;
        let gens = self.generator.get_values()?;
        let page = Page::new_pinned(self.header.page_size);
        page.add_tuple(Tuple::new(0, &to_allocvec(&gens)?))?;
        self.buffer.write_page(1usize.into(), &page)?;
        let page = Page::new_pinned(self.header.page_size);
        page.add_tuple(Tuple::new(0, &to_allocvec(&self.buffer.get_free_pages())?))?;
        self.buffer.write_page(2usize.into(), &page)?;

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
        let page = self.buffer.get_page(FREE_PAGE_TABLE_PAGE.into())?;
        let tuple = page.get(DBIdType::Int(0))?.unwrap_or_default();
        let free_pages = from_bytes::<Vec<_>>(&tuple.data)?;
        self.buffer.set_free_pages(free_pages);
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
/// `Db::commit`/`Db::rollback`'s per-record cleanup loops (those calls race
/// against the same per-page locks every other concurrent operation uses, and
/// under load a single transient lock timeout shouldn't abort the whole
/// commit/rollback — see `Transaction::into_id`) and by
/// `BPlusTree::insert`'s failure-cleanup path (undoing a data-page write after
/// a failed index insert must not itself be allowed to fail from ordinary
/// contention — that would leave the write permanently orphaned instead of
/// rolled back).
pub(crate) fn retry_on_contention<T>(
    mut f: impl FnMut() -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let mut attempt = 0u32;
    loop {
        match f() {
            // 16 attempts with a 300us/attempt linear backoff (was 8 at
            // 100us/attempt): each attempt already spends up to 5ms inside
            // the page lock's own wait (see PageBuffer::get_page_mut), so the
            // old budget's ~3.6ms of total backoff was negligible next to
            // realistic OS scheduling jitter under real load — a lock
            // holder preempted mid-critical-section for even one scheduling
            // quantum could exhaust every retry here despite never being
            // near a genuine deadlock. Confirmed as a real (not just
            // theoretical) contributor to a hard-to-reproduce flake in
            // Db::commit's tombstone reclaim.
            Err(StoreError::LockContentionError) if attempt < 16 => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_micros(300 * attempt as u64));
            }
            other => return other,
        }
    }
}

pub(crate) fn db_hash(bytes: &[u8]) -> u64 {
    let mut h = 0x811C9DC5;
    for b in bytes {
        h ^= *b as u64;
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
    use std::{sync::Arc, thread, time::Duration};

    use crate::{
        cursor::Cursor,
        db::{DEFAULT_PAGE_SIZE, Db, FileDB, Opener, ZERO_PAGE_SIZE, db_hash},
        error::StoreError,
        logger::MsgType,
        memfile::MemFile,
        table::TableIdType,
        tuple::{DBIdType, Tuple},
    };
    use postcard::take_from_bytes;
    use std::fs::File;
    type TestDB = Db<MemFile>;

    fn make_db_with_table() -> (Arc<TestDB>, TableIdType) {
        let db = TestDB::create("txn_test.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        (db, tid)
    }

    // Simulates a crash: direct clones of the live db's file handles
    // (MemFile::do_clone shares the underlying buffer), without going
    // through close(). close() is a clean shutdown — it flushes
    // everything and truncates the redo/undo logs, since a cleanly-closed
    // db has nothing left that needs replaying — so it can never be used
    // to test replay itself. This is what actually leaves whatever's
    // durable so far sitting in the (unclosed, untruncated) logs, exactly
    // like an abrupt process stop would.
    fn crash_clone(db: &TestDB) -> (MemFile, MemFile, MemFile) {
        (
            db.file.do_clone().unwrap(),
            db.undo_file.do_clone().unwrap(),
            db.redo_file.do_clone().unwrap(),
        )
    }

    // Passive record count (unlike Db::load_logs, which actually replays):
    // just walks MsgType entries off the raw bytes.
    fn count_log_records(file: &MemFile) -> usize {
        let data = file.data();
        let mut buf = &data[..];
        let mut count = 0;
        while !buf.is_empty() {
            let (_msg, remaining) = take_from_bytes::<MsgType>(buf).unwrap();
            buf = remaining;
            count += 1;
        }
        count
    }

    // log_redo/log_undo's send() over a bounded(1) channel only guarantees
    // the *previous* message has been dequeued by the writer thread, not
    // that it (or the message just sent) has actually been written to the
    // file yet — under contention (e.g. many tests running in parallel)
    // that write can lag behind the point where a test's last commit()
    // call returns. crash_clone must not race that: it takes a raw,
    // point-in-time clone, so a snapshot taken too early silently omits
    // the last record(s), which then corrupts replay (e.g. a Commit
    // marker missing from the undo log makes an already-committed
    // transaction look abandoned). Poll for the expected counts instead of
    // assuming synchronous delivery.
    fn wait_for_durable_logs(db: &TestDB, expected_redo: usize, expected_undo: usize) {
        for _ in 0..1000 {
            if count_log_records(&db.redo_file) == expected_redo
                && count_log_records(&db.undo_file) == expected_undo
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("timed out waiting for {expected_redo} redo / {expected_undo} undo records to land");
    }

    // The file header's page_count is only ever (re)written to disk by an
    // explicit write_header call — table creation and ordinary inserts bump
    // the *live*, in-process page_count atomic but never touch the on-disk
    // header. Db::open_using seeds ITS OWN live page_count purely from
    // whatever the header bytes say at open time, so a crash_clone snapshot
    // whose header is stale (e.g. from priming with a checkpoint at the
    // *start* of a test, before the test's own operations allocated more
    // pages) makes the reopened db under-count how many pages actually
    // exist. Replay's own page allocations (insert_if_needed/update_if_needed
    // falling back to a real insert/update) then start from that stale,
    // too-low count and can hand out a page id that collides with one the
    // original session already validly used for a *different* row —
    // silently clobbering it. Symptom: a replay test's assertions fail on a
    // seemingly random row, varying nondeterministically run to run.
    //
    // Fix: sync the header — with the CURRENT, live page_count — immediately
    // before crash_clone, not once at the start. Uses PageBuffer::checkpoint
    // (not Db::checkpoint) specifically so it does NOT touch
    // Logger::checkpoint, which would truncate the redo/undo logs these
    // tests need intact. write_header is itself fire-and-forget, but
    // buffer.checkpoint()'s own synchronous reply is queued strictly after
    // it on the same channel (FIFO) — waiting for that reply guarantees the
    // header write already landed, the same trick
    // test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file
    // uses for its own double-checkpoint.
    fn sync_header_without_truncating_logs<F>(db: &Db<F>)
    where
        F: crate::db::DBFile<Item = F> + 'static,
    {
        let mut hdr = (*db.header).clone();
        hdr.page_count = db.page_count();
        db.buffer.write_header(hdr).unwrap();
        db.buffer.checkpoint().unwrap();
    }

    fn make_db_with_two_tables() -> (Arc<TestDB>, TableIdType, TableIdType) {
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
        let db = TestDB::create(DB_NAME);
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
        let db = TestDB::create(DB_NAME).unwrap();
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
        let db = TestDB::create(DB_NAME);
        assert!(db.is_ok());
        let db = db.unwrap();
        let r = db.create_table("table_1".to_string());
        assert!(r.is_ok());
        assert_eq!(db.get_tables().unwrap().len(), 1);
        let (f, u, r) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, u, r).unwrap();
        let t = db.get_tables().unwrap();
        assert!(t.len() == 1);
        assert_eq!(t[0].name, "table_1");
        let r = db.create_table("table_1".to_string());
        assert!(matches!(r, Err(StoreError::DuplicateName(_))));
        //FileDB::delete(DB_NAME).unwrap_or_default()
    }

    // create_table's own index_entry_size is bplustree::MAX_ENTRY_BYTES —
    // this is a plain regression test that the convenience wrapper still
    // behaves exactly as it did before create_table_with_index_entry_size
    // existed (a plain Int key round-trips through insert/find normally).
    #[test]
    fn test_create_table_default_index_entry_size_round_trips_normal_rows() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"hello"
        );
    }

    // The concrete motivation for create_table_with_index_entry_size: a
    // composite Rec(IndexKey) key with Str fields easily exceeds
    // bplustree::MAX_ENTRY_BYTES (64, sized for a plain Int key) —
    // create_table's default fails at insert time with a TupleTooLarge
    // error that doesn't help you fix it up front. Sizing the table
    // explicitly avoids that failure entirely.
    #[test]
    fn test_create_table_with_index_entry_size_supports_a_composite_key_default_size_cannot() {
        use crate::valueitem::{IndexKey, ValueItem};

        let db = TestDB::create("index_entry_size_test.db").unwrap();
        let big_key = || -> DBIdType {
            DBIdType::Rec(
                IndexKey::new_from(&[
                    ValueItem::Str(("alpha".repeat(9), 50)),
                    ValueItem::Str(("beta".repeat(9), 50)),
                ])
                .unwrap(),
            )
        };

        // Default-sized table: fails, doesn't silently corrupt anything.
        let default_tid = db.create_table("default_sized".to_string()).unwrap();
        let t = db.begin().unwrap();
        let err = db
            .insert(
                default_tid,
                Tuple::new_with(big_key(), b"v", None, None),
                &t,
            )
            .unwrap_err();
        assert!(
            matches!(err, StoreError::TupleTooLarge(_, _)),
            "got {err:?}"
        );
        drop(t);

        // Deliberately sized table: the identical key/row round-trips.
        let sized_tid = db
            .create_table_with_index_entry_size("deliberately_sized".to_string(), 300)
            .unwrap();
        let t = db.begin().unwrap();
        let key = big_key();
        db.insert(
            sized_tid,
            Tuple::new_with(key.clone(), b"v", None, None),
            &t,
        )
        .unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(sized_tid, key, &t).unwrap().unwrap().data.to_vec(),
            b"v"
        );
    }

    // ── drop_table ──────────────────────────────────────────────────────────

    #[test]
    fn test_drop_table_removes_it_from_the_table_list() {
        let (db, tid) = make_db_with_table();
        assert_eq!(db.get_tables().unwrap().len(), 1);
        db.drop_table("rows").unwrap();
        assert_eq!(db.get_tables().unwrap().len(), 0);
        assert_eq!(db.table_id_by_name("rows").unwrap(), None);
        // The dropped id must actually be gone, not just unnamed — using it
        // (with a caller-held stale TableIdType, exactly the "don't do this
        // concurrently" case drop_table's own doc comment calls out) must
        // fail cleanly, not panic.
        let t = db.begin().unwrap();
        let err = db.find(tid, id(1), &t).unwrap_err();
        assert!(matches!(err, StoreError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_drop_table_missing_name_returns_table_not_found() {
        let (db, _tid) = make_db_with_table();
        let err = db.drop_table("does_not_exist").unwrap_err();
        assert!(matches!(err, StoreError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_drop_table_frees_the_name_for_reuse() {
        let (db, _tid) = make_db_with_table();
        db.drop_table("rows").unwrap();
        // Both the table registry (DuplicateName check) and the generator
        // (create_generator's own DuplicateName check) must have let go of
        // the name — creating "rows" again must succeed, not error, and
        // the new table must work normally.
        let new_tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        db.insert(new_tid, row(1, b"fresh"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(new_tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"fresh"
        );
    }

    #[test]
    fn test_drop_table_reuses_its_initial_pages_immediately() {
        let (db, _tid) = make_db_with_table();
        let page_count_before = db.page_count();
        db.drop_table("rows").unwrap();
        // A freshly created table's own initial 2 pages (first_index_page,
        // first_data_page) must come from what drop_table just freed
        // (alloc_page's free-list-pop path), not grow the file further.
        db.create_table("rows2".to_string()).unwrap();
        assert_eq!(
            db.page_count(),
            page_count_before,
            "a table created right after a drop must reuse its pages instead \
             of growing the file"
        );
    }

    #[test]
    fn test_drop_table_frees_every_page_a_multi_split_table_owned() {
        // Direct page-count accounting instead of "does a same-shaped table
        // fit back in the same space" (tried that first — it doesn't hold:
        // reusing pages off a LIFO free list can shift exactly where later
        // splits land, so an identical insert sequence into a fresh table
        // isn't guaranteed to produce an identical page count, only a
        // comparable one). What must hold precisely is that every page the
        // dropped table owned — not most of them — ends up on the free list.
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        for i in 0..500u64 {
            db.insert(tid, row(i, b"some reasonably sized payload"), &t)
                .unwrap();
        }
        db.commit(t).unwrap();
        assert_eq!(
            db.buffer.get_free_pages().len(),
            0,
            "nothing should be on the free list yet"
        );
        // The only pages that exist at this point are the 3 fixed system
        // pages (tables list / generators / free-page list, see
        // create_system_tables) plus everything "rows" itself allocated.
        let table_page_count = db.page_count() - 3;

        db.drop_table("rows").unwrap();

        assert_eq!(
            db.buffer.get_free_pages().len(),
            table_page_count as usize,
            "dropping the table must free every page it owned, not just some \
             of them"
        );
    }

    #[test]
    fn test_drop_table_reclaims_overflow_pages() {
        // Mirrors test_reused_freed_overflow_page_is_safe_to_write_fresh_data_into's
        // setup: a single row large enough to spill across an overflow
        // chain, not just its own primary data page. Same precise
        // free-list accounting as the multi-split test above — the point
        // here specifically is that the *overflow continuation* pages are
        // included in that count, not just the primary index/data pages.
        let (db, tid) = make_db_with_table();
        let page_sz = DEFAULT_PAGE_SIZE as usize;
        let big = vec![b'x'; 5 * page_sz];
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.buffer.get_free_pages().len(), 0);
        let table_page_count = db.page_count() - 3;

        db.drop_table("rows").unwrap();

        assert_eq!(
            db.buffer.get_free_pages().len(),
            table_page_count as usize,
            "dropping the table must free its overflow continuation pages \
             too, not just its primary index/data pages"
        );
    }

    #[test]
    fn test_drop_table_persists_across_close_and_reopen() {
        let db_name = temp_db_path("drop_table_persists");
        FileDB::delete(&db_name).unwrap_or_default();

        let db = FileDB::create(&db_name).unwrap();
        db.create_table("rows".to_string()).unwrap();
        db.drop_table("rows").unwrap();
        assert_eq!(db.get_tables().unwrap().len(), 0);
        let (f, u, r) = db.close().unwrap();

        let db2 = FileDB::open_using(&db_name, f, u, r).unwrap();
        assert_eq!(db2.get_tables().unwrap().len(), 0);
        assert_eq!(db2.table_id_by_name("rows").unwrap(), None);
        // The name must still be free after reopen too — proves the
        // generator removal itself persisted, not just the table list.
        db2.create_table("rows".to_string()).unwrap();

        FileDB::delete(&db_name).unwrap_or_default();
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
        assert_eq!(
            found.expect("row should be visible").data.to_vec(),
            b"hello"
        );
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
        assert_eq!(
            db.find(tid, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"A"
        );
        assert_eq!(
            db.find(tid, id(2), &txn2).unwrap().unwrap().data.to_vec(),
            b"B"
        );
        assert_eq!(
            db.find(tid, id(3), &txn2).unwrap().unwrap().data.to_vec(),
            b"C"
        );
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
        assert_eq!(
            found.expect("original must still be visible").data.to_vec(),
            b"v1"
        );

        db.commit(txn2).unwrap();
    }

    // Regression test for snapshot-isolation visibility (Db::find_visible_to)
    // AND deferred undo discard (Logger::discard_or_defer_undo /
    // drain_ready_undo_discards): a transaction's reads must stay internally
    // consistent even when a concurrent transaction updates AND COMMITS the
    // same row in between. Before find_visible_to existed, find()/
    // table_scan/range_scan all used find_last_committed, which decides
    // visibility live against whatever is committed "right now" — so a
    // transaction could see a different answer to the same read depending
    // purely on when, during its own lifetime, it happened to ask.
    // TransactionManager already captured a snapshot of active transactions
    // at begin() (and exposed it via TransactionManager::snapshot()) but
    // nothing consulted it — dead scaffolding for a feature that wasn't
    // actually wired up.
    //
    // Getting this right needed two parts, not one: find_visible_to alone
    // isn't enough, because Logger::log_undo used to discard a committing
    // transaction's ENTIRE undo trail unconditionally — so by the time a
    // still-open reader asked again, the pre-image needed to keep honoring
    // its snapshot was already gone (confirmed the hard way: an earlier
    // version of this fix made a real, committed row incorrectly look
    // missing after exactly this sequence). discard_or_defer_undo closes
    // that gap by mirroring TransactionManager's aborting/drain_aborting
    // pattern: a commit doesn't discard its undo trail if any other
    // transaction is still active (and might have it in its own snapshot)
    // — it parks the obligation and Db::begin's opportunistic drain finishes
    // the job once every such transaction has actually finished.
    #[test]
    fn test_find_is_repeatable_within_a_transaction_across_a_concurrent_write_and_commit() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();

        // Reader begins BEFORE the concurrent update below.
        let reader = db.begin().unwrap();
        let first_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must exist for the first read");
        assert_eq!(first_read.data.to_vec(), b"v1");

        // A fully separate, concurrent transaction updates but does NOT
        // commit yet.
        let writer = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &writer).unwrap();

        // The SAME reader, reading the SAME row again, must see the value it
        // saw the first time — not the concurrent, still-uncommitted write.
        let second_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must still be visible on the second read");
        assert_eq!(
            second_read.data.to_vec(),
            b"v1",
            "a transaction's own reads must stay consistent across a concurrent, uncommitted write"
        );

        // The writer now commits WHILE reader is still open — this is the
        // part that needs deferred undo discard: without it, the pre-image
        // "v1" would be gone by the next line.
        db.commit(writer).unwrap();

        let third_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must still be visible after the concurrent commit");
        assert_eq!(
            third_read.data.to_vec(),
            b"v1",
            "a transaction's reads must stay consistent even across a concurrent COMMIT, \
             not just a concurrent still-active write"
        );
        drop(reader);

        // A transaction that begins AFTER the writer committed must see the
        // new value — this isn't a permanently-stuck-in-the-past view, just
        // a per-transaction snapshot taken at begin() time.
        let later_reader = db.begin().unwrap();
        let later_read = db
            .find(tid, id(1), &later_reader)
            .unwrap()
            .expect("row must exist for a fresh transaction");
        assert_eq!(later_read.data.to_vec(), b"v2");
        drop(later_reader);
    }

    // Same guarantee, exercised through table_scan rather than a point
    // find() — the cursors resolve visibility the same way, so this
    // confirms the fix isn't find()-specific.
    #[test]
    fn test_table_scan_is_repeatable_within_a_transaction_across_a_concurrent_write_and_commit() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.insert(tid, row(2, b"v2"), &t).unwrap();
        db.commit(t).unwrap();

        // table_scan(None) begins its own internal Transaction, held for the
        // cursor's whole lifetime across every next() call — reading row 1
        // here starts that transaction's snapshot.
        let mut cursor = db.table_scan(tid).unwrap();
        let first = cursor.next().unwrap().expect("row 1 must be scanned");
        assert_eq!(first.data.to_vec(), b"v1");

        // A fully separate, concurrent transaction updates row 2 (not yet
        // reached by the scan) and commits, WHILE the cursor's transaction
        // is still open.
        let writer = db.begin().unwrap();
        db.update(tid, row(2, b"v2-updated"), &writer).unwrap();
        db.commit(writer).unwrap();

        // Continuing the SAME cursor (same underlying transaction) must
        // still see row 2's pre-commit value — the scan's snapshot was
        // taken when the cursor was created, not re-taken per row, and
        // deferred undo discard kept that pre-image reachable.
        let second = cursor.next().unwrap().expect("row 2 must be scanned");
        assert_eq!(
            second.data.to_vec(),
            b"v2",
            "a scan's own transaction must not observe a commit that landed after it began"
        );
        assert!(cursor.next().unwrap().is_none());
        drop(cursor);

        // A fresh scan (fresh transaction) must see the update — confirms
        // this isn't permanently stale, just snapshotted at begin() time.
        let mut cursor2 = db.table_scan(tid).unwrap();
        let row1_again = cursor2
            .next()
            .unwrap()
            .expect("row 1 must still be scanned");
        assert_eq!(row1_again.data.to_vec(), b"v1");
        let row2_again = cursor2
            .next()
            .unwrap()
            .expect("row 2 must still be scanned");
        assert_eq!(row2_again.data.to_vec(), b"v2-updated");
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
                .data
                .to_vec(),
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
        assert_eq!(
            db.find(tid, id(1), &txn3).unwrap().unwrap().data.to_vec(),
            b"A_v2"
        );
        assert_eq!(
            db.find(tid, id(2), &txn3).unwrap().unwrap().data.to_vec(),
            b"B"
        );
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
                found
                    .unwrap_or_else(|| panic!("row {i} missing"))
                    .data
                    .to_vec(),
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
        assert_eq!(
            found.expect("original row must still exist").data.to_vec(),
            b"v1"
        );
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
            found
                .expect("row must survive a rolled-back remove")
                .data
                .to_vec(),
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
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a1"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"b1"
        );
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
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a_v2"
        );
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
        assert_eq!(
            found
                .expect("large object must be visible after commit")
                .data
                .to_vec(),
            big
        );
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
        assert!(
            found.is_none(),
            "rolled-back large object must not be visible"
        );
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
        assert_eq!(
            found
                .expect("5-page object must survive commit")
                .data
                .to_vec(),
            big
        );
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
            found
                .expect("large object must survive close/reopen")
                .data
                .to_vec(),
            big
        );
    }

    #[test]
    fn test_multiple_large_objects_same_txn_commit() {
        let (db, tid) = make_db_with_table();
        // Three objects that collectively span many overflow pages
        let small = vec![b'E'; 20_000]; // 2 pages each
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &small), &txn).unwrap();
        db.insert(tid, row(2, &small), &txn).unwrap();
        db.insert(tid, row(3, &small), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        for i in 1u64..=3 {
            let found = db.find(tid, id(i), &txn2).unwrap();
            assert_eq!(
                found
                    .unwrap_or_else(|| panic!("row {i} missing"))
                    .data
                    .to_vec(),
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
        assert_eq!(
            found
                .expect("re-inserted large object must be visible")
                .data
                .to_vec(),
            big
        );
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
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("inserted")
                .data
                .to_vec(),
            base
        );
        db.rollback(t).unwrap();

        // update that GROWS the object (allocates more overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("grown")
                .data
                .to_vec(),
            grown
        );
        db.rollback(t).unwrap();

        // update that SHRINKS the object (frees overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("shrunk")
                .data
                .to_vec(),
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
            db2.find(tid, id(1), &t)
                .unwrap()
                .expect("reopened")
                .data
                .to_vec(),
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
            db.find(tid, id(5), &t)
                .unwrap()
                .expect("v1 must stand")
                .data
                .to_vec(),
            v1,
            "rolled-back large update must leave the committed value intact"
        );
        db.rollback(t).unwrap();
    }

    // A data page holding many small tuples, all updated at least once, must
    // never have its next_page (the link to the next sibling data page)
    // clobbered. update()'s replacement tuple is a few bytes larger than
    // what insert() originally wrote (this sets undo_id, which insert
    // leaves None) — on a page already packed to capacity, updating every
    // row on it used to push page_used_size past usable_data_size, which
    // handle_large_page_size misread as "this page needs a single-tuple
    // overflow chain", overwriting next_page with an overflow-page id
    // instead of the real next sibling. table_scan then silently
    // undercounted or hit a deserialization error trying to read that
    // "sibling" as an ordinary multi-tuple page. Found via a performance
    // harness (examples/perf) hitting exactly this after a bulk update.
    #[test]
    fn test_table_scan_correct_after_updating_every_row_across_multiple_data_pages() {
        let (db, tid) = make_db_with_table();
        // Enough small rows to span multiple data pages at the default page
        // size (empirically ~200 rows/page for a 64B value at 16KiB pages).
        let n = 500u64;
        let value = vec![b'v'; 64];
        for i in 0..n {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, &value), &t).unwrap();
            db.commit(t).unwrap();
        }

        // Update every row once, in reverse order (touches pages in a
        // different order than they were filled).
        for i in (0..n).rev() {
            let t = db.begin().unwrap();
            db.update(tid, row(i, &value), &t).unwrap();
            db.commit(t).unwrap();
        }

        let mut scanned = 0u64;
        let mut cursor = db.table_scan(tid).unwrap();
        while cursor.next().unwrap().is_some() {
            scanned += 1;
        }
        assert_eq!(
            scanned, n,
            "table_scan must see every row after all of them have been updated, \
             not stop early or error partway through the data-page chain"
        );

        // Every row must also still be independently findable with the
        // right value — relocation (if it happened) must have kept the
        // index pointing at wherever the row actually landed.
        let t = db.begin().unwrap();
        for i in 0..n {
            assert_eq!(
                db.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after update-all"))
                    .data
                    .to_vec(),
                value
            );
        }
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
                .data
                .to_vec(),
            v
        );
        db.rollback(t).unwrap();
    }

    // ── checkpoint ─────────────────────────────────────────────────────────
    // Db::checkpoint() drains the page buffer's deferred-write queue (making
    // every page dirtied so far durable to the backing store) and then
    // persists an updated header (page_count / last_checkpoint) — all without
    // requiring a full close(). These tests exercise both halves of that
    // contract, plus that checkpoint is a purely physical operation with no
    // effect on logical (MVCC) visibility.

    use crate::constant::timestamp;
    use crate::db::Header;
    use postcard::from_bytes;

    // Reads the on-disk header directly via the DBFile's own positioned read,
    // bypassing the page cache entirely, so these tests observe exactly what
    // checkpoint() has made durable rather than what's merely cached in
    // memory. A generous fixed-size buffer is fine: postcard ignores trailing
    // unconsumed bytes when deserializing.
    fn read_raw_header(db: &TestDB) -> Header {
        let mut buf = vec![0u8; 128];
        db.file.pread(&mut buf, 0).unwrap();
        from_bytes(&buf).unwrap()
    }

    // PageBuffer::write_header enqueues the header write asynchronously (it
    // does not round-trip like the page-flush half of checkpoint does), so
    // there is a short window after checkpoint() returns before the new
    // header is actually durable. Poll instead of asserting immediately.
    fn wait_for_raw_last_checkpoint_at_least(db: &TestDB, min_ts: u128) -> Header {
        for _ in 0..200 {
            let h = read_raw_header(db);
            if h.last_checkpoint >= min_ts {
                return h;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("persisted header's last_checkpoint never reached >= {min_ts}");
    }

    #[test]
    fn test_checkpoint_on_empty_db_succeeds() {
        let (db, _tid) = make_db_with_table();
        assert!(db.checkpoint().is_ok());
    }

    #[test]
    fn test_checkpoint_is_idempotent() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();
        let first = wait_for_raw_last_checkpoint_at_least(&db, 0);

        db.checkpoint().unwrap();
        let second = wait_for_raw_last_checkpoint_at_least(&db, first.last_checkpoint);

        assert!(
            second.last_checkpoint >= first.last_checkpoint,
            "a second, no-op checkpoint must not regress last_checkpoint"
        );
        assert_eq!(second.page_count, db.page_count());
    }

    #[test]
    fn test_checkpoint_flushes_allocated_pages_to_storage() {
        let (db, tid) = make_db_with_table();
        // Enough rows to allocate several data pages, not just the initial
        // system/index pages created at table creation.
        for i in 0..200u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("value-{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        db.checkpoint().unwrap();

        // Every page up to page_count must physically exist in the backing
        // store once checkpoint() returns — a page write still sitting in the
        // writer's deferred queue would leave the file shorter than this.
        let expected_min_len = db.header.first_page_offset + db.page_count() * db.header.page_size;
        // The header write itself is async (see wait_for_raw_last_checkpoint_at_least);
        // poll get_metadata the same way rather than asserting immediately.
        let mut len = db.file.get_metadata().unwrap().len;
        for _ in 0..200 {
            if len >= expected_min_len {
                break;
            }
            thread::sleep(Duration::from_millis(2));
            len = db.file.get_metadata().unwrap().len;
        }
        assert!(
            len >= expected_min_len,
            "checkpoint must flush every allocated page to storage: file len {len} < expected {expected_min_len}"
        );
    }

    #[test]
    fn test_checkpoint_persists_page_count_and_last_checkpoint() {
        let (db, tid) = make_db_with_table();
        for i in 0..50u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }

        let before = timestamp();
        db.checkpoint().unwrap();
        let persisted = wait_for_raw_last_checkpoint_at_least(&db, before);

        assert_eq!(
            persisted.page_count,
            db.page_count(),
            "persisted header page_count must match the live allocator state"
        );
        assert!(persisted.last_checkpoint >= before);
    }

    #[test]
    fn test_checkpoint_does_not_affect_visibility_of_rolled_back_data() {
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"committed"), &t0).unwrap();
        db.commit(t0).unwrap();

        let t1 = db.begin().unwrap();
        db.update(tid, row(1, b"uncommitted-update"), &t1).unwrap();
        db.rollback(t1).unwrap();

        // Checkpoint is purely physical; it must not resurrect or otherwise
        // change what's logically visible.
        db.checkpoint().unwrap();

        let t2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &t2).unwrap();
        drop(t2);
        assert_eq!(
            found.expect("row must still exist").data.to_vec(),
            b"committed",
            "checkpoint must not make a rolled-back write visible"
        );
    }

    #[test]
    fn test_checkpoint_across_multiple_tables() {
        let (db, ta, tb) = make_db_with_two_tables();
        let t = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &t).unwrap();
        db.insert(tb, row(1, b"b1"), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();

        let t2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &t2).unwrap().unwrap().data.to_vec(),
            b"a1"
        );
        assert_eq!(
            db.find(tb, id(1), &t2).unwrap().unwrap().data.to_vec(),
            b"b1"
        );
        drop(t2);
    }

    #[test]
    fn test_checkpoint_with_large_overflow_object() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'K'; 60_000]; // several overflow pages
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        let t2 = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t2)
                .unwrap()
                .expect("large object must survive checkpoint + close/reopen")
                .data
                .to_vec(),
            big
        );
        drop(t2);
    }

    // --- log-based crash recovery replay (Db::load_logs / process_redo /
    // process_undo) ---

    // crash_clone (not close(), which now truncates the logs on any clean
    // shutdown): the redo/undo logs still hold every record from these
    // commits, so reopening genuinely exercises replay (process_redo
    // re-applying committed Add ops via insert_if_needed) rather than
    // replaying against already-truncated, empty logs.
    #[test]
    fn test_replay_redoes_committed_writes_on_reopen() {
        let (db, tid) = make_db_with_table();
        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs(&db, 20, 20);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after replay"))
                    .data
                    .to_vec(),
                format!("v{i}").as_bytes()
            );
        }
    }

    // A transaction that never commits and is never dropped normally
    // (mem::forget, so Transaction::drop's implicit rollback — and
    // therefore close()'s own drain_aborting — never runs) can still have
    // durably flushed its write to the main file: PageBuffer's flush
    // timing is independent of transaction commit/rollback status. On
    // reopen, process_undo must revert it: its undo log has entries but no
    // Commit record, so it's excluded from process_redo's replay and
    // explicitly reverted via revert_undo_ops (the same remove_if_txn/
    // update_if_txn primitives normal rollback uses).
    #[test]
    fn test_replay_undoes_uncommitted_abandoned_writes_on_reopen() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"committed"), &t).unwrap();
        db.commit(t).unwrap();

        let uncommitted = db.begin().unwrap();
        db.insert(tid, row(2, b"uncommitted"), &uncommitted)
            .unwrap();
        std::mem::forget(uncommitted);

        wait_for_durable_logs(&db, 3, 3);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"committed"
        );
        assert!(
            db2.find(tid, id(2), &t).unwrap().is_none(),
            "an uncommitted, abandoned transaction's write must be reverted by undo replay"
        );
    }

    // Exercises Mod and Del redo/undo, not just Add, and mixes committed
    // and abandoned transactions in the same table.
    #[test]
    fn test_replay_handles_mixed_add_mod_del_across_committed_and_abandoned_txns() {
        let (db, tid) = make_db_with_table();

        // Row 1: inserted, then updated — both committed.
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, b"v1-updated"), &t).unwrap();
        db.commit(t).unwrap();

        // Row 2: inserted, then removed — both committed.
        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"v2"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(2), &t).unwrap();
        db.commit(t).unwrap();

        // Row 3: inserted and committed, then updated by an abandoned
        // (never committed) transaction — the update must not stick.
        let t = db.begin().unwrap();
        db.insert(tid, row(3, b"v3"), &t).unwrap();
        db.commit(t).unwrap();
        let abandoned = db.begin().unwrap();
        db.update(tid, row(3, b"v3-should-not-stick"), &abandoned)
            .unwrap();
        std::mem::forget(abandoned);

        wait_for_durable_logs(&db, 11, 11);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"v1-updated"
        );
        assert!(
            db2.find(tid, id(2), &t).unwrap().is_none(),
            "row 2's committed remove must survive replay"
        );
        assert_eq!(
            db2.find(tid, id(3), &t).unwrap().unwrap().data.to_vec(),
            b"v3",
            "row 3's abandoned update must be reverted, restoring the pre-image"
        );
    }

    // Replay must be safe to run more than once in a row: reopening a
    // second time (no new writes in between) re-scans logs that already
    // reflect reality and must not corrupt or duplicate anything.
    #[test]
    fn test_replay_is_idempotent_across_repeated_reopens() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        // First "crash": replay runs once against the original records.
        wait_for_durable_logs(&db, 10, 10);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        // A second "crash", of db2, with nothing new having happened on
        // it: process_redo/process_undo call the BPlusTree-level methods
        // directly, never logger.log_redo/log_undo, so db2's redo/undo
        // files still hold the exact same records from the original
        // crash. Replaying them a second time (via db3) must be just as
        // safe as the first. db2's own header still needs syncing before
        // ITS crash_clone — its page_count may have moved (replay itself
        // can allocate pages) since db2 opened.
        wait_for_durable_logs(&db2, 10, 10);
        sync_header_without_truncating_logs(&db2);
        let (f2, u2, r2) = crash_clone(&db2);
        let db3 = TestDB::open_using("txn_test.db", f2, u2, r2).unwrap();

        let t = db3.begin().unwrap();
        for i in 0..5u64 {
            assert_eq!(
                db3.find(tid, id(i), &t).unwrap().unwrap().data.to_vec(),
                format!("v{i}").as_bytes()
            );
        }
    }

    // The actual point of redo replay: reconstruct a write whose page
    // never made it to the main file before a crash, using only the
    // (already-durable) redo log. Constructed deterministically instead of
    // racing real background flush timing: snapshot the main file's bytes
    // right after a checkpoint (a known-consistent, fully-flushed state),
    // then perform one more committed insert and take the CURRENT redo/
    // undo logs (which, after the checkpoint's truncate, contain only that
    // insert's own records) — but pair them with the OLD, pre-insert main
    // file bytes instead of the real (already-flushed, in this test)
    // current ones. This reconstructs exactly what a crash between "redo
    // record durably logged" and "page flushed to the main file" would
    // leave behind, without needing to catch that race in real time.
    #[test]
    fn test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"pre-checkpoint"), &t).unwrap();
        db.commit(t).unwrap();
        db.checkpoint().unwrap();
        // checkpoint()'s own header write (PageBuffer::write_header) is
        // fire-and-forget, same as the log truncate — it enqueues a
        // BufMsg::WriteHeader and returns without waiting for the writer
        // thread to actually apply it. A second checkpoint's OWN
        // synchronous BufMsg::Checkpoint reply is queued strictly after
        // that WriteHeader message (same channel, FIFO), so waiting for
        // *this* checkpoint to return guarantees the first one's header
        // write already landed — otherwise this snapshot could race it and
        // capture a header whose page_count is still 0, which later fails
        // to even load system tables on reopen.
        db.checkpoint().unwrap();

        // The last known-durable state before the "crash".
        let stale_main_file_bytes = db.file.data();

        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"lost-on-crash"), &t).unwrap();
        db.commit(t).unwrap();

        // Simulate a crash right here: live clones of the (unclosed,
        // untruncated) redo/undo logs — close() would now truncate them,
        // since a clean close leaves nothing that needs replaying. Bounded
        // (1) redo/undo channels make log_redo/log_undo block until the
        // runner thread has actually received each record, so these
        // clones reliably contain row 2's Add+Commit (logged after the
        // checkpoint's truncate, so they're the only two records in it).
        wait_for_durable_logs(&db, 2, 2);
        let (_, undo_file, redo_file) = crash_clone(&db);

        // Rebuild a "crashed" main file from the pre-insert snapshot —
        // independent bytes, not sharing the live file's buffer.
        let crashed_file = MemFile::new();
        crashed_file.pwrite(&stale_main_file_bytes, 0).unwrap();

        let db2 = TestDB::open_using("txn_test.db", crashed_file, undo_file, redo_file).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"pre-checkpoint",
            "sanity: the pre-crash checkpointed row must still be there"
        );
        assert_eq!(
            db2.find(tid, id(2), &t)
                .unwrap()
                .unwrap_or_else(|| panic!(
                    "row 2 missing — redo replay failed to reconstruct a committed \
                     write whose page flush never reached the main file"
                ))
                .data
                .to_vec(),
            b"lost-on-crash"
        );
    }

    // --- log-based crash recovery replay, real File backend ---
    //
    // Everything above exercises MemFile's `load_logs` path (the buffer's
    // `.data()` branch). `File`-backed logs go through a different branch
    // (mmap over a real fd — see `load_logs`'s `as_any().downcast_ref::<File>`
    // arm), previously covered by logger.rs's own
    // test_load_logs_*_file_mmap/test_load_logs_accumulates_across_multiple_
    // sessions tests directly against a bare `Logger`. Those were removed
    // once `load_logs` moved from `Logger` to `Db` (a bare `Logger` can no
    // longer apply redo/undo without table access) — ported here through the
    // `Db`-level API so the mmap branch itself stays covered.

    fn temp_db_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "squeal_db_replay_test_{tag}_{}",
                std::process::id()
            ))
            .to_string_lossy()
            .into_owned()
    }

    // Same idea as crash_clone, but for the real File backend: File::
    // try_clone (Opener::do_clone's impl) dups the fd, giving a second
    // handle onto the same underlying open file — writes through either are
    // visible via the other, just like MemFile's Arc-shared buffer.
    fn crash_clone_file(db: &FileDB) -> (File, File, File) {
        (
            db.file.do_clone().unwrap(),
            db.undo_file.do_clone().unwrap(),
            db.redo_file.do_clone().unwrap(),
        )
    }

    // Passive record count straight off disk, by path rather than through a
    // shared fd — sidesteps any concern about interfering with the writer
    // thread's own seek position (see Opener::pread's doc comment on why
    // clones of the same fd share a cursor).
    fn count_log_records_at_path(path: &str) -> usize {
        let data = std::fs::read(path).unwrap_or_default();
        let mut buf = &data[..];
        let mut count = 0;
        while !buf.is_empty() {
            let (_msg, remaining) = take_from_bytes::<MsgType>(buf).unwrap();
            buf = remaining;
            count += 1;
        }
        count
    }

    // See wait_for_durable_logs (MemFile version) — same log_redo/log_undo
    // send()-doesn't-imply-written race applies to the File backend too.
    fn wait_for_durable_logs_file(db_name: &str, expected_redo: usize, expected_undo: usize) {
        let redo_path = format!("{db_name}.redo");
        let undo_path = format!("{db_name}.undo");
        for _ in 0..1000 {
            if count_log_records_at_path(&redo_path) == expected_redo
                && count_log_records_at_path(&undo_path) == expected_undo
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!(
            "timed out waiting for {expected_redo} redo / {expected_undo} undo file-backed \
             records to land"
        );
    }

    // mmap-ing a zero-length file is a classic edge case (some mmap
    // implementations error on it), and it's the state of every brand-new
    // database's log files before a single record has ever been written —
    // not a corner case, the common one.
    #[test]
    fn test_replay_handles_empty_file_backed_logs_without_panicking() {
        let db_name = temp_db_path("empty");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        db.create_table("rows".to_string()).unwrap();

        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, u, r).unwrap();
        assert_eq!(db2.get_tables().unwrap().len(), 1);

        FileDB::delete(&db_name).unwrap_or_default();
    }

    #[test]
    fn test_replay_recovers_committed_writes_on_file_backed_db() {
        let db_name = temp_db_path("recover");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs_file(&db_name, 20, 20);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, u, r).unwrap();

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after file-backed replay"))
                    .data
                    .to_vec(),
                format!("v{i}").as_bytes()
            );
        }
        drop(t);

        FileDB::delete(&db_name).unwrap_or_default();
    }

    // A restarted process reopens the same on-disk files rather than
    // truncating them — replay must see everything from every prior
    // session, not just the most recent one (mirrors the removed
    // test_load_logs_accumulates_across_multiple_sessions, through the Db
    // API instead of a bare Logger).
    #[test]
    fn test_replay_is_idempotent_across_repeated_reopens_file_backed() {
        let db_name = temp_db_path("idempotent");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs_file(&db_name, 10, 10);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, u, r).unwrap();

        wait_for_durable_logs_file(&db_name, 10, 10);
        sync_header_without_truncating_logs(&db2);
        let (f2, u2, r2) = crash_clone_file(&db2);
        let db3 = FileDB::open_using(&db_name, f2, u2, r2).unwrap();

        let t = db3.begin().unwrap();
        for i in 0..5u64 {
            assert_eq!(
                db3.find(tid, id(i), &t).unwrap().unwrap().data.to_vec(),
                format!("v{i}").as_bytes()
            );
        }
        drop(t);

        FileDB::delete(&db_name).unwrap_or_default();
    }

    // --- LSN watermark continuity across reopen ---
    //
    // process_redo now tracks the highest lsn_id seen while scanning the
    // redo log and calls `self.logger.clock().mark_written(lsn_id)` after
    // replay — intent: a freshly reopened Db's LsnClock (which otherwise
    // starts cold, watermark = u64::MAX) picks up where the prior session's
    // durable history left off, instead of forgetting it ever happened.

    fn wait_for_watermark_at_least(db: &TestDB, min: u64) {
        for _ in 0..1000 {
            if db.logger.clock().last_written().0 >= min {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out waiting for the LSN watermark to reach at least {min}");
    }

    #[test]
    fn test_replay_seeds_lsn_watermark_from_prior_session() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        // 5 Add + 5 Commit redo records, each with its own increasing lsn.
        wait_for_durable_logs(&db, 10, 10);
        let watermark_before_crash = db.logger.clock().last_written();
        assert_ne!(
            watermark_before_crash.0,
            u64::MAX,
            "sanity: the live session's own watermark must already be a real value, \
             not the cold-start sentinel"
        );

        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        assert_eq!(
            db2.logger.clock().last_written(),
            watermark_before_crash,
            "replay must seed the reopened db's watermark from the highest lsn in the \
             prior session's redo log, not leave it at the cold-start sentinel"
        );
    }

    // The deeper claim in "so it doesn't regress": once seeded by replay,
    // the watermark must never fall below where replay left it, even as
    // brand-new writes land in the new session. If the new session's lsn
    // *counter* isn't also resumed past the old session's highest value (a
    // freshly reset counter mints 0, 1, 2, ... again), the very first new
    // write's redo record durably landing calls mark_written with that low,
    // reused lsn — regressing the watermark right back down, undoing what
    // replay just established.
    #[test]
    fn test_lsn_watermark_does_not_regress_after_new_writes_post_reopen() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        wait_for_durable_logs(&db, 10, 10);
        sync_header_without_truncating_logs(&db);
        let (f, u, r) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let watermark_after_replay = db2.logger.clock().last_written();

        let t = db2.begin().unwrap();
        db2.insert(tid, row(999, b"new-after-reopen"), &t).unwrap();
        db2.commit(t).unwrap();
        wait_for_watermark_at_least(&db2, watermark_after_replay.0);

        assert!(
            db2.logger.clock().last_written().0 >= watermark_after_replay.0,
            "a new write's own redo record landing regressed the watermark from {} down to {} — \
             the lsn counter must resume past the prior session's highest lsn, not just the \
             watermark",
            watermark_after_replay.0,
            db2.logger.clock().last_written().0
        );
    }

    #[test]
    fn test_checkpoint_then_close_then_reopen_preserves_data() {
        let (db, tid) = make_db_with_table();
        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("row-{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        db.checkpoint().unwrap();
        let page_count_at_checkpoint = db.page_count();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        assert_eq!(db2.page_count(), page_count_at_checkpoint);

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after checkpoint+close+reopen"))
                    .data
                    .to_vec(),
                format!("row-{i}").as_bytes()
            );
        }
        drop(t);
    }

    #[test]
    fn test_checkpoint_truncates_redo_and_undo_log_files() {
        let (db, tid) = make_db_with_table();
        for i in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }

        db.checkpoint().unwrap();
        // close() calls logger.shutdown(), which sends ShutDown on the same
        // (FIFO) channel checkpoint()'s truncate message went to, then
        // blocks on the runner threads joining — so by the time close()
        // returns, the truncate is guaranteed to have actually run.
        // checkpoint() returning on its own only guarantees the truncate
        // was *requested* (Logger::checkpoint is fire-and-forget), not
        // that it's completed yet.
        let (_, undo_file, redo_file) = db.close().unwrap();

        assert_eq!(
            undo_file.get_metadata().unwrap().len,
            0,
            "undo log must be truncated after a checkpoint"
        );
        assert_eq!(
            redo_file.get_metadata().unwrap().len,
            0,
            "redo log must be truncated after a checkpoint"
        );
    }

    // The whole point of truncating on checkpoint is to keep the log
    // bounded over a long-lived database's life, not just to be empty
    // once. Runs several commit+checkpoint rounds and checks the log
    // never grows past what a SINGLE round would produce — if truncation
    // silently stopped working (or only worked the first time), this
    // would catch the log growing round over round instead.
    #[test]
    fn test_checkpoint_keeps_log_bounded_across_many_rounds() {
        let (db, tid) = make_db_with_table();

        // checkpoint()'s truncate is fire-and-forget (Logger::checkpoint
        // just enqueues it — see test_checkpoint_truncates_redo_and_undo_
        // log_files) — poll briefly for it to actually land before
        // checking either file's size, or this races the runner threads
        // and can observe a stale, pre-truncate length.
        fn wait_for_logs_to_settle(db: &TestDB) {
            let mut tries = 0;
            while (db.redo_file.get_metadata().unwrap().len > 0
                || db.undo_file.get_metadata().unwrap().len > 0)
                && tries < 200
            {
                thread::sleep(Duration::from_millis(1));
                tries += 1;
            }
        }

        for round in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(round, b"v"), &t).unwrap();
            db.commit(t).unwrap();
            db.checkpoint().unwrap();
            wait_for_logs_to_settle(&db);

            assert_eq!(
                db.redo_file.get_metadata().unwrap().len,
                0,
                "round {round}: redo log must be empty once its checkpoint settles \
                 — it must not accumulate round over round"
            );
            assert_eq!(
                db.undo_file.get_metadata().unwrap().len,
                0,
                "round {round}: undo log must be empty once its checkpoint settles \
                 — it must not accumulate round over round"
            );
        }
    }

    #[test]
    fn test_checkpoint_concurrent_with_active_writers() {
        use std::sync::Arc;

        const THREADS: u64 = 8;
        const ROWS_PER_THREAD: u64 = 50;

        // Each thread gets its own table (disjoint B+tree, no shared
        // index/data pages) so the only cross-thread interaction is via the
        // shared PageBuffer/writer thread that checkpoint() also touches —
        // this isolates checkpoint's own thread-safety from same-table insert
        // contention, which has its own pre-existing, unrelated correctness
        // issues under heavy concurrency (not a checkpoint concern).
        let db = TestDB::create("checkpoint_concurrent_test.db").unwrap();
        let tids: Vec<TableIdType> = (0..THREADS)
            .map(|i| db.create_table(format!("t{i}")).unwrap())
            .collect();

        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            let tid = tids[thread_idx as usize];
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    let t = db.begin().unwrap();
                    db.insert(tid, row(i, format!("v{thread_idx}-{i}").as_bytes()), &t)
                        .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }

        // Checkpoint repeatedly while writers are still active, interleaved
        // with their inserts rather than after — this is the scenario the
        // Checkpoint message type and its reply channel exist to handle
        // safely.
        let checkpoint_db = Arc::clone(&db);
        let checkpoint_handle = thread::spawn(move || {
            for _ in 0..10 {
                checkpoint_db.checkpoint().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        checkpoint_handle.join().unwrap();

        // A final checkpoint after all writers finished, then verify every
        // row committed by every thread is visible and correct.
        db.checkpoint().unwrap();
        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            let tid = tids[thread_idx as usize];
            for i in 0..ROWS_PER_THREAD {
                let found = db.find(tid, id(i), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!(
                            "table {thread_idx} row {i} missing after concurrent checkpoint"
                        ))
                        .data
                        .to_vec(),
                    format!("v{thread_idx}-{i}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // ── free-page persistence ────────────────────────────────────────────
    // Freed pages (e.g. overflow continuations reclaimed when a large object
    // shrinks) are tracked in-memory by PageBuffer::free_pages and, as of this
    // change, also serialized into the reserved FREE_PAGE_TABLE_PAGE (page 2)
    // by write_system_tables() and restored by load_system_tables(). Before
    // this, every close/reopen forgot any freed pages: they became permanent,
    // unreachable holes (page_count only ever grows, so a forgotten free page
    // could never be reused). These tests check both that the recorded set
    // round-trips exactly, and — the part that actually matters — that a page
    // reused after reopen is safe to write fresh data into.

    use crate::page::PageId;
    use std::collections::HashSet;

    #[test]
    fn test_free_pages_empty_by_default_persists_as_empty() {
        let (db, _tid) = make_db_with_table();
        assert!(db.buffer.get_free_pages().is_empty());
        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        assert!(
            db2.buffer.get_free_pages().is_empty(),
            "a DB that never freed anything must not spuriously report free pages"
        );
    }

    #[test]
    fn test_freed_overflow_pages_persist_across_close_reopen() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(1, 8 * page);
        let shrunk = big_payload(2, 4 * page); // frees several overflow pages

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let freed_before = db.buffer.get_free_pages();
        assert!(
            !freed_before.is_empty(),
            "shrinking an 8-page object to 4 pages must free some overflow pages"
        );

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        let freed_after = db2.buffer.get_free_pages();

        assert_eq!(
            freed_after, freed_before,
            "the exact set of freed pages must survive close/reopen"
        );
    }

    #[test]
    fn test_reopened_db_reuses_freed_pages_before_growing_page_count() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(3, 8 * page);
        let shrunk = big_payload(4, 4 * page);

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let free_count = db2.buffer.get_free_pages().len();
        assert!(free_count > 0);
        let page_count_at_reopen = db2.page_count();

        // Draining exactly the free list must satisfy every allocation from
        // it (alloc_page always checks free_pages before growing page_count),
        // so page_count must not move at all during this loop.
        for _ in 0..free_count {
            db2.buffer.alloc_page(false).unwrap();
            assert_eq!(
                db2.page_count(),
                page_count_at_reopen,
                "reusing a freed page must not grow page_count"
            );
        }
        assert!(db2.buffer.get_free_pages().is_empty());

        // The free list is now exhausted; the next allocation must fall back
        // to growing page_count, proving the earlier ones really did come
        // from reuse and not from some other accounting quirk.
        db2.buffer.alloc_page(false).unwrap();
        assert_eq!(db2.page_count(), page_count_at_reopen + 1);
    }

    #[test]
    // Regression test for a real bug this test originally caught: freeing an
    // overflow continuation page used to only clear its header FLAGS
    // (free_overflow_pages), never page_used_size, next_page, or the data
    // region — so a page freed while it held a chunk of an overflow object
    // came back from alloc_page()'s free-list reuse still carrying that old
    // page_used_size (observed: 131090, i.e. roughly the original 8-page
    // object's total size, on a page whose own page_data_size is 16304),
    // making a perfectly ordinary Page::add_tuple on it fail with
    // PageCapacityError (can_store() saw it as already full). Fixed by
    // PageBuffer::reset_freed_page, which writes a genuinely fresh, empty
    // Page through the normal write path before the page goes on the free
    // list — the same thing init_page does for a brand-new page.
    fn test_reused_freed_overflow_page_is_safe_to_write_fresh_data_into() {
        let (db, tid) = make_db_with_table();
        let page_sz = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(5, 8 * page_sz);
        let shrunk = big_payload(6, 4 * page_sz);

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();

        let reused_id = db2.buffer.alloc_page(false).unwrap();
        assert!(
            !db2.buffer.get_free_pages().contains(&reused_id),
            "sanity: alloc_page must not return an id still in the free list"
        );

        // Mutate-and-write through the exact same handle pattern
        // BPlusTree::write_page (the real production path for adding a tuple
        // to a newly allocated data-chain continuation) uses: get_page_mut,
        // Arc::make_mut, mutate, write_locked_page. This exercises reuse
        // exactly as it happens in practice, rather than bypassing whatever
        // get_page_mut actually returns for a page whose on-disk bytes are a
        // raw overflow-chunk slice, not a standalone serialized Page.
        let mut handle = db2.buffer.get_page_mut(reused_id).unwrap();
        Arc::make_mut(&mut handle.page)
            .add_tuple(Tuple::new(42, b"fresh-after-reuse"))
            .unwrap();
        db2.buffer.write_locked_page(handle).unwrap();

        let readback = db2.buffer.get_page(reused_id).unwrap();
        assert_eq!(readback.count().unwrap(), 1);
        assert_eq!(
            readback
                .get(DBIdType::Int(42))
                .unwrap()
                .expect("the freshly written tuple must be readable")
                .data
                .to_vec(),
            b"fresh-after-reuse"
        );

        // The original (shrunk) large object must still read back correctly
        // too — reuse of an unrelated freed page must not disturb it.
        let t2 = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t2)
                .unwrap()
                .expect("shrunk object must still be intact")
                .data
                .to_vec(),
            shrunk
        );
        drop(t2);
    }

    #[test]
    fn test_free_pages_do_not_accumulate_across_multiple_close_reopen_cycles() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;

        // Round 1: free some pages, close, reopen.
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big_payload(7, 8 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(8, 4 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let (f, u, r) = db.close().unwrap();
        let db = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        let round1_free: HashSet<PageId> = db.buffer.get_free_pages().into_iter().collect();
        assert!(!round1_free.is_empty());

        // Round 2: grow the SAME object back up (consuming free pages, and
        // possibly needing fresh ones too), then shrink again, then close.
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(9, 8 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(10, 4 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let round2_free_before_close: HashSet<PageId> =
            db.buffer.get_free_pages().into_iter().collect();

        let (f, u, r) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, u, r).unwrap();
        let round2_free_after_reopen: HashSet<PageId> =
            db2.buffer.get_free_pages().into_iter().collect();

        assert_eq!(
            round2_free_after_reopen, round2_free_before_close,
            "the persisted set must reflect only the latest close, not an \
             accumulation of every round's freed pages"
        );
    }

    // Regression test for a real bug found while investigating todo.txt item
    // [7] (spurious DuplicateKey under concurrent inserts into one shared
    // table). Confirmed root cause via targeted instrumentation: insert_index
    // failing with LockContentionError is normal under contention, but
    // BPlusTree::insert's cleanup of the just-written data-page row (undoing
    // write_data's write before returning the error) used a bare `?` on its
    // own get_page_mut/write_locked_page calls — so if *that* itself hit
    // LockContentionError, the cleanup was abandoned and the row was left
    // permanently orphaned (written, but never indexed, and invisible to
    // find() since nothing points to it). write_data's page selection is
    // deterministic (always starts from first_data_page), so a later retry
    // of the same key reliably lands on that same page and hits a real, but
    // bogus, DuplicateKey — permanently, not just transiently, since the
    // orphaned row is never cleaned up by anything. Fixed by wrapping the
    // cleanup in retry_on_contention so its own contention can't abort it.
    // This test reliably reproduced the bug before the fix (roughly 1 in 3
    // runs with 8 threads x 50 rows into a single shared table).
    #[test]
    fn test_concurrent_inserts_into_shared_table_do_not_orphan_rows() {
        const THREADS: u64 = 8;
        const ROWS_PER_THREAD: u64 = 50;
        let (db, tid) = make_db_with_table();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    let key = thread_idx * ROWS_PER_THREAD + i;
                    let t = db.begin().unwrap();
                    super::retry_on_contention(|| {
                        db.insert(tid, row(key, format!("v{key}").as_bytes()), &t)
                    })
                    .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            for i in 0..ROWS_PER_THREAD {
                let key = thread_idx * ROWS_PER_THREAD + i;
                let found = db.find(tid, id(key), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!("row {key} missing"))
                        .data
                        .to_vec(),
                    format!("v{key}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // Regression test for todo.txt [16]: a non-root leaf (or, separately, a
    // non-root inner node) filled to capacity by a concurrent insert in the
    // window between a routing check and this thread's actual descent.
    // Needs a small page size (few entries per page) and interleaved keys
    // (not per-thread disjoint ranges) so many threads route through the
    // *same* pages concurrently — the default page size's large fanout
    // makes this window vanishingly unlikely to hit in practice. Before the
    // fix: the leaf-level race panicked directly
    // ("count == nodes- should not happen"); the inner-node-level race
    // surfaced as an unhandled PageCapacityError (insert_index's retry loop
    // for it was a stale, never-compiled comment). Both closed by
    // split_if_needed carrying its already-held lock forward (no
    // release-then-reacquire gap) and insert_index actually retrying on
    // PageCapacityError. Confirmed both were reachable before the fix: 19/200
    // and several more/100 runs respectively hit one or the other at this
    // scale.
    #[test]
    fn test_concurrent_inserts_at_small_page_size_do_not_panic_or_lose_rows() {
        const THREADS: u64 = 16;
        const ROWS_PER_THREAD: u64 = 40;
        let db: Arc<TestDB> = TestDB::create_with_page_size("small_page_race.db", 512).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    // Interleaved (not partitioned) keys so concurrent
                    // threads route into the same leaf/inner pages, not
                    // disjoint parts of the tree.
                    let key = i * THREADS + thread_idx;
                    let t = db.begin().unwrap();
                    super::retry_on_contention(|| {
                        db.insert(tid, row(key, format!("v{key}").as_bytes()), &t)
                    })
                    .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let t = db.begin().unwrap();
        for i in 0..ROWS_PER_THREAD {
            for thread_idx in 0..THREADS {
                let key = i * THREADS + thread_idx;
                let found = db.find(tid, id(key), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!("row {key} missing"))
                        .data
                        .to_vec(),
                    format!("v{key}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // Regression test for todo.txt items [11] and [15]: BPlusTree::remove
    // (and remove_if_txn, used by rollback's revert-of-insert path) removed
    // the data tuple and the index entry as two separate steps. Under
    // contention the index-removal step could fail after the data step had
    // already succeeded, leaving a permanently stale index entry pointing
    // at a now-vacated page — "this key can never be re-inserted again"
    // (DuplicateKey forever), and combined with [7]'s insert-cleanup path
    // also occasionally exhausting its retries, sometimes worse: a later
    // insert's orphaned row landing on that same page, so a committed
    // remove's key comes back with a stale value that was never
    // legitimately written to it.
    // [11] fixed the first layer (retry the index-removal step internally
    // instead of relying on a caller-level retry, which is unsafe once the
    // data step already succeeded — the retried call hits KeyNotFound, not
    // LockContentionError, so the caller's own retry gives up without ever
    // retrying the index cleanup). That reduced the failure rate hugely but
    // left a residual ~7.5% flake, tracked as [15] and fully root-caused
    // there: the same "outer retry re-invokes a function that's unsafe to
    // re-invoke" pattern recurring one layer up in Db::commit's tombstone
    // reclaim (silently swallowing find()'s own LockContentionError), plus
    // an unrelated timing issue (the page lock's timeout and
    // retry_on_contention's total backoff were both far shorter than
    // realistic OS scheduling jitter). See todo.txt item [15] for the full
    // three-part root cause and fix.
    #[test]
    fn test_concurrent_insert_remove_reinsert_does_not_resurrect_stale_value() {
        const THREADS: u64 = 16;
        const KEYS_PER_THREAD: u64 = 20;
        const CYCLES: u64 = 10;
        let (db, tid) = make_db_with_table();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for cycle in 0..CYCLES {
                    for i in 0..KEYS_PER_THREAD {
                        let key = thread_idx * KEYS_PER_THREAD + i;
                        let value = format!("v{key}-{cycle}");
                        let t = db.begin().unwrap();
                        super::retry_on_contention(|| {
                            db.insert(tid, row(key, value.as_bytes()), &t)
                        })
                        .unwrap();
                        db.commit(t).unwrap();

                        let t = db.begin().unwrap();
                        super::retry_on_contention(|| db.remove(tid, id(key), &t)).unwrap();
                        db.commit(t).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Every key's last committed op (per thread, per cycle) was a
        // remove, so every key must now be absent.
        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            for i in 0..KEYS_PER_THREAD {
                let key = thread_idx * KEYS_PER_THREAD + i;
                let found = db.find(tid, id(key), &t).unwrap();
                assert!(
                    found.is_none(),
                    "key {key} should be absent (last committed op was remove) but found {found:?}"
                );
            }
        }
        drop(t);
    }

    // Regression test for a confirmed race in BPlusTree::find_page/
    // route_to_leaf: both do a fully unlocked, multi-step descent (route,
    // then separately read the leaf landed on), with no protection against
    // a concurrent split moving the target key to a new sibling in between.
    // That produces a spurious KeyNotFound for a key that genuinely exists
    // — and since KeyNotFound isn't LockContentionError, retry_on_contention
    // never retries it, so a remove/update attempt that hits this mid-flight
    // just fails outright, leaving the row's prior (un-removed) state as
    // the permanent, incorrect "final" value. Confirmed via direct,
    // instrumented reproduction: caught cases where a fresh find_page call
    // failed for a key whose existence had just been confirmed microseconds
    // earlier by a separate lookup in the very same call, with no
    // intervening removal — and where the eventual "resurrected" value's
    // own transaction timestamp was hundreds of milliseconds away from the
    // reader's (ruling out any timestamp-ordering ambiguity as the cause).
    // Runs several rounds since the race needs concurrent structural
    // splits to manifest — tolerant of individual operation failures (logs
    // them) so one early miss doesn't abort the whole run before the final
    // "every key must be absent" check, which is the actual assertion.
    #[test]
    fn test_concurrent_insert_remove_under_splits_does_not_resurrect_stale_value() {
        const THREADS: u64 = 16;
        const KEYS_PER_THREAD: u64 = 20;
        const CYCLES: u64 = 20;
        const ROUNDS: u32 = 20;
        let mut resurrections = 0u32;
        for _round in 0..ROUNDS {
            let (db, tid) = make_db_with_table();
            let mut handles = Vec::new();
            for thread_idx in 0..THREADS {
                let db = Arc::clone(&db);
                handles.push(thread::spawn(move || {
                    for cycle in 0..CYCLES {
                        for i in 0..KEYS_PER_THREAD {
                            let key = thread_idx * KEYS_PER_THREAD + i;
                            let value = format!("v{key}-{cycle}");
                            let Ok(t) = db.begin() else { continue };
                            if super::retry_on_contention(|| {
                                db.insert(tid, row(key, value.as_bytes()), &t)
                            })
                            .is_err()
                            {
                                continue;
                            }
                            if db.commit(t).is_err() {
                                continue;
                            }

                            let Ok(t) = db.begin() else { continue };
                            if super::retry_on_contention(|| db.remove(tid, id(key), &t)).is_err()
                            {
                                continue;
                            }
                            let _ = db.commit(t);
                        }
                    }
                }));
            }
            for h in handles {
                let _ = h.join();
            }

            let t = db.begin().unwrap();
            for thread_idx in 0..THREADS {
                for i in 0..KEYS_PER_THREAD {
                    let key = thread_idx * KEYS_PER_THREAD + i;
                    if db.find(tid, id(key), &t).unwrap().is_some() {
                        resurrections += 1;
                    }
                }
            }
            drop(t);
        }
        assert_eq!(
            resurrections, 0,
            "{resurrections} keys resurrected across {ROUNDS} rounds"
        );
    }
}
