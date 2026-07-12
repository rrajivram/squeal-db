use std::{
    collections::HashMap,
    fs::File,
    io::SeekFrom,
    sync::{Arc, atomic::AtomicU64},
    thread::{self, JoinHandle},
};

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::error;
use memmap::MmapOptions;
use parking_lot::RwLock;
use postcard::{take_from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp, db::DBFile, error::StoreError, memfile::MemFile, table::TableIdType,
    tuple::Tuple, txn::TransactionId,
};

/// Per-database write-ahead-log clock. Was two process-global statics, which
/// meant every `Db` (and every test) in the process shared one LSN counter and
/// one flush watermark — leaking write-ordering state across databases (flaky
/// close/reopen in the test suite; a latent corruption bug for >1 live `Db`).
/// One clock per `Logger`, shared with the `PageBuffer` (and its writer thread)
/// that the same `Db` owns, so the WAL deferral is scoped to a single database.
#[derive(Debug)]
pub(crate) struct LsnClock {
    /// Monotonic source of redo LSNs.
    counter: AtomicU64,
    /// Highest redo LSN durably written — the flush watermark. Starts very high
    /// so freshly created pages (stamped from it) are written promptly until the
    /// first redo record lands and pulls the watermark down to a real value.
    last_written: AtomicU64,
}

impl Default for LsnClock {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
            last_written: AtomicU64::new(u64::MAX),
        }
    }
}

impl LsnClock {
    /// Allocate the next redo LSN.
    pub(crate) fn next_lsn(&self) -> LsnId {
        LsnId(
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel),
        )
    }

    /// The current flush watermark (highest durably-written redo LSN).
    pub(crate) fn last_written(&self) -> LsnId {
        LsnId(self.last_written.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Advance the watermark as the redo runner persists records.
    pub(crate) fn mark_written(&self, lsn: LsnId) {
        self.last_written
            .store(lsn.0, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct LsnId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct UndoId(pub(crate) u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum MsgType {
    Undo(UndoOperation),
    Redo(RedoOperation),
    ShutDown,
    Checkpoint(u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RedoOperation {
    lsn_id: LsnId,
    operation: Operation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct UndoOperation {
    undo_id: Option<UndoId>,
    operation: Operation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Operation {
    Add(TransactionId, Record),
    Del(TransactionId, Record),
    Mod(TransactionId, Record),
    Commit(TransactionId, u128),
    Rollback(TransactionId, u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    pub(crate) table_id: TableIdType,
    timestamp: u128,
    pub(crate) tuple: Tuple,
}

#[derive(Debug, Default)]
pub(crate) struct Logger {
    redo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_tx: Option<Sender<MsgType>>,
    redo_tx: Option<Sender<MsgType>>,
    undo_txns: RwLock<HashMap<TransactionId, Vec<Operation>>>,
    clock: Arc<LsnClock>,
}

impl Logger {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn set_db(
        &mut self,
        undo_file: impl DBFile + 'static,
        redo_file: impl DBFile + 'static,
    ) -> Result<(), StoreError> {
        let _ = self.load_logs(&undo_file, &redo_file)?;
        let (redo_tx, redo_rx) = unbounded();
        let (undo_tx, undo_rx) = unbounded();
        self.undo_tx = Some(undo_tx);
        self.redo_tx = Some(redo_tx);

        let clock = self.clock.clone();
        self.undo_handle = Some(thread::spawn(move || undo_log_runner(undo_file, undo_rx)));
        self.redo_handle = Some(thread::spawn(move || {
            redo_log_runner(redo_file, redo_rx, clock)
        }));
        Ok(())
    }

    // Returns (redo_count, undo_count) so callers (and tests) can check
    // what was actually recovered, not just the println below.
    fn load_logs(
        &self,
        undo_file: &impl DBFile,
        redo_file: &impl DBFile,
    ) -> Result<(usize, usize), StoreError> {
        let (mut redo_count, mut undo_count) = (0, 0);
        // mmap-ing a zero-length file errors ("memory map must have a
        // non-zero length") rather than yielding an empty mapping — and a
        // brand-new database's redo/undo files are exactly that (0 bytes,
        // nothing ever written yet), so this isn't an edge case, it's the
        // state of every single fresh Db::create::<File>() call. Skip the
        // mmap entirely when there's nothing to read.
        if let Some(redo_file) = redo_file.as_any().downcast_ref::<File>()
            && redo_file.metadata()?.len() > 0
        {
            let map = unsafe { MmapOptions::new().map(redo_file)? };
            let mut buf = &map[..];
            while !buf.is_empty() {
                let redo = take_from_bytes::<MsgType>(buf)?;
                buf = redo.1;
                redo_count += 1;
            }
        }
        if let Some(undo_file) = undo_file.as_any().downcast_ref::<File>()
            && undo_file.metadata()?.len() > 0
        {
            let map = unsafe { MmapOptions::new().map(undo_file)? };
            let mut buf = &map[..];
            while !buf.is_empty() {
                let undo = take_from_bytes::<MsgType>(buf)?;
                buf = undo.1;
                undo_count += 1;
            }
        }
        if let Some(redo_file) = redo_file.as_any().downcast_ref::<MemFile>() {
            let mut buf = &redo_file.data()[..];
            while !buf.is_empty() {
                let redo = take_from_bytes::<MsgType>(buf)?;
                buf = redo.1;
                redo_count += 1;
            }
        }
        if let Some(undo_file) = undo_file.as_any().downcast_ref::<MemFile>() {
            let mut buf = &undo_file.data()[..];
            while !buf.is_empty() {
                let undo = take_from_bytes::<MsgType>(buf)?;
                buf = undo.1;
                undo_count += 1;
            }
        }

        println!("{} redo found, {} undo found", redo_count, undo_count);

        Ok((redo_count, undo_count))
    }

    /// Shared handle to this database's LSN clock, for the `PageBuffer` (and its
    /// writer thread) that stamp/compare page LSNs against the same watermark.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
    }

    pub(crate) fn log_undo(&self, op: Operation) -> Result<(), StoreError> {
        let msg = match &op {
            // Indexed by the operation's own txn (not record.tuple.txn_id):
            // a Mod/Del undo record deliberately carries the *pre-image* tuple
            // (with the previous, already-committed owner's txn_id) so that
            // rollback can restore it and MVCC reads can walk the chain to a
            // committed ancestor. That pre-image's txn_id is unrelated to which
            // transaction's undo log this entry belongs to.
            Operation::Add(txn, _) | Operation::Del(txn, _) | Operation::Mod(txn, _) => {
                let mut undo_txns = self.undo_txns.write();
                let id = undo_txns
                    .entry(txn.clone())
                    .and_modify(|v| v.push(op.clone()))
                    .or_insert(vec![op.clone()]);
                MsgType::Undo(UndoOperation {
                    undo_id: Some(UndoId(id.len() as u16)),
                    operation: op.clone(),
                })
            }
            _ => MsgType::Undo(UndoOperation {
                undo_id: None,
                operation: op.clone(),
            }),
        };
        if let Some(tx) = &self.undo_tx {
            tx.send(msg)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            // Now store record if available
        }
        match &op {
            Operation::Commit(id, _) | Operation::Rollback(id, _) => {
                self.undo_txns.write().remove(id);
            }
            _ => {}
        }
        Ok(())
    }

    /// Drop a transaction's in-memory undo records. Called after its undo has
    /// been fully replayed (abort reclamation) — a dropped/aborted txn logs no
    /// Commit/Rollback op, so its records aren't cleaned by log_undo above.
    pub(crate) fn discard_undo(&self, id: &TransactionId) {
        self.undo_txns.write().remove(id);
    }

    pub(crate) fn next_undo_id(&self, id: TransactionId) -> Result<UndoId, StoreError> {
        Ok(self
            .undo_txns
            .read()
            .get(&id)
            .map(|m| m.len().into())
            .unwrap_or(0.into()))
    }

    pub(crate) fn get_undo_operations(
        &self,
        id: TransactionId,
    ) -> Result<Vec<Operation>, StoreError> {
        // A transaction that never wrote anything (e.g. read-only — only
        // `find()` calls) has no entry here at all. That's not an error: it
        // just means there's nothing to undo/cleanup. Db::commit/Db::rollback
        // rely on this returning `Ok` so they can reach their final
        // tx_mgr.commit/rollback call and actually deactivate the
        // transaction — see Transaction::into_id.
        Ok(self.undo_txns.read().get(&id).cloned().unwrap_or_default())
    }

    pub(crate) fn find_undo_tuple(&self, id: TransactionId, undo_id: UndoId) -> Option<Tuple> {
        let map = self.undo_txns.read();

        let o = map.get(&id).and_then(|v| v.get(undo_id.0 as usize));
        if let Some(op) = o {
            let tuple = match op {
                Operation::Add(_, r) | Operation::Del(_, r) | Operation::Mod(_, r) => {
                    Some(r.tuple.clone())
                }
                _ => None,
            };
            return tuple;
        }
        None
    }

    pub(crate) fn log_redo(&self, op: Operation) -> Result<LsnId, StoreError> {
        let lsn_id = self.clock.next_lsn();
        let msg = MsgType::Redo(RedoOperation {
            lsn_id,
            operation: op,
        });

        if let Some(tx) = &self.redo_tx {
            tx.send(msg)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(lsn_id)
    }

    pub(crate) fn checkpoint(&self, ts: u128) -> Result<(), StoreError> {
        let msg = MsgType::Checkpoint(ts);
        if let Some(tx) = &self.redo_tx {
            tx.send(msg.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(tx) = &self.undo_tx {
            tx.send(msg.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn shutdown(self) -> Result<(), StoreError> {
        if let Some(tx) = self.redo_tx {
            tx.send(MsgType::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(tx) = self.undo_tx {
            tx.send(MsgType::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(h) = self.redo_handle {
            match h.join() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining redo.Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }
        if let Some(h) = self.undo_handle {
            match h.join() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining undo.Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }

        Ok(())
    }
}

impl Record {
    pub(crate) fn new(table_id: TableIdType, tuple: Tuple) -> Self {
        Self {
            table_id,
            tuple,
            timestamp: timestamp(),
        }
    }
}

impl Operation {
    pub(crate) fn new_add(txn: TransactionId, record: Record) -> Self {
        Self::Add(txn, record)
    }

    pub(crate) fn new_del(txn: TransactionId, record: Record) -> Self {
        Self::Del(txn, record)
    }

    pub(crate) fn new_mod(txn: TransactionId, record: Record) -> Self {
        Self::Mod(txn, record)
    }

    pub(crate) fn new_commit(tx_id: TransactionId) -> Self {
        Self::Commit(tx_id, timestamp())
    }

    pub(crate) fn new_rollback(tx_id: TransactionId) -> Self {
        Self::Rollback(tx_id, timestamp())
    }
}

impl MsgType {
    pub(crate) fn new_undo(operation: UndoOperation) -> Self {
        Self::Undo(operation)
    }

    pub(crate) fn new_redo(operation: RedoOperation) -> Self {
        Self::Redo(operation)
    }
}

impl PartialOrd<u64> for LsnId {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(other))
    }
}

impl PartialEq<u64> for LsnId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}
fn undo_log_runner(file: impl DBFile, recv: Receiver<MsgType>) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        let msg = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        match msg {
            MsgType::ShutDown => {
                break;
            }
            MsgType::Undo(undo_op) => {
                // Write the full MsgType wrapper, not just the inner
                // UndoOperation: load_logs (and any future recovery replay)
                // needs a uniform, self-describing tag on every record so
                // it can tell Undo/Redo/Checkpoint entries apart while
                // scanning the file — Checkpoint already did this
                // correctly; a plain UndoOperation on its own can't be
                // told apart from one by a reader that doesn't already
                // know where the record boundaries are.
                file.seek(SeekFrom::End(0))?;
                file.write_all(&to_allocvec(&MsgType::Undo(undo_op))?)?;
            }
            MsgType::Redo(r) => panic!("Unexpected redo in undo loop {:?}", r),
            MsgType::Checkpoint(_ts) => {
                file.seek(SeekFrom::End(0))?;
                file.write_all(&to_allocvec(&msg)?)?;
            }
        }
    }
    Ok(())
}

fn redo_log_runner(
    file: impl DBFile,
    recv: Receiver<MsgType>,
    clock: Arc<LsnClock>,
) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        let msg = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        match msg {
            MsgType::ShutDown => {
                break;
            }
            MsgType::Redo(redo_op) => {
                // See undo_log_runner's matching comment: write the full
                // MsgType wrapper, not just the inner RedoOperation.
                let lsn_id = redo_op.lsn_id;
                file.seek(SeekFrom::End(0))?;
                file.write_all(&to_allocvec(&MsgType::Redo(redo_op))?)?;
                clock.mark_written(lsn_id);
            }
            MsgType::Undo(u) => panic!("Unexpected undo in redo loop {:?}", u),
            MsgType::Checkpoint(_ts) => {
                file.seek(SeekFrom::End(0))?;
                file.write_all(&to_allocvec(&msg)?)?;
            }
        }
    }
    Ok(())
}

impl From<usize> for UndoId {
    fn from(value: usize) -> Self {
        Self(value as u16)
    }
}

#[cfg(test)]
mod tests {

    use super::UndoId;
    use crate::{
        logger::{Logger, Operation, Record},
        memfile::MemFile,
        tuple::Tuple,
        txn::TransactionId,
    };

    #[test]
    fn test_log_redo_returns_incrementing_lsn() {
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        let lsn1 = logger.log_redo(op.clone()).unwrap();
        let lsn2 = logger.log_redo(op).unwrap();
        assert!(lsn2 > lsn1);
    }

    #[test]
    fn test_log_redo_unique_lsns() {
        let logger = Logger::new();
        let mut lsns = Vec::new();
        for i in 0..5 {
            let op = Operation::new_commit(TransactionId::from(i));
            lsns.push(logger.log_redo(op).unwrap());
        }
        let unique: std::collections::HashSet<_> = lsns.iter().cloned().collect();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn test_log_undo_commit_op_no_db() {
        // Without set_db, log_undo should succeed (undo_tx is None, msg is silently dropped)
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_rollback_op_no_db() {
        let logger = Logger::new();
        let op = Operation::new_rollback(TransactionId::from(2));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_add_op_with_txn_id() {
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let mut tuple = Tuple::new(1, b"hello");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_indexes_by_operation_txn_not_tuple_txn_id() {
        // log_undo indexes by the Operation's own txn id, not record.tuple.txn_id.
        // This matters because Mod/Del undo records deliberately carry a
        // pre-image tuple tagged with a *different* (older, committed) txn_id
        // than the operation being logged. A tuple with no txn_id at all (as
        // here) must therefore still log successfully.
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let tuple = Tuple::new(1, b"hello"); // txn_id not set
        let record = Record::new(0.into(), tuple);
        let op = Operation::new_add(txn_id.clone(), record);
        assert!(logger.log_undo(op).is_ok());
        assert_eq!(logger.next_undo_id(txn_id).unwrap(), 1.into());
    }

    #[test]
    fn test_logger_with_memfile_shutdown() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId::from(99);
        let op = Operation::new_commit(txn_id);
        logger.log_redo(op.clone()).unwrap();
        logger.log_undo(op).unwrap();
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_logger_undo_add_with_db() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId::from(5);
        let mut tuple = Tuple::new(42, b"data");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_tuple_new_in_txn() {
        use crate::tuple::Tuple;
        // UndoId is only constructable from within logger module
        let txn = TransactionId::from(42);
        let undo = UndoId(7);
        let t = Tuple::new_with(
            crate::tuple::DBIdType::Int(1),
            b"hello",
            Some(txn.clone()),
            Some(undo),
        );
        assert_eq!(t.txn_id, Some(txn.clone()));
        assert_eq!(t.undo_id, Some(undo));
        assert_eq!(t.data.to_vec(), b"hello");
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.txn_id, Some(txn));
        assert_eq!(t2.data.to_vec(), b"hello");
    }

    // --- crash-recovery log loading (mmap-backed File, and MemFile) ---

    fn temp_log_paths(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        (
            dir.join(format!("squeal_logger_test_{tag}_{pid}_redo.log")),
            dir.join(format!("squeal_logger_test_{tag}_{pid}_undo.log")),
        )
    }

    fn open_fresh(path: &std::path::Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    }

    fn open_readonly(path: &std::path::Path) -> std::fs::File {
        std::fs::OpenOptions::new().read(true).open(path).unwrap()
    }

    #[test]
    fn test_load_logs_empty_memfiles_returns_zero_counts() {
        let logger = Logger::new();
        let (redo_count, undo_count) = logger
            .load_logs(&MemFile::new(), &MemFile::new())
            .unwrap();
        assert_eq!((redo_count, undo_count), (0, 0));
    }

    // mmap-ing a zero-length file is a classic edge case (some mmap
    // implementations error on it) — exercised here specifically because
    // it's the state of every brand-new database's log files, before a
    // single record has ever been written.
    #[test]
    fn test_load_logs_empty_files_returns_zero_counts_no_mmap_panic() {
        let (redo_path, undo_path) = temp_log_paths("empty");
        let redo_file = open_fresh(&redo_path);
        let undo_file = open_fresh(&undo_path);

        let logger = Logger::new();
        let result = logger.load_logs(&undo_file, &redo_file);

        let _ = std::fs::remove_file(&redo_path);
        let _ = std::fs::remove_file(&undo_path);

        assert_eq!(result.unwrap(), (0, 0));
    }

    #[test]
    fn test_load_logs_recovers_correct_record_counts_memfile() {
        let redo_file = MemFile::new();
        let undo_file = MemFile::new();

        // First "session": write some records, then shut down cleanly so
        // everything is flushed through the runner threads before we try
        // to read it back.
        let mut logger = Logger::new();
        logger
            .set_db(undo_file.clone(), redo_file.clone())
            .unwrap();
        for i in 0..4u64 {
            logger
                .log_redo(Operation::new_commit(TransactionId::from(i)))
                .unwrap();
        }
        for i in 0..3u64 {
            logger
                .log_undo(Operation::new_commit(TransactionId::from(100 + i)))
                .unwrap();
        }
        logger.shutdown().unwrap();

        // Second "session": a fresh Logger reading the same backing
        // buffers (MemFile::clone shares the underlying Arc<RwLock<..>>),
        // simulating what set_db does when a Db is reopened.
        let reopened = Logger::new();
        let (redo_count, undo_count) = reopened.load_logs(&undo_file, &redo_file).unwrap();
        assert_eq!((redo_count, undo_count), (4, 3));
    }

    #[test]
    fn test_load_logs_recovers_correct_record_counts_file_mmap() {
        let (redo_path, undo_path) = temp_log_paths("counts");

        let mut logger = Logger::new();
        logger
            .set_db(open_fresh(&undo_path), open_fresh(&redo_path))
            .unwrap();
        for i in 0..5u64 {
            logger
                .log_redo(Operation::new_commit(TransactionId::from(i)))
                .unwrap();
        }
        for i in 0..2u64 {
            logger
                .log_undo(Operation::new_commit(TransactionId::from(200 + i)))
                .unwrap();
        }
        logger.shutdown().unwrap();

        // Reopen the same on-disk files fresh, as if the process had
        // restarted — this is what actually exercises the mmap path
        // (load_logs only maps std::fs::File, not MemFile).
        let reopened = Logger::new();
        let result = reopened.load_logs(&open_readonly(&undo_path), &open_readonly(&redo_path));

        let _ = std::fs::remove_file(&redo_path);
        let _ = std::fs::remove_file(&undo_path);

        assert_eq!(result.unwrap(), (5, 2));
    }

    // Checkpoint markers are written to BOTH files (see Logger::checkpoint)
    // and interleaved with ordinary Undo/Redo records — load_logs must
    // parse straight through them as just another MsgType variant, not
    // choke on the shape difference or miscount.
    #[test]
    fn test_load_logs_counts_records_interleaved_with_checkpoints() {
        let (redo_path, undo_path) = temp_log_paths("checkpoint");

        let mut logger = Logger::new();
        logger
            .set_db(open_fresh(&undo_path), open_fresh(&redo_path))
            .unwrap();
        logger
            .log_redo(Operation::new_commit(TransactionId::from(1)))
            .unwrap();
        logger.checkpoint(1).unwrap();
        logger
            .log_redo(Operation::new_commit(TransactionId::from(2)))
            .unwrap();
        logger
            .log_undo(Operation::new_commit(TransactionId::from(3)))
            .unwrap();
        logger.checkpoint(2).unwrap();
        logger.shutdown().unwrap();

        let reopened = Logger::new();
        let result = reopened.load_logs(&open_readonly(&undo_path), &open_readonly(&redo_path));

        let _ = std::fs::remove_file(&redo_path);
        let _ = std::fs::remove_file(&undo_path);

        // redo file: 2 commits + 2 checkpoints; undo file: 1 commit + 2 checkpoints.
        assert_eq!(result.unwrap(), (4, 3));
    }

    // A restarted process appends to the same log files rather than
    // truncating them — load_logs must see everything from every prior
    // session, not just the most recent one.
    #[test]
    fn test_load_logs_accumulates_across_multiple_sessions() {
        let (redo_path, undo_path) = temp_log_paths("multisession");

        let mut logger1 = Logger::new();
        logger1
            .set_db(open_fresh(&undo_path), open_fresh(&redo_path))
            .unwrap();
        logger1
            .log_redo(Operation::new_commit(TransactionId::from(1)))
            .unwrap();
        logger1.shutdown().unwrap();

        // Reopen without truncating (as a real restart would), write more.
        let redo_append = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&redo_path)
            .unwrap();
        let undo_append = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&undo_path)
            .unwrap();
        let mut logger2 = Logger::new();
        logger2.set_db(undo_append, redo_append).unwrap();
        logger2
            .log_redo(Operation::new_commit(TransactionId::from(2)))
            .unwrap();
        logger2
            .log_redo(Operation::new_commit(TransactionId::from(3)))
            .unwrap();
        logger2.shutdown().unwrap();

        let reopened = Logger::new();
        let result = reopened.load_logs(&open_readonly(&undo_path), &open_readonly(&redo_path));

        let _ = std::fs::remove_file(&redo_path);
        let _ = std::fs::remove_file(&undo_path);

        assert_eq!(result.unwrap(), (3, 0));
    }
}
