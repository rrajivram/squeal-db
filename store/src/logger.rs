use std::{
    collections::HashMap,
    io::SeekFrom,
    sync::{
        Arc, RwLock,
        atomic::AtomicU64,
        mpsc::{Receiver, Sender, channel},
    },
    thread::{self, JoinHandle},
};

use log::error;
use postcard::to_allocvec;
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp,
    db::{DBFile, DBSizeType},
    error::StoreError,
    tuple::Tuple,
    txn::TransactionId,
};

static LSN_COUNTER: AtomicU64 = AtomicU64::new(0);
static LAST_WRITTEN_LSN: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct LsnId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct UndoId(pub(crate) u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum MsgType {
    Undo(UndoOperation),
    Redo(RedoOperation),
    ShutDown,
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
    table_id: DBSizeType,
    timestamp: u128,
    tuple: Arc<Tuple>,
    txn_id: TransactionId,
}

#[derive(Debug, Default)]
pub(crate) struct Logger {
    redo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_tx: Option<Sender<MsgType>>,
    redo_tx: Option<Sender<MsgType>>,
    undo_txns: RwLock<HashMap<TransactionId, Vec<Operation>>>,
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
        let (redo_tx, redo_rx) = channel();
        let (undo_tx, undo_rx) = channel();
        self.undo_tx = Some(undo_tx);
        self.redo_tx = Some(redo_tx);

        self.undo_handle = Some(thread::spawn(move || undo_log_runner(undo_file, undo_rx)));
        self.redo_handle = Some(thread::spawn(move || redo_log_runner(redo_file, redo_rx)));
        Ok(())
    }

    pub(crate) fn log_undo(&self, op: Operation) -> Result<(), StoreError> {
        let msg = match &op {
            Operation::Add(_, record) | Operation::Del(_, record) | Operation::Mod(_, record) => {
                let mut undo_txns = self.undo_txns.write()?;
                let id = undo_txns
                    .entry(
                        record
                            .tuple
                            .txn_id
                            .ok_or(StoreError::UnknownError("Missing transaction".into()))?,
                    )
                    .and_modify(|v| v.push(op.clone()))
                    .or_insert(vec![op.clone()]);
                MsgType::Undo(UndoOperation {
                    undo_id: Some(UndoId(id.len() as u16)),
                    operation: op,
                })
            }
            _ => MsgType::Undo(UndoOperation {
                undo_id: None,
                operation: op,
            }),
        };
        if let Some(tx) = &self.undo_tx {
            tx.send(msg)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            // Now store record if available
        }
        Ok(())
    }

    pub(crate) fn last_lsn() -> LsnId {
        LsnId(LAST_WRITTEN_LSN.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(crate) fn log_redo(&self, op: Operation) -> Result<LsnId, StoreError> {
        let lsn_id = LsnId(LSN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::AcqRel));
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
    pub(crate) fn new(table_id: DBSizeType, tuple: Arc<Tuple>, txn_id: TransactionId) -> Self {
        Self {
            table_id,
            tuple,
            txn_id,
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
            MsgType::Undo(msg) => {
                file.seek(SeekFrom::End(0))?;
                file.write(&to_allocvec(&msg)?)?;
            }
            MsgType::Redo(r) => panic!("Unexpected redo in undo loop {:?}", r),
        }
    }
    Ok(())
}

fn redo_log_runner(file: impl DBFile, recv: Receiver<MsgType>) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        let msg = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        match msg {
            MsgType::ShutDown => {
                break;
            }
            MsgType::Redo(msg) => {
                file.seek(SeekFrom::End(0))?;
                file.write(&to_allocvec(&msg)?)?;
                LAST_WRITTEN_LSN.store(msg.lsn_id.0, std::sync::atomic::Ordering::Relaxed);
            }
            MsgType::Undo(u) => panic!("Unexpected undo in redo loop {:?}", u),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
        let op = Operation::new_commit(TransactionId(1));
        let lsn1 = logger.log_redo(op.clone()).unwrap();
        let lsn2 = logger.log_redo(op).unwrap();
        assert!(lsn2 > lsn1);
    }

    #[test]
    fn test_log_redo_unique_lsns() {
        let logger = Logger::new();
        let mut lsns = Vec::new();
        for i in 0..5 {
            let op = Operation::new_commit(TransactionId(i));
            lsns.push(logger.log_redo(op).unwrap());
        }
        let unique: std::collections::HashSet<_> = lsns.iter().cloned().collect();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn test_log_undo_commit_op_no_db() {
        // Without set_db, log_undo should succeed (undo_tx is None, msg is silently dropped)
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId(1));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_rollback_op_no_db() {
        let logger = Logger::new();
        let op = Operation::new_rollback(TransactionId(2));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_add_op_with_txn_id() {
        let logger = Logger::new();
        let txn_id = TransactionId(10);
        let mut tuple = Tuple::new(1, b"hello");
        tuple.set_txn_id(txn_id);
        let record = Record::new(0, Arc::new(tuple), txn_id);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_add_missing_txn_id_returns_err() {
        // Tuple without txn_id set → log_undo should return an error
        let logger = Logger::new();
        let txn_id = TransactionId(10);
        let tuple = Tuple::new(1, b"hello"); // txn_id not set
        let record = Record::new(0, Arc::new(tuple), txn_id);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_err());
    }

    #[test]
    fn test_logger_with_memfile_shutdown() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId(99);
        let op = Operation::new_commit(txn_id);
        logger.log_redo(op.clone()).unwrap();
        logger.log_undo(op).unwrap();
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_logger_undo_add_with_db() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId(5);
        let mut tuple = Tuple::new(42, b"data");
        tuple.set_txn_id(txn_id);
        let record = Record::new(0, Arc::new(tuple), txn_id);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_tuple_new_in_txn() {
        use crate::tuple::Tuple;
        // UndoId is only constructable from within logger module
        let txn = TransactionId(42);
        let undo = UndoId(7);
        let t = Tuple::new_in_txn(crate::tuple::DBIdType::Int(1), b"hello", txn, undo);
        assert_eq!(t.txn_id, Some(txn));
        assert_eq!(t.undo_id, Some(undo));
        assert_eq!(t.data, b"hello");
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.txn_id, Some(txn));
        assert_eq!(t2.data, b"hello");
    }
}
