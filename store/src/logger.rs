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
pub(crate) struct LsnId(u64);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct UndoId(u16);

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
