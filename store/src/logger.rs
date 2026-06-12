use std::{
    collections::HashMap,
    fs::File,
    io::{Seek, SeekFrom, Write},
    sync::{
        RwLock,
        mpsc::{Receiver, Sender, channel},
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use log::error;
use postcard::to_allocvec;
use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, Db},
    error::StoreError,
    tuple::Tuple,
    txn::TransactionId,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum LogType {
    Undo(Operation),
    Redo(Operation),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Operation {
    Add(TransactionId, Record),
    Del(TransactionId, Record),
    Mod(TransactionId, Record),
    Commit(TransactionId, u128),
    Rollback(TransactionId, u128),
    CreateTable(String, DBSizeType),
    DropTable(String, DBSizeType),
    ShutDown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    table_id: DBSizeType,
    timestamp: u128,
    tuple: Tuple,
    txn_id: TransactionId,
}

#[derive(Debug, Default)]
pub(crate) struct Logger<'a> {
    db: Option<&'a Db>,
    redo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_tx: Option<Sender<Operation>>,
    redo_tx: Option<Sender<Operation>>,
    undo_logs: RwLock<HashMap<DBSizeType, Vec<Operation>>>,
}

impl<'a> Logger<'a> {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn set_db(&mut self, db: &'a Db) -> Result<(), StoreError> {
        self.db = Some(db);
        let (redo_tx, redo_rx) = channel();
        let (undo_tx, undo_rx) = channel();
        self.undo_tx = Some(undo_tx);
        self.redo_tx = Some(redo_tx);
        let undo_file = db.undo_file.try_clone()?;
        let redo_file = db.redo_file.try_clone()?;

        self.undo_handle = Some(thread::spawn(move || log_runner(undo_file, undo_rx)));
        self.redo_handle = Some(thread::spawn(move || log_runner(redo_file, redo_rx)));
        Ok(())
    }

    pub(crate) fn log_undo(&self, op: Operation) -> Result<(), StoreError> {
        let record_op = match &op {
            Operation::Add(_, record) | Operation::Del(_, record) | Operation::Mod(_, record) => {
                Some(record)
            }
            _ => None,
        };
        if let Some(tx) = &self.undo_tx {
            tx.send(op.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            // Now store record if available
            if let Some(record) = record_op {
                self.undo_logs
                    .write()?
                    .entry(record.table_id)
                    .and_modify(|v| v.push(op.clone()))
                    .or_insert(vec![op.clone()]);
            }
            // Remove undo logs on commit and rollback
            match op {
                Operation::Add(txn, _) | Operation::Rollback(txn, _) => {
                    let mut map = self.undo_logs.write()?;
                    //map.values_mut().
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn log_redo(&self, op: Operation) -> Result<(), StoreError> {
        if let Some(tx) = &self.redo_tx {
            tx.send(op.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn shutdown(self) -> Result<(), StoreError> {
        if let Some(tx) = self.redo_tx {
            tx.send(Operation::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(tx) = self.undo_tx {
            tx.send(Operation::ShutDown)
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
    pub(crate) fn new(table_id: DBSizeType, tuple: Tuple, txn_id: TransactionId) -> Self {
        Self {
            table_id,
            tuple,
            txn_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
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
        Self::Commit(
            tx_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
    }

    pub(crate) fn new_rollback(tx_id: TransactionId) -> Self {
        Self::Rollback(
            tx_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
    }

    pub(crate) fn new_table(name: String, id: DBSizeType) -> Self {
        Self::CreateTable(name, id)
    }

    pub(crate) fn drop_table(name: String, id: DBSizeType) -> Self {
        Self::DropTable(name, id)
    }
}

impl LogType {
    pub(crate) fn new_undo(operation: Operation) -> Self {
        Self::Undo(operation)
    }

    pub(crate) fn new_redo(operation: Operation) -> Self {
        Self::Redo(operation)
    }
}

fn log_runner(file: File, recv: Receiver<Operation>) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        let msg = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        match msg {
            Operation::ShutDown => {
                break;
            }
            _ => {
                file.seek(SeekFrom::End(0))?;
                file.write(&to_allocvec(&msg)?)?;
            }
        }
    }
    Ok(())
}
