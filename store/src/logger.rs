use std::{
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    db::{DBSizeType, Db},
    error::StoreError,
    tuple::Tuple,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum LogType {
    Undo(Operation),
    Redo(Operation),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Operation {
    Add(Record),
    Del(Record),
    Mod(Record),
    Commit(DBSizeType, u128),
    Rollback(DBSizeType, u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    table_id: DBSizeType,
    txn_id: DBSizeType,
    timestamp: u128,
    tuple: Tuple,
}

#[derive(Debug, Default)]
pub(crate) struct Logger<'a> {
    db: Option<&'a Db>,
    redo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_handle: Option<JoinHandle<Result<(), StoreError>>>,
}

impl<'a> Logger<'a> {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn set_db(&mut self, db: &'a Db) {
        self.db = Some(db);
    }
}

impl Record {
    pub(crate) fn new(table_id: DBSizeType, txn_id: DBSizeType, tuple: Tuple) -> Self {
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
    pub(crate) fn new_add(record: Record) -> Self {
        Self::Add(record)
    }

    pub(crate) fn new_del(record: Record) -> Self {
        Self::Del(record)
    }

    pub(crate) fn new_mod(record: Record) -> Self {
        Self::Mod(record)
    }

    pub(crate) fn new_commit(tx_id: DBSizeType) -> Self {
        Self::Commit(
            tx_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
    }

    pub(crate) fn new_rollback(tx_id: DBSizeType) -> Self {
        Self::Rollback(
            tx_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
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
