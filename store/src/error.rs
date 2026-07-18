use std::{
    fs::TryLockError,
    sync::{
        PoisonError,
        mpsc::{RecvError, TryRecvError},
    },
};

use thiserror::Error;

use crate::{db::DBSizeType, tuple::DBIdType};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("Serialization Error")]
    SerializationError(#[from] postcard::Error),
    #[error("Bad file")]
    FileError,
    #[error("Invalid row number {0}")]
    BadRowNumber(DBSizeType),
    #[error("No space in page")]
    PageCapacityError,
    #[error("Lock contention.")]
    LockContentionError,
    #[error("Duplicate key {0}")]
    DuplicateKey(DBIdType),
    #[error("Key not found {0}")]
    KeyNotFound(DBIdType),
    #[error("Table name max length is {0}, got {1}")]
    TableNameInvalid(usize, usize),
    #[error("Unknown error {0}")]
    UnknownError(String),
    #[error("Duplicate table name {0}")]
    DuplicateName(String),
    #[error("Missing key {0}")]
    MissingKey(String),
    #[error("Tuple too large. {0}: Max : {1}")]
    TupleTooLarge(DBSizeType, usize),
    #[error("Undo log error : {0}")]
    UndoLogError(String),
    #[error("Table not found : {0}")]
    TableNotFound(String),
    // The writer thread caught a page's live Arc mid-transition: a foreground
    // write has already grown its content past page_data_size but hasn't yet
    // (a separate, later lock acquisition) flipped has_overflow/next_page to
    // match. Transient by construction — the window is a couple of lock
    // acquisitions wide — so callers should retry rather than treat this as
    // real corruption. See buffer.rs's write_page and writer's own comments.
    #[error("Page {0:?} read mid-overflow-transition, retry")]
    PageTransientlyInconsistent(crate::page::PageId),
}

impl<T> From<PoisonError<T>> for StoreError {
    fn from(value: PoisonError<T>) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}

impl From<TryLockError> for StoreError {
    fn from(value: TryLockError) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}

impl From<RecvError> for StoreError {
    fn from(value: RecvError) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}

impl From<TryRecvError> for StoreError {
    fn from(value: TryRecvError) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}
