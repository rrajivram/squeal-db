use std::sync::PoisonError;

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
}

impl<T> From<PoisonError<T>> for StoreError {
    fn from(value: PoisonError<T>) -> Self {
        StoreError::UnknownError(value.to_string())
    }
}
