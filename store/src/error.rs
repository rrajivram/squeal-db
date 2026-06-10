use thiserror::Error;

use crate::db::DBSizeType;

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
    DuplicateKey(DBSizeType),
    #[error("Key not found {0}")]
    KeyNotFound(DBSizeType),
}
