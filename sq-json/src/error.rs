use store::error::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid JSON: {0}")]
    Json(String),
    /// A malformed query, update, projection or document (MongoDB's
    /// BadValue / FailedToParse).
    #[error("{0}")]
    BadValue(String),
    /// A unique index (or `_id`) already holds this key (MongoDB's E11000).
    #[error("E11000 duplicate key error collection: {ns} index: {index} dup key: {key}")]
    DuplicateKey {
        ns: String,
        index: String,
        key: String,
    },
    /// Another transaction changed this document first; retry.
    #[error("write conflict: another transaction modified this document; retry")]
    WriteConflict,
    #[error("index key too large: {0} bytes (at most {1})")]
    KeyTooLarge(usize, usize),
    #[error("cannot modify the immutable field '_id'")]
    ImmutableId,
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("{0}")]
    Transaction(String),
    #[error("store error: {0}")]
    Store(StoreError),
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::WriteConflict(_) | StoreError::WriteConflictTransactionAborted(_) => {
                Error::WriteConflict
            }
            other => Error::Store(other),
        }
    }
}

impl From<postcard::Error> for Error {
    fn from(e: postcard::Error) -> Self {
        Error::Store(StoreError::SerializationError(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
