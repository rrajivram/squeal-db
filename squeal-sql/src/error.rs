use store::{error::StoreError, tuple::DBIdType};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("IO Error")]
    IoError(StoreError),
    #[error("Internal Store Error")]
    InternalError(StoreError),
    #[error("Key not found : {0}")]
    KeyNotFound(DBIdType),
    #[error("Key not found : {0}")]
    DuplicateKey(DBIdType),
    #[error("Invalid table name: {0}")]
    BadTableName(String),
    #[error("User error: {0}")]
    UserError(String),
    #[error("Unknown error: {0}")]
    UnknownError(String),
    #[error("Parse error")]
    ParseError(#[from] sqlparser::parser::ParserError),
}

impl From<StoreError> for SchemaError {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::IoError(_) | StoreError::SerializationError(_) | StoreError::FileError => {
                Self::IoError(value)
            }
            StoreError::BadRowNumber(_)
            | StoreError::PageCapacityError
            | StoreError::UnknownError(_)
            | StoreError::UndoLogError(_)
            | StoreError::MissingKey(_)
            | StoreError::TupleTooLarge(_, _)
            | StoreError::PageTransientlyInconsistent(_)
            | StoreError::LockContentionError => Self::InternalError(value),
            StoreError::DuplicateKey(dbid_type) => Self::DuplicateKey(dbid_type),
            StoreError::KeyNotFound(dbid_type) => Self::KeyNotFound(dbid_type),
            StoreError::TableNameInvalid(_, _)
            | StoreError::TableNotFound(_)
            | StoreError::DuplicateName(_) => Self::BadTableName(value.to_string()),
        }
    }
}

impl From<postcard::Error> for SchemaError {
    fn from(value: postcard::Error) -> Self {
        SchemaError::InternalError(value.into())
    }
}
