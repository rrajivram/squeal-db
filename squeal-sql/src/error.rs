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
    #[error("Duplicate key : {0}")]
    DuplicateKey(DBIdType),
    #[error("Invalid table name: {0}")]
    BadTableName(String),
    #[error("User error: {0}")]
    UserError(String),
    #[error("Unknown error: {0}")]
    UnknownError(String),
    #[error("Parse error")]
    ParseError(#[from] sqlparser::parser::ParserError),
    #[error("Database already in use : {0}")]
    DatabaseInUseError(String),
    #[error("Schema already exists : {0}")]
    SchemaInUseError(String),
    #[error("Schema not found : {0}")]
    SchemaNotFound(String),
    #[error("No schema selected on this connection")]
    NoSchemaSelected,
    #[error("A transaction is already active on this connection")]
    TransactionAlreadyActive,
    #[error("No active transaction on this connection")]
    NoActiveTransaction,
    #[error("Prepared statements can only have one execution statement")]
    TooManyPreparedStatement,
    #[error("Only INSERT, UPDATE, DELETE , SELECT are allowed in prepared statements, found {0}")]
    BadPreparedStatement(String),
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
            | StoreError::PageTransientlyInconsistent(_)
            | StoreError::UnknownPageContentKind(_)
            | StoreError::DuplicatePageContentKind(_)
            | StoreError::LockContentionError => Self::InternalError(value),
            // Not internal — "this value is too big to fit in its
            // declared size" (a VARCHAR/BLOB literal longer than the
            // column's declared capacity, most commonly) is something
            // the caller's own input caused and can fix, the same way
            // a NOT NULL violation or a type mismatch is. Previously
            // bucketed with the genuinely internal errors above, so it
            // surfaced as an opaque "Internal Store Error" instead of
            // a message naming the actual sizes involved.
            StoreError::TupleTooLarge(_, _) => Self::UserError(value.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    // KeyNotFound and DuplicateKey previously shared the exact same
    // #[error(...)] message ("Key not found : {0}") — a copy-paste bug
    // that `matches!(err, SchemaError::DuplicateKey(_))`-style variant
    // checks elsewhere never caught, since they don't look at Display
    // output at all. Only surfaced by a human actually reading a printed
    // error message (in squeal-cli). Guards against the two silently
    // drifting back together.
    #[test]
    fn test_duplicate_key_and_key_not_found_have_distinct_messages() {
        let id = DBIdType::Int(1);
        let not_found = SchemaError::KeyNotFound(id.clone()).to_string();
        let duplicate = SchemaError::DuplicateKey(id).to_string();
        assert_ne!(not_found, duplicate);
        assert!(
            duplicate.to_lowercase().contains("duplicate"),
            "got {duplicate:?}"
        );
    }
}
