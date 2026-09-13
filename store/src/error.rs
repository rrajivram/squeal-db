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
    // Someone else's write to this exact row can't safely be built on top
    // of: its writer is still active, was active when the conflicting
    // transaction began (even if it has since committed), or began at or
    // after the conflicting transaction did. See Db::check_write_conflict
    // for the full three-way test this guards against a blind overwrite.
    #[error("Write conflict on key {0} — row was concurrently modified by another transaction")]
    WriteConflict(DBIdType),
    // Like WriteConflict, but the transaction's ConflictPolicy was
    // AbortOnConflict, so the conflict didn't just fail this one
    // operation — the entire transaction was automatically rolled back.
    // By the time this error is returned, the transaction is already
    // fully finished; any further operation against it returns
    // TransactionAlreadyFinished below.
    #[error(
        "Write conflict on key {0} — row was concurrently modified by another transaction; \
         the entire transaction was rolled back"
    )]
    WriteConflictTransactionAborted(DBIdType),
    // A write (insert/update/remove) or commit was attempted using a
    // TransactionId that is no longer active — most commonly because
    // AbortOnConflict already rolled the whole transaction back after an
    // earlier operation's WriteConflict. Distinguishes "you're trying to
    // keep using a transaction that's already gone" from a normal
    // KeyNotFound/WriteConflict on the operation itself.
    #[error("transaction is no longer active (already committed or rolled back)")]
    TransactionAlreadyFinished,
    #[error("Table name max length is {0}, got {1}")]
    TableNameInvalid(usize, usize),
    // STORE_AUDIT.md S7: the whole `__system.` namespace is reserved for
    // internal tables, not just the specific names currently in use —
    // rejected regardless of whether this particular name happens to
    // collide with a real internal table today.
    #[error("Table name {0} uses the reserved __system. prefix")]
    ReservedTableName(String),
    #[error("Unknown error {0}")]
    UnknownError(String),
    #[error("Duplicate table name {0}")]
    DuplicateName(String),
    #[error("Missing key {0}")]
    MissingKey(String),
    #[error("value too large: {0} byte(s), maximum allowed is {1} byte(s)")]
    TupleTooLarge(DBSizeType, usize),
    #[error("run page index {0} out of range (run has {1} page(s))")]
    RunPageIndexOutOfRange(usize, usize),
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
    #[error("No PageContent factory registered for kind {0}")]
    UnknownPageContentKind(u16),
    #[error("PageContent kind {0} is already registered")]
    DuplicatePageContentKind(u16),
    // The bytes read at this page's slot don't start with PAGE_MAGIC —
    // either this slot was never actually written (a corrupt/garbage
    // page id, a read racing an allocation) or its header has been
    // corrupted badly enough that trusting any other field in it would
    // be worse than refusing outright.
    #[error("Page {0:?} has an invalid magic number — not a valid page, or badly corrupted")]
    InvalidPageMagic(crate::page::PageId),
    // The header parsed fine (magic matched) but the data bytes don't
    // hash to the checksum stored alongside them — the header survived,
    // but the data didn't: truncation, a torn write, or on-disk bit rot.
    #[error("Page {0:?} failed its checksum — data is corrupted")]
    PageChecksumMismatch(crate::page::PageId),
    // STORE_AUDIT.md S3: IndexKey::from_bytes/ValueItem::from_bytes_many
    // hand-parse their own byte format (not through postcard), so a
    // truncated or malformed buffer — read straight off disk — used to
    // panic on an out-of-bounds slice index instead of surfacing as data.
    #[error("Truncated or malformed value item bytes: {0}")]
    TruncatedValueItem(String),
    // T4_S2_WAL_DESIGN.md §2: the WAL's own LogHeader (magic/version/
    // page_size) disagreed with what the paired main database file
    // expects — e.g. a log file restored from a different database, a
    // different build's WAL format, or a different page size. Refused
    // before any lock is taken and before recovery touches the file at
    // all, rather than failing deep inside decode or silently replaying
    // against the wrong page layout.
    #[error("WAL header mismatch: {0}")]
    LogHeaderMismatch(String),
    // T4_S2_WAL_DESIGN.md §3: a record's checksum failed AND a complete,
    // valid record follows it — which rules out "this is just the torn
    // tail of an in-progress write" (nothing coherent would follow that).
    // Real, mid-file corruption; recovery refuses to guess past it.
    #[error("WAL corruption: {0}")]
    LogCorruption(String),
    // STORE_AUDIT.md S1: the main file's own header had no format version,
    // no checksum, and no validation of page_size/first_page_offset before
    // this — a corrupted or foreign header decoded (or failed to decode)
    // in whatever way postcard happened to produce, with a bogus page_size
    // potentially driving a huge allocation downstream instead of being
    // refused up front with a clear cause.
    #[error("Header corruption: {0}")]
    HeaderCorruption(String),
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
