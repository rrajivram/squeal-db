#![allow(private_bounds)]
use crate::buffer::PageBuffer;
use crate::constant::FIRST_USER_PAGE;
use crate::constant::FREE_PAGE_TABLE_PAGE;
use crate::constant::GENERATOR_TABLE_PAGE;
use crate::constant::MAX_TABLE_NAME_LEN;
use crate::constant::RESERVED_TABLE_NAME_PREFIX;
use crate::constant::SYSTEM_TABLE_NAME;
use crate::constant::SYSTEM_TABLE_PAGE;
use crate::constant::timestamp;
use crate::cursor::RangeCursor;
use crate::cursor::TableCursor;
use crate::error::StoreError;
use crate::generator::Generator;
use crate::logger::LogRecord;
use crate::logger::Logger;
use crate::logger::LsnId;
use crate::logger::Operation;
use crate::logger::Record;
use crate::logger::ScannedLog;
use crate::logger::read_and_validate_log_header;
use crate::logger::scan_log;
use crate::logger::write_log_header;
use crate::logger::{Segment, list_segments, segment_path, segment_prefix};
use crate::maintenance::Maintenance;
use crate::memfile::MemFile;
use crate::page::Page;
use crate::page::PageId;
use crate::run::Run;
use crate::table::Table;
use crate::table::TableIdType;
use crate::tables::bplustree;
use crate::tables::bplustree::BPlusTree;
use crate::tables::bplustree::Decision;
use crate::tables::bplustree::Written;
use crate::tuple::DBIdType;
use crate::tuple::Tuple;
use crate::txn::ConflictPolicy;
use crate::txn::Transaction;
use crate::txn::TransactionId;
use crate::txn::TransactionManager;
use crate::txn::TxnSink;
use crate::utils::shardedmap::ShardedMap;
use crate::version::Tombstone;
use crate::version::VersionStore;
use log::LevelFilter;
use log::info;
use parking_lot::ArcRwLockReadGuard;
use parking_lot::RawRwLock;
use parking_lot::RwLock;
use portable_atomic::AtomicU128;
use postcard::from_bytes;
use postcard::to_allocvec;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::fs::remove_file;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

const RDB_MAGIC: u16 = 0x5365;
const MAGIC: [u8; 2] = [0x53, 0x65];
const ZERO_PAGE_SIZE: DBSizeType = 8 * 1024;
const DEFAULT_PAGE_SIZE: DBSizeType = 16 * 1024;
// Cap on pages the writer thread will hold in memory awaiting durable redo
// before applying backpressure to callers (see PageBuffer's writer). Not
// persisted — purely a runtime memory/throughput knob, safe to pick freshly
// on every open. Matches PageBuffer's existing page-cache size (max_entries)
// as a reasonable default order of magnitude.
// How often the maintenance thread runs a pass when nothing wakes it.
const MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
// WAL growth that triggers a checkpoint (checked by the maintenance thread).
const CHECKPOINT_LOG_BYTES: u64 = 16 * 1024 * 1024;
// Phase 5: how many times the maintenance thread retries an abort whose
// revert failed before the engine goes Degraded.
const ABORT_RETRY_BUDGET: u32 = 3;
// Phase 6: dirty pages reach disk only at a checkpoint, so the cache also
// checkpoints once this many are waiting (bounds memory between
// checkpoints independently of WAL growth).
const CHECKPOINT_DIRTY_PAGES: usize = 4096;
// Phase 7: what a long-lived transaction may pin before the engine aborts
// it with SnapshotTooOld (see `Db::set_snapshot_limits`).
const DEFAULT_MAX_RETAINED_WAL_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_VERSION_RECORDS: usize = 1_000_000;

/// How `Db::commit_with` waits for durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Return only once the Commit record is fsynced (the default, and
    /// what `Db::commit` does).
    Sync,
    /// Return as soon as the Commit record is queued. The transaction is
    /// committed for every reader at once, and becomes durable with the
    /// next log sync — a crash before that loses it, and only it and any
    /// later commit. For bulk loads that re-run on failure.
    Async,
}

/// Phase 7 caps on what a long-lived transaction may pin. Either one
/// exceeded makes the maintenance thread abort the oldest active
/// transaction with `SnapshotTooOld`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    pub max_retained_wal_bytes: u64,
    pub max_version_records: usize,
}

pub type FileDB = Db<File>;
pub struct Meta {
    pub len: u64,
}

pub trait Opener: Any {
    type Item;
    fn open<P: AsRef<Path>>(op: OpenOptions, p: P) -> std::io::Result<Self::Item>;
    fn truncate(&mut self) -> std::io::Result<()>;
    fn do_sync(&mut self) -> std::io::Result<()>;
    fn do_clone(&self) -> std::io::Result<Self::Item>;
    fn get_metadata(&self) -> std::io::Result<Meta>;
    fn do_lock(&self) -> Result<(), TryLockError>;
    fn as_any(&self) -> &dyn Any;

    /// Positioned read: fills as much of `buf` as available starting at
    /// `offset`, returning the number of bytes actually read (0 at EOF) —
    /// same partial-transfer contract as `Read::read`. Does not use or affect
    /// any shared seek cursor: `do_clone()`'d handles to the same underlying
    /// file (e.g. `std::fs::File::try_clone`) share their OS-level cursor, so
    /// a `seek` on one silently moves the position under a concurrent
    /// `seek`+`read`/`write` on another — this is what let `PageBuffer`'s
    /// `self_file` and its background writer thread's independently-cloned
    /// handle race each other into misaligned reads/writes of the wrong file
    /// offset. Positioned I/O (pread/pwrite) sidesteps that entirely: every
    /// call is self-contained and safe to run concurrently across clones.
    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;

    /// Positioned write — see `pread`. Returns bytes actually written (same
    /// partial-transfer contract as `Write::write`).
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;

    // TXN_SIMPLIFICATION_PLAN.md phase 6: the WAL is a set of segment files
    // next to the database file (`<name>.wal.<n>`). The engine manages them
    // through whichever backend `F` is, from a handle it already holds — a
    // real file's siblings live in its directory, an in-memory file's in
    // the namespace it was created in — so tests and the crash harness
    // never touch the real filesystem.

    /// Opens `path` in the same namespace as `self`.
    fn open_sibling(&self, path: &str, op: OpenOptions) -> std::io::Result<Self::Item>;
    /// Every path in `self`'s namespace that starts with `prefix`, any order.
    fn list_siblings(&self, prefix: &str) -> std::io::Result<Vec<String>>;
    /// Removes `path` from `self`'s namespace; a missing path is not an error.
    fn remove_sibling(&self, path: &str) -> std::io::Result<()>;
}

// Opener<Item = Self>, not just Opener: every real implementor (MemFile,
// std::fs::File, NamedMemFile — see their own `impl Opener` blocks)
// already has Item = Self, so this makes that already-universal fact
// part of DBFile's own contract instead of leaving it as a separate
// bound (`F: DBFile<Item = F>`) that every generic struct/impl touching
// a DBFile has to restate by hand to use anything gated on it (e.g.
// PageBuffer's page I/O methods, which need Item = F to open/clone the
// underlying file). Without this, adding a single new struct in that
// dependency chain that needs Item = F (see store::run::RunPages) forces
// every OTHER generic type that merely stores one of that struct's
// ancestors as a field to restate the bound too, cascading arbitrarily
// far up an unrelated type graph just to keep each struct's own
// declaration well-formed.
pub trait DBFile:
    std::io::Write
    + std::io::Read
    + std::io::Seek
    + std::marker::Send
    + std::marker::Sync
    + Opener<Item = Self>
{
}
pub(crate) type DBSizeType = u64;

impl<T> DBFile for T where
    T: std::io::Write
        + std::io::Read
        + std::io::Seek
        + std::marker::Send
        + std::marker::Sync
        + Opener<Item = T>
{
}

// STORE_AUDIT.md S1: format_version + header_checksum, plus validation of
// page_size/first_page_offset on open (see Header::validate). Scoped to
// exactly this per the design doc's own decision — NOT the audit's fuller
// double-buffered-alternating-header-slots recommendation, since T5's
// write_header_synced already closes the specific "torn header write"
// race that double-buffering primarily exists to survive; the double-slot
// mechanism would be belt-and-suspenders on top of that, not something
// closing a live bug, so it's deferred (see audit-progress.md).
const HEADER_FORMAT_VERSION: u32 = 4;
const MIN_PAGE_SIZE: DBSizeType = 4 * 1024;
const MAX_PAGE_SIZE: DBSizeType = 1024 * 1024;
// Persistence versioning Stage 3: the database-wide, per-file cap on a
// B-link tree page's `high_key` (see PageHeader — the one variable-size
// field in the page header, bounded by a column's declared capacity via
// ValueItem::validate, summed across an index's key columns). Chosen at
// creation time like page_size, persisted here, validated below.
pub(crate) const DEFAULT_MAX_INDEX_KEY_SIZE: DBSizeType = 512;
const MIN_MAX_INDEX_KEY_SIZE: DBSizeType = 64;
const MAX_MAX_INDEX_KEY_SIZE: DBSizeType = 8 * 1024;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Header {
    magic: [u8; 2],
    #[serde(with = "postcard::fixint::le")]
    pub(crate) format_version: u32,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) first_page_offset: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_count: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    pub(crate) page_size: DBSizeType,
    pub(crate) last_checkpoint: u128,
    // TXN_SIMPLIFICATION_PLAN.md phase 1: the clock's next value as of the
    // last checkpoint/close — the floor the single counter (LSNs AND
    // transaction ids) is seeded from on reopen, so no number below it is
    // ever reissued even if the log has been truncated to nothing.
    #[serde(with = "postcard::fixint::le")]
    pub(crate) counter: u64,
    // Phase 6/7: the retention floor of the last completed checkpoint. Every
    // record below it belongs to a transaction that finished before that
    // checkpoint's capture, whose pages that checkpoint wrote — so recovery
    // skips such records even if a retained segment still holds them (a
    // segment is kept whole while anything in it is at or above a floor).
    #[serde(with = "postcard::fixint::le")]
    pub(crate) checkpoint_lsn: u64,
    // Persistence versioning Stage 3 (format_version 4): see
    // DEFAULT_MAX_INDEX_KEY_SIZE above. Absent in format_version 3 files —
    // HeaderV3Shape::upgrade defaults it to DEFAULT_MAX_INDEX_KEY_SIZE.
    #[serde(with = "postcard::fixint::le")]
    pub(crate) max_index_key_size: DBSizeType,
    // Always the last field: computed over every OTHER field's bytes (see
    // checksum_input) and patched in via seal() right before a write —
    // never meaningful to read until seal() has run.
    #[serde(with = "postcard::fixint::le")]
    pub(crate) header_checksum: u32,
}

// Persistence versioning Stage 3: format_version 3's exact on-disk shape,
// frozen forever (never touched again once superseded — see
// PERSISTENCE_VERSIONING_PROGRESS.md). Every format_version-3 file this
// engine will ever encounter was written with exactly these fields, in
// exactly this order; decode dispatches here by format_version alone
// (Header::decode) rather than trying to read every version through one
// derive with optional/defaulted fields, which postcard has no concept of.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct HeaderV3Shape {
    magic: [u8; 2],
    #[serde(with = "postcard::fixint::le")]
    format_version: u32,
    #[serde(with = "postcard::fixint::le")]
    first_page_offset: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_count: DBSizeType,
    #[serde(with = "postcard::fixint::le")]
    page_size: DBSizeType,
    last_checkpoint: u128,
    #[serde(with = "postcard::fixint::le")]
    counter: u64,
    #[serde(with = "postcard::fixint::le")]
    checkpoint_lsn: u64,
    #[serde(with = "postcard::fixint::le")]
    header_checksum: u32,
}

impl HeaderV3Shape {
    // Frozen copy of what Header::checksum_input computed back when format
    //_version 3 was current — must never change, even if Header's own
    // checksum_input changes shape for a later version.
    fn checksum_input(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(32);
        v.extend_from_slice(&self.magic);
        v.extend_from_slice(&self.format_version.to_le_bytes());
        v.extend_from_slice(&self.first_page_offset.to_le_bytes());
        v.extend_from_slice(&self.page_count.to_le_bytes());
        v.extend_from_slice(&self.page_size.to_le_bytes());
        v.extend_from_slice(&self.last_checkpoint.to_le_bytes());
        v.extend_from_slice(&self.counter.to_le_bytes());
        v.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        v
    }

    // Validates this v3 header against ITS OWN (frozen) checksum formula
    // and page_size bounds, then upgrades it into a current Header with
    // max_index_key_size defaulted — the concrete proof this stage's own
    // versioning works: a v3 file (missing the field entirely) opens with
    // the default, not an error.
    fn validate_and_upgrade(self) -> Result<Header, StoreError> {
        let expected = crate::page::fnv1a_32(&self.checksum_input());
        if self.header_checksum != expected {
            return Err(StoreError::HeaderCorruption(format!(
                "header checksum mismatch: stored {}, computed {}",
                self.header_checksum, expected
            )));
        }
        if !self.page_size.is_power_of_two()
            || self.page_size < MIN_PAGE_SIZE
            || self.page_size > MAX_PAGE_SIZE
        {
            return Err(StoreError::HeaderCorruption(format!(
                "invalid page_size {} (must be a power of two in [{}, {}])",
                self.page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE
            )));
        }
        if self.first_page_offset < size_of::<Header>() as DBSizeType {
            return Err(StoreError::HeaderCorruption(format!(
                "first_page_offset {} is smaller than the header itself",
                self.first_page_offset
            )));
        }
        let mut header = Header {
            magic: self.magic,
            format_version: HEADER_FORMAT_VERSION,
            first_page_offset: self.first_page_offset,
            page_count: self.page_count,
            page_size: self.page_size,
            last_checkpoint: self.last_checkpoint,
            counter: self.counter,
            checkpoint_lsn: self.checkpoint_lsn,
            max_index_key_size: DEFAULT_MAX_INDEX_KEY_SIZE,
            header_checksum: 0,
        };
        header.seal();
        Ok(header)
    }
}

impl Header {
    // Explicit field-by-field bytes rather than serializing `self` with the
    // checksum zeroed out: avoids the chicken-and-egg of "the checksum's
    // own on-wire width could in principle change based on its value" that
    // a whole-struct-minus-one-field approach would have to worry about
    // (moot today since every field uses fixint's fixed width, but this
    // way it's true by construction, not by coincidence of the current
    // field types).
    fn checksum_input(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(32);
        v.extend_from_slice(&self.magic);
        v.extend_from_slice(&self.format_version.to_le_bytes());
        v.extend_from_slice(&self.first_page_offset.to_le_bytes());
        v.extend_from_slice(&self.page_count.to_le_bytes());
        v.extend_from_slice(&self.page_size.to_le_bytes());
        v.extend_from_slice(&self.last_checkpoint.to_le_bytes());
        v.extend_from_slice(&self.counter.to_le_bytes());
        v.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        v.extend_from_slice(&self.max_index_key_size.to_le_bytes());
        v
    }

    pub(crate) fn compute_checksum(&self) -> u32 {
        crate::page::fnv1a_32(&self.checksum_input())
    }

    // Persistence versioning Stage 3: the runtime-derived replacement for
    // the old global PAGE_OVERHEAD const — see page::page_overhead's own
    // comment for why size_of::<PageDto>() was never actually correct.
    // Computed from this database's own configured max_index_key_size, the
    // same way page_size already varies per database.
    pub(crate) fn page_overhead(&self) -> usize {
        crate::page::page_overhead(self.max_index_key_size)
    }

    // Must be called right before every write of a Header to disk — every
    // write site (create_core_db, Db::checkpoint, Db::close) mutates
    // page_count/last_checkpoint and then calls this before handing the
    // header to write_header/write_header_synced, so the persisted
    // checksum always matches the persisted fields.
    pub(crate) fn seal(&mut self) {
        self.header_checksum = self.compute_checksum();
    }

    // Persistence versioning Stage 3: real version dispatch, replacing the
    // old exact-match-or-fail gate. magic (2 bytes) + format_version (a
    // 4-byte fixint u32) sit at the same fixed byte offset in every format
    // version there has ever been — declared first in both HeaderV3Shape
    // and the current Header, and postcard's derive serializes struct
    // fields in declaration order regardless of Rust's in-memory layout —
    // so it's safe to peek them before deciding how to decode the rest.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let magic_bytes = bytes.get(0..2).ok_or_else(|| {
            StoreError::HeaderCorruption(format!(
                "need at least 2 byte(s) for the magic, buffer is {} byte(s)",
                bytes.len()
            ))
        })?;
        if magic_bytes != MAGIC {
            return Err(StoreError::FileError);
        }
        let version_bytes = bytes.get(2..6).ok_or_else(|| {
            StoreError::HeaderCorruption(format!(
                "need at least 6 byte(s) for magic+format_version, buffer is {} byte(s)",
                bytes.len()
            ))
        })?;
        let format_version = u32::from_le_bytes(version_bytes.try_into().unwrap());
        match format_version {
            3 => {
                let v3: HeaderV3Shape = from_bytes(bytes)?;
                v3.validate_and_upgrade()
            }
            HEADER_FORMAT_VERSION => {
                let header: Header = from_bytes(bytes)?;
                header.validate()?;
                Ok(header)
            }
            other => Err(StoreError::HeaderCorruption(format!(
                "unsupported header format_version {other} — this file may have been written \
                 by a newer, deprecated, or unrecognized build"
            ))),
        }
    }

    // Called once, right after magic+format_version dispatch (Header::decode),
    // before this Header is trusted for anything else (page_size drives
    // every subsequent I/O offset calculation, so a corrupt value here must
    // be caught before it can drive a huge or misaligned read/write
    // downstream). Only ever runs against the CURRENT format_version's
    // checksum formula — an older version validates via its own frozen
    // shape's validate_and_upgrade instead.
    fn validate(&self) -> Result<(), StoreError> {
        if self.format_version != HEADER_FORMAT_VERSION {
            return Err(StoreError::HeaderCorruption(format!(
                "unsupported header format_version {} (expected {})",
                self.format_version, HEADER_FORMAT_VERSION
            )));
        }
        let expected = self.compute_checksum();
        if self.header_checksum != expected {
            return Err(StoreError::HeaderCorruption(format!(
                "header checksum mismatch: stored {}, computed {}",
                self.header_checksum, expected
            )));
        }
        if !self.page_size.is_power_of_two()
            || self.page_size < MIN_PAGE_SIZE
            || self.page_size > MAX_PAGE_SIZE
        {
            return Err(StoreError::HeaderCorruption(format!(
                "invalid page_size {} (must be a power of two in [{}, {}])",
                self.page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE
            )));
        }
        // size_of::<Header>() is already how every write site sizes the
        // zero-padded on-disk header slot (see BufMsg::WriteHeader's
        // handling) and how open_using_with_limits sizes its initial read
        // — reusing it here as the "must be at least this big" floor keeps
        // this check consistent with what the rest of the header I/O code
        // already treats as the header's footprint.
        if self.first_page_offset < size_of::<Header>() as DBSizeType {
            return Err(StoreError::HeaderCorruption(format!(
                "first_page_offset {} is smaller than the header itself",
                self.first_page_offset
            )));
        }
        if self.max_index_key_size < MIN_MAX_INDEX_KEY_SIZE
            || self.max_index_key_size > MAX_MAX_INDEX_KEY_SIZE
        {
            return Err(StoreError::HeaderCorruption(format!(
                "invalid max_index_key_size {} (must be in [{}, {}])",
                self.max_index_key_size, MIN_MAX_INDEX_KEY_SIZE, MAX_MAX_INDEX_KEY_SIZE
            )));
        }
        Ok(())
    }
}

// Every constructor (create*/open*) returns Arc<Db<F>>, never a bare Db<F>:
// TableCursor needs to hold its own reference to the Db it's scanning (to
// resolve MVCC visibility per row via find_last_committed), so Db is meant
// to always be shared this way rather than owned outright by one caller.
// close() reflects this too — it takes Arc<Self> and unwraps it internally.
pub struct Db<F: DBFile + 'static> {
    name: String,
    pub(crate) header: Arc<Header>,
    file: F,
    pub(crate) log_file: F,
    page_count: Arc<AtomicU64>,
    tables: Arc<RwLock<HashMap<TableIdType, Arc<BPlusTree<F>>>>>,
    generator: Arc<Generator>,
    logger: Arc<Logger>,
    tx_mgr: Arc<TransactionManager>,
    buffer: Arc<PageBuffer<F>>,
    last_checkpoint: AtomicU128, // Store the actual checkopint so it can be mutated
    // TXN_SIMPLIFICATION_PLAN.md phase 3: every version a reader or a
    // rollback can still need, with retention decided by the horizon alone
    // (see version.rs). Replaces the three "defer until readers are gone"
    // queues that used to live across Logger and Db.
    versions: VersionStore,
    // The one background thread (see maintenance.rs): abort retries, vacuum,
    // checkpoint-by-log-growth.
    pub(crate) maintenance: Maintenance,
    // STORE_AUDIT.md T17: drop_table frees a table's pages back to the free
    // list while some other in-flight insert/update/remove/find against
    // that same table may still be reading/writing through those exact
    // page ids (obtained via its own earlier table_by_id call, before
    // drop_table ran) — those pages can be reused by an unrelated
    // allocation while the in-flight operation is still mid-use of them.
    // One lock per table id, created lazily: ordinary operations take the
    // read side for their whole call (see table_by_id_guarded) so the set
    // of in-flight operations against a given table can only shrink once
    // drop_table takes that table's write side; drop_table only removes
    // the table from `tables` and frees its pages after acquiring it,
    // guaranteeing no in-flight operation is (or can start) touching those
    // pages concurrently. Scoped to insert/update/remove/find only, not
    // table_scan's longer-lived TableCursor — matches the audit's own
    // framing that the current squeal-sql usage (dropping a just-created,
    // not-yet-published table after a failed CREATE TABLE) can't already
    // have a live scan against it; a long-lived scan racing a drop is the
    // same class of deferred limitation as T3's long-reader caveat.
    // STORE_AUDIT.md P2 survey: sharded (ShardedMap), same treatment as
    // ArcLock — see table_guard's own comment.
    table_locks: ShardedMap<TableIdType, Arc<RwLock<()>>>,
    // Held for the whole of checkpoint() (in addition to checkpoint_gate)
    // so a crash-simulation snapshot (Db<MemFile>::synced_snapshot) can
    // observe the data file and the log at one instant relative to a
    // checkpoint's sync-then-truncate sequence — a real power cut is one
    // instant across both files; two separate snapshot reads racing a
    // checkpoint could otherwise capture a data file from before its sync
    // and a log from after its truncation, a state no real crash produces.
    snapshot_mutex: parking_lot::Mutex<()>,
    // Phase 6: checkpoints are serialized (the maintenance thread and
    // explicit calls); they never wait for a transaction.
    checkpoint_mutex: parking_lot::Mutex<()>,
    // Phase 5: once an abort's revert has failed ABORT_RETRY_BUDGET times,
    // the engine refuses writes (reads continue) rather than spinning on a
    // transaction it cannot finish; `Some(reason)` names it.
    degraded: RwLock<Option<String>>,
    abort_attempts: parking_lot::Mutex<HashMap<TransactionId, u32>>,
    lock_timeouts: std::sync::atomic::AtomicU64,
    // Phase 7: retention caps, the transactions the maintenance thread
    // aborted for exceeding them (with the reason their owner will be
    // told), and how many times that happened.
    snapshot_limits: RwLock<SnapshotLimits>,
    forced_aborts: parking_lot::Mutex<HashMap<TransactionId, String>>,
    snapshot_too_old_aborts: std::sync::atomic::AtomicU64,
    // Records recovery replayed at open (those at or above the persisted
    // floor); 0 for a freshly created database.
    recovered_records: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fail_reverts: std::sync::atomic::AtomicBool,
}

/// A point-in-time snapshot of the engine's internal bookkeeping — what an
/// operator (or a test) needs to answer "what is it doing and what is it
/// holding on to" without a debugger. Every field is a cheap read of
/// existing state; nothing here takes a lock for longer than a lookup.
/// TXN_SIMPLIFICATION_PLAN.md phase 0.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbStats {
    /// Transactions begun and not yet committed/rolled back.
    pub active_transactions: usize,
    /// Transactions abandoned (guard dropped) whose writes are not yet
    /// physically reverted.
    pub aborting_transactions: usize,
    /// Numeric id of the oldest active transaction, if any — the reader
    /// that pins the most history.
    pub oldest_active: Option<u64>,
    /// Committed transactions still remembered because some active reader
    /// began before they committed.
    pub committed_retained: usize,
    /// In-memory version records currently retained.
    pub version_records: usize,
    /// Committed transactions whose records the horizon has not yet released.
    pub committed_awaiting_vacuum: usize,
    /// Committed deletes whose tombstone row has not yet been purged.
    pub tombstones_awaiting_purge: usize,
    /// Maintenance thread counters.
    pub maintenance_passes: u64,
    pub tombstones_purged: u64,
    pub abort_retries: u64,
    pub maintenance_errors: u64,
    pub maintenance_last_error: Option<String>,
    /// Bytes appended to the current WAL segment (a checkpoint rolls it).
    pub wal_segment_bytes: u64,
    /// Phase 6: WAL segments on disk — retained ones plus the current one.
    /// More than one after a checkpoint means a transaction that began
    /// before the older ones is still in flight.
    pub wal_segments: usize,
    /// Phase 5: page-lock waits that exceeded `lock_timeout` (each one a
    /// reported bug, never retried).
    pub lock_timeouts: u64,
    /// Phase 5: `Some(reason)` once the engine refuses writes.
    pub degraded: Option<String>,
    /// Phase 7: bytes across every WAL segment on disk — what long-lived
    /// transactions are pinning, capped by `SnapshotLimits`.
    pub wal_retained_bytes: u64,
    /// Phase 7: transactions aborted with `SnapshotTooOld`.
    pub snapshot_too_old_aborts: u64,
    /// Log records recovery replayed when this database was opened.
    pub recovered_records: usize,
    /// Bytes currently in the WAL file (header included).
    pub log_bytes: u64,
    /// Page writes the writer thread is holding back.
    /// Pages cache-resident right now.
    pub cached_pages: usize,
    /// Total pages the database file has (as tracked live, not the header).
    pub page_count: u64,
    /// Tables currently loaded.
    pub tables: usize,
}

/// What `Db::open_segments` hands back: the runner's starting state and
/// every record recovery must replay.
struct OpenedWal<F> {
    current_file: F,
    current: Segment,
    older: Vec<Segment>,
    header_bytes: Vec<u8>,
    records: Vec<LogRecord>,
}

struct NeededObjects<F: DBFile + 'static> {
    logger: Arc<Logger>,
    txn_mgr: Arc<TransactionManager>,
    buffer: Arc<PageBuffer<F>>,
}

// resolve_visible's outcome when no version satisfying its `is_visible`
// predicate was found — distinguishes two very different reasons, because
// only one of them is safe to paper over with a "just show the latest
// committed data" fallback (see Db::find_visible_to):
//   - NoAncestor: the walk reached a tuple whose own undo_id is None — a
//     genuine dead end, since only a fresh INSERT's tuple ever has no
//     undo_id (every update()/remove() always sets one). There is no
//     earlier version to find because none ever existed.
//   - MissingUndoRecord: the walk needed to go further back, but the undo
//     log entry for that step was already gone (discarded concurrently,
//     e.g. an aborting txn's undo being reverted, or the narrow
//     discard-timing race find_visible_to's own comment describes). An
//     ancestor DID exist — we just lost track of it.
enum Visibility<'a> {
    Found(Cow<'a, Tuple>),
    NoAncestor,
    MissingUndoRecord,
}

impl<F: DBFile + 'static> Db<F>
where
    F: DBFile<Item = F>,
{
    pub fn create<S: AsRef<str>>(name: S) -> Result<Arc<Self>, StoreError> {
        Self::create_with_page_size(name, DEFAULT_PAGE_SIZE)
    }

    pub fn create_with_page_size<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
    ) -> Result<Arc<Self>, StoreError> {
        Self::create_with_page_size_and_max_index_key_size(
            name,
            page_size,
            DEFAULT_MAX_INDEX_KEY_SIZE,
        )
    }

    // Persistence versioning Stage 3: lets a caller override the database-
    // wide cap on an index key's worst-case serialized size (see
    // DEFAULT_MAX_INDEX_KEY_SIZE) at creation time, the same way
    // create_with_page_size overrides page_size. create/create_with_page_size
    // both funnel through here with the default.
    pub fn create_with_page_size_and_max_index_key_size<S: AsRef<str>>(
        name: S,
        page_size: DBSizeType,
        max_index_key_size: DBSizeType,
    ) -> Result<Arc<Self>, StoreError> {
        let sf = Self::create_core_db(name.as_ref().to_string(), page_size, max_index_key_size)?;
        sf.generator.attach_logger(sf.logger.clone());
        sf.create_system_tables()?;
        let db = Arc::new(sf);
        db.maintenance.start(&db);
        Ok(db)
    }

    pub fn open_using<S: AsRef<str>>(
        name: S,
        file: F,
        log_file: F,
    ) -> Result<Arc<Self>, StoreError> {
        let mut bytes = vec![0u8; size_of::<Header>()];
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut bytes)?;
        // Persistence versioning Stage 3: Header::decode dispatches on
        // format_version (real dispatch, not an exact-match gate), and does
        // its own magic-check + validation (STORE_AUDIT.md S1's original
        // motivation: a corrupted or foreign header must be refused before
        // a bogus page_size can drive any later offset calculation).
        let header = Header::decode(&bytes)?;
        let header = Arc::new(header);
        // Phase 6: `log_file` is the handle through which the WAL segments
        // are found (its namespace: the directory for a real file). Every
        // segment's header is validated BEFORE any lock is taken, so a
        // wrongly-paired log (wrong database, WAL version, page size) is
        // refused with zero side effects.
        let mut wal = Self::open_segments(name.as_ref(), &log_file, header.page_size)?;
        // One comparison decides what recovery replays: a record below the
        // last checkpoint's floor is already on disk (see Header).
        let floor = header.checkpoint_lsn;
        let mut records = std::mem::take(&mut wal.records);
        records.retain(|r| r.lsn.0 >= floor);
        file.do_lock()?;
        let gens = Arc::new(Generator::new());
        // STORE_AUDIT.md T5 follow-up: the audit's own recommendation
        // ("derive page_count from file length instead of trusting the
        // header") was tried and reverted — it's unsound for THIS engine's
        // architecture, not just a rounding detail. write_locked_page
        // deliberately does NOT write pages to disk on every mutation; it
        // only updates the cache, deferring the actual write until
        // eviction, checkpoint, or shutdown (see its own doc comment). So
        // outside of a checkpoint/close boundary, the main file's length
        // reflects whichever pages happened to be evicted so far — sparse
        // and out of allocation order, not "every page up to the highest
        // one in use". Confirmed via a real failure, not just reasoning: a
        // file-length-derived page_count let replay route through a page
        // that was never actually flushed (all-zero bytes, no LEAF_NODE/
        // INNER_NODE flag set), panicking with "Unknown page PageId(3)" —
        // worse than the bug it was meant to fix, since the ORIGINAL
        // (stale-but-honest) header count at least never claimed a page
        // existed before it was actually durable. The specific race T5
        // describes (a stale header paired with an already-truncated log)
        // is closed by write_header_synced above instead: the header is
        // now guaranteed durable, with a page_count that accounts for
        // everything buffer.checkpoint() just flushed, before anything
        // truncates the log — so by the time a header is ever paired with
        // an empty log, it's already correct, not stale.
        let page_count = Arc::new(AtomicU64::new(header.page_count));
        let nm = Self::setup_needed_modules(
            header.clone(),
            page_count.clone(),
            file.do_clone()?,
            name.as_ref().to_string(),
            wal,
        )?;
        let sf = Self {
            last_checkpoint: AtomicU128::new(header.last_checkpoint),
            page_count,
            header,
            file,
            log_file,
            name: name.as_ref().to_string(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            // Must be the same Arc<Generator> passed to setup_needed_modules:
            // TransactionManager holds its own clone of `gens` and calls
            // gen_key(TXN_GENERATOR_NANE) on it directly. If `generator` were a
            // separate instance, load_system_tables()'s restore below would
            // never reach the generator tx_mgr actually uses, so the txn id
            // sequence would silently restart at 0 on every reopen — colliding
            // with transaction ids from the prior session.
            generator: gens,
            logger: nm.logger,
            tx_mgr: nm.txn_mgr,
            buffer: nm.buffer,
            versions: VersionStore::new(),
            maintenance: Maintenance::new(MAINTENANCE_INTERVAL),
            table_locks: ShardedMap::new(16),
            snapshot_mutex: parking_lot::Mutex::new(()),
            checkpoint_mutex: parking_lot::Mutex::new(()),
            degraded: RwLock::new(None),
            abort_attempts: parking_lot::Mutex::new(HashMap::new()),
            lock_timeouts: std::sync::atomic::AtomicU64::new(0),
            snapshot_limits: RwLock::new(SnapshotLimits {
                max_retained_wal_bytes: DEFAULT_MAX_RETAINED_WAL_BYTES,
                max_version_records: DEFAULT_MAX_VERSION_RECORDS,
            }),
            forced_aborts: parking_lot::Mutex::new(HashMap::new()),
            snapshot_too_old_aborts: std::sync::atomic::AtomicU64::new(0),
            recovered_records: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fail_reverts: std::sync::atomic::AtomicBool::new(false),
        };
        // TXN_SIMPLIFICATION_PLAN.md phase 1: seed the one counter from the
        // header's persisted floor before anything mints a number; replay
        // raises it further past whatever the log contains.
        sf.logger.clock().seed(sf.header.counter);
        sf.generator.attach_logger(sf.logger.clone());
        sf.load_system_tables()?;
        // STORE_AUDIT.md T16: reconcile the just-loaded (possibly stale)
        // free list against what's actually reachable, before replay can
        // hand out any of it. Must run after load_system_tables (needs
        // `self.tables` populated to walk them) but before load_logs
        // (replay's own insert_if_needed/alloc_page calls must never see a
        // free list that still lists a live page as available).
        sf.reconcile_free_list()?;
        let count = sf.process_log(records)?;
        info!("{count} log record(s) replayed (floor {floor})");
        sf.recovered_records
            .store(count, std::sync::atomic::Ordering::Relaxed);
        let db = Arc::new(sf);
        db.maintenance.start(&db);
        Ok(db)
    }

    /// Phase 6: finds, validates and scans every WAL segment next to the
    /// database, oldest first, through `handle`'s namespace. No segment at
    /// all means a fresh log: segment 1 is created. Returns the records of
    /// every segment concatenated in order (recovery replays them as one
    /// log) plus what the runner needs to continue appending.
    fn open_segments(
        name: &str,
        handle: &F,
        page_size: DBSizeType,
    ) -> Result<OpenedWal<F>, StoreError> {
        let segs = list_segments(handle, name)?;
        if segs.is_empty() {
            let path = segment_path(name, 1);
            let opts = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .clone();
            let mut f = handle.open_sibling(&path, opts)?;
            let header_bytes = write_log_header(&mut f, page_size)?;
            f.do_sync()?;
            return Ok(OpenedWal {
                current_file: f,
                current: Segment {
                    n: 1,
                    path,
                    max_lsn: 0,
                    bytes: header_bytes.len() as u64,
                },
                older: Vec::new(),
                header_bytes,
                records: Vec::new(),
            });
        }
        let opts = OpenOptions::new().read(true).write(true).clone();
        let mut records = Vec::new();
        let mut scanned = Vec::with_capacity(segs.len());
        let mut last_file = None;
        for (n, path) in segs {
            let mut f = handle.open_sibling(&path, opts.clone())?;
            let header_bytes = read_and_validate_log_header(&mut f, page_size)?;
            let mut bytes = Vec::new();
            f.seek(SeekFrom::Start(header_bytes.len() as u64))?;
            f.read_to_end(&mut bytes)?;
            // The torn-tail rule applies to every segment: only the last
            // can have one (a roll syncs a segment before opening the
            // next), and a clean segment simply has no tail to drop.
            let scan = scan_log(&bytes)?;
            let max_lsn = scan.records.iter().map(|r| r.lsn.0).max().unwrap_or(0);
            let size = (header_bytes.len() + bytes.len()) as u64;
            records.extend(scan.records);
            scanned.push(Segment {
                n,
                path,
                max_lsn,
                bytes: size,
            });
            last_file = Some(f);
        }
        drop(last_file);
        // Never append to a recovered segment: a crash may have left a torn
        // tail at its end, and records appended after that point would be
        // unreadable (the scan stops at the tear). A fresh segment costs one
        // file, deleted at the first checkpoint that passes it.
        let next_n = scanned.last().map(|s| s.n + 1).unwrap_or(1);
        let path = segment_path(name, next_n);
        let opts = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .clone();
        let mut f = handle.open_sibling(&path, opts)?;
        let header_bytes = write_log_header(&mut f, page_size)?;
        f.do_sync()?;
        Ok(OpenedWal {
            current_file: f,
            current: Segment {
                n: next_n,
                path,
                max_lsn: 0,
                bytes: header_bytes.len() as u64,
            },
            older: scanned,
            header_bytes,
            records,
        })
    }

    // STORE_AUDIT.md T16: the persisted free list is only ever written at
    // checkpoint/close (write_system_tables) — a transaction that takes a
    // page from it, writes, and commits (with the commit's own redo record
    // durable) between checkpoints leaves the ON-DISK free list stale: it
    // still lists that now-live page as free. On a crash-and-reopen with no
    // intervening checkpoint, the stale list would let a LATER allocation
    // hand that same page out again, silently overwriting committed data
    // that replay already restored (or is about to). A page reachable from
    // any live table's own structure is, by definition, not actually free
    // regardless of what a stale snapshot claims — remove it from the
    // loaded free list unconditionally.
    fn reconcile_free_list(&self) -> Result<(), StoreError> {
        let mut reachable: HashSet<PageId> = HashSet::new();
        // The three system pages (catalog/generator/free-list) are never
        // meant to be freed at all, but cost nothing to guard here too.
        reachable.insert(SYSTEM_TABLE_PAGE.into());
        reachable.insert(GENERATOR_TABLE_PAGE.into());
        reachable.insert(FREE_PAGE_TABLE_PAGE.into());
        for table in self.tables.read().values() {
            reachable.extend(table.all_index_page_ids()?);
            // Raw next_page-following, not the overflow-skipping
            // data_chain_next: a page's own next_page field points INTO
            // its overflow chain when it has one (data_chain_next exists
            // specifically to skip past that to the next sibling data
            // page) — so following it directly, page by page, visits
            // every physically linked page, overflow continuations
            // included, exactly like BPlusTree::free_page_chain's own
            // (destructive) walk already does for drop_table.
            //
            // Reads via read_page_header (raw header only), not get_page
            // (full content decode): an overflow *continuation* page's data
            // region is only a valid, decodable Page when reassembled
            // starting from its chain's primary (HAS_OVERFLOW) page — see
            // buffer::read_page. Calling get_page directly on a middle
            // page (IS_OVERFLOW, not HAS_OVERFLOW) tries to decode a raw
            // mid-stream byte chunk as a standalone tuple page, which
            // fails (or worse, silently "succeeds" on garbage). This walk
            // only needs next_page, which the header alone carries safely
            // for every page in the chain, continuations included.
            let mut cur = table.table.first_data_page;
            loop {
                if !reachable.insert(cur) {
                    break; // cycle guard; should be unreachable on sound data
                }
                let next = self.buffer.read_page_header(cur)?.next_page();
                if !next.is_valid_next_page() {
                    break;
                }
                cur = next;
            }
        }
        let mut free_pages = self.buffer.get_free_pages();
        free_pages.retain(|p| !reachable.contains(p));
        self.buffer.set_free_pages(free_pages);
        Ok(())
    }

    pub fn get_generator(self: &Arc<Self>) -> Arc<Generator> {
        self.generator.clone()
    }

    pub fn open<S: AsRef<str>>(name: S) -> Result<Arc<Self>, StoreError> {
        let f = OpenOptions::new()
            .create(false)
            .read(true)
            .write(true)
            .clone();
        let f = F::open(f, name.as_ref())?;
        // Phase 6: the WAL is `<name>.wal.<n>` segments. Hand open_using the
        // newest as its namespace handle; with none (a database whose log
        // was removed), the data file itself serves — open_using then
        // starts segment 1 next to it.
        let log_file = match list_segments(&f, name.as_ref())?.pop() {
            Some((_, path)) => {
                f.open_sibling(&path, OpenOptions::new().read(true).write(true).clone())?
            }
            None => f.do_clone()?,
        };
        Self::open_using(name, f, log_file)
    }

    /*
     * Close only return files used so this can be used with MemFile for testing,
     * as MemFile does not survive recreating. Similarly with open_using.
     */
    // Takes Arc<Self> (not a bare owned Db) because every constructor now
    // hands out Arc<Db<F>> — see the type's own doc comment. Closing still
    // needs unique ownership (to tear down the buffer/logger threads and
    // hand the underlying files back), so this unwraps the Arc first: it
    // errors instead of panicking if some other clone (e.g. a TableCursor,
    // or another thread) is still holding a reference.
    pub fn close(self: Arc<Self>) -> Result<(F, F), StoreError> {
        // Stop the maintenance thread first: it holds a Weak<Db> and upgrades
        // it for each pass, which would defeat try_unwrap below.
        self.maintenance.stop();
        let db = Arc::try_unwrap(self).map_err(|_| {
            StoreError::UnknownError(
                "Db::close: other Arc<Db> references still exist (e.g. a live TableCursor or \
                 another thread) — drop them before closing"
                    .into(),
            )
        })?;
        // Finish any abort whose revert failed earlier (the maintenance thread
        // would have retried it): an un-reverted aborted row would reappear as
        // committed after reopen.
        for id in db.tx_mgr.aborting_ids() {
            db.finish_abort(id)?;
        }
        // No transaction is in flight (every guard holds an Arc<Db>, and we
        // just proved ours is the last), so this checkpoint's retention
        // floor is the counter itself: every page is flushed, the header is
        // durable, and every older segment is deleted. What remains is one
        // empty segment — a reopen replays nothing.
        db.checkpoint()?;
        // Each BPlusTree in tables holds Arc<PageBuffer>, Arc<Logger>, and
        // Arc<TransactionManager>. Drop them before Arc::into_inner so the
        // reference counts reach 1 and into_inner succeeds.
        let Db {
            buffer,
            logger,
            tables,
            file,
            log_file,
            generator,
            name,
            ..
        } = db;
        drop(tables);
        // The generator (shared with callers via get_generator) holds an
        // Arc<Logger> for sequence logging; release it so the logger can be
        // uniquely owned and shut down below.
        generator.detach_logger();
        let buffer = Arc::into_inner(buffer).unwrap();
        buffer.shutdown()?;
        // Unwrapping here as the expectation is there is only this thread accessing logger
        let logger = Arc::into_inner(logger).unwrap();
        let current = logger.current_segment();
        logger.shutdown()?;
        // Hand back a handle to the live (current) segment, not the one we
        // were opened with — that one may have been deleted since.
        let log = log_file.open_sibling(
            &segment_path(&name, current),
            OpenOptions::new().read(true).write(true).clone(),
        )?;
        Ok((file, log))
    }

    pub fn page_count(&self) -> DBSizeType {
        self.page_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Structural dump of one table (index leaves, then the data chain) —
    /// see `BPlusTree::debug_dump`. Diagnostics only.
    pub fn debug_dump_table(&self, tid: TableIdType) -> Result<Vec<String>, StoreError> {
        self.table_by_id(tid)?.debug_dump()
    }

    /// See `DbStats`. Safe to call from any thread at any time.
    pub fn stats(&self) -> DbStats {
        let m = &self.maintenance.stats;
        DbStats {
            active_transactions: self.tx_mgr.active_count(),
            aborting_transactions: self.tx_mgr.aborting_count(),
            oldest_active: self.tx_mgr.oldest_active(),
            committed_retained: self.tx_mgr.committed_retained(),
            version_records: self.versions.records_len(),
            committed_awaiting_vacuum: self.versions.committed_pending(),
            tombstones_awaiting_purge: self.versions.tombstones_pending(),
            maintenance_passes: m.passes.load(std::sync::atomic::Ordering::Relaxed),
            tombstones_purged: m
                .tombstones_purged
                .load(std::sync::atomic::Ordering::Relaxed),
            abort_retries: m.abort_retries.load(std::sync::atomic::Ordering::Relaxed),
            maintenance_errors: m.errors.load(std::sync::atomic::Ordering::Relaxed),
            maintenance_last_error: m
                .last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            wal_segment_bytes: self.logger.segment_bytes(),
            wal_segments: self.logger.segments(),
            lock_timeouts: self
                .lock_timeouts
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded: self.degraded.read().clone(),
            wal_retained_bytes: self.logger.retained_wal_bytes(),
            recovered_records: self
                .recovered_records
                .load(std::sync::atomic::Ordering::Relaxed),
            snapshot_too_old_aborts: self
                .snapshot_too_old_aborts
                .load(std::sync::atomic::Ordering::Relaxed),
            log_bytes: self.log_file.get_metadata().map(|m| m.len).unwrap_or(0),
            cached_pages: self.buffer.cached_pages(),
            page_count: self.page_count(),
            tables: self.tables.read().len(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Phase 5: how long a page-lock wait may take before it is reported as
    /// a bug (`LockTimeout`). Default 1 s — a legitimate hold is
    /// microseconds, so this is a detector with a thousandfold margin, not
    /// a contention tunable.
    pub fn set_lock_timeout(&self, timeout: std::time::Duration) {
        self.buffer.set_lock_timeout(timeout);
    }

    /// Phase 7: caps on what a long-lived transaction may pin (retained WAL
    /// bytes, version records). Past either, the maintenance thread aborts
    /// the oldest active transaction; its owner's next call fails with
    /// `SnapshotTooOld`.
    pub fn set_snapshot_limits(&self, limits: SnapshotLimits) {
        *self.snapshot_limits.write() = limits;
    }

    pub fn snapshot_limits(&self) -> SnapshotLimits {
        *self.snapshot_limits.read()
    }

    fn refuse_if_degraded(&self) -> Result<(), StoreError> {
        match &*self.degraded.read() {
            Some(reason) => Err(StoreError::EngineDegraded(reason.clone())),
            None => Ok(()),
        }
    }

    fn enter_degraded(&self, reason: String) {
        log::error!("engine degraded: {reason}");
        let mut d = self.degraded.write();
        if d.is_none() {
            *d = Some(reason);
        }
    }

    /// TXN_SIMPLIFICATION_PLAN.md phase 6: a fuzzy checkpoint. It never
    /// waits for a transaction. Five steps, in this order:
    ///
    /// 0. With tree writers excluded (microseconds: they never wait on a
    ///    transaction), persist the system pages and read `floor` =
    ///    min(the counter's next value, the oldest Active or Aborting
    ///    transaction's id) — the counter read FIRST. A transaction that
    ///    begins after that read has an id above the floor, and every LSN
    ///    a transaction mints is above its own id, so every record the log
    ///    must keep (anything an unfinished transaction wrote) is at or
    ///    above the floor by construction; with no write in flight, there
    ///    is nothing else.
    /// 1. Still excluded, capture a copy of every dirty page — one
    ///    consistent instant of the whole tree, so no page on disk ever
    ///    points at one that is not.
    /// 2. Sync the log: every captured mutation's record was queued before
    ///    its page was published, so all of them are durable now (the WAL
    ///    rule). Then write the captured pages and fsync the data file.
    /// 3. Write the header (`page_count`, `counter`) and fsync it.
    /// 4. Roll the log to a new segment.
    /// 5. Delete every older segment whose highest LSN is below the floor.
    ///
    /// Why a record below the floor is never needed again: its transaction
    /// finished before step 0 (else the floor would be at or below that
    /// transaction's id), so its page was published before step 0 and
    /// flushed in step 2 — no redo — and a finished transaction needs no
    /// undo. An unfinished transaction's pages may reach disk in step 2;
    /// the records that undo them are above the floor and stay. A long-
    /// lived transaction therefore costs retained segments (`stats()`),
    /// never a stalled checkpoint.
    pub fn checkpoint(&self) -> Result<(), StoreError> {
        let _one_at_a_time = self.checkpoint_mutex.lock();
        let (floor, captured) = {
            let _no_writers = self.buffer.exclude_writers();
            // Floor first: a Sequence record minted after this read stays
            // above the floor (replayed), and one minted before it is in
            // the generator page written next.
            let floor = self.retention_floor();
            // The catalog and the generator's sequences live only on their
            // pinned pages; written here so they belong to the same instant.
            self.write_system_tables()?;
            (floor, self.buffer.capture_dirty_pages()?)
        };
        self.logger.sync()?;
        // The crash harness snapshots the data file and the segments at one
        // instant; the data fsync and the segment deletion below must look
        // like one instant to it as well (see synced_snapshot).
        let _snapshot_guard = self.snapshot_mutex.lock();
        self.buffer.write_captured(captured)?;
        let mut hdr = (*self.header).clone();
        hdr.page_count = self.page_count();
        let ts = timestamp();
        hdr.last_checkpoint = ts;
        hdr.counter = self.logger.clock().next_value();
        hdr.checkpoint_lsn = floor;
        // STORE_AUDIT.md T5: synced, so the segment deletion right below
        // can never run ahead of the header that accounts for the flush.
        hdr.seal();
        self.buffer.write_header_synced(hdr)?;
        self.logger.roll(floor)?;
        self.last_checkpoint
            .store(ts, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Step 0 of `checkpoint`: the LSN below which no log record is needed
    /// once this checkpoint completes. Counter first, then the table (see
    /// `checkpoint` on why the order matters).
    fn retention_floor(&self) -> u64 {
        let counter = self.logger.clock().next_value();
        match self.tx_mgr.oldest_in_flight() {
            Some(id) => id.min(counter),
            None => counter,
        }
    }

    // Replaces the old two-file process_redo/process_undo with one
    // three-pass scan over the single WAL (T4_S2_WAL_DESIGN.md §8):
    // analysis (which txns committed), redo (replay every record for a
    // committed txn), undo (revert every record for a txn that never
    // committed — abandoned or explicitly rolled back, treated identically,
    // exactly as the old process_undo already did: "not in `committed`"
    // fully characterizes "needs undo" either way). `buffer` is the WAL's
    // content AFTER its LogHeader — `load_logs` above hands this the raw
    // mmap/MemFile bytes directly since the header was already read and
    // validated separately (by create_core_db/open_using), never mixed
    // into the same scan as the framed records.
    fn process_log(&self, records: Vec<LogRecord>) -> Result<usize, StoreError> {
        let scanned = ScannedLog { records };
        let count = scanned.records.len();

        // --- Pass 1: analysis ---
        let mut committed: HashSet<TransactionId> = HashSet::new();
        let mut by_txn: HashMap<TransactionId, Vec<&LogRecord>> = HashMap::new();
        let mut max_lsn: Option<u64> = None;
        let bump = |v: u64, max_lsn: &mut Option<u64>| {
            *max_lsn = Some(max_lsn.map_or(v, |m| m.max(v)));
        };
        for record in &scanned.records {
            bump(record.lsn.0, &mut max_lsn);
            match &record.operation {
                Operation::Add { txn, .. }
                | Operation::Mod { txn, .. }
                | Operation::Del { txn, .. } => {
                    // Ids come from the same counter as LSNs (phase 1); fold
                    // them in so the seed covers every number ever issued.
                    bump(txn.0, &mut max_lsn);
                    by_txn.entry(*txn).or_default().push(record);
                }
                Operation::Commit(t) => {
                    bump(t.0, &mut max_lsn);
                    committed.insert(*t);
                }
                // "not in `committed`" below already covers an explicit
                // rollback exactly the same as a never-committed abandoned
                // txn.
                Operation::Rollback(t) => {
                    bump(t.0, &mut max_lsn);
                }
                Operation::Purge { txn, .. } => {
                    bump(txn.0, &mut max_lsn);
                }
                // Sequences are not transactional: applied in log order,
                // last record per name wins (see generator.rs).
                Operation::Sequence {
                    name,
                    high_water,
                    dropped,
                } => {
                    if *dropped {
                        self.generator.remove_unlogged(name)?;
                    } else {
                        self.generator.ensure_at_least(name, *high_water)?;
                    }
                }
            }
        }

        // --- Pass 2: redo — every record for a COMMITTED txn, in the log's
        // own (LSN-ascending) order.
        //
        // Deliberately iterates `scanned.records` directly, NOT `by_txn`
        // (a HashMap grouping each transaction's own records together,
        // built above for Pass 3's benefit): a HashMap's iteration order
        // across DIFFERENT keys is randomized per-process, so replaying
        // "one committed transaction's records, then the next" in
        // HashMap-key order can replay two DIFFERENT committed
        // transactions' operations on the SAME row out of their original
        // order. Concretely (the exact bug this fixes, caught by
        // test_replay_handles_mixed_add_mod_del_across_committed_and_abandoned_txns
        // failing intermittently — passing or failing depending on that
        // run's random hash seed): row 2 is inserted by committed txn C,
        // then removed by committed txn D. If D's group happened to be
        // visited before C's, replay ran remove(row2) — a no-op, row 2
        // isn't there yet — then insert_if_needed(row2) from C's own
        // group, resurrecting a row that was correctly, committedly
        // removed. Iterating the flat, already-LSN-ordered record list
        // instead preserves the true order operations happened in,
        // regardless of which transactions logged them.
        for record in &scanned.records {
            let txn = match &record.operation {
                Operation::Add { txn, .. }
                | Operation::Mod { txn, .. }
                | Operation::Del { txn, .. } => txn,
                _ => continue,
            };
            if !committed.contains(txn) {
                continue;
            }
            match &record.operation {
                Operation::Add { post, .. } => {
                    if let Some(table) = self.table_by_id_or_dropped(post.table_id)? {
                        table.insert_if_needed(&post.tuple)?;
                    }
                }
                Operation::Mod { post, .. } => {
                    if let Some(table) = self.table_by_id_or_dropped(post.table_id)? {
                        table.update_if_needed(post.tuple.clone())?;
                    }
                }
                // Phase 3: a delete's redo re-tombstones the row in place
                // (physical removal is Purge's job), so a later record in
                // this same suffix that builds on the tombstone — an insert
                // over it — still finds it.
                Operation::Del { txn, pre } => {
                    if let Some(table) = self.table_by_id_or_dropped(pre.table_id)? {
                        let mut t = pre.tuple.clone();
                        t.set_txn_id(*txn);
                        t.set_pre_lsn(record.lsn);
                        t.tombstone();
                        table.update_if_needed(t)?;
                    }
                }
                Operation::Purge { txn, table_id, key } => {
                    if let Some(table) = self.table_by_id_or_dropped(*table_id)? {
                        Self::purge_if_still_tombstone(
                            &table,
                            key.clone(),
                            *txn,
                            self.logger.next_lsn(),
                        )?;
                    }
                }
                _ => {}
            }
        }

        // --- Pass 3: undo — every record for a txn that never committed, in
        // REVERSE log order (phase 3): a transaction's own chain restores
        // step by step, each record's pre-image being the version just
        // before it.
        for record in scanned.records.iter().rev() {
            let txn = match &record.operation {
                Operation::Add { txn, .. }
                | Operation::Mod { txn, .. }
                | Operation::Del { txn, .. } => txn,
                _ => continue,
            };
            if committed.contains(txn) {
                continue;
            }
            self.revert_one(&record.operation, txn)?;
        }
        let _ = &by_txn;

        if let Some(lsn) = max_lsn {
            self.logger.clock().advance_counter_past(LsnId(lsn));
        }
        // Every page write replay just made re-applies a record that is
        // already durable, but the tree stamped those pages with freshly
        // minted LSNs that no record will ever carry. Declare everything
        // minted so far durable, or those pages would sit in the writer's
        // deferred set until some unrelated later record landed — and a
        // read-only session after recovery would eventually block on
        // backpressure with nothing to wait for.
        let clock = self.logger.clock();
        clock.mark_written(LsnId(clock.next_value().saturating_sub(1)));
        Ok(count)
    }

    /// Begins a transaction with the default conflict policy
    /// (`ConflictPolicy::ContinueOnConflict` — a `WriteConflict` fails just
    /// the conflicting operation, leaving the transaction open). Use
    /// `begin_with_conflict_policy` for `AbortOnConflict` behavior.
    pub fn begin(self: &Arc<Self>) -> Result<Transaction, StoreError> {
        self.begin_with_conflict_policy(ConflictPolicy::ContinueOnConflict)
    }

    /// Like `begin`, but lets the caller choose how a `WriteConflict` on any
    /// operation within this transaction is handled — see `ConflictPolicy`'s
    /// own doc comment.
    ///
    /// TXN_SIMPLIFICATION_PLAN.md phase 3: one counter increment and one map
    /// insert. No cleanup, no log-size check — the maintenance thread owns
    /// both.
    pub fn begin_with_conflict_policy(
        self: &Arc<Self>,
        policy: ConflictPolicy,
    ) -> Result<Transaction, StoreError> {
        let id = self.tx_mgr.create_transaction(policy)?;
        Ok(Transaction::new(id, Arc::clone(self) as Arc<dyn TxnSink>))
    }

    pub fn commit(&self, txn: Transaction) -> Result<(), StoreError> {
        self.commit_with(txn, Durability::Sync)
    }

    /// `commit` with a choice of durability wait — see `Durability`.
    pub fn commit_with(&self, txn: Transaction, durability: Durability) -> Result<(), StoreError> {
        // Detach the id before doing any fallible work below. If that work
        // fails partway, returning `?` must NOT trigger the guard's abort —
        // the transaction simply stays active (and invisible) until a retried
        // commit completes.
        self.commit_id_with(txn.into_id(), durability)
    }

    fn commit_id(&self, id: TransactionId) -> Result<(), StoreError> {
        self.commit_id_with(id, Durability::Sync)
    }

    /// The commit path (proposal §3.7): one append, one state flip, wake
    /// vacuum, wait for durability. No tree work.
    fn commit_id_with(&self, id: TransactionId, durability: Durability) -> Result<(), StoreError> {
        self.refuse_if_degraded()?;
        // AbortOnConflict may already have aborted this transaction; a
        // commit of a finished id must not report success.
        self.require_active(&id)?;
        let commit_lsn = self.logger.log_new(Operation::Commit(id))?;
        // THE commit point for every other thread. Versions are never
        // discarded here — retention is the horizon's decision — so there
        // is no window where a walker sees "not committed" and "pre-image
        // gone" at once (the race STORE_AUDIT.md T14 was about).
        self.tx_mgr.commit(id, commit_lsn.0)?;
        self.versions.mark_committed(id, commit_lsn.0);
        self.maintenance.wake();
        // STORE_AUDIT.md T1: don't report success until the record is
        // fsynced. Last, so is_committed flips promptly regardless of how
        // long the fsync takes.
        if durability == Durability::Sync {
            crate::buffer::debug_assert_no_page_locks_held("waiting for commit durability");
            self.logger.wait_until_durable(commit_lsn);
        }
        Ok(())
    }

    pub fn rollback(&self, txn: Transaction) -> Result<(), StoreError> {
        self.abort(txn.into_id())
    }

    /// THE abort path (proposal §3.7) — explicit rollback, a dropped guard,
    /// `AbortOnConflict`, and (later) snapshot-too-old all come here.
    /// Flips the transaction to Aborting (invisible from this instant),
    /// reverts its writes in reverse log order, logs, forgets its versions,
    /// and removes it. If the revert fails (I/O, corruption) the transaction
    /// stays Aborting and the maintenance thread retries.
    pub(crate) fn abort(&self, id: TransactionId) -> Result<(), StoreError> {
        match self.tx_mgr.abort(id) {
            Ok(()) => {}
            // Already finished — most commonly AbortOnConflict already took
            // it down and the caller's own ROLLBACK follows. A harmless
            // no-op, as in every SQL engine; never a second revert.
            Err(StoreError::TransactionAlreadyFinished) => {
                self.forced_aborts.lock().remove(&id);
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        let res = self.finish_abort(id);
        if res.is_err() {
            self.maintenance.wake();
        }
        res
    }

    /// The second half of `abort`, also what the maintenance thread retries.
    fn finish_abort(&self, id: TransactionId) -> Result<(), StoreError> {
        self.revert(id)?;
        self.logger.log_new(Operation::Rollback(id))?;
        self.versions.discard(&id);
        self.tx_mgr.abort_complete(&id);
        Ok(())
    }

    /// Physically revert `id`'s writes, newest first. Each step is
    /// conditional (update_if_txn / remove_if_txn: only if the row still
    /// belongs to `id`), so nothing another transaction has since written is
    /// ever clobbered.
    fn revert(&self, id: TransactionId) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_reverts.load(std::sync::atomic::Ordering::Acquire) {
            return Err(StoreError::UnknownError(
                "test: revert failure injected".into(),
            ));
        }
        for op in self.versions.ops_of(&id).iter().rev() {
            self.revert_one(op, &id)?;
        }
        Ok(())
    }

    fn revert_one(&self, op: &Operation, id: &TransactionId) -> Result<(), StoreError> {
        match op {
            Operation::Add { post, .. } => {
                // STORE_AUDIT.md T17: the table may have been dropped since;
                // nothing to revert against a table that no longer exists.
                if let Some(table) = self.table_by_id_or_dropped(post.table_id)? {
                    table.remove_if_txn(post.tuple.id.clone(), id)?;
                }
            }
            Operation::Del { pre, .. } | Operation::Mod { pre, .. } => {
                if let Some(table) = self.table_by_id_or_dropped(pre.table_id)? {
                    table.update_if_txn(pre.tuple.clone(), id)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// One maintenance pass (proposal §3.6), run by the maintenance thread:
    /// retry aborts whose revert failed, vacuum by the horizon, purge the
    /// tombstones vacuum released, checkpoint if the log has grown enough.
    pub(crate) fn maintenance_pass(&self) -> Result<(), StoreError> {
        let stats = &self.maintenance.stats;
        for id in self.tx_mgr.aborting_ids() {
            match self.finish_abort(id) {
                Ok(()) => {
                    stats
                        .abort_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.abort_attempts.lock().remove(&id);
                }
                Err(e) => {
                    stats.record_error(&format!("retrying abort of {id}: {e}"));
                    let attempts = {
                        let mut a = self.abort_attempts.lock();
                        let n = a.entry(id).or_insert(0);
                        *n += 1;
                        *n
                    };
                    if attempts >= ABORT_RETRY_BUDGET {
                        self.enter_degraded(format!(
                            "abort of {id} failed {attempts} times (last: {e}); its rows stay \
                             invisible and writes are refused until restart"
                        ));
                    }
                }
            }
        }
        let horizon = self.tx_mgr.oldest_active();
        self.tx_mgr.prune_committed();
        let v = self.versions.vacuum(horizon);
        if v.transactions_forgotten > 0 || !v.tombstones.is_empty() {
            stats
                .vacuums_with_work
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            stats.transactions_forgotten.fetch_add(
                v.transactions_forgotten as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            stats.records_discarded.fetch_add(
                v.records_discarded as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        if !v.tombstones.is_empty() {
            let mut retry = Vec::new();
            for t in v.tombstones {
                match self.purge(&t) {
                    Ok(()) => {
                        stats
                            .tombstones_purged
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        stats.record_error(&format!("purging {:?}: {e}", t.key));
                        retry.push(t);
                    }
                }
            }
            self.versions.requeue_tombstones(retry);
        }
        if self.logger.segment_bytes() > CHECKPOINT_LOG_BYTES
            || self.buffer.dirty_pages() > CHECKPOINT_DIRTY_PAGES
        {
            self.checkpoint()?;
            stats
                .checkpoints
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.enforce_snapshot_limits()?;
        Ok(())
    }

    /// Phase 7: when what long-lived transactions pin exceeds a cap, abort
    /// the oldest active transaction (one per pass) with a reason its
    /// owner sees on its next call, then checkpoint so the space is
    /// actually released.
    fn enforce_snapshot_limits(&self) -> Result<(), StoreError> {
        let limits = self.snapshot_limits();
        let wal = self.logger.retained_wal_bytes();
        let records = self.versions.records_len();
        let over = if wal > limits.max_retained_wal_bytes {
            Some(format!(
                "retained WAL {wal} bytes exceeds the cap of {} bytes",
                limits.max_retained_wal_bytes
            ))
        } else if records > limits.max_version_records {
            Some(format!(
                "{records} retained version records exceed the cap of {}",
                limits.max_version_records
            ))
        } else {
            None
        };
        let Some(why) = over else { return Ok(()) };
        let Some(oldest) = self.tx_mgr.oldest_active() else {
            return Ok(());
        };
        let id = TransactionId(oldest);
        let reason = format!("transaction {id} aborted by the engine: {why}");
        log::warn!("{reason}");
        self.forced_aborts.lock().insert(id, reason);
        self.abort(id)?;
        self.snapshot_too_old_aborts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.checkpoint()
    }

    /// Remove a committed tombstone's row and index entry, if it is still
    /// that transaction's tombstone (a later insert may have brought the key
    /// back to life). Logged first — under the same leaf lock as the removal
    /// — so recovery converges either way.
    fn purge(&self, t: &Tombstone) -> Result<(), StoreError> {
        let Some(table) = self.table_by_id_or_dropped(t.table_id)? else {
            return Ok(());
        };
        let key = t.key.clone();
        let txn = t.txn;
        let table_id = t.table_id;
        table.write_version(key.clone(), self.logger.next_lsn(), |cur| {
            let purge = match cur {
                Some(c) => c.is_tombstoned() && c.is_same_txn(txn),
                // An entry with no row behind it is never valid: finish it.
                None => true,
            };
            if !purge {
                return Ok(Decision::Skip);
            }
            self.logger.log_new(Operation::Purge {
                txn,
                table_id,
                key: key.clone(),
            })?;
            Ok(Decision::Delete)
        })?;
        Ok(())
    }

    // Purge redo: remove the row only if it is still `txn`'s tombstone; a
    // dangling index entry with no row behind it is removed either way.
    fn purge_if_still_tombstone(
        table: &BPlusTree<F>,
        key: DBIdType,
        txn: TransactionId,
        lsn: LsnId,
    ) -> Result<(), StoreError> {
        table.write_version(key, lsn, |cur| {
            Ok(match cur {
                Some(c) if c.is_tombstoned() && c.is_same_txn(txn) => Decision::Delete,
                Some(_) => Decision::Skip,
                None => Decision::Delete,
            })
        })?;
        Ok(())
    }

    pub(crate) fn get_last_checkpoint(&self) -> u128 {
        self.last_checkpoint
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn table_by_id(&self, id: TableIdType) -> Result<Arc<BPlusTree<F>>, StoreError> {
        // ok_or_else, not ok_or: ok_or's argument is eager, so
        // `id.to_string()` (a heap allocation) ran on every SUCCESSFUL
        // lookup too, not just the error path — this is a hot path
        // (once per row on every insert, via Schema::get_table), so that
        // was a real, measured source of small allocations (confirmed
        // via dhat: ~70K allocations across a 41K-row import).
        self.tables
            .read()
            .get(&id)
            .map(Arc::clone)
            .ok_or_else(|| StoreError::TableNotFound(id.to_string()))
    }

    // STORE_AUDIT.md T17: a redo/undo record can legitimately name a table
    // that no longer exists — drop_table between the record's own commit
    // and the next checkpoint, followed by a crash before the log is
    // truncated. The table being gone is, by definition, the state replay
    // is converging toward either way (the record's effect on a
    // now-nonexistent table can't matter), so this is treated as "nothing
    // to do", not a recovery failure. Used by both process_log's redo/undo
    // passes and revert_undo_ops (also reachable from live rollback, if a
    // transaction's own table is dropped out from under it after one of
    // its writes already returned but before the transaction ends).
    fn table_by_id_or_dropped(
        &self,
        id: TableIdType,
    ) -> Result<Option<Arc<BPlusTree<F>>>, StoreError> {
        match self.table_by_id(id) {
            Ok(table) => Ok(Some(table)),
            Err(StoreError::TableNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // STORE_AUDIT.md T17: lazily gets-or-creates the per-table lock used to
    // keep drop_table from freeing a table's pages while an in-flight
    // operation is still using them. A short-lived write lock on
    // `table_locks` itself only guards inserting the entry (once per table,
    // ever) — never held while anyone waits on the per-table lock it
    // returns.
    fn table_guard(&self, id: TableIdType) -> Arc<RwLock<()>> {
        self.table_locks
            .get_or_insert_with(id, || Arc::new(RwLock::new(())))
    }

    // STORE_AUDIT.md T17: insert/update/remove/find's replacement for a
    // bare table_by_id call — acquires the table's read guard BEFORE
    // looking the table up, so if the lookup succeeds, drop_table cannot
    // be concurrently mid-flight against this same id (it takes the write
    // side, see drop_table, before ever removing the table or freeing a
    // page), and cannot start until every guard returned here for this id
    // is dropped. Callers must hold the returned guard for their whole
    // operation, not just the lookup — binding it to a local variable (not
    // `_`) for the rest of the calling function's body is what actually
    // provides the protection.
    fn table_by_id_guarded(
        &self,
        id: TableIdType,
    ) -> Result<(Arc<BPlusTree<F>>, ArcRwLockReadGuard<RawRwLock, ()>), StoreError> {
        let guard = self.table_guard(id).read_arc();
        let table = self.table_by_id(id)?;
        Ok((table, guard))
    }

    pub fn table_id_by_name<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Option<TableIdType>, StoreError> {
        Ok(self
            .tables
            .read()
            .values()
            .find(|t| t.table.name == name.as_ref())
            .map(|t| t.id()))
    }

    pub fn insert(
        &self,
        id: TableIdType,
        tuple: Tuple,
        txn: &Transaction,
    ) -> Result<(), StoreError> {
        let tx_id = txn.id();
        self.require_active(&tx_id)?;
        let mut tuple = tuple;
        tuple.set_txn_id(tx_id);
        tuple.set_pre_lsn_none();
        tuple.clear_tombstone();
        // STORE_AUDIT.md T17: guard held for the whole call.
        let (table, _table_guard) = self.table_by_id_guarded(id)?;
        // STORE_AUDIT.md T2: minted BEFORE mutating; the pages this write
        // touches, the version record, and the WAL record all carry it.
        let lsn = self.logger.next_lsn();
        let key = tuple.id.clone();
        self.write_with_policy(&table, key.clone(), lsn, &tx_id, |current| {
            match current {
                None => {
                    let op = Operation::Add {
                        txn: tx_id,
                        post: Record::new(id, tuple.clone(), None),
                    };
                    self.versions.insert(lsn, op.clone());
                    self.logger.log(lsn, op)?;
                    Ok(Decision::Insert(tuple.clone()))
                }
                // Phase 3: a tombstone visible to this transaction is free
                // to reuse — a new version over it, logged as a Mod whose
                // pre-image is the tombstone (an older reader still
                // resolves to "deleted"; a rollback restores the tombstone).
                Some(current) if current.is_tombstoned() => {
                    self.check_write_conflict(current, &tx_id)?;
                    let mut revived = current.clone();
                    revived.set_txn_id(tx_id);
                    revived.set_pre_lsn(lsn);
                    revived.set_data(&tuple.data);
                    revived.clear_tombstone();
                    let op = Operation::Mod {
                        txn: tx_id,
                        pre: Record::new(id, current.clone(), None),
                        post: Record::new(id, revived.clone(), None),
                    };
                    self.versions.insert(lsn, op.clone());
                    self.logger.log(lsn, op)?;
                    Ok(Decision::Replace(revived))
                }
                Some(_) => Err(StoreError::DuplicateKey(key.clone())),
            }
        })?;
        Ok(())
    }

    pub fn find(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn: &Transaction,
    ) -> Result<Option<Tuple>, StoreError> {
        let txn_id = txn.id();
        // A finished transaction no longer pins its snapshot (vacuum may
        // have reclaimed what it could see), so it may not read: the error
        // says why it finished (SnapshotTooOld, or already finished).
        self.require_active(&txn_id)?;
        // STORE_AUDIT.md T17: guard held for the whole call.
        let (table, _table_guard) = self.table_by_id_guarded(tid)?;
        let tuple = table.find(id.clone())?;
        if let Some(tuple) = tuple {
            let visible = self
                .find_visible_to(&tuple, &txn_id)?
                .map(|t| t.into_owned());
            // A committed tombstone means the key was removed — it must be
            // invisible even if its physical row hasn't been reclaimed yet.
            // (commit reclaims tombstones best-effort AFTER its commit point, so
            // a committed-but-not-yet-reclaimed tombstone can legitimately still
            // be present in the tree.)
            match visible {
                Some(t) if t.is_tombstoned() => Ok(None),
                other => Ok(other),
            }
        } else {
            Ok(None)
        }
    }

    pub fn update(
        &self,
        tid: TableIdType,
        new_tuple: Tuple,
        txn_id: &Transaction,
    ) -> Result<(), StoreError> {
        let txn = txn_id.id();
        self.require_active(&txn)?;
        // STORE_AUDIT.md T17: guard held for the whole call.
        let (table, _table_guard) = self.table_by_id_guarded(tid)?;
        let id = new_tuple.id.clone();
        let lsn = self.logger.next_lsn();
        self.write_with_policy(&table, id.clone(), lsn, &txn, |current| {
            let current = current.ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
            self.check_write_conflict(current, &txn)?;
            // Phase 3: the pre-image is the version being replaced — a
            // committed ancestor for a first write, or this transaction's
            // own previous version (undo replays in reverse).
            let old_tuple = if current.txn_id == Some(txn) {
                current.clone()
            } else {
                self.find_last_committed(current)?
                    .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?
                    .into_owned()
            };
            // A tombstone is "not there": find() reports it absent, so
            // update() must too.
            if old_tuple.is_tombstoned() {
                return Err(StoreError::KeyNotFound(id.clone()));
            }
            let mut updated = old_tuple.clone();
            updated.set_txn_id(txn);
            updated.set_pre_lsn(lsn);
            updated.set_data(&new_tuple.data);
            // Version record first, then the WAL, then (in write_version)
            // the page: a concurrent reader that sees `updated` can always
            // resolve its pre_lsn.
            let op = Operation::Mod {
                txn,
                pre: Record::new(tid, old_tuple, None),
                post: Record::new(tid, updated.clone(), None),
            };
            self.versions.insert(lsn, op.clone());
            self.logger.log(lsn, op)?;
            Ok(Decision::Replace(updated))
        })?;
        Ok(())
    }

    pub fn remove(
        &self,
        tid: TableIdType,
        id: DBIdType,
        txn_id: &Transaction,
    ) -> Result<Tuple, StoreError> {
        let txn = txn_id.id();
        self.require_active(&txn)?;
        // STORE_AUDIT.md T17: guard held for the whole call.
        let (table, _table_guard) = self.table_by_id_guarded(tid)?;
        let lsn = self.logger.next_lsn();
        match self.write_with_policy(&table, id.clone(), lsn, &txn, |current| {
            let current = current.ok_or_else(|| StoreError::KeyNotFound(id.clone()))?;
            self.check_write_conflict(current, &txn)?;
            let old_tuple = if current.txn_id == Some(txn) {
                current.clone()
            } else {
                self.find_last_committed(current)?
                    .ok_or_else(|| StoreError::KeyNotFound(id.clone()))?
                    .into_owned()
            };
            // Removing an already-removed row is KeyNotFound, not a second
            // tombstone.
            if old_tuple.is_tombstoned() {
                return Err(StoreError::KeyNotFound(id.clone()));
            }
            let mut tombstoned = old_tuple.clone();
            tombstoned.set_txn_id(txn);
            tombstoned.tombstone();
            tombstoned.set_pre_lsn(lsn);
            let op = Operation::Del {
                txn,
                pre: Record::new(tid, old_tuple, None),
            };
            self.versions.insert(lsn, op.clone());
            self.logger.log(lsn, op)?;
            Ok(Decision::Replace(tombstoned))
        })? {
            Written::Replaced(old) => Ok(old),
            _ => Err(StoreError::KeyNotFound(id)),
        }
    }

    // One write through the tree's single primitive, with the transaction's
    // ConflictPolicy applied to a WriteConflict: AbortOnConflict takes the
    // whole transaction down through the single abort path.
    fn write_with_policy(
        &self,
        table: &BPlusTree<F>,
        id: DBIdType,
        lsn: LsnId,
        txn: &TransactionId,
        decide: impl FnOnce(Option<&Tuple>) -> Result<Decision, StoreError>,
    ) -> Result<Written, StoreError> {
        self.refuse_if_degraded()?;
        let result = table.write_version(id, lsn, decide);
        match &result {
            Err(StoreError::WriteConflict(key))
                if self.tx_mgr.conflict_policy(txn) == ConflictPolicy::AbortOnConflict =>
            {
                self.abort(*txn)?;
                return Err(StoreError::WriteConflictTransactionAborted(key.clone()));
            }
            // Phase 5: a lock-order violation or a lock timeout is a bug
            // signal, not a retryable condition. The transaction is taken
            // down through the single abort path; nothing retries.
            Err(StoreError::LockTimeout(_)) | Err(StoreError::LockOrderViolation(_)) => {
                self.lock_timeouts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = self.abort(*txn);
            }
            _ => {}
        }
        result
    }

    // Guards insert/update/remove/commit against being called with a
    // TransactionId that's no longer active — most commonly because
    // AbortOnConflict already rolled the whole transaction back after an
    // earlier operation's conflict (see update_checked_with_retry). Without
    // this, a caller unaware the transaction was already finished could
    // keep writing under a dead id (silently invisible forever, since
    // nothing commits it) or, worse, have `commit()` report success for a
    // transaction that was actually rolled back — `tx_mgr.commit` removing
    // a non-member from its active set is a silent no-op, not an error.
    pub(crate) fn require_active(&self, txn: &TransactionId) -> Result<(), StoreError> {
        if self.tx_mgr.is_transaction_active(txn) {
            Ok(())
        } else if let Some(reason) = self.forced_aborts.lock().remove(txn) {
            // Phase 7: the engine took this transaction down; say why, once.
            Err(StoreError::SnapshotTooOld(reason))
        } else {
            Err(StoreError::TransactionAlreadyFinished)
        }
    }

    // Takes &Arc<Self>, not &self: the returned cursor holds its own
    // Arc<Db<F>> clone (it needs to call find_last_committed per row to
    // resolve MVCC visibility as it scans), which only a caller already
    // holding the Db as Arc<Db<F>> can provide.
    //
    // Scans under a fresh, cursor-owned transaction — reads only what was
    // committed as of when the scan begins. A caller with its own
    // already-open transaction (e.g. an explicit BEGIN) wants
    // table_scan_in_txn instead, so the scan can also see that
    // transaction's own not-yet-committed writes.
    pub fn table_scan(self: &Arc<Self>, tid: TableIdType) -> Result<TableCursor<F>, StoreError> {
        TableCursor::new(Arc::clone(self), tid, None)
    }

    // Like table_scan, but reads under `txn` instead of a fresh transaction
    // of the scan's own — so the scan sees `txn`'s own uncommitted writes,
    // not just what's already committed. `txn` must stay open (not
    // committed/rolled back) for at least as long as the returned cursor is
    // used; the cursor only borrows its id; the caller's own guard is what
    // keeps it registered as active.
    pub fn table_scan_in_txn(
        self: &Arc<Self>,
        tid: TableIdType,
        txn: &Transaction,
    ) -> Result<TableCursor<F>, StoreError> {
        self.require_active(&txn.id())?;
        TableCursor::new(Arc::clone(self), tid, Some(txn.id()))
    }

    /// Starts a new, empty Run — an append-only, unkeyed page chain for
    /// query-execution scratch space (sort runs, hash-join/aggregation
    /// spill partitions, ...). Unlike a table, a Run needs no MVCC/txn
    /// machinery, so this only needs `&self`, not `&Arc<Self>`.
    pub fn create_run(&self) -> Result<Run<F>, StoreError> {
        Run::create(self.buffer.clone())
    }

    /// Like `create_run`, but every page (see `Run::create_slotted`'s own
    /// comment) is backed by `SlottedPage` instead of `RunPage` —
    /// individually addressable, in-place-mutable slots for a caller
    /// with a fixed, page-per-bucket-range layout (e.g. a hash index)
    /// that mutates one slot at a time, repeatedly.
    pub fn create_slotted_run(&self) -> Result<Run<F>, StoreError> {
        Run::create_slotted(self.buffer.clone())
    }

    pub fn range_scan(
        self: &Arc<Self>,
        tid: TableIdType,
        start: DBIdType,
        end: DBIdType,
    ) -> Result<RangeCursor<F>, StoreError> {
        RangeCursor::new(Arc::clone(self), tid, None, start, end)
    }

    // Shared undo-chain walk: returns the first version (this tuple, or an
    // ancestor reached by walking the undo chain backward) for which
    // `is_visible` holds. `find_last_committed` and `find_visible_to` are
    // both this walk with a different visibility predicate — the former
    // ("is anybody's write here durable/committed") for write paths that
    // must act on the true latest state, the latter ("is this write visible
    // to *this specific reader's* snapshot") for read paths.
    fn resolve_visible<'a>(
        &self,
        tuple: &'a Tuple,
        is_visible: impl Fn(&TransactionId) -> bool,
    ) -> Result<Visibility<'a>, StoreError> {
        if let Some(txn) = tuple.txn_id {
            if is_visible(&txn) {
                Ok(Visibility::Found(Cow::Borrowed(tuple)))
            } else {
                let mut tuple = tuple.clone();
                loop {
                    // A genuine dead end: this tuple has no prior version
                    // at all (only true of a fresh INSERT), so there is
                    // nothing further back to find. Must NOT be treated
                    // the same as a missing undo record below — see
                    // Visibility's own doc comment.
                    let Some(pre_lsn) = tuple.pre_lsn else {
                        return Ok(Visibility::NoAncestor);
                    };

                    // Tolerate a missing record: an aborting txn's undo can
                    // be discarded concurrently once its rows are reverted. If we
                    // can't walk further, treat the row as ambiguous rather than
                    // panicking or silently asserting it has no ancestor.
                    let Some(op) = self.versions.find(pre_lsn) else {
                        return Ok(Visibility::MissingUndoRecord);
                    };
                    let next_tuple = match op {
                        Operation::Add { post, .. } => post.tuple,
                        Operation::Mod { pre, .. } => pre.tuple,
                        Operation::Del { pre, .. } => pre.tuple,
                        // A record without a usable pre-image (a "redo-only"
                        // Mod, or a Commit/Rollback marker) should never be
                        // what a pre_lsn points at — defensive fallback,
                        // matching the old code's leniency (it mapped every
                        // Add/Del/Mod to a tuple unconditionally).
                        _ => return Ok(Visibility::MissingUndoRecord),
                    };
                    let Some(next_txn) = next_tuple.txn_id else {
                        return Ok(Visibility::NoAncestor);
                    };
                    if is_visible(&next_txn) {
                        // next_tuple is the visible ancestor we walked back
                        // to — return it, not the in-flight `tuple` we started
                        // from (which belongs to a not-yet-visible txn and must
                        // stay invisible to other readers).
                        return Ok(Visibility::Found(Cow::Owned(next_tuple)));
                    }
                    tuple = next_tuple;
                }
            }
        } else {
            // STORE_AUDIT.md S8: every real insert/update/remove always
            // sets txn_id — this used to be `panic!`, which for an
            // embedded library is a process crash for the host. A
            // corrupted or hand-crafted on-disk file could easily produce
            // a tuple missing it; surface that as a typed error instead.
            Err(StoreError::Corruption(format!(
                "tuple {:?} has no txn_id",
                tuple.id
            )))
        }
    }

    // Write-write conflict guard for update()/remove(), called against the
    // row's raw, current physical tuple (whoever last wrote it) before
    // building on top of it. Page-level locks (ArcLock) only serialize the
    // *physical* write — they say nothing about whether blindly overwriting
    // the row is *logically* safe — so without this, two transactions
    // updating the same row concurrently both silently succeed and both
    // commit, with the first one's write simply gone. Confirmed via direct
    // repro before this existed: T1 and T2 both begin, both update() the
    // same row, both update() calls and both commit() calls return Ok, and
    // the final value is T2's, with no error ever surfaced to T1.
    //
    // `current`'s writer is safe to build on top of only if it's exactly
    // the set find_visible_to already treats as visible to `txn` as a
    // *reader* — this is deliberately that same predicate, negated:
    //   - writer == txn: txn is re-writing its own not-yet-committed row.
    //   - writer committed strictly before txn began, AND wasn't still
    //     active when txn's snapshot was captured (i.e. txn's own
    //     find()-then-update is building on exactly the state its
    //     snapshot already accounts for).
    // Anything else is a conflict:
    //   - writer is active right now (a live, in-flight competing write).
    //   - writer was active when txn began (in txn's own snapshot) — even
    //     if it has since committed, txn's snapshot didn't (and couldn't)
    //     account for that write, so building on it would silently
    //     discard it.
    //   - writer began at or after txn did — concurrent work by
    //     definition, since txn couldn't have observed it at its own
    //     begin() regardless of commit order.
    fn check_write_conflict(&self, current: &Tuple, txn: &TransactionId) -> Result<(), StoreError> {
        let writer = current
            .txn_id
            .expect("tuple returned by table.find() must carry a txn_id");
        // TXN_SIMPLIFICATION_PLAN.md phase 2: the whole rule lives in
        // TransactionManager::conflicts (first-committer-wins).
        if self.tx_mgr.conflicts(&writer, txn) {
            return Err(StoreError::WriteConflict(current.id.clone()));
        }
        Ok(())
    }

    // Latest committed version, full stop — no snapshot filtering. Used
    // internally by update()/remove() to resolve the current pre-image for
    // undo-log construction once check_write_conflict has already
    // confirmed the row's current writer is safe to build on top of.
    pub(crate) fn find_last_committed<'a>(
        &self,
        tuple: &'a Tuple,
    ) -> Result<Option<Cow<'a, Tuple>>, StoreError> {
        // Visible iff the writer COMMITTED — i.e. it is neither still active
        // nor aborting-with-unreverted-writes. A dropped/aborted txn stays
        // in `aborting` and is therefore correctly invisible here even
        // though it has left the active set. No snapshot filtering here, so
        // (unlike find_visible_to) there's no meaningful difference between
        // NoAncestor and MissingUndoRecord to preserve — either way, no
        // committed version was found.
        match self.resolve_visible(tuple, |txn| self.tx_mgr.is_committed(txn))? {
            Visibility::Found(t) => Ok(Some(t)),
            Visibility::NoAncestor | Visibility::MissingUndoRecord => Ok(None),
        }
    }

    // Snapshot-isolated visibility for reads (Db::find, TableCursor,
    // RangeCursor). TXN_SIMPLIFICATION_PLAN.md phase 2: the whole rule lives
    // in TransactionManager::is_visible — a version is visible iff its
    // writer is the reader, or committed before the reader began
    // (commit_ts < reader.id). No per-reader snapshot set exists anymore.
    pub(crate) fn find_visible_to<'a>(
        &self,
        tuple: &'a Tuple,
        reader: &TransactionId,
    ) -> Result<Option<Cow<'a, Tuple>>, StoreError> {
        match self.resolve_visible(tuple, |txn| self.tx_mgr.is_visible(txn, reader))? {
            Visibility::Found(t) => Ok(Some(t)),
            // A genuine dead end — this version has no ancestor at all, so
            // there is nothing to fall back to. This is also exactly what a
            // phantom row looks like: a fresh INSERT by a writer that isn't
            // visible to `reader` has pre_lsn == None, so the walk hits this
            // on its first step. Must NOT fall through to
            // find_last_committed below.
            Visibility::NoAncestor => Ok(None),
            // Phase 3: retention is the horizon's rule (proposal §3.6), so a
            // version a live reader can still need is never gone. Reaching
            // this is an invariant violation, reported as such — not papered
            // over with the latest committed version.
            Visibility::MissingUndoRecord => Err(StoreError::Corruption(format!(
                "version record missing for pre_lsn of {:?} (reader {reader}, oldest active {:?})",
                tuple.id,
                self.tx_mgr.oldest_active()
            ))),
        }
    }

    /// Convenience wrapper around `create_table_with_index_entry_size` using
    /// `crate::tables::bplustree::MAX_ENTRY_BYTES` as the index entry size —
    /// sized for a plain `Int`/`Vec` key (see that constant's own comment).
    /// If your table's primary key is a composite `Rec(IndexKey)` with
    /// `Str`/`Blob` fields, this default is very likely too small: call
    /// `create_table_with_index_entry_size` directly with a size you've
    /// computed for your actual key shape instead of hoping this guess
    /// covers it.
    pub fn create_table(&self, name: String) -> Result<TableIdType, StoreError> {
        self.create_table_with_index_entry_size(name, bplustree::MAX_ENTRY_BYTES)
    }

    /// Like `create_table`, but takes the fixed per-entry byte budget for
    /// this table's index pages explicitly instead of assuming a default —
    /// see `BPlusTree::new`'s own doc comment for what this bounds and how
    /// to size it for a given key shape.
    pub fn create_table_with_index_entry_size(
        &self,
        name: String,
        index_entry_size: DBSizeType,
    ) -> Result<TableIdType, StoreError> {
        let table_id = {
            let _writer = self.buffer.writer_permit();
            self.validate_table_name(&name)?;
            let mut tables = self.tables.write();
            self.generator.create_generator(&name, None)?;
            //let table_page = self.buffer.alloc_page(false)?;
            let table = BPlusTree::new(
                self.generator.gen_key(SYSTEM_TABLE_NAME)?.into(),
                name.clone(),
                self.buffer.clone(),
                self.tx_mgr.clone(),
                self.logger.clone(),
                index_entry_size,
            )?;
            let id = table.id();
            tables.insert(id, Arc::new(table));
            id
        };
        // Durability fix (was: "create_table is not logged, so a crash
        // before the next checkpoint loses the table" — TXN_SIMPLIFICATION_
        // PROGRESS.md, phase 6 known gaps): create_table/drop_table used to
        // call write_system_tables() directly, which pwrites the catalog/
        // generator/free-page pages eagerly but never fsyncs — durability
        // depended on whatever checkpoint happened to run next, and on a
        // real (non-mem) file, nothing ordered those eager pwrites relative
        // to the page allocations above or to each other, so a real power
        // loss could persist any subset of them. A real checkpoint() is
        // exactly the already-correct, fsync-ordered, atomic-capture
        // primitive this needs: it takes exclude_writers(), writes the
        // catalog, captures every dirty page (including the ones the table
        // build above just dirtied) at one instant, and fsyncs before
        // returning — so create_table is durable the moment it returns,
        // like commit()'s own wait_until_durable, not "eventually, at the
        // next periodic checkpoint".
        //
        // Called *outside* writer_permit's scope above: checkpoint() takes
        // exclude_writers() (the write side of the same write_gate) and
        // would deadlock against a read guard this same thread still held.
        // The structural work above is already internally consistent by
        // the time that guard drops (the new table is fully built and
        // registered before the block ends), so it's fine for a concurrent
        // writer to land before this checkpoint captures — checkpoint
        // doesn't care whose dirty pages it's flushing.
        //
        // STORE_AUDIT.md S6: the system catalog (page 0) is a single
        // fixed-size page — write_system_tables (called inside checkpoint)
        // can fail with PageCapacityError once enough tables exist that
        // their combined metadata no longer fits. Before this rollback,
        // that failure left the new table fully registered in memory
        // (table_id_by_name found it, the generator entry existed) despite
        // create_table itself returning Err — and since it stayed in
        // `self.tables`, every LATER write_system_tables call (including a
        // real checkpoint's own) hit the exact same PageCapacityError
        // trying to serialize it too, permanently breaking checkpoint() on
        // this otherwise-fine database. Roll back every step above on
        // failure — deregister the generator, drop it from `tables`, and
        // free the pages BPlusTree::new already allocated for it — so a
        // rejected create_table leaves the database exactly as if it had
        // never been called. Re-takes writer_permit for the same reason
        // the structural work above needed it: these resets must not be
        // visible to a checkpoint mid-rollback.
        if let Err(e) = self.checkpoint() {
            let _writer = self.buffer.writer_permit();
            let table = self.tables.write().remove(&table_id);
            self.generator.remove_generator(&name)?;
            if let Some(table) = table {
                for page_id in table.all_index_page_ids()? {
                    let record_size = self.buffer.get_page(page_id)?.record_size();
                    self.buffer.reset_and_free_page(page_id, record_size)?;
                }
                self.buffer.free_page_chain(table.table.first_data_page)?;
            }
            return Err(e);
        }
        Ok(table_id)
    }

    /// Drops a table and reclaims every page it owns (its whole index tree
    /// and its whole data chain, including any overflow continuation
    /// pages) back to the free list, so later `create_table` calls can
    /// reuse that space instead of the file only ever growing.
    ///
    /// Not transactional, like `create_table`/`create_table_with_index_
    /// entry_size` themselves — this is DDL, not a row-level operation
    /// tracked by a `Transaction`'s undo log, so there's no `&Transaction`
    /// parameter and nothing here can be rolled back once called.
    ///
    /// STORE_AUDIT.md T17: blocks until every insert/update/remove/find
    /// currently in flight against this table (i.e. that already looked it
    /// up via `table_by_id_guarded` before this call started) has
    /// finished, and blocks any new one from starting until this whole
    /// call completes — see `table_locks`' own doc comment. This makes it
    /// safe against exactly those four operations; it is NOT safe against
    /// a live `table_scan` cursor obtained before this call, since a
    /// `TableCursor` can be held far longer than one call and isn't
    /// guarded the same way (deferred, like T3's long-reader caveat — the
    /// audit's own text notes the current squeal-sql usage, dropping a
    /// just-created, not-yet-published table after a failed CREATE TABLE,
    /// can't already have a live scan against it).
    pub fn drop_table<S: AsRef<str>>(&self, name: S) -> Result<(), StoreError> {
        let name = name.as_ref();
        {
            let _writer = self.buffer.writer_permit();
            let id = self
                .tables
                .read()
                .values()
                .find(|t| t.table.name == name)
                .map(|t| t.id())
                .ok_or_else(|| StoreError::TableNotFound(name.to_string()))?;
            let _table_guard = self.table_guard(id).write_arc();
            // Re-resolved (not reused from the id above) under the same guard
            // that now excludes every in-flight reader/writer: if a racing
            // drop_table for this same name already won, this table is gone
            // and we report TableNotFound cleanly instead of panicking on a
            // stale id.
            let table = self
                .tables
                .write()
                .remove(&id)
                .ok_or_else(|| StoreError::TableNotFound(name.to_string()))?;
            for page_id in table.all_index_page_ids()? {
                let record_size = self.buffer.get_page(page_id)?.record_size();
                self.buffer.reset_and_free_page(page_id, record_size)?;
            }
            self.buffer.free_page_chain(table.table.first_data_page)?;
            self.generator.remove_generator(name)?;
        }
        // Durability/corruption fix (TXN_SIMPLIFICATION_PROGRESS.md, phase 6
        // known gaps): this used to call write_system_tables() directly —
        // an eager, unsynced pwrite of the catalog with no ordering
        // guarantee relative to the page resets above, on a real file. A
        // crash between them (or the OS reordering the unsynced writes on
        // a real power loss) could leave a catalog still naming this table
        // while its pages are already blank. checkpoint() is the same
        // fsync-ordered, atomic-capture primitive create_table now uses:
        // called outside writer_permit's scope (already dropped above) to
        // avoid deadlocking against exclude_writers(), which is safe here
        // because the structural work above is already fully consistent
        // (table deregistered, its pages reset and freed, its generator
        // gone) by the time the guard drops.
        self.checkpoint()?;
        Ok(())
    }

    fn setup_needed_modules(
        header: Arc<Header>,
        page_counter: Arc<AtomicU64>,
        file: F,
        name: String,
        wal: OpenedWal<F>,
    ) -> Result<NeededObjects<F>, StoreError> {
        let mut logger = Logger::new();
        logger.set_db(
            wal.current_file,
            name,
            wal.current,
            wal.older,
            wal.header_bytes,
        )?;
        // Buffer shares the logger's WAL clock, so page-flush deferral and redo
        // LSNs are scoped to this one database (not a process global).
        let clock = logger.clock();
        // Defaults to the two built-in kinds — Db::open/create don't yet
        // expose a way for a caller to supply custom kinds; that's a
        // natural follow-up once something actually needs to register one.
        let content_registry = Arc::new(crate::pages::content::PageContentRegistry::builtin());
        let buffer = Arc::new(PageBuffer::new(
            header.page_size,
            page_counter,
            file,
            header,
            8192,
            clock.clone(),
            content_registry,
        )?);
        let nm = NeededObjects {
            buffer,
            logger: Arc::new(logger),
            txn_mgr: Arc::new(TransactionManager::new(clock)),
        };
        Ok(nm)
    }

    fn create_core_db(
        name: String,
        page_size: DBSizeType,
        max_index_key_size: DBSizeType,
    ) -> Result<Self, StoreError> {
        // STORE_AUDIT.md S4: create(true) opens-or-creates, so Db::create
        // on an already-existing path silently reopened it, then
        // unconditionally overwrote its header with page_count=0 and
        // started handing out pages from scratch — destroying any
        // existing data at that path with no warning at all.
        // create_new(true) instead fails with an AlreadyExists io error if
        // either file is already there, matching create()'s documented
        // contract of making a brand new database. Db::open (the "load an
        // existing database" entry point) is unaffected — it has its own,
        // separate file-opening path.
        let f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .clone();
        let mut f = F::open(f, &name)?;
        f.do_lock()?;
        // Phase 6: the first WAL segment, next to the data file.
        let wal = Self::open_segments(&name, &f, page_size)?;
        let mut header = Header {
            magic: MAGIC,
            format_version: HEADER_FORMAT_VERSION,
            first_page_offset: ZERO_PAGE_SIZE,
            page_count: 0,
            page_size,
            last_checkpoint: timestamp(),
            counter: 1,
            checkpoint_lsn: 0,
            max_index_key_size,
            header_checksum: 0,
        };
        header.seal();
        let bytes = to_allocvec(&header)?;
        f.write_all(&bytes)?;
        let header = Arc::new(header);
        let gens = Generator::new();
        gens.create_generator(SYSTEM_TABLE_NAME, None)?;
        let gens = Arc::new(gens);
        let page_count = Arc::new(AtomicU64::new(0));
        let log_file = wal.current_file.do_clone()?;
        let nm = Self::setup_needed_modules(
            header.clone(),
            page_count.clone(),
            f.do_clone()?,
            name.clone(),
            wal,
        )?;

        Ok(Self {
            last_checkpoint: AtomicU128::new(header.last_checkpoint),
            name,
            header,
            file: f,
            log_file,
            page_count,
            tables: Arc::new(RwLock::new(HashMap::new())),
            generator: gens,
            logger: nm.logger,
            tx_mgr: nm.txn_mgr,
            buffer: nm.buffer,
            versions: VersionStore::new(),
            maintenance: Maintenance::new(MAINTENANCE_INTERVAL),
            table_locks: ShardedMap::new(16),
            snapshot_mutex: parking_lot::Mutex::new(()),
            checkpoint_mutex: parking_lot::Mutex::new(()),
            degraded: RwLock::new(None),
            abort_attempts: parking_lot::Mutex::new(HashMap::new()),
            lock_timeouts: std::sync::atomic::AtomicU64::new(0),
            snapshot_limits: RwLock::new(SnapshotLimits {
                max_retained_wal_bytes: DEFAULT_MAX_RETAINED_WAL_BYTES,
                max_version_records: DEFAULT_MAX_VERSION_RECORDS,
            }),
            forced_aborts: parking_lot::Mutex::new(HashMap::new()),
            snapshot_too_old_aborts: std::sync::atomic::AtomicU64::new(0),
            recovered_records: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fail_reverts: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn validate_table_name(&self, name: &String) -> Result<(), StoreError> {
        if name.len() > MAX_TABLE_NAME_LEN {
            return Err(StoreError::TableNameInvalid(MAX_TABLE_NAME_LEN, name.len()));
        }
        if name.starts_with(RESERVED_TABLE_NAME_PREFIX) {
            return Err(StoreError::ReservedTableName(name.to_string()));
        }
        let tables = self.tables.read();
        let present = tables
            .values()
            .map(|t| &t.table.name)
            .position(|n| n == name);
        if present.is_some() {
            return Err(StoreError::DuplicateName(name.to_string()));
        }
        Ok(())
    }

    fn write_system_tables(&self) -> Result<(), StoreError> {
        let page = Page::new_pinned(self.header.page_size, self.buffer.page_overhead());
        let tables = self.tables.read();
        for (i, t) in tables.values().enumerate() {
            let bytes = to_allocvec(&t.table)?;
            // We dont care what the tables id is or if it is consistent across saves.
            page.add_tuple(Tuple::new(i as DBSizeType, &bytes))?;
        }
        self.buffer.write_page(0usize.into(), &page)?;
        let gens = self.generator.get_values()?;
        let page = Page::new_pinned(self.header.page_size, self.buffer.page_overhead());
        page.add_tuple(Tuple::new(0, &to_allocvec(&gens)?))?;
        self.buffer.write_page(1usize.into(), &page)?;
        let page = Page::new_pinned(self.header.page_size, self.buffer.page_overhead());
        page.add_tuple(Tuple::new(0, &to_allocvec(&self.buffer.get_free_pages())?))?;
        self.buffer.write_page(2usize.into(), &page)?;

        Ok(())
    }

    fn create_system_tables(&self) -> Result<(), StoreError> {
        let t = self.buffer.alloc_page(true)?; // system
        assert!(t == 0usize.into());
        let t = self.buffer.alloc_page(true)?; // generators
        assert!(t == 1usize.into());
        let t = self.buffer.alloc_page(true)?; // free pages
        assert!(t == 2usize.into());
        assert!(self.page_count() == 3);
        Ok(())
    }

    fn load_system_tables(&self) -> Result<(), StoreError> {
        if self.page_count() < FIRST_USER_PAGE {
            return Err(StoreError::UnknownError(
                "Unable to load system tables".into(),
            ));
        }
        let page = self.buffer.get_page(SYSTEM_TABLE_PAGE.into())?;
        let mut tables = self.tables.write();
        for t in page.iter() {
            let t: BPlusTree<F> = BPlusTree::from_bytes(
                &t.data,
                self.buffer.clone(),
                self.tx_mgr.clone(),
                self.logger.clone(),
            )?;
            tables.insert(t.table.id, Arc::new(t));
        }
        let page = self.buffer.get_page(GENERATOR_TABLE_PAGE.into())?;
        let tuple = page.get(DBIdType::Int(0))?.unwrap_or_default();
        let gens = from_bytes(&tuple.data)?;
        self.generator.set_values(gens)?;
        let page = self.buffer.get_page(FREE_PAGE_TABLE_PAGE.into())?;
        let tuple = page.get(DBIdType::Int(0))?.unwrap_or_default();
        let free_pages = from_bytes::<Vec<_>>(&tuple.data)?;
        self.buffer.set_free_pages(free_pages);
        Ok(())
    }

    fn get_tables(&self) -> Result<Vec<Table>, StoreError> {
        Ok(self
            .tables
            .read()
            .values()
            .map(|t| t.table.clone())
            .collect::<Vec<_>>())
    }

    pub fn delete<S: AsRef<str>>(name: S) -> Result<(), StoreError> {
        // STORE_AUDIT.md S5: locking is otherwise only ever checked on the
        // way IN (create/open take an exclusive advisory lock) — delete()
        // never checked it at all, so it could unlink a live, in-use
        // database's files out from under whatever process/handle still
        // has it open. delete() has no existing file handle of its own (a
        // static, path-based API), so it opens each file fresh and
        // attempts the same exclusive, non-blocking lock create()/open()
        // use: succeeding means no one else holds it, safe to drop
        // immediately and proceed; failing means some other open handle
        // owns it right now. A path that doesn't exist yet fails this
        // open with NotFound, same as remove_file below would have — not
        // a new failure mode, just surfaced one step earlier.
        let opts = OpenOptions::new().read(true).write(true).clone();
        let f = F::open(opts, name.as_ref())?;
        f.do_lock().map_err(|_| {
            StoreError::UnknownError(format!(
                "refusing to delete {}: still open/locked elsewhere",
                name.as_ref()
            ))
        })?;
        let segments = list_segments(&f, name.as_ref())?;
        drop(f);
        remove_file(name.as_ref())?;
        let handle = F::open(OpenOptions::new().read(true).clone(), name.as_ref());
        for (_, path) in segments {
            match &handle {
                Ok(h) => h.remove_sibling(&path)?,
                Err(_) => remove_file(&path)?,
            }
        }
        Ok(())
    }

    pub fn get_page_data_size(&self) -> usize {
        self.buffer.page_data_size()
    }
}

impl<F: DBFile + 'static> TxnSink for Db<F>
where
    F: DBFile<Item = F>,
{
    fn commit_id(&self, id: TransactionId) -> Result<(), StoreError> {
        Db::commit_id(self, id)
    }

    fn abort_id(&self, id: TransactionId) -> Result<(), StoreError> {
        Db::abort(self, id)
    }
}

impl Db<MemFile> {
    /// What the "disk" would hold if the power were cut right now: a new,
    /// independent pair of `(data file, log file)` containing exactly the
    /// bytes that some `do_sync` has published — nothing written since. The
    /// data file is captured first, then the log, so the log is never older
    /// than the data it protects (a log that is newer is harmless: recovery
    /// re-applies its records idempotently; a log that is older could leave
    /// a flushed page without the record that explains it). Feed the result
    /// to `open_using` to simulate recovery. See `crash_harness`.
    pub fn synced_snapshot(&self) -> (MemFile, MemFile) {
        let _not_during_a_checkpoint = self.snapshot_mutex.lock();
        let data = self.file.synced_snapshot();
        // Phase 6: the log is a set of segments; the returned handle is a
        // namespace holding every segment's synced bytes under its name.
        let disk = MemFile::new();
        for (path, bytes) in self.log_file.synced_siblings(&segment_prefix(&self.name)) {
            disk.add_sibling_from_bytes(&path, bytes);
        }
        (data, disk)
    }
}

pub(crate) fn db_hash(bytes: &[u8]) -> u64 {
    let mut h = 0x811C9DC5;
    for b in bytes {
        h ^= *b as u64;
        h = (h * 0x01000193) & 0xFFFFFFFF;
    }
    h
}

fn init_logger() {
    let res = env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                //"{}:{} {} [{}] - {}",
                "{}:{}:{:?} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                std::thread::current().id(),
                //chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .is_test(true)
        .filter(None, LevelFilter::Debug)
        .try_init();
    if res.is_ok() {
        info!("Logging enabled.")
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use crate::{
        cursor::Cursor,
        db::{DEFAULT_PAGE_SIZE, Db, FileDB, Opener, ZERO_PAGE_SIZE},
        error::StoreError,
        logger::LogHeader,
        memfile::MemFile,
        table::TableIdType,
        tuple::{DBIdType, Tuple},
        txn::{ConflictPolicy, TransactionId},
        valueitem::ValueItem,
    };
    use std::fs::File;
    type TestDB = Db<MemFile>;

    fn make_db_with_table() -> (Arc<TestDB>, TableIdType) {
        let db = TestDB::create("txn_test.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        (db, tid)
    }

    // Simulates a crash: direct clones of the live db's file handles
    // (MemFile::do_clone shares the underlying buffer), without going
    // through close(). close() is a clean shutdown — it flushes
    // everything and truncates the log, since a cleanly-closed db has
    // nothing left that needs replaying — so it can never be used to test
    // replay itself. This is what actually leaves whatever's durable so
    // far sitting in the (unclosed, untruncated) log, exactly like an
    // abrupt process stop would.
    fn crash_clone(db: &TestDB) -> (MemFile, MemFile) {
        (db.file.do_clone().unwrap(), db.log_file.do_clone().unwrap())
    }

    // Every WAL segment visible from `file`'s namespace, oldest first, as
    // (path, live bytes). Phase 6: the log is `<name>.wal.<n>` segments, and
    // a handle taken at create time may be to a segment a checkpoint has
    // since deleted — so tests go through the namespace, never one handle.
    fn wal_segments_of(file: &MemFile) -> Vec<(String, Vec<u8>)> {
        let mut paths: Vec<(u64, String)> = file
            .list_siblings("")
            .unwrap()
            .into_iter()
            .filter_map(|p| {
                let (_, n) = p.rsplit_once(".wal.")?;
                n.parse::<u64>().ok().map(|n| (n, p.clone()))
            })
            .collect();
        paths.sort();
        paths
            .into_iter()
            .map(|(_, p)| {
                let f = file
                    .open_sibling(&p, std::fs::OpenOptions::new().clone())
                    .unwrap();
                (p, f.data())
            })
            .collect()
    }

    // The newest segment's (path, bytes) — what a test that wants to tamper
    // with "the end of the log" works on.
    fn current_segment(db: &TestDB) -> (String, Vec<u8>) {
        wal_segments_of(&db.log_file)
            .pop()
            .expect("a database always has a current segment")
    }

    fn count_records_in_segment(data: &[u8]) -> usize {
        let header_len = LogHeader::encoded_len();
        if data.len() < header_len {
            return 0;
        }
        // Transactional records only: Sequence records (phase 1) are engine
        // bookkeeping and are not what these tests count. Purge records
        // (phase 3: the maintenance thread's vacuum reclaiming a committed
        // tombstone) are the same kind of thing — logged asynchronously,
        // on the maintenance thread's own timer/wake schedule, whenever it
        // happens to notice a reclaimable tombstone. A test that commits a
        // Del and then waits for an exact record count is otherwise racing
        // that thread: it can purge the tombstone (and log a Purge record)
        // before the test takes its snapshot, an extra record with no
        // connection to the test's own operations. Confirmed reachable
        // even before this comment existed — see git blame — at a low but
        // nonzero rate; a slower path anywhere upstream of a commit (e.g.
        // create_table's own checkpoint) makes the window wider.
        crate::logger::scan_log(&data[header_len..])
            .unwrap()
            .records
            .iter()
            .filter(|r| {
                !matches!(
                    r.operation,
                    crate::logger::Operation::Sequence { .. }
                        | crate::logger::Operation::Purge { .. }
                )
            })
            .count()
    }

    // Passive record count across every retained segment (unlike replay):
    // skips each LogHeader, then walks framed records off the raw bytes via
    // the same scan_log recovery uses.
    fn count_log_records(file: &MemFile) -> usize {
        wal_segments_of(file)
            .iter()
            .map(|(_, data)| count_records_in_segment(data))
            .sum()
    }

    // log()'s send() over a bounded channel only guarantees the *previous*
    // message has been dequeued by the writer thread, not that it (or the
    // message just sent) has actually been written to the file yet — under
    // contention (e.g. many tests running in parallel) that write can lag
    // behind the point where a test's last commit() call returns.
    // crash_clone must not race that: it takes a raw, point-in-time clone,
    // so a snapshot taken too early silently omits the last record(s),
    // which then corrupts replay (e.g. a Commit marker missing makes an
    // already-committed transaction look abandoned). Poll for the expected
    // count instead of assuming synchronous delivery.
    fn wait_for_durable_logs(db: &TestDB, expected: usize) {
        for _ in 0..1000 {
            if count_log_records(&db.log_file) == expected {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("timed out waiting for {expected} log record(s) to land");
    }

    // The file header's page_count is only ever (re)written to disk by an
    // explicit write_header call — table creation and ordinary inserts bump
    // the *live*, in-process page_count atomic but never touch the on-disk
    // header. Db::open_using seeds ITS OWN live page_count purely from
    // whatever the header bytes say at open time, so a crash_clone snapshot
    // whose header is stale (e.g. from priming with a checkpoint at the
    // *start* of a test, before the test's own operations allocated more
    // pages) makes the reopened db under-count how many pages actually
    // exist. Replay's own page allocations (insert_if_needed/update_if_needed
    // falling back to a real insert/update) then start from that stale,
    // too-low count and can hand out a page id that collides with one the
    // original session already validly used for a *different* row —
    // silently clobbering it. Symptom: a replay test's assertions fail on a
    // seemingly random row, varying nondeterministically run to run.
    //
    // Fix: sync the header — with the CURRENT, live page_count — immediately
    // before crash_clone, not once at the start. Uses PageBuffer::checkpoint
    // (not Db::checkpoint) specifically so it does NOT touch
    // Logger::checkpoint, which would truncate the redo/undo logs these
    // tests need intact. write_header is itself fire-and-forget, but
    // buffer.checkpoint()'s own synchronous reply is queued strictly after
    // it on the same channel (FIFO) — waiting for that reply guarantees the
    // header write already landed, the same trick
    // test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file
    // uses for its own double-checkpoint.
    fn sync_header_without_truncating_logs<F>(db: &Db<F>)
    where
        F: crate::db::DBFile<Item = F> + 'static,
    {
        let mut hdr = (*db.header).clone();
        hdr.page_count = db.page_count();
        hdr.seal();
        // Synced: PageBuffer::checkpoint no longer goes through the writer
        // thread, so a fire-and-forget header write could still be queued
        // when the caller snapshots the file.
        db.buffer.write_header_synced(hdr).unwrap();
        db.buffer.checkpoint().unwrap();
    }

    fn make_db_with_two_tables() -> (Arc<TestDB>, TableIdType, TableIdType) {
        let db = TestDB::create("txn_test_multi.db").unwrap();
        let ta = db.create_table("table_a".to_string()).unwrap();
        let tb = db.create_table("table_b".to_string()).unwrap();
        (db, ta, tb)
    }

    fn row(id: u64, data: &[u8]) -> Tuple {
        Tuple::new(id, data)
    }

    fn id(n: u64) -> DBIdType {
        DBIdType::Int(n)
    }

    #[test]
    fn test_create() {
        const DB_NAME: &str = "test1.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME);
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.first_page_offset, ZERO_PAGE_SIZE);
        assert_eq!(db.page_count(), 3);
        let (f, l) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, l);
        assert!(db.is_ok());
        let db = db.unwrap();
        assert_eq!(db.header.page_count, 3);
        assert_eq!(db.header.page_size, DEFAULT_PAGE_SIZE);
        //FileDB::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_simple_alloc_page() {
        const DB_NAME: &str = "test2.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME).unwrap();
        let page = db.buffer.alloc_page(false);
        assert!(page.is_ok());
        let page = page.unwrap();
        assert_eq!(page, 3usize.into());
        thread::sleep(Duration::from_millis(100));
        let m = db.file.get_metadata().unwrap();
        assert_eq!(m.len, DEFAULT_PAGE_SIZE * 4 + ZERO_PAGE_SIZE);
        let page = db.buffer.alloc_page(false).unwrap_or(0usize.into());
        assert_eq!(page, 4usize.into());
        thread::sleep(Duration::from_millis(100));
        let m = db.file.get_metadata().unwrap();
        assert_eq!(m.len, ZERO_PAGE_SIZE + 5 * DEFAULT_PAGE_SIZE);
        assert_eq!(db.page_count(), 5);
        let (f, l) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, l).unwrap();
        assert_eq!(db.page_count(), 5);
        //FileDB::delete(DB_NAME).unwrap_or_default();
    }

    #[test]
    fn test_create_table() {
        const DB_NAME: &str = "test3.db";
        //FileDB::delete(DB_NAME).unwrap_or_default();
        let db = TestDB::create(DB_NAME);
        assert!(db.is_ok());
        let db = db.unwrap();
        let r = db.create_table("table_1".to_string());
        assert!(r.is_ok());
        assert_eq!(db.get_tables().unwrap().len(), 1);
        let (f, l) = db.close().unwrap();
        let db = TestDB::open_using(DB_NAME, f, l).unwrap();
        let t = db.get_tables().unwrap();
        assert!(t.len() == 1);
        assert_eq!(t[0].name, "table_1");
        let r = db.create_table("table_1".to_string());
        assert!(matches!(r, Err(StoreError::DuplicateName(_))));
        //FileDB::delete(DB_NAME).unwrap_or_default()
    }

    // create_table's own index_entry_size is bplustree::MAX_ENTRY_BYTES —
    // this is a plain regression test that the convenience wrapper still
    // behaves exactly as it did before create_table_with_index_entry_size
    // existed (a plain Int key round-trips through insert/find normally).
    #[test]
    fn test_create_table_default_index_entry_size_round_trips_normal_rows() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"hello"
        );
    }

    // The concrete motivation for create_table_with_index_entry_size: a
    // composite Rec(IndexKey) key with Str fields easily exceeds
    // bplustree::MAX_ENTRY_BYTES (64, sized for a plain Int key) —
    // create_table's default fails at insert time with a TupleTooLarge
    // error that doesn't help you fix it up front. Sizing the table
    // explicitly avoids that failure entirely.
    #[test]
    fn test_create_table_with_index_entry_size_supports_a_composite_key_default_size_cannot() {
        use crate::valueitem::{IndexKey, ValueItem};

        let db = TestDB::create("index_entry_size_test.db").unwrap();
        let big_key = || -> DBIdType {
            DBIdType::Rec(
                IndexKey::new_from(&[
                    ValueItem::Str(("alpha".repeat(9), 50)),
                    ValueItem::Str(("beta".repeat(9), 50)),
                ])
                .unwrap(),
            )
        };

        // Default-sized table: fails, doesn't silently corrupt anything.
        let default_tid = db.create_table("default_sized".to_string()).unwrap();
        let t = db.begin().unwrap();
        let err = db
            .insert(
                default_tid,
                Tuple::new_with(big_key(), b"v", None, None),
                &t,
            )
            .unwrap_err();
        assert!(
            matches!(err, StoreError::TupleTooLarge(_, _)),
            "got {err:?}"
        );
        drop(t);

        // Deliberately sized table: the identical key/row round-trips.
        let sized_tid = db
            .create_table_with_index_entry_size("deliberately_sized".to_string(), 300)
            .unwrap();
        let t = db.begin().unwrap();
        let key = big_key();
        db.insert(
            sized_tid,
            Tuple::new_with(key.clone(), b"v", None, None),
            &t,
        )
        .unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(sized_tid, key, &t).unwrap().unwrap().data.to_vec(),
            b"v"
        );
    }

    // ── drop_table ──────────────────────────────────────────────────────────

    #[test]
    fn test_drop_table_removes_it_from_the_table_list() {
        let (db, tid) = make_db_with_table();
        assert_eq!(db.get_tables().unwrap().len(), 1);
        db.drop_table("rows").unwrap();
        assert_eq!(db.get_tables().unwrap().len(), 0);
        assert_eq!(db.table_id_by_name("rows").unwrap(), None);
        // The dropped id must actually be gone, not just unnamed — using it
        // (with a caller-held stale TableIdType, exactly the "don't do this
        // concurrently" case drop_table's own doc comment calls out) must
        // fail cleanly, not panic.
        let t = db.begin().unwrap();
        let err = db.find(tid, id(1), &t).unwrap_err();
        assert!(matches!(err, StoreError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_drop_table_missing_name_returns_table_not_found() {
        let (db, _tid) = make_db_with_table();
        let err = db.drop_table("does_not_exist").unwrap_err();
        assert!(matches!(err, StoreError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn test_drop_table_frees_the_name_for_reuse() {
        let (db, _tid) = make_db_with_table();
        db.drop_table("rows").unwrap();
        // Both the table registry (DuplicateName check) and the generator
        // (create_generator's own DuplicateName check) must have let go of
        // the name — creating "rows" again must succeed, not error, and
        // the new table must work normally.
        let new_tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        db.insert(new_tid, row(1, b"fresh"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(new_tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"fresh"
        );
    }

    #[test]
    fn test_drop_table_reuses_its_initial_pages_immediately() {
        let (db, _tid) = make_db_with_table();
        let page_count_before = db.page_count();
        db.drop_table("rows").unwrap();
        // A freshly created table's own initial 2 pages (first_index_page,
        // first_data_page) must come from what drop_table just freed
        // (alloc_page's free-list-pop path), not grow the file further.
        db.create_table("rows2".to_string()).unwrap();
        assert_eq!(
            db.page_count(),
            page_count_before,
            "a table created right after a drop must reuse its pages instead \
             of growing the file"
        );
    }

    #[test]
    fn test_drop_table_frees_every_page_a_multi_split_table_owned() {
        // Direct page-count accounting instead of "does a same-shaped table
        // fit back in the same space" (tried that first — it doesn't hold:
        // reusing pages off a LIFO free list can shift exactly where later
        // splits land, so an identical insert sequence into a fresh table
        // isn't guaranteed to produce an identical page count, only a
        // comparable one). What must hold precisely is that every page the
        // dropped table owned — not most of them — ends up on the free list.
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        for i in 0..500u64 {
            db.insert(tid, row(i, b"some reasonably sized payload"), &t)
                .unwrap();
        }
        db.commit(t).unwrap();
        assert_eq!(
            db.buffer.get_free_pages().len(),
            0,
            "nothing should be on the free list yet"
        );
        // The only pages that exist at this point are the 3 fixed system
        // pages (tables list / generators / free-page list, see
        // create_system_tables) plus everything "rows" itself allocated.
        let table_page_count = db.page_count() - 3;

        db.drop_table("rows").unwrap();

        assert_eq!(
            db.buffer.get_free_pages().len(),
            table_page_count as usize,
            "dropping the table must free every page it owned, not just some \
             of them"
        );
    }

    #[test]
    fn test_drop_table_reclaims_overflow_pages() {
        // Mirrors test_reused_freed_overflow_page_is_safe_to_write_fresh_data_into's
        // setup: a single row large enough to spill across an overflow
        // chain, not just its own primary data page. Same precise
        // free-list accounting as the multi-split test above — the point
        // here specifically is that the *overflow continuation* pages are
        // included in that count, not just the primary index/data pages.
        let (db, tid) = make_db_with_table();
        let page_sz = DEFAULT_PAGE_SIZE as usize;
        let big = vec![b'x'; 5 * page_sz];
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.buffer.get_free_pages().len(), 0);
        let table_page_count = db.page_count() - 3;

        db.drop_table("rows").unwrap();

        assert_eq!(
            db.buffer.get_free_pages().len(),
            table_page_count as usize,
            "dropping the table must free its overflow continuation pages \
             too, not just its primary index/data pages"
        );
    }

    #[test]
    fn test_drop_table_persists_across_close_and_reopen() {
        let db_name = temp_db_path("drop_table_persists");
        FileDB::delete(&db_name).unwrap_or_default();

        let db = FileDB::create(&db_name).unwrap();
        db.create_table("rows".to_string()).unwrap();
        db.drop_table("rows").unwrap();
        assert_eq!(db.get_tables().unwrap().len(), 0);
        let (f, l) = db.close().unwrap();

        let db2 = FileDB::open_using(&db_name, f, l).unwrap();
        assert_eq!(db2.get_tables().unwrap().len(), 0);
        assert_eq!(db2.table_id_by_name("rows").unwrap(), None);
        // The name must still be free after reopen too — proves the
        // generator removal itself persisted, not just the table list.
        db2.create_table("rows".to_string()).unwrap();

        // STORE_AUDIT.md S5: delete() now refuses to remove a still-locked
        // database — db2 (and the file handles close() handed off to it)
        // must actually be gone first, or this cleanup silently no-ops.
        drop(db2);
        FileDB::delete(&db_name).unwrap_or_default();
    }

    // TXN_SIMPLIFICATION_PROGRESS.md phase 6 "known, pre-existing, out of
    // scope" gaps, now fixed: create_table/drop_table used to persist via
    // write_system_tables alone — an eager pwrite with no fsync, so
    // durability depended on whatever periodic checkpoint happened to run
    // next (create_table) and nothing ordered the page resets relative to
    // the catalog write on a real file (drop_table). Both now trigger a
    // real Db::checkpoint() (see their own comments), which is fsync-
    // ordered and atomic, so either operation is durable the instant it
    // returns. Tested with `synced_snapshot` — not `crash_clone` — on
    // purpose: `crash_clone` shares the live, not-yet-synced buffer
    // directly (see its own doc comment), so it would have passed even
    // against the old, buggy code; `synced_snapshot` keeps only what a
    // `do_sync` has actually published, which is what a real power cut
    // would leave behind and is exactly what distinguishes the fix.
    #[test]
    fn test_create_table_survives_a_power_cut_with_no_checkpoint_in_between() {
        let db = TestDB::create("create_table_power_cut.db").unwrap();
        db.create_table("rows".to_string()).unwrap();
        let (f, l) = db.synced_snapshot();
        let db2 = TestDB::open_using("create_table_power_cut.db", f, l).unwrap();
        assert_eq!(db2.get_tables().unwrap().len(), 1);
        let tid = db2
            .table_id_by_name("rows")
            .unwrap()
            .expect("table must survive a power cut immediately after create_table returns");
        let t = db2.begin().unwrap();
        db2.insert(tid, row(1, b"v1"), &t).unwrap();
        db2.commit(t).unwrap();
    }

    #[test]
    fn test_drop_table_survives_a_power_cut_with_no_checkpoint_in_between() {
        let db = TestDB::create("drop_table_power_cut.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        db.drop_table("rows").unwrap();

        let (f, l) = db.synced_snapshot();
        let db2 = TestDB::open_using("drop_table_power_cut.db", f, l).unwrap();
        assert_eq!(
            db2.get_tables().unwrap().len(),
            0,
            "table must be fully gone after a power cut right after drop_table returns"
        );
        assert_eq!(db2.table_id_by_name("rows").unwrap(), None);
        // The name and its pages must be immediately reusable — proves
        // this isn't a half-dropped state (e.g. catalog updated but pages
        // not actually freed, or vice versa).
        let tid2 = db2.create_table("rows".to_string()).unwrap();
        let t = db2.begin().unwrap();
        assert!(
            db2.find(tid2, id(1), &t).unwrap().is_none(),
            "must be a fresh, empty table, not the dropped one's leftover row"
        );
    }

    // ── runs ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_run_append_and_cursor_preserves_order_across_pages() {
        // Small page size so a handful of records forces at least one
        // page-chain extension, not just a single-page happy path.
        let db: Arc<TestDB> = TestDB::create_with_page_size_and_max_index_key_size("run_order.db", 512, 8).unwrap();
        let mut run = db.create_run().unwrap();
        let records: Vec<Vec<u8>> = (0..50).map(|i: u32| i.to_be_bytes().to_vec()).collect();
        for r in &records {
            run.append(r).unwrap();
        }

        let mut cursor = run.cursor().unwrap();
        let mut read_back = Vec::new();
        while let Some(t) = cursor.next().unwrap() {
            read_back.push(t.data().to_vec());
        }
        assert_eq!(
            &read_back, &records,
            "must read back in append order, not sorted"
        );
    }

    #[test]
    fn test_dropping_a_run_frees_its_pages_once_nothing_else_references_them() {
        let db: Arc<TestDB> = TestDB::create_with_page_size_and_max_index_key_size("run_drop_frees.db", 512, 8).unwrap();
        let mut run = db.create_run().unwrap();
        for i in 0..50u32 {
            run.append(&i.to_be_bytes()).unwrap();
        }
        let head = run.head();
        assert_eq!(db.buffer.get_free_pages().len(), 0);

        drop(run);

        assert!(
            db.buffer.get_free_pages().contains(&head),
            "dropping a run's last reference must reclaim its own head page, not just later ones"
        );
        assert!(
            db.buffer.get_free_pages().len() >= 2,
            "a 50-record run at a 512-byte page size must span more than one page"
        );
    }

    #[test]
    fn test_a_live_cursor_keeps_a_dropped_runs_pages_from_being_freed() {
        // A cursor holds its own Arc onto the same page chain the Run
        // does (see RunPages) — dropping the Run it came from must not
        // free pages the cursor is still reading, and the pages must
        // finally free once the cursor itself is also dropped.
        let db: Arc<TestDB> =
            TestDB::create_with_page_size_and_max_index_key_size("run_cursor_keeps_alive.db", 512, 8).unwrap();
        let mut run = db.create_run().unwrap();
        run.append(b"a").unwrap();
        run.append(b"b").unwrap();
        let head = run.head();

        let mut cursor = run.cursor().unwrap();
        drop(run);
        assert!(
            !db.buffer.get_free_pages().contains(&head),
            "the run's pages must survive as long as a cursor still references them"
        );

        assert_eq!(cursor.next().unwrap().unwrap().data().to_vec(), b"a");
        assert_eq!(cursor.next().unwrap().unwrap().data().to_vec(), b"b");
        assert!(cursor.next().unwrap().is_none());

        drop(cursor);
        assert!(
            db.buffer.get_free_pages().contains(&head),
            "once the last cursor referencing it drops too, the run's pages must free"
        );
    }

    // ── transactional insert / find ───────────────────────────────────────────

    #[test]
    fn test_txn_insert_commit_find() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found.expect("row should be visible").data.to_vec(),
            b"hello"
        );
    }

    #[test]
    fn test_txn_find_sees_its_own_uncommitted_insert() {
        // Regression test for find_visible_to's self-write exception: a
        // fresh INSERT's Tuple carries no undo_id, so before this check
        // existed, resolve_visible had nothing to walk back to and
        // Db::find incorrectly reported the row as missing to the very
        // transaction that just wrote it.
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &txn).unwrap();
        let found = db.find(tid, id(1), &txn).unwrap();
        assert_eq!(
            found
                .expect("a transaction must see its own uncommitted insert")
                .data
                .to_vec(),
            b"hello"
        );
        db.commit(txn).unwrap();
    }

    #[test]
    fn test_txn_uncommitted_insert_is_invisible_to_a_different_transaction() {
        // The self-write exception must not leak into cross-transaction
        // visibility — an uncommitted row stays invisible to anyone else
        // exactly as before.
        let (db, tid) = make_db_with_table();
        let writer = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &writer).unwrap();

        let reader = db.begin().unwrap();
        assert!(db.find(tid, id(1), &reader).unwrap().is_none());
        drop(reader);

        db.commit(writer).unwrap();
    }

    #[test]
    fn test_table_scan_in_txn_sees_its_own_uncommitted_insert() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &txn).unwrap();

        let mut cursor = db.table_scan_in_txn(tid, &txn).unwrap();
        let found = cursor.next().unwrap();
        assert_eq!(
            found
                .expect("scan under the writer's own txn must see the row")
                .data
                .to_vec(),
            b"hello"
        );
        assert!(cursor.next().unwrap().is_none());
        drop(cursor);

        db.commit(txn).unwrap();
    }

    #[test]
    fn test_table_scan_without_a_txn_does_not_see_a_concurrent_uncommitted_insert() {
        // table_scan (no caller-supplied txn) still reads only committed
        // state as of when the scan begins — read-your-own-writes only
        // applies when the scan is explicitly run under the writer's own
        // transaction via table_scan_in_txn.
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &txn).unwrap();

        let mut cursor = db.table_scan(tid).unwrap();
        assert!(cursor.next().unwrap().is_none());
        drop(cursor);

        db.commit(txn).unwrap();
    }

    #[test]
    fn test_txn_insert_rollback_not_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"gone"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(found.is_none(), "rolled-back insert must not be visible");
    }

    #[test]
    fn test_txn_dropped_guard_insert_not_visible() {
        // A transaction guard dropped WITHOUT commit/rollback (e.g. an uncaught
        // error) must not leak its writes as committed. Its write stays invisible
        // (parked in `aborting`) and is physically reverted on the next drain.
        let (db, tid) = make_db_with_table();
        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"dropped"), &txn).unwrap();
            // txn dropped here — no commit, no rollback.
        }
        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(
            found.is_none(),
            "a dropped transaction's insert must not be visible as committed"
        );
    }

    #[test]
    fn test_txn_dropped_guard_update_reverts_to_committed() {
        // A dropped guard that UPDATED a committed key must not leak the update;
        // the committed value stands, and is physically restored on the next
        // drain (Mod-revert path).
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t0).unwrap();
        db.commit(t0).unwrap();
        {
            let t1 = db.begin().unwrap();
            db.update(tid, row(1, b"v2"), &t1).unwrap();
            // t1 dropped here — no commit, no rollback.
        }
        let t2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &t2).unwrap();
        drop(t2);
        assert_eq!(
            found.expect("committed v1 must remain").data.to_vec(),
            b"v1",
            "a dropped update must not be visible; committed value stands"
        );
    }

    #[test]
    fn test_txn_dropped_guard_then_reinsert_succeeds() {
        // After a dropped transaction's insert is reverted, the same key can be
        // re-inserted cleanly (its orphaned row/index entry must be gone).
        let (db, tid) = make_db_with_table();
        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"dropped"), &txn).unwrap();
        }
        let txn2 = db.begin().unwrap();
        db.insert(tid, row(1, b"real"), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("re-insert must be visible").data(), b"real");
    }

    #[test]
    fn test_txn_multiple_inserts_commit_all_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn).unwrap();
        db.insert(tid, row(2, b"B"), &txn).unwrap();
        db.insert(tid, row(3, b"C"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"A"
        );
        assert_eq!(
            db.find(tid, id(2), &txn2).unwrap().unwrap().data.to_vec(),
            b"B"
        );
        assert_eq!(
            db.find(tid, id(3), &txn2).unwrap().unwrap().data.to_vec(),
            b"C"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_multiple_inserts_rollback_none_visible() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn).unwrap();
        db.insert(tid, row(2, b"B"), &txn).unwrap();
        db.insert(tid, row(3, b"C"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(db.find(tid, id(1), &txn2).unwrap().is_none());
        assert!(db.find(tid, id(2), &txn2).unwrap().is_none());
        assert!(db.find(tid, id(3), &txn2).unwrap().is_none());
        drop(txn2);
    }

    // ── read isolation (uncommitted writes are invisible) ─────────────────────

    #[test]
    fn test_txn_uncommitted_insert_not_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        // T1 inserts but doesn't commit yet
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(42, b"secret"), &txn1).unwrap();

        // T2 (concurrent) must not see T1's uncommitted row
        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn2).unwrap();
        drop(txn2);
        assert!(
            found.is_none(),
            "uncommitted insert must be invisible to other txns"
        );

        // After T1 commits, T3 should see it
        db.commit(txn1).unwrap();
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("committed row must be visible").data.to_vec(),
            b"secret"
        );
    }

    // ── update ────────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_update_commit_sees_new_data() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("updated row must exist").data.to_vec(), b"v2");
    }

    #[test]
    fn test_txn_uncommitted_update_not_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // T2 updates but doesn't commit
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();

        // T3 must still see the old committed value
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("original must still be visible").data.to_vec(),
            b"v1"
        );

        db.commit(txn2).unwrap();
    }

    // Regression test: before check_write_conflict existed, two
    // transactions both update()ing the same row concurrently both
    // succeeded and both commit()ted with no error at all — the first
    // writer's change was just silently gone. Confirmed via direct repro
    // (this exact sequence) before the fix landed.
    #[test]
    fn test_concurrent_updates_to_the_same_row_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();

        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        let result = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(
            matches!(result, Err(StoreError::WriteConflict(_))),
            "T2 must be rejected while T1's write to the same row is still live: {result:?}"
        );

        db.commit(txn1).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("row must exist").data.to_vec(),
            b"from_t1",
            "T1's committed write must survive, not be silently overwritten"
        );
    }

    // Same conflict, but caught the other direction: T1 begins, updates and
    // *commits* first; T2 (which began before T1 committed, so its
    // snapshot never accounted for T1's write) must still be rejected when
    // it tries to update the same row — first-committer-wins, not
    // first-caller-of-update()-wins.
    #[test]
    fn test_update_after_a_concurrent_transaction_already_committed_the_same_row_conflicts() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();

        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let result = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(
            matches!(result, Err(StoreError::WriteConflict(_))),
            "T2 must be rejected — T1 committed a change to this row after T2's snapshot was \
             taken: {result:?}"
        );
    }

    // The non-conflict case: T1 begins and commits its update to row 1
    // entirely before T2 even begins. T2's own update must succeed —
    // conflict detection must not become so conservative that it blocks
    // ordinary sequential writes.
    #[test]
    fn test_update_after_a_prior_transaction_fully_committed_does_not_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t2"), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(found.expect("row must exist").data.to_vec(), b"from_t2");
    }

    // Two transactions updating two DIFFERENT rows concurrently must not
    // conflict with each other at all — the guard is keyed on the row's
    // own current writer, not on "any other transaction is active".
    #[test]
    fn test_concurrent_updates_to_different_rows_do_not_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.insert(tid, row(2, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();

        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        db.update(tid, row(2, b"from_t2"), &txn2).unwrap();

        db.commit(txn1).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &txn3).unwrap().unwrap().data.to_vec(),
            b"from_t1"
        );
        assert_eq!(
            db.find(tid, id(2), &txn3).unwrap().unwrap().data.to_vec(),
            b"from_t2"
        );
    }

    // remove() must be guarded the same way update() is — a concurrent
    // remove of a row another still-active transaction just wrote must be
    // rejected, not silently tombstone the row out from under it.
    #[test]
    fn test_concurrent_remove_and_update_of_the_same_row_conflicts() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();

        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        let result = db.remove(tid, id(1), &txn2);
        assert!(
            matches!(result, Err(StoreError::WriteConflict(_))),
            "T2's remove must be rejected while T1's write to the same row is still live: \
             {result:?}"
        );
    }

    // A transaction re-updating its OWN not-yet-committed row must never
    // be treated as a conflict against itself.
    #[test]
    fn test_a_transaction_updating_its_own_row_twice_does_not_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        db.update(tid, row(1, b"v1"), &txn1).unwrap();
        db.update(tid, row(1, b"v2"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(found.expect("row must exist").data.to_vec(), b"v2");
    }

    // ── txn hardening: edge cases from TXN_HARDENING.md ────────────────────

    // Regression test for the TOCTOU gap (see TXN_HARDENING.md's first
    // critical item): before BPlusTree::update_checked existed, Db::update
    // did table.find() (its own page lock, released immediately), then
    // check_write_conflict() with no lock held at all, then — much later —
    // a separate table.update() call (a fresh page lock). Two real threads
    // racing through that pair could both read+pass-the-check before
    // either wrote, then race on the final replace, silently losing one
    // commit. This was proven deterministically at the time (manually
    // replaying that exact interleaving) — now that the check, the
    // pre-image resolution, and the physical write all run inside
    // update_checked's single page-lock critical section, there is no
    // longer a seam to inject a racer into at all, so the only way left to
    // exercise this is genuine concurrent threads: two transactions began
    // before either attempts to write (each sees the other as active in
    // its own snapshot), then race an update to the same row through a
    // Barrier to maximize overlap. Whichever thread's update_checked call
    // acquires the page lock first always wins (the row still belongs to
    // the original committed writer at that point); the second is
    // guaranteed to conflict, because it began while the first was already
    // active. Exactly one must ever win — never both, never neither.
    #[test]
    fn test_concurrent_updates_to_the_same_row_never_produce_two_winners_or_a_lost_commit() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let db_a = db.clone();
        let barrier_a = barrier.clone();
        let a = std::thread::spawn(move || {
            let txn = db_a.begin().unwrap();
            barrier_a.wait();
            match db_a.update(tid, row(1, b"from_a"), &txn) {
                Ok(()) => {
                    db_a.commit(txn).unwrap();
                    true
                }
                Err(StoreError::WriteConflict(_)) => {
                    db_a.rollback(txn).unwrap();
                    false
                }
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        });

        let db_b = db.clone();
        let barrier_b = barrier.clone();
        let b = std::thread::spawn(move || {
            let txn = db_b.begin().unwrap();
            barrier_b.wait();
            match db_b.update(tid, row(1, b"from_b"), &txn) {
                Ok(()) => {
                    db_b.commit(txn).unwrap();
                    true
                }
                Err(StoreError::WriteConflict(_)) => {
                    db_b.rollback(txn).unwrap();
                    false
                }
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        });

        let a_won = a.join().unwrap();
        let b_won = b.join().unwrap();
        assert!(
            a_won ^ b_won,
            "exactly one of the two racing updates must win — a_won={a_won}, b_won={b_won}"
        );

        let reader = db.begin().unwrap();
        let found = db.find(tid, id(1), &reader).unwrap().unwrap();
        drop(reader);
        let expected: &[u8] = if a_won { b"from_a" } else { b"from_b" };
        assert_eq!(
            found.data.to_vec(),
            expected,
            "the committed value must match whichever update actually won, never a mix and \
             never the loser's"
        );
    }

    // Documents ConflictPolicy::ContinueOnConflict (the default, used by
    // plain db.begin()): a WriteConflict does not poison the transaction —
    // nothing marks it as doomed, so it can go on to do unrelated work and
    // commit successfully. Contrast with AbortOnConflict below.
    #[test]
    fn test_a_transaction_can_still_commit_after_one_of_its_writes_hits_a_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.insert(tid, row(2, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();

        let conflict = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(matches!(conflict, Err(StoreError::WriteConflict(_))));

        // txn2 goes on to touch an unrelated row and commit anyway.
        db.update(tid, row(2, b"t2_unrelated"), &txn2).unwrap();
        db.commit(txn2).unwrap();
        db.commit(txn1).unwrap();

        let txn3 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(2), &txn3).unwrap().unwrap().data.to_vec(),
            b"t2_unrelated"
        );
    }

    // Mirrors the ContinueOnConflict test above, but with
    // ConflictPolicy::AbortOnConflict: a WriteConflict must now take down
    // the WHOLE transaction — the caller gets a distinct
    // WriteConflictTransactionAborted error (not a plain WriteConflict),
    // and by the time it returns, the losing row's own update has already
    // been rolled back too (not just the conflicting operation refused).
    #[test]
    fn test_abort_on_conflict_rolls_back_the_whole_transaction_on_a_single_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.insert(tid, row(2, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db
            .begin_with_conflict_policy(ConflictPolicy::AbortOnConflict)
            .unwrap();

        // txn2 does some other, perfectly valid work first...
        db.update(tid, row(2, b"t2_row2"), &txn2).unwrap();
        // ...then conflicts with txn1 on row 1.
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        let result = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(
            matches!(result, Err(StoreError::WriteConflictTransactionAborted(_))),
            "{result:?}"
        );

        // txn2 is now fully finished — ANY further use of it, including its
        // earlier, perfectly valid write to row 2, must be rejected: the
        // whole transaction was rolled back, not just the conflicting op.
        let further = db.update(tid, row(2, b"t2_row2_again"), &txn2);
        assert!(
            matches!(further, Err(StoreError::TransactionAlreadyFinished)),
            "{further:?}"
        );
        let commit_result = db.commit(txn2);
        assert!(
            matches!(commit_result, Err(StoreError::TransactionAlreadyFinished)),
            "committing an already-auto-aborted transaction must not silently succeed: \
             {commit_result:?}"
        );

        db.commit(txn1).unwrap();

        // Row 2's earlier, valid write from txn2 must be gone too — the
        // WHOLE transaction rolled back, not just row 1's conflicting op.
        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"from_t1"
        );
        assert_eq!(
            db.find(tid, id(2), &reader).unwrap().unwrap().data.to_vec(),
            b"v0",
            "txn2's earlier write to row 2 must have been rolled back along with everything \
             else in its transaction"
        );
    }

    // An explicit db.rollback() on a transaction AbortOnConflict already
    // auto-aborted must be a safe, harmless no-op — matching how ROLLBACK
    // on an already-aborted transaction behaves in most SQL engines, and
    // matching this codebase's own idempotent revert primitives
    // (update_if_txn/remove_if_txn).
    #[test]
    fn test_explicit_rollback_after_an_auto_abort_is_a_harmless_no_op() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db
            .begin_with_conflict_policy(ConflictPolicy::AbortOnConflict)
            .unwrap();

        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        let result = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(matches!(
            result,
            Err(StoreError::WriteConflictTransactionAborted(_))
        ));

        db.rollback(txn2).unwrap();
        db.commit(txn1).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"from_t1"
        );
    }

    // A transaction that never conflicts at all behaves identically under
    // either policy — AbortOnConflict only changes behavior when a
    // conflict actually happens.
    #[test]
    fn test_abort_on_conflict_does_not_change_behavior_when_nothing_conflicts() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db
            .begin_with_conflict_policy(ConflictPolicy::AbortOnConflict)
            .unwrap();
        db.update(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v1"
        );
    }

    #[test]
    fn test_winner_of_a_conflict_rolling_back_still_frees_the_row_for_a_third_txn() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        assert!(db.update(tid, row(1, b"from_t2"), &txn2).is_err());

        db.rollback(txn1).unwrap(); // winner backs out instead of committing

        let txn3 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t3"), &txn3).unwrap();
        db.commit(txn3).unwrap();

        let txn4 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &txn4).unwrap().unwrap().data.to_vec(),
            b"from_t3"
        );
    }

    #[test]
    fn test_after_a_conflicting_transaction_gives_up_a_third_transaction_can_proceed() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        assert!(db.update(tid, row(1, b"from_t2"), &txn2).is_err());
        db.rollback(txn2).unwrap(); // t2 gives up

        db.commit(txn1).unwrap();

        let txn3 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t3"), &txn3).unwrap();
        db.commit(txn3).unwrap();

        let txn4 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &txn4).unwrap().unwrap().data.to_vec(),
            b"from_t3"
        );
    }

    // An update against a row another transaction has inserted but not yet
    // committed must be a genuine write conflict — not KeyNotFound (the
    // row IS physically present) and not a silent success (that would
    // clobber an insert that might still commit).
    #[test]
    fn test_update_against_an_uncommitted_insert_from_another_txn_conflicts() {
        let (db, tid) = make_db_with_table();
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"from_t1"), &txn1).unwrap();

        let txn2 = db.begin().unwrap();
        let result = db.update(tid, row(1, b"from_t2"), &txn2);
        assert!(
            matches!(result, Err(StoreError::WriteConflict(_))),
            "must be a write conflict, not KeyNotFound or a silent success: {result:?}"
        );
    }

    // TXN_HARDENING.md's second critical item, now fixed: dropping a guard
    // (rather than an explicit db.rollback()) parks the transaction in
    // `aborting` without reverting its write — drain_aborting only ever
    // runs from Db::begin(), so a conflict caused by it used to never
    // clear no matter how many times the blocked caller retried
    // update()/remove() alone; only some OTHER transaction calling
    // begin() elsewhere would unblock it. update()/remove() now
    // opportunistically drain the aborting set and retry once on a
    // WriteConflict (see Db::update_checked_with_retry), so this self-heals
    // within the SAME call — no caller-visible retry loop needed.
    #[test]
    fn test_update_self_heals_a_conflict_against_a_dropped_but_undrained_transaction() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn_stale = db.begin().unwrap();
        let txn_check = db.begin().unwrap(); // snapshot includes txn_stale as active

        db.update(tid, row(1, b"stale"), &txn_stale).unwrap();
        drop(txn_stale); // abandoned, not db.rollback()'d — revert is deferred

        // txn_check's own snapshot recorded txn_stale as active, so a
        // naive single attempt would conflict — but this now succeeds on
        // the first call: the internal retry drains txn_stale (reverting
        // its write back to txn0's committed value) and re-checks against
        // that, which txn_check's snapshot has no quarrel with.
        db.update(tid, row(1, b"check"), &txn_check).unwrap();
        db.commit(txn_check).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"check"
        );
    }

    // Extends the existing single-intervening-commit repeatable-read test
    // to a longer chain: a reader's snapshot must survive ANY number of
    // concurrent commits landing in between its reads, not just one.
    #[test]
    fn test_find_is_repeatable_across_more_than_one_intervening_commit() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.commit(t).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0"
        );

        for v in ["v1", "v2", "v3"] {
            let w = db.begin().unwrap();
            db.update(tid, row(1, v.as_bytes()), &w).unwrap();
            db.commit(w).unwrap();
            assert_eq!(
                db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
                b"v0",
                "reader must still see its original snapshot after commit of {v}"
            );
        }
        drop(reader);
    }

    // table_scan must not observe a write left behind by a transaction
    // that was dropped (not explicitly rolled back) and hasn't been
    // drained yet — mirrors the existing tombstoned/uncommitted scan
    // tests, but specifically for this transient "aborting" state.
    #[test]
    fn test_table_scan_does_not_see_a_dropped_but_undrained_transactions_write() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.commit(t).unwrap();

        let stale = db.begin().unwrap();
        db.update(tid, row(1, b"stale"), &stale).unwrap();
        drop(stale); // aborting, not yet drained

        let mut cursor = db.table_scan(tid).unwrap();
        let found = cursor.next().unwrap().expect("row must be scanned");
        assert_eq!(
            found.data.to_vec(),
            b"v0",
            "scan must not observe a dropped-but-undrained transaction's write"
        );
    }

    // Phantom-insert check, mirroring the existing update-based repeatable
    // read tests: a row inserted AND committed by someone else after this
    // transaction began must stay invisible to it, not just an updated
    // value on a pre-existing row.
    #[test]
    fn test_find_does_not_see_a_row_inserted_and_committed_by_another_txn_after_this_txn_began() {
        let (db, tid) = make_db_with_table();
        let reader = db.begin().unwrap();

        assert!(db.find(tid, id(1), &reader).unwrap().is_none());

        let writer = db.begin().unwrap();
        db.insert(tid, row(1, b"new"), &writer).unwrap();
        db.commit(writer).unwrap();

        assert!(
            db.find(tid, id(1), &reader).unwrap().is_none(),
            "a transaction must not see a row inserted+committed by someone else after it began"
        );
        drop(reader);

        let later = db.begin().unwrap();
        assert!(db.find(tid, id(1), &later).unwrap().is_some());
    }

    // A sequence that only succeeds because a write conflict was correctly
    // detected and resolved (t2 conflicts, rolls back; t1's write is what
    // actually commits) must survive an ordinary close/reopen — replay
    // itself never calls check_write_conflict (it applies committed redo
    // records directly via insert_if_needed/update_if_needed), so this
    // pins down that the conflict feature has no surprising interaction
    // with persistence.
    #[test]
    fn test_close_reopen_preserves_data_written_after_a_resolved_write_conflict() {
        let (db, tid) = make_db_with_table();
        let txn0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &txn0).unwrap();
        db.commit(txn0).unwrap();

        let txn1 = db.begin().unwrap();
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"from_t1"), &txn1).unwrap();
        assert!(db.update(tid, row(1, b"from_t2"), &txn2).is_err());
        db.commit(txn1).unwrap();
        db.rollback(txn2).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let reader = db2.begin().unwrap();
        let found = db2.find(tid, id(1), &reader).unwrap();
        drop(reader);
        assert_eq!(
            found.expect("row must survive close/reopen").data.to_vec(),
            b"from_t1"
        );
    }

    // Real multi-threaded stress test: several threads hammering a small,
    // shared set of rows with a retry-on-conflict update loop. The main
    // things this guards against: a panic or deadlock anywhere in the
    // conflict-check/commit/rollback path under genuine contention, and
    // (best-effort — the TOCTOU window above is narrow) a chance at
    // empirically catching the same lost-update gap the deterministic test
    // above proves directly.
    #[test]
    fn test_concurrent_writers_stress_no_panics_no_deadlocks_no_missing_rows() {
        const ROWS: u64 = 4;
        const THREADS: usize = 8;
        const ITERS: usize = 50;

        let (db, tid) = make_db_with_table();
        let setup = db.begin().unwrap();
        for i in 0..ROWS {
            db.insert(tid, row(i, b"init"), &setup).unwrap();
        }
        db.commit(setup).unwrap();

        let mut handles = vec![];
        for t in 0..THREADS {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..ITERS {
                    let key = (i as u64 + t as u64) % ROWS;
                    let val = format!("t{t}-{i}");
                    loop {
                        let txn = db.begin().unwrap();
                        match db.update(tid, row(key, val.as_bytes()), &txn) {
                            Ok(()) => {
                                db.commit(txn).unwrap();
                                break;
                            }
                            Err(StoreError::WriteConflict(_)) => {
                                db.rollback(txn).unwrap();
                            }
                            Err(e) => panic!("unexpected error: {e:?}"),
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let reader = db.begin().unwrap();
        for i in 0..ROWS {
            assert!(
                db.find(tid, id(i), &reader).unwrap().is_some(),
                "row {i} must still exist after concurrent contention"
            );
        }
    }

    // Regression test for snapshot-isolation visibility (Db::find_visible_to)
    // AND deferred undo discard (Logger::discard_or_defer_undo /
    // drain_ready_undo_discards): a transaction's reads must stay internally
    // consistent even when a concurrent transaction updates AND COMMITS the
    // same row in between. Before find_visible_to existed, find()/
    // table_scan/range_scan all used find_last_committed, which decides
    // visibility live against whatever is committed "right now" — so a
    // transaction could see a different answer to the same read depending
    // purely on when, during its own lifetime, it happened to ask.
    // TransactionManager already captured a snapshot of active transactions
    // at begin() (and exposed it via TransactionManager::snapshot()) but
    // nothing consulted it — dead scaffolding for a feature that wasn't
    // actually wired up.
    //
    // Getting this right needed two parts, not one: find_visible_to alone
    // isn't enough, because Logger::log_undo used to discard a committing
    // transaction's ENTIRE undo trail unconditionally — so by the time a
    // still-open reader asked again, the pre-image needed to keep honoring
    // its snapshot was already gone (confirmed the hard way: an earlier
    // version of this fix made a real, committed row incorrectly look
    // missing after exactly this sequence). discard_or_defer_undo closes
    // that gap by mirroring TransactionManager's aborting/drain_aborting
    // pattern: a commit doesn't discard its undo trail if any other
    // transaction is still active (and might have it in its own snapshot)
    // — it parks the obligation and Db::begin's opportunistic drain finishes
    // the job once every such transaction has actually finished.
    #[test]
    fn test_find_is_repeatable_within_a_transaction_across_a_concurrent_write_and_commit() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();

        // Reader begins BEFORE the concurrent update below.
        let reader = db.begin().unwrap();
        let first_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must exist for the first read");
        assert_eq!(first_read.data.to_vec(), b"v1");

        // A fully separate, concurrent transaction updates but does NOT
        // commit yet.
        let writer = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &writer).unwrap();

        // The SAME reader, reading the SAME row again, must see the value it
        // saw the first time — not the concurrent, still-uncommitted write.
        let second_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must still be visible on the second read");
        assert_eq!(
            second_read.data.to_vec(),
            b"v1",
            "a transaction's own reads must stay consistent across a concurrent, uncommitted write"
        );

        // The writer now commits WHILE reader is still open — this is the
        // part that needs deferred undo discard: without it, the pre-image
        // "v1" would be gone by the next line.
        db.commit(writer).unwrap();

        let third_read = db
            .find(tid, id(1), &reader)
            .unwrap()
            .expect("row must still be visible after the concurrent commit");
        assert_eq!(
            third_read.data.to_vec(),
            b"v1",
            "a transaction's reads must stay consistent even across a concurrent COMMIT, \
             not just a concurrent still-active write"
        );
        drop(reader);

        // A transaction that begins AFTER the writer committed must see the
        // new value — this isn't a permanently-stuck-in-the-past view, just
        // a per-transaction snapshot taken at begin() time.
        let later_reader = db.begin().unwrap();
        let later_read = db
            .find(tid, id(1), &later_reader)
            .unwrap()
            .expect("row must exist for a fresh transaction");
        assert_eq!(later_read.data.to_vec(), b"v2");
        drop(later_reader);
    }

    // Same guarantee, exercised through table_scan rather than a point
    // find() — the cursors resolve visibility the same way, so this
    // confirms the fix isn't find()-specific.
    #[test]
    fn test_table_scan_is_repeatable_within_a_transaction_across_a_concurrent_write_and_commit() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.insert(tid, row(2, b"v2"), &t).unwrap();
        db.commit(t).unwrap();

        // table_scan(None) begins its own internal Transaction, held for the
        // cursor's whole lifetime across every next() call — reading row 1
        // here starts that transaction's snapshot.
        let mut cursor = db.table_scan(tid).unwrap();
        let first = cursor.next().unwrap().expect("row 1 must be scanned");
        assert_eq!(first.data.to_vec(), b"v1");

        // A fully separate, concurrent transaction updates row 2 (not yet
        // reached by the scan) and commits, WHILE the cursor's transaction
        // is still open.
        let writer = db.begin().unwrap();
        db.update(tid, row(2, b"v2-updated"), &writer).unwrap();
        db.commit(writer).unwrap();

        // Continuing the SAME cursor (same underlying transaction) must
        // still see row 2's pre-commit value — the scan's snapshot was
        // taken when the cursor was created, not re-taken per row, and
        // deferred undo discard kept that pre-image reachable.
        let second = cursor.next().unwrap().expect("row 2 must be scanned");
        assert_eq!(
            second.data.to_vec(),
            b"v2",
            "a scan's own transaction must not observe a commit that landed after it began"
        );
        assert!(cursor.next().unwrap().is_none());
        drop(cursor);

        // A fresh scan (fresh transaction) must see the update — confirms
        // this isn't permanently stale, just snapshotted at begin() time.
        let mut cursor2 = db.table_scan(tid).unwrap();
        let row1_again = cursor2
            .next()
            .unwrap()
            .expect("row 1 must still be scanned");
        assert_eq!(row1_again.data.to_vec(), b"v1");
        let row2_again = cursor2
            .next()
            .unwrap()
            .expect("row 2 must still be scanned");
        assert_eq!(row2_again.data.to_vec(), b"v2-updated");
    }

    #[test]
    fn test_txn_update_nonexistent_returns_err() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        let r = db.update(tid, row(99, b"x"), &txn);
        assert!(
            matches!(r, Err(StoreError::KeyNotFound(_))),
            "updating missing row must return KeyNotFound, got {r:?}"
        );
        // No writes were logged for this txn; dropping the guard rolls it
        // back at the manager level, which is sufficient to remove it from
        // the active set.
        drop(txn);
    }

    // ── remove ────────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_remove_commit_not_findable() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"bye"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert!(found.is_none(), "committed remove must make row invisible");
    }

    #[test]
    fn test_txn_uncommitted_remove_row_still_visible_to_concurrent_reader() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"alive"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // T2 removes but doesn't commit yet
        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();

        // T3 must still see the row (T2 is uncommitted)
        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found
                .expect("row must still be visible before remove commits")
                .data
                .to_vec(),
            b"alive"
        );

        db.commit(txn2).unwrap();
    }

    #[test]
    fn test_txn_remove_nonexistent_returns_err() {
        let (db, tid) = make_db_with_table();
        let txn = db.begin().unwrap();
        let r = db.remove(tid, id(999), &txn);
        assert!(
            matches!(r, Err(StoreError::KeyNotFound(_))),
            "removing missing row must return KeyNotFound, got {r:?}"
        );
        drop(txn); // no writes logged → drop cleans up the active set
    }

    // ── multiple operations in a single transaction ───────────────────────────

    #[test]
    fn test_txn_multiple_ops_in_one_txn_commit() {
        let (db, tid) = make_db_with_table();

        // Seed three rows
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"A"), &txn1).unwrap();
        db.insert(tid, row(2, b"B"), &txn1).unwrap();
        db.insert(tid, row(3, b"C"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        // One txn: update row 1, leave row 2 alone, remove row 3
        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"A_v2"), &txn2).unwrap();
        db.remove(tid, id(3), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &txn3).unwrap().unwrap().data.to_vec(),
            b"A_v2"
        );
        assert_eq!(
            db.find(tid, id(2), &txn3).unwrap().unwrap().data.to_vec(),
            b"B"
        );
        assert!(
            db.find(tid, id(3), &txn3).unwrap().is_none(),
            "removed row must be gone"
        );
        drop(txn3);
    }

    #[test]
    fn test_txn_large_number_of_inserts_all_findable() {
        let (db, tid) = make_db_with_table();
        const N: u64 = 200;

        let txn = db.begin().unwrap();
        for i in 0..N {
            db.insert(tid, row(i, format!("val_{i}").as_bytes()), &txn)
                .unwrap();
        }
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        for i in 0..N {
            let found = db.find(tid, id(i), &txn2).unwrap();
            assert_eq!(
                found
                    .unwrap_or_else(|| panic!("row {i} missing"))
                    .data
                    .to_vec(),
                format!("val_{i}").as_bytes(),
                "row {i} has wrong data"
            );
        }
        drop(txn2);
    }

    // ── persistence (close + reopen) ─────────────────────────────────────────

    #[test]
    fn test_txn_close_reopen_data_persists() {
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        db.insert(tid, row(1, b"persistent"), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found.expect("data must survive close/reopen").data.to_vec(),
            b"persistent"
        );
    }

    #[test]
    fn test_txn_close_reopen_new_txn_id_does_not_collide_with_prior_session() {
        // Regression test for the generator-restoration bug: open_using() used to
        // assign a fresh Generator to self.generator instead of reusing the Arc
        // passed to TransactionManager, so the txn id sequence silently restarted
        // at 0 after every reopen and collided with ids from the prior session.
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        let first_txn_id = txn.id();
        db.insert(tid, row(1, b"v1"), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let txn2 = db2.begin().unwrap();
        assert_ne!(
            txn2.id(),
            first_txn_id,
            "txn id sequence must not restart after reopen"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_close_reopen_removed_row_stays_gone() {
        let (db, tid) = make_db_with_table();

        let txn = db.begin().unwrap();
        db.insert(tid, row(7, b"temp"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn = db.begin().unwrap();
        db.remove(tid, id(7), &txn).unwrap();
        db.commit(txn).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(7), &txn2).unwrap();
        drop(txn2);
        assert!(found.is_none(), "removed row must stay gone after reopen");
    }

    // ── error cases ───────────────────────────────────────────────────────────

    #[test]
    fn test_txn_duplicate_insert_returns_err() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"first"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        let r = db.insert(tid, row(1, b"second"), &txn2);
        assert!(
            matches!(r, Err(StoreError::DuplicateKey(_))),
            "duplicate insert must return DuplicateKey, got {r:?}"
        );
        drop(txn2); // insert failed before any writes were logged → drop cleans up active set
    }

    #[test]
    fn test_txn_insert_on_nonexistent_table_returns_err() {
        let (db, _) = make_db_with_table();
        let fake_tid: TableIdType = 9999u64.into();
        let txn = db.begin().unwrap();
        let r = db.insert(fake_tid, row(1, b"x"), &txn);
        assert!(
            matches!(r, Err(StoreError::TableNotFound(_))),
            "expected TableNotFound, got {r:?}"
        );
        drop(txn); // no writes logged → drop cleans up the active set
    }

    // ── RAII guard ────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_drop_without_explicit_commit_rolls_back_at_mgr_level() {
        // Transaction::Drop calls mgr.rollback (not Db::rollback), so it doesn't
        // replay undo ops, but it does remove the txn from the active set — which
        // means concurrent readers stop being blocked by it.
        let (db, tid) = make_db_with_table();

        {
            let txn = db.begin().unwrap();
            db.insert(tid, row(1, b"ephemeral"), &txn).unwrap();
            // txn drops here → mgr.rollback fires but undo-log replay does NOT
        }

        // Because undo-log replay didn't fire, the row may or may not be
        // physically present — but the guard-level test is that the txn is no
        // longer active (so it can't block readers). Use Db::rollback for full
        // application-level undo.
        assert_eq!(
            db.tx_mgr.active_count(),
            0,
            "dropped txn must be removed from active set"
        );
    }

    // ── rollback of Mod/Del restores the pre-image ────────────────────────────

    #[test]
    fn test_txn_update_rollback_sees_original_data() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &txn2).unwrap();
        db.rollback(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found.expect("original row must still exist").data.to_vec(),
            b"v1"
        );
    }

    #[test]
    fn test_txn_remove_rollback_row_still_visible() {
        let (db, tid) = make_db_with_table();

        let txn1 = db.begin().unwrap();
        db.insert(tid, row(1, b"alive"), &txn1).unwrap();
        db.commit(txn1).unwrap();

        let txn2 = db.begin().unwrap();
        db.remove(tid, id(1), &txn2).unwrap();
        db.rollback(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found
                .expect("row must survive a rolled-back remove")
                .data
                .to_vec(),
            b"alive"
        );
    }

    // ── multi-table transactions ───────────────────────────────────────────────

    #[test]
    fn test_txn_insert_across_two_tables_commit_both_visible() {
        let (db, ta, tb) = make_db_with_two_tables();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &txn).unwrap();
        db.insert(tb, row(1, b"b1"), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a1"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"b1"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_insert_across_two_tables_rollback_neither_visible() {
        let (db, ta, tb) = make_db_with_two_tables();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &txn).unwrap();
        db.insert(tb, row(1, b"b1"), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(db.find(ta, id(1), &txn2).unwrap().is_none());
        assert!(db.find(tb, id(1), &txn2).unwrap().is_none());
        drop(txn2);
    }

    #[test]
    fn test_txn_update_and_remove_across_two_tables_commit() {
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(ta, row(1, b"a_v1"), &setup).unwrap();
        db.insert(tb, row(1, b"b_v1"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.update(ta, row(1, b"a_v2"), &txn).unwrap();
        db.remove(tb, id(1), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a_v2"
        );
        assert!(db.find(tb, id(1), &txn2).unwrap().is_none());
        drop(txn2);
    }

    #[test]
    fn test_txn_update_and_remove_across_two_tables_rollback_restores_both() {
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(ta, row(1, b"a_v1"), &setup).unwrap();
        db.insert(tb, row(1, b"b_v1"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.update(ta, row(1, b"a_v2"), &txn).unwrap();
        db.remove(tb, id(1), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"a_v1",
            "table A update must be rolled back"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"b_v1",
            "table B remove must be rolled back"
        );
        drop(txn2);
    }

    #[test]
    fn test_txn_partial_failure_across_tables_rollback_undoes_successful_table() {
        // Table A's insert succeeds; table B's insert fails (duplicate key
        // already present, inserted by an earlier committed txn). Rolling back
        // the failed txn must undo table A's insert even though table B's
        // write never got logged in the first place.
        let (db, ta, tb) = make_db_with_two_tables();

        let setup = db.begin().unwrap();
        db.insert(tb, row(1, b"existing"), &setup).unwrap();
        db.commit(setup).unwrap();

        let txn = db.begin().unwrap();
        db.insert(ta, row(1, b"a_new"), &txn).unwrap();
        let r = db.insert(tb, row(1, b"dup"), &txn);
        assert!(
            matches!(r, Err(StoreError::DuplicateKey(_))),
            "expected DuplicateKey, got {r:?}"
        );
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        assert!(
            db.find(ta, id(1), &txn2).unwrap().is_none(),
            "table A's successful insert must be undone by the txn-wide rollback"
        );
        assert_eq!(
            db.find(tb, id(1), &txn2).unwrap().unwrap().data.to_vec(),
            b"existing",
            "table B must be unaffected by the failed duplicate insert"
        );
        drop(txn2);
    }

    // ── large-object (multi-page overflow) ───────────────────────────────────
    // DEFAULT_PAGE_SIZE=16384, page_data_size=16304.
    // A payload of N bytes → serialized tuple ≈ N+20 bytes.
    // Spans: 2 pages ≈ 20 KB payload, 4 pages ≈ 50 KB, 5 pages ≈ 70 KB.

    #[test]
    fn test_large_object_commit_is_findable() {
        let (db, tid) = make_db_with_table();
        // 50 KB payload → serialized ≈ 50 020 B → 4 overflow pages
        let big = vec![b'A'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found
                .expect("large object must be visible after commit")
                .data
                .to_vec(),
            big
        );
    }

    #[test]
    fn test_large_object_rollback_not_visible() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'B'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &big), &txn).unwrap();
        db.rollback(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &txn2).unwrap();
        drop(txn2);
        assert!(
            found.is_none(),
            "rolled-back large object must not be visible"
        );
    }

    #[test]
    fn test_very_large_object_five_pages_commit() {
        let (db, tid) = make_db_with_table();
        // 70 KB payload → serialized ≈ 70 020 B → 5 overflow pages
        let big = vec![b'C'; 70_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(42, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        let found = db.find(tid, id(42), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found
                .expect("5-page object must survive commit")
                .data
                .to_vec(),
            big
        );
    }

    #[test]
    fn test_large_objects_persist_across_close_reopen() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'D'; 50_000];
        let txn = db.begin().unwrap();
        db.insert(tid, row(99, &big), &txn).unwrap();
        db.commit(txn).unwrap();

        // Close and reopen — overflow pages must be readable from disk
        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let txn2 = db2.begin().unwrap();
        let found = db2.find(tid, id(99), &txn2).unwrap();
        drop(txn2);
        assert_eq!(
            found
                .expect("large object must survive close/reopen")
                .data
                .to_vec(),
            big
        );
    }

    #[test]
    fn test_multiple_large_objects_same_txn_commit() {
        let (db, tid) = make_db_with_table();
        // Three objects that collectively span many overflow pages
        let small = vec![b'E'; 20_000]; // 2 pages each
        let txn = db.begin().unwrap();
        db.insert(tid, row(1, &small), &txn).unwrap();
        db.insert(tid, row(2, &small), &txn).unwrap();
        db.insert(tid, row(3, &small), &txn).unwrap();
        db.commit(txn).unwrap();

        let txn2 = db.begin().unwrap();
        for i in 1u64..=3 {
            let found = db.find(tid, id(i), &txn2).unwrap();
            assert_eq!(
                found
                    .unwrap_or_else(|| panic!("row {i} missing"))
                    .data
                    .to_vec(),
                small
            );
        }
        drop(txn2);
    }

    #[test]
    fn test_large_object_rollback_then_reinsert_succeeds() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'F'; 50_000];

        // Insert + rollback
        let txn1 = db.begin().unwrap();
        db.insert(tid, row(7, &big), &txn1).unwrap();
        db.rollback(txn1).unwrap();

        // Re-insert the same key + commit
        let txn2 = db.begin().unwrap();
        db.insert(tid, row(7, &big), &txn2).unwrap();
        db.commit(txn2).unwrap();

        let txn3 = db.begin().unwrap();
        let found = db.find(tid, id(7), &txn3).unwrap();
        drop(txn3);
        assert_eq!(
            found
                .expect("re-inserted large object must be visible")
                .data
                .to_vec(),
            big
        );
    }

    // ── large-object: exercise EVERY public op at 4–8× the page size ──────────

    /// A payload of `len` bytes whose byte at each position varies with both the
    /// position and `seed`. A uniform `vec![b'X'; len]` would still verify equal
    /// even if the overflow-page chain were reassembled out of order or a page
    /// were duplicated; a position-dependent pattern catches those.
    fn big_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((i as u64).wrapping_mul(31).wrapping_add(seed as u64) & 0xFF) as u8)
            .collect()
    }

    #[test]
    fn test_large_object_full_lifecycle_all_ops() {
        // One object driven through every public operation while it spans 4–8
        // overflow pages: insert, find, update (grow), update (shrink), remove,
        // re-insert, commit, rollback, and close/reopen. DEFAULT_PAGE_SIZE=16 KB,
        // so 4×≈64 KB … 8×≈128 KB payloads.
        let page = DEFAULT_PAGE_SIZE as usize;
        assert!((4 * page..=8 * page).contains(&(6 * page)));
        let base = big_payload(1, 6 * page); // ~6× page
        let grown = big_payload(2, 8 * page); // ~8× page — more overflow pages
        let shrunk = big_payload(3, 4 * page); // ~4× page — fewer overflow pages
        let reins = big_payload(4, 5 * page); // ~5× page

        let (db, tid) = make_db_with_table();

        // insert + commit → find returns exactly what went in
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &base), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("inserted")
                .data
                .to_vec(),
            base
        );
        db.rollback(t).unwrap();

        // update that GROWS the object (allocates more overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("grown")
                .data
                .to_vec(),
            grown
        );
        db.rollback(t).unwrap();

        // update that SHRINKS the object (frees overflow pages)
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t)
                .unwrap()
                .expect("shrunk")
                .data
                .to_vec(),
            shrunk
        );
        db.rollback(t).unwrap();

        // remove → gone
        let t = db.begin().unwrap();
        db.remove(tid, id(1), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        assert!(
            db.find(tid, id(1), &t).unwrap().is_none(),
            "removed large object must be gone"
        );
        db.rollback(t).unwrap();

        // re-insert, then close/reopen → overflow chain readable from storage
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &reins), &t).unwrap();
        db.commit(t).unwrap();
        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t)
                .unwrap()
                .expect("reopened")
                .data
                .to_vec(),
            reins
        );
        db2.rollback(t).unwrap();
    }

    #[test]
    fn test_large_object_update_rollback_keeps_committed() {
        // Rolling back an update between two large values must restore the exact
        // committed overflow chain (Mod-revert across multiple pages).
        let page = DEFAULT_PAGE_SIZE as usize;
        let v1 = big_payload(10, 6 * page);
        let v2 = big_payload(20, 8 * page);
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(5, &v1), &t).unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        db.update(tid, row(5, &v2), &t).unwrap();
        db.rollback(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(5), &t)
                .unwrap()
                .expect("v1 must stand")
                .data
                .to_vec(),
            v1,
            "rolled-back large update must leave the committed value intact"
        );
        db.rollback(t).unwrap();
    }

    // A data page holding many small tuples, all updated at least once, must
    // never have its next_page (the link to the next sibling data page)
    // clobbered. update()'s replacement tuple is a few bytes larger than
    // what insert() originally wrote (this sets undo_id, which insert
    // leaves None) — on a page already packed to capacity, updating every
    // row on it used to push page_used_size past usable_data_size, which
    // handle_large_page_size misread as "this page needs a single-tuple
    // overflow chain", overwriting next_page with an overflow-page id
    // instead of the real next sibling. table_scan then silently
    // undercounted or hit a deserialization error trying to read that
    // "sibling" as an ordinary multi-tuple page. Found via a performance
    // harness (examples/perf) hitting exactly this after a bulk update.
    #[test]
    fn test_table_scan_correct_after_updating_every_row_across_multiple_data_pages() {
        let (db, tid) = make_db_with_table();
        // Enough small rows to span multiple data pages at the default page
        // size (empirically ~200 rows/page for a 64B value at 16KiB pages).
        let n = 500u64;
        let value = vec![b'v'; 64];
        for i in 0..n {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, &value), &t).unwrap();
            db.commit(t).unwrap();
        }

        // Update every row once, in reverse order (touches pages in a
        // different order than they were filled).
        for i in (0..n).rev() {
            let t = db.begin().unwrap();
            db.update(tid, row(i, &value), &t).unwrap();
            db.commit(t).unwrap();
        }

        let mut scanned = 0u64;
        let mut cursor = db.table_scan(tid).unwrap();
        while cursor.next().unwrap().is_some() {
            scanned += 1;
        }
        assert_eq!(
            scanned, n,
            "table_scan must see every row after all of them have been updated, \
             not stop early or error partway through the data-page chain"
        );

        // Every row must also still be independently findable with the
        // right value — relocation (if it happened) must have kept the
        // index pointing at wherever the row actually landed.
        let t = db.begin().unwrap();
        for i in 0..n {
            assert_eq!(
                db.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after update-all"))
                    .data
                    .to_vec(),
                value
            );
        }
    }

    #[test]
    fn test_large_object_remove_rollback_still_visible() {
        // Rolling back the removal of a large object must leave it fully readable.
        let page = DEFAULT_PAGE_SIZE as usize;
        let v = big_payload(30, 7 * page);
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(8, &v), &t).unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        db.remove(tid, id(8), &t).unwrap();
        db.rollback(t).unwrap();

        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(8), &t)
                .unwrap()
                .expect("large object must survive a rolled-back remove")
                .data
                .to_vec(),
            v
        );
        db.rollback(t).unwrap();
    }

    // ── checkpoint ─────────────────────────────────────────────────────────
    // Db::checkpoint() drains the page buffer's deferred-write queue (making
    // every page dirtied so far durable to the backing store) and then
    // persists an updated header (page_count / last_checkpoint) — all without
    // requiring a full close(). These tests exercise both halves of that
    // contract, plus that checkpoint is a purely physical operation with no
    // effect on logical (MVCC) visibility.

    use crate::constant::timestamp;
    use crate::db::Header;
    use postcard::from_bytes;

    // Reads the on-disk header directly via the DBFile's own positioned read,
    // bypassing the page cache entirely, so these tests observe exactly what
    // checkpoint() has made durable rather than what's merely cached in
    // memory. A generous fixed-size buffer is fine: postcard ignores trailing
    // unconsumed bytes when deserializing.
    fn read_raw_header(db: &TestDB) -> Header {
        let mut buf = vec![0u8; 128];
        db.file.pread(&mut buf, 0).unwrap();
        from_bytes(&buf).unwrap()
    }

    // PageBuffer::write_header enqueues the header write asynchronously (it
    // does not round-trip like the page-flush half of checkpoint does), so
    // there is a short window after checkpoint() returns before the new
    // header is actually durable. Poll instead of asserting immediately.
    fn wait_for_raw_last_checkpoint_at_least(db: &TestDB, min_ts: u128) -> Header {
        for _ in 0..200 {
            let h = read_raw_header(db);
            if h.last_checkpoint >= min_ts {
                return h;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("persisted header's last_checkpoint never reached >= {min_ts}");
    }

    #[test]
    fn test_checkpoint_on_empty_db_succeeds() {
        let (db, _tid) = make_db_with_table();
        assert!(db.checkpoint().is_ok());
    }

    #[test]
    fn test_checkpoint_is_idempotent() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"hello"), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();
        let first = wait_for_raw_last_checkpoint_at_least(&db, 0);

        db.checkpoint().unwrap();
        let second = wait_for_raw_last_checkpoint_at_least(&db, first.last_checkpoint);

        assert!(
            second.last_checkpoint >= first.last_checkpoint,
            "a second, no-op checkpoint must not regress last_checkpoint"
        );
        assert_eq!(second.page_count, db.page_count());
    }

    #[test]
    fn test_checkpoint_flushes_allocated_pages_to_storage() {
        let (db, tid) = make_db_with_table();
        // Enough rows to allocate several data pages, not just the initial
        // system/index pages created at table creation.
        for i in 0..200u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("value-{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        db.checkpoint().unwrap();

        // Every page up to page_count must physically exist in the backing
        // store once checkpoint() returns — a page write still sitting in the
        // writer's deferred queue would leave the file shorter than this.
        let expected_min_len = db.header.first_page_offset + db.page_count() * db.header.page_size;
        // The header write itself is async (see wait_for_raw_last_checkpoint_at_least);
        // poll get_metadata the same way rather than asserting immediately.
        let mut len = db.file.get_metadata().unwrap().len;
        for _ in 0..200 {
            if len >= expected_min_len {
                break;
            }
            thread::sleep(Duration::from_millis(2));
            len = db.file.get_metadata().unwrap().len;
        }
        assert!(
            len >= expected_min_len,
            "checkpoint must flush every allocated page to storage: file len {len} < expected {expected_min_len}"
        );
    }

    #[test]
    fn test_checkpoint_persists_page_count_and_last_checkpoint() {
        let (db, tid) = make_db_with_table();
        for i in 0..50u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }

        let before = timestamp();
        db.checkpoint().unwrap();
        let persisted = wait_for_raw_last_checkpoint_at_least(&db, before);

        assert_eq!(
            persisted.page_count,
            db.page_count(),
            "persisted header page_count must match the live allocator state"
        );
        assert!(persisted.last_checkpoint >= before);
    }

    #[test]
    fn test_checkpoint_does_not_affect_visibility_of_rolled_back_data() {
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"committed"), &t0).unwrap();
        db.commit(t0).unwrap();

        let t1 = db.begin().unwrap();
        db.update(tid, row(1, b"uncommitted-update"), &t1).unwrap();
        db.rollback(t1).unwrap();

        // Checkpoint is purely physical; it must not resurrect or otherwise
        // change what's logically visible.
        db.checkpoint().unwrap();

        let t2 = db.begin().unwrap();
        let found = db.find(tid, id(1), &t2).unwrap();
        drop(t2);
        assert_eq!(
            found.expect("row must still exist").data.to_vec(),
            b"committed",
            "checkpoint must not make a rolled-back write visible"
        );
    }

    #[test]
    fn test_checkpoint_across_multiple_tables() {
        let (db, ta, tb) = make_db_with_two_tables();
        let t = db.begin().unwrap();
        db.insert(ta, row(1, b"a1"), &t).unwrap();
        db.insert(tb, row(1, b"b1"), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();

        let t2 = db.begin().unwrap();
        assert_eq!(
            db.find(ta, id(1), &t2).unwrap().unwrap().data.to_vec(),
            b"a1"
        );
        assert_eq!(
            db.find(tb, id(1), &t2).unwrap().unwrap().data.to_vec(),
            b"b1"
        );
        drop(t2);
    }

    #[test]
    fn test_checkpoint_with_large_overflow_object() {
        let (db, tid) = make_db_with_table();
        let big = vec![b'K'; 60_000]; // several overflow pages
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big), &t).unwrap();
        db.commit(t).unwrap();

        db.checkpoint().unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let t2 = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t2)
                .unwrap()
                .expect("large object must survive checkpoint + close/reopen")
                .data
                .to_vec(),
            big
        );
        drop(t2);
    }

    // --- log-based crash recovery replay (Db::load_logs / process_redo /
    // process_undo) ---

    // crash_clone (not close(), which now truncates the logs on any clean
    // shutdown): the redo/undo logs still hold every record from these
    // commits, so reopening genuinely exercises replay (process_redo
    // re-applying committed Add ops via insert_if_needed) rather than
    // replaying against already-truncated, empty logs.
    #[test]
    fn test_replay_redoes_committed_writes_on_reopen() {
        let (db, tid) = make_db_with_table();
        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs(&db, 20);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after replay"))
                    .data
                    .to_vec(),
                format!("v{i}").as_bytes()
            );
        }
    }

    // A transaction that never commits and is never dropped normally
    // (mem::forget, so Transaction::drop's implicit rollback — and
    // therefore close()'s own drain_aborting — never runs) can still have
    // durably flushed its write to the main file: PageBuffer's flush
    // timing is independent of transaction commit/rollback status. On
    // reopen, process_undo must revert it: its undo log has entries but no
    // Commit record, so it's excluded from process_redo's replay and
    // explicitly reverted via revert_undo_ops (the same remove_if_txn/
    // update_if_txn primitives normal rollback uses).
    #[test]
    fn test_replay_undoes_uncommitted_abandoned_writes_on_reopen() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"committed"), &t).unwrap();
        db.commit(t).unwrap();

        let uncommitted = db.begin().unwrap();
        db.insert(tid, row(2, b"uncommitted"), &uncommitted)
            .unwrap();
        std::mem::forget(uncommitted);

        wait_for_durable_logs(&db, 3);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"committed"
        );
        assert!(
            db2.find(tid, id(2), &t).unwrap().is_none(),
            "an uncommitted, abandoned transaction's write must be reverted by undo replay"
        );
    }

    // Exercises Mod and Del redo/undo, not just Add, and mixes committed
    // and abandoned transactions in the same table.
    #[test]
    fn test_replay_handles_mixed_add_mod_del_across_committed_and_abandoned_txns() {
        let (db, tid) = make_db_with_table();

        // Row 1: inserted, then updated — both committed.
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, b"v1-updated"), &t).unwrap();
        db.commit(t).unwrap();

        // Row 2: inserted, then removed — both committed.
        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"v2"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(2), &t).unwrap();
        db.commit(t).unwrap();

        // Row 3: inserted and committed, then updated by an abandoned
        // (never committed) transaction — the update must not stick.
        let t = db.begin().unwrap();
        db.insert(tid, row(3, b"v3"), &t).unwrap();
        db.commit(t).unwrap();
        let abandoned = db.begin().unwrap();
        db.update(tid, row(3, b"v3-should-not-stick"), &abandoned)
            .unwrap();
        std::mem::forget(abandoned);

        wait_for_durable_logs(&db, 11);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"v1-updated"
        );
        assert!(
            db2.find(tid, id(2), &t).unwrap().is_none(),
            "row 2's committed remove must survive replay"
        );
        assert_eq!(
            db2.find(tid, id(3), &t).unwrap().unwrap().data.to_vec(),
            b"v3",
            "row 3's abandoned update must be reverted, restoring the pre-image"
        );
    }

    // Replay must be safe to run more than once in a row: reopening a
    // second time (no new writes in between) re-scans logs that already
    // reflect reality and must not corrupt or duplicate anything.
    #[test]
    fn test_replay_is_idempotent_across_repeated_reopens() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        // First "crash": replay runs once against the original records.
        wait_for_durable_logs(&db, 10);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        // A second "crash", of db2, with nothing new having happened on
        // it: process_redo/process_undo call the BPlusTree-level methods
        // directly, never logger.log_redo/log_undo, so db2's redo/undo
        // files still hold the exact same records from the original
        // crash. Replaying them a second time (via db3) must be just as
        // safe as the first. db2's own header still needs syncing before
        // ITS crash_clone — its page_count may have moved (replay itself
        // can allocate pages) since db2 opened.
        wait_for_durable_logs(&db2, 10);
        sync_header_without_truncating_logs(&db2);
        let (f2, l2) = crash_clone(&db2);
        let db3 = TestDB::open_using("txn_test.db", f2, l2).unwrap();

        let t = db3.begin().unwrap();
        for i in 0..5u64 {
            assert_eq!(
                db3.find(tid, id(i), &t).unwrap().unwrap().data.to_vec(),
                format!("v{i}").as_bytes()
            );
        }
    }

    // The actual point of redo replay: reconstruct a write whose page
    // never made it to the main file before a crash, using only the
    // (already-durable) redo log. Constructed deterministically instead of
    // racing real background flush timing: snapshot the main file's bytes
    // right after a checkpoint (a known-consistent, fully-flushed state),
    // then perform one more committed insert and take the CURRENT redo/
    // undo logs (which, after the checkpoint's truncate, contain only that
    // insert's own records) — but pair them with the OLD, pre-insert main
    // file bytes instead of the real (already-flushed, in this test)
    // current ones. This reconstructs exactly what a crash between "redo
    // record durably logged" and "page flushed to the main file" would
    // leave behind, without needing to catch that race in real time.
    #[test]
    fn test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file() {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"pre-checkpoint"), &t).unwrap();
        db.commit(t).unwrap();
        db.checkpoint().unwrap();
        // checkpoint()'s own header write (PageBuffer::write_header) is
        // fire-and-forget, same as the log truncate — it enqueues a
        // BufMsg::WriteHeader and returns without waiting for the writer
        // thread to actually apply it. A second checkpoint's OWN
        // synchronous BufMsg::Checkpoint reply is queued strictly after
        // that WriteHeader message (same channel, FIFO), so waiting for
        // *this* checkpoint to return guarantees the first one's header
        // write already landed — otherwise this snapshot could race it and
        // capture a header whose page_count is still 0, which later fails
        // to even load system tables on reopen.
        db.checkpoint().unwrap();

        // The last known-durable state before the "crash".
        let stale_main_file_bytes = db.file.data();

        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"lost-on-crash"), &t).unwrap();
        db.commit(t).unwrap();

        // Simulate a crash right here: a live clone of the (unclosed,
        // untruncated) log — close() would now truncate it, since a clean
        // close leaves nothing that needs replaying. The bounded log
        // channel makes log() block until the runner thread has actually
        // received each record, so this clone reliably contains row 2's
        // Add+Commit (logged after the checkpoint's truncate, so they're
        // the only two records in it).
        wait_for_durable_logs(&db, 2);
        let (_, log_file) = crash_clone(&db);

        // Rebuild a "crashed" main file from the pre-insert snapshot —
        // independent bytes, not sharing the live file's buffer.
        let crashed_file = MemFile::new();
        crashed_file.pwrite(&stale_main_file_bytes, 0).unwrap();

        let db2 = TestDB::open_using("txn_test.db", crashed_file, log_file).unwrap();

        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"pre-checkpoint",
            "sanity: the pre-crash checkpointed row must still be there"
        );
        assert_eq!(
            db2.find(tid, id(2), &t)
                .unwrap()
                .unwrap_or_else(|| panic!(
                    "row 2 missing — redo replay failed to reconstruct a committed \
                     write whose page flush never reached the main file"
                ))
                .data
                .to_vec(),
            b"lost-on-crash"
        );
    }

    // --- T4_S2_WAL_DESIGN.md: LogHeader mismatch / torn-tail / corruption,
    // exercised end-to-end through Db::open_using (not just scan_log/
    // read_and_validate_log_header in isolation — logger.rs's own test
    // module already covers those directly). Uses the same raw byte-
    // surgery-on-a-MemFile pattern
    // test_replay_recovers_a_write_whose_page_flush_never_reached_the_main_file
    // above already relies on, rather than a separate fault-injecting
    // DBFile wrapper: every scenario here only needs one specific, known
    // corruption applied once to a snapshot taken via crash_clone, which
    // plain byte manipulation on MemFile's own buffer already does
    // directly and exactly — a generic wrapper would mean plumbing fault
    // injection through Db::create_core_db/open_using's generic `F: DBFile`
    // open path for no additional coverage here.

    // A log file created for one database's page_size, paired with a
    // DIFFERENT database's main file, must be refused before recovery
    // ever runs against it — not decoded, not silently tolerated.
    #[test]
    fn test_open_using_refuses_a_log_file_from_a_different_page_size_database() {
        let db_4k = TestDB::create_with_page_size("mismatch_4k.db", 4096).unwrap();
        let db_8k = TestDB::create_with_page_size("mismatch_8k.db", 8192).unwrap();

        let (_, log_bytes_from_8k) = current_segment(&db_8k);
        let (main_file_4k, _) = crash_clone(&db_4k);
        // The 8k database's segment, presented under the 4k database's name.
        let log_file_from_8k = MemFile::new();
        log_file_from_8k.add_sibling_from_bytes("mismatch_4k.db.wal.1", log_bytes_from_8k);

        let err = match TestDB::open_using("mismatch_4k.db", main_file_4k, log_file_from_8k) {
            Err(e) => e,
            Ok(_) => panic!("expected LogHeaderMismatch, got Ok"),
        };
        assert!(
            matches!(err, StoreError::LogHeaderMismatch(_)),
            "expected LogHeaderMismatch, got {err:?}"
        );
    }

    // A torn tail (crash mid write_all, leaving a partial final record) must
    // not fail Db::open at all — recovery drops the incomplete record and
    // opens normally with everything before it intact.
    #[test]
    fn test_open_using_tolerates_a_torn_tail_and_recovers_everything_before_it() {
        let (db, tid) = make_db_with_table();
        // Transaction 1: fully committed, Add+Commit both intact — must
        // survive. Transaction 2: its own Commit marker (the log's very
        // last record) is what gets torn below.
        let t1 = db.begin().unwrap();
        db.insert(tid, row(1, b"fully-committed"), &t1).unwrap();
        db.commit(t1).unwrap();
        let t2 = db.begin().unwrap();
        db.insert(tid, row(2, b"torn-away"), &t2).unwrap();
        db.commit(t2).unwrap();
        wait_for_durable_logs(&db, 4);
        sync_header_without_truncating_logs(&db);

        let (main_file, _) = crash_clone(&db);
        let (segment_path, mut torn_bytes) = current_segment(&db);
        // Truncate off the last few bytes — landing mid-payload of the
        // last complete record (transaction 2's Commit marker), simulating
        // a crash partway through its write_all.
        torn_bytes.truncate(torn_bytes.len() - 3);
        let torn_log_file = MemFile::new();
        torn_log_file.add_sibling_from_bytes(&segment_path, torn_bytes);

        let db2 = TestDB::open_using("txn_test.db", main_file, torn_log_file)
            .expect("a torn tail must not fail Db::open");
        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"fully-committed",
            "everything before the torn record must still be recovered"
        );
        assert!(
            db2.find(tid, id(2), &t).unwrap().is_none(),
            "transaction 2's own Commit marker was torn off, so it must NOT appear \
             committed — recovery correctly treats it the same as a crash before commit"
        );
    }

    // Real, mid-file corruption — a checksum mismatch with more valid,
    // complete records after it — must refuse to open rather than silently
    // treat it as a torn tail (which would incorrectly stop recovery early,
    // discarding records that actually did land durably).
    #[test]
    fn test_open_using_refuses_a_log_file_with_mid_file_corruption() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"v2"), &t).unwrap();
        db.commit(t).unwrap();
        wait_for_durable_logs(&db, 4);
        sync_header_without_truncating_logs(&db);

        let (main_file, _) = crash_clone(&db);
        let (segment_path, mut corrupted) = current_segment(&db);
        // Flip a byte inside the FIRST record's payload (right after the
        // header + one frame's worth of length/checksum prefix) — leaves
        // the rest of the file (including the second record) intact and
        // valid, so this can't be mistaken for a torn tail.
        let header_len = LogHeader::encoded_len();
        let flip_at = header_len + 9; // a few bytes into record 1's payload
        corrupted[flip_at] ^= 0xFF;
        let corrupted_log_file = MemFile::new();
        corrupted_log_file.add_sibling_from_bytes(&segment_path, corrupted);

        let err = match TestDB::open_using("txn_test.db", main_file, corrupted_log_file) {
            Err(e) => e,
            Ok(_) => panic!("expected LogCorruption, got Ok"),
        };
        assert!(
            matches!(err, StoreError::LogCorruption(_)),
            "expected LogCorruption, got {err:?}"
        );
    }

    // --- log-based crash recovery replay, real File backend ---
    //
    // Everything above exercises MemFile's `load_logs` path (the buffer's
    // `.data()` branch). `File`-backed logs go through a different branch
    // (mmap over a real fd — see `load_logs`'s `as_any().downcast_ref::<File>`
    // arm), previously covered by logger.rs's own
    // test_load_logs_*_file_mmap/test_load_logs_accumulates_across_multiple_
    // sessions tests directly against a bare `Logger`. Those were removed
    // once `load_logs` moved from `Logger` to `Db` (a bare `Logger` can no
    // longer apply redo/undo without table access) — ported here through the
    // `Db`-level API so the mmap branch itself stays covered.

    fn temp_db_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "squeal_db_replay_test_{tag}_{}",
                std::process::id()
            ))
            .to_string_lossy()
            .into_owned()
    }

    // Same idea as crash_clone, but for the real File backend: File::
    // try_clone (Opener::do_clone's impl) dups the fd, giving a second
    // handle onto the same underlying open file — writes through either are
    // visible via the other, just like MemFile's Arc-shared buffer.
    fn crash_clone_file(db: &FileDB) -> (File, File) {
        (db.file.do_clone().unwrap(), db.log_file.do_clone().unwrap())
    }

    // Passive record count straight off disk, by path rather than through a
    // shared fd — sidesteps any concern about interfering with the writer
    // thread's own seek position (see Opener::pread's doc comment on why
    // clones of the same fd share a cursor). Skips the LogHeader, then
    // walks framed records the same way count_log_records (MemFile
    // version) does.
    // Records across every on-disk segment of a file-backed database.
    fn count_log_records_on_disk(db_name: &str) -> usize {
        crate::memfile::list_files_with_prefix(&format!("{db_name}.wal."))
            .unwrap_or_default()
            .iter()
            .map(|p| count_records_in_segment(&std::fs::read(p).unwrap_or_default()))
            .sum()
    }

    // See wait_for_durable_logs (MemFile version) — same log()
    // send()-doesn't-imply-written race applies to the File backend too.
    fn wait_for_durable_logs_file(db_name: &str, expected: usize) {
        for _ in 0..1000 {
            if count_log_records_on_disk(db_name) == expected {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out waiting for {expected} file-backed log record(s) to land");
    }

    // mmap-ing a zero-length file is a classic edge case (some mmap
    // implementations error on it), and it's the state of every brand-new
    // database's log files before a single record has ever been written —
    // not a corner case, the common one.
    #[test]
    fn test_replay_handles_empty_file_backed_logs_without_panicking() {
        let db_name = temp_db_path("empty");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        db.create_table("rows".to_string()).unwrap();

        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, l).unwrap();
        assert_eq!(db2.get_tables().unwrap().len(), 1);

        // STORE_AUDIT.md S5: delete() now refuses to remove a still-locked
        // database — both handles must actually be gone first, or this
        // cleanup silently no-ops.
        drop(db2);
        drop(db);
        FileDB::delete(&db_name).unwrap_or_default();
    }

    #[test]
    fn test_replay_recovers_committed_writes_on_file_backed_db() {
        let db_name = temp_db_path("recover");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs_file(&db_name, 20);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, l).unwrap();

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after file-backed replay"))
                    .data
                    .to_vec(),
                format!("v{i}").as_bytes()
            );
        }
        drop(t);

        // STORE_AUDIT.md S5: delete() now refuses to remove a still-locked
        // database — both handles must actually be gone first, or this
        // cleanup silently no-ops.
        drop(db2);
        drop(db);
        FileDB::delete(&db_name).unwrap_or_default();
    }

    // A restarted process reopens the same on-disk files rather than
    // truncating them — replay must see everything from every prior
    // session, not just the most recent one (mirrors the removed
    // test_load_logs_accumulates_across_multiple_sessions, through the Db
    // API instead of a bare Logger).
    #[test]
    fn test_replay_is_idempotent_across_repeated_reopens_file_backed() {
        let db_name = temp_db_path("idempotent");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }

        wait_for_durable_logs_file(&db_name, 10);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone_file(&db);
        let db2 = FileDB::open_using(&db_name, f, l).unwrap();

        wait_for_durable_logs_file(&db_name, 10);
        sync_header_without_truncating_logs(&db2);
        let (f2, l2) = crash_clone_file(&db2);
        let db3 = FileDB::open_using(&db_name, f2, l2).unwrap();

        let t = db3.begin().unwrap();
        for i in 0..5u64 {
            assert_eq!(
                db3.find(tid, id(i), &t).unwrap().unwrap().data.to_vec(),
                format!("v{i}").as_bytes()
            );
        }
        drop(t);

        // STORE_AUDIT.md S5: delete() now refuses to remove a still-locked
        // database — all three handles must actually be gone first, or
        // this cleanup silently no-ops.
        drop(db3);
        drop(db2);
        drop(db);
        FileDB::delete(&db_name).unwrap_or_default();
    }

    // --- LSN watermark continuity across reopen ---
    //
    // process_redo now tracks the highest lsn_id seen while scanning the
    // redo log and calls `self.logger.clock().mark_written(lsn_id)` after
    // replay — intent: a freshly reopened Db's LsnClock (which otherwise
    // starts cold, watermark = u64::MAX) picks up where the prior session's
    // durable history left off, instead of forgetting it ever happened.

    fn wait_for_watermark_at_least(db: &TestDB, min: u64) {
        for _ in 0..1000 {
            if db.logger.clock().last_written().0 >= min {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out waiting for the LSN watermark to reach at least {min}");
    }

    #[test]
    fn test_replay_seeds_lsn_watermark_from_prior_session() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        // 5 Add + 5 Commit redo records, each with its own increasing lsn.
        wait_for_durable_logs(&db, 10);
        let watermark_before_crash = db.logger.clock().last_written();
        assert_ne!(
            watermark_before_crash.0,
            u64::MAX,
            "sanity: the live session's own watermark must already be a real value, \
             not the cold-start sentinel"
        );

        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        assert_eq!(
            db2.logger.clock().last_written(),
            watermark_before_crash,
            "replay must seed the reopened db's watermark from the highest lsn in the \
             prior session's redo log, not leave it at the cold-start sentinel"
        );
    }

    // The deeper claim in "so it doesn't regress": once seeded by replay,
    // the watermark must never fall below where replay left it, even as
    // brand-new writes land in the new session. If the new session's lsn
    // *counter* isn't also resumed past the old session's highest value (a
    // freshly reset counter mints 0, 1, 2, ... again), the very first new
    // write's redo record durably landing calls mark_written with that low,
    // reused lsn — regressing the watermark right back down, undoing what
    // replay just established.
    #[test]
    fn test_lsn_watermark_does_not_regress_after_new_writes_post_reopen() {
        let (db, tid) = make_db_with_table();
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        wait_for_durable_logs(&db, 10);
        sync_header_without_truncating_logs(&db);
        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let watermark_after_replay = db2.logger.clock().last_written();

        let t = db2.begin().unwrap();
        db2.insert(tid, row(999, b"new-after-reopen"), &t).unwrap();
        db2.commit(t).unwrap();
        wait_for_watermark_at_least(&db2, watermark_after_replay.0);

        assert!(
            db2.logger.clock().last_written().0 >= watermark_after_replay.0,
            "a new write's own redo record landing regressed the watermark from {} down to {} — \
             the lsn counter must resume past the prior session's highest lsn, not just the \
             watermark",
            watermark_after_replay.0,
            db2.logger.clock().last_written().0
        );
    }

    #[test]
    fn test_checkpoint_then_close_then_reopen_preserves_data() {
        let (db, tid) = make_db_with_table();
        for i in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, format!("row-{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        db.checkpoint().unwrap();
        let page_count_at_checkpoint = db.page_count();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        assert_eq!(db2.page_count(), page_count_at_checkpoint);

        let t = db2.begin().unwrap();
        for i in 0..10u64 {
            assert_eq!(
                db2.find(tid, id(i), &t)
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {i} missing after checkpoint+close+reopen"))
                    .data
                    .to_vec(),
                format!("row-{i}").as_bytes()
            );
        }
        drop(t);
    }

    #[test]
    fn test_checkpoint_truncates_the_log_file_down_to_just_its_header() {
        let (db, tid) = make_db_with_table();
        for i in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }

        db.checkpoint().unwrap();
        // close() calls logger.shutdown(), which sends ShutDown on the same
        // (FIFO) channel checkpoint()'s truncate message went to, then
        // blocks on the runner thread joining — so by the time close()
        // returns, the truncate is guaranteed to have actually run.
        // checkpoint() returning on its own only guarantees the truncate
        // was *requested* (Logger::checkpoint is fire-and-forget), not
        // that it's completed yet.
        let (_, log_file) = db.close().unwrap();

        // T4_S2_WAL_DESIGN.md §2: truncate() sets the file to zero bytes,
        // but log_runner immediately restores the LogHeader afterward — so
        // the truncated length is the header's own size, not literal zero.
        assert_eq!(
            log_file.get_metadata().unwrap().len,
            LogHeader::encoded_len() as u64,
            "the log file must be truncated down to just its header after a checkpoint"
        );
    }

    // The whole point of truncating on checkpoint is to keep the log
    // bounded over a long-lived database's life, not just to be empty
    // once. Runs several commit+checkpoint rounds and checks the log
    // never grows past what a SINGLE round would produce — if truncation
    // silently stopped working (or only worked the first time), this
    // would catch the log growing round over round instead.
    #[test]
    fn test_checkpoint_keeps_log_bounded_across_many_rounds() {
        let (db, tid) = make_db_with_table();

        // Phase 6: a checkpoint rolls to a fresh segment and, with nothing
        // in flight, deletes every older one — synchronously, so no polling.
        let header_len = LogHeader::encoded_len();
        for round in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(round, b"v"), &t).unwrap();
            db.commit(t).unwrap();
            db.checkpoint().unwrap();

            let segments = wal_segments_of(&db.log_file);
            assert_eq!(
                segments.len(),
                1,
                "round {round}: nothing in flight, so only the fresh segment may remain: {:?}",
                segments
                    .iter()
                    .map(|(p, b)| (p.clone(), b.len()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                segments[0].1.len(),
                header_len,
                "round {round}: the fresh segment holds just its header — the log must not \
                 accumulate round over round"
            );
            assert_eq!(db.stats().wal_segments, 1);
        }
    }

    #[test]
    fn test_checkpoint_concurrent_with_active_writers() {
        use std::sync::Arc;

        const THREADS: u64 = 8;
        const ROWS_PER_THREAD: u64 = 50;

        // Each thread gets its own table (disjoint B+tree, no shared
        // index/data pages) so the only cross-thread interaction is via the
        // shared PageBuffer/writer thread that checkpoint() also touches —
        // this isolates checkpoint's own thread-safety from same-table insert
        // contention, which has its own pre-existing, unrelated correctness
        // issues under heavy concurrency (not a checkpoint concern).
        let db = TestDB::create("checkpoint_concurrent_test.db").unwrap();
        let tids: Vec<TableIdType> = (0..THREADS)
            .map(|i| db.create_table(format!("t{i}")).unwrap())
            .collect();

        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            let tid = tids[thread_idx as usize];
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    let t = db.begin().unwrap();
                    db.insert(tid, row(i, format!("v{thread_idx}-{i}").as_bytes()), &t)
                        .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }

        // Checkpoint repeatedly while writers are still active, interleaved
        // with their inserts rather than after — this is the scenario the
        // Checkpoint message type and its reply channel exist to handle
        // safely.
        let checkpoint_db = Arc::clone(&db);
        let checkpoint_handle = thread::spawn(move || {
            for _ in 0..10 {
                checkpoint_db.checkpoint().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        });

        for h in handles {
            h.join().unwrap();
        }
        checkpoint_handle.join().unwrap();

        // A final checkpoint after all writers finished, then verify every
        // row committed by every thread is visible and correct.
        db.checkpoint().unwrap();
        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            let tid = tids[thread_idx as usize];
            for i in 0..ROWS_PER_THREAD {
                let found = db.find(tid, id(i), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!(
                            "table {thread_idx} row {i} missing after concurrent checkpoint"
                        ))
                        .data
                        .to_vec(),
                    format!("v{thread_idx}-{i}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // ── free-page persistence ────────────────────────────────────────────
    // Freed pages (e.g. overflow continuations reclaimed when a large object
    // shrinks) are tracked in-memory by PageBuffer::free_pages and, as of this
    // change, also serialized into the reserved FREE_PAGE_TABLE_PAGE (page 2)
    // by write_system_tables() and restored by load_system_tables(). Before
    // this, every close/reopen forgot any freed pages: they became permanent,
    // unreachable holes (page_count only ever grows, so a forgotten free page
    // could never be reused). These tests check both that the recorded set
    // round-trips exactly, and — the part that actually matters — that a page
    // reused after reopen is safe to write fresh data into.

    use crate::page::PageId;
    use std::collections::HashSet;

    #[test]
    fn test_free_pages_empty_by_default_persists_as_empty() {
        let (db, _tid) = make_db_with_table();
        assert!(db.buffer.get_free_pages().is_empty());
        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        assert!(
            db2.buffer.get_free_pages().is_empty(),
            "a DB that never freed anything must not spuriously report free pages"
        );
    }

    #[test]
    fn test_freed_overflow_pages_persist_across_close_reopen() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(1, 8 * page);
        let shrunk = big_payload(2, 4 * page); // frees several overflow pages

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let freed_before = db.buffer.get_free_pages();
        assert!(
            !freed_before.is_empty(),
            "shrinking an 8-page object to 4 pages must free some overflow pages"
        );

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let freed_after = db2.buffer.get_free_pages();

        assert_eq!(
            freed_after, freed_before,
            "the exact set of freed pages must survive close/reopen"
        );
    }

    #[test]
    fn test_reopened_db_reuses_freed_pages_before_growing_page_count() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(3, 8 * page);
        let shrunk = big_payload(4, 4 * page);

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let free_count = db2.buffer.get_free_pages().len();
        assert!(free_count > 0);
        let page_count_at_reopen = db2.page_count();

        // Draining exactly the free list must satisfy every allocation from
        // it (alloc_page always checks free_pages before growing page_count),
        // so page_count must not move at all during this loop.
        for _ in 0..free_count {
            db2.buffer.alloc_page(false).unwrap();
            assert_eq!(
                db2.page_count(),
                page_count_at_reopen,
                "reusing a freed page must not grow page_count"
            );
        }
        assert!(db2.buffer.get_free_pages().is_empty());

        // The free list is now exhausted; the next allocation must fall back
        // to growing page_count, proving the earlier ones really did come
        // from reuse and not from some other accounting quirk.
        db2.buffer.alloc_page(false).unwrap();
        assert_eq!(db2.page_count(), page_count_at_reopen + 1);
    }

    #[test]
    // Regression test for a real bug this test originally caught: freeing an
    // overflow continuation page used to only clear its header FLAGS
    // (free_overflow_pages), never page_used_size, next_page, or the data
    // region — so a page freed while it held a chunk of an overflow object
    // came back from alloc_page()'s free-list reuse still carrying that old
    // page_used_size (observed: 131090, i.e. roughly the original 8-page
    // object's total size, on a page whose own page_data_size is 16304),
    // making a perfectly ordinary Page::add_tuple on it fail with
    // PageCapacityError (can_store() saw it as already full). Fixed by
    // PageBuffer::reset_freed_page, which writes a genuinely fresh, empty
    // Page through the normal write path before the page goes on the free
    // list — the same thing init_page does for a brand-new page.
    fn test_reused_freed_overflow_page_is_safe_to_write_fresh_data_into() {
        let (db, tid) = make_db_with_table();
        let page_sz = DEFAULT_PAGE_SIZE as usize;
        let grown = big_payload(5, 8 * page_sz);
        let shrunk = big_payload(6, 4 * page_sz);

        let t = db.begin().unwrap();
        db.insert(tid, row(1, &grown), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &shrunk), &t).unwrap();
        db.commit(t).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();

        let reused_id = db2.buffer.alloc_page(false).unwrap();
        assert!(
            !db2.buffer.get_free_pages().contains(&reused_id),
            "sanity: alloc_page must not return an id still in the free list"
        );

        // Mutate-and-write through the exact same handle pattern
        // BPlusTree::write_page (the real production path for adding a tuple
        // to a newly allocated data-chain continuation) uses: get_page_mut,
        // Arc::make_mut, mutate, write_locked_page. This exercises reuse
        // exactly as it happens in practice, rather than bypassing whatever
        // get_page_mut actually returns for a page whose on-disk bytes are a
        // raw overflow-chunk slice, not a standalone serialized Page.
        let mut handle = db2
            .buffer
            .get_page_mut(reused_id, crate::buffer::LockLevel::Data)
            .unwrap();
        Arc::make_mut(&mut handle.page)
            .add_tuple(Tuple::new(42, b"fresh-after-reuse"))
            .unwrap();
        db2.buffer.write_locked_page(handle).unwrap();

        let readback = db2.buffer.get_page(reused_id).unwrap();
        assert_eq!(readback.count().unwrap(), 1);
        assert_eq!(
            readback
                .get(DBIdType::Int(42))
                .unwrap()
                .expect("the freshly written tuple must be readable")
                .data
                .to_vec(),
            b"fresh-after-reuse"
        );

        // The original (shrunk) large object must still read back correctly
        // too — reuse of an unrelated freed page must not disturb it.
        let t2 = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t2)
                .unwrap()
                .expect("shrunk object must still be intact")
                .data
                .to_vec(),
            shrunk
        );
        drop(t2);
    }

    #[test]
    fn test_free_pages_do_not_accumulate_across_multiple_close_reopen_cycles() {
        let (db, tid) = make_db_with_table();
        let page = DEFAULT_PAGE_SIZE as usize;

        // Round 1: free some pages, close, reopen.
        let t = db.begin().unwrap();
        db.insert(tid, row(1, &big_payload(7, 8 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(8, 4 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let (f, l) = db.close().unwrap();
        let db = TestDB::open_using("txn_test.db", f, l).unwrap();
        let round1_free: HashSet<PageId> = db.buffer.get_free_pages().into_iter().collect();
        assert!(!round1_free.is_empty());

        // Round 2: grow the SAME object back up (consuming free pages, and
        // possibly needing fresh ones too), then shrink again, then close.
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(9, 8 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(1, &big_payload(10, 4 * page)), &t)
            .unwrap();
        db.commit(t).unwrap();
        let round2_free_before_close: HashSet<PageId> =
            db.buffer.get_free_pages().into_iter().collect();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let round2_free_after_reopen: HashSet<PageId> =
            db2.buffer.get_free_pages().into_iter().collect();

        assert_eq!(
            round2_free_after_reopen, round2_free_before_close,
            "the persisted set must reflect only the latest close, not an \
             accumulation of every round's freed pages"
        );
    }

    // Regression test for a real bug found while investigating todo.txt item
    // [7] (spurious DuplicateKey under concurrent inserts into one shared
    // table). Confirmed root cause via targeted instrumentation: insert_index
    // failing with LockContentionError is normal under contention, but
    // BPlusTree::insert's cleanup of the just-written data-page row (undoing
    // write_data's write before returning the error) used a bare `?` on its
    // own get_page_mut/write_locked_page calls — so if *that* itself hit
    // LockContentionError, the cleanup was abandoned and the row was left
    // permanently orphaned (written, but never indexed, and invisible to
    // find() since nothing points to it). write_data's page selection is
    // deterministic (always starts from first_data_page), so a later retry
    // of the same key reliably lands on that same page and hits a real, but
    // bogus, DuplicateKey — permanently, not just transiently, since the
    // orphaned row is never cleaned up by anything. Fixed by wrapping the
    // cleanup in retry_on_contention so its own contention can't abort it.
    // This test reliably reproduced the bug before the fix (roughly 1 in 3
    // runs with 8 threads x 50 rows into a single shared table).
    #[test]
    fn test_concurrent_inserts_into_shared_table_do_not_orphan_rows() {
        const THREADS: u64 = 8;
        const ROWS_PER_THREAD: u64 = 50;
        let (db, tid) = make_db_with_table();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    let key = thread_idx * ROWS_PER_THREAD + i;
                    let t = db.begin().unwrap();
                    db.insert(tid, row(key, format!("v{key}").as_bytes()), &t)
                        .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            for i in 0..ROWS_PER_THREAD {
                let key = thread_idx * ROWS_PER_THREAD + i;
                let found = db.find(tid, id(key), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!("row {key} missing"))
                        .data
                        .to_vec(),
                    format!("v{key}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 0, found by the crash harness's scan
    // check: BPlusTree::write_data released the tail page's lock between
    // "no successor" and "link my new page", so two writers extending the
    // data chain at once could both link a new page to the same tail; the
    // second link overwrote the first, orphaning a page that already held
    // rows and that the index already pointed at. find() saw those rows;
    // table_scan never did. Small page + wide rows + many threads makes
    // tail extension frequent enough to hit the race reliably.
    #[test]
    fn test_concurrent_inserts_extending_the_data_chain_are_all_reachable_by_scan() {
        const THREADS: u64 = 8;
        const ROWS_PER_THREAD: u64 = 200;
        let db = TestDB::create_with_page_size("chain_race.db", 4096).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    let key = thread_idx * ROWS_PER_THREAD + i;
                    let t = db.begin().unwrap();
                    db.insert(tid, row(key, &[7u8; 200]), &t).unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = db.table_scan(tid).unwrap();
        while let Some(t) = cursor.next().unwrap() {
            let DBIdType::Int(k) = t.id else { panic!() };
            assert!(seen.insert(k), "key {k} scanned twice");
        }
        assert_eq!(
            seen.len() as u64,
            THREADS * ROWS_PER_THREAD,
            "scan reached {} of {} committed rows — a data page fell off the chain",
            seen.len(),
            THREADS * ROWS_PER_THREAD
        );
    }

    // A MemFile whose fsync on the WAL can be held open by a test — the
    // fault-injection seam STORE_AUDIT.md's Phase 0 asked for, in its
    // smallest useful form. Opened by name like NamedMemFile would be, but
    // every open shares one process-wide gate (Opener::open is static), so
    // tests using it must not run concurrently with each other: they take
    // GATED_SYNC_TEST_LOCK.
    #[derive(Debug, Clone)]
    struct GatedSyncFile {
        inner: MemFile,
        is_wal: bool,
    }

    static WAL_SYNC_BLOCKED: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    static GATED_SYNC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl Opener for GatedSyncFile {
        type Item = GatedSyncFile;
        fn open<P: AsRef<std::path::Path>>(
            op: std::fs::OpenOptions,
            p: P,
        ) -> std::io::Result<Self> {
            Ok(Self {
                inner: MemFile::open(op, &p)?,
                is_wal: p.as_ref().to_string_lossy().contains(".wal"),
            })
        }
        fn open_sibling(&self, path: &str, op: std::fs::OpenOptions) -> std::io::Result<Self> {
            Ok(Self {
                inner: self.inner.open_sibling(path, op)?,
                is_wal: path.contains(".wal"),
            })
        }
        fn list_siblings(&self, prefix: &str) -> std::io::Result<Vec<String>> {
            self.inner.list_siblings(prefix)
        }
        fn remove_sibling(&self, path: &str) -> std::io::Result<()> {
            self.inner.remove_sibling(path)
        }
        fn truncate(&mut self) -> std::io::Result<()> {
            self.inner.truncate()
        }
        fn do_sync(&mut self) -> std::io::Result<()> {
            if self.is_wal {
                while WAL_SYNC_BLOCKED.load(std::sync::atomic::Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            self.inner.do_sync()
        }
        fn do_clone(&self) -> std::io::Result<Self> {
            Ok(self.clone())
        }
        fn get_metadata(&self) -> std::io::Result<crate::db::Meta> {
            self.inner.get_metadata()
        }
        fn do_lock(&self) -> Result<(), std::fs::TryLockError> {
            Ok(())
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
            self.inner.pwrite(buf, offset)
        }
    }
    impl std::io::Write for GatedSyncFile {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }
    impl std::io::Read for GatedSyncFile {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(buf)
        }
    }
    impl std::io::Seek for GatedSyncFile {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    // Phase 1 found (via the crash harness) that a committing transaction
    // leaves the active set before its Commit record is durable. Phase 6's
    // fuzzy checkpoint never waits for transactions, so it must be safe in
    // exactly that state: it syncs the log first (step 1), and the data
    // file must not be checkpointed ahead of the record — so while the WAL
    // sync is held open, the checkpoint is held open with it, and the
    // committed row survives a crash taken after both complete.
    #[test]
    fn test_checkpoint_syncs_the_log_before_flushing_a_committing_transaction() {
        let _serial = GATED_SYNC_TEST_LOCK.lock().unwrap();
        WAL_SYNC_BLOCKED.store(false, std::sync::atomic::Ordering::Release);
        let db = Db::<GatedSyncFile>::create("gated_sync.db").unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        db.checkpoint().unwrap();

        WAL_SYNC_BLOCKED.store(true, std::sync::atomic::Ordering::Release);
        let committing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let committer = {
            let db = Arc::clone(&db);
            let committing = Arc::clone(&committing);
            thread::spawn(move || {
                let t = db.begin().unwrap();
                db.insert(tid, row(1, b"v"), &t).unwrap();
                committing.store(true, std::sync::atomic::Ordering::Release);
                db.commit(t).unwrap(); // blocks: WAL sync is held open
            })
        };
        // Wait until the transaction has written and then left the active
        // set — the state the old quiesce mistook for "nothing in flight".
        for _ in 0..5000 {
            if committing.load(std::sync::atomic::Ordering::Acquire)
                && db.stats().active_transactions == 0
            {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(committing.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            db.stats().active_transactions,
            0,
            "the committer must have flipped state: {:?}",
            db.stats()
        );
        assert!(
            !committer.is_finished(),
            "commit must still be waiting on the blocked sync"
        );

        let checkpointer = {
            let db = Arc::clone(&db);
            thread::spawn(move || db.checkpoint().unwrap())
        };
        thread::sleep(Duration::from_millis(150));
        assert!(
            !checkpointer.is_finished(),
            "checkpoint must not complete while the log it depends on cannot be synced"
        );

        WAL_SYNC_BLOCKED.store(false, std::sync::atomic::Ordering::Release);
        committer.join().unwrap();
        checkpointer.join().unwrap();
        // The checkpoint deleted every segment below its floor; the row is
        // on disk (flushed) or in the retained log — either way, present.
        if db.stats().wal_segments != 1 {
            for (path, bytes) in wal_segments_of(&db.log_file.inner) {
                eprintln!("segment {path}:");
                for line in crate::logger::describe_wal(&bytes) {
                    eprintln!("  {line}");
                }
            }
            eprintln!("stats: {:?}", db.stats());
        }
        assert_eq!(
            db.stats().wal_segments,
            1,
            "nothing was in flight at the floor"
        );
        let t = db.begin().unwrap();
        assert!(db.find(tid, id(1), &t).unwrap().is_some());
        db.rollback(t).unwrap();
        let (f, l) = db.close().unwrap();
        let db = Db::<GatedSyncFile>::open_using("gated_sync.db", f, l).unwrap();
        let t = db.begin().unwrap();
        assert!(
            db.find(tid, id(1), &t).unwrap().is_some(),
            "durable across reopen"
        );
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 4: an insert onto a committed, not-
    // yet-purged tombstone is a new version over it (no DuplicateKey from
    // the still-present index entry), committing and rolling back like any
    // other write — with vacuum held off so the tombstone is really there.
    #[test]
    fn test_insert_over_a_committed_tombstone_is_a_new_version() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        let t = db.begin().unwrap();
        db.insert(tid, row(3, b"first"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(3), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.stats().tombstones_awaiting_purge, 1);

        // Rolled back: the tombstone comes back, the key stays absent.
        let t = db.begin().unwrap();
        db.insert(tid, row(3, b"rolled back"), &t).unwrap();
        assert_eq!(
            db.find(tid, id(3), &t).unwrap().unwrap().data.to_vec(),
            b"rolled back"
        );
        db.rollback(t).unwrap();
        let t = db.begin().unwrap();
        assert!(db.find(tid, id(3), &t).unwrap().is_none());
        db.rollback(t).unwrap();

        // Committed: the key is back, with the new value, and an older
        // reader still sees it as deleted.
        let older = db.begin().unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(3, b"second"), &t).unwrap();
        db.commit(t).unwrap();
        assert!(
            db.find(tid, id(3), &older).unwrap().is_none(),
            "older reader: still deleted"
        );
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(3), &t).unwrap().unwrap().data.to_vec(),
            b"second"
        );
        // A second insert of a live key is a duplicate.
        assert!(matches!(
            db.insert(tid, row(3, b"third"), &t),
            Err(StoreError::DuplicateKey(_))
        ));
        db.rollback(older).unwrap();
        db.rollback(t).unwrap();
        // And once vacuum runs, nothing about the row's history is retained.
        db.maintenance.set_paused(false);
        for _ in 0..500 {
            if db.stats().version_records == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(db.stats().version_records, 0);
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(3), &t).unwrap().unwrap().data.to_vec(),
            b"second"
        );
    }

    // An insert over a tombstone that another transaction wrote but has not
    // committed is a write conflict (first-committer-wins), not a duplicate
    // and not a silent success.
    #[test]
    fn test_insert_over_an_uncommitted_tombstone_from_another_txn_conflicts() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(4, b"v0"), &t).unwrap();
        db.commit(t).unwrap();
        let remover = db.begin().unwrap();
        db.remove(tid, id(4), &remover).unwrap();
        let t = db.begin().unwrap();
        let r = db.insert(tid, row(4, b"v1"), &t);
        assert!(matches!(r, Err(StoreError::WriteConflict(_))), "got {r:?}");
        db.rollback(remover).unwrap();
        // The remover rolled back: the row is live again → duplicate.
        assert!(matches!(
            db.insert(tid, row(4, b"v1"), &t),
            Err(StoreError::DuplicateKey(_))
        ));
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 3: a dropped guard is fully aborted
    // inline — by the time `drop` returns, the write is physically reverted,
    // the versions are gone, and nothing is parked for later.
    #[test]
    fn test_a_dropped_guard_is_fully_reverted_before_drop_returns() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true); // nothing else may clean up for us
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.commit(t).unwrap();
        {
            let t = db.begin().unwrap();
            db.update(tid, row(1, b"dropped"), &t).unwrap();
            db.insert(tid, row(2, b"dropped"), &t).unwrap();
            assert_eq!(db.stats().version_records, 3);
            // dropped here
        }
        let s = db.stats();
        assert_eq!(s.aborting_transactions, 0, "nothing parked");
        assert_eq!(s.active_transactions, 0);
        assert_eq!(
            s.version_records, 1,
            "only the committed insert's record remains"
        );
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"v0"
        );
        assert!(db.find(tid, id(2), &t).unwrap().is_none());
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 3: the horizon rule. A reader that
    // began before a commit can still walk to the pre-image no matter how
    // many vacuum passes run, and once the reader is gone the records go.
    #[test]
    fn test_vacuum_never_reclaims_a_version_a_live_reader_can_reach() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.commit(t).unwrap();
        let reader = db.begin().unwrap();
        // Many commits after the reader began, each a new version of row 1.
        for i in 1..=20u64 {
            let t = db.begin().unwrap();
            db.update(tid, row(1, format!("v{i}").as_bytes()), &t)
                .unwrap();
            db.commit(t).unwrap();
        }
        // Give the maintenance thread every chance to be wrong.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0",
            "the reader's snapshot must survive 20 commits and any number of vacuum passes"
        );
        assert!(
            db.stats().version_records >= 20,
            "the chain is retained while the reader lives"
        );
        db.rollback(reader).unwrap();
        for _ in 0..500 {
            if db.stats().version_records == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        let s = db.stats();
        assert_eq!(
            s.version_records, 0,
            "nothing needs the chain once the reader is gone: {s:?}"
        );
        assert_eq!(s.committed_retained, 0);
        assert_eq!(s.committed_awaiting_vacuum, 0);
    }

    // After every transaction has ended, the maintenance thread brings every
    // queue to zero on its own: no foreground call is needed.
    #[test]
    fn test_stats_report_zero_pending_work_after_quiescence() {
        let (db, tid) = make_db_with_table();
        for k in 0..10u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(k, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }
        for k in 0..5u64 {
            let t = db.begin().unwrap();
            db.remove(tid, id(k), &t).unwrap();
            db.commit(t).unwrap();
        }
        {
            let t = db.begin().unwrap();
            db.update(tid, row(7, b"abandoned"), &t).unwrap();
        }
        for _ in 0..500 {
            let s = db.stats();
            if s.version_records == 0
                && s.tombstones_awaiting_purge == 0
                && s.committed_retained == 0
            {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        let s = db.stats();
        assert_eq!(s.active_transactions, 0);
        assert_eq!(s.aborting_transactions, 0);
        assert_eq!(s.version_records, 0, "{s:?}");
        assert_eq!(s.committed_retained, 0, "{s:?}");
        assert_eq!(s.committed_awaiting_vacuum, 0, "{s:?}");
        assert_eq!(s.tombstones_awaiting_purge, 0, "{s:?}");
        assert_eq!(s.tombstones_purged, 5);
        assert_eq!(s.maintenance_errors, 0, "{s:?}");
        // The purged keys are physically gone and reinsertable; the rest intact.
        let t = db.begin().unwrap();
        for k in 0..5u64 {
            assert!(db.find(tid, id(k), &t).unwrap().is_none());
        }
        for k in 5..10u64 {
            assert!(db.find(tid, id(k), &t).unwrap().is_some());
        }
        db.insert(tid, row(0, b"again"), &t).unwrap();
        db.commit(t).unwrap();
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 1: transaction ids and LSNs come from
    // one counter, seeded on reopen from max(header.counter, highest number
    // in the log) + 1. After a crash (no clean close, so the header's floor
    // is stale), every id and LSN issued by the new session must still be
    // above everything the old session issued that left a trace.
    #[test]
    fn test_reopen_after_crash_seeds_the_counter_above_every_id_and_lsn_in_the_log() {
        let (db, tid) = make_db_with_table();
        db.checkpoint().unwrap();
        let mut highest = 0u64;
        for i in 0..5u64 {
            let t = db.begin().unwrap();
            highest = highest.max(t.id().id_num());
            db.insert(tid, row(i, b"x"), &t).unwrap();
            db.commit(t).unwrap();
        }
        // The last commit's record is durable (commit waits), so its LSN is
        // in the log and is above every id issued.
        let (data, log) = db.synced_snapshot();
        drop(db);
        let db = TestDB::open_using("txn_test.db", data, log).unwrap();
        let t = db.begin().unwrap();
        assert!(
            t.id().id_num() > highest,
            "new id {} must exceed every pre-crash id (highest {highest})",
            t.id().id_num()
        );
        // And the same holds for the numbers a clean close persists.
        db.rollback(t).unwrap();
        let (f, l) = db.close().unwrap();
        let db = TestDB::open_using("txn_test.db", f, l).unwrap();
        let t = db.begin().unwrap();
        assert!(t.id().id_num() > highest);
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 1 (proposal §3.11): a named sequence
    // logs a high-water mark before handing out the first value of each
    // chunk, so a crash after committed writes that used sequence values
    // never hands those values out again — the SQL layer's row ids for
    // tables without a primary key depend on this. Before, sequences were
    // persisted only at checkpoint and a crash reissued already-used ids.
    #[test]
    fn test_sequence_values_are_never_reissued_after_a_crash() {
        let (db, tid) = make_db_with_table();
        db.get_generator()
            .create_generator("rowid", Some(0))
            .unwrap();
        db.checkpoint().unwrap();
        // Cross a chunk boundary (32) so more than one high-water record is
        // involved, and commit rows keyed by the values.
        let mut used = Vec::new();
        for _ in 0..40 {
            let k = db.get_generator().gen_key("rowid").unwrap();
            let t = db.begin().unwrap();
            db.insert(tid, row(k, b"r"), &t).unwrap();
            db.commit(t).unwrap();
            used.push(k);
        }
        let (data, log) = db.synced_snapshot();
        drop(db);
        let db = TestDB::open_using("txn_test.db", data, log).unwrap();
        let next = db.get_generator().gen_key("rowid").unwrap();
        assert!(
            next > *used.iter().max().unwrap(),
            "sequence handed out {next} again after a crash; {} was already committed",
            used.iter().max().unwrap()
        );
        // The rows keyed by the old values are all there, and inserting under
        // the new value works (no DuplicateKey).
        let t = db.begin().unwrap();
        for k in &used {
            assert!(db.find(tid, id(*k), &t).unwrap().is_some());
        }
        db.insert(tid, row(next, b"r"), &t).unwrap();
        db.commit(t).unwrap();
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 1, found by the crash harness: a
    // checkpoint syncs the data file, then asks the log runner to truncate,
    // and that truncation is its own later sync. A crash in between leaves
    // the log holding records from BEFORE the checkpoint. Replaying a Mod
    // for a key that a later, already-checkpointed delete reclaimed used to
    // fail recovery with KeyNotFound. Built deterministically: the log
    // snapshot is taken before the second checkpoint, the data snapshot
    // after it.
    #[test]
    fn test_recovery_tolerates_a_log_older_than_the_checkpoint() {
        let (db, tid) = make_db_with_table();
        // Vacuum held off so the purge lands exactly where this test wants it.
        db.maintenance.set_paused(true);
        let t = db.begin().unwrap();
        db.insert(tid, row(7, b"v0"), &t).unwrap();
        db.commit(t).unwrap();
        // Checkpoint #1 truncates the Add away.
        db.checkpoint().unwrap();
        let t = db.begin().unwrap();
        db.update(tid, row(7, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(7), &t).unwrap();
        db.commit(t).unwrap();
        // Wait for checkpoint #1's async truncate and both new records
        // (Mod, Commit, Del, Commit) to be the whole log, then keep that
        // pre-checkpoint log.
        wait_for_durable_logs(&db, 4);
        let (_, stale_log) = db.synced_snapshot();
        // Let vacuum purge the tombstone, then checkpoint #2: the data file
        // no longer has key 7 at all.
        db.maintenance.set_paused(false);
        for _ in 0..500 {
            if db.stats().tombstones_awaiting_purge == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(db.stats().tombstones_awaiting_purge, 0);
        db.checkpoint().unwrap();
        let (data, _) = db.synced_snapshot();
        drop(db);
        let db = TestDB::open_using("txn_test.db", data, stale_log)
            .expect("a log older than the checkpoint must replay as a no-op, not fail");
        let t = db.begin().unwrap();
        assert!(db.find(tid, id(7), &t).unwrap().is_none());
    }

    // A sequence created after the last checkpoint (e.g. by CREATE TABLE)
    // must exist after a crash, and one dropped after it must not.
    #[test]
    fn test_sequence_creation_and_removal_survive_a_crash() {
        let (db, _tid) = make_db_with_table();
        db.get_generator()
            .create_generator("doomed", Some(0))
            .unwrap();
        db.checkpoint().unwrap();
        db.get_generator()
            .create_generator("fresh", Some(100))
            .unwrap();
        db.get_generator().remove_generator("doomed").unwrap();
        // Force the sequence records to be durable: a commit waits on
        // everything logged before it.
        let t = db.begin().unwrap();
        db.commit(t).unwrap();
        let (data, log) = db.synced_snapshot();
        drop(db);
        let db = TestDB::open_using("txn_test.db", data, log).unwrap();
        assert_eq!(db.get_generator().gen_key("fresh").unwrap(), 100);
        assert!(matches!(
            db.get_generator().gen_key("doomed"),
            Err(StoreError::MissingKey(_))
        ));
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 0, found by the stress harness's
    // repeatable-read check: update()/remove() on a committed-but-unreclaimed
    // tombstone used to succeed, producing a NEW version that still carried
    // the tombstone flag — so the writer's own subsequent find() returned
    // None for a row it had just "updated".
    #[test]
    fn test_update_and_remove_on_a_committed_unreclaimed_tombstone_are_key_not_found() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(5, b"v0"), &t).unwrap();
        db.commit(t).unwrap();
        // A concurrent reader keeps the tombstone from being purged (its
        // commit_ts is above the reader's id, so the horizon holds it).
        let reader = db.begin().unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(5), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.stats().tombstones_awaiting_purge, 1);

        let t = db.begin().unwrap();
        assert!(db.find(tid, id(5), &t).unwrap().is_none());
        assert!(matches!(
            db.update(tid, row(5, b"v1"), &t),
            Err(StoreError::KeyNotFound(_))
        ));
        assert!(matches!(
            db.remove(tid, id(5), &t),
            Err(StoreError::KeyNotFound(_))
        ));
        // Still absent for this transaction after the failed writes.
        assert!(db.find(tid, id(5), &t).unwrap().is_none());
        db.rollback(t).unwrap();
        db.rollback(reader).unwrap();
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 0, found by the crash harness: a
    // committed delete's tombstone is physically reclaimed later, and that
    // reclaim is not logged. Checkpoint with the tombstone still present,
    // reclaim it, insert the same key again and commit, crash: replay found
    // the checkpoint's tombstone under the key and skipped the Add as
    // "already exists", so the committed reinsert read back as absent.
    #[test]
    fn test_replay_applies_a_committed_insert_over_a_checkpointed_tombstone() {
        let (db, tid) = make_db_with_table();
        // Hold vacuum off so the tombstone is still physically present when
        // the checkpoint runs (phase 3: purging is the maintenance thread's).
        db.maintenance.set_paused(true);
        let t = db.begin().unwrap();
        db.insert(tid, row(7, b"first"), &t).unwrap();
        db.commit(t).unwrap();
        let t = db.begin().unwrap();
        db.remove(tid, id(7), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.stats().tombstones_awaiting_purge, 1);
        // Checkpoint persists the tombstone.
        db.checkpoint().unwrap();
        // Now let vacuum purge it (row and index entry go away; the purge is
        // logged), then reinsert the key as a plain Add.
        db.maintenance.set_paused(false);
        for _ in 0..500 {
            if db.stats().tombstones_awaiting_purge == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(db.stats().tombstones_awaiting_purge, 0);
        let t = db.begin().unwrap();
        db.insert(tid, row(7, b"second"), &t).unwrap();
        db.commit(t).unwrap();
        // Power cut: the data file is exactly the checkpoint (tombstone
        // present); the log has the reinsert.
        let (data, log) = db.synced_snapshot();
        drop(db);
        let db = TestDB::open_using("txn_test.db", data, log).unwrap();
        let t = db.begin().unwrap();
        let found = db.find(tid, id(7), &t).unwrap();
        assert_eq!(
            found.map(|x| x.data.to_vec()),
            Some(b"second".to_vec()),
            "the committed reinsert must survive recovery over the checkpointed tombstone"
        );
    }

    // Regression test for todo.txt [16]: a non-root leaf (or, separately, a
    // non-root inner node) filled to capacity by a concurrent insert in the
    // window between a routing check and this thread's actual descent.
    // Needs a small page size (few entries per page) and interleaved keys
    // (not per-thread disjoint ranges) so many threads route through the
    // *same* pages concurrently — the default page size's large fanout
    // makes this window vanishingly unlikely to hit in practice. Before the
    // fix: the leaf-level race panicked directly
    // ("count == nodes- should not happen"); the inner-node-level race
    // surfaced as an unhandled PageCapacityError (insert_index's retry loop
    // for it was a stale, never-compiled comment). Both closed by
    // split_if_needed carrying its already-held lock forward (no
    // release-then-reacquire gap) and insert_index actually retrying on
    // PageCapacityError. Confirmed both were reachable before the fix: 19/200
    // and several more/100 runs respectively hit one or the other at this
    // scale.
    #[test]
    fn test_concurrent_inserts_at_small_page_size_do_not_panic_or_lose_rows() {
        const THREADS: u64 = 16;
        const ROWS_PER_THREAD: u64 = 40;
        let db: Arc<TestDB> = TestDB::create_with_page_size_and_max_index_key_size("small_page_race.db", 512, 8).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..ROWS_PER_THREAD {
                    // Interleaved (not partitioned) keys so concurrent
                    // threads route into the same leaf/inner pages, not
                    // disjoint parts of the tree.
                    let key = i * THREADS + thread_idx;
                    let t = db.begin().unwrap();
                    db.insert(tid, row(key, format!("v{key}").as_bytes()), &t)
                        .unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let t = db.begin().unwrap();
        for i in 0..ROWS_PER_THREAD {
            for thread_idx in 0..THREADS {
                let key = i * THREADS + thread_idx;
                let found = db.find(tid, id(key), &t).unwrap();
                assert_eq!(
                    found
                        .unwrap_or_else(|| panic!("row {key} missing"))
                        .data
                        .to_vec(),
                    format!("v{key}").as_bytes()
                );
            }
        }
        drop(t);
    }

    // Regression test for todo.txt items [11] and [15]: BPlusTree::remove
    // (and remove_if_txn, used by rollback's revert-of-insert path) removed
    // the data tuple and the index entry as two separate steps. Under
    // contention the index-removal step could fail after the data step had
    // already succeeded, leaving a permanently stale index entry pointing
    // at a now-vacated page — "this key can never be re-inserted again"
    // (DuplicateKey forever), and combined with [7]'s insert-cleanup path
    // also occasionally exhausting its retries, sometimes worse: a later
    // insert's orphaned row landing on that same page, so a committed
    // remove's key comes back with a stale value that was never
    // legitimately written to it.
    // [11] fixed the first layer (retry the index-removal step internally
    // instead of relying on a caller-level retry, which is unsafe once the
    // data step already succeeded — the retried call hits KeyNotFound, not
    // LockContentionError, so the caller's own retry gives up without ever
    // retrying the index cleanup). That reduced the failure rate hugely but
    // left a residual ~7.5% flake, tracked as [15] and fully root-caused
    // there: the same "outer retry re-invokes a function that's unsafe to
    // re-invoke" pattern recurring one layer up in Db::commit's tombstone
    // reclaim (silently swallowing find()'s own LockContentionError), plus
    // an unrelated timing issue (the page lock's timeout and
    // retry_on_contention's total backoff were both far shorter than
    // realistic OS scheduling jitter). See todo.txt item [15] for the full
    // three-part root cause and fix.
    #[test]
    fn test_concurrent_insert_remove_reinsert_does_not_resurrect_stale_value() {
        const THREADS: u64 = 16;
        const KEYS_PER_THREAD: u64 = 20;
        const CYCLES: u64 = 10;
        let (db, tid) = make_db_with_table();
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for cycle in 0..CYCLES {
                    for i in 0..KEYS_PER_THREAD {
                        let key = thread_idx * KEYS_PER_THREAD + i;
                        let value = format!("v{key}-{cycle}");
                        let t = db.begin().unwrap();
                        db.insert(tid, row(key, value.as_bytes()), &t).unwrap();
                        db.commit(t).unwrap();

                        let t = db.begin().unwrap();
                        db.remove(tid, id(key), &t).unwrap();
                        db.commit(t).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Every key's last committed op (per thread, per cycle) was a
        // remove, so every key must now be absent.
        let t = db.begin().unwrap();
        for thread_idx in 0..THREADS {
            for i in 0..KEYS_PER_THREAD {
                let key = thread_idx * KEYS_PER_THREAD + i;
                let found = db.find(tid, id(key), &t).unwrap();
                assert!(
                    found.is_none(),
                    "key {key} should be absent (last committed op was remove) but found {found:?}"
                );
            }
        }
        drop(t);
    }

    // Regression test for a confirmed race in BPlusTree::find_page/
    // route_to_leaf: both do a fully unlocked, multi-step descent (route,
    // then separately read the leaf landed on), with no protection against
    // a concurrent split moving the target key to a new sibling in between.
    // That produces a spurious KeyNotFound for a key that genuinely exists
    // — and since KeyNotFound isn't LockContentionError, retry_on_contention
    // never retries it, so a remove/update attempt that hits this mid-flight
    // just fails outright, leaving the row's prior (un-removed) state as
    // the permanent, incorrect "final" value. Confirmed via direct,
    // instrumented reproduction: caught cases where a fresh find_page call
    // failed for a key whose existence had just been confirmed microseconds
    // earlier by a separate lookup in the very same call, with no
    // intervening removal — and where the eventual "resurrected" value's
    // own transaction timestamp was hundreds of milliseconds away from the
    // reader's (ruling out any timestamp-ordering ambiguity as the cause).
    // Runs several rounds since the race needs concurrent structural
    // splits to manifest — tolerant of individual operation failures (logs
    // them) so one early miss doesn't abort the whole run before the final
    // "every key must be absent" check, which is the actual assertion.
    #[test]
    fn test_concurrent_insert_remove_under_splits_does_not_resurrect_stale_value() {
        const THREADS: u64 = 16;
        const KEYS_PER_THREAD: u64 = 20;
        const CYCLES: u64 = 20;
        const ROUNDS: u32 = 20;
        let mut resurrections = 0u32;
        for _round in 0..ROUNDS {
            let (db, tid) = make_db_with_table();
            let mut handles = Vec::new();
            for thread_idx in 0..THREADS {
                let db = Arc::clone(&db);
                handles.push(thread::spawn(move || {
                    for cycle in 0..CYCLES {
                        for i in 0..KEYS_PER_THREAD {
                            let key = thread_idx * KEYS_PER_THREAD + i;
                            let value = format!("v{key}-{cycle}");
                            let Ok(t) = db.begin() else { continue };
                            if db.insert(tid, row(key, value.as_bytes()), &t).is_err() {
                                continue;
                            }
                            if db.commit(t).is_err() {
                                continue;
                            }

                            let Ok(t) = db.begin() else { continue };
                            if db.remove(tid, id(key), &t).is_err() {
                                continue;
                            }
                            let _ = db.commit(t);
                        }
                    }
                }));
            }
            for h in handles {
                let _ = h.join();
            }

            let t = db.begin().unwrap();
            for thread_idx in 0..THREADS {
                for i in 0..KEYS_PER_THREAD {
                    let key = thread_idx * KEYS_PER_THREAD + i;
                    if db.find(tid, id(key), &t).unwrap().is_some() {
                        resurrections += 1;
                    }
                }
            }
            drop(t);
        }
        assert_eq!(
            resurrections, 0,
            "{resurrections} keys resurrected across {ROUNDS} rounds"
        );
    }

    // ── STORE_AUDIT.md Phase 1 findings ─────────────────────────────────────

    // T8: Transaction derives Clone, and abort() doesn't check the id is
    // currently active before moving it to `aborting` — so dropping a
    // CLONE of an already-committed Transaction guard triggers
    // Drop::drop's default rollback, which silently reverts a
    // committed write. Reproduction straight from STORE_AUDIT.md.
    #[test]
    fn test_audit_t8_abort_refuses_to_move_an_already_finished_transaction_into_aborting() {
        // The ORIGINAL reproduction (clone a committed Transaction guard,
        // drop the clone, watch Drop's default rollback revert the
        // committed write) is no longer expressible at all now that
        // Transaction isn't Clone — that in itself is the fix for that
        // exact path, verified by this file compiling without it. This
        // test covers the second, independent half of T8's fix: hardening
        // abort() itself so an already-finished id can never be
        // re-processed as if it were still live, which also protects the
        // AbortOnConflict path (see ConflictPolicy) that relies on
        // "moving an id into aborting twice is harmless" reasoning — true
        // only once this guard exists.
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        let t0_id = t0.id();
        db.insert(tid, row(1, b"v0"), &t0).unwrap();
        db.commit(t0).unwrap();

        let result = db.tx_mgr.abort(t0_id);
        assert!(
            result.is_err(),
            "abort() must refuse to move an already-finished (committed) transaction into \
             `aborting`: {result:?}"
        );

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0",
            "the committed row must be unaffected regardless"
        );
    }

    // T9: a transaction can't update or delete a row it inserted itself —
    // find_last_committed insists on a *committed* ancestor, but a fresh
    // own-insert has none (undo_id is None from the start).
    #[test]
    fn test_audit_t9_a_transaction_can_update_a_row_it_inserted_itself() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        let result = db.update(tid, row(1, b"v1"), &t);
        assert!(
            result.is_ok(),
            "updating a row inserted earlier in the SAME transaction must succeed: {result:?}"
        );
        db.commit(t).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v1"
        );
    }

    #[test]
    fn test_audit_t9_a_transaction_can_remove_a_row_it_inserted_itself() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        let result = db.remove(tid, id(1), &t);
        assert!(
            result.is_ok(),
            "removing a row inserted earlier in the SAME transaction must succeed: {result:?}"
        );
        db.commit(t).unwrap();

        let reader = db.begin().unwrap();
        assert!(db.find(tid, id(1), &reader).unwrap().is_none());
    }

    // Per STORE_AUDIT.md T9's own recommended test list: insert then
    // update in one transaction, then ROLL BACK, must make the row look
    // like it never existed — not restore some intermediate value.
    #[test]
    fn test_audit_t9_rollback_after_insert_then_update_leaves_no_trace_of_the_row() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.update(tid, row(1, b"v1"), &t).unwrap();
        db.rollback(t).unwrap();

        let reader = db.begin().unwrap();
        assert!(
            db.find(tid, id(1), &reader).unwrap().is_none(),
            "rolling back insert-then-update must leave the row looking like it never existed"
        );
    }

    // Mirror case: insert then remove in one transaction, then commit —
    // the row must simply not exist, with no leftover tombstone artifact
    // tripping up a later insert of the same key.
    #[test]
    fn test_audit_t9_insert_then_remove_then_commit_allows_reinserting_the_same_key() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t).unwrap();
        db.remove(tid, id(1), &t).unwrap();
        db.commit(t).unwrap();

        let t2 = db.begin().unwrap();
        db.insert(tid, row(1, b"v2"), &t2).unwrap();
        db.commit(t2).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v2"
        );
    }

    // T13: undo replay applies operations forward, not reverse. Harmless
    // today only because find_last_committed always resolves a pre-image
    // to the true committed ancestor regardless of chain position — but
    // TWO updates to an EXISTING committed row within one transaction
    // build a real two-hop undo chain (v0 -> v1 -> v2), and rolling back
    // must restore v0 (the original), not v1 (an intermediate value a
    // forward replay would stop at).
    #[test]
    fn test_audit_t13_rollback_after_two_updates_in_one_txn_restores_the_original_not_an_intermediate_value()
     {
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t0).unwrap();
        db.commit(t0).unwrap();

        let t1 = db.begin().unwrap();
        db.update(tid, row(1, b"v1"), &t1).unwrap();
        db.update(tid, row(1, b"v2"), &t1).unwrap();
        db.rollback(t1).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0",
            "rollback must restore the ORIGINAL pre-transaction value, not an intermediate one"
        );
    }

    // T6: reads are not snapshot-isolated against a committed DELETE —
    // commit's tombstone reclaim is a physical removal, not deferred like
    // undo discard, so a reader with an already-open snapshot loses a row
    // it could see a moment ago.
    #[test]
    fn test_audit_t6_a_reader_snapshot_survives_a_concurrent_committed_delete() {
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t0).unwrap();
        db.commit(t0).unwrap();

        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0"
        );

        let t1 = db.begin().unwrap();
        db.remove(tid, id(1), &t1).unwrap();
        db.commit(t1).unwrap();

        assert_eq!(
            db.find(tid, id(1), &reader).unwrap().unwrap().data.to_vec(),
            b"v0",
            "a transaction's own snapshot must still see a row that was deleted and committed \
             by someone else after it began"
        );
    }

    // STORE_AUDIT.md S4: Db::create on an existing path rewrites the
    // header with page_count=0 and starts overwriting old pages — no
    // create_new/truncate/exists-check at all. Needs the real File
    // backend: MemFile::open always returns a brand new, unshared buffer
    // regardless of "path", so it can't reproduce a same-path collision.
    #[test]
    fn test_audit_s4_create_on_an_existing_path_does_not_silently_destroy_it() {
        let db_name = temp_db_path("audit_s4_create_existing");
        FileDB::delete(&db_name).unwrap_or_default();

        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"precious"), &t).unwrap();
        db.commit(t).unwrap();
        db.close().unwrap();

        let result = FileDB::create(&db_name);
        assert!(
            result.is_err(),
            "Db::create on an already-existing path must fail, not silently truncate it back \
             to an empty database"
        );

        FileDB::delete(&db_name).unwrap_or_default();
    }

    // STORE_AUDIT.md S5: Db::delete unlinks all three files with no lock
    // check at all, so a second process (or, as tested here, a second
    // file description in the same process — flock is per-open-file-
    // description, not per-process, so this is a faithful repro without
    // needing real multi-process orchestration) can delete a database
    // that's still open and in active use.
    #[test]
    fn test_audit_s5_delete_refuses_to_remove_a_locked_live_database() {
        let db_name = temp_db_path("audit_s5_delete_locked");
        FileDB::delete(&db_name).unwrap_or_default();

        let db = FileDB::create(&db_name).unwrap(); // holds an flock on all 3 files

        let result = FileDB::delete(&db_name);
        assert!(
            result.is_err(),
            "delete must refuse to remove a database that's still open/locked elsewhere"
        );

        drop(db);
        FileDB::delete(&db_name).unwrap_or_default();
    }

    // STORE_AUDIT.md S6: the catalog (system table page) has a hard
    // capacity — create_table inserts into the in-memory `tables` map
    // BEFORE persisting, so once the catalog page is full, the failing
    // call leaves a table that's visible in memory (table_id_by_name
    // finds it) but was never actually persisted — and every later
    // checkpoint()/close() fails forever after, since they keep trying
    // to persist a catalog that no longer fits.
    #[test]
    fn test_audit_s6_create_table_fails_cleanly_once_the_catalog_page_is_full() {
        let db = TestDB::create("audit_s6_catalog_full.db").unwrap();
        let mut failed_name = None;
        for i in 0..1000 {
            let name = format!("table_number_{i:05}");
            if db.create_table(name.clone()).is_err() {
                failed_name = Some(name);
                break;
            }
        }
        let failed_name = failed_name
            .expect("expected create_table to eventually fail once the catalog page is full");

        assert!(
            db.table_id_by_name(&failed_name).unwrap().is_none(),
            "a table whose create_table call FAILED must not be left half-created in memory"
        );
        assert!(
            db.checkpoint().is_ok(),
            "checkpoint must still succeed after a cleanly-rejected create_table, not fail \
             forever afterward"
        );
    }

    // STORE_AUDIT.md S7 (part 1): validate_table_name only checks length
    // and exact-duplicate — nothing reserves the WHOLE `__system.`
    // namespace, only the specific internal names that happen to already
    // exist collide by accident. A name that doesn't happen to collide
    // with any actual internal generator succeeds today.
    #[test]
    fn test_audit_s7_the_system_prefix_is_reserved_as_a_whole_namespace() {
        let db = TestDB::create("audit_s7_system_prefix.db").unwrap();
        let result = db.create_table("__system.not_actually_used_anywhere".to_string());
        assert!(
            result.is_err(),
            "the entire __system. prefix must be reserved, not just the specific names \
             already in use internally"
        );
    }

    // STORE_AUDIT.md S7 (part 2): ValueItem::Ord panics on any mixed-type
    // comparison (and on Blob at all) — a Rec key whose field types
    // differ from an existing key in the same tree (store has no schema
    // to prevent this) panics inside route_to_leaf, under a page lock.
    // Wrapped in catch_unwind rather than asserted directly, matching S3:
    // the eventual fix (a type-rank ordering) doesn't lock in what
    // specific Ordering comes back, only that comparing never panics.
    #[test]
    fn test_audit_s7_value_item_ord_does_not_panic_on_mixed_types() {
        let result = std::panic::catch_unwind(|| {
            ValueItem::Integer(1).cmp(&ValueItem::Str(("x".into(), 1)))
        });
        assert!(
            result.is_ok(),
            "comparing two ValueItems of different types must never panic"
        );
    }

    #[test]
    fn test_audit_s7_value_item_ord_does_not_panic_on_blob() {
        let a = ValueItem::Blob((Arc::from(&b"x"[..]), 1));
        let b = ValueItem::Blob((Arc::from(&b"y"[..]), 1));
        let result = std::panic::catch_unwind(|| a.cmp(&b));
        assert!(
            result.is_ok(),
            "comparing two Blob ValueItems must never panic"
        );
    }

    // STORE_AUDIT.md T12: into_id() disarms Transaction::drop's default
    // rollback unconditionally, before revert_txn_writes has actually
    // run — so if the revert itself fails partway through, nothing
    // re-arms anything: the id is left in `active` forever (permanent
    // WriteConflicts on its rows, permanently pinning every later
    // reader's snapshot, per T7). `Db::rollback_by_id` guards against this
    // generally: any `revert_txn_writes` failure moves the transaction to
    // `aborting` (via `tx_mgr.abort`) instead of leaving it stranded in
    // `active`.
    //
    // This test's ORIGINAL reproduction (still described by its name) used
    // dropping the transaction's own table mid-flight to make
    // revert_txn_writes fail with TableNotFound. STORE_AUDIT.md T17
    // deliberately closed exactly that failure mode — revert_undo_ops now
    // treats a since-dropped table as "nothing to revert" (a table that no
    // longer exists can't have a row to restore either way) instead of
    // propagating the error — so rollback here now succeeds outright
    // rather than failing into T12's aborting-fallback path. That's a
    // strictly better outcome (a full, immediate resolution instead of a
    // permanently-retried-but-never-succeeding one — the table is gone
    // forever, so T12's own fallback would otherwise retry this exact
    // revert from `aborting` on every future `begin()`, forever, and never
    // succeed). T12's fallback itself is unchanged and still in place for
    // every OTHER way revert_txn_writes can fail (e.g. get_undo_operations
    // itself failing, or a genuine contention timeout) — this specific
    // scenario just no longer exercises it.
    #[test]
    fn test_audit_t12_rollback_of_a_transaction_whose_table_was_dropped_mid_flight_now_succeeds_cleanly()
     {
        let (db, tid) = make_db_with_table();
        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v0"), &t0).unwrap();
        db.commit(t0).unwrap();

        let t1 = db.begin().unwrap();
        let t1_id = t1.id();
        db.update(tid, row(1, b"v1"), &t1).unwrap();

        db.drop_table("rows").unwrap();

        let result = db.rollback(t1);
        assert!(
            result.is_ok(),
            "STORE_AUDIT.md T17: rollback must succeed here — its table is gone, so there's \
             nothing left to revert, not a failure: {result:?}"
        );

        assert!(
            !db.tx_mgr.is_transaction_active(&t1_id),
            "a transaction whose rollback completed must not still be active"
        );
    }

    // STORE_AUDIT.md T14: row relocation on update (BPlusTree::
    // update_checked's "doesn't fit alongside its siblings" branch)
    // removes the tuple from its old page, releases that page's lock,
    // writes the new copy elsewhere, and ONLY THEN repoints the index —
    // three separate lock acquisitions, not one atomic step. A find()
    // landing in the gap between "removed from old page" and "index
    // repointed" resolves the STALE index entry, finds nothing on the
    // (now tuple-less) old page, and reports a committed, existing key
    // as missing. CONFIRMED empirically before writing this (real
    // threads, no artificial delay): ~1140 false "missing" reads out of
    // 20,000 iterations on a first run.
    #[test]
    fn test_audit_t14_concurrent_find_never_observes_a_committed_row_as_missing_during_relocation()
    {
        let db = TestDB::create_with_page_size("audit_t14_probe.db", 1024).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();

        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, &[0u8; 50]), &t0).unwrap();
        db.insert(tid, row(2, &[0u8; 50]), &t0).unwrap(); // shares row 1's page
        db.commit(t0).unwrap();

        const ITERS: usize = 20_000;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let missing = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let db_w = db.clone();
        let stop_w = stop.clone();
        let writer = thread::spawn(move || {
            let mut big = true;
            for _ in 0..ITERS {
                let data = if big { vec![7u8; 800] } else { vec![7u8; 50] };
                big = !big;
                let t = db_w.begin().unwrap();
                let _ = db_w.update(tid, row(1, &data), &t);
                let _ = db_w.commit(t);
            }
            stop_w.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        let db_r = db.clone();
        let missing_r = missing.clone();
        let reader = thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let t = db_r.begin().unwrap();
                if db_r.find(tid, id(1), &t).unwrap().is_none() {
                    missing_r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        assert_eq!(
            missing.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "row 1 always exists (only ever updated, never removed) — a concurrent find() must              never observe it as missing, even mid-relocation"
        );
    }

    // T10: UndoId is minted as `id.len() as u16`, where `id.len()` is the
    // transaction's own TOTAL undo-op count so far across every row it
    // touches (not per-row) — this wraps at 65,536 ops, not just saturates
    // or errors. A wrapped id then silently resolves, via
    // `Logger::find_undo_tuple`'s `v.get(undo_id.0 as usize)`, to whatever
    // op happens to sit at that (much lower) index in the transaction's
    // op list — if that index belongs to a DIFFERENT row, the walk hands
    // back that other row's pre-image entirely.
    //
    // The audit's own suggested repro — one row, updated 70,000+ times,
    // then rolled back — turns out NOT to exercise this (confirmed by
    // running it against the unfixed code below: it passes). Two reasons,
    // both traced directly: (1) rollback (`Db::revert_txn_writes`) replays
    // the raw undo `Vec<Operation>` directly and never goes through
    // `UndoId`/`find_undo_tuple` at all; (2) `update()`'s `build` resolves
    // every one of a transaction's own repeated updates to the SAME row
    // via `find_last_committed`, which always walks straight past the
    // txn's own in-flight chain to the true committed ancestor — so every
    // entry logged for repeated updates to one row has IDENTICAL content
    // regardless of index, and even a wrong slot within that row's own
    // entries is indistinguishable. The bug's actually-observable
    // consequence is the other one the audit names: a concurrent MVCC read
    // (`Db::find_visible_to`) walking a wrapped `undo_id` into a
    // DIFFERENT row's undo entry. Reproduced below by giving row 1 (not
    // the row under test) the low slot the wraparound aliases into.
    #[test]
    fn test_audit_t10_a_wrapped_undo_id_must_not_alias_a_different_rows_undo_entry() {
        // Sized so the LAST of row 2's updates is pushed at absolute undo
        // slot 65,536 — whose UndoId (minted from the txn's running op
        // count, truncated to u16) wraps to 0, aliasing row 1's slot.
        const NUM_ROW2_UPDATES: u64 = 65_536;

        let (db, tid) = make_db_with_table();

        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"row1_original"), &t0).unwrap();
        db.insert(tid, row(2, b"row2_original"), &t0).unwrap();
        db.commit(t0).unwrap();

        let t1 = db.begin().unwrap();
        // Occupies undo slot 0 in t1's op list.
        db.update(tid, row(1, b"row1_touched"), &t1).unwrap();
        // 65,536 updates to a DIFFERENT row: the last one lands at
        // absolute slot 65,536, which wraps to 0 and aliases row 1's slot
        // above instead of row 2's own true committed ancestor.
        for i in 0..NUM_ROW2_UPDATES {
            let data = format!("row2_v{i}");
            db.update(tid, row(2, data.as_bytes()), &t1).unwrap();
        }

        // t1 never commits, so it's invisible to any other reader — this
        // forces find()'s undo-chain walk instead of a read-your-own-writes
        // shortcut.
        let reader = db.begin().unwrap();
        let visible = db.find(tid, id(2), &reader).unwrap().unwrap();
        assert_eq!(
            visible.id,
            id(2),
            "a lookup for row 2 resolved to a DIFFERENT row's identity — a wrapped UndoId \
             aliased row 1's undo slot instead of walking to row 2's own committed ancestor"
        );
        assert_eq!(visible.data.to_vec(), b"row2_original");
    }

    // STORE_AUDIT.md T2: Page::set_dirty stamps a freshly-dirtied page's own
    // `lsn` field from `clock.last_written()` — the CURRENT flush watermark
    // ("whatever's already durable") — not from the redo LSN of the
    // mutation dirtying it right now. That LSN doesn't exist yet at dirty
    // time: Db::insert calls table.insert() (which mutates + dirties the
    // page) BEFORE self.logger.log_new(op) (which mints the LSN and logs
    // the record). The writer thread's flush gate is `page.lsn <
    // clock.last_written()` — "flush once anything newer than this page's
    // stamped LSN is durable" — so a page stamped with a STALE watermark
    // can satisfy that gate (and get flushed) before its own change's redo
    // record has even been logged, let alone synced.
    //
    // Reproduced here without any timing/concurrency/fault-injection:
    // advance the watermark to a known value via an unrelated, already-
    // durable prior write, then check whether a NEW insert's page ends up
    // stamped with something <= that old watermark (proving it got the
    // stale value) or something strictly greater (proving it got a fresh
    // LSN of its own, which — since LSNs only increase and this op's LSN
    // is minted after the warmup already committed — could only happen if
    // the page was correctly stamped with ITS OWN operation's LSN).
    #[test]
    fn test_audit_t2_a_page_is_stamped_with_its_own_operations_lsn_not_a_stale_watermark() {
        let (db, tid) = make_db_with_table();

        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"warmup"), &t0).unwrap();
        db.commit(t0).unwrap();
        // Add + Commit = 2 records; wait for both to actually land so the
        // watermark below reflects a real, already-durable value rather
        // than racing the async log runner.
        wait_for_durable_logs(&db, 2);
        let watermark_before = db.logger.clock().last_written();

        let table = db.table_by_id(tid).unwrap();
        let t1 = db.begin().unwrap();
        let page_id = table.insert(row(2, b"v2"), t1.id()).unwrap();
        let page = db.buffer.get_page(page_id).unwrap();
        assert!(
            page.lsn_id().unwrap() > watermark_before,
            "the page must be stamped with THIS operation's own (not yet durable) redo lsn, \
             not the stale watermark from before this insert began — otherwise the writer \
             thread's flush gate (page.lsn < last_written) is already satisfied by the \
             watermark alone, and could flush this page before its own redo record is durable"
        );
    }

    // STORE_AUDIT.md T1: Db::commit logged its Commit record via log_new,
    // a plain channel send that returns as soon as the log runner thread
    // has merely accepted the message — not once it's actually written and
    // fsynced. The runner also deliberately lingers up to LOG_BATCH_LINGER
    // (200us) after the first message of a batch, hoping more arrive to
    // batch under the same sync (see its own comment) — so even with
    // MemFile's effectively-instant do_sync, there's a real, near-
    // guaranteed window right after commit() returns during which the
    // runner hasn't even started processing this record yet.
    //
    // No fake/slow DBFile needed to observe this: checking the record
    // count in the underlying log buffer IMMEDIATELY after commit()
    // returns (no polling — see wait_for_durable_logs, which exists
    // precisely because this ISN'T normally guaranteed) is enough. Without
    // the fix, this reliably fails (the record often isn't there yet);
    // with the fix, it's true synchronously by construction.
    #[test]
    fn test_audit_t1_commit_does_not_return_before_its_own_record_is_durable() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(
            count_log_records(&db.log_file),
            2, // Add + Commit
            "commit() must not return until its own commit record (and everything logged \
             earlier in this transaction) is actually durable, not just queued for the log \
             runner thread"
        );
    }

    // STORE_AUDIT.md T3: checkpoint() flushed EVERY dirty page (including
    // one written by a transaction that was abandoned, not committed) and
    // then truncated the log — after a crash, a fresh session finds no
    // trace of that transaction in either the active or aborting set and
    // (see TransactionManager::is_committed: "absent from both" means
    // committed) wrongly treats its write as committed.
    //
    // Adapted from the audit's own reproduction: it used mem::forget (a
    // transaction that never drops, so Transaction::drop's own move into
    // `aborting` never runs) — under a QUIESCED checkpoint (the fix this
    // test verifies), that specific shape is unfixable by construction:
    // checkpoint() must wait for every in-flight transaction to actually
    // resolve, and a forgotten one never will, by definition, regardless
    // of design. That's an accepted, inherent limit of quiescing (a
    // genuinely leaked transaction blocks all future checkpoints forever
    // either way), not a gap this fix leaves open — a real caller bug, not
    // a data-integrity one. The scenario this fix DOES need to close is a
    // transaction that's abandoned normally (dropped without an explicit
    // commit/rollback, moving to `aborting` for later reclaim) or one
    // that's still genuinely active — both tested below.
    #[test]
    fn test_audit_t3_checkpoint_reverts_an_abandoned_write_before_flushing() {
        let (db, tid) = make_db_with_table();
        {
            let t = db.begin().unwrap();
            db.insert(tid, row(7, b"uncommitted"), &t).unwrap();
            // t drops here without commit/rollback — moves to `aborting`,
            // not reclaimed until something drains it (normally only
            // begin(), which checkpoint() itself now also does — see
            // wait_for_no_in_flight_transactions).
        }
        db.checkpoint().unwrap();

        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let reader = db2.begin().unwrap();
        assert!(
            db2.find(tid, id(7), &reader).unwrap().is_none(),
            "checkpoint must not let an abandoned, never-committed write survive as if \
             it had committed — either by reverting it before flushing (this fix), or \
             (if that somehow didn't happen) by leaving enough log behind for replay to \
             still catch it"
        );
    }

    // Phase 6 replaces STORE_AUDIT.md T3's quiesce: a checkpoint never waits
    // for a transaction. With one genuinely still active (mid-work, on
    // another thread) the checkpoint completes, its records are retained
    // (an extra segment), a crash right then reverts its flushed write,
    // and once it commits and the next checkpoint runs the retention drops
    // back to one segment with the write intact.
    #[test]
    fn test_checkpoint_does_not_wait_for_a_still_active_transaction() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true); // only our checkpoints
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"committed-before"), &t).unwrap();
        db.commit(t).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.stats().wal_segments, 1);

        let started = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let db_w = db.clone();
        let started_w = started.clone();
        let release_w = release.clone();
        let writer = thread::spawn(move || {
            let t = db_w.begin().unwrap();
            db_w.insert(tid, row(9, b"in-flight"), &t).unwrap();
            started_w.wait();
            while !release_w.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            db_w.commit(t).unwrap();
        });
        started.wait();

        // The writer's transaction is active right now: the checkpoint
        // must complete anyway, promptly.
        let start = std::time::Instant::now();
        db.checkpoint().unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "checkpoint waited on a transaction"
        );
        assert!(!writer.is_finished());
        assert_eq!(
            db.stats().wal_segments,
            2,
            "the active transaction's records live in the older segment, which is retained"
        );

        // A crash here: its page may have been flushed, but its records
        // were kept, so recovery reverts it and keeps the committed row.
        let (data, log) = db.synced_snapshot();
        let crashed = TestDB::open_using("txn_test.db", data, log).unwrap();
        let r = crashed.begin().unwrap();
        assert_eq!(
            crashed.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"committed-before"
        );
        assert!(
            crashed.find(tid, id(9), &r).unwrap().is_none(),
            "uncommitted at the cut"
        );
        drop(r);
        drop(crashed);

        release.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();
        db.checkpoint().unwrap();
        assert_eq!(
            db.stats().wal_segments,
            1,
            "nothing in flight: only the fresh segment"
        );
        let reader = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(9), &reader).unwrap().unwrap().data.to_vec(),
            b"in-flight",
            "the transaction's write committed after the checkpoint — it must still be there"
        );
    }

    // STORE_AUDIT.md T5: PageBuffer::write_header was fire-and-forget (a
    // plain channel send, no reply, no fsync) — Db::checkpoint truncated
    // the log right after calling it with no guarantee the header write had
    // even been dequeued yet, let alone durably written. A crash in that
    // window could leave a stale on-disk header (wrong page_count/
    // last_checkpoint) paired with an already-empty log. Fixed via
    // write_header_synced (a reply-channel variant that pwrite's AND
    // fsyncs before replying) — checked here with NO polling, mirroring
    // T1's own count_log_records-no-polling pattern: if checkpoint()
    // returned without the header actually being durable, this would be
    // flaky/failing rather than reliably true.
    #[test]
    fn test_audit_t5_checkpoint_header_write_is_durable_before_returning() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();

        let page_count_before = db.page_count();
        db.checkpoint().unwrap();

        let bytes = db.file.data();
        let on_disk: Header = from_bytes(&bytes[..size_of::<Header>()]).unwrap();
        assert_eq!(
            on_disk.page_count, page_count_before,
            "checkpoint() must not return until the header it just wrote is actually \
             durable on disk, not merely queued for the buffer's writer thread"
        );
    }

    // STORE_AUDIT.md T16: the persisted free list is only ever written at
    // checkpoint/close — a transaction that takes a page from it, writes,
    // and commits (its own page flushed some other way, e.g. ordinary
    // eviction, and its redo record durable) between checkpoints leaves
    // the ON-DISK free list stale: it still lists that now-live page as
    // free. On reopen with no intervening checkpoint, the stale list would
    // let a later allocation hand that same page out again, silently
    // clobbering committed data replay already restored.
    //
    // Constructed directly at the byte level (per this finding's own
    // design doc) rather than trying to engineer the exact "flushed via
    // eviction but never checkpointed" timing: writes a free-list page
    // that wrongly lists `first_data_page` (a real, definitely-reachable
    // page) as free, exactly the state a stale checkpoint snapshot would
    // produce, and checks reconciliation removes it on open.
    #[test]
    fn test_audit_t16_reopen_reconciles_the_free_list_against_reachable_pages() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        wait_for_durable_logs(&db, 2);

        let table = db.table_by_id(tid).unwrap();
        let reachable_page = table.table.first_data_page;

        // Directly write a free-list page that wrongly claims a real,
        // reachable page is free — simulates a checkpoint snapshot taken
        // before that page was ever allocated, now stale relative to
        // committed reality.
        let page = crate::page::Page::new_pinned(db.header.page_size, db.buffer.page_overhead());
        page.add_tuple(Tuple::new(
            0,
            &postcard::to_allocvec(&vec![reachable_page]).unwrap(),
        ))
        .unwrap();
        db.buffer
            .write_page(crate::constant::FREE_PAGE_TABLE_PAGE.into(), &page)
            .unwrap();
        // Sync the header's page_count (never touched since Db::create) to
        // the live value WITHOUT truncating the log — see this helper's
        // own comment. Without this, open_using's freshly-cloned header
        // still says whatever page_count was at table creation, well
        // under FIRST_USER_PAGE for a from-scratch test db, and
        // load_system_tables refuses to even start.
        sync_header_without_truncating_logs(&db);

        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        assert!(
            !db2.buffer.get_free_pages().contains(&reachable_page),
            "a page reachable from a live table's own structure must never be left on \
             the free list after open, regardless of what a stale persisted snapshot \
             claims — otherwise a later allocation could hand it out again and clobber \
             committed data"
        );
    }

    // STORE_AUDIT.md S1: the main file's header had no format version, no
    // checksum, and no validation of page_size/first_page_offset before
    // this — a corrupted or foreign header decoded (or failed to decode)
    // however postcard happened to interpret the bytes, with a bogus
    // page_size able to drive every later page-offset calculation instead
    // of being refused up front. All three tests round-trip the real
    // on-disk header through Header itself (decode, mutate one field,
    // re-encode) rather than hand-computing byte offsets, so they don't
    // depend on the header's exact wire layout.
    fn tamper_header(f: &MemFile, mutate: impl FnOnce(&mut Header)) {
        let mut buf = vec![0u8; 128];
        f.pread(&mut buf, 0).unwrap();
        let mut hdr: Header = postcard::from_bytes(&buf).unwrap();
        mutate(&mut hdr);
        let bytes = postcard::to_allocvec(&hdr).unwrap();
        f.pwrite(&bytes, 0).unwrap();
    }

    #[test]
    fn test_audit_s1_open_rejects_a_corrupted_header_checksum() {
        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        // Flip the stored checksum without touching anything else (or
        // resealing) — an otherwise byte-for-byte-valid header whose
        // checksum simply no longer matches, exactly what bit rot or a
        // torn write would produce.
        tamper_header(&f, |hdr| hdr.header_checksum ^= 0xFFFF_FFFF);
        let result = TestDB::open_using("txn_test.db", f, l);
        assert!(
            matches!(result, Err(StoreError::HeaderCorruption(_))),
            "expected HeaderCorruption, got {:?}",
            result.err()
        );
    }

    #[test]
    fn test_audit_s1_open_rejects_an_invalid_page_size() {
        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        // Resealed after mutating, so the checksum check passes and only
        // the page_size validation is what's actually under test here.
        tamper_header(&f, |hdr| {
            hdr.page_size = 12345; // not a power of two
            hdr.seal();
        });
        let result = TestDB::open_using("txn_test.db", f, l);
        assert!(
            matches!(result, Err(StoreError::HeaderCorruption(_))),
            "expected HeaderCorruption, got {:?}",
            result.err()
        );
    }

    #[test]
    fn test_audit_s1_open_rejects_a_first_page_offset_smaller_than_the_header_itself() {
        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        tamper_header(&f, |hdr| {
            hdr.first_page_offset = 4;
            hdr.seal();
        });
        let result = TestDB::open_using("txn_test.db", f, l);
        assert!(
            matches!(result, Err(StoreError::HeaderCorruption(_))),
            "expected HeaderCorruption, got {:?}",
            result.err()
        );
    }

    // Persistence versioning Stage 3: downgrades a real, freshly-closed
    // database's on-disk header to the pre-Stage-3 (format_version 3) shape
    // — no max_index_key_size field at all, sealed with THAT version's own
    // (frozen) checksum formula — the actual proof this stage's own
    // versioning works: a v3 file opens with the default, not an error.
    #[test]
    fn test_header_decodes_pre_stage3_v3_fixture_with_default_key_size() {
        use super::{DEFAULT_MAX_INDEX_KEY_SIZE, HEADER_FORMAT_VERSION, HeaderV3Shape};

        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        let mut buf = vec![0u8; 128];
        f.pread(&mut buf, 0).unwrap();
        let current: Header = postcard::from_bytes(&buf).unwrap();
        let mut v3 = HeaderV3Shape {
            magic: current.magic,
            format_version: 3,
            first_page_offset: current.first_page_offset,
            page_count: current.page_count,
            page_size: current.page_size,
            last_checkpoint: current.last_checkpoint,
            counter: current.counter,
            checkpoint_lsn: current.checkpoint_lsn,
            header_checksum: 0,
        };
        v3.header_checksum = crate::page::fnv1a_32(&v3.checksum_input());
        let bytes = postcard::to_allocvec(&v3).unwrap();
        f.pwrite(&bytes, 0).unwrap();

        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        assert_eq!(db2.header.max_index_key_size, DEFAULT_MAX_INDEX_KEY_SIZE);
        assert_eq!(db2.header.format_version, HEADER_FORMAT_VERSION);
        assert_eq!(db2.header.page_count, current.page_count);
        assert_eq!(db2.header.counter, current.counter);
    }

    #[test]
    fn test_header_decode_rejects_a_v3_fixture_with_a_bad_checksum() {
        use super::HeaderV3Shape;

        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        let mut buf = vec![0u8; 128];
        f.pread(&mut buf, 0).unwrap();
        let current: Header = postcard::from_bytes(&buf).unwrap();
        let v3 = HeaderV3Shape {
            magic: current.magic,
            format_version: 3,
            first_page_offset: current.first_page_offset,
            page_count: current.page_count,
            page_size: current.page_size,
            last_checkpoint: current.last_checkpoint,
            counter: current.counter,
            checkpoint_lsn: current.checkpoint_lsn,
            header_checksum: 0xDEAD_BEEF, // wrong on purpose
        };
        let bytes = postcard::to_allocvec(&v3).unwrap();
        f.pwrite(&bytes, 0).unwrap();

        let result = TestDB::open_using("txn_test.db", f, l);
        assert!(
            matches!(result, Err(StoreError::HeaderCorruption(_))),
            "expected HeaderCorruption, got {:?}",
            result.err()
        );
    }

    #[test]
    fn test_header_decode_rejects_an_unrecognized_format_version() {
        let (db, _tid) = make_db_with_table();
        let (f, l) = db.close().unwrap();
        tamper_header(&f, |hdr| {
            hdr.format_version = 99;
            hdr.seal();
        });
        let result = TestDB::open_using("txn_test.db", f, l);
        assert!(
            matches!(result, Err(StoreError::HeaderCorruption(_))),
            "expected HeaderCorruption, got {:?}",
            result.err()
        );
    }

    // Persistence versioning Stage 3: the actual motivating bug, reproduced
    // and proven fixed. Before this stage, PAGE_OVERHEAD was a fixed 112
    // bytes (Rust's size_of::<PageDto>(), unrelated to a real high_key's
    // postcard-serialized size — measured directly during this stage's
    // design, not assumed). A composite index key wide enough to make
    // high_key's real encoding exceed that boundary once a split set it
    // would silently misalign the header/data split on every future read
    // of that page. This inserts enough 200-byte composite-keyed rows
    // (well past the old 112-byte ceiling, comfortably under the new
    // page_overhead's default 512-byte cap) to force at least one index
    // split — the only event that ever sets high_key (see
    // alloc_sibling_index_page) — then verifies every row still round-
    // trips correctly across a close/reopen.
    #[test]
    fn test_persistence_versioning_stage3_wide_composite_key_survives_split_and_reopen() {
        use crate::valueitem::{IndexKey, ValueItem};

        const KEY_WIDTH: usize = 200;
        const ROWS: u64 = 100; // comfortably more than DEFAULT_PAGE_SIZE / 250 nodes_per_page

        let wide_key = |i: u64| -> DBIdType {
            let s = format!("{i:0>width$}", width = KEY_WIDTH);
            DBIdType::Rec(IndexKey::new_from(&[ValueItem::Str((s, KEY_WIDTH as u32))]).unwrap())
        };

        let db = TestDB::create_with_page_size("wide_key.db", DEFAULT_PAGE_SIZE).unwrap();
        // index_entry_size wide enough for this composite key (well past
        // the production default MAX_ENTRY_BYTES, which only fits a plain
        // Int key) — forces real splits within ROWS inserts at the default
        // 16 KiB page size.
        let tid = db
            .create_table_with_index_entry_size("rows".to_string(), 250)
            .unwrap();

        let t = db.begin().unwrap();
        for i in 0..ROWS {
            db.insert(
                tid,
                Tuple::new_with(wide_key(i), format!("value-{i}").as_bytes(), None, None),
                &t,
            )
            .unwrap();
        }
        db.commit(t).unwrap();

        let (f, l) = db.close().unwrap();
        let db2 = TestDB::open_using("wide_key.db", f, l).unwrap();
        let t2 = db2.begin().unwrap();
        for i in 0..ROWS {
            let found = db2.find(tid, wide_key(i), &t2).unwrap();
            assert_eq!(
                found.map(|t| t.data().to_vec()),
                Some(format!("value-{i}").into_bytes()),
                "row {i} did not survive the composite-key split + reopen"
            );
        }
    }

    // STORE_AUDIT.md T17: drop_table used to remove a table and free its
    // pages with nothing stopping an insert/update/remove/find already in
    // flight against that same table (looked up before drop_table started)
    // from continuing to touch those exact page ids after they'd been
    // handed back to the free list and potentially reused. Reproduces the
    // "in flight" window directly via table_by_id_guarded (the same guard
    // insert/update/remove/find now hold for their whole call) rather than
    // trying to time a race against real page I/O — mirrors
    // test_audit_t3_checkpoint_waits_for_a_still_active_transaction's own
    // "spawn, wait on a barrier, assert the other side is still blocked"
    // shape.
    #[test]
    fn test_audit_t17_drop_table_waits_for_an_in_flight_operation() {
        let (db, tid) = make_db_with_table();
        let started = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let db_h = db.clone();
        let started_h = started.clone();
        let release_h = release.clone();
        let holder = thread::spawn(move || {
            let (_table, _guard) = db_h.table_by_id_guarded(tid).unwrap();
            started_h.wait();
            while !release_h.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        started.wait();
        let db_d = db.clone();
        let dropper = thread::spawn(move || db_d.drop_table("rows"));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !dropper.is_finished(),
            "drop_table must wait for an in-flight insert/update/remove/find to finish \
             before freeing the table's pages"
        );

        release.store(true, std::sync::atomic::Ordering::Relaxed);
        holder.join().unwrap();
        dropper.join().unwrap().unwrap();
    }

    // STORE_AUDIT.md T17 (logging half): drop_table doesn't log anything to
    // the WAL, only write_system_tables — so a crash between drop_table
    // and the next checkpoint leaves the log still holding redo/undo
    // records for a table id the catalog no longer has, e.g. this test's
    // own insert+commit for "rows". Before the fix, process_log's redo
    // pass propagated table_by_id's TableNotFound with `?`, failing
    // Db::open entirely — a table drop should never make an otherwise
    // sound database unopenable. Flushes via PageBuffer::checkpoint (not
    // Db::checkpoint), matching sync_header_without_truncating_logs's own
    // trick, so the catalog's removal is durable but the log is not
    // truncated — reproducing the exact "dropped but not yet checkpointed"
    // crash window.
    #[test]
    fn test_audit_t17_replay_survives_log_records_for_a_table_dropped_before_the_next_checkpoint() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        wait_for_durable_logs(&db, 2);

        db.drop_table("rows").unwrap();
        // Flushes the now-table-less system catalog page (PageBuffer::
        // checkpoint) and syncs the header's page_count — never touched by
        // an ordinary drop_table, only Db::checkpoint/close — WITHOUT
        // truncating the log (see this helper's own comment; T16's own
        // test needed the same fix for the same reason).
        sync_header_without_truncating_logs(&db);

        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        assert!(
            db2.table_id_by_name("rows").unwrap().is_none(),
            "the dropped table must stay gone after replay, not resurrect or fail open"
        );
    }

    // STORE_AUDIT.md T15: recovery's `insert_if_needed`/`update_if_needed`
    // use an "exists ⇒ already applied" heuristic instead of a real
    // per-page LSN idempotence check. The audit's own text argues this is
    // safe given T1+T2 hold (a page can never become durable before its
    // own record does, and commit() never returns before its own record
    // is durable — see each fix's own writeup) — the specific bad
    // ordering that would break the heuristic ("txn B's page flush is
    // durable but txn A's own commit record, which B's data logically
    // depends on, is not") cannot happen: the WAL is a single, physically
    // sequential file, so a later record can never be durable while an
    // earlier one in the same append stream is not. This test reproduces
    // the audit's own concrete, "should already work" scenario directly:
    // A inserts K, commits, checkpoints (durable). B then updates K,
    // commits — logged, but crashes before its own page write is ever
    // flushed to the main file (still just sitting on the durable log).
    // Replay must still converge on B's value: A's Add is skipped (the
    // key already exists, on the checkpointed page), B's Mod is applied
    // via update_if_needed (the on-page data doesn't match B's post-image,
    // so it's not a no-op). No fix expected here — this exists to turn
    // the audit's "should be fine, verify once T4 lands" into a real,
    // permanent regression test instead of an unverified assumption.
    #[test]
    fn test_audit_t15_replay_applies_a_committed_update_whose_own_page_flush_never_landed() {
        let (db, tid) = make_db_with_table();

        let t0 = db.begin().unwrap();
        db.insert(tid, row(1, b"v1"), &t0).unwrap();
        db.commit(t0).unwrap();
        db.checkpoint().unwrap();
        // Second checkpoint's own synchronous reply is queued strictly
        // after the first one's fire-and-forget header write (same
        // channel, FIFO) — see test_replay_recovers_a_write_whose_page_
        // flush_never_reached_the_main_file's identical use of this.
        db.checkpoint().unwrap();
        let stale_main_file_bytes = db.file.data();

        let t1 = db.begin().unwrap();
        db.update(tid, row(1, b"v2"), &t1).unwrap();
        db.commit(t1).unwrap();

        // Only txn B's (post-checkpoint) records are in the log at all —
        // checkpoint truncated everything from A above.
        wait_for_durable_logs(&db, 2);
        let (_, log_file) = crash_clone(&db);

        // A "crashed" main file built from the pre-B snapshot — B's page
        // write never reached it, only the log knows B happened.
        let crashed_file = MemFile::new();
        crashed_file.pwrite(&stale_main_file_bytes, 0).unwrap();

        let db2 = TestDB::open_using("txn_test.db", crashed_file, log_file).unwrap();
        let t = db2.begin().unwrap();
        assert_eq!(
            db2.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"v2",
            "replay must apply B's committed update even though its own page flush \
             never reached the main file — only A's (older) flush did"
        );
    }

    // STORE_AUDIT.md T11: the persisted ts generator snapshot is only
    // written by write_system_tables (checkpoint/close/table-creation),
    // same staleness risk the numeric transaction-id sequence already has
    // — so it alone isn't enough to guarantee a new session's ts()
    // sequence starts above every ts a PRIOR session ever used. Db::
    // process_log additionally reconciles it against every ts seen while
    // scanning the (un-truncated) log itself. This test forces exactly
    // the gap that closes: the on-disk generator snapshot is frozen at
    // table-creation time (before the transaction below ever began), and
    // ONLY the log — not checkpointed away — knows that transaction's own
    // ts. A fresh transaction after reopen must still be ordered after it.
    #[test]
    fn test_audit_t11_reopen_advances_ts_past_everything_seen_in_the_log_not_just_the_persisted_value()
     {
        let (db, tid) = make_db_with_table();

        let t = db.begin().unwrap();
        let a_ts = t.id().ts();
        db.insert(tid, row(1, b"v1"), &t).unwrap();
        db.commit(t).unwrap();
        wait_for_durable_logs(&db, 2);

        // Syncs page_count only (needed for load_system_tables to accept
        // the reopened header) — deliberately not a real checkpoint or
        // write_system_tables call, so the on-disk generator snapshot
        // stays exactly as it was at table-creation time, before the
        // transaction above ever began.
        sync_header_without_truncating_logs(&db);

        let (f, l) = crash_clone(&db);
        let db2 = TestDB::open_using("txn_test.db", f, l).unwrap();
        let t2 = db2.begin().unwrap();
        assert!(
            t2.id().ts() > a_ts,
            "a transaction begun after reopen must be ordered after every transaction \
             from the prior session, even one the persisted generator snapshot never \
             saw — got {} which must exceed {}",
            t2.id().ts(),
            a_ts
        );
    }

    // STORE_AUDIT.md S8: resolve_visible panicked outright on a tuple with
    // no txn_id — every real Db::insert/update/remove always sets one, but
    // a corrupted or hand-crafted on-disk file could easily contain a
    // tuple that doesn't, and for an embedded library a panic is a process
    // crash for the host, not a recoverable error. Writes such a tuple
    // through the real production write path (BPlusTree::insert_at_lsn
    // directly, bypassing Db::insert — which is the one place that always
    // calls tuple.set_txn_id first) rather than raw byte surgery, so this
    // is exactly the shape a genuinely corrupted database would produce.
    #[test]
    fn test_audit_s8_find_returns_a_typed_error_for_a_tuple_missing_its_txn_id() {
        let (db, tid) = make_db_with_table();
        let table = db.table_by_id(tid).unwrap();
        let lsn = db.logger.next_lsn();
        table
            .insert_at_lsn(Tuple::new(1, b"corrupt"), TransactionId::from(1), lsn)
            .unwrap();

        let t = db.begin().unwrap();
        let result = db.find(tid, id(1), &t);
        assert!(
            matches!(result, Err(StoreError::Corruption(_))),
            "expected StoreError::Corruption, got {result:?}"
        );
    }

    // ---- Phase 5: early failure instead of hangs ----

    // A lock timeout inside a write is a bug report, not a retryable
    // condition: the write fails with LockTimeout, the transaction is taken
    // down through the one abort path, the engine counts it, and nothing
    // retries. Once the holder lets go, the engine is fully usable again.
    #[test]
    fn test_lock_timeout_aborts_the_transaction_and_is_counted() {
        use crate::buffer::LockLevel::Index;
        let (db, tid) = make_db_with_table();
        db.set_lock_timeout(Duration::from_millis(50));
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"a"), &t).unwrap();
        db.commit(t).unwrap();

        // Another thread sits on the table's index root, which every write
        // must lock.
        let root = db.table_by_id(tid).unwrap().table.first_index_page;
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let h = db.buffer.get_page_mut(root, Index).unwrap();
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                drop(h);
            })
        };
        held_rx.recv().unwrap();

        let t = db.begin().unwrap();
        let start = std::time::Instant::now();
        let r = db.update(tid, row(1, b"b"), &t);
        assert!(matches!(r, Err(StoreError::LockTimeout(_))), "{r:?}");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "must not wait past the timeout"
        );
        assert_eq!(db.stats().lock_timeouts, 1);
        // The transaction is gone: its commit cannot report success.
        assert!(db.commit(t).is_err());

        release_tx.send(()).unwrap();
        holder.join().unwrap();
        // Nothing lingers: the row is untouched and new writes go through.
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"a"
        );
        db.update(tid, row(1, b"c"), &t).unwrap();
        db.commit(t).unwrap();
        assert_eq!(db.stats().lock_timeouts, 1);
        assert!(db.stats().degraded.is_none());
    }

    // An abort whose revert keeps failing is not retried forever: after the
    // budget the engine goes Degraded — writes and commits are refused with
    // the reason, reads continue, and the failed transaction's rows stay
    // invisible (it is Aborting, never Committed).
    #[test]
    fn test_repeated_abort_failure_degrades_engine_instead_of_spinning() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true); // we drive the retries by hand
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"a"), &t).unwrap();
        db.commit(t).unwrap();

        let t = db.begin().unwrap();
        db.update(tid, row(1, b"dirty"), &t).unwrap();
        db.fail_reverts
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(
            db.rollback(t).is_err(),
            "the injected revert failure surfaces"
        );
        assert!(
            db.stats().degraded.is_none(),
            "one failure is not degradation"
        );
        assert_eq!(db.stats().aborting_transactions, 1);

        for attempt in 1..super::ABORT_RETRY_BUDGET {
            db.maintenance_pass().unwrap();
            assert!(
                db.stats().degraded.is_none(),
                "attempt {attempt} of {} must not degrade yet",
                super::ABORT_RETRY_BUDGET
            );
        }
        db.maintenance_pass().unwrap();
        let reason = db
            .stats()
            .degraded
            .expect("degraded after the retry budget");
        assert!(reason.contains("abort of"), "{reason}");
        assert!(reason.contains("revert failure injected"), "{reason}");

        // Writes and commits are refused, naming the reason.
        let t = db.begin().unwrap();
        match db.insert(tid, row(2, b"b"), &t) {
            Err(StoreError::EngineDegraded(r)) => assert_eq!(r, reason),
            other => panic!("expected EngineDegraded, got {other:?}"),
        }
        // Reads continue, and the aborting transaction's write is invisible.
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"a"
        );
        assert!(matches!(db.commit(t), Err(StoreError::EngineDegraded(_))));
        // Degradation is sticky: a later successful retry does not lift it.
        db.fail_reverts
            .store(false, std::sync::atomic::Ordering::Release);
        db.maintenance_pass().unwrap();
        assert_eq!(db.stats().aborting_transactions, 0);
        assert!(db.stats().degraded.is_some());
    }

    // Extending a table's data chain allocates the new page with no page
    // lock held, re-locks the tail and re-checks it. Many writers filling
    // one chain concurrently must neither orphan a page (phase 0) nor leak
    // one: every allocated page is reachable from the chain or back on the
    // free list.
    #[test]
    fn test_concurrent_chain_extension_neither_orphans_nor_leaks_pages() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        // Pages the engine owns outside this table (header, catalog, ...):
        // whatever is neither reachable from the table nor free right now.
        let unaccounted = |db: &TestDB| -> u64 {
            let reachable = db
                .table_by_id(tid)
                .unwrap()
                .reachable_pages()
                .unwrap()
                .len() as u64;
            let free = db.buffer.get_free_pages().len() as u64;
            db.buffer.page_count_val() - reachable - free
        };
        let unaccounted_before = unaccounted(&db);
        const THREADS: usize = 8;
        const ROWS: usize = 150;
        let payload = vec![7u8; 300];
        let mut handles = Vec::new();
        for thread_idx in 0..THREADS {
            let db = Arc::clone(&db);
            let payload = payload.clone();
            handles.push(thread::spawn(move || {
                for i in 0..ROWS {
                    let key = (thread_idx * ROWS + i) as u64;
                    let t = db.begin().unwrap();
                    db.insert(tid, row(key, &payload), &t).unwrap();
                    db.commit(t).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut cursor = db.table_scan(tid).unwrap();
        let mut scanned = 0;
        while cursor.next().unwrap().is_some() {
            scanned += 1;
        }
        drop(cursor);
        assert_eq!(
            scanned,
            THREADS * ROWS,
            "every committed row is on the chain"
        );
        let t = db.begin().unwrap();
        for key in 0..(THREADS * ROWS) as u64 {
            assert!(
                db.find(tid, id(key), &t).unwrap().is_some(),
                "key {key} via the index"
            );
        }
        db.rollback(t).unwrap();
        assert_eq!(db.stats().lock_timeouts, 0);
        // Page accounting closes: every page allocated during the run is
        // either reachable from the table or back on the free list.
        assert_eq!(
            unaccounted(&db),
            unaccounted_before,
            "a page allocated during the run is neither reachable nor free: leaked by a lost extension race"
        );
    }

    // ---- Phase 6: segmented WAL, fuzzy checkpoint ----

    // The retention floor counts an Aborting transaction (one whose revert
    // keeps failing) exactly like an Active one: its records stay until it
    // is finished, and go at the next checkpoint after that.
    #[test]
    fn test_retention_keeps_segments_for_an_aborting_transaction() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"a"), &t).unwrap();
        db.commit(t).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.stats().wal_segments, 1);

        let t = db.begin().unwrap();
        db.update(tid, row(1, b"dirty"), &t).unwrap();
        db.fail_reverts
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(db.rollback(t).is_err());
        assert_eq!(db.stats().aborting_transactions, 1);
        db.checkpoint().unwrap();
        assert_eq!(
            db.stats().wal_segments,
            2,
            "an aborting transaction's records must outlive the checkpoint"
        );
        // A crash now: the flushed dirty page is undone from the retained log.
        let (data, log) = db.synced_snapshot();
        let crashed = TestDB::open_using("txn_test.db", data, log).unwrap();
        let r = crashed.begin().unwrap();
        assert_eq!(
            crashed.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"a"
        );
        drop(r);
        drop(crashed);

        db.fail_reverts
            .store(false, std::sync::atomic::Ordering::Release);
        db.maintenance_pass().unwrap();
        assert_eq!(db.stats().aborting_transactions, 0);
        db.checkpoint().unwrap();
        assert_eq!(
            db.stats().wal_segments,
            1,
            "finished: nothing pins the older segment"
        );
        let r = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"a"
        );
    }

    // A long-lived transaction pins every segment since it began; commits
    // keep landing in newer segments; a crash replays all of them, in order
    // (later versions win), and the pinned transaction itself is undone.
    #[test]
    fn test_recovery_replays_every_retained_segment_in_order() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        let pin = db.begin().unwrap();
        db.insert(tid, row(100, b"pinned-uncommitted"), &pin)
            .unwrap();
        for round in 0..3u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(round, format!("v{round}").as_bytes()), &t)
                .unwrap();
            if round == 0 {
                db.insert(tid, row(50, b"r0"), &t).unwrap();
            } else {
                db.update(tid, row(50, format!("r{round}").as_bytes()), &t)
                    .unwrap();
            }
            db.commit(t).unwrap();
            db.checkpoint().unwrap();
            assert_eq!(
                db.stats().wal_segments,
                round as usize + 2,
                "the pinned transaction keeps every segment since it began"
            );
        }
        let (data, log) = db.synced_snapshot();
        assert_eq!(log.list_siblings("txn_test.db.wal.").unwrap().len(), 4);
        let crashed = TestDB::open_using("txn_test.db", data, log).unwrap();
        let r = crashed.begin().unwrap();
        for round in 0..3u64 {
            assert_eq!(
                crashed
                    .find(tid, id(round), &r)
                    .unwrap()
                    .unwrap()
                    .data
                    .to_vec(),
                format!("v{round}").as_bytes(),
                "row from segment {}",
                round + 1
            );
        }
        assert_eq!(
            crashed
                .find(tid, id(50), &r)
                .unwrap()
                .unwrap()
                .data
                .to_vec(),
            b"r2",
            "last version wins"
        );
        assert!(
            crashed.find(tid, id(100), &r).unwrap().is_none(),
            "never committed"
        );
        drop(r);
        drop(crashed);

        db.rollback(pin).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(db.stats().wal_segments, 1);
    }

    // Db::open on a real filesystem finds the segments by name.
    #[test]
    fn test_file_backed_open_finds_segments_by_name() {
        let db_name = temp_db_path("segments");
        FileDB::delete(&db_name).unwrap_or_default();
        let db = FileDB::create(&db_name).unwrap();
        let tid = db.create_table("rows".to_string()).unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"one"), &t).unwrap();
        db.commit(t).unwrap();
        db.close().unwrap();
        let segments = crate::memfile::list_files_with_prefix(&format!("{db_name}.wal.")).unwrap();
        assert_eq!(
            segments.len(),
            1,
            "close leaves exactly one (empty) segment: {segments:?}"
        );
        let db = FileDB::open(&db_name).unwrap();
        let t = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &t).unwrap().unwrap().data.to_vec(),
            b"one"
        );
        drop(t);
        db.close().unwrap();
        FileDB::delete(&db_name).unwrap();
        assert!(
            crate::memfile::list_files_with_prefix(&format!("{db_name}.wal."))
                .unwrap()
                .is_empty()
        );
    }

    // ---- Phase 7: caps and follow-ups ----

    // Durability::Async returns before the fsync; the commit is visible at
    // once and durable by the next sync, so a synced snapshot taken after a
    // later Sync commit contains it.
    #[test]
    fn test_async_commit_is_visible_at_once_and_durable_after_the_next_sync() {
        let (db, tid) = make_db_with_table();
        db.checkpoint().unwrap();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"async"), &t).unwrap();
        db.commit_with(t, super::Durability::Async).unwrap();
        let r = db.begin().unwrap();
        assert_eq!(
            db.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"async"
        );
        db.rollback(r).unwrap();
        // A later Sync commit drags the earlier record to disk with it.
        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"sync"), &t).unwrap();
        db.commit(t).unwrap();
        let (data, log) = db.synced_snapshot();
        let crashed = TestDB::open_using("txn_test.db", data, log).unwrap();
        let r = crashed.begin().unwrap();
        assert_eq!(
            crashed.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"async"
        );
        assert_eq!(
            crashed.find(tid, id(2), &r).unwrap().unwrap().data.to_vec(),
            b"sync"
        );
    }

    // A transaction pinning more retained WAL than the cap allows is aborted
    // by the maintenance thread; its owner learns why on the next call, the
    // space is released, and everything else is unaffected.
    #[test]
    fn test_snapshot_too_old_on_retained_wal_bytes() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        db.set_snapshot_limits(super::SnapshotLimits {
            max_retained_wal_bytes: u64::MAX,
            max_version_records: usize::MAX,
        });
        let pin = db.begin().unwrap();
        for i in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, &[7u8; 200]), &t).unwrap();
            db.commit(t).unwrap();
            db.checkpoint().unwrap();
        }
        assert!(db.stats().wal_segments >= 2, "the pin retains segments");
        let retained = db.stats().wal_retained_bytes;
        assert!(retained > 1000);
        // Under the cap: nothing happens.
        db.maintenance_pass().unwrap();
        assert_eq!(db.stats().snapshot_too_old_aborts, 0);
        assert!(
            db.find(tid, id(0), &pin).unwrap().is_none(),
            "pin still sees its snapshot"
        );

        db.set_snapshot_limits(super::SnapshotLimits {
            max_retained_wal_bytes: retained / 2,
            max_version_records: usize::MAX,
        });
        db.maintenance_pass().unwrap();
        assert_eq!(db.stats().snapshot_too_old_aborts, 1);
        match db.find(tid, id(0), &pin) {
            Err(StoreError::SnapshotTooOld(reason)) => {
                assert!(reason.contains("retained WAL"), "{reason}");
                assert!(reason.contains(&format!("{}", pin.id())), "{reason}");
            }
            other => panic!("expected SnapshotTooOld, got {other:?}"),
        }
        // Said once; afterwards it is just a finished transaction.
        assert!(matches!(
            db.find(tid, id(0), &pin),
            Err(StoreError::TransactionAlreadyFinished)
        ));
        db.rollback(pin).unwrap();
        assert_eq!(
            db.stats().wal_segments,
            1,
            "the pinned segments went with it"
        );
        assert!(db.stats().wal_retained_bytes < retained);
        let r = db.begin().unwrap();
        assert_eq!(db.find(tid, id(19), &r).unwrap().unwrap().data.len(), 200);
    }

    #[test]
    fn test_snapshot_too_old_on_version_records() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        db.set_snapshot_limits(super::SnapshotLimits {
            max_retained_wal_bytes: u64::MAX,
            max_version_records: 10,
        });
        let pin = db.begin().unwrap();
        for i in 0..20u64 {
            let t = db.begin().unwrap();
            db.insert(tid, row(i, b"v"), &t).unwrap();
            db.commit(t).unwrap();
        }
        assert!(
            db.stats().version_records > 10,
            "the pin keeps versions alive"
        );
        db.maintenance_pass().unwrap();
        assert_eq!(db.stats().snapshot_too_old_aborts, 1);
        assert!(matches!(db.commit(pin), Err(StoreError::SnapshotTooOld(_))));
        db.maintenance_pass().unwrap();
        assert!(
            db.stats().version_records <= 10,
            "vacuum reclaimed once the pin was gone"
        );
    }

    // Recovery replays only records at or above the floor the last
    // checkpoint persisted, even when a retained segment still holds older
    // ones (kept because something later in it is above the floor).
    #[test]
    fn test_recovery_skips_records_below_the_persisted_floor() {
        let (db, tid) = make_db_with_table();
        db.maintenance.set_paused(true);
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"before-the-pin"), &t).unwrap();
        db.commit(t).unwrap();
        let pin = db.begin().unwrap(); // floor for the next checkpoint
        let t = db.begin().unwrap();
        db.insert(tid, row(2, b"after-the-pin"), &t).unwrap();
        db.commit(t).unwrap();
        db.checkpoint().unwrap();
        assert_eq!(
            db.stats().wal_segments,
            2,
            "row 2's records keep the segment, row 1's ride along"
        );
        let total: usize = wal_segments_of(&db.log_file)
            .iter()
            .map(|(_, b)| count_records_in_segment(b))
            .sum();
        assert!(
            total >= 4,
            "both transactions' records are on disk: {total}"
        );
        let floor = read_raw_header(&db).checkpoint_lsn;
        assert_eq!(floor, pin.id().id_num());

        let (data, log) = db.synced_snapshot();
        let crashed = TestDB::open_using("txn_test.db", data, log).unwrap();
        let replayed = crashed.stats().recovered_records;
        assert!(
            replayed < total,
            "records below the floor ({floor}) are not replayed: {replayed} of {total}"
        );
        assert!(replayed >= 2, "row 2's Add and Commit are above the floor");
        let r = crashed.begin().unwrap();
        assert_eq!(
            crashed.find(tid, id(1), &r).unwrap().unwrap().data.to_vec(),
            b"before-the-pin"
        );
        assert_eq!(
            crashed.find(tid, id(2), &r).unwrap().unwrap().data.to_vec(),
            b"after-the-pin"
        );
        drop(r);
        drop(crashed);
        db.rollback(pin).unwrap();
    }

    // Reopening never appends to a recovered segment (its tail may be torn);
    // the next records land in a fresh one and a checkpoint retires the old.
    #[test]
    fn test_reopen_starts_a_fresh_segment_and_a_torn_tail_never_hides_later_records() {
        let (db, tid) = make_db_with_table();
        let t = db.begin().unwrap();
        db.insert(tid, row(1, b"one"), &t).unwrap();
        db.commit(t).unwrap();
        wait_for_durable_logs(&db, 2);
        sync_header_without_truncating_logs(&db);
        let (main_file, _) = crash_clone(&db);
        let (segment_path, mut torn) = current_segment(&db);
        torn.truncate(torn.len() - 3); // tears row 1's Commit
        let disk = MemFile::new();
        disk.add_sibling_from_bytes(&segment_path, torn);

        let db2 = TestDB::open_using("txn_test.db", main_file, disk).unwrap();
        assert_eq!(
            db2.stats().wal_segments,
            2,
            "the recovered segment plus a fresh one"
        );
        let t = db2.begin().unwrap();
        assert!(
            db2.find(tid, id(1), &t).unwrap().is_none(),
            "torn commit: not committed"
        );
        db2.insert(tid, row(2, b"two"), &t).unwrap();
        db2.commit(t).unwrap();
        wait_for_durable_logs(&db2, 2 + 1); // row 1's Add survives the tear; row 2's Add + Commit are new
        // A second crash: row 2's records must be readable — they are in the
        // fresh segment, not behind the torn frame.
        let (data, log) = db2.synced_snapshot();
        let db3 = TestDB::open_using("txn_test.db", data, log).unwrap();
        let r = db3.begin().unwrap();
        assert_eq!(
            db3.find(tid, id(2), &r).unwrap().unwrap().data.to_vec(),
            b"two"
        );
        assert!(db3.find(tid, id(1), &r).unwrap().is_none());
    }
}
