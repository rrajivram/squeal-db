use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    mem::size_of,
    sync::{Arc, atomic::AtomicU64},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, bounded};
use log::error;
use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp,
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{PageId, fnv1a_32},
    table::TableIdType,
    tuple::Tuple,
    txn::TransactionId,
};

/// Per-database write-ahead-log clock. Was two process-global statics, which
/// meant every `Db` (and every test) in the process shared one LSN counter and
/// one flush watermark — leaking write-ordering state across databases (flaky
/// close/reopen in the test suite; a latent corruption bug for >1 live `Db`).
/// One clock per `Logger`, shared with the `PageBuffer` (and its writer thread)
/// that the same `Db` owns, so the WAL deferral is scoped to a single database.
#[derive(Debug)]
pub(crate) struct LsnClock {
    /// Monotonic source of redo LSNs.
    counter: AtomicU64,
    /// Highest redo LSN durably written — the flush watermark. Starts very high
    /// so freshly created pages (stamped from it) are written promptly until the
    /// first redo record lands and pulls the watermark down to a real value.
    last_written: AtomicU64,
    // STORE_AUDIT.md T1: lets a caller (Db::commit) block until a specific
    // lsn has actually become durable, instead of returning as soon as its
    // record is merely queued. `last_written` above is the source of truth
    // ("has it happened yet") — this pair exists purely so a waiter can
    // sleep instead of spin-polling that atomic, and be woken promptly when
    // it changes. `Mutex<()>` guards nothing on its own; the atomic is
    // still what's actually checked. See `wait_until_durable`/`mark_written`
    // for the standard mutex+condvar pairing this relies on for correctness
    // (no missed-wakeup window between checking the atomic and starting to
    // wait) and why the wait loop is timeout-bounded regardless (belt and
    // suspenders — even a hypothetical missed notify just costs one extra
    // loop iteration, never a permanent hang).
    durable_mutex: std::sync::Mutex<()>,
    durable_condvar: std::sync::Condvar,
}

impl Default for LsnClock {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
            last_written: AtomicU64::new(u64::MAX),
            durable_mutex: std::sync::Mutex::new(()),
            durable_condvar: std::sync::Condvar::new(),
        }
    }
}

impl LsnClock {
    pub(crate) fn next_lsn(&self) -> LsnId {
        LsnId(
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel),
        )
    }

    pub(crate) fn last_written(&self) -> LsnId {
        LsnId(self.last_written.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Advance the watermark as the log runner persists records. Holds
    /// `durable_mutex` around the store (not just around the notify) —
    /// this is the standard mutex+condvar pairing: a waiter always checks
    /// the atomic and starts waiting while holding the SAME mutex (see
    /// `wait_until_durable`), so this store can never land in the narrow
    /// window between a waiter's check and its wait call, which is
    /// precisely the window a plain unlocked notify_all could miss.
    pub(crate) fn mark_written(&self, lsn: LsnId) {
        let _guard = self.durable_mutex.lock().unwrap();
        self.last_written
            .store(lsn.0, std::sync::atomic::Ordering::Relaxed);
        self.durable_condvar.notify_all();
    }

    /// STORE_AUDIT.md T1: blocks until `lsn` is durable (i.e. until some
    /// `mark_written` call reports a value >= `lsn`), so `Db::commit` can
    /// actually wait for its own commit record to be fsynced instead of
    /// returning as soon as it's merely queued for the log runner. Group
    /// commit is preserved exactly as-is: this only waits on the SAME
    /// `last_written` watermark the runner already advances once per
    /// batch, so N concurrent committers waiting on lsns within one batch
    /// all wake from the SAME `notify_all` — nothing here changes how
    /// often the runner actually syncs.
    ///
    /// Timeout-bounded (`wait_timeout` in a loop, not a plain `wait`) as a
    /// second, independent safety net beyond the mutex+condvar pairing
    /// itself: even a hypothetical missed wakeup just costs one extra
    /// 50ms loop iteration before rechecking, never a permanent hang.
    pub(crate) fn wait_until_durable(&self, lsn: LsnId) {
        if self.is_durable(lsn) {
            return;
        }
        let mut guard = self.durable_mutex.lock().unwrap();
        while !self.is_durable(lsn) {
            let (g, _timeout) = self
                .durable_condvar
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap();
            guard = g;
        }
    }

    // `last_written() >= lsn` alone is wrong here: last_written starts at
    // the u64::MAX cold-start sentinel ("nothing tracked yet"), which is
    // deliberately >= any real lsn so a freshly-dirtied page's flush gate
    // (see Page::set_dirty's own comment) doesn't defer forever waiting
    // for a watermark that hasn't started moving yet. That's the right
    // call for gating a page flush (nothing to protect it FROM yet, so
    // let it through) but the wrong one here — the sentinel means no
    // `mark_written` has ever actually run, i.e. nothing is durable, the
    // exact opposite of what `>=` would otherwise conclude. Confirmed via
    // a real failure, not just reasoning: an early version of this method
    // used the bare `last_written() >= lsn` comparison and
    // `test_audit_t1_commit_does_not_return_before_its_own_record_is_durable`
    // failed with 0 records found — wait_until_durable returned instantly,
    // never actually waiting, on the very first commit of a fresh Db.
    fn is_durable(&self, lsn: LsnId) -> bool {
        let w = self.last_written.load(std::sync::atomic::Ordering::Relaxed);
        w != u64::MAX && w >= lsn.0
    }

    /// Ensure `next_lsn()` never mints a value <= `lsn`. Used by replay
    /// (`process_log`) once it has scanned the prior session's log and
    /// found the highest LSN it contains — without this, a freshly reopened
    /// session's counter restarts at 0 and the first new write's own record
    /// landing would regress the watermark this just set right back down
    /// (see `mark_written`'s own comment, and
    /// `test_lsn_watermark_does_not_regress_after_new_writes_post_reopen`).
    pub(crate) fn advance_counter_past(&self, lsn: LsnId) {
        self.counter
            .fetch_max(lsn.0 + 1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
// The struct itself is `pub` (not `pub(crate)`) even though it's otherwise
// an internal WAL/clock concept — `Tuple::pre_lsn`'s type, which appears in
// `Tuple::new_with`/`set_pre_lsn`'s public signatures, needs to be
// NAMEABLE from squeal-sql (an external crate) even though external code
// can never construct a real value (`Some(LsnId(_))`) — only ever pass
// `None`, since the inner field stays `pub(crate)`. Mirrors the old
// `UndoId`'s exact same visibility split before it was retired
// (STORE_AUDIT.md T10 / T4_S2_WAL_DESIGN.md §7).
pub struct LsnId(pub(crate) u64);

// ---------------------------------------------------------------------------
// Log file header (T4_S2_WAL_DESIGN.md §2)
//
// The log file's first bytes are this fixed-size header, written once when
// the file is created and never rewritten with different content afterward
// (only re-written VERBATIM after a checkpoint truncate — see
// `log_runner`'s Checkpoint arm). This is what lets `Db::open` refuse
// cleanly if the WAL sitting next to a database file doesn't actually
// belong to it (wrong database, wrong build's WAL format, wrong page
// size) instead of either failing deep inside recovery with a confusing
// decode error, or — worse — silently replaying operations that assume a
// different page layout.
// ---------------------------------------------------------------------------

/// "SqWL" — deliberately a different length AND value from the main file's
/// own 2-byte `MAGIC` (`db.rs`), so the two file kinds can never be mistaken
/// for each other even under partial/garbled reads.
pub(crate) const LOG_MAGIC: [u8; 4] = [0x53, 0x71, 0x57, 0x4c];
/// WAL format version. Bump on any incompatible framing/`Operation` change —
/// checked for an EXACT match on open (not `<=`): a version this build
/// doesn't recognize is exactly the "don't guess" case, not something to
/// silently tolerate.
pub(crate) const CURRENT_LOG_VERSION: u16 = 1;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct LogHeader {
    magic: [u8; 4],
    #[serde(with = "postcard::fixint::le")]
    version: u16,
    #[serde(with = "postcard::fixint::le")]
    page_size: DBSizeType,
}

impl LogHeader {
    // Every field uses a fixed-width encoding (`postcard::fixint::le` for
    // integers, a raw `[u8; N]` for magic), so the postcard-encoded size is
    // always the same regardless of field VALUES — but it is NOT
    // `size_of::<LogHeader>()`: Rust's in-memory struct layout pads for the
    // widest field's alignment (`page_size: u64` needs 8-byte alignment,
    // padding the 14 meaningful bytes up to 16), while postcard just
    // concatenates each field's encoding with no padding at all. `db.rs`'s
    // `Header` makes the same `size_of` assumption and gets away with it
    // only because the main file always has real page data immediately
    // after its header — an over-sized read absorbs a few harmless bytes
    // of that (postcard's `from_bytes` ignores unconsumed trailing bytes)
    // instead of hitting EOF. The log file has no such guarantee (a fresh
    // WAL has nothing at all after its header until the first record is
    // written), so this computes the TRUE encoded length directly instead
    // of trusting size_of to match it.
    pub(crate) fn encoded_len() -> usize {
        to_allocvec(&LogHeader {
            magic: LOG_MAGIC,
            version: CURRENT_LOG_VERSION,
            page_size: 0,
        })
        .map(|v| v.len())
        .unwrap_or(size_of::<LogHeader>())
    }
}

/// Writes a fresh `LogHeader` to `file` (assumed freshly created/positioned
/// at the start) and returns its encoded bytes — the caller threads these
/// into `Logger::set_db` so the log runner can restore them verbatim after
/// every checkpoint truncate.
pub(crate) fn write_log_header<F: DBFile>(
    file: &mut F,
    page_size: DBSizeType,
) -> Result<Vec<u8>, StoreError> {
    let header = LogHeader {
        magic: LOG_MAGIC,
        version: CURRENT_LOG_VERSION,
        page_size,
    };
    let bytes = to_allocvec(&header)?;
    file.write_all(&bytes)?;
    Ok(bytes)
}

/// Reads and validates an EXISTING log file's header against the paired main
/// file's `page_size`. Returns the raw header bytes on success (byte-
/// identical to what a fresh `write_log_header` would produce, since every
/// field it read back just got re-verified) for the same post-checkpoint
/// restore purpose as above. Checks all three fields before failing, not
/// just the first mismatch, so one error message can name everything that
/// disagreed at once.
pub(crate) fn read_and_validate_log_header<F: DBFile>(
    file: &mut F,
    expected_page_size: DBSizeType,
) -> Result<Vec<u8>, StoreError> {
    let mut bytes = vec![0u8; LogHeader::encoded_len()];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes)?;
    let header: LogHeader = from_bytes(&bytes)?;
    let mut problems = Vec::new();
    if header.magic != LOG_MAGIC {
        problems.push(format!(
            "magic {:?} != expected {:?} — this file is not a squeal_db WAL",
            header.magic, LOG_MAGIC
        ));
    }
    if header.version != CURRENT_LOG_VERSION {
        problems.push(format!(
            "WAL format version {} != this build's version {}",
            header.version, CURRENT_LOG_VERSION
        ));
    }
    if header.page_size != expected_page_size {
        problems.push(format!(
            "page_size {} != the paired database file's page_size {}",
            header.page_size, expected_page_size
        ));
    }
    if !problems.is_empty() {
        return Err(StoreError::LogHeaderMismatch(problems.join("; ")));
    }
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// On-disk record framing + recovery scan (T4_S2_WAL_DESIGN.md §3)
// ---------------------------------------------------------------------------

/// `[u32 len (LE)] [u32 checksum (LE)] [len bytes: postcard-encoded LogRecord]`.
/// `len`/`checksum` are raw fixed-width LE integers (not postcard-encoded) —
/// recovery must be able to parse the frame header without depending on
/// postcard's own varint format, and it keeps the header a fixed,
/// trivially-skippable 8 bytes. `checksum` is FNV-1a-32 (`page::fnv1a_32`) —
/// the same hash `page.rs` already uses for physical page checksums, reused
/// here rather than pulling in a new crate dependency for the identical job
/// (detect a torn write/bit rot, nothing cryptographic needed).
const FRAME_HEADER_LEN: usize = 8;

fn frame_record(payload: &[u8]) -> Vec<u8> {
    let checksum = fnv1a_32(payload);
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Does `buf` start with one COMPLETE, checksum-valid frame? Used only to
/// answer `scan_log`'s "is there more valid data after the failing record"
/// question — a checksum failure with a complete, valid frame right after
/// it can't be a torn tail (an in-progress write wouldn't have anything
/// coherent AFTER it), so it must be real, mid-file corruption.
fn frame_is_complete_and_valid(buf: &[u8]) -> bool {
    if buf.len() < FRAME_HEADER_LEN {
        return false;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let checksum = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let payload_start = FRAME_HEADER_LEN;
    if buf.len() - payload_start < len {
        return false;
    }
    fnv1a_32(&buf[payload_start..payload_start + len]) == checksum
}

#[derive(Debug)]
pub(crate) struct ScannedLog {
    pub(crate) records: Vec<LogRecord>,
}

/// Scans a buffer of framed records (starting right after the `LogHeader`
/// region — callers slice that off first) per the recovery scan rule:
///
/// 1. Fewer than 8 bytes left → torn tail at the frame header. Stop; not an
///    error.
/// 2. A full header but fewer than `len` payload bytes follow → torn tail at
///    the payload. Stop; not an error.
/// 3. A full frame is present: checksum match → valid record, keep scanning.
///    Checksum mismatch → check whether a complete, valid frame follows
///    immediately after.
///      - Yes → real, mid-file corruption (there's more valid log after it,
///        so this can't be an in-progress tail write) → hard error
///        (`StoreError::LogCorruption`), not a silent drop.
///      - No (EOF, or only another torn-tail-shaped fragment) → torn tail.
///        Stop; not an error.
///
/// Either way, "stop" means: everything scanned before this point is the
/// durable prefix; nothing past it is trusted or replayed. This function
/// does not touch the file — it's pure buffer-in, records/error-out — so
/// the framing/scan-rule logic is testable in total isolation from `Logger`
/// or `Db`.
pub(crate) fn scan_log(buf: &[u8]) -> Result<ScannedLog, StoreError> {
    let mut records = Vec::new();
    let mut pos = 0usize;
    loop {
        if buf.len() - pos < FRAME_HEADER_LEN {
            break; // torn tail at the frame header
        }
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
        let payload_start = pos + FRAME_HEADER_LEN;
        if buf.len() - payload_start < len {
            break; // torn tail at the payload
        }
        let payload = &buf[payload_start..payload_start + len];
        let actual = fnv1a_32(payload);
        if actual != checksum {
            let next_pos = payload_start + len;
            if frame_is_complete_and_valid(&buf[next_pos..]) {
                return Err(StoreError::LogCorruption(format!(
                    "checksum mismatch at byte offset {pos} (stored {checksum:#x}, computed \
                     {actual:#x}) — a complete, valid record follows it, so this is not a torn \
                     tail from an in-progress write; the log is corrupted"
                )));
            } else {
                break; // torn tail: nothing valid follows, treat as never fully written
            }
        }
        let record: LogRecord = from_bytes(payload)?;
        records.push(record);
        pos = payload_start + len;
    }
    Ok(ScannedLog { records })
}

// ---------------------------------------------------------------------------
// Unified operation / log record (T4_S2_WAL_DESIGN.md §4-5)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct LogRecord {
    pub(crate) lsn: LsnId,
    pub(crate) operation: Operation,
}

/// Channel message to the single log runner thread. Only `Record` variants
/// are ever framed/written to disk — `ShutDown`/`Checkpoint` are pure
/// in-memory control signals, exactly as they were under the old split
/// redo/undo design.
#[derive(Debug, Clone)]
pub(crate) enum LogMsg {
    Record(LogRecord),
    ShutDown,
    Checkpoint(u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Operation {
    Add {
        txn: TransactionId,
        post: Record,
    },
    // `pre: None` marks a "redo-only" Mod — logged so replay reconstructs
    // this write, but contributing nothing to undo replay. The one case
    // this arises: a transaction revising a row it inserted itself within
    // the SAME transaction (see `Db::update`'s `build` closure) — the
    // original `Add`'s own undo already fully reverts the row (by removing
    // it), so a second, per-update pre-image would be not just redundant
    // but actively WRONG to replay during rollback: reverting it would
    // re-materialize a row the Add's own revert is also removing in the
    // same pass. `pre: Some(_)` is the common case (an ordinary update to
    // an already-committed row) and always contributes to undo replay.
    Mod {
        txn: TransactionId,
        pre: Option<Record>,
        post: Record,
    },
    // Unlike Mod, Del's `pre` is never skipped, even for the analogous
    // own-insert-then-remove case — STORE_AUDIT.md T9's follow-up finding:
    // `Db::commit`'s tombstone-reclaim pass finds rows to physically clean
    // up by scanning for `Operation::Del` records specifically, not by
    // inspecting the tuple's own `pre_lsn` — so skipping this log entry
    // permanently orphans the index entry.
    Del {
        txn: TransactionId,
        pre: Record,
    },
    Commit(TransactionId, u128),
    Rollback(TransactionId, u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    pub(crate) table_id: TableIdType,
    pub(crate) timestamp: u128,
    pub(crate) tuple: Tuple,
    pub(crate) data_page: Option<PageId>,
}

impl Record {
    pub(crate) fn new(
        table_id: TableIdType,
        tuple: Tuple,
        data_page: Option<PageId>, // Only set for Add
    ) -> Self {
        Self {
            table_id,
            tuple,
            timestamp: timestamp(),
            data_page,
        }
    }
}

impl Operation {
    pub(crate) fn new_add(txn: TransactionId, post: Record) -> Self {
        Self::Add { txn, post }
    }

    pub(crate) fn new_commit(tx_id: TransactionId) -> Self {
        Self::Commit(tx_id, timestamp())
    }

    pub(crate) fn new_rollback(tx_id: TransactionId) -> Self {
        Self::Rollback(tx_id, timestamp())
    }
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct Logger {
    log_handle: Option<JoinHandle<Result<(), StoreError>>>,
    log_tx: Option<Sender<LogMsg>>,
    // Global, keyed by LSN — replaces the old per-transaction `Vec<Operation>`
    // (and its positional `UndoId` index, which wrapped at 65,536 entries;
    // STORE_AUDIT.md T10). An LSN is minted once, globally, by
    // `LsnClock::next_lsn`, and never reused within a session, so a lookup
    // by LSN can never alias a different transaction's — or a different
    // row's — entry the way a wrapped positional index could.
    records: RwLock<HashMap<LsnId, Operation>>,
    // Which LSNs belong to which transaction — for revert (rollback replays
    // exactly its own txn's ops) and for cleanup (discarding a finished
    // txn's records means removing its LSNs from both maps).
    by_txn: RwLock<HashMap<TransactionId, Vec<LsnId>>>,
    // Committed transactions whose undo trail can't be discarded YET —
    // mirrors TransactionManager's aborting/drain_aborting pattern: don't
    // clean up immediately if doing so could pull a still-open reader's
    // snapshot out from under it, park the obligation and let a later,
    // opportunistic drain (Db::begin, alongside drain_aborting) finish the
    // job once every transaction that captured this one in its snapshot
    // has itself finished. See Db::commit's discard_or_defer_undo call site
    // and drain_ready_undo_discards's own comment for the full mechanism.
    pending_undo_discards: RwLock<Vec<(TransactionId, HashSet<TransactionId>)>>,
    clock: Arc<LsnClock>,
}

impl Logger {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
    pub(crate) fn new_with_lsn(lsn: LsnId) -> Self {
        Self {
            clock: Arc::new(LsnClock {
                counter: AtomicU64::new(lsn.0 + 1),
                last_written: AtomicU64::new(u64::MAX),
                durable_mutex: std::sync::Mutex::new(()),
                durable_condvar: std::sync::Condvar::new(),
            }),
            ..Default::default()
        }
    }

    /// `log_header_bytes`: the exact bytes `Db::create_core_db`/`open_using`
    /// already wrote-or-validated at the front of `file` — handed to the
    /// runner so it can restore them verbatim after every checkpoint
    /// truncate (see `log_runner`'s `Checkpoint` arm).
    pub(crate) fn set_db(
        &mut self,
        file: impl DBFile + 'static,
        log_header_bytes: Vec<u8>,
    ) -> Result<(), StoreError> {
        // Wide enough that group commit (see log_runner) has something real
        // to batch under concurrent load, instead of bounded(1)'s "at most
        // one message ever queued" — which structurally prevented batching,
        // since a sender blocks until the runner dequeues the previous
        // message before a second one can even land in the channel. This
        // does trade away bounded(1)'s "near-synchronous" property (log()
        // returning was previously a rough proxy for "the previous record
        // is durable") — tests that need an actual durability guarantee use
        // the explicit wait_for_durable_logs poll helper instead of relying
        // on that timing coincidence.
        const LOG_CHANNEL_CAPACITY: usize = 256;
        let (tx, rx) = bounded(LOG_CHANNEL_CAPACITY);
        self.log_tx = Some(tx);

        let clock = self.clock.clone();
        self.log_handle = Some(thread::spawn(move || {
            log_runner(file, rx, clock, log_header_bytes)
        }));
        Ok(())
    }

    /// Shared handle to this database's LSN clock, for the `PageBuffer` (and
    /// its writer thread) that stamp/compare page LSNs against the same
    /// watermark.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
    }

    /// Mints a fresh LSN without logging anything yet. `Db::update`/
    /// `Db::remove`'s `build` closures call this to stamp the value onto a
    /// tuple's own `pre_lsn` field BEFORE the corresponding record is
    /// actually logged (which happens later, in `before_write`, still
    /// before the tuple is physically written into the tree) — the tuple's
    /// `pre_lsn` and the record `log()` is given afterward must be the
    /// SAME lsn, which is why minting and logging are split into two steps
    /// instead of one.
    pub(crate) fn next_lsn(&self) -> LsnId {
        self.clock.next_lsn()
    }

    /// STORE_AUDIT.md T1: blocks until `lsn` is durable — see
    /// `LsnClock::wait_until_durable`'s own comment. `Db::commit` calls this
    /// on the lsn its own Commit record was logged under, right before
    /// finishing the commit, so a caller is never told a commit succeeded
    /// before it's actually fsynced.
    pub(crate) fn wait_until_durable(&self, lsn: LsnId) {
        self.clock.wait_until_durable(lsn)
    }

    /// Records `op` under the given (already-minted — see `next_lsn`) lsn:
    /// tracks it in-memory for Add/Mod/Del (so a later rollback or MVCC walk
    /// can find it), handles Rollback's immediate-discard special case, and
    /// enqueues the framed record for the log runner thread. Replaces the
    /// old split `log_redo`+`log_undo` pair with ONE call per operation.
    pub(crate) fn log(&self, lsn: LsnId, op: Operation) -> Result<(), StoreError> {
        match &op {
            Operation::Add { txn, .. } | Operation::Mod { txn, .. } | Operation::Del { txn, .. } => {
                self.records.write().insert(lsn, op.clone());
                self.by_txn.write().entry(txn.clone()).or_default().push(lsn);
            }
            // Rollback physically reverts the transaction's writes before
            // this op is even logged (see Db::rollback_by_id) — nothing is
            // ever left "owned" by a rolled-back transaction for a
            // concurrent reader to need to walk back through, so its undo
            // trail is always safe to drop immediately, unlike Commit's
            // (see discard_or_defer_undo).
            Operation::Rollback(id, _) => {
                self.discard_txn_records(id);
            }
            _ => {}
        }
        if let Some(tx) = &self.log_tx {
            tx.send(LogMsg::Record(LogRecord { lsn, operation: op }))
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    /// Convenience for call sites that don't need the lsn ahead of time
    /// (insert's fresh Add, and the Commit/Rollback markers) — mints and
    /// logs in one step, returning the lsn assigned.
    pub(crate) fn log_new(&self, op: Operation) -> Result<LsnId, StoreError> {
        let lsn = self.next_lsn();
        self.log(lsn, op)?;
        Ok(lsn)
    }

    fn discard_txn_records(&self, id: &TransactionId) {
        if let Some(lsns) = self.by_txn.write().remove(id) {
            let mut records = self.records.write();
            for lsn in lsns {
                records.remove(&lsn);
            }
        }
    }

    /// Drop a transaction's in-memory undo records. Called after its undo has
    /// been fully replayed (abort reclamation) — a dropped/aborted txn logs no
    /// Commit/Rollback op, so its records aren't cleaned by log()'s Rollback
    /// branch above.
    pub(crate) fn discard_undo(&self, id: &TransactionId) {
        self.discard_txn_records(id);
    }

    /// Called by Db::commit right after logging a Commit op: decides
    /// whether `id`'s undo trail can be dropped now or must wait. Mirrors
    /// TransactionManager's aborting/drain_aborting pattern — `others` is
    /// every OTHER transaction that's still active at this exact commit
    /// point (captured once, here, not re-checked later): any one of them
    /// might have `id` in its own snapshot (captured at ITS begin()), which
    /// means `id`'s pre-commit state must stay reachable via undo-walk for
    /// as long as that reader could still ask for it. If none are active,
    /// this is the common (low-concurrency) case and the old immediate-
    /// discard behavior applies unchanged.
    pub(crate) fn discard_or_defer_undo(&self, id: TransactionId, others: HashSet<TransactionId>) {
        if others.is_empty() {
            self.discard_txn_records(&id);
        } else {
            self.pending_undo_discards.write().push((id, others));
        }
    }

    /// Opportunistic maintenance for deferred undo discards (see
    /// discard_or_defer_undo) — called alongside drain_aborting, e.g. at
    /// Db::begin(). For each committed transaction whose discard was
    /// deferred, drops from its waiter set any transaction that has since
    /// finished (committed or aborted, so it's no longer in
    /// `currently_active`); once a transaction's waiter set is empty —
    /// nothing that could still need its pre-commit state remains active —
    /// its undo trail is actually removed.
    pub(crate) fn drain_ready_undo_discards(&self, currently_active: &HashSet<TransactionId>) {
        let mut pending = self.pending_undo_discards.write();
        if pending.is_empty() {
            return;
        }
        let mut still_pending = Vec::with_capacity(pending.len());
        for (id, waiters) in pending.drain(..) {
            let remaining: HashSet<TransactionId> = waiters
                .into_iter()
                .filter(|w| currently_active.contains(w))
                .collect();
            if remaining.is_empty() {
                self.discard_txn_records(&id);
            } else {
                still_pending.push((id, remaining));
            }
        }
        *pending = still_pending;
    }

    pub(crate) fn get_undo_operations(
        &self,
        id: TransactionId,
    ) -> Result<Vec<Operation>, StoreError> {
        // A transaction that never wrote anything (e.g. read-only — only
        // `find()` calls) has no entry here at all. That's not an error: it
        // just means there's nothing to undo/cleanup. Db::commit/Db::rollback
        // rely on this returning `Ok` so they can reach their final
        // tx_mgr.commit/rollback call and actually deactivate the
        // transaction — see Transaction::into_id.
        let by_txn = self.by_txn.read();
        let Some(lsns) = by_txn.get(&id) else {
            return Ok(Vec::new());
        };
        let records = self.records.read();
        Ok(lsns.iter().filter_map(|l| records.get(l).cloned()).collect())
    }

    /// Resolves a tuple's `pre_lsn` pointer to the operation that recorded
    /// its pre-image — replaces the old, per-transaction-positional
    /// `find_undo_tuple(TransactionId, UndoId)`. No `TransactionId` needed
    /// at all: the record already carries its own txn, and an LSN is
    /// globally unique so there's nothing to disambiguate by transaction.
    pub(crate) fn find_record(&self, lsn: LsnId) -> Option<Operation> {
        self.records.read().get(&lsn).cloned()
    }

    pub(crate) fn checkpoint(&self, ts: u128) -> Result<(), StoreError> {
        if let Some(tx) = &self.log_tx {
            tx.send(LogMsg::Checkpoint(ts))
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn shutdown(self) -> Result<(), StoreError> {
        if let Some(tx) = self.log_tx {
            tx.send(LogMsg::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(h) = self.log_handle {
            match h.join() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining log runner. Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }
        Ok(())
    }
}

// Caps how many already-queued messages one batch pulls off the channel
// before writing. Tried 10 (much smaller than the channel's own 256-slot
// capacity, on the theory that it would bound worst-case per-message
// latency) — measured DRAMATICALLY worse File-backend throughput instead
// (~13-15k ops/s -> ~600-1400 ops/s on the perf harness's single-threaded
// insert/update/find phases). Root cause: once fsync (a real, ~ms-scale
// cost) is slower than production, the channel genuinely backs up with a
// real backlog — not a linger artifact, the messages are already queued,
// no waiting involved. A cap smaller than the channel's own capacity then
// forces MANY separate drain-and-fsync cycles to clear one backlog instead
// of one (e.g. a 200-message backlog needs 20 cycles at cap=10 vs 1 at
// cap=256) — ~20x more fsync calls for the identical amount of work, which
// is almost exactly the regression measured. The cap needs to be at least
// as large as the channel capacity so one drain can always fully empty
// whatever's already backlogged, regardless of how that backlog formed.
const MAX_LOG_BATCH: usize = 256;

// How long to linger after the first message, hoping a concurrent sender's
// message lands in time to join the same batch (and thus the same fsync).
// A plain non-blocking try_recv() (no linger at all) only ever catches a
// message that happens to already be queued at the exact instant this
// thread wakes up — under realistic per-operation latency (lock
// acquisition, tree traversal, ...) that's rarely more than one, so nearly
// every batch ends up size 1 regardless of how many threads are actually
// concurrent. That defeats the entire point of batching before fsync
// (do_sync is real, millisecond-scale disk I/O): confirmed via the `perf`
// example — batching without a linger measured WORSE than no batching at
// all on the File backend (insert dropped from ~41k to ~14k ops/s single-
// threaded, since fsync now fires on nearly every record instead of never).
// This linger is intentionally much smaller than a typical fsync latency,
// so it costs little when nothing else is happening, but gives real
// concurrent load a genuine window to accumulate into one batch instead of
// paying its own separate fsync.
const LOG_BATCH_LINGER: Duration = Duration::from_micros(200);

/// The single runner thread for the WAL (T4_S2_WAL_DESIGN.md §6) — replaces
/// the old `undo_log_runner`+`redo_log_runner` pair. One file, one thread,
/// one fsync per batch: a batch either lands completely or the crash caught
/// it mid-`write_all`, handled by `scan_log`'s torn-tail rule on the next
/// open. There is no longer a "redo landed, undo didn't" state to reach
/// (STORE_AUDIT.md T4), because there's only one artifact.
fn log_runner(
    file: impl DBFile,
    recv: Receiver<LogMsg>,
    clock: Arc<LsnClock>,
    log_header_bytes: Vec<u8>,
) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        // Block for the first message, then linger briefly for more to
        // accumulate into the same batch (see LOG_BATCH_LINGER's own
        // comment) — stopping at the first timeout, not retrying the full
        // linger window MAX_LOG_BATCH times, so an isolated single message
        // only ever pays one linger's worth of extra latency.
        let first = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        let mut batch: Vec<u8> = Vec::new();
        let mut highest_lsn: Option<LsnId> = None;
        let mut special: Option<LogMsg> = None;
        let mut pending = Some(first);
        for _ in 0..MAX_LOG_BATCH {
            let msg = match pending.take() {
                Some(m) => m,
                None => match recv.recv_timeout(LOG_BATCH_LINGER) {
                    Ok(m) => m,
                    Err(_) => break,
                },
            };
            match msg {
                LogMsg::ShutDown | LogMsg::Checkpoint(_) => {
                    // Stop batching here — flush what's accumulated so far
                    // (preserving order: everything queued strictly before
                    // this message lands on disk first), then handle this
                    // message on its own once the batch write below runs.
                    special = Some(msg);
                    break;
                }
                LogMsg::Record(rec) => {
                    highest_lsn = Some(match highest_lsn {
                        Some(l) if l.0 >= rec.lsn.0 => l,
                        _ => rec.lsn,
                    });
                    batch.extend_from_slice(&frame_record(&to_allocvec(&rec)?));
                }
            }
        }
        if !batch.is_empty() {
            file.seek(SeekFrom::End(0))?;
            file.write_all(&batch)?;
            file.do_sync()?;
            // Only after the whole batch is durable — mark_written signals
            // "everything up to this lsn is safe to flush its page", which
            // must not be true before the bytes actually landed.
            if let Some(lsn) = highest_lsn {
                clock.mark_written(lsn);
            }
        }
        match special {
            Some(LogMsg::ShutDown) => break,
            Some(LogMsg::Checkpoint(_ts)) => {
                // Truncating to zero would also erase the LogHeader written
                // at file creation/validated at open — restore it
                // immediately so the file always starts with a valid
                // header, the same invariant Db::open's validation depends
                // on (T4_S2_WAL_DESIGN.md §2). truncate() resets the file's
                // LENGTH but not its seek cursor — write_all without an
                // explicit seek first would resume writing wherever the
                // cursor was left (the old end-of-file, now past the
                // truncated length), padding the gap with zeros instead of
                // landing the header at the actual start of the file.
                file.truncate()?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(&log_header_bytes)?;
                file.do_sync()?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::{LOG_MAGIC, LogHeader, LsnId, read_and_validate_log_header, scan_log,
                write_log_header};
    use crate::{
        error::StoreError,
        logger::{Logger, Operation, Record, frame_record},
        memfile::MemFile,
        tuple::Tuple,
        txn::TransactionId,
    };
    use postcard::to_allocvec;

    #[test]
    fn test_log_redo_returns_incrementing_lsn() {
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        let lsn1 = logger.log_new(op.clone()).unwrap();
        let lsn2 = logger.log_new(op).unwrap();
        assert!(lsn2 > lsn1);
    }

    #[test]
    fn test_log_redo_unique_lsns() {
        let logger = Logger::new();
        let mut lsns = Vec::new();
        for i in 0..5 {
            let op = Operation::new_commit(TransactionId::from(i));
            lsns.push(logger.log_new(op).unwrap());
        }
        let unique: std::collections::HashSet<_> = lsns.iter().cloned().collect();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn test_log_undo_commit_op_no_db() {
        // Without set_db, log() should succeed (log_tx is None, msg is silently dropped)
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        assert!(logger.log_new(op).is_ok());
    }

    #[test]
    fn test_log_undo_rollback_op_no_db() {
        let logger = Logger::new();
        let op = Operation::new_rollback(TransactionId::from(2));
        assert!(logger.log_new(op).is_ok());
    }

    #[test]
    fn test_log_undo_add_op_with_txn_id() {
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let mut tuple = Tuple::new(1, b"hello");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_new(op).is_ok());
    }

    #[test]
    fn test_log_undo_indexes_by_operation_txn_not_tuple_txn_id() {
        // log() indexes by the Operation's own txn id, not record.tuple.txn_id.
        // This matters because Mod/Del undo records deliberately carry a
        // pre-image tuple tagged with a *different* (older, committed) txn_id
        // than the operation being logged. A tuple with no txn_id at all (as
        // here) must therefore still log successfully.
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let tuple = Tuple::new(1, b"hello"); // txn_id not set
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id.clone(), record);
        assert!(logger.log_new(op).is_ok());
        assert_eq!(logger.get_undo_operations(txn_id).unwrap().len(), 1);
    }

    #[test]
    fn test_logger_with_memfile_shutdown() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), Vec::new()).unwrap();
        let txn_id = TransactionId::from(99);
        let op = Operation::new_commit(txn_id);
        logger.log_new(op).unwrap();
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_logger_undo_add_with_db() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), Vec::new()).unwrap();
        let txn_id = TransactionId::from(5);
        let mut tuple = Tuple::new(42, b"data");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_new(op).is_ok());
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_tuple_new_in_txn() {
        use crate::tuple::Tuple;
        // LsnId's inner field is pub(crate) — constructable directly from
        // anywhere in this crate, no special-casing needed (unlike the old,
        // now-retired UndoId, which was only ever constructed inside this
        // module).
        let txn = TransactionId::from(42);
        let pre_lsn = LsnId(7);
        let t = Tuple::new_with(
            crate::tuple::DBIdType::Int(1),
            b"hello",
            Some(txn.clone()),
            Some(pre_lsn),
        );
        assert_eq!(t.txn_id, Some(txn.clone()));
        assert_eq!(t.pre_lsn, Some(pre_lsn));
        assert_eq!(t.data.to_vec(), b"hello");
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.txn_id, Some(txn));
        assert_eq!(t2.data.to_vec(), b"hello");
    }

    // --- LogHeader (T4_S2_WAL_DESIGN.md §2) ---

    #[test]
    fn test_log_header_round_trips_through_write_and_validate() {
        let mut file = MemFile::new();
        let written = write_log_header(&mut file, 4096).unwrap();
        let validated = read_and_validate_log_header(&mut file, 4096).unwrap();
        assert_eq!(written, validated);
    }

    #[test]
    fn test_log_header_rejects_a_mismatched_page_size() {
        let mut file = MemFile::new();
        write_log_header(&mut file, 4096).unwrap();
        let err = read_and_validate_log_header(&mut file, 8192).unwrap_err();
        assert!(matches!(err, StoreError::LogHeaderMismatch(_)));
    }

    #[test]
    fn test_log_header_rejects_a_wrong_magic() {
        let mut file = MemFile::new();
        write_log_header(&mut file, 4096).unwrap();
        // Corrupt just the magic bytes (first 4 bytes of the header).
        let mut bytes = file.data();
        bytes[0] ^= 0xFF;
        let mut corrupted = MemFile::new();
        std::io::Write::write_all(&mut corrupted, &bytes).unwrap();
        let err = read_and_validate_log_header(&mut corrupted, 4096).unwrap_err();
        assert!(matches!(err, StoreError::LogHeaderMismatch(_)));
    }

    #[test]
    fn test_log_header_rejects_a_wrong_version() {
        // Hand-construct a header with a future/unknown version — a real
        // version bump would use a different CURRENT_LOG_VERSION constant,
        // so this constructs the mismatch directly rather than waiting for
        // one to exist.
        let bad = LogHeader {
            magic: LOG_MAGIC,
            version: CURRENT_LOG_VERSION_FOR_TEST_ONLY_NEVER_MATCHES,
            page_size: 4096,
        };
        let bytes = to_allocvec(&bad).unwrap();
        let mut file = MemFile::new();
        std::io::Write::write_all(&mut file, &bytes).unwrap();
        let err = read_and_validate_log_header(&mut file, 4096).unwrap_err();
        assert!(matches!(err, StoreError::LogHeaderMismatch(_)));
    }

    const CURRENT_LOG_VERSION_FOR_TEST_ONLY_NEVER_MATCHES: u16 = 0xFFFF;

    #[test]
    fn test_log_header_names_every_mismatched_field_at_once() {
        let bad = LogHeader {
            magic: [0, 0, 0, 0],
            version: CURRENT_LOG_VERSION_FOR_TEST_ONLY_NEVER_MATCHES,
            page_size: 1,
        };
        let bytes = to_allocvec(&bad).unwrap();
        let mut file = MemFile::new();
        std::io::Write::write_all(&mut file, &bytes).unwrap();
        let StoreError::LogHeaderMismatch(msg) = read_and_validate_log_header(&mut file, 4096)
            .unwrap_err()
        else {
            panic!("expected LogHeaderMismatch");
        };
        assert!(msg.contains("magic"), "message was: {msg}");
        assert!(msg.contains("version"), "message was: {msg}");
        assert!(msg.contains("page_size"), "message was: {msg}");
    }

    // --- record framing / recovery scan rule (T4_S2_WAL_DESIGN.md §3) ---

    fn sample_record_bytes(lsn: u64) -> Vec<u8> {
        let op = Operation::new_commit(TransactionId::from(lsn));
        let record = super::LogRecord {
            lsn: LsnId(lsn),
            operation: op,
        };
        to_allocvec(&record).unwrap()
    }

    #[test]
    fn test_scan_log_decodes_several_back_to_back_records() {
        let mut buf = Vec::new();
        buf.extend(frame_record(&sample_record_bytes(1)));
        buf.extend(frame_record(&sample_record_bytes(2)));
        buf.extend(frame_record(&sample_record_bytes(3)));
        let scanned = scan_log(&buf).unwrap();
        assert_eq!(scanned.records.len(), 3);
        assert_eq!(scanned.records[0].lsn, LsnId(1));
        assert_eq!(scanned.records[2].lsn, LsnId(3));
    }

    #[test]
    fn test_scan_log_empty_buffer_is_not_an_error() {
        let scanned = scan_log(&[]).unwrap();
        assert!(scanned.records.is_empty());
    }

    #[test]
    fn test_scan_log_torn_tail_at_frame_header_is_dropped_not_errored() {
        let mut buf = Vec::new();
        buf.extend(frame_record(&sample_record_bytes(1)));
        buf.extend_from_slice(&[1, 2, 3]); // fewer than 8 bytes — a torn frame header
        let scanned = scan_log(&buf).unwrap();
        assert_eq!(scanned.records.len(), 1, "the one complete record must still be recovered");
    }

    #[test]
    fn test_scan_log_torn_tail_at_payload_is_dropped_not_errored() {
        let mut buf = Vec::new();
        buf.extend(frame_record(&sample_record_bytes(1)));
        let full_second = frame_record(&sample_record_bytes(2));
        // A complete, correct frame HEADER claiming more payload than
        // actually follows — exactly what a crash mid-write_all leaves.
        buf.extend_from_slice(&full_second[..full_second.len() - 2]);
        let scanned = scan_log(&buf).unwrap();
        assert_eq!(scanned.records.len(), 1, "the torn second record must be dropped, not errored");
    }

    #[test]
    fn test_scan_log_checksum_mismatch_with_nothing_valid_after_is_a_torn_tail() {
        let mut buf = frame_record(&sample_record_bytes(1));
        // Flip a payload byte so the checksum no longer matches, with
        // nothing else following — indistinguishable from a torn write
        // that happened to leave a coherent length prefix behind.
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        let scanned = scan_log(&buf).unwrap();
        assert!(scanned.records.is_empty(), "must be treated as a torn tail, not an error");
    }

    #[test]
    fn test_scan_log_checksum_mismatch_with_a_valid_record_after_is_real_corruption() {
        let mut buf = frame_record(&sample_record_bytes(1));
        let last = buf.len() - 1;
        buf[last] ^= 0xFF; // corrupt record 1's payload
        buf.extend(frame_record(&sample_record_bytes(2))); // but record 2 is intact
        let err = scan_log(&buf).unwrap_err();
        assert!(
            matches!(err, StoreError::LogCorruption(_)),
            "a corrupted record with more valid data after it must be a hard error, not a \
             silently-dropped torn tail"
        );
    }

    // --- crash-recovery log loading (mmap-backed File, and MemFile) ---

    fn temp_log_paths(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        (
            dir.join(format!("squeal_logger_test_{tag}_{pid}_wal.log")),
            dir.join(format!("squeal_logger_test_{tag}_{pid}_wal2.log")),
        )
    }

    fn open_fresh(path: &std::path::Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    }

    fn open_readonly(path: &std::path::Path) -> std::fs::File {
        std::fs::OpenOptions::new().read(true).open(path).unwrap()
    }

    // Exercises the real-File path of write_log_header/read_and_validate_log_header
    // (MemFile's Opener::open ignores its path/OpenOptions argument entirely,
    // so this is the one place that actually needs a distinct file on disk).
    #[test]
    fn test_log_header_round_trips_through_a_real_file() {
        let (path, _unused) = temp_log_paths("header_real_file");
        {
            let mut f = open_fresh(&path);
            write_log_header(&mut f, 4096).unwrap();
        }
        let mut f = open_readonly(&path);
        let bytes = read_and_validate_log_header(&mut f, 4096).unwrap();
        assert_eq!(bytes.len(), LogHeader::encoded_len());
        std::fs::remove_file(&path).unwrap_or_default();
    }
}
