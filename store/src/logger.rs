use std::{
    io::SeekFrom,
    mem::size_of,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, bounded};
use log::error;
use postcard::{from_bytes, to_allocvec};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, SeqAccess, Visitor},
};

use crate::{
    db::{DBFile, DBSizeType},
    error::StoreError,
    page::{PageId, fnv1a_32},
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    txn::TransactionId,
};

/// Per-database clock: the ONE source of every ordered number in the engine
/// (TXN_SIMPLIFICATION_PLAN.md phase 1) — record LSNs, transaction ids, and
/// (from phase 2) commit timestamps all come from `counter`. `last_written`
/// is the durable watermark: every LSN at or below it has been fsynced.
///
/// `counter` starts at 1 and `last_written` at 0, so "nothing durable yet" is
/// the plain value 0 and an unlogged page (LSN 0) is always flushable — no
/// sentinel value anywhere.
#[derive(Debug)]
pub(crate) struct LsnClock {
    /// Next number to hand out.
    counter: AtomicU64,
    /// Highest LSN durably written.
    last_written: AtomicU64,
    // STORE_AUDIT.md T1: lets a caller (Db::commit) block until a specific
    // lsn has actually become durable. `last_written` is the source of
    // truth; this pair exists so a waiter can sleep instead of spinning.
    durable_mutex: std::sync::Mutex<()>,
    durable_condvar: std::sync::Condvar,
}

impl Default for LsnClock {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(1),
            last_written: AtomicU64::new(0),
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

    /// The next number that `next_lsn` would hand out — persisted in the
    /// header at checkpoint/close as the floor to seed from on reopen.
    pub(crate) fn next_value(&self) -> u64 {
        self.counter.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn last_written(&self) -> LsnId {
        LsnId(self.last_written.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Advance the durable watermark (monotonic) and wake waiters. Holds
    /// `durable_mutex` around the store so a waiter's check-then-wait can't
    /// miss it.
    pub(crate) fn mark_written(&self, lsn: LsnId) {
        let _guard = self.durable_mutex.lock().unwrap();
        self.last_written
            .fetch_max(lsn.0, std::sync::atomic::Ordering::AcqRel);
        self.durable_condvar.notify_all();
    }

    /// STORE_AUDIT.md T1: blocks until `lsn` is durable. Group commit is
    /// preserved: this waits on the same per-batch watermark the runner
    /// advances, so every committer in a batch wakes from one notify.
    /// Timeout-bounded as a belt-and-suspenders against a missed wakeup.
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

    fn is_durable(&self, lsn: LsnId) -> bool {
        self.last_written.load(std::sync::atomic::Ordering::Relaxed) >= lsn.0
    }

    /// Ensure `next_lsn()` never mints a value <= `lsn`. Used by replay once
    /// it has scanned the prior session's log.
    pub(crate) fn advance_counter_past(&self, lsn: LsnId) {
        self.counter
            .fetch_max(lsn.0 + 1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Seed from a persisted floor (the header's `counter`, written at the
    /// last checkpoint/close): nothing below it can still be in flight, so
    /// it is both the next value to hand out and the durable watermark.
    pub(crate) fn seed(&self, next: u64) {
        let next = next.max(1);
        self.counter
            .fetch_max(next, std::sync::atomic::Ordering::AcqRel);
        self.mark_written(LsnId(next - 1));
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
pub(crate) const CURRENT_LOG_VERSION: u16 = 2;

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

/// Byte length of the WAL's `LogHeader` — so a tool can slice a WAL file
/// into header and record region without knowing the header's layout.
pub fn header_len() -> usize {
    LogHeader::encoded_len()
}

/// Human-readable dump of a whole WAL file's bytes (header included) — one
/// line per record, plus the header and the scan verdict. For the
/// `wal_dump` example and for reading a recovery failure without a
/// debugger. Never touches a `Db`; pure bytes in, strings out.
pub fn describe_wal(bytes: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let header_len = LogHeader::encoded_len();
    if bytes.len() < header_len {
        out.push(format!(
            "file is {} byte(s), shorter than the {header_len}-byte LogHeader",
            bytes.len()
        ));
        return out;
    }
    match from_bytes::<LogHeader>(&bytes[..header_len]) {
        Ok(h) => out.push(format!(
            "LogHeader magic={:?} version={} page_size={} (magic {}, version {})",
            h.magic,
            h.version,
            h.page_size,
            if h.magic == LOG_MAGIC { "ok" } else { "MISMATCH" },
            if h.version == CURRENT_LOG_VERSION { "ok" } else { "MISMATCH" },
        )),
        Err(e) => out.push(format!("LogHeader undecodable: {e}")),
    }
    let body = &bytes[header_len..];
    let scanned = match scan_log(body) {
        Ok(s) => s,
        Err(e) => {
            out.push(format!("scan: {e}"));
            return out;
        }
    };
    let mut consumed = 0usize;
    for r in &scanned.records {
        let line = match &r.operation {
            Operation::Add { txn, post } => format!(
                "lsn={} ADD    txn={} table={} key={} page={:?}",
                r.lsn.0, txn.id_num(), post.table_id, post.tuple.id, post.data_page
            ),
            Operation::Mod { txn, pre, post } => format!(
                "lsn={} MOD    txn={} table={} key={} pre_txn={}{}",
                r.lsn.0,
                txn.id_num(),
                post.table_id,
                post.tuple.id,
                pre.tuple.txn_id.map(|t| t.id_num()).unwrap_or(0),
                if pre.tuple.is_tombstoned() { "(tombstone)" } else { "" }
            ),
            Operation::Del { txn, pre } => format!(
                "lsn={} DEL    txn={} table={} key={}",
                r.lsn.0, txn.id_num(), pre.table_id, pre.tuple.id
            ),
            Operation::Commit(t) => format!("lsn={} COMMIT txn={}", r.lsn.0, t.id_num()),
            Operation::Rollback(t) => format!("lsn={} ABORT  txn={}", r.lsn.0, t.id_num()),
            Operation::Purge { txn, table_id, key } => format!(
                "lsn={} PURGE  txn={} table={} key={}",
                r.lsn.0,
                txn.id_num(),
                table_id,
                key
            ),
            Operation::Sequence { name, high_water, dropped } => format!(
                "lsn={} SEQ    {name} {}",
                r.lsn.0,
                if *dropped { "dropped".to_string() } else { format!("high_water={high_water}") }
            ),
        };
        out.push(line);
        consumed += 1;
    }
    out.push(format!(
        "{consumed} record(s); {} byte(s) of record data; {} trailing byte(s) not part of a complete record",
        body.len(),
        trailing_unscanned_bytes(body)
    ));
    out
}

// How many bytes at the end of `body` were NOT consumed as complete, valid
// frames — the torn tail, if any. Re-walks the frames the same way scan_log
// does, stopping where it would.
fn trailing_unscanned_bytes(body: &[u8]) -> usize {
    let mut pos = 0usize;
    loop {
        if body.len() - pos < FRAME_HEADER_LEN {
            break;
        }
        let len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(body[pos + 4..pos + 8].try_into().unwrap());
        let payload_start = pos + FRAME_HEADER_LEN;
        if body.len() - payload_start < len {
            break;
        }
        if fnv1a_32(&body[payload_start..payload_start + len]) != checksum {
            break;
        }
        pos = payload_start + len;
    }
    body.len() - pos
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
/// are ever framed/written to disk; the rest are control signals.
#[derive(Debug, Clone)]
pub(crate) enum LogMsg {
    Record(LogRecord),
    ShutDown,
    /// Reply once everything queued before this message is durable.
    Sync(Sender<()>),
    /// Phase 6: start a new segment, then delete every older segment whose
    /// highest LSN is below `floor` (see `Db::checkpoint`).
    Roll {
        floor: u64,
        reply: Sender<Result<(), StoreError>>,
    },
}

// ---------------------------------------------------------------------------
// WAL segments (TXN_SIMPLIFICATION_PLAN.md phase 6)
// ---------------------------------------------------------------------------

/// One WAL segment file, `<name>.wal.<n>`. `max_lsn` is the highest LSN
/// appended to it (0 for none) — the only fact segment deletion needs.
#[derive(Debug, Clone)]
pub(crate) struct Segment {
    pub(crate) n: u64,
    pub(crate) path: String,
    pub(crate) max_lsn: u64,
    /// File size, for the retained-bytes accounting (phase 7's cap).
    pub(crate) bytes: u64,
}

pub(crate) fn segment_prefix(name: &str) -> String {
    format!("{name}.wal.")
}

pub(crate) fn segment_path(name: &str, n: u64) -> String {
    format!("{name}.wal.{n}")
}

pub(crate) fn segment_number(prefix: &str, path: &str) -> Option<u64> {
    path.strip_prefix(prefix)?.parse().ok()
}

/// The database's segments as `handle`'s namespace lists them, oldest first.
pub(crate) fn list_segments<F: DBFile>(handle: &F, name: &str) -> Result<Vec<(u64, String)>, StoreError> {
    let prefix = segment_prefix(name);
    let mut segs: Vec<(u64, String)> = handle
        .list_siblings(&prefix)?
        .into_iter()
        .filter_map(|p| segment_number(&prefix, &p).map(|n| (n, p)))
        .collect();
    segs.sort();
    Ok(segs)
}

// Hand-rolled codec, not derived: see table.rs's TableType for why. Operation
// is the WAL's own record body, so its wire tag must never depend on Rust
// declaration order. The tags below (0-6) are fixed forever at their
// historical declaration-index values; a future variant picks an unused tag
// (10+ is free) rather than reordering these.
#[derive(Debug, Clone)]
pub(crate) enum Operation {
    Add {
        txn: TransactionId,
        post: Record,
    },
    /// `pre` is always the version this write replaced — including a
    /// transaction's own earlier version (phase 3: undo replays in reverse
    /// LSN order, so an own-chain restores step by step), and a visible
    /// tombstone that an insert brought back to life.
    Mod {
        txn: TransactionId,
        pre: Record,
        post: Record,
    },
    /// Redo re-tombstones the row in place (never a physical removal —
    /// that is `Purge`'s job), so a later record in the same log suffix
    /// that builds on the tombstone still finds it.
    Del {
        txn: TransactionId,
        pre: Record,
    },
    Commit(TransactionId),
    Rollback(TransactionId),
    // TXN_SIMPLIFICATION_PLAN.md phase 1 (§3.11 of the proposal): a named
    // sequence's chunk high-water mark, its creation (`high_water` = start),
    // or its removal (`dropped`). Not transactional.
    Sequence {
        name: String,
        high_water: u64,
        dropped: bool,
    },
    /// Phase 3: vacuum physically removed the tombstone row for `key` (and
    /// its index entry) that `txn` committed. Logged before the removal so
    /// a crash mid-purge (or a checkpoint that caught half of it) replays
    /// to a consistent state: redo removes the row if it is still that
    /// transaction's tombstone, and removes a dangling index entry either
    /// way.
    Purge {
        txn: TransactionId,
        table_id: TableIdType,
        key: DBIdType,
    },
}

impl Serialize for Operation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Operation::Add { txn, post } => (0u8, txn, post).serialize(serializer),
            Operation::Mod { txn, pre, post } => (1u8, txn, pre, post).serialize(serializer),
            Operation::Del { txn, pre } => (2u8, txn, pre).serialize(serializer),
            Operation::Commit(txn) => (3u8, txn).serialize(serializer),
            Operation::Rollback(txn) => (4u8, txn).serialize(serializer),
            Operation::Sequence {
                name,
                high_water,
                dropped,
            } => (5u8, name, high_water, dropped).serialize(serializer),
            Operation::Purge { txn, table_id, key } => {
                (6u8, txn, table_id, key).serialize(serializer)
            }
        }
    }
}

struct OperationVisitor;

impl<'de> Visitor<'de> for OperationVisitor {
    type Value = Operation;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a tagged Operation")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Operation, A::Error> {
        macro_rules! next {
            ($what:literal) => {
                seq.next_element()?
                    .ok_or_else(|| DeError::custom(concat!("Operation: missing ", $what)))?
            };
        }
        let tag: u8 = next!("tag");
        match tag {
            0 => Ok(Operation::Add {
                txn: next!("txn"),
                post: next!("post"),
            }),
            1 => Ok(Operation::Mod {
                txn: next!("txn"),
                pre: next!("pre"),
                post: next!("post"),
            }),
            2 => Ok(Operation::Del {
                txn: next!("txn"),
                pre: next!("pre"),
            }),
            3 => Ok(Operation::Commit(next!("txn"))),
            4 => Ok(Operation::Rollback(next!("txn"))),
            5 => Ok(Operation::Sequence {
                name: next!("name"),
                high_water: next!("high_water"),
                dropped: next!("dropped"),
            }),
            6 => Ok(Operation::Purge {
                txn: next!("txn"),
                table_id: next!("table_id"),
                key: next!("key"),
            }),
            other => Err(DeError::custom(format!("unknown Operation tag {other}"))),
        }
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_tuple(4, OperationVisitor)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Record {
    pub(crate) table_id: TableIdType,
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
            data_page,
        }
    }
}

impl Operation {
    pub(crate) fn new_add(txn: TransactionId, post: Record) -> Self {
        Self::Add { txn, post }
    }

    pub(crate) fn new_commit(tx_id: TransactionId) -> Self {
        Self::Commit(tx_id)
    }

    pub(crate) fn new_rollback(tx_id: TransactionId) -> Self {
        Self::Rollback(tx_id)
    }
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub(crate) struct Logger {
    log_handle: Option<JoinHandle<Result<(), StoreError>>>,
    log_tx: Option<Sender<LogMsg>>,
    clock: Arc<LsnClock>,
    // Bytes the runner has appended to the current segment — what the
    // maintenance thread reads to decide when to checkpoint (and roll).
    segment_bytes: Arc<AtomicU64>,
    // Phase 6: how many segments exist (retained + current) and the current
    // segment's number, kept by the runner for stats and for close().
    segment_count: Arc<AtomicUsize>,
    current_segment: Arc<AtomicU64>,
    // Bytes in every segment on disk (retained + current).
    retained_bytes: Arc<AtomicU64>,
}

impl Logger {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// A logger sharing an existing clock — what production does via
    /// `Db::setup_needed_modules` (buffer, logger, and transaction manager
    /// all read one clock) and what test fixtures must do too: a page
    /// buffer gating flushes on a clock nobody else advances defers every
    /// stamped page forever.
    pub(crate) fn with_clock(clock: Arc<LsnClock>) -> Self {
        Self {
            clock,
            ..Default::default()
        }
    }

    pub(crate) fn new_with_lsn(lsn: LsnId) -> Self {
        Self {
            clock: Arc::new(LsnClock {
                counter: AtomicU64::new(lsn.0 + 1),
                last_written: AtomicU64::new(0),
                durable_mutex: std::sync::Mutex::new(()),
                durable_condvar: std::sync::Condvar::new(),
            }),
            ..Default::default()
        }
    }

    /// Starts the runner on `file`, the open handle to the `current`
    /// segment, with `older` the retained segments before it (oldest
    /// first). `log_header_bytes` is the exact header every segment starts
    /// with, written to each new one on roll.
    pub(crate) fn set_db(
        &mut self,
        file: impl DBFile + 'static,
        name: String,
        current: Segment,
        older: Vec<Segment>,
        log_header_bytes: Vec<u8>,
    ) -> Result<(), StoreError> {
        // Wide enough that group commit (see log_runner) has something real
        // to batch under concurrent load.
        const LOG_CHANNEL_CAPACITY: usize = 256;
        let (tx, rx) = bounded(LOG_CHANNEL_CAPACITY);
        self.log_tx = Some(tx);
        self.segment_count
            .store(older.len() + 1, std::sync::atomic::Ordering::Relaxed);
        self.current_segment
            .store(current.n, std::sync::atomic::Ordering::Relaxed);
        self.segment_bytes
            .store(current.bytes, std::sync::atomic::Ordering::Relaxed);
        let state = WalState {
            file,
            name,
            current,
            older,
            header_bytes: log_header_bytes,
            segment_bytes: self.segment_bytes.clone(),
            segment_count: self.segment_count.clone(),
            current_segment: self.current_segment.clone(),
            retained_bytes: self.retained_bytes.clone(),
        };
        let clock = self.clock.clone();
        self.log_handle = Some(thread::spawn(move || log_runner(state, rx, clock)));
        Ok(())
    }

    /// Test fixture: a runner over one in-memory segment.
    #[cfg(test)]
    pub(crate) fn set_db_for_test(&mut self, file: impl DBFile + 'static) -> Result<(), StoreError> {
        self.set_db(
            file,
            "test".into(),
            Segment {
                n: 1,
                path: "test.wal.1".into(),
                max_lsn: 0,
                bytes: 0,
            },
            Vec::new(),
            Vec::new(),
        )
    }

    /// Shared handle to this database's LSN clock.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
    }

    /// Bytes appended to the current segment.
    pub(crate) fn segment_bytes(&self) -> u64 {
        self.segment_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Segments on disk: the retained ones plus the current one.
    pub(crate) fn segments(&self) -> usize {
        self.segment_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn current_segment(&self) -> u64 {
        self.current_segment
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes across every segment on disk — what a long-lived transaction
    /// costs (phase 7's cap).
    pub(crate) fn retained_wal_bytes(&self) -> u64 {
        self.retained_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Mints a fresh LSN without logging anything yet — the write path mints
    /// before mutating (STORE_AUDIT.md T2) and logs under the same value.
    pub(crate) fn next_lsn(&self) -> LsnId {
        self.clock.next_lsn()
    }

    /// STORE_AUDIT.md T1: blocks until `lsn` is durable.
    pub(crate) fn wait_until_durable(&self, lsn: LsnId) {
        self.clock.wait_until_durable(lsn)
    }

    /// Append `op` under the given (already-minted) lsn. The WAL is
    /// append-only: it keeps no in-memory state (phase 3 — versions live in
    /// `VersionStore`).
    pub(crate) fn log(&self, lsn: LsnId, op: Operation) -> Result<(), StoreError> {
        if let Some(tx) = &self.log_tx {
            tx.send(LogMsg::Record(LogRecord { lsn, operation: op }))
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    /// Mint and log in one step, returning the lsn assigned.
    pub(crate) fn log_new(&self, op: Operation) -> Result<LsnId, StoreError> {
        let lsn = self.next_lsn();
        self.log(lsn, op)?;
        Ok(lsn)
    }

    /// Blocks until every record queued before this call is durable.
    pub(crate) fn sync(&self) -> Result<(), StoreError> {
        if let Some(tx) = &self.log_tx {
            let (reply, done) = bounded(1);
            tx.send(LogMsg::Sync(reply))
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            done.recv()
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    /// Phase 6: starts a new segment and deletes every older one whose
    /// highest LSN is below `floor`. Blocks until done.
    pub(crate) fn roll(&self, floor: u64) -> Result<(), StoreError> {
        if let Some(tx) = &self.log_tx {
            let (reply, done) = bounded(1);
            tx.send(LogMsg::Roll { floor, reply })
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            done.recv()
                .map_err(|e| StoreError::UnknownError(e.to_string()))??;
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
/// Everything the runner owns about the segment files.
struct WalState<F: DBFile> {
    file: F,
    name: String,
    current: Segment,
    older: Vec<Segment>,
    header_bytes: Vec<u8>,
    segment_bytes: Arc<AtomicU64>,
    segment_count: Arc<AtomicUsize>,
    current_segment: Arc<AtomicU64>,
    retained_bytes: Arc<AtomicU64>,
}

impl<F: DBFile> WalState<F> {
    fn publish_counts(&self) {
        self.segment_count
            .store(self.older.len() + 1, std::sync::atomic::Ordering::Relaxed);
        self.current_segment
            .store(self.current.n, std::sync::atomic::Ordering::Relaxed);
        let older: u64 = self.older.iter().map(|s| s.bytes).sum();
        self.retained_bytes.store(
            older + self.segment_bytes.load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Phase 6, steps 4 and 5 of `Db::checkpoint`: open `<name>.wal.<n+1>`
    /// with a fresh header and make it current, then delete every older
    /// segment whose highest LSN is below `floor`. The new segment is
    /// durable before the old one stops being current, so a crash between
    /// the two leaves at worst an extra empty segment.
    fn roll(&mut self, floor: u64) -> Result<(), StoreError> {
        let next = Segment {
            n: self.current.n + 1,
            path: segment_path(&self.name, self.current.n + 1),
            max_lsn: 0,
            bytes: self.header_bytes.len() as u64,
        };
        let opts = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .clone();
        let mut f = self.file.open_sibling(&next.path, opts)?;
        f.write_all(&self.header_bytes)?;
        f.do_sync()?;
        let mut previous = std::mem::replace(&mut self.current, next);
        previous.bytes = self.segment_bytes.load(std::sync::atomic::Ordering::Relaxed);
        self.file = f;
        self.older.push(previous);
        self.segment_bytes
            .store(self.current.bytes, std::sync::atomic::Ordering::Relaxed);
        // One rule: a segment goes when nothing at or above the floor is in
        // it. Oldest first, so a failure mid-way leaves a contiguous suffix.
        let mut keep = Vec::with_capacity(self.older.len());
        for seg in self.older.drain(..) {
            if seg.max_lsn < floor {
                self.file.remove_sibling(&seg.path)?;
            } else {
                keep.push(seg);
            }
        }
        self.older = keep;
        self.publish_counts();
        Ok(())
    }
}

fn log_runner<F: DBFile>(
    state: WalState<F>,
    recv: Receiver<LogMsg>,
    clock: Arc<LsnClock>,
) -> Result<(), StoreError> {
    let mut st = state;
    st.publish_counts();
    // STORE_AUDIT.md P10: whether the PREVIOUS batch actually had more than
    // one record in it — i.e. whether there was real concurrent load to
    // batch last time around. `LOG_BATCH_LINGER` exists purely to give
    // concurrent senders a window to join one fsync; paying it after an
    // isolated write (nothing else in flight) is pure added latency with
    // no batching benefit. Once T1 makes `commit()` actually wait on
    // durability, this linger is no longer a hidden cost — it's directly
    // visible as commit latency, so it's worth skipping when there's
    // nothing to gain from it. Starts true (linger on the very first
    // batch): with no history yet, behave exactly as before until there's
    // real evidence one way or the other.
    let mut last_batch_had_concurrency = true;
    loop {
        // Block for the first message, then linger briefly for more to
        // accumulate into the same batch (see LOG_BATCH_LINGER's own
        // comment) — stopping at the first timeout, not retrying the full
        // linger window MAX_LOG_BATCH times, so an isolated single message
        // only ever pays one linger's worth of extra latency. Skipped
        // entirely (a non-blocking try_recv instead) when the previous
        // batch was NOT itself evidence of concurrent load — see
        // last_batch_had_concurrency's own comment above.
        let first = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        let mut batch: Vec<u8> = Vec::new();
        let mut batch_record_count = 0usize;
        let mut highest_lsn: Option<LsnId> = None;
        let mut special: Option<LogMsg> = None;
        let mut pending = Some(first);
        for _ in 0..MAX_LOG_BATCH {
            let msg = match pending.take() {
                Some(m) => m,
                None => {
                    let next = if last_batch_had_concurrency {
                        recv.recv_timeout(LOG_BATCH_LINGER).ok()
                    } else {
                        recv.try_recv().ok()
                    };
                    match next {
                        Some(m) => m,
                        None => break,
                    }
                }
            };
            match msg {
                LogMsg::ShutDown | LogMsg::Sync(_) | LogMsg::Roll { .. } => {
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
                    batch_record_count += 1;
                }
            }
        }
        last_batch_had_concurrency = batch_record_count > 1;
        if !batch.is_empty() {
            st.file.seek(SeekFrom::End(0))?;
            st.file.write_all(&batch)?;
            st.file.do_sync()?;
            st.segment_bytes
                .fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
            // Only after the whole batch is durable — mark_written signals
            // "everything up to this lsn is safe to flush its page", which
            // must not be true before the bytes actually landed.
            if let Some(lsn) = highest_lsn {
                st.current.max_lsn = st.current.max_lsn.max(lsn.0);
                clock.mark_written(lsn);
            }
            st.publish_counts();
        }
        match special {
            Some(LogMsg::ShutDown) => break,
            Some(LogMsg::Sync(reply)) => {
                // Everything queued before this message went out in the
                // batches above, each synced before its LSNs were marked.
                let _ = reply.send(());
            }
            Some(LogMsg::Roll { floor, reply }) => {
                let _ = reply.send(st.roll(floor));
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
    use postcard::{from_bytes, to_allocvec};
    use crate::{
        error::StoreError,
        logger::{Logger, Operation, Record, frame_record},
        memfile::MemFile,
        table::TableIdType,
        tuple::{DBIdType, Tuple},
        txn::TransactionId,
    };

    #[test]
    fn test_operation_round_trip() {
        let txn = TransactionId::from(1);
        let tuple = Tuple::new(5, b"hello");
        let record = Record::new(TableIdType::from(2), tuple, Some(crate::page::PageId::from(9u64)));
        // Tuple's cached serialized_size (#[serde(skip)]) is 0 after any
        // real deserialize; round-trip the freshly-built record once so
        // the "expected" side matches that, instead of comparing a
        // never-deserialized Tuple's populated cache against a decoded
        // one's cleared cache.
        let record: Record = from_bytes(&to_allocvec(&record).unwrap()).unwrap();
        let ops = [
            Operation::Add { txn, post: record.clone() },
            Operation::Mod { txn, pre: record.clone(), post: record.clone() },
            Operation::Del { txn, pre: record.clone() },
            Operation::Commit(txn),
            Operation::Rollback(txn),
            Operation::Sequence { name: "s".to_string(), high_water: 7, dropped: false },
            Operation::Purge { txn, table_id: TableIdType::from(2), key: DBIdType::Int(5) },
        ];
        for op in ops {
            let bytes = to_allocvec(&op).unwrap();
            let back: Operation = from_bytes(&bytes).unwrap();
            assert_eq!(format!("{op:?}"), format!("{back:?}"));
        }
    }

    #[test]
    fn test_operation_unknown_tag_errors() {
        assert!(from_bytes::<Operation>(&[99, 1]).is_err());
    }

    // Fixtures captured from the pre-Stage-1 `#[derive(Serialize,
    // Deserialize)]` encoding (commit bfbc240), before Operation grew a
    // hand-rolled codec. One per variant, since the WAL is crash/replay-
    // critical (T4_S2_WAL_DESIGN.md).
    #[test]
    fn test_operation_decodes_pre_stage1_derived_fixtures() {
        const ADD_BYTES: &[u8] = &[0, 1, 2, 0, 5, 0, 0, 5, 104, 101, 108, 108, 111, 0, 1, 9];
        const MOD_BYTES: &[u8] = &[
            1, 1, 2, 0, 5, 0, 0, 5, 104, 101, 108, 108, 111, 0, 1, 9, 2, 0, 5, 0, 0, 5, 104, 101,
            108, 108, 111, 0, 1, 9,
        ];
        const DEL_BYTES: &[u8] = &[2, 1, 2, 0, 5, 0, 0, 5, 104, 101, 108, 108, 111, 0, 1, 9];
        const COMMIT_BYTES: &[u8] = &[3, 1];
        const ROLLBACK_BYTES: &[u8] = &[4, 1];
        const SEQ_BYTES: &[u8] = &[5, 1, 115, 7, 0];
        const PURGE_BYTES: &[u8] = &[6, 1, 2, 0, 5];

        let txn = TransactionId::from(1);
        let tuple = Tuple::new(5, b"hello");
        let record = Record::new(TableIdType::from(2), tuple, Some(crate::page::PageId::from(9u64)));
        // See test_operation_round_trip: normalize the cached, non-persisted
        // Tuple::serialized_size the same way a real decode does.
        let record: Record = from_bytes(&to_allocvec(&record).unwrap()).unwrap();

        macro_rules! assert_decodes {
            ($bytes:expr, $expected:expr) => {
                assert_eq!(format!("{:?}", from_bytes::<Operation>($bytes).unwrap()), format!("{:?}", $expected));
            };
        }
        assert_decodes!(ADD_BYTES, Operation::Add { txn, post: record.clone() });
        assert_decodes!(MOD_BYTES, Operation::Mod { txn, pre: record.clone(), post: record.clone() });
        assert_decodes!(DEL_BYTES, Operation::Del { txn, pre: record.clone() });
        assert_decodes!(COMMIT_BYTES, Operation::Commit(txn));
        assert_decodes!(ROLLBACK_BYTES, Operation::Rollback(txn));
        assert_decodes!(SEQ_BYTES, Operation::Sequence { name: "s".to_string(), high_water: 7, dropped: false });
        assert_decodes!(PURGE_BYTES, Operation::Purge { txn, table_id: TableIdType::from(2), key: DBIdType::Int(5) });
    }

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
        tuple.set_txn_id(txn_id);
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_new(op).is_ok());
    }

    #[test]
    fn test_logger_with_memfile_shutdown() {
        let mut logger = Logger::new();
        logger.set_db_for_test(MemFile::new()).unwrap();
        let txn_id = TransactionId::from(99);
        let op = Operation::new_commit(txn_id);
        logger.log_new(op).unwrap();
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_logger_undo_add_with_db() {
        let mut logger = Logger::new();
        logger.set_db_for_test(MemFile::new()).unwrap();
        let txn_id = TransactionId::from(5);
        let mut tuple = Tuple::new(42, b"data");
        tuple.set_txn_id(txn_id);
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_new(op).is_ok());
        assert!(logger.shutdown().is_ok());
    }

    // STORE_AUDIT.md P10: the group-commit linger existed purely to give a
    // concurrent sender a window to join the same fsync — paying it after
    // an isolated write (nothing else in flight) is pure added latency
    // with zero batching benefit, and since T1 makes commit() actually
    // wait on durability, this linger is now directly visible as commit
    // latency rather than a hidden background cost.
    #[test]
    fn test_audit_p10_an_isolated_write_skips_the_group_commit_linger() {
        let mut logger = Logger::new();
        logger.set_db_for_test(MemFile::new()).unwrap();

        // First write: log_runner starts assuming concurrency (see
        // last_batch_had_concurrency's own comment in log_runner) so this
        // one still pays the linger — nothing to compare against yet
        // either way, this just establishes "the previous batch had
        // exactly one record" for the second write below.
        let lsn1 = logger
            .log_new(Operation::new_commit(TransactionId::from(1)))
            .unwrap();
        logger.wait_until_durable(lsn1);

        // Second write, with nothing else in flight: since the first batch
        // had exactly one record (no concurrency), this one should skip
        // the linger entirely instead of paying the full
        // super::LOG_BATCH_LINGER unconditionally.
        // Best of several isolated writes: this is a magnitude check, and a
        // single sample on a loaded machine (the whole suite runs in
        // parallel) can be preempted for longer than the linger itself.
        // The property under test — no linger is charged — makes the
        // minimum the meaningful statistic.
        let mut elapsed = std::time::Duration::MAX;
        for i in 0..10u64 {
            let start = std::time::Instant::now();
            let lsn2 = logger
                .log_new(Operation::new_commit(TransactionId::from(2 + i)))
                .unwrap();
            logger.wait_until_durable(lsn2);
            elapsed = elapsed.min(start.elapsed());
        }
        assert!(
            elapsed < super::LOG_BATCH_LINGER,
            "an isolated write (no concurrent sender) should skip the group-commit \
             linger entirely once the previous batch showed no concurrency, not pay \
             the full {:?} every time — took {:?}",
            super::LOG_BATCH_LINGER,
            elapsed
        );
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
            Some(txn),
            Some(pre_lsn),
        );
        assert_eq!(t.txn_id, Some(txn));
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
