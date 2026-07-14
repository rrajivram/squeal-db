use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    sync::{Arc, atomic::AtomicU64},
    thread::{self, JoinHandle},
    time::Duration,
};

use crossbeam::channel::{Receiver, Sender, bounded};
use log::error;
use parking_lot::RwLock;
use postcard::to_allocvec;
use serde::{Deserialize, Serialize};

use crate::{
    constant::timestamp, db::DBFile, error::StoreError, page::PageId, table::TableIdType,
    tuple::Tuple, txn::TransactionId,
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
}

impl Default for LsnClock {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
            last_written: AtomicU64::new(u64::MAX),
        }
    }
}

impl LsnClock {
    /// Allocate the next redo LSN.
    pub(crate) fn next_lsn(&self) -> LsnId {
        LsnId(
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel),
        )
    }

    /// The current flush watermark (highest durably-written redo LSN).
    pub(crate) fn last_written(&self) -> LsnId {
        LsnId(self.last_written.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Advance the watermark as the redo runner persists records.
    pub(crate) fn mark_written(&self, lsn: LsnId) {
        self.last_written
            .store(lsn.0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Ensure `next_lsn()` never mints a value <= `lsn`. Used by replay
    /// (process_redo) once it has scanned the prior session's redo log and
    /// knows the highest lsn it used: a freshly reopened `Db`'s `LsnClock`
    /// otherwise starts `counter` back at 0 (see `Default`), so the very
    /// first new write after reopen would reuse an already-used lsn — and
    /// once that write's own redo record durably lands, `mark_written`
    /// would stamp the watermark with that low, reused value, regressing it
    /// right back down from whatever replay just (correctly) set it to.
    /// `fetch_max` (not a plain store) so this is safe to call even if
    /// something has already advanced the counter past `lsn` by the time
    /// this runs.
    pub(crate) fn advance_counter_past(&self, lsn: LsnId) {
        self.counter
            .fetch_max(lsn.0 + 1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct LsnId(pub(crate) u64);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Hash, Serialize, Deserialize)]
pub struct UndoId(pub(crate) u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum MsgType {
    Undo(UndoOperation),
    Redo(RedoOperation),
    ShutDown,
    Checkpoint(u128),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct RedoOperation {
    pub(crate) lsn_id: LsnId,
    pub(crate) operation: Operation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct UndoOperation {
    pub(crate) undo_id: Option<UndoId>,
    pub(crate) operation: Operation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Operation {
    Add(TransactionId, Record),
    Del(TransactionId, Record),
    Mod(TransactionId, Record),
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

#[derive(Debug, Default)]
pub(crate) struct Logger {
    redo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_handle: Option<JoinHandle<Result<(), StoreError>>>,
    undo_tx: Option<Sender<MsgType>>,
    redo_tx: Option<Sender<MsgType>>,
    undo_txns: RwLock<HashMap<TransactionId, Vec<Operation>>>,
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
            }),
            ..Default::default()
        }
    }

    pub(crate) fn set_db(
        &mut self,
        undo_file: impl DBFile + 'static,
        redo_file: impl DBFile + 'static,
    ) -> Result<(), StoreError> {
        // Wide enough that group commit (see undo_log_runner/redo_log_runner)
        // has something real to batch under concurrent load, instead of
        // bounded(1)'s "at most one message ever queued" — which structurally
        // prevented batching, since a sender blocks until the runner
        // dequeues the previous message before a second one can even land in
        // the channel. This does trade away bounded(1)'s "near-synchronous"
        // property (log_redo/log_undo returning was previously a rough proxy
        // for "the previous record is durable") — tests that need an actual
        // durability guarantee use the explicit wait_for_durable_logs poll
        // helper instead of relying on that timing coincidence.
        const LOG_CHANNEL_CAPACITY: usize = 256;
        let (redo_tx, redo_rx) = bounded(LOG_CHANNEL_CAPACITY);
        let (undo_tx, undo_rx) = bounded(LOG_CHANNEL_CAPACITY);
        self.undo_tx = Some(undo_tx);
        self.redo_tx = Some(redo_tx);

        let clock = self.clock.clone();
        self.undo_handle = Some(thread::spawn(move || undo_log_runner(undo_file, undo_rx)));
        self.redo_handle = Some(thread::spawn(move || {
            redo_log_runner(redo_file, redo_rx, clock)
        }));
        Ok(())
    }

    /// Shared handle to this database's LSN clock, for the `PageBuffer` (and its
    /// writer thread) that stamp/compare page LSNs against the same watermark.
    pub(crate) fn clock(&self) -> Arc<LsnClock> {
        self.clock.clone()
    }

    pub(crate) fn log_undo(&self, op: Operation) -> Result<(), StoreError> {
        let msg = match &op {
            // Indexed by the operation's own txn (not record.tuple.txn_id):
            // a Mod/Del undo record deliberately carries the *pre-image* tuple
            // (with the previous, already-committed owner's txn_id) so that
            // rollback can restore it and MVCC reads can walk the chain to a
            // committed ancestor. That pre-image's txn_id is unrelated to which
            // transaction's undo log this entry belongs to.
            Operation::Add(txn, _) | Operation::Del(txn, _) | Operation::Mod(txn, _) => {
                let mut undo_txns = self.undo_txns.write();
                let id = undo_txns
                    .entry(txn.clone())
                    .and_modify(|v| v.push(op.clone()))
                    .or_insert(vec![op.clone()]);
                MsgType::Undo(UndoOperation {
                    undo_id: Some(UndoId(id.len() as u16)),
                    operation: op.clone(),
                })
            }
            _ => MsgType::Undo(UndoOperation {
                undo_id: None,
                operation: op.clone(),
            }),
        };
        if let Some(tx) = &self.undo_tx {
            tx.send(msg)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
            // Now store record if available
        }
        // Rollback physically reverts the transaction's writes before this
        // op is even logged (see Db::rollback) — nothing is ever left
        // "owned" by a rolled-back transaction for a concurrent reader to
        // need to walk back through, so its undo trail is always safe to
        // drop immediately, unlike Commit's (see discard_or_defer_undo).
        if let Operation::Rollback(id, _) = &op {
            self.undo_txns.write().remove(id);
        }
        Ok(())
    }

    /// Drop a transaction's in-memory undo records. Called after its undo has
    /// been fully replayed (abort reclamation) — a dropped/aborted txn logs no
    /// Commit/Rollback op, so its records aren't cleaned by log_undo above.
    pub(crate) fn discard_undo(&self, id: &TransactionId) {
        self.undo_txns.write().remove(id);
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
            self.undo_txns.write().remove(&id);
        } else {
            self.pending_undo_discards.write().push((id, others));
        }
    }

    /// Opportunistic maintenance for deferred undo discards (see
    /// discard_or_defer_undo) — called alongside drain_aborting, e.g. at
    /// Db::begin(). For each committed transaction whose discard was
    /// deferred, drops it from the waiter set any transaction that has
    /// since finished (committed or aborted, so it's no longer in
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
                self.undo_txns.write().remove(&id);
            } else {
                still_pending.push((id, remaining));
            }
        }
        *pending = still_pending;
    }

    pub(crate) fn next_undo_id(&self, id: TransactionId) -> Result<UndoId, StoreError> {
        Ok(self
            .undo_txns
            .read()
            .get(&id)
            .map(|m| m.len().into())
            .unwrap_or(0.into()))
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
        Ok(self.undo_txns.read().get(&id).cloned().unwrap_or_default())
    }

    pub(crate) fn find_undo_tuple(&self, id: TransactionId, undo_id: UndoId) -> Option<Tuple> {
        let map = self.undo_txns.read();

        let o = map.get(&id).and_then(|v| v.get(undo_id.0 as usize));
        if let Some(op) = o {
            let tuple = match op {
                Operation::Add(_, r) | Operation::Del(_, r) | Operation::Mod(_, r) => {
                    Some(r.tuple.clone())
                }
                _ => None,
            };
            return tuple;
        }
        None
    }

    pub(crate) fn log_redo(&self, op: Operation) -> Result<LsnId, StoreError> {
        let lsn_id = self.clock.next_lsn();
        let msg = MsgType::Redo(RedoOperation {
            lsn_id,
            operation: op,
        });

        if let Some(tx) = &self.redo_tx {
            tx.send(msg)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(lsn_id)
    }

    pub(crate) fn checkpoint(&self, ts: u128) -> Result<(), StoreError> {
        let msg = MsgType::Checkpoint(ts);
        if let Some(tx) = &self.redo_tx {
            tx.send(msg.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(tx) = &self.undo_tx {
            tx.send(msg.clone())
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn shutdown(self) -> Result<(), StoreError> {
        if let Some(tx) = self.redo_tx {
            tx.send(MsgType::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(tx) = self.undo_tx {
            tx.send(MsgType::ShutDown)
                .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        }
        if let Some(h) = self.redo_handle {
            match h.join() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining redo.Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }
        if let Some(h) = self.undo_handle {
            match h.join() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Unknown error joining undo.Thread panic! {}",
                        e.downcast::<String>().unwrap_or_default()
                    );
                }
            }
        }

        Ok(())
    }
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
    pub(crate) fn new_add(txn: TransactionId, record: Record) -> Self {
        Self::Add(txn, record)
    }

    pub(crate) fn new_del(txn: TransactionId, record: Record) -> Self {
        Self::Del(txn, record)
    }

    pub(crate) fn new_mod(txn: TransactionId, record: Record) -> Self {
        Self::Mod(txn, record)
    }

    pub(crate) fn new_commit(tx_id: TransactionId) -> Self {
        Self::Commit(tx_id, timestamp())
    }

    pub(crate) fn new_rollback(tx_id: TransactionId) -> Self {
        Self::Rollback(tx_id, timestamp())
    }
}

impl MsgType {
    pub(crate) fn new_undo(operation: UndoOperation) -> Self {
        Self::Undo(operation)
    }

    pub(crate) fn new_redo(operation: RedoOperation) -> Self {
        Self::Redo(operation)
    }
}

impl PartialOrd<u64> for LsnId {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(other))
    }
}

impl PartialEq<u64> for LsnId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
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

fn undo_log_runner(file: impl DBFile, recv: Receiver<MsgType>) -> Result<(), StoreError> {
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
        let mut special: Option<MsgType> = None;
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
                MsgType::ShutDown | MsgType::Checkpoint(_) => {
                    // Stop batching here — flush what's accumulated so far
                    // (preserving order: everything queued strictly before
                    // this message lands on disk first), then handle this
                    // message on its own once the batch write below runs.
                    special = Some(msg);
                    break;
                }
                MsgType::Undo(undo_op) => {
                    // Write the full MsgType wrapper, not just the inner
                    // UndoOperation: load_logs (and any future recovery
                    // replay) needs a uniform, self-describing tag on every
                    // record so it can tell Undo/Redo/Checkpoint entries
                    // apart while scanning the file.
                    batch.extend_from_slice(&to_allocvec(&MsgType::Undo(undo_op))?);
                }
                MsgType::Redo(r) => panic!("Unexpected redo in undo loop {:?}", r),
            }
        }
        if !batch.is_empty() {
            file.seek(SeekFrom::End(0))?;
            file.write_all(&batch)?;
            file.do_sync()?;
        }
        match special {
            Some(MsgType::ShutDown) => break,
            Some(MsgType::Checkpoint(_ts)) => file.truncate()?,
            _ => {}
        }
    }
    Ok(())
}

fn redo_log_runner(
    file: impl DBFile,
    recv: Receiver<MsgType>,
    clock: Arc<LsnClock>,
) -> Result<(), StoreError> {
    let mut file = file;
    loop {
        // See undo_log_runner's matching comment on LOG_BATCH_LINGER.
        let first = recv
            .recv()
            .map_err(|e| StoreError::UnknownError(e.to_string()))?;
        let mut batch: Vec<u8> = Vec::new();
        let mut highest_lsn: Option<LsnId> = None;
        let mut special: Option<MsgType> = None;
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
                MsgType::ShutDown | MsgType::Checkpoint(_) => {
                    special = Some(msg);
                    break;
                }
                MsgType::Redo(redo_op) => {
                    // See undo_log_runner's matching comment: write the full
                    // MsgType wrapper, not just the inner RedoOperation.
                    highest_lsn = Some(match highest_lsn {
                        Some(l) if l.0 >= redo_op.lsn_id.0 => l,
                        _ => redo_op.lsn_id,
                    });
                    batch.extend_from_slice(&to_allocvec(&MsgType::Redo(redo_op))?);
                }
                MsgType::Undo(u) => panic!("Unexpected undo in redo loop {:?}", u),
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
            Some(MsgType::ShutDown) => break,
            Some(MsgType::Checkpoint(_ts)) => file.truncate()?,
            _ => {}
        }
    }
    Ok(())
}

impl From<usize> for UndoId {
    fn from(value: usize) -> Self {
        Self(value as u16)
    }
}

#[cfg(test)]
mod tests {

    use super::UndoId;
    use crate::{
        logger::{Logger, Operation, Record},
        memfile::MemFile,
        tuple::Tuple,
        txn::TransactionId,
    };

    #[test]
    fn test_log_redo_returns_incrementing_lsn() {
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        let lsn1 = logger.log_redo(op.clone()).unwrap();
        let lsn2 = logger.log_redo(op).unwrap();
        assert!(lsn2 > lsn1);
    }

    #[test]
    fn test_log_redo_unique_lsns() {
        let logger = Logger::new();
        let mut lsns = Vec::new();
        for i in 0..5 {
            let op = Operation::new_commit(TransactionId::from(i));
            lsns.push(logger.log_redo(op).unwrap());
        }
        let unique: std::collections::HashSet<_> = lsns.iter().cloned().collect();
        assert_eq!(unique.len(), 5);
    }

    #[test]
    fn test_log_undo_commit_op_no_db() {
        // Without set_db, log_undo should succeed (undo_tx is None, msg is silently dropped)
        let logger = Logger::new();
        let op = Operation::new_commit(TransactionId::from(1));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_rollback_op_no_db() {
        let logger = Logger::new();
        let op = Operation::new_rollback(TransactionId::from(2));
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_add_op_with_txn_id() {
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let mut tuple = Tuple::new(1, b"hello");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
    }

    #[test]
    fn test_log_undo_indexes_by_operation_txn_not_tuple_txn_id() {
        // log_undo indexes by the Operation's own txn id, not record.tuple.txn_id.
        // This matters because Mod/Del undo records deliberately carry a
        // pre-image tuple tagged with a *different* (older, committed) txn_id
        // than the operation being logged. A tuple with no txn_id at all (as
        // here) must therefore still log successfully.
        let logger = Logger::new();
        let txn_id = TransactionId::from(10);
        let tuple = Tuple::new(1, b"hello"); // txn_id not set
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id.clone(), record);
        assert!(logger.log_undo(op).is_ok());
        assert_eq!(logger.next_undo_id(txn_id).unwrap(), 1.into());
    }

    #[test]
    fn test_logger_with_memfile_shutdown() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId::from(99);
        let op = Operation::new_commit(txn_id);
        logger.log_redo(op.clone()).unwrap();
        logger.log_undo(op).unwrap();
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_logger_undo_add_with_db() {
        let mut logger = Logger::new();
        logger.set_db(MemFile::new(), MemFile::new()).unwrap();
        let txn_id = TransactionId::from(5);
        let mut tuple = Tuple::new(42, b"data");
        tuple.set_txn_id(txn_id.clone());
        let record = Record::new(0.into(), tuple, None);
        let op = Operation::new_add(txn_id, record);
        assert!(logger.log_undo(op).is_ok());
        assert!(logger.shutdown().is_ok());
    }

    #[test]
    fn test_tuple_new_in_txn() {
        use crate::tuple::Tuple;
        // UndoId is only constructable from within logger module
        let txn = TransactionId::from(42);
        let undo = UndoId(7);
        let t = Tuple::new_with(
            crate::tuple::DBIdType::Int(1),
            b"hello",
            Some(txn.clone()),
            Some(undo),
        );
        assert_eq!(t.txn_id, Some(txn.clone()));
        assert_eq!(t.undo_id, Some(undo));
        assert_eq!(t.data.to_vec(), b"hello");
        let b = t.to();
        let t2 = Tuple::from(&b).unwrap();
        assert_eq!(t2.txn_id, Some(txn));
        assert_eq!(t2.data.to_vec(), b"hello");
    }

    // --- crash-recovery log loading (mmap-backed File, and MemFile) ---

    fn temp_log_paths(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        (
            dir.join(format!("squeal_logger_test_{tag}_{pid}_redo.log")),
            dir.join(format!("squeal_logger_test_{tag}_{pid}_undo.log")),
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
}
