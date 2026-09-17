//! Transaction identity and the transaction table.
//!
//! TXN_SIMPLIFICATION_PLAN.md phase 1: a transaction is identified by ONE
//! `u64` drawn from the database's single counter (`LsnClock`) at `begin()`.
//! That number is also its start timestamp: because every LSN is drawn from
//! the same counter, `id < every LSN of this transaction's records < its
//! commit record's LSN` holds by construction. Recovery seeds the counter from
//! `max(header.counter, highest LSN in the log) + 1`, so an id is never
//! reissued to a transaction that left any trace. There is no separate
//! sequence, no wall clock, and no `(id, ts)` pair to reconcile.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{error::StoreError, logger::LsnClock};

/// Transaction identifier — `Copy`, ordered by start time. Freely passable to
/// lower-level operations; does NOT own the transaction lifecycle (see
/// `Transaction` for that).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct TransactionId(pub(crate) u64);

impl TransactionId {
    /// The numeric id — for display and stats.
    pub fn id_num(&self) -> u64 {
        self.0
    }

    /// Start order. Phase 2 replaces every ordering use of this with commit
    /// timestamps; until then, "began earlier" is "has the smaller id".
    pub(crate) fn ts(&self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(id: u64) -> Self {
        Self(id)
    }
}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "txn{}", self.0)
    }
}

/// How a transaction wants a `WriteConflict` (see `crate::error::StoreError`)
/// handled — set once at `begin()` time, similar to a SQL engine's
/// "continue/ignore on error" transaction option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    /// The conflicting operation fails and returns the error, but the
    /// transaction itself stays open and usable for further operations.
    #[default]
    ContinueOnConflict,
    /// The conflicting operation fails AND the entire transaction is
    /// immediately, automatically rolled back. Any further operation against
    /// it returns `StoreError::TransactionAlreadyFinished`; an explicit
    /// `db.rollback` afterward is still a harmless no-op.
    AbortOnConflict,
}

/// TXN_SIMPLIFICATION_PLAN.md phase 2: one entry per transaction the engine
/// still needs to remember. Absence means "committed before every active
/// reader began" — visible to everyone, and there is nothing left to say
/// about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active { policy: ConflictPolicy },
    /// Rolled back or abandoned; its writes are being (or waiting to be)
    /// physically reverted. Invisible, and a conflict for any writer.
    Aborting,
    /// Committed at `commit_ts` (the LSN of its Commit record). Retained
    /// until every active reader began after it (see `prune_committed`).
    Committed { commit_ts: u64 },
}

#[derive(Debug)]
pub(crate) struct TransactionManager {
    clock: Arc<LsnClock>,
    // BTreeMap so the oldest active transaction (the retention horizon) is
    // the first Active key.
    states: RwLock<BTreeMap<TransactionId, TxnState>>,
}

/// What a `Transaction` guard needs from its owner: the one place a commit,
/// a rollback, or an abandoned guard's abort actually happens
/// (TXN_SIMPLIFICATION_PLAN.md phase 3, proposal §3.7). `Db<F>` implements
/// it with the real thing; `TransactionManager` implements a bookkeeping-only
/// version for its own unit tests. Object-safe so the guard stays
/// non-generic.
pub(crate) trait TxnSink: Send + Sync {
    fn commit_id(&self, id: TransactionId) -> Result<(), StoreError>;
    fn abort_id(&self, id: TransactionId) -> Result<(), StoreError>;
}

/// RAII transaction guard.
///
/// Rolls back automatically when dropped if `commit()`/`rollback()` was never
/// called — fully, inline, through the same single abort path an explicit
/// rollback uses; by the time `drop` returns the transaction is gone (or,
/// on an I/O failure, parked for the maintenance thread to retry).
/// Deliberately NOT `Clone` (STORE_AUDIT.md T8).
pub struct Transaction {
    id: Option<TransactionId>,
    sink: Arc<dyn TxnSink>,
}

impl Transaction {
    pub(crate) fn new(id: TransactionId, sink: Arc<dyn TxnSink>) -> Self {
        Self { id: Some(id), sink }
    }

    /// Returns the raw id for passing to lower-level operations.
    pub fn id(&self) -> TransactionId {
        self.id.expect("transaction already finished")
    }

    /// Commits and consumes the guard.
    pub fn commit(mut self) -> Result<(), StoreError> {
        let id = self.id.take().expect("transaction already finished");
        self.sink.commit_id(id)
    }

    /// Explicit rollback — also happens automatically on drop.
    pub fn rollback(mut self) -> Result<(), StoreError> {
        let id = self.id.take().expect("transaction already finished");
        self.sink.abort_id(id)
    }

    /// Detaches the raw id from this guard without finishing the transaction
    /// and without triggering `Drop`'s abort — for `Db::commit`, which must
    /// leave a transaction ACTIVE (invisible) if its own work fails partway.
    pub(crate) fn into_id(mut self) -> TransactionId {
        self.id.take().expect("transaction already finished")
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.sink.abort_id(id);
        }
    }
}

impl TxnSink for TransactionManager {
    fn commit_id(&self, id: TransactionId) -> Result<(), StoreError> {
        let ts = self.clock.next_lsn().0;
        self.commit(id, ts)
    }

    fn abort_id(&self, id: TransactionId) -> Result<(), StoreError> {
        self.abort(id)
    }
}

impl TransactionManager {
    pub(crate) fn new(clock: Arc<LsnClock>) -> Self {
        Self {
            clock,
            states: RwLock::new(BTreeMap::new()),
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.states
            .read()
            .values()
            .filter(|s| matches!(s, TxnState::Active { .. }))
            .count()
    }

    pub(crate) fn aborting_count(&self) -> usize {
        self.states
            .read()
            .values()
            .filter(|s| **s == TxnState::Aborting)
            .count()
    }

    /// Number of committed transactions still remembered (awaiting prune).
    pub(crate) fn committed_retained(&self) -> usize {
        self.states
            .read()
            .values()
            .filter(|s| matches!(s, TxnState::Committed { .. }))
            .count()
    }

    /// Id of the oldest still-active transaction: the retention horizon.
    /// The oldest transaction that is not finished — Active or Aborting.
    /// Phase 6: the WAL retention floor. Every LSN a transaction mints is
    /// above its own id, so nothing at or above this id can be discarded
    /// while it is in flight.
    pub(crate) fn oldest_in_flight(&self) -> Option<u64> {
        self.states
            .read()
            .iter()
            .find(|(_, s)| matches!(s, TxnState::Active { .. } | TxnState::Aborting))
            .map(|(id, _)| id.0)
    }

    pub(crate) fn oldest_active(&self) -> Option<u64> {
        self.states
            .read()
            .iter()
            .find(|(_, s)| matches!(s, TxnState::Active { .. }))
            .map(|(id, _)| id.0)
    }

    pub(crate) fn get_active_transactions(&self) -> Result<HashSet<TransactionId>, StoreError> {
        Ok(self
            .states
            .read()
            .iter()
            .filter_map(|(id, s)| matches!(s, TxnState::Active { .. }).then_some(*id))
            .collect())
    }

    pub(crate) fn create_transaction(
        &self,
        policy: ConflictPolicy,
    ) -> Result<TransactionId, StoreError> {
        let txn = TransactionId(self.clock.next_lsn().0);
        self.states.write().insert(txn, TxnState::Active { policy });
        Ok(txn)
    }

    /// Creates a bookkeeping-only guard (no Db behind it) — for tests of the
    /// manager itself. `Db::begin` builds the real one.
    pub(crate) fn begin(
        self: &Arc<Self>,
        policy: ConflictPolicy,
    ) -> Result<Transaction, StoreError> {
        let id = self.create_transaction(policy)?;
        Ok(Transaction::new(id, Arc::clone(self) as Arc<dyn TxnSink>))
    }

    pub(crate) fn is_transaction_active(&self, txn: &TransactionId) -> bool {
        matches!(self.states.read().get(txn), Some(TxnState::Active { .. }))
    }

    /// The policy `txn` was `begin()`-ed with — `ContinueOnConflict` for an
    /// id this manager has no active record of.
    pub(crate) fn conflict_policy(&self, txn: &TransactionId) -> ConflictPolicy {
        match self.states.read().get(txn) {
            Some(TxnState::Active { policy }) => *policy,
            _ => ConflictPolicy::default(),
        }
    }

    /// Committed at all (regardless of when) — the write path's "is this
    /// version a real committed ancestor" question. Absence means committed
    /// long ago.
    pub(crate) fn is_committed(&self, txn: &TransactionId) -> bool {
        matches!(
            self.states.read().get(txn),
            None | Some(TxnState::Committed { .. })
        )
    }

    /// THE visibility rule (proposal §3.3): a version written by `writer` is
    /// visible to `reader` iff it is the reader's own, or the writer
    /// committed before the reader began — `commit_ts < reader.id`, both
    /// drawn from the one counter. A writer absent from the table committed
    /// before every active reader began.
    pub(crate) fn is_visible(&self, writer: &TransactionId, reader: &TransactionId) -> bool {
        if writer == reader {
            return true;
        }
        match self.states.read().get(writer) {
            None => true,
            Some(TxnState::Committed { commit_ts }) => *commit_ts < reader.0,
            Some(_) => false,
        }
    }

    /// THE write-conflict rule (first-committer-wins snapshot isolation): a
    /// row currently written by `writer` may be overwritten by `me` only if
    /// `writer` is `me`, or `writer` committed before `me` began. Anything
    /// still in flight, aborting, or committed after `me` began is a
    /// conflict — `me` could not have seen that write, so building on it
    /// would silently discard it.
    pub(crate) fn conflicts(&self, writer: &TransactionId, me: &TransactionId) -> bool {
        if writer == me {
            return false;
        }
        match self.states.read().get(writer) {
            None => false,
            Some(TxnState::Committed { commit_ts }) => *commit_ts > me.0,
            Some(_) => true,
        }
    }

    /// Mark committed at `commit_ts` (the Commit record's LSN). The one
    /// atomic commit point every other thread observes.
    pub(crate) fn commit(&self, txn: TransactionId, commit_ts: u64) -> Result<(), StoreError> {
        let mut states = self.states.write();
        match states.get_mut(&txn) {
            Some(state @ TxnState::Active { .. }) => {
                *state = TxnState::Committed { commit_ts };
                Ok(())
            }
            _ => Err(StoreError::TransactionAlreadyFinished),
        }
    }

    /// Forget every committed transaction that no active reader can still
    /// need to distinguish from "committed long ago": one whose commit_ts is
    /// below the oldest active id (or all of them, if nothing is active).
    /// Once forgotten, absence means visible — which is exactly what every
    /// remaining reader would have concluded anyway.
    pub(crate) fn prune_committed(&self) {
        let mut states = self.states.write();
        let horizon = states
            .iter()
            .find(|(_, s)| matches!(s, TxnState::Active { .. }))
            .map(|(id, _)| id.0);
        states.retain(|_, s| match s {
            TxnState::Committed { commit_ts } => match horizon {
                Some(h) => *commit_ts >= h,
                None => false,
            },
            _ => true,
        });
    }

    /// Begin aborting `txn`: flip it to Aborting in place so its writes stay
    /// invisible. The physical revert is done separately by `Db`, which then
    /// calls `abort_complete`. Refuses for anything not currently Active
    /// (STORE_AUDIT.md T8).
    pub(crate) fn abort(&self, txn: TransactionId) -> Result<(), StoreError> {
        let mut states = self.states.write();
        match states.get_mut(&txn) {
            Some(state @ TxnState::Active { .. }) => {
                *state = TxnState::Aborting;
                Ok(())
            }
            _ => Err(StoreError::TransactionAlreadyFinished),
        }
    }

    /// Retire a transaction whose writes were already physically reverted by
    /// a Db-level rollback (revert-while-active).
    pub(crate) fn finish_rolled_back(&self, txn: TransactionId) {
        self.states.write().remove(&txn);
    }

    /// Finish aborting `txn` — call only after its undo log has been fully
    /// replayed.
    pub(crate) fn abort_complete(&self, txn: &TransactionId) {
        self.states.write().remove(txn);
    }

    /// Snapshot of transactions whose undo still needs to be replayed.
    pub(crate) fn aborting_ids(&self) -> Vec<TransactionId> {
        self.states
            .read()
            .iter()
            .filter_map(|(id, s)| (*s == TxnState::Aborting).then_some(*id))
            .collect()
    }

    /// An explicit rollback with no undo replay just parks the txn as
    /// aborting (invisible); Db-level rollback replays the undo itself.
    pub(crate) fn rollback(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.abort(txn)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        error::StoreError,
        logger::LsnClock,
        txn::{ConflictPolicy, TransactionId, TransactionManager},
    };

    fn make_mgr() -> TransactionManager {
        TransactionManager::new(Arc::new(LsnClock::default()))
    }

    fn make_mgr_arc() -> Arc<TransactionManager> {
        Arc::new(make_mgr())
    }

    #[test]
    fn test_create_unique_transactions() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert_ne!(t1, t2);
    }

    #[test]
    fn test_is_transaction_active() {
        let mgr = make_mgr();
        let t = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.is_transaction_active(&t));
        assert!(!mgr.is_transaction_active(&TransactionId::from(99999u64)));
    }

    #[test]
    fn test_commit_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.commit(t, 100).unwrap();
        assert!(!mgr.is_transaction_active(&t));
        assert!(mgr.is_committed(&t));
        assert!(matches!(mgr.commit(t, 101), Err(StoreError::TransactionAlreadyFinished)));
    }

    #[test]
    fn test_rollback_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.rollback(t).unwrap();
        assert!(!mgr.is_transaction_active(&t));
    }

    #[test]
    fn test_active_count_tracks_lifecycle() {
        let mgr = make_mgr();
        assert_eq!(mgr.active_count(), 0);
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert_eq!(mgr.active_count(), 1);
        let _t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert_eq!(mgr.active_count(), 2);
        mgr.commit(t1, 100).unwrap();
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_get_active_transactions_contains_all() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&t1));
        assert!(ids.contains(&t2));
    }

    #[test]
    fn test_committed_not_in_active_list() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.commit(t1, 100).unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert!(!ids.contains(&t1));
        assert!(ids.contains(&t2));
    }

    // ---- phase 2: the two rules and the horizon ----

    #[test]
    fn test_visibility_is_commit_before_reader_began() {
        let clock = Arc::new(LsnClock::default());
        let mgr = TransactionManager::new(clock.clone());
        let w = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let early_reader = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        // In flight: invisible to everyone but itself.
        assert!(!mgr.is_visible(&w, &early_reader));
        assert!(mgr.is_visible(&w, &w));
        let commit_ts = clock.next_lsn().0;
        mgr.commit(w, commit_ts).unwrap();
        let late_reader = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        // Committed after early_reader began, before late_reader began.
        assert!(!mgr.is_visible(&w, &early_reader));
        assert!(mgr.is_visible(&w, &late_reader));
        // Forgotten transactions are visible to everyone.
        assert!(mgr.is_visible(&TransactionId::for_test(0), &early_reader));
        // Aborting: invisible.
        let a = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.abort(a).unwrap();
        assert!(!mgr.is_visible(&a, &late_reader));
    }

    #[test]
    fn test_conflict_is_first_committer_wins() {
        let clock = Arc::new(LsnClock::default());
        let mgr = TransactionManager::new(clock.clone());
        let old = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.commit(old, clock.next_lsn().0).unwrap();
        let me = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let concurrent = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(!mgr.conflicts(&old, &me), "committed before I began: fine");
        assert!(!mgr.conflicts(&me, &me), "my own write: fine");
        assert!(mgr.conflicts(&concurrent, &me), "in flight: conflict");
        mgr.commit(concurrent, clock.next_lsn().0).unwrap();
        assert!(mgr.conflicts(&concurrent, &me), "committed after I began: conflict");
        let aborting = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.abort(aborting).unwrap();
        assert!(mgr.conflicts(&aborting, &me), "aborting, not yet reverted: conflict");
        assert!(!mgr.conflicts(&TransactionId::for_test(0), &me), "forgotten: fine");
    }

    #[test]
    fn test_prune_keeps_commits_a_live_reader_must_not_see() {
        let clock = Arc::new(LsnClock::default());
        let mgr = TransactionManager::new(clock.clone());
        let reader = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let w = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.commit(w, clock.next_lsn().0).unwrap();
        mgr.prune_committed();
        assert_eq!(mgr.committed_retained(), 1, "reader began before w committed");
        assert!(!mgr.is_visible(&w, &reader));
        mgr.commit(reader, clock.next_lsn().0).unwrap();
        mgr.prune_committed();
        assert_eq!(mgr.committed_retained(), 0, "nothing active: everything is forgotten");
        let later = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.is_visible(&w, &later));
    }

    // TXN_SIMPLIFICATION_PLAN.md phase 1: ids come from the one shared
    // counter, so they strictly increase and interleave with LSNs drawn
    // from the same counter.
    #[test]
    fn test_ids_come_from_the_shared_counter_and_strictly_increase() {
        let clock = Arc::new(LsnClock::default());
        let mgr = TransactionManager::new(clock.clone());
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let lsn = clock.next_lsn();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(t1.ts() < lsn.0, "a later LSN must exceed an earlier id");
        assert!(lsn.0 < t2.ts(), "a later id must exceed an earlier LSN");
        assert!(t1 < t2);
    }

    // --- Transaction guard tests ---

    #[test]
    fn test_transaction_drop_rolls_back_automatically() {
        let mgr = make_mgr_arc();
        let id = {
            let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
            let id = txn.id();
            assert!(mgr.is_transaction_active(&id));
            id
        };
        assert!(!mgr.is_transaction_active(&id));
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_transaction_commit_does_not_trigger_rollback() {
        let mgr = make_mgr_arc();
        let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
        let id = txn.id();
        txn.commit().unwrap();
        assert!(!mgr.is_transaction_active(&id));
        assert!(mgr.is_committed(&id));
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_transaction_explicit_rollback_removes_txn() {
        let mgr = make_mgr_arc();
        let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
        let id = txn.id();
        txn.rollback().unwrap();
        assert!(!mgr.is_transaction_active(&id));
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_copying_transaction_id_does_not_trigger_rollback() {
        let mgr = make_mgr_arc();
        let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
        let id = txn.id();
        {
            let _copy = id;
        }
        assert!(mgr.is_transaction_active(&id));
        txn.commit().unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    // Throwaway wall-clock measurement of is_committed's per-call cost under
    // concurrency; `#[ignore]`d.
    #[test]
    #[ignore]
    fn bench_is_committed_concurrent() {
        const THREADS: u64 = 8;
        const ITERS_PER_THREAD: u64 = 500_000;
        const NOISE_TXNS: u64 = 100;
        let mgr = make_mgr_arc();
        let mut held = vec![];
        for i in 0..NOISE_TXNS {
            let t = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
            if i % 2 == 0 {
                let id = t.id();
                t.rollback().unwrap();
                held.push(id);
            } else {
                held.push(t.id());
                std::mem::forget(t);
            }
        }
        let never_registered = TransactionId::for_test(u64::MAX);
        let start = std::time::Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let mgr = mgr.clone();
                let id = never_registered;
                s.spawn(move || {
                    for _ in 0..ITERS_PER_THREAD {
                        assert!(mgr.is_committed(&id));
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        eprintln!(
            "bench_is_committed_concurrent: {THREADS} threads x {ITERS_PER_THREAD} iters in {elapsed:?} ({:.0} ops/s)",
            (THREADS * ITERS_PER_THREAD) as f64 / elapsed.as_secs_f64()
        );
    }
}
