use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, atomic::AtomicU64},
};

use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use crate::{constant::timestamp, error::StoreError, generator::Generator};

/// Raw transaction identifier — freely cloneable and passable to lower-level
/// operations (BPlusTree, page writes, etc.). Does NOT own the transaction
/// lifecycle; use `Transaction` for that.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionId(Arc<TransactionInner>);

#[allow(dead_code)]
enum TxMsg {
    Shutdown,
    Rollback(u64),
    Commit(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInner {
    id: u64,
    ts: u128,
}

/// How a transaction wants a `WriteConflict` (see `crate::error::StoreError`)
/// handled — set once at `begin()` time, similar to a SQL engine's
/// "continue/ignore on error" transaction option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    /// The conflicting operation fails and returns the error, but the
    /// transaction itself stays open and usable for further operations —
    /// this was the only behavior before ConflictPolicy existed, and
    /// remains the default so existing callers see no change.
    #[default]
    ContinueOnConflict,
    /// The conflicting operation fails AND the entire transaction is
    /// immediately, automatically rolled back — matching a SQL engine's
    /// `ignore_errors = false` behavior. The transaction is fully finished
    /// by the time the failing call returns; any further operation against
    /// it (insert/update/remove/commit) returns
    /// `StoreError::TransactionAlreadyFinished`. An explicit `db.rollback`
    /// afterward is still safe (a harmless no-op), matching how ROLLBACK
    /// on an already-aborted transaction behaves in most SQL engines.
    AbortOnConflict,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionData {
    id: TransactionId,
    snapshot: HashSet<TransactionId>,
    policy: ConflictPolicy,
}

const TXN_GENERATOR_NANE: &str = "__system.transactions";
// STORE_AUDIT.md T11: a separate named sequence (not reusing
// TXN_GENERATOR_NANE's own counter) for the ts ordering primitive —
// see TransactionId::ts's own doc comment for why this exists and what
// it replaced. Kept as its own Generator entry rather than a bespoke
// AtomicU64 field so it rides along for free on the exact same
// persistence path (write_system_tables/load_system_tables, generic
// over every named generator) the numeric id sequence already uses.
const TXN_TS_GENERATOR_NAME: &str = "__system.transactions.ts";

// STORE_AUDIT.md P7 (second half): a transaction is either still in flight
// (Active) or aborted-but-not-yet-physically-reverted (Aborting) — those
// used to be two separate `RwLock<HashSet<TransactionId>>`s, so
// `is_committed` (called on every undo-chain hop `resolve_visible` walks)
// took two independent lock reads to answer one question. Collapsed into
// one `RwLock<HashMap<TransactionId, TxnState>>`: presence/absence alone
// still says "not committed"/"committed" (no per-committed-txn record is
// kept, matching the old design's footprint), but now via a single lock
// acquisition and lookup instead of two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Aborting,
}

#[derive(Debug)]
pub(crate) struct TransactionManager {
    gens: Arc<Generator>,
    // A transaction is "committed" (and therefore visible) only if it is
    // absent from this map entirely: committing removes it, so it falls
    // through to visible; aborting flips its entry to Aborting in place
    // (still present, still invisible) until `Db` drains it (replays its
    // undo log, then calls abort_complete, which removes it). This is what
    // keeps a dropped/aborted txn's un-reverted rows from being read as
    // committed — without storing any per-committed-txn record. Bounded by
    // concurrency / abort backlog, not history.
    transaction_states: RwLock<HashMap<TransactionId, TxnState>>,
    transaction_data: RwLock<HashMap<u64, TransactionData>>,
}

/// RAII transaction guard.
///
/// Rolls back automatically when dropped if `commit()` was never called. Use
/// `id()` to get the raw `TransactionId` for passing to lower-level operations.
///
/// Deliberately NOT `Clone` (see STORE_AUDIT.md T8): a clone dropped after
/// the original committed used to trigger Drop's default rollback,
/// silently reverting a committed write. `TransactionId` (via `id()`)
/// stays freely cloneable for that purpose — it's a bare identifier with
/// no ownership over the transaction's lifecycle, so cloning and dropping
/// it has no effect on anything.
pub struct Transaction {
    id: Option<TransactionId>,
    mgr: Arc<TransactionManager>,
}

impl Transaction {
    /// Returns the raw id for passing to lower-level operations.
    pub fn id(&self) -> TransactionId {
        self.id
            .as_ref()
            .expect("transaction already finished")
            .clone()
    }

    /// Commits and consumes the guard. `Drop` fires at end of this fn but sees
    /// `id = None` so no rollback occurs.
    pub fn commit(mut self) -> Result<(), StoreError> {
        let id = self.id.take().expect("transaction already finished");
        self.mgr.commit(id)
    }

    /// Explicit rollback — also happens automatically on drop.
    pub fn rollback(mut self) -> Result<(), StoreError> {
        let id = self.id.take().expect("transaction already finished");
        self.mgr.rollback(id)
    }

    /// Detaches the raw id from this guard without marking the transaction
    /// committed or rolled back, and without triggering `Drop`'s auto-rollback.
    ///
    /// `Db::commit`/`Db::rollback` need this: they do their own post-processing
    /// (replaying undo records, cleaning up tombstones) after taking ownership
    /// of the guard, and that work can fail partway through (e.g. on
    /// `LockContentionError`). If `Drop` ran its default rollback in that case,
    /// the transaction would be marked inactive — and therefore "committed" as
    /// far as `find_last_committed` is concerned — even though its data was
    /// never actually committed or undone. Detaching up front means an early
    /// return on failure just leaves the transaction active (and so correctly
    /// invisible to readers) instead of silently mislabeling it.
    pub(crate) fn into_id(mut self) -> TransactionId {
        self.id.take().expect("transaction already finished")
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.mgr.rollback(id);
        }
    }
}

static TX_COUNTER: AtomicU64 = AtomicU64::new(0);

impl TransactionManager {
    pub(crate) fn new(gens: Arc<Generator>, last_id: TransactionId) -> Result<Self, StoreError> {
        gens.create_generator(TXN_GENERATOR_NANE, Some(last_id.0.id))?;
        // STORE_AUDIT.md T11: starts at 1, not 0 — 0 is Default's/for_test's
        // usual placeholder value elsewhere, and this way a fresh db's very
        // first ts() is never ambiguous with "never initialized". Reopening
        // an existing db restores the real persisted value over this via
        // load_system_tables's generic Generator::set_values (same path
        // TXN_GENERATOR_NANE's own value takes) — see advance_ts_past for
        // the additional log-scan-based reconciliation that also runs on
        // open, closing the gap for anything logged since the last
        // checkpoint.
        gens.create_generator(TXN_TS_GENERATOR_NAME, Some(1))?;
        Ok(Self {
            gens,
            transaction_states: RwLock::new(HashMap::new()),
            transaction_data: RwLock::new(HashMap::new()),
        })
    }

    // STORE_AUDIT.md T11: reconciles this Db's ts sequence against the
    // highest ts value Db::process_log observed while scanning the log
    // (every TransactionId embedded in a redo/undo record or a Commit/
    // Rollback marker) — called once, during replay. See `TransactionId::
    // ts`'s own doc comment for why the persisted generator value alone
    // (restored by load_system_tables, stale as of the last checkpoint)
    // isn't enough on its own. Never lowers the sequence, only raises it
    // if the log revealed a higher value than whatever was persisted.
    pub(crate) fn advance_ts_past(&self, max_seen: u128) -> Result<(), StoreError> {
        let min_next = max_seen.saturating_add(1).min(u64::MAX as u128) as u64;
        self.gens.advance_past(TXN_TS_GENERATOR_NAME, min_next)
    }

    pub(crate) fn active_count(&self) -> usize {
        self.transaction_states
            .read()
            .values()
            .filter(|s| **s == TxnState::Active)
            .count()
    }

    pub(crate) fn get_active_transactions(&self) -> Result<HashSet<TransactionId>, StoreError> {
        Ok(self
            .transaction_states
            .read()
            .iter()
            .filter_map(|(id, s)| (*s == TxnState::Active).then(|| id.clone()))
            .collect())
    }

    pub(crate) fn create_transaction(
        &self,
        policy: ConflictPolicy,
    ) -> Result<TransactionId, StoreError> {
        // The numeric id can be generated outside the lock below (its own
        // uniqueness comes from the generator, independent of lock
        // ordering) — but ts() must NOT be: see the comment on the block
        // below for why it's stamped inside the same critical section as
        // snapshot capture/registration, not here.
        let id = self.gens.gen_key(TXN_GENERATOR_NANE)?;
        // Assigning ts(), capturing the snapshot, and registering as active
        // must all happen under ONE lock acquisition, not stamped/read/
        // written separately, or two transactions beginning at nearly the
        // same moment can end up with a snapshot inconsistent with their
        // own ts() ordering — two distinct bugs that both manifested as
        // the same symptom (confirmed via direct repro: two threads racing
        // an update to the same row, synchronized with a Barrier, both
        // returning Ok):
        //   1. Read-then-insert as two separate lock acquisitions left a
        //      gap where a second transaction beginning in between could
        //      read the same "before I was added" snapshot as the first —
        //      each absent from the other's snapshot.
        //   2. Even after fixing (1), stamping ts() used to run BEFORE
        //      this lock was acquired — so which thread's write-lock
        //      acquisition actually went first (and therefore whose
        //      snapshot saw whom) could disagree with which thread's
        //      ts() was numerically smaller, since OS scheduling can
        //      reorder "read the ts source" independently of "acquire
        //      the lock". That breaks check_write_conflict's fallback
        //      `writer.ts() >= txn.ts()` test — it needs ts() ordering to
        //      always agree with snapshot-registration ordering, which is
        //      only guaranteed if both happen under the same lock.
        // STORE_AUDIT.md T11: minting ts() here via gen_key (a per-Db
        // logical counter) rather than the old timestamp() (wall-clock)
        // additionally guarantees ts() itself can never go backward
        // between two calls, regardless of what the OS clock does — the
        // ordering bug the two numbered points above describe was about
        // ts() disagreeing with LOCK ordering; a non-monotonic wall clock
        // could ALSO make ts() disagree with TRUE chronological order
        // even with perfect lock discipline. Both are closed the same
        // way: ts() is now just "the next value of a sequence this exact
        // critical section hands out one at a time", so it's monotonic
        // with respect to real time by construction, not by assumption
        // about the clock.
        let (txn, snapshot) = {
            let mut states = self.transaction_states.write();
            let ts = self.gens.gen_key(TXN_TS_GENERATOR_NAME)? as u128;
            let txn = TransactionId::new(id, ts);
            // Filtered to Active only, matching the old two-separate-sets
            // design exactly (the snapshot was always built from
            // active_transactions alone, never aborting_transactions).
            // Doesn't actually change find_visible_to's behavior either
            // way: an Aborting txn is never committed, so its predicate's
            // `is_committed(txn) && ...` already short-circuits before
            // `reader_snapshot.contains(txn)` is ever consulted for it.
            let snapshot = states
                .iter()
                .filter_map(|(id, s)| (*s == TxnState::Active).then(|| id.clone()))
                .collect::<HashSet<_>>();
            states.insert(txn.clone(), TxnState::Active);
            (txn, snapshot)
        };
        self.transaction_data.write().insert(
            txn.0.id,
            TransactionData {
                id: txn.clone(),
                snapshot,
                policy,
            },
        );
        Ok(txn)
    }

    /// Creates a `Transaction` RAII guard that auto-rolls back on drop.
    /// Requires `self: &Arc<Self>` so the guard can hold a reference back to
    /// this manager for its deferred rollback.
    pub(crate) fn begin(
        self: &Arc<Self>,
        policy: ConflictPolicy,
    ) -> Result<Transaction, StoreError> {
        let id = self.create_transaction(policy)?;
        Ok(Transaction {
            id: Some(id),
            mgr: Arc::clone(self),
        })
    }

    pub(crate) fn is_transaction_active(&self, txn: &TransactionId) -> bool {
        matches!(self.transaction_states.read().get(txn), Some(TxnState::Active))
    }

    /// The policy `txn` was `begin()`-ed with — defaults to
    /// `ContinueOnConflict` for an id this manager has no record of (e.g.
    /// a synthetic id built for tests via `TransactionId::for_test`, or a
    /// transaction that's already fully finished), matching the
    /// pre-ConflictPolicy behavior rather than surprising an unrelated
    /// caller with an abort they never asked for.
    pub(crate) fn conflict_policy(&self, txn: &TransactionId) -> ConflictPolicy {
        self.transaction_data
            .read()
            .get(&txn.0.id)
            .map(|d| d.policy)
            .unwrap_or_default()
    }

    /// A transaction's writes are visible ("committed") only if it is neither
    /// still in flight nor aborting-with-unreverted-writes. Absence from
    /// `transaction_states` is the definition of committed — no
    /// per-committed-txn record is kept. STORE_AUDIT.md P7 (second half):
    /// one lock read now, not two — Active and Aborting used to live in
    /// separate sets, so this needed two independent RwLock acquisitions
    /// to answer what's now a single map lookup.
    pub(crate) fn is_committed(&self, txn: &TransactionId) -> bool {
        !self.transaction_states.read().contains_key(txn)
    }

    pub(crate) fn commit(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.transaction_states.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
        Ok(())
    }

    /// Begin aborting `txn`: move it out of `active` and into `aborting` so its
    /// writes stay invisible. The physical revert (replaying the undo log) is
    /// done separately by `Db` (which has table access), which then calls
    /// `abort_complete`. Callers without table access (Transaction::drop) can
    /// only get this far; the revert is drained by the next Db operation.
    // STORE_AUDIT.md T8 (second half): refuses to move `txn` into
    // `aborting` unless it's currently `active`. Independent hardening
    // alongside removing `Clone` from `Transaction` — this is the
    // invariant that actually matters (an already-finished transaction
    // must never be re-processed as if it were still live), and it also
    // guards the AbortOnConflict path, which relies on "moving an id into
    // aborting twice is harmless" reasoning that only holds while nothing
    // else has already finished it.
    pub(crate) fn abort(&self, txn: TransactionId) -> Result<(), StoreError> {
        // One write-lock acquisition now instead of two (see
        // transaction_states' own doc comment) — this also closes a narrow
        // window the old two-set design had: between removing `txn` from
        // `active` and inserting it into `aborting`, a concurrent
        // is_committed(txn) (itself two separate reads) could observe it
        // absent from BOTH sets and momentarily misreport it as committed.
        // Flipping the state in place, under one lock, makes that
        // in-between state unobservable.
        let mut states = self.transaction_states.write();
        match states.get_mut(&txn) {
            Some(state @ TxnState::Active) => {
                *state = TxnState::Aborting;
                Ok(())
            }
            _ => Err(StoreError::TransactionAlreadyFinished),
        }
    }

    /// Retire a transaction whose writes were already physically reverted by a
    /// Db-level rollback (revert-while-active). Set-level bookkeeping is
    /// identical to `commit` — the txn simply stops being tracked — but its
    /// writes were undone rather than published. It never enters `aborting`, so
    /// no drain ever touches it: the owner reverts its own writes with no
    /// cross-thread interference, then calls this.
    pub(crate) fn finish_rolled_back(&self, txn: TransactionId) {
        self.transaction_states.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
    }

    /// Finish aborting `txn` — call only after its undo log has been fully
    /// replayed (all its rows reverted). Now it becomes "committed" by absence,
    /// but no rows carrying its id remain, so nothing reads it as committed.
    pub(crate) fn abort_complete(&self, txn: &TransactionId) {
        self.transaction_states.write().remove(txn);
        self.transaction_data.write().remove(&txn.0.id);
    }

    /// Snapshot of transactions whose undo still needs to be replayed.
    pub(crate) fn aborting_ids(&self) -> Vec<TransactionId> {
        self.transaction_states
            .read()
            .iter()
            .filter_map(|(id, s)| (*s == TxnState::Aborting).then(|| id.clone()))
            .collect()
    }

    /// Back-compat shim: an explicit rollback with no undo replay just parks the
    /// txn as aborting (invisible). Kept so `Transaction::rollback`/`drop` never
    /// mislabel a txn as committed. Db-level rollback replays the undo itself.
    pub(crate) fn rollback(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.abort(txn)
    }

    pub(crate) fn snapshots(&self) -> RwLockReadGuard<'_, HashMap<u64, TransactionData>> {
        self.transaction_data.read()
    }

    pub(crate) fn snapshot(
        &self,
        id: &TransactionId,
    ) -> Option<MappedRwLockReadGuard<'_, HashSet<TransactionId>>> {
        if !self.transaction_data.read().contains_key(&id.0.id) {
            None
        } else {
            Some(RwLockReadGuard::map(self.transaction_data.read(), |m| {
                &m.get(&id.0.id).unwrap().snapshot
            }))
        }
    }
}

impl TransactionId {
    // STORE_AUDIT.md T11: takes `ts` explicitly rather than minting it
    // internally (it used to call `timestamp()` here) — the one real
    // caller, TransactionManager::create_transaction, must mint it from
    // its own per-Db logical sequence, under the exact lock that also
    // orders snapshot registration (see that method's own comment for
    // why). Every OTHER caller of this type (Default, From<u64>, tests)
    // has no such sequence to draw from and isn't part of the real
    // ordering-sensitive path, so they keep using `timestamp()` as a
    // placeholder — fine for identity/uniqueness purposes, just never
    // used for the isolation/conflict-detection ordering `ts()` backs.
    pub fn new(id: u64, ts: u128) -> Self {
        Self(Arc::new(TransactionInner { id, ts }))
    }

    // Test-only: builds a TransactionId with an explicit, caller-chosen
    // `ts` — needed to deterministically construct two distinct
    // transactions with a colliding (or specifically ordered) `ts` for
    // testing check_write_conflict/find_visible_to's own comparison
    // logic in isolation. Identical to `new` now that `new` also takes an
    // explicit `ts` (STORE_AUDIT.md T11) — kept as a separate, `#[cfg(test)]`
    // name since callers reach for it specifically to signal "this is a
    // synthetic id built to pin down comparison logic, not a real
    // transaction", not because the implementation differs.
    #[cfg(test)]
    pub(crate) fn for_test(id: u64, ts: u128) -> Self {
        Self(Arc::new(TransactionInner { id, ts }))
    }

    /// The transaction's ordering primitive — used to decide which of two
    /// transactions began first (`check_write_conflict`'s `writer.ts() >=
    /// txn.ts()`, `find_visible_to`'s `txn.ts() < reader_ts`).
    ///
    /// STORE_AUDIT.md T11: this used to be real wall-clock time
    /// (`SystemTime::now()`, at construction). `SystemTime` is not
    /// monotonic — an NTP step, VM migration pause, or manual clock change
    /// can make it run backward — which could make a transaction that
    /// truly began LATER get a SMALLER `ts()` than one that began earlier,
    /// silently breaking both checks above (a reader could see a writer's
    /// row that hadn't logically happened yet from the reader's own
    /// perspective, or a write conflict could go undetected). Replaced
    /// with a per-Db logical counter (`TransactionManager::
    /// create_transaction` mints it via `Generator::gen_key` on a
    /// dedicated sequence) — monotonic by construction, immune to
    /// anything the OS clock does. Deliberately still not the numeric
    /// `id`: the id generator's own persisted sequence isn't guaranteed to
    /// have caught up to the true high-water mark right after a reopen
    /// (only refreshed at checkpoint/close/table-creation, not on every
    /// transaction), so comparing raw ids across a reopen boundary can be
    /// wrong. `ts`'s own sequence has the same staleness risk on its
    /// persisted value alone, which is why `Db::process_log` additionally
    /// reconciles it against every ts seen while replaying the log (see
    /// `TransactionManager::advance_ts_past`) — together, "persisted value
    /// as of the last checkpoint" and "everything logged since" span the
    /// full history, so a transaction from a prior session is always
    /// ordered before any transaction in a later one, regardless of what
    /// either generator happens to resume from.
    pub(crate) fn ts(&self) -> u128 {
        self.0.ts
    }
}

impl TransactionInner {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ts: timestamp(),
        }
    }
}

// Compares both id and ts, not just id: the numeric id alone is only
// unique within a single continuously-running session (the generator that
// hands them out never repeats a value while it's live). On reopen after a
// crash or a checkpoint-but-no-close, the persisted id sequence it resumes
// from can be stale (it's only refreshed by write_system_tables, called at
// table creation and at close()), so a freshly begun transaction can be
// handed a numeric id that an old, already-committed transaction also
// used. If equality only checked id, that old transaction would become
// indistinguishable from the new one to anything keying off it — notably
// TransactionManager::is_committed's active-set lookup, which would then
// treat the old, legitimately-committed transaction's rows as "still in
// flight" (invisible) for as long as the new, colliding transaction stays
// active. ts is set once at creation (timestamp()) and never changes for a
// given transaction, including across all of its Arc-shared clones and a
// faithful deserialize-from-log round trip during replay, so two
// transactions that are actually the same always still compare equal —
// this only ever makes two *different* transactions compare unequal,
// which is what should have been happening all along.
impl PartialEq for TransactionInner {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.ts == other.ts
    }
}

impl Hash for TransactionInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.ts.hash(state);
    }
}

impl Default for TransactionInner {
    fn default() -> Self {
        Self {
            id: 0,
            ts: timestamp(),
        }
    }
}

impl Eq for TransactionInner {}

// Delegates to TransactionInner's own (id, ts) comparison — see its doc
// comment for why id alone isn't enough.
impl PartialEq for TransactionId {
    fn eq(&self, other: &Self) -> bool {
        *self.0 == *other.0
    }
}

impl Hash for TransactionId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (*self.0).hash(state)
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self(Arc::new(0.into()))
    }
}

impl Eq for TransactionId {}

impl From<u64> for TransactionInner {
    fn from(value: u64) -> Self {
        Self {
            id: value,
            ts: timestamp(),
        }
    }
}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        Self(Arc::new(value.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        generator::Generator,
        txn::{ConflictPolicy, TransactionId, TransactionInner, TransactionManager},
    };

    fn make_mgr() -> TransactionManager {
        let gens = Arc::new(Generator::new());
        TransactionManager::new(gens, TransactionId::default()).unwrap()
    }

    fn make_mgr_arc() -> Arc<TransactionManager> {
        let gens = Arc::new(Generator::new());
        Arc::new(TransactionManager::new(gens, TransactionId::default()).unwrap())
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
        mgr.commit(t.clone()).unwrap();
        assert!(!mgr.is_transaction_active(&t));
    }

    #[test]
    fn test_rollback_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.rollback(t.clone()).unwrap();
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
        mgr.commit(t1).unwrap();
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
        mgr.commit(t1.clone()).unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert!(!ids.contains(&t1));
        assert!(ids.contains(&t2));
    }

    #[test]
    fn test_txn_id_has_nonzero_timestamp() {
        let t = TransactionId::from(1u64);
        assert!(t.0.ts > 0);
    }

    #[test]
    fn test_txn_new_sets_id() {
        let t = TransactionId::new(5, 1);
        assert_eq!(t.0.id, 5);
    }

    #[test]
    fn test_txn_equality_requires_matching_id_and_ts() {
        // See TransactionInner's PartialEq doc comment: equality checks both
        // id and ts, not just id, so two separately-constructed ids sharing
        // a numeric id are NOT the same transaction unless they also share a
        // ts (e.g. via clone()). Builds ts directly (rather than via
        // TransactionId::new, which mints a fresh timestamp()) so the two
        // "different transaction" instances below are deterministically
        // guaranteed to differ, not dependent on two timestamp() calls
        // landing on different nanoseconds.
        let t1 = TransactionId(Arc::new(TransactionInner { id: 42, ts: 1 }));
        let t2 = t1.clone();
        assert_eq!(t1, t2);

        let t3 = TransactionId(Arc::new(TransactionInner { id: 42, ts: 2 }));
        assert_ne!(
            t1, t3,
            "same numeric id but a different ts must not compare equal"
        );
    }

    #[test]
    fn test_txn_first_has_empty_snapshot() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.snapshot(&t1).unwrap().is_empty());
    }

    #[test]
    fn test_txn_snapshot_captures_active_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(mgr.snapshot(&t2).unwrap().contains(&t1));
        assert!(!mgr.snapshot(&t1).unwrap().contains(&t2));
    }

    #[test]
    fn test_txn_snapshot_excludes_committed_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        mgr.commit(t1.clone()).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert!(!mgr.snapshot(&t2).unwrap().contains(&t1));
    }

    #[test]
    fn test_txn_from_u64_sets_id() {
        let t = TransactionId::from(77u64);
        assert_eq!(t.0.id, 77);
    }

    // STORE_AUDIT.md T11: ts() is the ordering primitive check_write_conflict
    // and find_visible_to use to decide which of two transactions began
    // first. Before this fix it was minted from SystemTime::now() — not
    // monotonic (NTP steps, VM migration, manual changes), so two
    // transactions created in true chronological order could still get
    // ts() values in the WRONG order, silently breaking both checks. A
    // real backward clock step can't be safely or portably forced inside
    // a unit test (there's no clock-injection seam, and actually moving
    // the OS clock would be flaky and would affect the whole test
    // process) — but the underlying defect is directly, deterministically
    // observable without one: consecutive REAL ts() values minted via
    // create_transaction must differ by exactly 1 once they come from a
    // per-Db logical counter. A wall-clock source can never guarantee
    // that (the gap between two SystemTime::now() calls is whatever the
    // OS clock and surrounding code happen to take — routinely thousands
    // of nanoseconds, never reliably exactly 1), so this is a direct,
    // reliable proxy for "still wall-clock-derived" vs. "a true logical
    // sequence" — and incidentally proves monotonicity is now guaranteed
    // by construction (a counter can't go backward) rather than by
    // assumption about the OS clock.
    #[test]
    fn test_successive_transaction_timestamps_are_a_monotonic_counter_not_wall_clock_deltas() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t2 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        let t3 = mgr.create_transaction(ConflictPolicy::ContinueOnConflict).unwrap();
        assert_eq!(
            t2.ts(),
            t1.ts() + 1,
            "ts() must advance by exactly one logical tick per transaction, not by an \
             arbitrary wall-clock delta"
        );
        assert_eq!(t3.ts(), t2.ts() + 1);
    }

    // --- Transaction guard tests ---

    #[test]
    fn test_transaction_drop_rolls_back_automatically() {
        let mgr = make_mgr_arc();
        let id = {
            let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
            let id = txn.id();
            assert!(
                mgr.is_transaction_active(&id),
                "must be active while guard lives"
            );
            id
            // txn dropped here without commit → rollback fires
        };
        assert!(
            !mgr.is_transaction_active(&id),
            "must be inactive after guard dropped without commit"
        );
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_transaction_commit_does_not_trigger_rollback() {
        let mgr = make_mgr_arc();
        let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
        let id = txn.id();
        txn.commit().unwrap();
        // If Drop had mis-fired a rollback after commit, the txn would still be
        // gone (rollback is idempotent here), but active_count must be 0 either way.
        assert!(!mgr.is_transaction_active(&id));
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
    fn test_cloning_transaction_id_does_not_trigger_rollback() {
        // Cloning the raw TransactionId (e.g. to pass to insert/find) must not
        // cause rollback when the clone is dropped.
        let mgr = make_mgr_arc();
        let txn = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
        let id = txn.id(); // this clone is dropped at end of block below
        {
            let _clone = id.clone(); // simulate passing id to a method
        } // clone dropped here — must NOT roll back
        assert!(
            mgr.is_transaction_active(&id),
            "dropping a cloned TransactionId must not roll back the transaction"
        );
        txn.commit().unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    // STORE_AUDIT.md P7 (second half) — throwaway (not a committed criterion
    // bench, same call as this session's other direct microbenchmarks)
    // measurement of is_committed's per-call cost under concurrency. Old
    // code took two separate RwLock reads (one over active_transactions,
    // one over aborting_transactions); new code takes one (over the merged
    // transaction_states map). `#[ignore]`d since it's a raw wall-clock
    // loop, not something that needs to run on every `cargo test`. Run the
    // identical test text against this revision and against
    // `git show <pre-fix commit>:store/src/txn.rs` (patched with this same
    // fn) to get a before/after comparison — see BASELINE.md.
    #[test]
    #[ignore]
    fn bench_is_committed_concurrent() {
        const THREADS: u64 = 8;
        const ITERS_PER_THREAD: u64 = 500_000;
        const NOISE_TXNS: u64 = 100;

        let mgr = make_mgr_arc();
        // Populate transaction_states with a realistic mix of Active and
        // Aborting entries so the map/lookup isn't trivially empty.
        let mut held = vec![];
        for i in 0..NOISE_TXNS {
            let t = mgr.begin(ConflictPolicy::ContinueOnConflict).unwrap();
            if i % 2 == 0 {
                let id = t.id();
                t.rollback().unwrap();
                held.push(id); // kept aborting (never drained via abort_complete)
            } else {
                held.push(t.id());
                std::mem::forget(t); // stays Active
            }
        }
        // A synthetic id never registered at all — is_committed's most common
        // real-world case (querying an arbitrary writer id found in a tuple).
        let never_registered = TransactionId::for_test(u64::MAX, u128::MAX);

        let start = std::time::Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let mgr = mgr.clone();
                let id = never_registered.clone();
                s.spawn(move || {
                    for _ in 0..ITERS_PER_THREAD {
                        assert!(mgr.is_committed(&id));
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        eprintln!(
            "bench_is_committed_concurrent: {THREADS} threads x {ITERS_PER_THREAD} \
             iters in {elapsed:?} ({:.0} ops/s)",
            (THREADS * ITERS_PER_THREAD) as f64 / elapsed.as_secs_f64()
        );
    }
}
