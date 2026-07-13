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

#[derive(Debug)]
pub(crate) struct TransactionData {
    id: TransactionId,
    snapshot: HashSet<u64>,
}

const TXN_GENERATOR_NANE: &str = "__system.transactions";

#[derive(Debug)]
pub(crate) struct TransactionManager {
    gens: Arc<Generator>,
    active_transactions: RwLock<HashSet<TransactionId>>,
    // Aborted transactions whose writes have NOT yet been physically reverted.
    // A transaction is "committed" (and therefore visible) only if it is in
    // NEITHER `active_transactions` NOR here: committing removes it from
    // `active`, so it falls through to visible; aborting moves it here and it
    // stays invisible until `Db` drains it (replays its undo log, then calls
    // abort_complete). This is what keeps a dropped/aborted txn's un-reverted
    // rows from being read as committed — without storing any per-committed-txn
    // record. Both sets are bounded by concurrency / abort backlog, not history.
    aborting_transactions: RwLock<HashSet<TransactionId>>,
    transaction_data: RwLock<HashMap<u64, TransactionData>>,
}

/// RAII transaction guard.
///
/// Rolls back automatically when dropped if `commit()` was never called. Use
/// `id()` to get the raw `TransactionId` for passing to lower-level operations.
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
        Ok(Self {
            gens,
            active_transactions: RwLock::new(HashSet::new()),
            aborting_transactions: RwLock::new(HashSet::new()),
            transaction_data: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active_transactions.read().len()
    }

    pub(crate) fn get_active_transactions(&self) -> Result<HashSet<u64>, StoreError> {
        Ok(self
            .active_transactions
            .read()
            .iter()
            .map(|t| t.0.id)
            .collect::<HashSet<_>>())
    }

    pub(crate) fn create_transaction(&self) -> Result<TransactionId, StoreError> {
        let snapshot = self.get_active_transactions()?;
        let txn = TransactionId::new(self.gens.gen_key(TXN_GENERATOR_NANE)?);
        self.active_transactions.write().insert(txn.clone());
        self.transaction_data.write().insert(
            txn.0.id,
            TransactionData {
                id: txn.clone(),
                snapshot,
            },
        );
        Ok(txn)
    }

    /// Creates a `Transaction` RAII guard that auto-rolls back on drop.
    /// Requires `self: &Arc<Self>` so the guard can hold a reference back to
    /// this manager for its deferred rollback.
    pub(crate) fn begin(self: &Arc<Self>) -> Result<Transaction, StoreError> {
        let id = self.create_transaction()?;
        Ok(Transaction {
            id: Some(id),
            mgr: Arc::clone(self),
        })
    }

    pub(crate) fn is_transaction_active(&self, txn: &TransactionId) -> bool {
        self.active_transactions.read().contains(txn)
    }

    /// A transaction's writes are visible ("committed") only if it is neither
    /// still in flight nor aborting-with-unreverted-writes. Absence from both
    /// sets is the definition of committed — no per-committed-txn record is kept.
    pub(crate) fn is_committed(&self, txn: &TransactionId) -> bool {
        !self.active_transactions.read().contains(txn)
            && !self.aborting_transactions.read().contains(txn)
    }

    pub(crate) fn commit(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
        Ok(())
    }

    /// Begin aborting `txn`: move it out of `active` and into `aborting` so its
    /// writes stay invisible. The physical revert (replaying the undo log) is
    /// done separately by `Db` (which has table access), which then calls
    /// `abort_complete`. Callers without table access (Transaction::drop) can
    /// only get this far; the revert is drained by the next Db operation.
    pub(crate) fn abort(&self, txn: TransactionId) -> Result<(), StoreError> {
        let mut active = self.active_transactions.write();
        let mut aborting = self.aborting_transactions.write();
        active.remove(&txn);
        aborting.insert(txn);
        Ok(())
    }

    /// Retire a transaction whose writes were already physically reverted by a
    /// Db-level rollback (revert-while-active). Set-level bookkeeping is
    /// identical to `commit` — the txn simply stops being tracked — but its
    /// writes were undone rather than published. It never enters `aborting`, so
    /// no drain ever touches it: the owner reverts its own writes with no
    /// cross-thread interference, then calls this.
    pub(crate) fn finish_rolled_back(&self, txn: TransactionId) {
        self.active_transactions.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
    }

    /// Finish aborting `txn` — call only after its undo log has been fully
    /// replayed (all its rows reverted). Now it becomes "committed" by absence,
    /// but no rows carrying its id remain, so nothing reads it as committed.
    pub(crate) fn abort_complete(&self, txn: &TransactionId) {
        self.aborting_transactions.write().remove(txn);
        self.transaction_data.write().remove(&txn.0.id);
    }

    /// Snapshot of transactions whose undo still needs to be replayed.
    pub(crate) fn aborting_ids(&self) -> Vec<TransactionId> {
        self.aborting_transactions.read().iter().cloned().collect()
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
    ) -> Option<MappedRwLockReadGuard<'_, HashSet<u64>>> {
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
    pub fn new(id: u64) -> Self {
        Self(Arc::new(TransactionInner {
            id,
            ts: timestamp(),
        }))
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
        txn::{TransactionId, TransactionManager},
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
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        assert_ne!(t1, t2);
    }

    #[test]
    fn test_is_transaction_active() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(&t));
        assert!(!mgr.is_transaction_active(&TransactionId::from(99999u64)));
    }

    #[test]
    fn test_commit_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.commit(t.clone()).unwrap();
        assert!(!mgr.is_transaction_active(&t));
    }

    #[test]
    fn test_rollback_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.rollback(t.clone()).unwrap();
        assert!(!mgr.is_transaction_active(&t));
    }

    #[test]
    fn test_active_count_tracks_lifecycle() {
        let mgr = make_mgr();
        assert_eq!(mgr.active_count(), 0);
        let t1 = mgr.create_transaction().unwrap();
        assert_eq!(mgr.active_count(), 1);
        let _t2 = mgr.create_transaction().unwrap();
        assert_eq!(mgr.active_count(), 2);
        mgr.commit(t1).unwrap();
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_get_active_transactions_contains_all() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&t1.0.id));
        assert!(ids.contains(&t2.0.id));
    }

    #[test]
    fn test_committed_not_in_active_list() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        mgr.commit(t1.clone()).unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert!(!ids.contains(&t1.0.id));
        assert!(ids.contains(&t2.0.id));
    }

    #[test]
    fn test_txn_id_has_nonzero_timestamp() {
        let t = TransactionId::from(1u64);
        assert!(t.0.ts > 0);
    }

    #[test]
    fn test_txn_new_sets_id() {
        let t = TransactionId::new(5);
        assert_eq!(t.0.id, 5);
    }

    #[test]
    fn test_txn_equality_by_id_ignores_ts() {
        let t1 = TransactionId::new(42);
        let t2 = TransactionId::new(42);
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_txn_first_has_empty_snapshot() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        assert!(mgr.snapshot(&t1).unwrap().is_empty());
    }

    #[test]
    fn test_txn_snapshot_captures_active_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        assert!(mgr.snapshot(&t2).unwrap().contains(&t1.0.id));
        assert!(!mgr.snapshot(&t1).unwrap().contains(&t2.0.id));
    }

    #[test]
    fn test_txn_snapshot_excludes_committed_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        mgr.commit(t1.clone()).unwrap();
        let t2 = mgr.create_transaction().unwrap();
        assert!(!mgr.snapshot(&t2).unwrap().contains(&t1.0.id));
    }

    #[test]
    fn test_txn_from_u64_sets_id() {
        let t = TransactionId::from(77u64);
        assert_eq!(t.0.id, 77);
    }

    // --- Transaction guard tests ---

    #[test]
    fn test_transaction_drop_rolls_back_automatically() {
        let mgr = make_mgr_arc();
        let id = {
            let txn = mgr.begin().unwrap();
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
        let txn = mgr.begin().unwrap();
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
        let txn = mgr.begin().unwrap();
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
        let txn = mgr.begin().unwrap();
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
}
