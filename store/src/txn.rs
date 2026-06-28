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

    pub(crate) fn commit(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
        Ok(())
    }

    pub(crate) fn rollback(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write().remove(&txn);
        self.transaction_data.write().remove(&txn.0.id);
        Ok(())
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

impl PartialEq for TransactionInner {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for TransactionInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
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

impl PartialEq for TransactionId {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}

impl Hash for TransactionId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
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
