use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use crate::{constant::timestamp, error::StoreError, generator::Generator};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TransactionId {
    id: u64,
    ts: u128,
}

const TXN_GENERATOR_NANE: &str = "__system.transactions";

#[derive(Debug, Default)]
pub(crate) struct TransactionManager {
    gens: Arc<Generator>,
    active_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    snapshots: RwLock<HashMap<u64, Vec<u64>>>,
}

impl TransactionManager {
    pub(crate) fn new(gens: Arc<Generator>, last_id: TransactionId) -> Result<Self, StoreError> {
        gens.create_generator(TXN_GENERATOR_NANE, Some(last_id.id))?;
        Ok(Self {
            gens,
            ..Default::default()
        })
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active_transactions.read().len()
    }

    pub(crate) fn get_active_transactions(&self) -> Result<Vec<u64>, StoreError> {
        Ok(self
            .active_transactions
            .read()
            .iter()
            .map(|t| t.id)
            .collect::<Vec<_>>())
    }

    pub(crate) fn create_transaction(&self) -> Result<TransactionId, StoreError> {
        let snapshot = self.get_active_transactions()?;
        let txn = TransactionId::new(self.gens.gen_key(TXN_GENERATOR_NANE)?);
        self.active_transactions.write().insert(txn);
        self.snapshots.write().insert(txn.id, snapshot);
        Ok(txn)
    }

    pub(crate) fn is_transaction_active(&self, txn: &TransactionId) -> bool {
        self.active_transactions.read().contains(txn)
    }

    pub(crate) fn commit(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write().remove(&txn);
        self.snapshots.write().remove(&txn.id);
        Ok(())
    }

    pub(crate) fn rollback(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write().remove(&txn);
        self.snapshots.write().remove(&txn.id);
        Ok(())
    }

    pub(crate) fn snapshots(&self) -> RwLockReadGuard<'_, HashMap<u64, Vec<u64>>> {
        self.snapshots.read()
    }

    pub(crate) fn snapshot(
        &self,
        id: TransactionId,
    ) -> Option<MappedRwLockReadGuard<'_, Vec<u64>>> {
        if !self.snapshots.read().contains_key(&id.id) {
            None
        } else {
            Some(RwLockReadGuard::map(self.snapshots.read(), |m| {
                m.get(&id.id).unwrap()
            }))
        }
    }
}

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ts: timestamp(),
        }
    }
}

impl PartialEq for TransactionId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for TransactionId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self {
            id: 0,
            ts: timestamp(),
        }
    }
}

impl Eq for TransactionId {}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        Self {
            id: value,
            ts: timestamp(),
        }
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
        mgr.commit(t).unwrap();
        assert!(!mgr.is_transaction_active(&t));
    }

    #[test]
    fn test_rollback_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(&t));
        mgr.rollback(t).unwrap();
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
        //let ids: Vec<_> = active.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&t1.id));
        assert!(ids.contains(&t2.id));
    }

    #[test]
    fn test_committed_not_in_active_list() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        mgr.commit(t1).unwrap();
        let ids = mgr.get_active_transactions().unwrap();
        assert!(!ids.contains(&t1.id));
        assert!(ids.contains(&t2.id));
    }

    // --- TransactionId struct tests ---

    #[test]
    fn test_txn_id_has_nonzero_timestamp() {
        let t = TransactionId::from(1u64);
        assert!(t.ts > 0);
    }

    #[test]
    fn test_txn_new_sets_id() {
        let t = TransactionId::new(5);
        assert_eq!(t.id, 5);
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
        assert!(mgr.snapshots().get(&t1.id).unwrap().is_empty());
    }

    #[test]
    fn test_txn_snapshot_captures_active_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        let snapshots = mgr.snapshots();
        // t2 was created while t1 was active, so t2's snapshot must include t1.id
        assert!(snapshots.get(&t2.id).unwrap().contains(&t1.id));
        // t1 was created first with no active txns
        assert!(!snapshots.get(&t1.id).unwrap().contains(&t2.id));
    }

    #[test]
    fn test_txn_snapshot_excludes_committed_txns() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        mgr.commit(t1).unwrap();
        // t2 created after t1 committed — snapshot should not include t1
        let t2 = mgr.create_transaction().unwrap();
        assert!(!mgr.snapshots().get(&t2.id).unwrap().contains(&t1.id));
    }

    #[test]
    fn test_txn_from_u64_sets_id() {
        let t = TransactionId::from(77u64);
        assert_eq!(t.id, 77);
    }
}
