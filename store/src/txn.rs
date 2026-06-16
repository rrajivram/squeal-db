use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::{constant::timestamp, error::StoreError, generator::Generator};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, Default)]
pub struct TransactionId(pub u64);

const TXN_GENERATOR_NANE: &str = "__system.transactions";

#[derive(Debug, Default)]
pub(crate) struct TransactionManager {
    gens: Arc<Generator>,
    active_transactions: Arc<RwLock<HashMap<TransactionId, u128>>>,
}

impl TransactionManager {
    pub(crate) fn new(gens: Arc<Generator>, last_id: TransactionId) -> Result<Self, StoreError> {
        gens.create_generator(TXN_GENERATOR_NANE, Some(last_id.0))?;
        Ok(Self {
            gens,
            ..Default::default()
        })
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active_transactions.read().ok().unwrap().len()
    }

    pub(crate) fn get_active_transactions(&self) -> Result<Vec<(TransactionId, u128)>, StoreError> {
        Ok(self
            .active_transactions
            .read()?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>())
    }

    pub(crate) fn create_transaction(&self) -> Result<TransactionId, StoreError> {
        let txn = TransactionId(self.gens.gen_key(TXN_GENERATOR_NANE)?);
        // Safe to unwrap here as call only fails if earlies is less than self.
        self.active_transactions.write()?.insert(txn, timestamp());
        Ok(txn)
    }

    pub(crate) fn is_transaction_active(&self, txn: TransactionId) -> bool {
        self.active_transactions
            .read()
            .ok()
            .unwrap()
            .contains_key(&txn)
    }

    pub(crate) fn commit(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write()?.remove(&txn);
        Ok(())
    }

    pub(crate) fn rollback(&self, txn: TransactionId) -> Result<(), StoreError> {
        self.active_transactions.write()?.remove(&txn);
        Ok(())
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
        assert!(mgr.is_transaction_active(t));
        assert!(!mgr.is_transaction_active(TransactionId(99999)));
    }

    #[test]
    fn test_commit_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(t));
        mgr.commit(t).unwrap();
        assert!(!mgr.is_transaction_active(t));
    }

    #[test]
    fn test_rollback_removes_transaction() {
        let mgr = make_mgr();
        let t = mgr.create_transaction().unwrap();
        assert!(mgr.is_transaction_active(t));
        mgr.rollback(t).unwrap();
        assert!(!mgr.is_transaction_active(t));
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
        let active = mgr.get_active_transactions().unwrap();
        assert_eq!(active.len(), 2);
        let ids: Vec<_> = active.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&t1));
        assert!(ids.contains(&t2));
    }

    #[test]
    fn test_committed_not_in_active_list() {
        let mgr = make_mgr();
        let t1 = mgr.create_transaction().unwrap();
        let t2 = mgr.create_transaction().unwrap();
        mgr.commit(t1).unwrap();
        let active = mgr.get_active_transactions().unwrap();
        let ids: Vec<_> = active.iter().map(|(id, _)| *id).collect();
        assert!(!ids.contains(&t1));
        assert!(ids.contains(&t2));
    }
}
