use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{error::StoreError, generator::Generator};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct TransactionId(pub u64);

const TXN_GENERATOR_NANE: &str = "__system.transactions";

#[derive(Debug, Default)]
pub(crate) struct TransactionManager {
    gens: Generator,
    active_transactions: Arc<RwLock<HashMap<TransactionId, u128>>>,
}

impl TransactionManager {
    pub(crate) fn new(gens: Generator, last_id: TransactionId) -> Result<Self, StoreError> {
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        self.active_transactions.write()?.insert(txn, now);
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
