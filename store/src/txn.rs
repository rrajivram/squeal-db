use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::{error::StoreError, generator::Generator};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct TransactionId(u64);

const TXN_GENERATOR_NANE: &str = "__system.transactions";

#[derive(Debug, Default)]
pub(crate) struct TransactionManager {
    gens: Generator,
    active_transactions: Arc<RwLock<HashSet<TransactionId>>>,
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

    pub(crate) fn create_transaction(&self) -> Result<TransactionId, StoreError> {
        let txn = TransactionId(self.gens.gen_key(TXN_GENERATOR_NANE)?);
        self.active_transactions.write()?.insert(txn);
        Ok(txn)
    }

    pub(crate) fn is_transaction_active(&self, txn: TransactionId) -> bool {
        self.active_transactions.read().ok().unwrap().contains(&txn)
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
