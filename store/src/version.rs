//! In-memory version store (TXN_SIMPLIFICATION_PLAN.md phase 3, proposal §3.4).
//!
//! The WAL is append-only and is never read at runtime; what readers and
//! rollback need lives here: every `Add`/`Mod`/`Del` a transaction has
//! written, keyed by the LSN its tuple's `pre_lsn` points at, plus the two
//! queues the horizon drains — committed transactions whose records are
//! still needed by some older reader, and committed deletes whose tombstone
//! row still needs physical purging.
//!
//! Retention is one rule (proposal §3.6): a committed transaction's records
//! and tombstones are reclaimable once `commit_ts < H`, where `H` is the
//! oldest active transaction's id. An aborted transaction's records go the
//! moment its revert completes. Nothing else decides when to forget.

use std::sync::Mutex;

use crate::{
    logger::{LsnId, Operation},
    table::TableIdType,
    tuple::DBIdType,
    txn::TransactionId,
    utils::shardedmap::ShardedMap,
};

/// A committed delete awaiting physical purge.
#[derive(Debug, Clone)]
pub(crate) struct Tombstone {
    pub(crate) commit_ts: u64,
    pub(crate) table_id: TableIdType,
    pub(crate) key: DBIdType,
    pub(crate) txn: TransactionId,
}

#[derive(Debug, Default)]
pub(crate) struct VersionStore {
    records: ShardedMap<LsnId, Operation>,
    by_txn: ShardedMap<TransactionId, Vec<LsnId>>,
    /// Committed transactions not yet forgotten: `(commit_ts, txn)`.
    committed: Mutex<Vec<(u64, TransactionId)>>,
    tombstones: Mutex<Vec<Tombstone>>,
}

/// What one vacuum pass reclaimed / handed back.
#[derive(Debug, Default)]
pub(crate) struct Vacuumed {
    pub(crate) transactions_forgotten: usize,
    pub(crate) records_discarded: usize,
    pub(crate) tombstones: Vec<Tombstone>,
}

impl VersionStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record a write. Only `Add`/`Mod`/`Del` carry versions.
    pub(crate) fn insert(&self, lsn: LsnId, op: Operation) {
        let txn = match &op {
            Operation::Add { txn, .. } | Operation::Mod { txn, .. } | Operation::Del { txn, .. } => {
                *txn
            }
            _ => return,
        };
        self.records.insert(lsn, op);
        self.by_txn.with_entry_or_default(txn, |v| v.push(lsn));
    }

    /// The record a tuple's `pre_lsn` points at.
    pub(crate) fn find(&self, lsn: LsnId) -> Option<Operation> {
        self.records.get(&lsn)
    }

    /// A transaction's writes in log order. Empty for a transaction that
    /// never wrote (read-only), which is not an error.
    pub(crate) fn ops_of(&self, txn: &TransactionId) -> Vec<Operation> {
        self.by_txn
            .get(txn)
            .map(|lsns| lsns.iter().filter_map(|l| self.records.get(l)).collect())
            .unwrap_or_default()
    }

    /// Forget a transaction's records outright (aborted and reverted).
    pub(crate) fn discard(&self, txn: &TransactionId) -> usize {
        let mut n = 0;
        if let Some(lsns) = self.by_txn.remove(txn) {
            for lsn in lsns {
                if self.records.remove(&lsn).is_some() {
                    n += 1;
                }
            }
        }
        n
    }

    /// The transaction committed at `commit_ts`: keep its records until the
    /// horizon passes, and queue its deletes for purging then.
    pub(crate) fn mark_committed(&self, txn: TransactionId, commit_ts: u64) {
        let mut tombstones = Vec::new();
        for op in self.ops_of(&txn) {
            if let Operation::Del { pre, .. } = op {
                tombstones.push(Tombstone {
                    commit_ts,
                    table_id: pre.table_id,
                    key: pre.tuple.id.clone(),
                    txn,
                });
            }
        }
        if self.by_txn.get(&txn).is_some() {
            self.committed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((commit_ts, txn));
        }
        if !tombstones.is_empty() {
            self.tombstones
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(tombstones);
        }
    }

    /// Apply the retention rule. `horizon` is the oldest active id, or
    /// `None` when nothing is active (everything committed is reclaimable).
    /// Returns the tombstones the caller must now purge physically.
    pub(crate) fn vacuum(&self, horizon: Option<u64>) -> Vacuumed {
        let reclaimable = |commit_ts: u64| match horizon {
            Some(h) => commit_ts < h,
            None => true,
        };
        let mut out = Vacuumed::default();
        {
            let mut committed = self.committed.lock().unwrap_or_else(|e| e.into_inner());
            let (done, keep): (Vec<_>, Vec<_>) =
                committed.drain(..).partition(|(ts, _)| reclaimable(*ts));
            *committed = keep;
            for (_, txn) in done {
                out.records_discarded += self.discard(&txn);
                out.transactions_forgotten += 1;
            }
        }
        {
            let mut tombstones = self.tombstones.lock().unwrap_or_else(|e| e.into_inner());
            let (done, keep): (Vec<_>, Vec<_>) = tombstones
                .drain(..)
                .partition(|t| reclaimable(t.commit_ts));
            *tombstones = keep;
            out.tombstones = done;
        }
        out
    }

    /// Put tombstones back that could not be purged this pass (e.g. the
    /// table was busy); they are retried next pass.
    pub(crate) fn requeue_tombstones(&self, ts: Vec<Tombstone>) {
        if !ts.is_empty() {
            self.tombstones
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(ts);
        }
    }

    pub(crate) fn records_len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn committed_pending(&self) -> usize {
        self.committed.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub(crate) fn tombstones_pending(&self) -> usize {
        self.tombstones.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{logger::Record, tuple::Tuple};

    fn add(txn: u64, key: u64) -> Operation {
        Operation::Add {
            txn: TransactionId::from(txn),
            post: Record::new(1.into(), Tuple::new(key, b"v"), None),
        }
    }

    fn del(txn: u64, key: u64) -> Operation {
        Operation::Del {
            txn: TransactionId::from(txn),
            pre: Record::new(1.into(), Tuple::new(key, b"v"), None),
        }
    }

    #[test]
    fn test_records_are_retained_until_the_horizon_passes_the_commit() {
        let vs = VersionStore::new();
        vs.insert(LsnId(10), add(5, 1));
        vs.insert(LsnId(11), del(5, 2));
        assert_eq!(vs.ops_of(&TransactionId::from(5u64)).len(), 2);
        vs.mark_committed(TransactionId::from(5u64), 12);
        // A reader with id 8 began before the commit: keep everything.
        let v = vs.vacuum(Some(8));
        assert_eq!(v.transactions_forgotten, 0);
        assert!(v.tombstones.is_empty());
        assert_eq!(vs.records_len(), 2);
        // Oldest active began after the commit: reclaim.
        let v = vs.vacuum(Some(20));
        assert_eq!(v.transactions_forgotten, 1);
        assert_eq!(v.records_discarded, 2);
        assert_eq!(v.tombstones.len(), 1);
        assert_eq!(v.tombstones[0].key, DBIdType::Int(2));
        assert_eq!(vs.records_len(), 0);
    }

    #[test]
    fn test_no_active_transaction_means_everything_is_reclaimable() {
        let vs = VersionStore::new();
        vs.insert(LsnId(10), add(5, 1));
        vs.mark_committed(TransactionId::from(5u64), 12);
        let v = vs.vacuum(None);
        assert_eq!(v.transactions_forgotten, 1);
        assert_eq!(vs.records_len(), 0);
    }

    #[test]
    fn test_discard_drops_an_aborted_transactions_records_immediately() {
        let vs = VersionStore::new();
        vs.insert(LsnId(10), add(5, 1));
        vs.insert(LsnId(11), add(6, 2));
        assert_eq!(vs.discard(&TransactionId::from(5u64)), 1);
        assert!(vs.find(LsnId(10)).is_none());
        assert!(vs.find(LsnId(11)).is_some());
    }
}
