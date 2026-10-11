//! In-memory version store (TXN_SIMPLIFICATION_PLAN.md phase 3, proposal §3.4).
//!
//! The WAL is append-only and is never read at runtime; what readers and
//! rollback need lives here, and only that:
//!
//! - **Pre-images**, keyed by the LSN a tuple's `pre_lsn` points at: the
//!   version an update or delete replaced, which an older reader walks back
//!   to (and a rollback restores). Only updates and deletes have one.
//! - **Each transaction's writes**, in order, for its rollback: an insert as
//!   its table and key (a rollback removes the row; no reader ever walks
//!   back to "before an insert", whose tuple has no `pre_lsn`), an update or
//!   delete as the LSN of its pre-image.
//! - The two queues the horizon drains: committed transactions whose
//!   pre-images some older reader may still need, and committed deletes
//!   whose tombstone row still needs physical purging.
//!
//! What it does NOT keep is any written row: an insert's or update's new
//! version is on its page (and in the WAL). Keeping a copy of every written
//! row here was ~450 bytes a row while its transaction ran — a million-row
//! load held half a gigabyte, and tripped the version-record cap.
//!
//! Retention is one rule (proposal §3.6): a committed transaction's records
//! and tombstones are reclaimable once `commit_ts < H`, where `H` is the
//! oldest active transaction's id. An aborted transaction's records go the
//! moment its revert completes. Nothing else decides when to forget.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    logger::{LsnId, Operation},
    table::TableIdType,
    tuple::{DBIdType, Tuple},
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

/// What rolling back one write does.
#[derive(Debug, Clone)]
pub(crate) enum Undo {
    /// An insert: remove the row.
    Remove { table_id: TableIdType, key: DBIdType },
    /// An update or delete: put back the version it replaced.
    Restore { table_id: TableIdType, pre: Tuple },
}

// The version an update or delete replaced.
#[derive(Debug)]
struct PreImage {
    table_id: TableIdType,
    tuple: Tuple,
    // Replaced by a delete: once its transaction commits, the row is a
    // tombstone to purge.
    deleted: bool,
}

// One write, as its transaction's rollback needs it.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum Written {
    Inserted { table_id: TableIdType, key: DBIdType },
    // The LSN its pre-image is kept under.
    Replaced(LsnId),
}

// A transaction's writes, encoded one after another in one buffer (postcard,
// as the WAL encodes them): ~15 bytes an insert, where a Vec of `Written`
// was ~170 (the entry, and its key's own allocation). Read back only to
// roll back or commit. A million-row insert held 170 MB in the Vec.
#[derive(Debug, Default)]
struct WriteLog {
    bytes: Vec<u8>,
    count: usize,
}

impl WriteLog {
    fn push(&mut self, w: &Written) {
        let mut out = std::mem::take(&mut self.bytes);
        out = postcard::to_extend(w, out).expect("a write encodes");
        self.bytes = out;
        self.count += 1;
    }

    fn iter(&self) -> impl Iterator<Item = Written> + '_ {
        let mut rest: &[u8] = &self.bytes;
        std::iter::from_fn(move || {
            if rest.is_empty() {
                return None;
            }
            let (w, tail) = postcard::take_from_bytes::<Written>(rest).expect("a write decodes");
            rest = tail;
            Some(w)
        })
    }

    // Keeps only the writes `keep` says to.
    fn retain(&mut self, keep: impl Fn(&Written) -> bool) {
        let kept: Vec<Written> = self.iter().filter(|w| keep(w)).collect();
        *self = WriteLog::default();
        for w in &kept {
            self.push(w);
        }
        self.bytes.shrink_to_fit();
    }
}

// Maps are shrunk when vacuum leaves them a quarter full — but not small
// ones, which would only regrow on the next write.
const SHRINK_ABOVE: usize = 4096;

#[derive(Debug, Default)]
pub(crate) struct VersionStore {
    pre_images: ShardedMap<LsnId, PreImage>,
    by_txn: ShardedMap<TransactionId, WriteLog>,
    /// Committed transactions not yet forgotten: `(commit_ts, txn, the
    /// pre-images it keeps)`.
    committed: Mutex<Vec<(u64, TransactionId, usize)>>,
    /// Pre-images kept by committed transactions (the sum of `committed`'s
    /// counts): what older readers pin, and what the version-record cap
    /// limits. A running transaction's own are not in it.
    retained: AtomicUsize,
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

    /// Record a write. Only `Add`/`Mod`/`Del` are writes.
    pub(crate) fn insert(&self, lsn: LsnId, op: Operation) {
        let (txn, written) = match op {
            Operation::Add { txn, post } => (
                txn,
                Written::Inserted {
                    table_id: post.table_id,
                    key: post.tuple.id,
                },
            ),
            Operation::Mod { txn, pre, .. } => {
                self.keep_pre_image(lsn, pre.table_id, pre.tuple, false);
                (txn, Written::Replaced(lsn))
            }
            Operation::Del { txn, pre } => {
                self.keep_pre_image(lsn, pre.table_id, pre.tuple, true);
                (txn, Written::Replaced(lsn))
            }
            _ => return,
        };
        self.by_txn.with_entry_or_default(txn, |v| v.push(&written));
    }

    fn keep_pre_image(&self, lsn: LsnId, table_id: TableIdType, tuple: Tuple, deleted: bool) {
        self.pre_images.insert(
            lsn,
            PreImage {
                table_id,
                tuple,
                deleted,
            },
        );
    }

    /// The version a tuple's `pre_lsn` points at: the one its write replaced.
    pub(crate) fn find(&self, lsn: LsnId) -> Option<Tuple> {
        self.pre_images.with(&lsn, |p| p.map(|p| p.tuple.clone()))
    }

    /// How to roll back a transaction's writes, in the order it made them
    /// (undo them newest first). Empty for one that never wrote.
    pub(crate) fn undo_of(&self, txn: &TransactionId) -> Vec<Undo> {
        let lsns_or_keys: Vec<Result<LsnId, (TableIdType, DBIdType)>> =
            self.by_txn.with(txn, |w| {
                w.map(|w| {
                    w.iter()
                        .map(|w| match w {
                            Written::Inserted { table_id, key } => Err((table_id, key)),
                            Written::Replaced(lsn) => Ok(lsn),
                        })
                        .collect()
                })
                .unwrap_or_default()
            });
        lsns_or_keys
            .into_iter()
            .filter_map(|w| match w {
                Err((table_id, key)) => Some(Undo::Remove { table_id, key }),
                Ok(lsn) => self.pre_images.with(&lsn, |p| {
                    p.map(|p| Undo::Restore {
                        table_id: p.table_id,
                        pre: p.tuple.clone(),
                    })
                }),
            })
            .collect()
    }

    /// Whether `txn` has registered any write. False for a read-only
    /// transaction (every SELECT). Writes register here BEFORE they are
    /// logged (see Db::insert & co.), so `false` means nothing was ever
    /// logged for this transaction either.
    pub(crate) fn has_writes(&self, txn: &TransactionId) -> bool {
        self.by_txn.contains_key(txn)
    }

    /// Forget a transaction's writes outright (aborted and reverted, or
    /// committed and past the horizon). Returns how many there were.
    pub(crate) fn discard(&self, txn: &TransactionId) -> usize {
        let Some(written) = self.by_txn.remove(txn) else {
            return 0;
        };
        for w in written.iter() {
            if let Written::Replaced(lsn) = w {
                self.pre_images.remove(&lsn);
            }
        }
        written.count
    }

    /// The transaction committed at `commit_ts`: keep its pre-images until
    /// the horizon passes, and queue its deletes for purging then.
    pub(crate) fn mark_committed(&self, txn: TransactionId, commit_ts: u64) {
        let lsns: Vec<LsnId> = self.by_txn.with(&txn, |w| {
            w.map(|w| {
                w.iter()
                    .filter_map(|w| match w {
                        Written::Replaced(lsn) => Some(lsn),
                        Written::Inserted { .. } => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
        });
        if !self.by_txn.contains_key(&txn) {
            return;
        }
        let mut tombstones = Vec::new();
        for lsn in &lsns {
            self.pre_images.with(lsn, |p| {
                if let Some(p) = p
                    && p.deleted
                {
                    tombstones.push(Tombstone {
                        commit_ts,
                        table_id: p.table_id,
                        key: p.tuple.id.clone(),
                        txn,
                    });
                }
            });
        }
        // Inserts need nothing once committed: nobody can roll them back,
        // and no reader walks back past one.
        if lsns.is_empty() {
            self.by_txn.remove(&txn);
        } else {
            self.by_txn.with_entry_or_default(txn, |w| {
                w.retain(|w| matches!(w, Written::Replaced(_)));
            });
            self.retained.fetch_add(lsns.len(), Ordering::AcqRel);
            self.committed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((commit_ts, txn, lsns.len()));
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
                committed.drain(..).partition(|(ts, _, _)| reclaimable(*ts));
            *committed = keep;
            for (_, txn, kept) in done {
                out.records_discarded += self.discard(&txn);
                self.retained.fetch_sub(kept, Ordering::AcqRel);
                out.transactions_forgotten += 1;
            }
        }
        {
            let mut tombstones = self.tombstones.lock().unwrap_or_else(|e| e.into_inner());
            let (done, keep): (Vec<_>, Vec<_>) = tombstones
                .drain(..)
                .partition(|t| reclaimable(t.commit_ts));
            *tombstones = keep;
            if tombstones.capacity() > SHRINK_ABOVE && tombstones.len() * 4 < tombstones.capacity() {
                tombstones.shrink_to_fit();
            }
            out.tombstones = done;
        }
        if out.transactions_forgotten > 0 {
            self.shrink();
        }
        out
    }

    // Gives back what emptied maps hold (see ShardedMap::shrink_sparse).
    fn shrink(&self) {
        if self.pre_images.capacity() > SHRINK_ABOVE {
            self.pre_images.shrink_sparse();
        }
        if self.by_txn.capacity() > SHRINK_ABOVE {
            self.by_txn.shrink_sparse();
        }
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

    /// Pre-images held, for anyone (stats).
    pub(crate) fn records_len(&self) -> usize {
        self.pre_images.len()
    }

    /// Pre-images committed transactions keep for older readers: what the
    /// version-record cap limits.
    pub(crate) fn retained_len(&self) -> usize {
        self.retained.load(Ordering::Acquire)
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

    fn modify(txn: u64, key: u64) -> Operation {
        Operation::Mod {
            txn: TransactionId::from(txn),
            pre: Record::new(1.into(), Tuple::new(key, b"old"), None),
            post: Record::new(1.into(), Tuple::new(key, b"new"), None),
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
        assert_eq!(vs.undo_of(&TransactionId::from(5u64)).len(), 2);
        // Only the delete has a pre-image.
        assert_eq!(vs.records_len(), 1);
        assert_eq!(vs.retained_len(), 0, "a running transaction's own aren't retained");
        vs.mark_committed(TransactionId::from(5u64), 12);
        assert_eq!(vs.retained_len(), 1);
        // A reader with id 8 began before the commit: keep everything.
        let v = vs.vacuum(Some(8));
        assert_eq!(v.transactions_forgotten, 0);
        assert!(v.tombstones.is_empty());
        assert_eq!(vs.records_len(), 1);
        // Oldest active began after the commit: reclaim.
        let v = vs.vacuum(Some(20));
        assert_eq!(v.transactions_forgotten, 1);
        assert_eq!(v.records_discarded, 1);
        assert_eq!(v.tombstones.len(), 1);
        assert_eq!(v.tombstones[0].key, DBIdType::Int(2));
        assert_eq!(vs.records_len(), 0);
        assert_eq!(vs.retained_len(), 0);
    }

    #[test]
    fn test_a_committed_insert_is_forgotten_at_once() {
        let vs = VersionStore::new();
        vs.insert(LsnId(10), add(5, 1));
        vs.mark_committed(TransactionId::from(5u64), 12);
        assert!(!vs.has_writes(&TransactionId::from(5u64)));
        assert_eq!(vs.committed_pending(), 0);
        let v = vs.vacuum(None);
        assert_eq!(v.transactions_forgotten, 0);
        assert_eq!(vs.records_len(), 0);
    }

    #[test]
    fn test_undo_restores_pre_images_and_removes_inserts_in_write_order() {
        let vs = VersionStore::new();
        let t = TransactionId::from(5u64);
        vs.insert(LsnId(10), add(5, 1));
        vs.insert(LsnId(11), modify(5, 2));
        vs.insert(LsnId(12), del(5, 3));
        let undo = vs.undo_of(&t);
        assert!(matches!(&undo[0], Undo::Remove { key: DBIdType::Int(1), .. }));
        assert!(matches!(&undo[1], Undo::Restore { pre, .. } if &*pre.data == b"old"));
        assert!(matches!(&undo[2], Undo::Restore { pre, .. } if pre.id == DBIdType::Int(3)));
        // A reader walking back from the update's version finds what it replaced.
        assert_eq!(&*vs.find(LsnId(11)).unwrap().data, b"old");
        assert!(vs.find(LsnId(10)).is_none(), "an insert keeps no image");
    }

    #[test]
    fn test_discard_drops_an_aborted_transactions_records_immediately() {
        let vs = VersionStore::new();
        vs.insert(LsnId(10), modify(5, 1));
        vs.insert(LsnId(11), modify(6, 2));
        assert_eq!(vs.discard(&TransactionId::from(5u64)), 1);
        assert!(vs.find(LsnId(10)).is_none());
        assert!(vs.find(LsnId(11)).is_some());
    }

    // Vacuum gives back what a large transaction's records held.
    #[test]
    fn test_vacuum_shrinks_maps_a_large_transaction_grew() {
        let vs = VersionStore::new();
        for k in 0..50_000u64 {
            vs.insert(LsnId(10 + k), modify(5, k));
        }
        vs.mark_committed(TransactionId::from(5u64), 100_000);
        assert!(vs.pre_images.capacity() >= 50_000);
        vs.vacuum(None);
        assert_eq!(vs.records_len(), 0);
        assert!(vs.pre_images.capacity() < 1_000, "{}", vs.pre_images.capacity());
    }
}
