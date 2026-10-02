//! Table locks: what keeps a table's definition still while a statement
//! (or a transaction) depends on it.
//!
//! A table's definition — its columns, indexes and partitions — is not
//! versioned: there is one current copy, and ALTER TABLE, CREATE INDEX and
//! DROP TABLE replace it. A statement that writes rows decides where they
//! go (which partition, which index trees) from the copy it read, so the
//! definition must not change between that read and the write; and a
//! transaction that reads a table twice must find the same table.
//!
//! So each table has one lock, shared or exclusive:
//! - shared: by a statement that writes the table's rows, for as long as
//!   the statement runs; and by every statement of an explicit transaction
//!   (reads included), until the transaction ends;
//! - exclusive: by a statement that changes the definition, for as long as
//!   it runs.
//!
//! A SELECT outside a transaction takes none. Its plan is fixed when it is
//! built, and every tree it reads stays readable (a dropped partition's or
//! table's trees are left in the store), so a definition changing under it
//! cannot hurt it.
//!
//! A waiting exclusive lock holds back new shared ones, so DDL is not
//! starved. Every wait gives up after LOCK_TIMEOUT rather than hang: two
//! transactions that each hold what the other's DDL wants would otherwise
//! wait forever.

use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::{Condvar, Mutex};

use crate::error::SchemaError;

pub(crate) const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct State {
    shared: usize,
    exclusive: bool,
    // Exclusive requests waiting: new shared ones queue behind them.
    waiting: usize,
}

pub(crate) struct TableLock {
    table: String,
    state: Mutex<State>,
    changed: Condvar,
}

/// Held: the table's definition does not change.
pub(crate) struct SharedGuard(Arc<TableLock>);

/// Held: nothing else is using the table.
pub(crate) struct ExclusiveGuard(Arc<TableLock>);

impl TableLock {
    pub(crate) fn new(table: &str) -> Arc<Self> {
        Arc::new(Self {
            table: table.to_string(),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        })
    }

    fn timed_out(&self, why: &str) -> SchemaError {
        SchemaError::UserError(format!(
            "timed out after {}s waiting for table {:?}: {why}",
            LOCK_TIMEOUT.as_secs(),
            self.table
        ))
    }

    pub(crate) fn shared(self: &Arc<Self>) -> Result<SharedGuard, SchemaError> {
        self.shared_within(LOCK_TIMEOUT)
    }

    pub(crate) fn exclusive(self: &Arc<Self>) -> Result<ExclusiveGuard, SchemaError> {
        self.exclusive_within(LOCK_TIMEOUT)
    }

    fn shared_within(self: &Arc<Self>, timeout: Duration) -> Result<SharedGuard, SchemaError> {
        let mut state = self.state.lock();
        while state.exclusive || state.waiting > 0 {
            if self.changed.wait_for(&mut state, timeout).timed_out() {
                return Err(self.timed_out("its definition is being changed"));
            }
        }
        state.shared += 1;
        Ok(SharedGuard(self.clone()))
    }

    fn exclusive_within(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<ExclusiveGuard, SchemaError> {
        let mut state = self.state.lock();
        state.waiting += 1;
        while state.exclusive || state.shared > 0 {
            if self.changed.wait_for(&mut state, timeout).timed_out() {
                state.waiting -= 1;
                // Shared requests queued behind this one can go on.
                self.changed.notify_all();
                return Err(self.timed_out("a statement or an open transaction is using it"));
            }
        }
        state.waiting -= 1;
        state.exclusive = true;
        Ok(ExclusiveGuard(self.clone()))
    }
}

impl Drop for SharedGuard {
    fn drop(&mut self) {
        self.0.state.lock().shared -= 1;
        self.0.changed.notify_all();
    }
}

impl Drop for ExclusiveGuard {
    fn drop(&mut self) {
        self.0.state.lock().exclusive = false;
        self.0.changed.notify_all();
    }
}

/// A schema's table locks, by table name. A lock outlives its table's
/// definition (the table may be altered or dropped under it), so they are
/// kept by name, apart from the definitions.
#[derive(Default)]
pub(crate) struct TableLocks {
    locks: Mutex<HashMap<String, Arc<TableLock>>>,
}

impl TableLocks {
    pub(crate) fn get(&self, table: &str) -> Arc<TableLock> {
        self.locks
            .lock()
            .entry(table.to_string())
            .or_insert_with(|| TableLock::new(table))
            .clone()
    }
}

/// The shared locks an explicit transaction holds until it ends, each taken
/// once however many statements use the table.
#[derive(Default)]
pub(crate) struct HeldLocks {
    held: Mutex<HashMap<usize, (Arc<TableLock>, SharedGuard)>>,
}

impl HeldLocks {
    fn key(lock: &Arc<TableLock>) -> usize {
        Arc::as_ptr(lock) as usize
    }

    /// Takes `lock` shared unless this transaction already holds it.
    pub(crate) fn hold(&self, lock: &Arc<TableLock>) -> Result<(), SchemaError> {
        if self.held.lock().contains_key(&Self::key(lock)) {
            return Ok(());
        }
        // Not under `held`'s own lock: this may wait.
        let guard = lock.shared()?;
        self.held
            .lock()
            .entry(Self::key(lock))
            .or_insert((lock.clone(), guard));
        Ok(())
    }

    /// Gives `lock` up, if held: true if it was.
    pub(crate) fn release(&self, lock: &Arc<TableLock>) -> bool {
        self.held.lock().remove(&Self::key(lock)).is_some()
    }

    /// The transaction ended.
    pub(crate) fn release_all(&self) {
        self.held.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOON: Duration = Duration::from_millis(30);

    #[test]
    fn test_shared_locks_coexist_and_exclude_an_exclusive_one() {
        let lock = TableLock::new("t");
        let a = lock.shared().unwrap();
        let b = lock.shared().unwrap();
        assert!(lock.exclusive_within(SOON).is_err());
        drop(a);
        assert!(lock.exclusive_within(SOON).is_err());
        drop(b);
        let x = lock.exclusive().unwrap();
        assert!(lock.shared_within(SOON).is_err());
        assert!(lock.exclusive_within(SOON).is_err());
        drop(x);
        lock.shared().unwrap();
    }

    #[test]
    fn test_a_waiting_exclusive_lock_holds_back_new_shared_ones_then_gets_the_table() {
        let lock = TableLock::new("t");
        let reader = lock.shared().unwrap();
        std::thread::scope(|s| {
            let writer = s.spawn(|| lock.exclusive().map(drop));
            while lock.state.lock().waiting == 0 {
                std::thread::yield_now();
            }
            assert!(
                lock.shared_within(SOON).is_err(),
                "queued behind the writer"
            );
            drop(reader);
            writer.join().unwrap().unwrap();
        });
        // A request that gave up no longer holds anyone back.
        let reader = lock.shared().unwrap();
        assert!(lock.exclusive_within(SOON).is_err());
        lock.shared_within(SOON).unwrap();
        drop(reader);
    }

    #[test]
    fn test_a_transaction_holds_each_lock_once_until_it_ends() {
        let locks = TableLocks::default();
        let (t, u) = (locks.get("t"), locks.get("u"));
        assert!(Arc::ptr_eq(&t, &locks.get("t")));
        let held = HeldLocks::default();
        held.hold(&t).unwrap();
        held.hold(&t).unwrap();
        held.hold(&u).unwrap();
        assert_eq!(t.state.lock().shared, 1);
        assert!(t.exclusive_within(SOON).is_err());
        assert!(held.release(&t));
        assert!(!held.release(&t));
        t.exclusive_within(SOON).unwrap();
        held.release_all();
        u.exclusive_within(SOON).unwrap();
    }
}
