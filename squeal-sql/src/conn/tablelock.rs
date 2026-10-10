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
//! starved.
//!
//! Locks are held by an owner — a connection — and every lock knows its
//! holders and its waiters. A request that would have to wait for an owner
//! who is, through a chain of such waits, waiting for the requester is a
//! deadlock: it fails at once instead (see `deadlocked`). Two transactions
//! that each hold what the other's DDL wants are the usual case. A wait
//! still gives up after LOCK_TIMEOUT, as a backstop.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
    time::Duration,
};

// Not std's: its now() panics on wasm32 (see store::clock).
use store::clock::Instant;

use parking_lot::{Condvar, Mutex};

use crate::error::SchemaError;

pub(crate) const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

// How often a waiting request looks for a deadlock again: one that formed
// while two requests began waiting at the same moment is seen by neither
// at first.
const RECHECK: Duration = Duration::from_millis(50);

/// Who holds a lock: a connection (see Connection::lock_owner).
pub(crate) type Owner = u128;

#[derive(Default)]
struct State {
    // Owners holding it shared, and how many times each.
    shared: HashMap<Owner, usize>,
    exclusive: Option<Owner>,
    // Owners waiting for it exclusively: new shared requests queue behind
    // them.
    waiting: Vec<Owner>,
}

impl State {
    // Who `owner` must wait for to get the lock: holders in a conflicting
    // mode, and — for a shared request — exclusive requests queued first.
    // Never `owner` itself: one connection's statements run one at a time.
    fn blockers(&self, owner: Owner, exclusive: bool) -> Vec<Owner> {
        let mut b: Vec<Owner> = self.exclusive.into_iter().collect();
        if exclusive {
            b.extend(self.shared.keys().copied());
        } else {
            b.extend(self.waiting.iter().copied());
        }
        b.retain(|o| *o != owner);
        b.sort();
        b.dedup();
        b
    }
}

pub(crate) struct TableLock {
    table: String,
    state: Mutex<State>,
    changed: Condvar,
}

/// Held: the table's definition does not change.
pub(crate) struct SharedGuard(Arc<TableLock>, Owner);

/// Held: nothing else is using the table.
pub(crate) struct ExclusiveGuard(Arc<TableLock>);

// What a waiting owner waits for.
#[derive(Clone)]
struct Wait {
    lock: Arc<TableLock>,
    exclusive: bool,
    // When it began waiting: in a cycle, the latest to begin is the one
    // that closed it, and the one refused (see deadlocked).
    since: Instant,
}

fn waits() -> &'static Mutex<HashMap<Owner, Wait>> {
    static WAITS: OnceLock<Mutex<HashMap<Owner, Wait>>> = OnceLock::new();
    WAITS.get_or_init(Default::default)
}

// Whether `owner`, waiting for `lock`, must give up: it waits — through
// the owners blocking it, what they wait for, and so on — for itself, and
// of the owners in that cycle it began waiting last. Every owner in the
// cycle reaches the same verdict, so exactly one gives up and the rest go
// on once it does. Reads one lock's state at a time, holding none while it
// reads another.
fn deadlocked(owner: Owner, lock: &Arc<TableLock>, exclusive: bool) -> bool {
    // Breadth first, remembering how each owner was reached, to name the
    // cycle's members once it closes.
    let mut via: HashMap<Owner, Owner> = HashMap::new();
    let mut todo: std::collections::VecDeque<Owner> = std::collections::VecDeque::new();
    for b in lock.state.lock().blockers(owner, exclusive) {
        via.insert(b, owner);
        todo.push_back(b);
    }
    let mut seen = HashSet::new();
    while let Some(o) = todo.pop_front() {
        if o == owner {
            let mut members = vec![owner];
            let mut at = via[&owner];
            while at != owner && members.len() <= via.len() {
                members.push(at);
                at = via[&at];
            }
            // Read while walking, the cycle may already be broken: a member
            // no longer waiting (it gave up, or got its lock) means it is.
            let waits = waits().lock();
            let Some(sinces) = members
                .iter()
                .map(|m| waits.get(m).map(|w| (w.since, *m)))
                .collect::<Option<Vec<_>>>()
            else {
                return false;
            };
            return sinces.iter().max().map(|(_, m)| *m) == Some(owner);
        }
        if !seen.insert(o) {
            continue;
        }
        let waiting = waits().lock().get(&o).cloned();
        if let Some(w) = waiting {
            for b in w.lock.state.lock().blockers(o, w.exclusive) {
                via.entry(b).or_insert(o);
                todo.push_back(b);
            }
        }
    }
    false
}

impl TableLock {
    pub(crate) fn new(table: &str) -> Arc<Self> {
        Arc::new(Self {
            table: table.to_string(),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        })
    }

    pub(crate) fn shared(self: &Arc<Self>, owner: Owner) -> Result<SharedGuard, SchemaError> {
        self.acquire(owner, false, LOCK_TIMEOUT)?;
        Ok(SharedGuard(self.clone(), owner))
    }

    pub(crate) fn exclusive(self: &Arc<Self>, owner: Owner) -> Result<ExclusiveGuard, SchemaError> {
        self.acquire(owner, true, LOCK_TIMEOUT)?;
        Ok(ExclusiveGuard(self.clone()))
    }

    fn acquire(
        self: &Arc<Self>,
        owner: Owner,
        exclusive: bool,
        timeout: Duration,
    ) -> Result<(), SchemaError> {
        let since = Instant::now();
        let deadline = since + timeout;
        let mut state = self.state.lock();
        if exclusive {
            state.waiting.push(owner);
        }
        let result = loop {
            if state.blockers(owner, exclusive).is_empty() {
                break Ok(());
            }
            // Look for a deadlock with no lock state held (it reads others').
            waits().lock().insert(
                owner,
                Wait {
                    lock: self.clone(),
                    exclusive,
                    since,
                },
            );
            drop(state);
            let dead = deadlocked(owner, self, exclusive);
            state = self.state.lock();
            if dead {
                break Err(SchemaError::UserError(format!(
                    "deadlock: waiting for table {:?} would wait forever — another \
                     connection waits for a table this one holds; this statement is \
                     refused, the other goes on once this transaction ends",
                    self.table
                )));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break Err(SchemaError::UserError(format!(
                    "timed out after {}s waiting for table {:?}: {}",
                    timeout.as_secs(),
                    self.table,
                    if exclusive {
                        "a statement or an open transaction is using it"
                    } else {
                        "its definition is being changed"
                    }
                )));
            }
            self.changed.wait_for(&mut state, left.min(RECHECK));
        };
        waits().lock().remove(&owner);
        if exclusive && let Some(i) = state.waiting.iter().position(|o| *o == owner) {
            state.waiting.remove(i);
        }
        match result {
            Ok(()) if exclusive => state.exclusive = Some(owner),
            Ok(()) => *state.shared.entry(owner).or_default() += 1,
            // Shared requests queued behind this one can go on.
            Err(_) => {
                self.changed.notify_all();
            }
        }
        result
    }
}

impl Drop for SharedGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        if let Some(n) = state.shared.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                state.shared.remove(&self.1);
            }
        }
        drop(state);
        self.0.changed.notify_all();
    }
}

impl Drop for ExclusiveGuard {
    fn drop(&mut self) {
        self.0.state.lock().exclusive = None;
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

    /// Takes `lock` shared for `owner` unless this transaction already
    /// holds it.
    pub(crate) fn hold(&self, lock: &Arc<TableLock>, owner: Owner) -> Result<(), SchemaError> {
        if self.held.lock().contains_key(&Self::key(lock)) {
            return Ok(());
        }
        // Not under `held`'s own lock: this may wait.
        let guard = lock.shared(owner)?;
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
    // Owners no other test uses: what each waits for is process-wide.
    fn owners() -> (Owner, Owner, Owner) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let base = NEXT.fetch_add(3, Ordering::SeqCst) as Owner;
        (base, base + 1, base + 2)
    }

    fn try_shared(lock: &Arc<TableLock>, owner: Owner) -> Result<(), SchemaError> {
        lock.acquire(owner, false, SOON)
    }

    fn try_exclusive(lock: &Arc<TableLock>, owner: Owner) -> Result<(), SchemaError> {
        lock.acquire(owner, true, SOON)
    }

    fn release_exclusive(lock: &Arc<TableLock>) {
        drop(ExclusiveGuard(lock.clone()));
    }

    #[test]
    fn test_shared_locks_coexist_and_exclude_an_exclusive_one() {
        #[allow(non_snake_case)]
        let (A, B, C) = owners();
        let lock = TableLock::new("t");
        let a = lock.shared(A).unwrap();
        let b = lock.shared(B).unwrap();
        assert!(try_exclusive(&lock, C).is_err());
        drop(a);
        assert!(try_exclusive(&lock, C).is_err());
        drop(b);
        let x = lock.exclusive(C).unwrap();
        assert!(try_shared(&lock, A).is_err());
        assert!(try_exclusive(&lock, A).is_err());
        drop(x);
        lock.shared(A).unwrap();
    }

    #[test]
    fn test_a_waiting_exclusive_lock_holds_back_new_shared_ones_then_gets_the_table() {
        #[allow(non_snake_case)]
        let (A, B, C) = owners();
        let lock = TableLock::new("t");
        let reader = lock.shared(A).unwrap();
        std::thread::scope(|s| {
            let writer = s.spawn(|| lock.exclusive(B).map(drop));
            while lock.state.lock().waiting.is_empty() {
                std::thread::yield_now();
            }
            assert!(try_shared(&lock, C).is_err(), "queued behind the writer");
            drop(reader);
            writer.join().unwrap().unwrap();
        });
        // A request that gave up no longer holds anyone back.
        let reader = lock.shared(A).unwrap();
        assert!(try_exclusive(&lock, B).is_err());
        try_shared(&lock, C).unwrap();
        drop(reader);
    }

    #[test]
    fn test_a_transaction_holds_each_lock_once_until_it_ends() {
        #[allow(non_snake_case)]
        let (A, B, _) = owners();
        let locks = TableLocks::default();
        let (t, u) = (locks.get("t"), locks.get("u"));
        assert!(Arc::ptr_eq(&t, &locks.get("t")));
        let held = HeldLocks::default();
        held.hold(&t, A).unwrap();
        held.hold(&t, A).unwrap();
        held.hold(&u, A).unwrap();
        assert_eq!(t.state.lock().shared.get(&A), Some(&1));
        assert!(try_exclusive(&t, B).is_err());
        assert!(held.release(&t));
        assert!(!held.release(&t));
        try_exclusive(&t, B).unwrap();
        release_exclusive(&t);
        held.release_all();
        try_exclusive(&u, B).unwrap();
    }

    // A holds t, B holds u; A waits for u: no cycle yet. B asking for t
    // closes one: refused at once, well before the timeout. A then gets u
    // once B lets it go.
    #[test]
    fn test_a_request_closing_a_cycle_of_waits_is_refused_at_once() {
        #[allow(non_snake_case)]
        let (A, B, C) = owners();
        let (t, u) = (TableLock::new("t"), TableLock::new("u"));
        let a_holds = t.shared(A).unwrap();
        let b_holds = u.shared(B).unwrap();
        std::thread::scope(|s| {
            let a_waits = s.spawn(|| u.exclusive(A).map(drop));
            while !waits().lock().contains_key(&A) {
                std::thread::yield_now();
            }
            let start = Instant::now();
            let err = t.exclusive(B).map(drop).unwrap_err().to_string();
            assert!(err.contains("deadlock"), "{err}");
            assert!(start.elapsed() < Duration::from_secs(2));
            drop(b_holds);
            a_waits.join().unwrap().unwrap();
        });
        drop(a_holds);
        // Three owners round a cycle, through a queued exclusive request:
        // A holds t and waits for u (held by B); B waits, shared, for v,
        // where C's exclusive request is queued; C holds t... C asks t.
        let (t, u, v) = (
            TableLock::new("t"),
            TableLock::new("u"),
            TableLock::new("v"),
        );
        let _a = t.shared(A).unwrap();
        let _b = u.shared(B).unwrap();
        let _c = v.shared(C).unwrap();
        std::thread::scope(|s| {
            let a = s.spawn(|| u.exclusive(A).map(drop));
            let b = s.spawn(|| v.exclusive(B).map(drop));
            while !waits().lock().contains_key(&A) || !waits().lock().contains_key(&B) {
                std::thread::yield_now();
            }
            let err = t.exclusive(C).map(drop).unwrap_err().to_string();
            assert!(err.contains("deadlock"), "{err}");
            drop(_c);
            b.join().unwrap().unwrap();
            drop(_b);
            a.join().unwrap().unwrap();
        });
    }
}
