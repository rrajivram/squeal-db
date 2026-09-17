use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
    thread,
    time::Duration,
};

use log::trace;
use parking_lot::{ArcReentrantMutexGuard, RawThreadId, ReentrantMutex, RwLock};

// STORE_AUDIT.md P2: this used to be `Arc<u8>` (a strong-count trick standing
// in for "is anyone else holding this"), with the actual mutual exclusion
// implemented by hand in `ArcLock::lock`/`wait_for_lock` — one global
// `RwLock<HashMap<T, ArcLockGuard<T>>>`, taken on its *write* side for every
// single lock/unlock, and a 100us-sleep poll loop for waiters. Two
// consequences, both now fixed: every `lock()` call serialized through that
// one map lock regardless of whether the keys involved were even related
// (measured directly — see benches/arclock.rs's `disjoint_keys_concurrent`),
// and a waiter never actually blocked, just burned CPU re-checking every
// 100us (and re-taking the write lock to do so).
//
// Now: the map holds one real `Arc<ReentrantMutex<()>>` per key, created
// once (lazily) and reused. `lock()` only needs the map's lock briefly, to
// fetch-or-create that per-key mutex (a `read()` in the common case — see
// `get_or_create`) — the actual wait/reentrancy/timeout is handled by
// `try_lock_arc_for` on that per-key mutex directly, which blocks on a real
// futex (parking_lot's own park/unpark), not a sleep loop, and honors
// same-thread reentrancy natively (a `ReentrantMutex`'s whole purpose).
pub struct ArcLockGuard<T: Sized + Clone + Debug> {
    value: T,
    guard: ArcReentrantMutexGuard<parking_lot::RawMutex, RawThreadId, ()>,
    holder: Arc<Holder>,
}

/// Who holds a key right now, for the timeout diagnostic
/// (TXN_SIMPLIFICATION_PLAN.md phase 5). Only the holding thread touches
/// `depth`; the description is read by a timed-out waiter.
#[derive(Debug, Default)]
pub struct Holder {
    depth: std::sync::atomic::AtomicU32,
    since: parking_lot::Mutex<Option<(String, std::time::Instant)>>,
}

impl Holder {
    fn acquired(&self) {
        if self
            .depth
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            == 0
        {
            let t = thread::current();
            *self.since.lock() = Some((
                format!("{:?} {}", t.id(), t.name().unwrap_or("")),
                std::time::Instant::now(),
            ));
        }
    }

    fn released(&self) {
        if self
            .depth
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            *self.since.lock() = None;
        }
    }

    /// "held by <thread> for <duration>" or "not held" (a race with a
    /// release is fine: this is a diagnostic).
    pub fn describe(&self) -> String {
        match &*self.since.lock() {
            Some((who, since)) => format!("held by {who} for {:?}", since.elapsed()),
            None => "not held (released while this waiter timed out)".to_string(),
        }
    }
}

impl<T> Deref for ArcLockGuard<T>
where
    T: Clone + Debug,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> Debug for ArcLockGuard<T>
where
    T: Sized + Clone + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcLockGuard")
            .field("value", &self.value)
            .finish()
    }
}

impl<T> Drop for ArcLockGuard<T>
where
    T: Clone + Debug,
{
    fn drop(&mut self) {
        self.holder.released();
        trace!(
            "Thread {:?}: Dropped lock on {:?}",
            thread::current().id(),
            self.value
        );
    }
}

/// What a lock attempt can come back with.
pub enum LockAttempt<T: Sized + Clone + Debug> {
    Acquired(ArcLockGuard<T>),
    /// Waited out the timeout; carries the holder's description.
    TimedOut(String),
}

// STORE_AUDIT.md P2 follow-up: sharded the same way `buffer.rs`'s
// `ShardedPQ` shards its eviction heap, for the same reason — a single
// `RwLock`, even taken only on its *read* side by every caller (see
// `get_or_create`), still has one shared reader-count atomic that every
// thread's read()/drop bounces across cores. `disjoint_keys_concurrent`
// (benches/arclock.rs) only improved ~11% over the old design despite the
// keys involved being completely unrelated — far short of the near-linear
// scaling that ought to be possible — which is exactly the signature of
// that kind of false sharing on the lock's own internal state, not real
// contention over the map's *contents*. `SHARD_COUNT` shards means threads
// hashing to different shards (the common case once there are more shards
// than threads) touch entirely different cache lines. Hashed by `T`'s own
// `Hash` impl (not `ShardedPQ`'s `Rem<usize> + From<usize>` scheme, which
// only works because its key is numeric) — more general, and `ArcLock<T>`
// already requires `Hash` for the map itself.
//
// This does NOT help genuinely hot-key contention (`same_key_contended` in
// the same bench file): every thread locking the SAME key hashes to the
// SAME shard, so that case is exactly as contended as an unsharded design
// — sharding only spreads DIFFERENT keys apart, it can't split one key's
// own mutex across shards.
const SHARD_COUNT: usize = 16;

/// One shard: key → (its reentrant mutex, who holds it and since when).
type Shard<T> = RwLock<HashMap<T, (Arc<ReentrantMutex<()>>, Arc<Holder>)>>;

pub struct ArcLock<T>
where
    T: Sized + Clone + Debug,
{
    shards: Arc<Vec<Shard<T>>>,
}

impl<T> ArcLock<T>
where
    T: Eq + Hash + Debug + Clone,
{
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            shards: Arc::new((0..SHARD_COUNT).map(|_| RwLock::new(HashMap::new())).collect()),
        })
    }

    fn shard_for(&self, val: &T) -> &Shard<T> {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        val.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % self.shards.len()]
    }

    // Read-mostly: once a key's mutex has been created, every later caller
    // (including concurrent ones on OTHER keys, most of which now land in a
    // DIFFERENT shard entirely) only ever takes that one shard's *read*
    // side here — the write side is only needed the first time a given key
    // is ever locked. This is the fix for the "every lock() call serializes
    // on one global write lock" problem: two threads locking two different,
    // already-seen keys now don't contend on each other at all, whether or
    // not they happen to land in the same shard.
    fn get_or_create(&self, val: &T) -> (Arc<ReentrantMutex<()>>, Arc<Holder>) {
        let shard = self.shard_for(val);
        if let Some(existing) = shard.read().get(val) {
            return existing.clone();
        }
        shard
            .write()
            .entry(val.clone())
            .or_insert_with(|| (Arc::new(ReentrantMutex::new(())), Arc::new(Holder::default())))
            .clone()
    }

    // `timeout` is in microseconds (matching the pre-existing caller
    // convention — see PageBuffer::get_page_mut). STORE_AUDIT.md P2/S9: the
    // old implementation accepted this parameter but silently ignored it,
    // hardcoding a 60s wait regardless of what was asked for — the one real
    // caller passes 5000 (5ms) expecting exactly that, per its own comment.
    // Honored for real now: a per-key ReentrantMutex's `try_lock_arc_for`
    // does a genuine bounded wait.
    pub fn lock(self: &Arc<Self>, val: T, timeout: u64) -> Option<ArcLockGuard<T>> {
        match self.lock_for(val, Duration::from_micros(timeout)) {
            LockAttempt::Acquired(g) => Some(g),
            LockAttempt::TimedOut(_) => None,
        }
    }

    /// Like `lock`, but a timeout comes back with who held the key.
    pub fn lock_for(self: &Arc<Self>, val: T, timeout: Duration) -> LockAttempt<T> {
        let (mutex, holder) = self.get_or_create(&val);
        let Some(guard) = mutex.try_lock_arc_for(timeout) else {
            return LockAttempt::TimedOut(holder.describe());
        };
        holder.acquired();
        trace!(
            "Thread:{:?} : Locked on {:?}.",
            thread::current().id(),
            val
        );
        LockAttempt::Acquired(ArcLockGuard {
            value: val,
            guard,
            holder,
        })
    }

    // Prunes map entries for keys nobody currently holds or is waiting on.
    // `Arc::strong_count(mutex) == 1` means only this map's own copy exists
    // — every live ArcLockGuard (held or in-flight inside `lock()`, which
    // clones the Arc via get_or_create before ever blocking) keeps its own
    // clone alive, so this can never prune a key out from under an active
    // holder or waiter. Not wired into any production call site — same as
    // before this change (a real grep confirms nothing calls it), so this
    // preserves existing behavior (the map still only grows) rather than
    // introducing new eviction that wasn't part of this finding.
    pub fn cleanup(&self) {
        for shard in self.shards.iter() {
            let mut map = shard.write();
            let unused = map
                .iter()
                .filter(|&(_, (m, _))| Arc::strong_count(m) == 1)
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>();
            for u in unused {
                map.remove(&u);
            }
        }
    }

    #[cfg(test)]
    fn total_len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }
}

impl<T> Clone for ArcLock<T>
where
    T: Sized + Clone + Debug,
{
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
        }
    }
}

impl<T> std::fmt::Debug for ArcLock<T>
where
    T: Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total: usize = self.shards.iter().map(|s| s.read().len()).sum();
        write!(f, "Locks: {total}")?;
        Ok(())
    }
}

#[cfg(test)]
mod arclock_tests {
    use std::sync::Arc;
    use std::thread;

    use super::ArcLock;

    static mut STAT_VALUE: usize = 0;

    // Reentrant: same thread can re-acquire a lock it already holds, and
    // the underlying key stays held (blocking other threads) until BOTH
    // guards have dropped.
    #[test]
    fn test_reentrant_same_thread() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0);
        assert!(l1.is_some(), "first lock must succeed");
        // Same thread re-locks — must succeed immediately (no timeout wait).
        let l2 = lock.lock(1, 0);
        assert!(l2.is_some(), "reentrant lock on same thread must succeed");

        // Still held (l1 outstanding): another thread must be blocked.
        let tlock = lock.clone();
        let blocked = thread::spawn(move || tlock.lock(1, 500).is_some()).join().unwrap();
        assert!(
            !blocked,
            "another thread must not acquire while either reentrant guard is held"
        );

        drop(l2);
        // l1 still outstanding: still blocked.
        let tlock = lock.clone();
        let still_blocked = thread::spawn(move || tlock.lock(1, 500).is_some()).join().unwrap();
        assert!(
            !still_blocked,
            "another thread must not acquire while the outer reentrant guard is held"
        );

        drop(l1);
        // Both dropped: now free.
        let tlock = lock.clone();
        let now_free = thread::spawn(move || tlock.lock(1, 500).is_some()).join().unwrap();
        assert!(
            now_free,
            "another thread must acquire once both reentrant guards have dropped"
        );
    }

    // STORE_AUDIT.md S8: `shards` used to be a single std::sync::RwLock,
    // which poisons permanently on any panic while held — an unrelated bug
    // elsewhere (in a completely different call that happened to be
    // holding this same lock at the wrong moment) would turn every FUTURE
    // ArcLock::lock/cleanup/Debug call across the whole process into a
    // panic too, since each one .unwrap()s the lock result. For a page
    // lock registry backing every page in the database, one unrelated
    // panic anywhere would have taken down every other page's locking
    // entirely. Reproduced directly against one shard's internal lock
    // (same-file access) rather than trying to engineer a panic inside
    // ArcLock's own methods.
    #[test]
    fn test_arclock_remains_usable_after_a_panic_while_its_internal_lock_was_held() {
        let lock = ArcLock::new();
        let l = lock.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = l.shards[0].write();
            panic!("simulated bug elsewhere, unrelated to ArcLock itself");
        }));
        assert!(result.is_err(), "sanity: the panic above must actually unwind");

        // With a poisoning std::sync::RwLock, this next call would itself
        // panic (on the .unwrap() inside ArcLock::lock) instead of
        // returning normally.
        let guard = lock.lock(42, 0);
        assert!(
            guard.is_some(),
            "ArcLock must remain fully usable after an unrelated panic merely held \
             (not corrupted) its internal lock"
        );
    }

    // Reentrant many times on the same thread should never block, and the
    // key becomes free again only once every guard has dropped.
    #[test]
    fn test_reentrant_multiple_times() {
        let lock = ArcLock::new();
        let guards: Vec<_> = (0..10).map(|_| lock.lock(42, 0).unwrap()).collect();
        drop(guards);
        // After all guards dropped, another thread can acquire immediately.
        let tlock = lock.clone();
        let result = thread::spawn(move || tlock.lock(42, 500).is_some()).join().unwrap();
        assert!(result, "must be free once every reentrant guard has dropped");
    }

    // A different thread cannot acquire a lock held by another thread (times out).
    #[test]
    fn test_other_thread_blocked_while_held() {
        let lock = ArcLock::new();
        let _holder = lock.lock(99, 0).unwrap();

        let tlock = lock.clone();
        let result = thread::spawn(move || {
            tlock.lock(99, 500).is_some() // 500 µs timeout
        })
        .join()
        .unwrap();

        assert!(
            !result,
            "other thread must time out while lock is held"
        );
    }

    // A different thread can acquire the lock once the holder drops it.
    #[test]
    fn test_other_thread_succeeds_after_release() {
        use std::sync::{Arc, Barrier};
        let lock = ArcLock::new();
        let barrier = Arc::new(Barrier::new(2));

        let holder = lock.lock(7, 0).unwrap();

        let tlock = lock.clone();
        let b2 = barrier.clone();
        let handle = thread::spawn(move || {
            b2.wait(); // signal: ready to lock
            tlock.lock(7, 100_000).is_some() // wait up to 100 ms
        });

        drop(holder); // release before the thread tries
        barrier.wait();
        let result = handle.join().unwrap();
        assert!(
            result,
            "other thread must acquire lock after release"
        );
    }

    #[test]
    fn test_simple_lock() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0);
        assert!(l1.is_some());
        // Different thread must be blocked while l1 is held.
        let tlock = lock.clone();
        let blocked = thread::spawn(move || tlock.lock(1, 500).is_some()).join().unwrap();
        assert!(!blocked);
        drop(l1);
        let l2 = lock.lock(1, 10);
        assert!(l2.is_some());
    }

    #[test]
    fn test_cleanup() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0).unwrap();
        let l2 = lock.lock(2, 0).unwrap();
        let l3 = lock.lock(3, 0).unwrap();
        assert_eq!(lock.total_len(), 3);
        lock.cleanup();
        assert_eq!(lock.total_len(), 3);
        drop(l1);
        drop(l2);
        lock.cleanup();
        assert_eq!(lock.total_len(), 1);
        // A different thread is blocked by l3.
        let tlock = lock.clone();
        let blocked = thread::spawn(move || tlock.lock(3, 500).is_some()).join().unwrap();
        assert!(!blocked);
        drop(l3);
        lock.cleanup();
        assert_eq!(lock.total_len(), 0);
    }

    // Regression test for a real bug in the OLD hand-rolled design: the map
    // entry's recorded "owner" thread_id was never updated when a
    // different thread legitimately re-acquired a released key, so the
    // ORIGINAL creating thread stayed permanently (falsely) treated as a
    // reentrant owner even after someone else took over. A real
    // ReentrantMutex can't have this bug by construction (ownership is
    // tracked by the mutex itself, correctly, on every acquire) — kept as
    // a permanent regression test for the behavior, not the old
    // implementation detail.
    #[test]
    fn test_stale_thread_id_lets_old_owner_bypass_current_holder() {
        use std::sync::mpsc;

        let lock = ArcLock::new();

        // This (main/test) thread creates the entry for key=1, then releases it.
        let g1 = lock.lock(1, 0).unwrap();
        drop(g1);

        // A different thread now legitimately acquires key=1 and holds it open.
        let (acquired_tx, acquired_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let lock2 = lock.clone();
        let other = thread::spawn(move || {
            let _g2 = lock2.lock(1, 0).unwrap();
            acquired_tx.send(()).unwrap();
            release_rx.recv().unwrap(); // keep holding until told to stop
        });
        acquired_rx.recv().unwrap();

        // This thread (the ORIGINAL creator, who already released) tries to
        // lock key=1 again while the other thread demonstrably still holds it.
        // A correct implementation must block/timeout here.
        let result = lock.lock(1, 1000); // 1ms timeout

        release_tx.send(()).unwrap();
        other.join().unwrap();

        assert!(
            result.is_none(),
            "a thread that previously released this lock must not bypass \
             another thread's current hold"
        );
    }

    // Regression test: cleanup() must never panic a thread mid-wait, even
    // when it races a concurrent lock() on the same key.
    #[test]
    fn test_cleanup_racing_with_waiter_panics() {
        let lock = ArcLock::new();
        let holder = lock.lock(1, 0).unwrap();

        let tlock = lock.clone();
        let waiter = thread::spawn(move || {
            tlock.lock(1, 50_000).is_some() // 50ms: long enough to land in the wait
        });

        thread::sleep(std::time::Duration::from_millis(2)); // let it start waiting
        drop(holder);
        lock.cleanup(); // races the waiter's own in-flight Arc clone

        let result = waiter.join();
        assert!(
            result.is_ok(),
            "waiting thread panicked instead of returning cleanly: {:?}",
            result
        );
    }

    #[test]
    #[allow(static_mut_refs)]
    fn test_multi_threaded() {
        let mut threads = vec![];
        let lock = ArcLock::new();
        for _ in 0..100 {
            let t = thread::spawn(move || {
                for _ in 0..100 {
                    unsafe {
                        STAT_VALUE += 1;
                    }
                }
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        let unsynched;
        unsafe {
            println!("unsynched stat is {STAT_VALUE}");
            unsynched = STAT_VALUE;
            STAT_VALUE = 0;
        }
        let mut threads = vec![];
        for _ in 0..10 {
            let tlock = lock.clone();
            let t = thread::spawn(move || {
                for _ in 0..100 {
                    let l = tlock.lock(1, 500);
                    if let Some(_l) = l {
                        unsafe {
                            STAT_VALUE += 1;
                        }
                    }
                }
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        unsafe {
            println!("synched stat is {STAT_VALUE}");
            assert!(STAT_VALUE < unsynched);
            STAT_VALUE = 0;
        }
        let mut threads = vec![];
        for _ in 0..10 {
            let tlock = lock.clone();
            let t = thread::spawn(move || {
                for _ in 0..100 {
                    let l = tlock.lock(1, u64::MAX);
                    if let Some(_l) = l {
                        unsafe {
                            STAT_VALUE += 1;
                        }
                    }
                }
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        unsafe {
            println!("heavily synched stat is {STAT_VALUE}");
            assert!(STAT_VALUE >= 990);
        }
    }

    // STORE_AUDIT.md P2 — allocation-count proxy, complementing
    // benches/arclock.rs's wall-clock measurements. `#[ignore]`d (run
    // explicitly, alone) because `crate::alloc::stats()` reads this whole
    // PROCESS's global allocator counters — any other test allocating
    // concurrently would pollute the delta. Run with:
    //   cargo test -p store --lib arclock::arclock_tests::alloc_proxy \
    //     -- --ignored --nocapture --test-threads=1
    //
    // The old design allocated a fresh `Arc::new(0)` (ArcLockGuard::new)
    // on every non-reentrant lock() call, even for keys that had been
    // locked thousands of times before — the map only ever cached a
    // *guard*, not a reusable lock object, so "already seen this key"
    // bought nothing. The new design allocates an `Arc<ReentrantMutex<()>>`
    // exactly once per DISTINCT key, ever (cached in the map thereafter);
    // every subsequent lock()/unlock() on that key, from any thread, is
    // allocation-free. With THREADS distinct keys and OPS_PER_THREAD
    // acquisitions each, this predicts total allocation *events* dropping
    // from roughly THREADS * OPS_PER_THREAD (one per acquisition) to
    // roughly THREADS (one per distinct key) — not a constant-factor win,
    // an asymptotic one.
    #[test]
    #[ignore]
    fn alloc_proxy_disjoint_keys_concurrent() {
        const THREADS: u64 = 8;
        const OPS_PER_THREAD: u64 = 2_000;

        let lock: Arc<ArcLock<u64>> = ArcLock::new();
        let before = crate::alloc::stats();

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let l = lock.clone();
                thread::spawn(move || {
                    for _ in 0..OPS_PER_THREAD {
                        let g = l.lock(t, 5_000_000).expect("must not time out");
                        drop(g);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let after = crate::alloc::stats();
        let total_ops = THREADS * OPS_PER_THREAD;
        let alloc_events: usize = after
            .size_histogram
            .iter()
            .zip(before.size_histogram.iter())
            .map(|(a, b)| a - b)
            .sum();
        let bytes = after.total_allocated - before.total_allocated;
        println!(
            "disjoint_keys_concurrent: {total_ops} ops across {THREADS} keys — \
             {alloc_events} allocation events ({:.4}/op), {bytes} bytes ({:.2}/op)",
            alloc_events as f64 / total_ops as f64,
            bytes as f64 / total_ops as f64,
        );
    }
}
