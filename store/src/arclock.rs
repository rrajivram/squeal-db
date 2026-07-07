use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::{Arc, RwLock},
    thread::{self, ThreadId, current},
    time::{Duration, Instant},
};

use log::trace;

//pub type ArcLockGuard = Arc<u16>;
pub struct ArcLockGuard<T: Sized + Clone + Debug> {
    value: T,
    thread_id: ThreadId,
    lock: Arc<u8>,
}

impl<T> ArcLockGuard<T>
where
    T: Sized + Clone + Debug,
{
    fn new(value: T) -> Self {
        Self {
            value,
            lock: Arc::new(0),
            thread_id: current().id(),
        }
    }

    fn lock_count(&self) -> usize {
        Arc::strong_count(&self.lock)
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

impl<T> Clone for ArcLockGuard<T>
where
    T: Clone + Debug,
{
    fn clone(&self) -> Self {
        // Preserve the recorded owner's thread_id rather than stamping it to
        // whichever thread happens to call clone() — this field identifies
        // who *owns* the lock, not who is merely holding a reference to the
        // guard right now. Re-stamping it here previously let ownership
        // silently "drift" to the cloning thread.
        Self {
            lock: self.lock.clone(),
            value: self.value.clone(),
            thread_id: self.thread_id,
        }
    }
}

impl<T> Debug for ArcLockGuard<T>
where
    T: Sized + Clone + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcLockGuard")
            .field("value", &self.value)
            .field("lock", &Arc::strong_count(&self.lock))
            .finish()
    }
}

impl<T> Drop for ArcLockGuard<T>
where
    T: Clone + Debug,
{
    fn drop(&mut self) {
        trace!(
            "Thread {:?}: Dropped lock on {:?}",
            thread::current().id(),
            self.value
        );
    }
}

pub struct ArcLock<T>
where
    T: Sized + Clone + Debug,
{
    locks: Arc<RwLock<HashMap<T, ArcLockGuard<T>>>>,
}

impl<T> ArcLock<T>
where
    T: Eq + Hash + Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn lock(&self, val: T, timeout: u64) -> Option<ArcLockGuard<T>> {
        let now = Instant::now();
        let mut map = self.locks.write().unwrap();
        trace!(
            "Thread:{:?} : write lock on {:?} after {} usecs.",
            thread::current().id(),
            val,
            now.elapsed().as_micros()
        );
        if let Some(existing) = map.get(&val) {
            let count = existing.lock_count();
            let owner = existing.thread_id;
            if count == 1 {
                // Free right now (no live guards besides the map's own copy):
                // claim it under OUR identity by *replacing* the entry, not
                // cloning the old one. Cloning would keep whichever thread_id
                // was recorded when this entry was first created — letting
                // that original thread bypass the lock later via the
                // reentrancy check below, even after ownership has moved on.
                let fresh = ArcLockGuard::new(val.clone());
                map.insert(val, fresh.clone());
                trace!(
                    "Thread:{:?} : Locked on {:?} within {} usecs.",
                    thread::current().id(),
                    fresh.value,
                    now.elapsed().as_micros()
                );
                return Some(fresh);
            }
            if owner == current().id() {
                // Actively held (count > 1) by this same thread: genuine
                // reentrancy. Clone to share the existing Arc chain so
                // lock_count() still reflects every outstanding guard.
                let value = map.get(&val).unwrap().clone();
                trace!(
                    "Thread:{:?} : Locked on {:?} within {} usecs.",
                    thread::current().id(),
                    val,
                    now.elapsed().as_micros()
                );
                return Some(value);
            }
            // Actively held by a different thread: wait for it.
            drop(map);
            return self.wait_for_lock(val, timeout);
        }
        let fresh = ArcLockGuard::new(val.clone());
        map.insert(val, fresh.clone());
        Some(fresh)
    }

    fn wait_for_lock(&self, val: T, timeout: u64) -> Option<ArcLockGuard<T>> {
        let now = Instant::now();
        let mut checked = 0;
        loop {
            if now.elapsed().as_micros() > timeout as u128 {
                trace!(
                    "Thread: {:?}: Timed out on {:?} after {:?} usecs and {checked} tries.",
                    thread::current().id(),
                    val,
                    now.elapsed().as_micros()
                );
                return None;
            }
            checked += 1;
            // Needs the write lock (not just read): deciding "it's free" and
            // claiming it must be atomic, or two waiters could both observe
            // count == 1 and both think they won.
            let mut map = self.locks.write().unwrap();
            // Treat a missing entry (e.g. removed by cleanup() racing with
            // this poll) the same as "free", instead of unwrapping into a
            // panic.
            let free = map.get(&val).map(|v| v.lock_count() == 1).unwrap_or(true);
            if free {
                let fresh = ArcLockGuard::new(val.clone());
                map.insert(val.clone(), fresh.clone());
                trace!(
                    "Thread:{:?} : Lock on {:?} released after {} usecs.",
                    thread::current().id(),
                    val,
                    now.elapsed().as_micros()
                );
                return Some(fresh);
            }
            drop(map);
            thread::sleep(Duration::from_micros(100));
        }
    }

    pub fn cleanup(&self) {
        let mut map = self.locks.write().unwrap();
        let unused = map
            .iter()
            .filter(|&(_, v)| v.lock_count() == 1)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        for u in unused {
            map.remove(&u);
        }
    }
}

impl<T> Clone for ArcLock<T>
where
    T: Sized + Clone + Debug,
{
    fn clone(&self) -> Self {
        Self {
            locks: self.locks.clone(),
        }
    }
}

impl<T> std::fmt::Debug for ArcLock<T>
where
    T: Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Locks: {}", self.locks.read().unwrap().len())?;
        Ok(())
    }
}

#[cfg(test)]
mod arclock_tests {
    use std::thread;

    use super::ArcLock;

    static mut STAT_VALUE: usize = 0;

    // Reentrant: same thread can re-acquire a lock it already holds.
    #[test]
    fn test_reentrant_same_thread() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0);
        assert!(l1.is_some(), "first lock must succeed");
        // Same thread re-locks — must succeed immediately (no timeout wait).
        let l2 = lock.lock(1, 0);
        assert!(l2.is_some(), "reentrant lock on same thread must succeed");
        // Both guards alive: ref count is 3 (map entry + l1 + l2).
        assert_eq!(l1.as_ref().unwrap().lock_count(), 3);
        drop(l2);
        assert_eq!(l1.as_ref().unwrap().lock_count(), 2);
        drop(l1);
    }

    // Reentrant many times on the same thread should never block.
    #[test]
    fn test_reentrant_multiple_times() {
        let lock = ArcLock::new();
        let guards: Vec<_> = (0..10).map(|_| lock.lock(42, 0).unwrap()).collect();
        assert_eq!(guards[0].lock_count(), 11); // map entry + 10 guards
        drop(guards);
        // After all guards dropped, count back to 1 (map entry only).
        let l = lock.lock(42, 0).unwrap();
        assert_eq!(l.lock_count(), 2); // map entry + this guard
    }

    // A different thread cannot acquire a lock held by another thread (times out).
    #[test]
    fn test_other_thread_blocked_while_held() {
        let lock = ArcLock::new();
        let _holder = lock.lock(99, 0).unwrap();

        let tlock = lock.clone();
        let result = thread::spawn(move || {
            tlock.lock(99, 500) // 500 µs timeout
        })
        .join()
        .unwrap();

        assert!(
            result.is_none(),
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
            tlock.lock(7, 100_000) // wait up to 100 ms
        });

        drop(holder); // release before the thread tries
        barrier.wait();
        let result = handle.join().unwrap();
        assert!(
            result.is_some(),
            "other thread must acquire lock after release"
        );
    }

    // Reentrant: thread_id recorded correctly on first lock, honoured on re-entry.
    #[test]
    fn test_reentrant_thread_id_matches() {
        let lock = ArcLock::new();
        let l1 = lock.lock(5, 0).unwrap();
        let recorded_id = l1.thread_id;
        assert_eq!(recorded_id, thread::current().id());

        let l2 = lock.lock(5, 0).unwrap();
        assert_eq!(l2.thread_id, thread::current().id());
        drop(l2);
        drop(l1);
    }

    #[test]
    fn test_simple_lock() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0);
        assert!(l1.is_some());
        // Different thread must be blocked while l1 is held.
        let tlock = lock.clone();
        let blocked = thread::spawn(move || tlock.lock(1, 500)).join().unwrap();
        assert!(blocked.is_none());
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
        assert_eq!(lock.locks.read().unwrap().len(), 3);
        lock.cleanup();
        assert_eq!(lock.locks.read().unwrap().len(), 3);
        drop(l1);
        drop(l2);
        lock.cleanup();
        assert_eq!(lock.locks.read().unwrap().len(), 1);
        // A different thread is blocked by l3.
        let tlock = lock.clone();
        let blocked = thread::spawn(move || tlock.lock(3, 500)).join().unwrap();
        assert!(blocked.is_none());
        drop(l3);
        lock.cleanup();
        assert_eq!(lock.locks.read().unwrap().len(), 0);
    }

    // ArcLock::lock() never updates the map entry's recorded thread_id once a
    // *different* thread re-acquires a released key via the lock_count()==1
    // path. That means the original creating thread is permanently (falsely)
    // treated as "reentrant owner" for that key, even after someone else has
    // legitimately taken it. This test forces that exact sequence
    // deterministically (via channels, no timing luck) and shows the original
    // thread is wrongly granted the lock while another thread still holds it.
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
             another thread's current hold, but it did — ArcLock's map entry \
             never updates thread_id when a different thread re-acquires, so \
             the original creator is permanently misidentified as the owner"
        );
    }

    // The wait-loop in lock() does `map.get(&val).unwrap()` on every poll.
    // cleanup() removes any entry whose lock_count() == 1 — which is exactly
    // the state right after a holder releases and before a waiter's next
    // poll. If cleanup() runs in that window, the waiter's next `.unwrap()`
    // panics instead of cleanly timing out or acquiring. cleanup() isn't
    // wired into production code today (grep confirms), so this is latent —
    // but it WILL panic a waiting thread the moment something calls it
    // concurrently with contended locks, which is exactly what cleanup() is
    // for.
    #[test]
    fn test_cleanup_racing_with_waiter_panics() {
        let lock = ArcLock::new();
        let holder = lock.lock(1, 0).unwrap();

        let tlock = lock.clone();
        let waiter = thread::spawn(move || {
            tlock.lock(1, 50_000) // 50ms: long enough to land in the wait loop
        });

        thread::sleep(std::time::Duration::from_millis(2)); // let it enter the loop
        drop(holder); // lock_count drops to 1 (map entry only)
        lock.cleanup(); // races the waiter's next `map.get(&val).unwrap()`

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
}
