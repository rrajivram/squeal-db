use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant},
};

use log::trace;

//pub type ArcLockGuard = Arc<u16>;
pub struct ArcLockGuard<T: Sized + Clone + Debug> {
    value: T,
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
        }
    }

    fn lock_count(&self) -> usize {
        Arc::strong_count(&self.lock)
    }
}

impl<T> Deref for ArcLockGuard<T>
where
    T: Sized + Clone + Debug,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Sized> Clone for ArcLockGuard<T>
where
    T: Sized + Clone + Debug,
{
    fn clone(&self) -> Self {
        Self {
            lock: self.lock.clone(),
            value: self.value.clone(),
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

#[derive(Default)]
pub struct ArcLock<T>
where
    T: Sized + Clone + Debug,
{
    locks: Arc<RwLock<HashMap<T, ArcLockGuard<T>>>>,
}

impl<T> ArcLock<T>
where
    T: Eq + Hash + Copy + Default + Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn lock(&self, val: T, timeout: u64) -> Option<ArcLockGuard<T>> {
        let now = Instant::now();
        let map = self.locks.write().unwrap();
        trace!(
            "Thread:{:?} : write lock on {:?} after {} usecs.",
            thread::current().id(),
            val,
            now.elapsed().as_micros()
        );
        let v = map.get(&val);
        if let Some(value) = v {
            if value.lock_count() == 1 {
                trace!(
                    "Thread:{:?} : Locked on {:?} within {} usecs.",
                    thread::current().id(),
                    val,
                    now.elapsed().as_micros()
                );
                return Some(value.clone());
            }
            drop(map);
            let mut checked = 0;
            let now = Instant::now();
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
                let map = self.locks.read().unwrap();
                let value = map.get(&val).unwrap();
                if value.lock_count() == 1 {
                    trace!(
                        "Thread:{:?} : Lock on {:?} released after {} usecs.",
                        thread::current().id(),
                        val,
                        now.elapsed().as_micros()
                    );
                    return Some(value.clone());
                }
                drop(map);
                thread::sleep(Duration::from_micros(100));
            }
        } else {
            drop(map);
            let mut map = self.locks.write().unwrap();
            let v = ArcLockGuard::new(val);
            map.insert(val, v.clone());
            return Some(v);
        }
    }

    pub fn cleanup(&self) {
        let mut map = self.locks.write().unwrap();
        let unused = map
            .iter()
            .filter(|&(_, v)| v.lock_count() == 1)
            .map(|(k, _)| *k)
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

#[cfg(test)]
mod arclock_tests {
    use std::{thread, u64};

    use super::ArcLock;

    static mut STAT_VALUE: usize = 0;

    #[test]
    fn test_simple_lock() {
        let lock = ArcLock::new();
        let l1 = lock.lock(1, 0);
        assert!(l1.is_some());
        let l2 = lock.lock(1, 10);
        assert!(l2.is_none());
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
        let l4 = lock.lock(3, 100);
        assert!(l4.is_none());
        drop(l3);
        drop(l4);
        lock.cleanup();
        assert_eq!(lock.locks.read().unwrap().len(), 0);
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
