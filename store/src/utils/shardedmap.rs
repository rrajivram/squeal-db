use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use parking_lot::RwLock;

// STORE_AUDIT.md P2 follow-up: the same pattern ArcLock and ShardedPQ already
// use — a single RwLock<HashMap<K, V>> serializes every reader on that one
// lock's own internal state (a shared reader-count atomic bounces across
// cores under concurrent access, even though a read() is nominally
// non-exclusive), regardless of whether the keys involved are related at
// all. Splitting into N independent shards, each with its own RwLock, means
// two threads touching two different keys usually don't contend on any
// shared state at all — see benches/arclock.rs / BASELINE.md's P2 section
// for the measured effect on ArcLock, which this generalizes.
//
// Hashed by `K`'s own `Hash` impl (unlike ShardedPQ, which requires
// `Rem<usize> + From<usize>` and only works for numeric-like keys) — this
// works for any key already required to be Hash + Eq for HashMap itself, at
// the cost of one DefaultHasher computation per call. Not a drop-in
// replacement for every `RwLock<HashMap<..>>` in this crate: it only helps
// when the keys involved are genuinely independent of each other (no
// operation needs to touch two different keys atomically) — see
// audit-progress.md's P2 survey entry for which locks in this crate do and
// don't fit that shape.
const DEFAULT_SHARDS: usize = 16;

#[derive(Debug)]
pub(crate) struct ShardedMap<K, V> {
    shards: Vec<RwLock<HashMap<K, V>>>,
}

impl<K, V> Default for ShardedMap<K, V>
where
    K: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new(DEFAULT_SHARDS)
    }
}

impl<K, V> ShardedMap<K, V>
where
    K: Hash + Eq + Clone,
{
    pub(crate) fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count.max(1))
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard_for(&self, key: &K) -> &RwLock<HashMap<K, V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        &self.shards[(hasher.finish() as usize) % self.shards.len()]
    }

    pub(crate) fn insert(&self, key: K, value: V) -> Option<V> {
        self.shard_for(&key).write().insert(key, value)
    }

    pub(crate) fn remove(&self, key: &K) -> Option<V> {
        self.shard_for(key).write().remove(key)
    }

    pub(crate) fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.shard_for(key).read().get(key).cloned()
    }

    // Applies `f` to the entry for `key`, inserting `V::default()` first if
    // it's absent — the sharded equivalent of
    // `map.entry(key).or_default()`, for callers that need to mutate the
    // value in place (e.g. pushing onto a `Vec`) rather than read-clone-
    // write it.
    pub(crate) fn with_entry_or_default(&self, key: K, f: impl FnOnce(&mut V))
    where
        V: Default,
    {
        let shard = self.shard_for(&key);
        f(shard.write().entry(key).or_default());
    }

    // Read-mostly get-or-create: returns the existing value for `key`, or
    // computes and inserts `make()` if absent. Only takes the shard's write
    // side the first time a given key is ever seen — every later caller
    // (including concurrent ones on other keys) takes the read side. This
    // is the pattern behind `ArcLock`'s own `get_or_create` and `Db::
    // table_guard`, generalized.
    pub(crate) fn get_or_insert_with(&self, key: K, make: impl FnOnce() -> V) -> V
    where
        V: Clone,
    {
        let shard = self.shard_for(&key);
        if let Some(existing) = shard.read().get(&key) {
            return existing.clone();
        }
        shard.write().entry(key).or_insert_with(make).clone()
    }

    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::ShardedMap;

    #[test]
    fn test_insert_get_remove_roundtrip() {
        let m: ShardedMap<u64, String> = ShardedMap::new(4);
        assert_eq!(m.get(&1), None);
        assert_eq!(m.insert(1, "a".to_string()), None);
        assert_eq!(m.get(&1), Some("a".to_string()));
        assert_eq!(m.insert(1, "b".to_string()), Some("a".to_string()));
        assert_eq!(m.remove(&1), Some("b".to_string()));
        assert_eq!(m.get(&1), None);
    }

    #[test]
    fn test_distinct_keys_spread_across_shards_but_all_reachable() {
        let m: ShardedMap<u64, u64> = ShardedMap::new(4);
        for i in 0..100u64 {
            m.insert(i, i * 10);
        }
        assert_eq!(m.len(), 100);
        for i in 0..100u64 {
            assert_eq!(m.get(&i), Some(i * 10));
        }
    }

    #[test]
    fn test_with_entry_or_default_pushes_onto_existing_or_fresh_vec() {
        let m: ShardedMap<u64, Vec<u64>> = ShardedMap::new(4);
        m.with_entry_or_default(1, |v| v.push(10));
        m.with_entry_or_default(1, |v| v.push(20));
        assert_eq!(m.get(&1), Some(vec![10, 20]));
    }

    #[test]
    fn test_get_or_insert_with_creates_once_and_reuses_after() {
        let m: ShardedMap<u64, std::sync::Arc<u64>> = ShardedMap::new(4);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let make = || {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::sync::Arc::new(42)
        };
        let a = m.get_or_insert_with(1, make);
        let b = m.get_or_insert_with(1, make);
        assert!(std::sync::Arc::ptr_eq(&a, &b), "must return the SAME instance on repeat calls");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_single_shard_still_works() {
        let m: ShardedMap<u64, u64> = ShardedMap::new(1);
        m.insert(1, 100);
        m.insert(2, 200);
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.get(&2), Some(200));
        assert_eq!(m.len(), 2);
    }
}
