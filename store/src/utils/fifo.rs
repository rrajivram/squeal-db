use std::{
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, Hash},
};

use parking_lot::Mutex;

// Items in the order they were last pushed, oldest first: the pages
// eviction considers (see PageBuffer::evict_one). Pushing an item that is
// already queued moves it to the back, as the priority queue this replaced
// did — each item's newest position is kept, and `pop` passes over the
// older ones. Every operation is O(1) amortized, under one lock. That
// priority queue (keyed by insertion order) was sharded, and its pop read-
// locked every shard (819 of them at the default cache size) to find the
// oldest: 13% of a scan bigger than the cache.
pub(crate) struct Fifo<I, S> {
    inner: Mutex<Inner<I, S>>,
}

struct Inner<I, S> {
    // Every push, oldest first, with its sequence number.
    queue: VecDeque<(I, u64)>,
    // Each queued item's newest sequence number; an entry of `queue` with
    // an older one is stale.
    newest: HashMap<I, u64, S>,
    next: u64,
}

impl<I, S> std::fmt::Debug for Fifo<I, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fifo")
            .field("len", &self.inner.lock().newest.len())
            .finish()
    }
}

impl<I, S> Fifo<I, S>
where
    I: Hash + Eq + Copy,
    S: BuildHasher + Default,
{
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                queue: VecDeque::new(),
                newest: HashMap::default(),
                next: 0,
            }),
        }
    }

    /// Queues `item` at the back, wherever it was.
    pub(crate) fn push(&self, item: I) {
        let mut inner = self.inner.lock();
        let seq = inner.next;
        inner.next += 1;
        inner.newest.insert(item, seq);
        inner.queue.push_back((item, seq));
        // Stale entries leave when popped; one pushed over and over while
        // queued would pile them up, so past twice the live count they go.
        if inner.queue.len() > 2 * inner.newest.len() + 64 {
            let Inner { queue, newest, .. } = &mut *inner;
            queue.retain(|(i, s)| newest.get(i) == Some(s));
        }
    }

    /// The item pushed longest ago, taken off the queue.
    pub(crate) fn pop(&self) -> Option<I> {
        let mut inner = self.inner.lock();
        while let Some((item, seq)) = inner.queue.pop_front() {
            if inner.newest.get(&item) == Some(&seq) {
                inner.newest.remove(&item);
                return Some(item);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::hash::RandomState;

    use super::*;

    #[test]
    fn test_pops_oldest_first_and_a_repush_moves_to_the_back() {
        let q = Fifo::<u64, RandomState>::new();
        for i in 0..4 {
            q.push(i);
        }
        q.push(1);
        assert_eq!(q.inner.lock().newest.len(), 4);
        let order: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(order, vec![0, 2, 3, 1]);
        assert_eq!(q.inner.lock().newest.len(), 0);
    }

    #[test]
    fn test_pushing_one_item_over_and_over_stays_bounded() {
        let q = Fifo::<u64, RandomState>::new();
        q.push(7);
        for _ in 0..10_000 {
            q.push(1);
        }
        assert!(q.inner.lock().queue.len() <= 2 * 2 + 64 + 1);
        assert_eq!(std::iter::from_fn(|| q.pop()).collect::<Vec<_>>(), vec![7, 1]);
    }
}
