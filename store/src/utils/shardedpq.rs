use std::{hash::Hash, ops::Rem};

use parking_lot::RwLock;
use priority_queue::PriorityQueue;

#[derive(Debug)]
pub(crate) struct ShardedPQ<I, P> {
    queues: Vec<RwLock<PriorityQueue<I, P>>>,
}

impl<I, P> ShardedPQ<I, P>
where
    P: std::cmp::Ord + Copy,
    I: Hash + Eq + Rem<Output = usize> + From<usize> + Copy,
{
    pub(crate) fn new(shards: usize) -> Self {
        let mut queues = Vec::with_capacity(shards);
        for _ in 0..shards {
            queues.push(RwLock::new(PriorityQueue::new()));
        }
        Self { queues }
    }

    pub(crate) fn contains(&self, item: &I) -> bool {
        let shard = *item % self.queues.len().into();
        self.queues[shard].read().contains(item)
    }

    pub(crate) fn push(&self, item: I, priority: P) -> Option<P> {
        let shard = item % self.queues.len().into();
        self.queues[shard].write().push(item, priority)
    }

    pub(crate) fn change_priority(&self, item: &I, new_priority: P) -> Option<P> {
        let shard = *item % self.queues.len().into();
        self.queues[shard]
            .write()
            .change_priority(item, new_priority)
    }

    #[allow(clippy::unnecessary_map_or, clippy::collapsible_if)]
    pub(crate) fn pop(&self) -> Option<(I, P)> {
        loop {
            // PriorityQueue::pop() is max-first, so the global pop must be
            // too: peek every shard (read lock only) and track the best.
            // Only the winning shard ever needs a write lock.
            let mut best: Option<(usize, I, P)> = None;
            for (idx, q) in self.queues.iter().enumerate() {
                if let Some((&item, &priority)) = q.read().peek() {
                    if best.map_or(true, |(_, _, best_p)| priority > best_p) {
                        best = Some((idx, item, priority));
                    }
                }
            }
            let (shard, item, _) = best?;

            // Re-check under the write lock: another thread may have popped
            // or pushed a new max into this shard between the peek above and
            // now. If so, recompute the global max from scratch.
            let mut q = self.queues[shard].write();
            if q.peek().map(|(&i, _)| i) == Some(item) {
                return q.pop();
            }
        }
    }
}
