use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const NUM_LATENCY_BUCKETS: usize = 28; // covers <1us .. >=~134s in power-of-two steps

#[derive(Debug, Default)]
pub struct OpCounters {
    pub success: AtomicU64,
    pub key_not_found: AtomicU64,
    pub duplicate_key: AtomicU64,
    pub lock_contention: AtomicU64,
    pub other_error: AtomicU64,
}

impl OpCounters {
    pub fn total(&self) -> u64 {
        self.success.load(Ordering::Relaxed)
            + self.key_not_found.load(Ordering::Relaxed)
            + self.duplicate_key.load(Ordering::Relaxed)
            + self.lock_contention.load(Ordering::Relaxed)
            + self.other_error.load(Ordering::Relaxed)
    }
}

pub struct Stats {
    pub start: Instant,
    pub completed_ops: AtomicU64,
    pub insert: OpCounters,
    pub update: OpCounters,
    pub remove: OpCounters,
    pub find: OpCounters,
    pub txn_committed: AtomicU64,
    pub txn_rolled_back: AtomicU64,
    pub dropped_after_retry_exhaustion: AtomicU64,
    pub latency_buckets: Vec<AtomicU64>,
    /// Millis-since-start of each thread's last completed op; used by the
    /// watchdog to report exactly which threads are stuck.
    pub thread_last_activity_ms: Vec<AtomicU64>,
    pub thread_ops_done: Vec<AtomicU64>,
    pub peak_rss_kb: AtomicU64,
}

impl Stats {
    pub fn new(num_threads: usize) -> Self {
        Self {
            start: Instant::now(),
            completed_ops: AtomicU64::new(0),
            insert: OpCounters::default(),
            update: OpCounters::default(),
            remove: OpCounters::default(),
            find: OpCounters::default(),
            txn_committed: AtomicU64::new(0),
            txn_rolled_back: AtomicU64::new(0),
            dropped_after_retry_exhaustion: AtomicU64::new(0),
            latency_buckets: (0..NUM_LATENCY_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            thread_last_activity_ms: (0..num_threads).map(|_| AtomicU64::new(0)).collect(),
            thread_ops_done: (0..num_threads).map(|_| AtomicU64::new(0)).collect(),
            peak_rss_kb: AtomicU64::new(0),
        }
    }

    pub fn record_latency(&self, d: Duration) {
        let micros = d.as_micros().max(1);
        // floor(log2(micros)) — micros is u128, so leading_zeros is in 0..=127
        let bucket = (127 - micros.leading_zeros() as usize).min(NUM_LATENCY_BUCKETS - 1);
        self.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_thread_progress(&self, thread_idx: usize) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        self.thread_last_activity_ms[thread_idx].store(elapsed_ms, Ordering::Relaxed);
        self.thread_ops_done[thread_idx].fetch_add(1, Ordering::Relaxed);
        self.completed_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Bucket lower-bound in microseconds, for reporting.
pub fn bucket_lower_bound_us(bucket: usize) -> u64 {
    1u64 << bucket
}
