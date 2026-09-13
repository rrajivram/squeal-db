//! STORE_AUDIT.md P2 — contention benchmarks for `ArcLock`, the per-page
//! write lock every `PageBuffer::get_page_mut` call goes through.
//!
//! The audit's complaint: every `lock()` call takes the *write* side of one
//! global `RwLock<HashMap<PageId, _>>`, even when the keys involved are
//! completely disjoint (different pages, no real contention at all) — so
//! throughput on concurrent, independent pages is bottlenecked on that one
//! shared map lock, not on the pages themselves. `disjoint_keys_concurrent`
//! is the direct measurement of that: N threads, each hammering its own
//! distinct key, should scale close to linearly with thread count if the
//! only shared state is a brief, infrequent map lookup — and should barely
//! scale at all (or regress) if every lock/unlock serializes through one
//! global lock instead.
//!
//! `same_key_contended`/`same_key_contended_with_work` are the contrasting
//! case: N threads on the SAME key have to serialize no matter how the
//! implementation works, so these exist as a sanity check that a
//! disjoint-keys win isn't coming from accidentally weakening real mutual
//! exclusion — and, empirically, to honestly surface a real tradeoff: see
//! `benches/BASELINE.md`'s P2 section. Real per-key mutex contention (this
//! benchmark) turned out measurably SLOWER than the old spin-and-recheck
//! design for genuinely hot keys, even though it's a large win for the
//! disjoint case and for allocation count (see arclock.rs's own
//! `alloc_proxy_disjoint_keys_concurrent` test). End-to-end stress
//! throughput was flat either way — see BASELINE.md for the full picture
//! and reasoning before assuming either number in isolation tells the
//! whole story.
//!
//!     cargo bench -p store --bench arclock

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use store::arclock::ArcLock;

const THREADS: u64 = 8;
const OPS_PER_THREAD: u64 = 2_000;

fn disjoint_keys_concurrent(c: &mut Criterion) {
    c.bench_function("disjoint_keys_concurrent", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let lock: Arc<ArcLock<u64>> = ArcLock::new();
                let start = Instant::now();
                let handles: Vec<_> = (0..THREADS)
                    .map(|t| {
                        let l = lock.clone();
                        thread::spawn(move || {
                            // Each thread only ever touches its own key — no
                            // real contention should exist between threads.
                            let key = t;
                            for _ in 0..OPS_PER_THREAD {
                                let g = l.lock(key, 5_000_000).expect("must not time out");
                                drop(g);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        })
    });
}

fn same_key_contended(c: &mut Criterion) {
    c.bench_function("same_key_contended", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let lock: Arc<ArcLock<u64>> = ArcLock::new();
                let start = Instant::now();
                let handles: Vec<_> = (0..THREADS)
                    .map(|_| {
                        let l = lock.clone();
                        thread::spawn(move || {
                            for _ in 0..OPS_PER_THREAD {
                                let g = l.lock(0u64, 5_000_000).expect("must not time out");
                                drop(g);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        })
    });
}

// Same as `same_key_contended`, but each holder does ~1us of real work
// (a tight wrapping-add loop) before releasing, instead of dropping the
// guard immediately. Exists to rule out "the zero-work variant only
// measures who wins an empty-critical-section race, not real contention
// cost" — confirmed this doesn't change the finding: whichever
// implementation is faster/slower for `same_key_contended` is faster/
// slower here too, by roughly the same margin.
fn same_key_contended_with_work(c: &mut Criterion) {
    c.bench_function("same_key_contended_with_work", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let lock: Arc<ArcLock<u64>> = ArcLock::new();
                let start = Instant::now();
                let handles: Vec<_> = (0..THREADS)
                    .map(|_| {
                        let l = lock.clone();
                        thread::spawn(move || {
                            for _ in 0..OPS_PER_THREAD {
                                let g = l.lock(0u64, 5_000_000).expect("must not time out");
                                let mut x = 0u64;
                                for i in 0..300u64 {
                                    x = x.wrapping_add(i);
                                }
                                criterion::black_box(x);
                                drop(g);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        })
    });
}

fn uncontended_single_thread(c: &mut Criterion) {
    let lock: Arc<ArcLock<u64>> = ArcLock::new();
    c.bench_function("uncontended_single_thread", |b| {
        b.iter(|| {
            let g = lock.lock(0u64, 5_000_000).unwrap();
            drop(g);
        })
    });
}

criterion_group!(
    benches,
    disjoint_keys_concurrent,
    same_key_contended,
    same_key_contended_with_work,
    uncontended_single_thread
);
criterion_main!(benches);
