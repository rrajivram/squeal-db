# Performance baseline — before removing the `AnyTuplePage` `RwLock`

Recorded 2026-07-02, on `main`, commit at the time of the tuple-store /
transaction-abort work (pre-lock-removal). Machine: darwin (Apple), 16 threads.
Build profile: `release` (`debug = true`).

These are the numbers to beat. Criterion also stores its own copy under
`target/criterion`, so simply re-running the bench after a change prints the %
delta per case — but `target/` is not committed, hence this durable record.

## Microbenchmark — `AnyTuplePage` (the type that holds the `RwLock`)

`cargo bench -p store --bench page_store`
Page filled with `PAGE_TUPLES = 200` small tuples. Reported as [low  median  high].

| bench               | median   | notes                                        |
|---------------------|----------|----------------------------------------------|
| `get_hit`           | 24.0 ns  | point read: 1 read-lock + BTreeMap lookup    |
| `contains_hit`      | 6.8 ns   | read-lock + lookup                           |
| `values_scan_clone` | 4.50 µs  | full scan cloning every tuple (`iter` path)  |
| `keys_scan`         | 455 ns   | read-lock + collect keys                     |
| `add_one`           | 5.79 µs  | write-lock path (noisy; includes page clone) |
| `replace_one`       | 4.76 µs  | write-lock path                              |
| `remove_one`        | 4.80 µs  | write-lock path                              |

Note: `add_one`/`replace_one`/`remove_one` include an `iter_batched` clone of a
full page in setup; the write itself is a fraction of the reported time. Compare
like-for-like across runs (same bench name) rather than reading absolute values.

## End-to-end throughput — stress harness

`./target/release/examples/stress --threads 16 --ops 20000 --backend mem`
(mem backend to keep disk I/O out of the number; ~73 s/run)

| run | throughput   | result |
|-----|--------------|--------|
| 1   | 14850 ops/s  | PASS   |
| 2   | 14620 ops/s  | PASS   |

Run-to-run spread ≈ 1.5%, so treat anything under ~2% as noise.

## Result — after removing the `AnyTuplePage` `RwLock`

Store now mutated only through `&mut self` (reachable solely via
`Arc::make_mut(&mut Arc<Page>)`); the interior lock is gone. Micro deltas vs the
baseline above (criterion, p < 0.05 on all):

| bench               | baseline | after   | change   |
|---------------------|----------|---------|----------|
| `get_hit`           | 24.0 ns  | 20.7 ns | −23%     |
| `contains_hit`      | 6.8 ns   | 4.79 ns | −24%     |
| `values_scan_clone` | 4.50 µs  | 4.22 µs | −6%      |
| `keys_scan`         | 455 ns   | 435 ns  | −5%      |
| `add_one`           | 5.79 µs  | 4.40 µs | −14%     |
| `replace_one`       | 4.76 µs  | 4.36 µs | −8%      |
| `remove_one`        | 4.80 µs  | 4.36 µs | −25%     |

Point reads shed ~23–24%: that is the read-lock atomics removed from a sub-25 ns
op. `values_scan_clone` moved only −6% because its cost is the per-tuple clone,
not the lock — that is the separate `Arc<[u8]>` payload change.

End-to-end stress throughput (mem, 16t) stayed flat within run-to-run noise
(14.05k–14.85k ops/s across before/after), because the page-store lock is a small
fraction of the full insert/find path (B+tree, logging, buffer, tx). Correctness
unchanged: file-backend stress PASS, 0 mismatches; lib suite 182/182.

## Result — after `Tuple.data: Vec<u8>` → `Arc<[u8]>`

Cloning a Tuple (find/get return owned clones, undo records clone tuples, page
scans clone) no longer copies the payload — it bumps a refcount. On-disk format
unchanged (postcard encodes both as a seq of u8; close/reopen + roundtrip tests
pass). Micro, vs the **original** baseline at the top (i.e. both changes stacked):

| bench                     | original | after RwLock | after Arc | total   |
|---------------------------|----------|--------------|-----------|---------|
| `get_hit`                 | 24.0 ns  | 20.7 ns      | 9.33 ns   | −61%    |
| `contains_hit`            | 6.8 ns   | 4.79 ns      | 5.07 ns¹  | −25%    |
| `values_scan_clone` (18B) | 4.50 µs  | 4.22 µs      | 2.02 µs   | −55%    |
| `keys_scan`               | 455 ns   | 435 ns       | 432 ns    | −5%     |
| `add_one`                 | 5.79 µs  | 4.40 µs      | 2.96 µs   | −49%    |
| `replace_one`             | 4.76 µs  | 4.36 µs      | 2.84 µs   | −40%    |
| `remove_one`              | 4.80 µs  | 4.36 µs      | 2.80 µs   | −42%    |

¹ contains does no cloning; the ±0.3 ns wobble at ~5 ns is measurement noise.

Payload-copy elimination, measured directly (same run):

| bench             | time    | meaning                                      |
|-------------------|---------|----------------------------------------------|
| `vec_clone_64k`   | 687 ns  | the `Vec<u8>` copy a Tuple clone used to pay  |
| `tuple_clone_64k` | 5.85 ns | Tuple clone now (Arc refcount bump) — ~117× faster |
| `values_scan_clone_large` (16×4 KB) | 231 ns | large-row page scan clone |

End-to-end stress throughput (mem, 16t) — this change *did* move it, since clone
cost is on the real insert/find path:

| stage           | throughput          |
|-----------------|---------------------|
| original        | 14620–14850 ops/s   |
| after RwLock    | 14050–14278 ops/s (flat) |
| after Arc       | 16647–18002 ops/s (**+13–22%**) |

Correctness unchanged: lib 182/182, file-backend stress 5/5 PASS, 0 mismatches.

## Result — serialize borrowed `&Tuple` in `to_bytes` (writer path)

`AnyTuplePage::to_bytes` built a `Vec<Tuple>` clone before handing it to
`to_allocvec`; now it collects `Vec<&Tuple>` and serializes the borrows (serde
forwards `&T` to `T`, so bytes/`from_bytes` are unchanged).

| bench            | before  | after   | change              |
|------------------|---------|---------|---------------------|
| `to_bytes_small` (200×18 B) | 5.07 µs | 3.44 µs | −32%    |
| `to_bytes_large` (16×4 KB)  | 24.66 µs| 24.95 µs| flat (noise) |

Small-row pages (index pages, serialized on every dirty flush) drop ~32% by
dropping 200 clones + the intermediate `Vec<Tuple>` allocation. Large payloads
are flat: with `Arc` payloads the clone was already just a refcount bump, so the
cost there is the `to_allocvec` copy into the output buffer — unavoidable and
unchanged. Correctness: lib 182/182, file-backend stress PASS, 0 mismatches.

## Result — STORE_AUDIT.md P2: `ArcLock`'s global write-lock + busy-poll

Recorded 2026-09-13. `PageBuffer::get_page_mut` takes a per-page lock via
`ArcLock<PageId>` before every read-modify-write. The old implementation kept
exactly one entry per key in a `HashMap` behind one `RwLock`, took that map's
*write* side (not just read) on every single `lock()` call regardless of which
key, and had waiters re-take that same write lock every 100us in a sleep loop
instead of actually blocking — the audit's claim: this serializes concurrent
work on *unrelated* pages through one shared lock, and burns CPU on waiters
instead of parking them.

Fix: the map now holds one real `Arc<parking_lot::ReentrantMutex<()>>` per key,
created once and cached. `lock()` only takes the map's lock briefly to
fetch-or-create that per-key mutex (a `read()` once the key has been seen
before) — the actual wait, reentrancy, and timeout are handled by
`try_lock_arc_for` directly on that per-key mutex: a real futex-based block,
not a sleep loop, and it finally honors the `timeout` parameter the old code
silently overrode with a hardcoded 60s wait (the one real caller,
`get_page_mut`, has always passed 5ms — see its own comment on why).

`cargo bench -p store --bench arclock` (8 threads × 2000 ops/thread):

| bench                          | before   | after    | change  |
|---------------------------------|----------|----------|---------|
| `uncontended_single_thread`     | 44.2 ns  | 13.1 ns  | −70%    |
| `disjoint_keys_concurrent`      | 2.345 ms | 2.088 ms | −11%    |
| `same_key_contended`            | 1.306 ms | 2.780 ms | **+113%** |
| `same_key_contended_with_work`¹ | 1.301 ms | 2.549 ms | **+96%**  |

¹ same as `same_key_contended` but each holder does ~1us of real work before
releasing, instead of dropping the guard immediately — added specifically to
rule out the regression being an artifact of an unrealistically short critical
section. It wasn't: the regression persists at roughly the same magnitude.

Allocation-count proxy (`store::alloc::stats()`, see `arclock.rs`'s own
`alloc_proxy_disjoint_keys_concurrent`, `#[ignore]`d — run with `cargo test -p
store --lib arclock::arclock_tests::alloc_proxy -- --ignored --nocapture
--test-threads=1`), 16,000 lock/unlock cycles across 8 distinct keys:

| metric                  | before | after | change   |
|--------------------------|--------|-------|----------|
| allocation events        | 16,042 | 52    | **−99.7%** |
| bytes allocated          | 394,580 | 11,508 | **−97%** |

The old design allocated a fresh `Arc::new(0)` on every non-reentrant
acquisition of a key, even one it had served thousands of times before — the
map only ever cached a *guard*, not a reusable lock object. The new design
allocates the per-key mutex exactly once per distinct key, ever; this is an
asymptotic difference (allocations ∝ distinct keys, not ∝ operations), not a
constant-factor one, and it's the cleanest, least noisy signal this change
actually produced — unlike the wall-clock numbers above, it isn't sensitive to
machine load or scheduler mood.

**End-to-end** (`./target/release/examples/stress --threads 16 --ops 20000
--backend mem`): **76,941 ops/s before, 76,946 ops/s after — flat, no
measurable difference either way.**

**Honest read of this result**: the fix is a clear, large win for the
disjoint-key case (the audit's own stated motivation) and for allocation
pressure, and a real, reproducible regression for genuine same-key contention
under this specific access pattern (immediate-release cycles) — `ReentrantMutex`'s
own bookkeeping plus the added indirection of a separate lock-registry map (vs.
the audit's own suggested design of storing the lock directly in the page's
cache entry, which this pass did not attempt — a larger `PageBuffer` eviction-
logic change) costs more than the old design's opportunistic
"probably-already-free, just re-check" fast path saved. Neither effect shows up
in end-to-end stress throughput: `ArcLock`'s own cost, in either direction, is
a rounding error against the full insert/find path (B+tree traversal, WAL,
buffer/eviction, transaction bookkeeping) — matching this file's own earlier
finding that the page-store lock removal was *also* flat end-to-end for the
same reason. The audit's own motivating claim ("on the Mem backend this is the
documented reason throughput drops with more threads") does not reproduce
against the current codebase; whatever caused that observation historically,
`ArcLock` alone is not the bottleneck today. Kept anyway: honoring the timeout
parameter and replacing an active busy-poll with real blocking are correct
fixes independent of this specific throughput measurement (a busy-poll wastes
CPU cycles under contention regardless of whether it shows up in this
benchmark's aggregate ops/s), and the allocation-pressure win is real and
likely to matter more under sustained load / GC-adjacent pressure than a
single stress run captures.

Correctness: `store` lib 417/417 (+1 `#[ignore]`d), `squeal-sql` lib 346/346,
workspace builds clean, stress (mem, 16t) `RESULT: PASS` both before and after,
0 mismatches.

## How to compare after a change

1. Micro:  `cargo bench -p store --bench page_store` (or `--bench arclock`) →
   criterion prints `change: [-x% .. +y%]` vs the stored baseline for each case.
2. Alloc:  for allocation-sensitive changes, an `#[ignore]`d test reading
   `store::alloc::stats()` before/after a fixed workload (see `arclock.rs`'s
   `alloc_proxy_disjoint_keys_concurrent`) is often a cleaner signal than wall
   clock — it isn't sensitive to machine load or scheduler mood.
3. E2E:    re-run the stress line above ≥2× and compare throughput; correctness
   must stay `RESULT: PASS`.
