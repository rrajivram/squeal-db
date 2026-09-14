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

## Result — P2 follow-up: shard the lock registry (16 shards, hashed by key)

Prompted by a direct question: does `disjoint_keys_concurrent`'s modest −11%
(above) mean the single `RwLock` around the lock-registry map is *also* a
bottleneck, the same way `buffer.rs`'s `access_map` needed `ShardedPQ` to stop
being one? Yes — even taken only on its `read()` side (the common path once a
key exists), one `RwLock` still has a single shared reader-count atomic that
every thread's read()/drop bounces across cores; 8 threads all touching that
one cache line, 2000 times each, is exactly the kind of false sharing sharding
exists to remove. Sharded the map itself (`Vec<RwLock<HashMap<...>>>>`, picked
by `T`'s own `Hash` — more general than `ShardedPQ`'s `Rem<usize>` scheme,
which only works because its key is numeric) rather than anything about the
per-key `ReentrantMutex`, since the hypothesis was specifically about the
*registry's* lock, not the per-key one.

`cargo bench -p store --bench arclock`, all three points now for comparison:

| bench                          | original | unsharded fix | **sharded (16)** | Δ vs original | Δ vs unsharded |
|---------------------------------|----------|----------------|-------------------|----------------|-----------------|
| `uncontended_single_thread`     | 44.2 ns  | 13.1 ns        | 16.1 ns           | −64%           | +23% (hash cost) |
| `disjoint_keys_concurrent`      | 2.345 ms | 2.088 ms       | **0.324 ms**      | **−86%**       | **−84%**        |
| `same_key_contended`            | 1.306 ms | 2.780 ms       | 3.229 ms          | +147%          | +16%            |
| `same_key_contended_with_work`  | 1.301 ms | 2.549 ms       | 3.182 ms          | +145%          | +25%            |

Confirms the hypothesis precisely: sharding turns the disjoint-key case from a
disappointing −11% into a genuinely large −86%, because it was never really
about the map's *contents* (each key's own entry) — it was the registry lock's
own internal state being hammered by every thread regardless of which key they
wanted. It does nothing for `same_key_contended` (every thread hashing to the
SAME key still lands on the SAME shard, so that case is exactly as contended
as the unsharded design) and adds a small, expected tax everywhere else — one
`DefaultHasher` computation per call — visible in `uncontended_single_thread`
and compounding the existing same-key regression a bit further.

Allocation proxy, same workload: 41 events / 2,144 bytes (down from 52 / 11,508
unsharded, 16,042 / 394,580 original) — fewer, not more, despite 16 pre-
allocated shard maps up front, since spreading entries across shards means
less per-shard `HashMap` resizing overhead than one map absorbing all of them.

**End-to-end** (same stress command): 76,924 ops/s — flat again, consistent
with every other variant measured here. `ArcLock` genuinely is not what this
particular workload's throughput is bottlenecked on, so none of these designs
(original, unsharded fix, sharded fix) is distinguishable at the whole-system
level for it. That doesn't make the micro-level results meaningless — a
different, more page-contended workload (many transactions hammering few hot
rows, or a working set close to `max_entries` under heavy churn) would be far
more likely to actually feel the difference between these three, in either
direction.

**Kept: the sharded design.** The disjoint-key case is the audit's own stated
motivation and the realistic common case (a working database touches many
different pages far more often than one thread hammers a single page with no
other work between acquisitions); the same-key cost, while real, is worse in a
narrower, less representative scenario and — per the reasoning above — could
likely be closed separately by the audit's own alternate suggestion (lock
inside the page's cache entry, no separate registry lookup at all), still
flagged as unattempted follow-up work.

Correctness: `store` lib 417/417 (+1 `#[ignore]`d), `squeal-sql` lib 346/346,
workspace builds clean, stress (mem, 16t) `RESULT: PASS`, 0 mismatches.

## Result — P2 survey follow-up: shard `logger.rs`'s undo maps and `db.rs`'s `table_locks`

Prompted by asking where else in `store/src` the same pattern (a hot, read-mostly registry
whose *own* `RwLock` state, not its contents, is the bottleneck) shows up — full survey in
`audit-progress.md`'s P2 entry. Two candidates matched `ArcLock`'s exact shape (independent
keys, no cross-key invariants): `logger.rs`'s `records`/`by_txn` (written on every insert/
update/remove, read on every rollback/discard) and `db.rs`'s `table_locks` (this doc's own T17
fix). Extracted the sharding logic into a small, reusable `utils::shardedmap::ShardedMap<K, V>`
rather than hand-rolling it a third and fourth time; both now use it (16 shards, same as
`ArcLock`).

No dedicated micro-benchmark for these two — the physical effect being fixed (one shared
reader-count atomic bouncing across cores) is the *exact* one already measured for `ArcLock`
above; a second benchmark would confirm the same physics again, not add evidence. Verified via
the full test suite (422/422 `store`, +5 new `ShardedMap` unit tests, 346/346 `squeal-sql`) and
one end-to-end stress run: **76,958 ops/s** — consistent with every other number in this file's
P2 section (flat; this stress workload isn't bottlenecked on these locks in any design tried).
`db.rs`'s `tables` map and `buffer.rs`'s main page-cache map were surveyed and deliberately NOT
sharded this pass (few enough distinct tables to blunt the benefit; real cross-key invariants
in the page cache's eviction path respectively) — see `audit-progress.md` for the full reasoning
per candidate.

Correctness: `store` lib 422/422 (+1 `#[ignore]`d), `squeal-sql` lib 346/346, workspace builds
clean, stress (mem, 16t) `RESULT: PASS`, 0 mismatches.

## Result — STORE_AUDIT.md P3: CLOCK eviction instead of timestamp + heap update per access

`PageBuffer::get_page`'s cache-hit path called `update_page_access` (a `timestamp()` call plus a
`ShardedPQ::change_priority`, an O(log n) heap reorder under a shard lock) and, separately,
`Page::accessed()` (another `timestamp()` call) on *every* hit — the overwhelming majority of
all page accesses. A throwaway, since-deleted microbenchmark isolating just this cost (2x
`timestamp()` + one `change_priority`, vs. a bare `AtomicBool` swap) measured **~47ns vs. ~1ns**
— confirmed worth fixing before touching the eviction logic.

Fix: `Page` gains `referenced: AtomicBool` (replacing the `accessed`/`saved`/`written:
AtomicU128` fields the audit also flagged as dead — confirmed via grep that nothing ever read
any of the three back). A hit now just calls `mark_referenced()` — a relaxed store, no lock, no
syscall, no heap touch. `access_map` (`ShardedPQ`, internals unchanged) is now keyed by a
monotonic insertion sequence instead of a timestamp, and is only touched when a page first
becomes Strong or during an eviction sweep — never on an ordinary hit. Eviction checks
`take_referenced()` before committing: a page accessed since it was last considered gets a
second chance (cleared, re-pushed with a fresh sequence, sweep continues) instead of being
evicted immediately.

No new criterion bench for the end-to-end hit-path cost — the isolated 47x number above already
demonstrates the mechanism directly, and the interesting remaining question (does this show up
in real page-cache-heavy workloads) is the same "flat at the E2E level for this stress
workload" story every other change in this file has told, confirmed below rather than
re-litigated per change.

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): 76,908 ops/s — flat,
consistent with the rest of this phase.

Correctness: `store` lib 423/423 (+1 `#[ignore]`d), `squeal-sql` lib 346/346, workspace builds
clean, stress `RESULT: PASS`, 0 mismatches.

## Result — P3 follow-up, same pass: shard `buffer` itself (16 shards, hashed by `PageId`)

The P3 fix above only changed what feeds `access_map`; `buffer` (the main
`PageId -> PageEntry` cache map every `get_page`/`get_page_mut` call touches) was still a single
`RwLock<HashMap<..>>` — flagged by the earlier P2 survey as the highest-traffic lock left
unsharded, deferred pending its own investigation because eviction needs a cache-wide "which page
is globally oldest" answer, which a naively sharded map can't give per-shard alone.

Fix: kept `strong_count`/`access_map` global (`access_map` is already a `ShardedPQ`, which shards
itself internally and exposes a global-max `pop()`), and sharded only
`buffer: Vec<RwLock<HashMap<PageId, PageEntry>>>` (16 shards, hashed by `PageId` via a new
`shard_for` helper). Collapsed `cache_strong_locked`/`get_or_install`/`evict_lru_locked` (each
written against one `&mut HashMap` under one caller-held lock) into `install(page_num, page,
mode: InstallMode)` + a standalone `evict_one()`, since sharding removes the single lock that used
to make "check if already Strong, evict if needed, insert" atomic for free. `install` never holds
two shards' locks at once — it drops the target shard's lock entirely before calling `evict_one`
(which locks only the victim's own, possibly different, shard), then loops back to recheck. No
lock-ordering discipline needed, so no deadlock risk. `InstallMode` (`Overwrite` for writers,
`ReuseIfPresent` for readers) preserves the original reader-vs-writer "what if it's already
Strong" semantics; `Evicted` (`Yes(Option<..>)` vs `Exhausted`) tells "evicted a clean page" apart
from "nothing left to evict", which the retry loop needs to handle differently.

New throwaway (not committed as a criterion bench, same call as P3's own microbenchmark above)
in-crate `#[ignore]`d test, `bench_get_page_concurrent_disjoint_pages`: 8 threads, each hammering
`get_page` on its own disjoint slice of a pre-warmed, all-Strong cache — isolates lock contention
from disk I/O or eviction. Run via `cargo test --release -- --ignored`, alone, on this revision
and on the pre-sharding one (`git show 0c176ec:store/src/buffer.rs`, patched with the identical
bench):

| revision | ops/s (3 runs) |
|---|---|
| before (single `RwLock<HashMap<..>>`) | 22.1M / 26.6M / 20.8M |
| after (16-shard `Vec<RwLock<HashMap<..>>>`) | 38.3M / 37.9M / 39.1M |

A real, reproducible ~55-70% win. Smaller than `ArcLock`'s disjoint-key win because `get_page`'s
hot path already only takes a *read* lock — `parking_lot::RwLock` readers don't serialize against
each other even unsharded. The win here is purely from spreading the lock's own reader-count
atomic across independent cache lines instead of every thread bouncing the same one.

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): 76,884–76,931 ops/s
across two runs — flat, same story as every other change in this file: this workload isn't
bottlenecked on this lock either way.

Correctness: `store` lib 423/423 (+2 `#[ignore]`d), `squeal-sql` lib 346/346, workspace builds
clean, stress `RESULT: PASS`, 0 mismatches.

## Result — STORE_AUDIT.md P7: hoist the per-row snapshot clone; merge active/aborting txn sets

`Db::find_visible_to` resolved AND cloned the reader's whole snapshot `HashSet<TransactionId>` on
every call — `TableCursor::next`/`RangeCursor::next` call it once per row candidate, so a scan of
M rows under N concurrently active transactions did M clones of an N-entry set, all to answer a
question whose answer never changes for the life of the scan (a snapshot is captured once at
`begin()` and never mutated after).

Fix: `find_visible_to` now takes the resolved snapshot as a `&HashSet` parameter (via a new
`Db::snapshot_of` helper) instead of resolving it internally. `TableCursor`/`RangeCursor` each
resolve it once, in their constructor, and reuse it for every row. `Db::find` (a single lookup,
not a loop) resolves its own one-off snapshot right before the call — unchanged cost for that
caller.

Allocation-count proxy (`store::alloc::stats()`, `cursor::tests::alloc_proxy_table_scan_snapshot`,
`#[ignore]`d) — scan of 2,000 rows, 100 concurrently active noise transactions:

| revision | allocation events | bytes | bytes/row |
|---|---|---|---|
| before | 2,026 | 2,765,383 | 1382.69 |
| after | 27 | 446,543 | 223.27 |

Run by executing the identical caller-level test (just `table_scan` + a `next()` loop — the fix
is entirely internal, invisible to the test itself) against both this revision and
`git show <pre-fix>:store/src/{db,cursor}.rs`.

**Second half, same pass** — the audit's own suggested companion fix: `TransactionManager::
is_committed` (called on every undo-chain hop) took two separate `RwLock` reads, one over
`active_transactions: RwLock<HashSet<TransactionId>>` and a second, independent
`aborting_transactions: RwLock<HashSet<TransactionId>>`. Collapsed into one `transaction_states:
RwLock<HashMap<TransactionId, TxnState>>` (`Active` | `Aborting`; absence from the map is still
"committed", matching the old zero-footprint-for-committed-txns design). `is_committed` drops to
one lock read; `abort` drops from two lock acquisitions to one (and incidentally closes a narrow,
previously-unflagged race: the old two-step abort had a real window where a concurrent
`is_committed` could see the txn absent from both sets and momentarily misreport it committed).

Throwaway wall-clock measurement (`txn::tests::bench_is_committed_concurrent`, `#[ignore]`d): 8
threads calling `is_committed` on a never-registered id, against a manager pre-populated with 100
Active/Aborting noise transactions:

| revision | ops/s (3 runs) |
|---|---|
| before (two `RwLock`s) | 3.36M / 3.37M / 3.56M |
| after (one merged `RwLock`) | 12.3M / 12.6M / 13.1M |

~3.5-4x — bigger than a simple "half the locks" 2x, consistent with contended `RwLock` read
acquisition across independent cache lines scaling worse than linearly with lock count.

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): 76,878–76,958 ops/s
across two runs — flat, same story as every other change in this file.

Correctness: `store` lib 423/423 (+4 `#[ignore]`d total this session), `squeal-sql` lib 346/346,
workspace builds clean, stress `RESULT: PASS`, 0 mismatches.

## Result — STORE_AUDIT.md P9: skip the overflow-chain rebuild when its length hasn't changed

`handle_large_page_size` tore down and rebuilt a whole overflow chain on every write to an
already-oversized page — free every existing continuation page (each re-written blank
synchronously), then allocate a brand-new chain with fresh page ids and N synchronous
`write_page_header` pwrites — even when the required page count hadn't changed at all.
Documented in `ARCHITECTURE.md` as the reason large-value writes ran at ~600-1000/s.

Fix: `Page` gains `overflow_page_count` (bundled into the same lock as `has_overflow`/`next_page`
for the same tearing-prevention reason). If the existing chain already has exactly the length the
new size needs, `handle_large_page_size` returns immediately — no free, no realloc, no writes at
all. A page freshly loaded from disk always starts at 0 (can never falsely match a real length, always
`>= 1`), so a cold page's first oversized write still takes the full rebuild path; the fast path
only kicks in once the same cached `Arc<Page>` has already built a chain once, which is exactly the
case for repeated updates to the same large-value row.

Throwaway (not a committed criterion bench) wall-clock measurement, `buffer::tests::
bench_repeated_same_size_overflow_write`, `#[ignore]`d — 2,000 repeated same-size updates to a
single already-oversized page (mem backend, so this is a conservative CPU/allocation-only proxy;
the audit's ~600-1000/s figure was against real disk I/O, where the now-eliminated synchronous
pwrites carry real latency this proxy can't capture):

| revision | writes/s (3 runs) |
|---|---|
| before (always rebuild) | 618K / 1.32M / 1.36M |
| after (reuse unchanged chain) | 2.60M / 5.96M / 6.27M |

Roughly 2-4x even without real disk latency in the mix — expect a much larger relative win on a
real disk backend, where this change turns O(chain length) synchronous pwrites into zero.

Second suggested fix in the audit ("queue continuation-page writes through the writer thread")
deliberately NOT implemented this pass — scoped and rejected as unsafe to do as a drop-in change;
see `audit-progress.md`'s P9 entry for the full reasoning (the async writer thread's content-walk,
and any subsequent `handle_large_page_size` call for the same page, both depend on the chain's
structural writes already being durable by the time they run — making those async reopens a
read-before-write race that needs its own dedicated design, not a quick swap).

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): 76,873 ops/s — flat;
this workload isn't oriented around large overflow-value updates specifically.

Correctness: `store` lib 424/424 (+5 `#[ignore]`d total this session), `squeal-sql` lib 346/346,
workspace builds clean, stress `RESULT: PASS`, 0 mismatches.

## Result — STORE_AUDIT.md P4: drop the dead txn_id from index/routing entries

Index/routing entries (`Node::Inner`/`Node::Leaf`-encoded `Tuple`s, resolved purely by key) were
unconditionally stamped with a live `TransactionId` on every construction site, despite nothing
ever reading it back — visibility is resolved entirely through the DATA tuple. Fixed by passing
`None` at all 6 construction sites (`Option<TransactionId>` already serializes to a 1-byte
discriminant for `None`, no format restructuring needed) and shrinking `MAX_ENTRY_BYTES` (the
worst-case per-entry budget controlling `nodes_per_page = page_size / index_entry_size`) from 64
to 48 to reflect it.

Measured actual per-entry byte savings (`Tuple::size()`, not just the worst-case budget) —
postcard's varint cost scales with the runtime VALUE, not the field's declared type width, so the
real savings depends on how "mature" the database is:

| TransactionId magnitude | with live txn_id | without (None) | saved |
|---|---|---|---|
| small (id=1, ts=1 — a fresh test db) | 12 B | 10 B | 2 B |
| large (id/ts ≈ 50,000,000 — a mature db) | 18 B | 10 B | 8 B |

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): **76,873 → 89,517-89,625
ops/s (3 runs) — a genuine, reproducible ~16.5% improvement.** The first fix in this whole
performance pass to move the E2E number at all — every other one (P2, P3, P7, P9, buffer-sharding)
stayed flat. Makes sense here specifically: smaller index entries mean shallower trees and less
split/allocation overhead during the stress workload's own inserts, not just smaller on-disk bytes.

Correctness: `store` lib 426/426 (+5 `#[ignore]`d total this session), `squeal-sql` lib 346/346,
workspace builds clean, stress `RESULT: PASS`, 0 mismatches.

Deliberately not done: narrowing `TransactionInner.ts` from `u128` to `u64` (the audit's other
suggested P4 lever) — investigated and found near-zero-value now that T11 made `ts` a small
logical counter rather than a wall-clock nanosecond timestamp: postcard's varint cost tracks the
runtime value, not the declared width, so a `u128` holding a small counter already encodes
byte-identically to a `u64` holding the same value. See `audit-progress.md`'s P4 entry.

## Result — STORE_AUDIT.md P5: successor() instead of clone-every-tuple-then-linear-scan

Every inner-node routing decision (`route_to_leaf`, `remove_index_entry`, `update_index_entry`,
`insert_recursive`'s split-target scan) answered "which child covers this key" via `Page::iter()`
— clones EVERY tuple on the page into a `Vec`, then linearly scans it, `postcard`-decoding each
entry until the first match. O(N) clone + up to O(N) decodes for what's structurally one B-tree
range query; P4's own fanout increase made N bigger for the same page_size, raising this cost
further.

Fix: `PageTuple::successor(&id)` — "smallest key strictly greater than id" — backed by
`BTreeMap::range((Excluded(id), Unbounded)).next()` in `AnyTuplePage` (O(log N), clones only the
matched entry). All 4 call sites now do `successor(id).or_else(last)` (both O(log N), no
full-page clone) instead of the old scan.

Throwaway (not a committed criterion bench) wall-clock measurement, `tables::bplustree::tests::
bench_find_traversal_cost`: 20,000 scattered `find()` lookups over a 20,000-row tree at a 16 KiB
page size (approaching the audit's own `nodes_per_page ≈ 256` reference case):

| revision | lookups/s (3 runs) |
|---|---|
| before (clone-and-scan) | 726K / 860K / 877K |
| after (successor-based range query) | 3.02M / 3.06M / 3.42M |

Roughly 3.5-4x.

**End-to-end** (`examples/stress --threads 16 --ops 20000 --backend mem`): **89,517 (post-P4) →
107,254-107,308 ops/s (3 runs) — another genuine ~20% improvement, stacking on P4's own ~16.5%.
Cumulative from this whole performance pass's original 76,873 ops/s baseline: +39.5%.** Makes
sense given how central `route_to_leaf` is — every `find`/`insert`/`update`/`remove` walks it.

Correctness: `store` lib 430/430 (+6 `#[ignore]`d total this session), `squeal-sql` lib 346/346,
workspace builds clean, stress `RESULT: PASS`, 0 mismatches.

Deliberately not done: the audit's other suggested P5 lever, a fixed 9-byte `Node` encoding
readable without `postcard`. `successor` already cuts each routing decision to exactly one
`from_bytes::<Node>` call per tree level (down from up to N) — the O(N)→O(1) fix above already
captures the large majority of the win; a fixed layout would only shave that one remaining decode,
a much smaller marginal return for a genuine on-disk format change. See `audit-progress.md`.

## How to compare after a change

1. Micro:  `cargo bench -p store --bench page_store` (or `--bench arclock`) →
   criterion prints `change: [-x% .. +y%]` vs the stored baseline for each case.
2. Alloc:  for allocation-sensitive changes, an `#[ignore]`d test reading
   `store::alloc::stats()` before/after a fixed workload (see `arclock.rs`'s
   `alloc_proxy_disjoint_keys_concurrent`) is often a cleaner signal than wall
   clock — it isn't sensitive to machine load or scheduler mood.
3. E2E:    re-run the stress line above ≥2× and compare throughput; correctness
   must stay `RESULT: PASS`.
