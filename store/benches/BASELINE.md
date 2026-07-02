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

## How to compare after a change

1. Micro:  `cargo bench -p store --bench page_store` → criterion prints
   `change: [-x% .. +y%]` vs the stored baseline for each case.
2. E2E:    re-run the stress line above ≥2× and compare throughput; correctness
   must stay `RESULT: PASS`.
