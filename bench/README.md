# bench

squeal-db against other embedded databases: speed and memory, under each
engine's default memory settings and with large ones.

| binary | engines | workload |
|---|---|---|
| `sql` | squeal-db, SQLite 3.53 (bundled, via `rusqlite`), Turso 0.8.2 | customers and orders: bulk load, indexes, lookups, scans, joins, sorts, subqueries, durable writes, concurrent reads |
| `kv` | squeal-db's store, SQLite as a table of `(key, blob)`, redb 4.3 | keyed 100-byte values: bulk insert, point lookups, scans, prefix and range scans, durable writes, deletes, concurrent reads |

```bash
cargo run --release -p bench --bin sql -- --size 200000 --runs 3
cargo run --release -p bench --bin kv  -- --size 200000 --runs 3
# --engines squeal,sqlite --profiles large --threads 8 narrow it
```

## How it measures

- **One process per engine.** The binary runs itself once per engine,
  profile and repeat, so each engine's memory is its own. Times and
  memory are medians over the repeats.
- **Memory** is the process's physical footprint (`ri_phys_footprint` on
  macOS; the resident set on Linux). That is heap and other dirty memory,
  as Activity Monitor shows it, not clean file pages the OS can drop. It
  is read after each group of phases, plus the peak over the whole run.
- **Same answers.** Every read phase's results are reduced to a row count
  and a sum of their values. They must agree across engines, and a
  disagreement is reported.
- **Equal durability.** Every commit waits for a full flush to stable
  storage (`F_FULLFSYNC` on macOS). squeal-db and redb get it from
  Rust's `sync_data`. SQLite and Turso run in WAL mode with
  `synchronous=FULL` and `fullfsync` on, and the report shows the
  settings each read back.
- **Prepared statements for SQLite and Turso**, their usual mode of use.
  squeal-db is sent SQL text; its parse cache shares parses across
  statements that differ only in literals. The store and redb are called
  directly.
- **Same allocator.** Turso's default features install mimalloc as the
  global allocator; they are turned off, so every engine uses the system
  allocator.

## Memory profiles

| profile | squeal-db / store | SQLite | Turso | redb |
|---|---|---|---|---|
| `default` | 128 MiB page cache, 64 MiB per query, 64 MiB scratch | `cache_size` -2000 (~2 MiB) | 2,000 pages | 1 GiB cache |
| `large` | 1 GiB page cache, 512 MiB per query, 512 MiB scratch | 1 GiB, temp tables in memory | 1 GiB, temp tables in memory | 1 GiB cache |

The caches are limits, not reservations: an engine only uses what the
data needs. So at 200k rows, which fit every engine's default cache except
SQLite's and Turso's, `large` mostly changes the two SQLite engines. The
larger runs (1M orders, 2M key-value rows) are where the caches differ.

**Bulk loads are 100,000 rows a transaction for every engine.** squeal-db
keeps a version record for each row a transaction writes until it commits,
and aborts a transaction holding more than a million of them
(`max_version_records`, a guard against long transactions pinning
history). So one transaction can't load a million rows; see "Memory" below.

## Results

Measured 2026-10-10 on an Apple M4 Max (48 GB, macOS 27), release build.
The full reports, every phase and every memory reading, are in
[`results/`](results/):

| report | size | runs |
|---|---|---|
| [`sql_200k.md`](results/sql_200k.md) | 20k customers, 200k orders | median of 3 |
| [`sql_1m.md`](results/sql_1m.md) | 100k customers, 1M orders | 1 |
| [`kv_200k.md`](results/kv_200k.md) | 200k rows a table | median of 3 |
| [`kv_2m.md`](results/kv_2m.md) | 2M rows a table | 1 |

Every engine returned the same answers in every read phase of every run.

### SQL: squeal-db, SQLite, Turso (200k orders)

Time per operation, `default` / `large` profile:

| phase | squeal-db | SQLite | Turso |
|---|---:|---:|---:|
| Bulk load, per row | 4.96 µs / 4.97 µs | 569 ns / 566 ns | 1.07 µs / 1.06 µs |
| Create an index | 518 ms / 511 ms | 34 ms / 36 ms | 82 ms / 81 ms |
| Point lookup by primary key | 7.71 µs / 7.75 µs | 1.30 µs / 1.02 µs | 1.69 µs / 1.43 µs |
| Secondary-index lookup + aggregate | 20.7 µs / 19.9 µs | 9.5 µs / 6.0 µs | 7.9 µs / 4.4 µs |
| Index range (1 week) + aggregate | 3.7 ms / 3.7 ms | 1.8 ms / 0.80 ms | 1.9 ms / 0.93 ms |
| Full scan with `GROUP BY` | 66 ms / 66 ms | 46 ms / 44 ms | 40 ms / 31 ms |
| Join + `GROUP BY` | **117 ms** / 116 ms | 135 ms / **80 ms** | 160 ms / 87 ms |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.3 ms / 1.3 ms | 0.42 ms / 0.22 ms | 0.45 ms / 0.30 ms |
| Sort the whole table | 94 ms / 93 ms | 64 ms / 60 ms | 66 ms / 56 ms |
| `IN (subquery)` | 32 ms / 32 ms | 21 ms / 9.8 ms | 25 ms / 13 ms |
| Correlated `EXISTS` | 28 ms / 29 ms | 26 ms / 14 ms | 24 ms / 18 ms |
| Correlated `NOT EXISTS` | **15 ms / 15 ms** | 66 ms / 32 ms | 42 ms / 37 ms |
| `UPDATE` one row, autocommit | 4.6 ms / 4.7 ms | 4.0 ms / 4.0 ms | 4.0 ms / 3.2 ms |
| `INSERT` one row, autocommit | 4.2 ms / 4.2 ms | 4.0 ms / 4.1 ms | 4.1 ms / 3.4 ms |
| `UPDATE`, 100 rows a transaction, per row | 90 µs / 85 µs | 79 µs / 73 µs | 77 µs / 64 µs |
| Point lookups on 4 threads | 3.67 µs / 3.76 µs | 1.41 µs / 1.39 µs | **812 ns / 821 ns** |

Memory (physical footprint), `default` / `large`:

| | squeal-db | SQLite | Turso |
|---|---:|---:|---:|
| after the load | 316 / 329 MiB | 35 / 22 MiB | 42 / 45 MiB |
| at the end | 322 / 323 MiB | 46 / 61 MiB | 78 / 73 MiB |
| peak | 426 / 420 MiB | 46 / 61 MiB | 78 / 73 MiB |

At 1M orders ([`sql_1m.md`](results/sql_1m.md)) the picture holds:
- **squeal-db** stays ahead on correlated `NOT EXISTS` (95 ms against
  457 and 526 ms) and correlated `EXISTS`.
- **SQLite and Turso** gain the most from the large cache, which the data
  now outgrows at their defaults.
- **squeal-db's memory** peaks at 1.4 GB on the default profile and 2.0 GB
  on the large one, against SQLite's 140 / 223 MiB and Turso's
  206 / 392 MiB.

### Key-value: the store, SQLite, redb (200k rows a table)

Time per operation, `default` / `large`:

| phase | store | SQLite | redb |
|---|---:|---:|---:|
| Insert, per row | 2.37 µs / 2.33 µs | 716 ns / 598 ns | 894 ns / 872 ns |
| Point lookup, integer key | 538 ns / 513 ns | 753 ns / 400 ns | **202 ns / 201 ns** |
| Point lookup, a transaction each | 2.90 µs / 2.84 µs | 1.38 µs / 994 ns | 469 ns / 464 ns |
| Point lookup, composite key | 1.11 µs / 1.09 µs | 954 ns / 497 ns | 274 ns / 275 ns |
| Full scan, per row | **19 ns / 19 ns** | 42 ns / 29 ns | 22 ns / 22 ns |
| Prefix scan of 100 rows | 10.1 µs / 9.9 µs | 6.7 µs / 4.9 µs | 3.5 µs / 3.5 µs |
| Range scan, per row | 86 ns / 86 ns | 50 ns / 36 ns | 24 ns / 25 ns |
| Update one row and commit | 4.2 ms / 3.4 ms | 4.0 ms / 3.1 ms | 4.6 ms / 3.7 ms |
| Update, 100 rows a transaction, per row | 64 µs / 55 µs | 64 µs / 50 µs | 78 µs / 63 µs |
| Delete, per row | 6.09 µs / 6.00 µs | 816 ns / 647 ns | 712 ns / 632 ns |
| Point lookups on 4 threads | 388 ns / 403 ns | 1.24 µs / 1.50 µs | **68 ns / 62 ns** |

Memory, `default` / `large`: the store peaks at 216 / 218 MiB, SQLite at
21 / 177 MiB, redb at 72 / 72 MiB.

At 2M rows a table ([`kv_2m.md`](results/kv_2m.md)) the data no longer fits
the store's default 128 MiB cache:
- **Its lookups slow down 4-8x** (4.56 µs a point lookup), and recover
  with the 1 GiB cache (1.14 µs).
- **Its memory:** the store reaches 1.68 GB with the 1 GiB cache, against
  SQLite's 620 MiB and redb's 612 MiB.

### What it says about squeal-db

- **Level** on durable commits, which are bound by the disk flush.
- **Ahead** on correlated `NOT EXISTS`, which becomes a hash anti-join,
  and on the store's full scans.
- **redb is the fastest key-value store here**, by 2-3x on lookups and
  more on concurrent reads.
- **Turso is close to SQLite throughout**, ahead on concurrent reads.
- **Memory is squeal-db's weakest point.** It uses several times what the
  others do, and goes over its own page-cache limit. See below.

### Memory: where squeal-db's goes

Measured on the store alone, loading 200k rows (27 MB of pages) in one
transaction (with the store's `alloc-tracking` feature):

| | footprint | live heap | version records |
|---|---:|---:|---:|
| before the commit | 196 MiB | 119 MiB | 200,000 |
| just after the commit | 238 MiB | 119 MiB | 200,000 |
| after cleanup | 118 MiB | 94 MiB | 0 |

Three things over the page cache:
1. **Version records.** One per row a transaction writes, about 450 bytes
   each, kept until it commits and no reader needs them. This is also
   what caps a transaction at a million writes.
2. **The version store's hash maps keep their peak capacity** once
   emptied: about 67 MB here, which is most of the live heap after
   cleanup.
3. **The allocator keeps freed memory**: the gap between footprint and
   live heap.

On top of those, a checkpoint copies every dirty page before writing it
(see the store README), so its peak is briefly twice the dirty pages.

These are in [TODO.md](../TODO.md).
