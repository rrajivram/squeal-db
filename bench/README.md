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

**Bulk loads are one transaction for every engine**: all the customers
and orders in one, and each key-value table in one.

## Results

Measured 2026-10-10 on an Apple M4 Max (48 GB, macOS 27), release build,
after the memory fixes described below. The full reports, every phase and
every memory reading, are in [`results/`](results/):

| report | size | runs |
|---|---|---|
| [`sql_200k.md`](results/sql_200k.md) | 20k customers, 200k orders | median of 3 |
| [`sql_1m.md`](results/sql_1m.md) | 100k customers, 1M orders | 1 |
| [`kv_200k.md`](results/kv_200k.md) | 200k rows a table | median of 3 |
| [`kv_2m.md`](results/kv_2m.md) | 2M rows a table | 1 |

Every engine returned the same answers in every read phase of every run.
The single-run reports are noisier: a rerun of squeal-db's 1M `large`
point lookup gave 10.5 µs, not the report's 38.8.

### SQL: squeal-db, SQLite, Turso (200k orders)

Time per operation, `default` / `large` profile:

| phase | squeal-db | SQLite | Turso |
|---|---:|---:|---:|
| Bulk load, one transaction, per row | 8.38 µs / 5.05 µs | 554 ns / 563 ns | 1.11 µs / 1.18 µs |
| Create an index | 715 ms / 619 ms | 37 ms / 45 ms | 99 ms / 95 ms |
| Point lookup by primary key | 9.52 µs / 9.73 µs | 1.52 µs / 1.26 µs | 2.04 µs / 1.66 µs |
| Secondary-index lookup + aggregate | 29.9 µs / 27.9 µs | 11.2 µs / 7.2 µs | 9.9 µs / 5.8 µs |
| Index range (1 week) + aggregate | 5.2 ms / 5.2 ms | 2.2 ms / 1.0 ms | 2.4 ms / 1.2 ms |
| Full scan with `GROUP BY` | 46 ms / 47 ms | 50 ms / 51 ms | 46 ms / **39 ms** |
| Join + `GROUP BY` | **107 ms / 106 ms** | 157 ms / 99 ms | 194 ms / 119 ms |
| Join, filtered, `ORDER BY` + `LIMIT` | 1.8 ms / 1.7 ms | 0.48 ms / 0.26 ms | 0.54 ms / 0.37 ms |
| Sort the whole table | 114 ms / 111 ms | 72 ms / 71 ms | 70 ms / 67 ms |
| `IN (subquery)` | 35 ms / 35 ms | 24 ms / 12 ms | 32 ms / 16 ms |
| Correlated `EXISTS` | 34 ms / 34 ms | 30 ms / 17 ms | 30 ms / 22 ms |
| Correlated `NOT EXISTS` | **17 ms / 17 ms** | 80 ms / 39 ms | 58 ms / 48 ms |
| `UPDATE` one row, autocommit | 4.6 ms / 4.7 ms | 4.1 ms / 4.1 ms | 4.1 ms / 4.1 ms |
| `INSERT` one row, autocommit | 4.3 ms / 4.1 ms | 4.1 ms / 4.1 ms | 4.1 ms / 4.1 ms |
| `UPDATE`, 100 rows a transaction, per row | 78 µs / 80 µs | 72 µs / 71 µs | 82 µs / 69 µs |
| Point lookups on 4 threads | 5.75 µs / 5.77 µs | 1.80 µs / 1.75 µs | **1.09 µs / 992 ns** |

Memory (physical footprint), `default` / `large`:

| | squeal-db | SQLite | Turso |
|---|---:|---:|---:|
| after the load | 163 / 163 MiB | 35 / 22 MiB | 48 / 49 MiB |
| at the end | 161 / 161 MiB | 47 / 62 MiB | 88 / 88 MiB |
| peak | 223 / 224 MiB | 47 / 62 MiB | 88 / 88 MiB |

At 1M orders ([`sql_1m.md`](results/sql_1m.md)):
- **squeal-db leads on the blocking queries.** Correlated `NOT EXISTS`
  takes 172 ms against 599 and 723 ms. Join + `GROUP BY` takes 756 ms
  against 1.2 and 1.5 s on the default profile.
- **Its lookups need the large cache.** The default 128 MiB no longer
  holds the indexes, and a secondary-index lookup takes 167 µs (34 µs
  with 1 GiB).
- **Its memory peaks at 459 MiB on the default profile and 1.07 GB on
  the large one**, within its limits (a 1 GiB cache, plus 512 MiB a
  query). SQLite peaks at 140 / 224 MiB and Turso at 230 / 398 MiB.

### Key-value: the store, SQLite, redb (200k rows a table)

Time per operation, `default` / `large`:

| phase | store | SQLite | redb |
|---|---:|---:|---:|
| Insert, one transaction, per row | 3.31 µs / 2.69 µs | 679 ns / 654 ns | 756 ns / 693 ns |
| Point lookup, integer key | 734 ns / 745 ns | 920 ns / 518 ns | **262 ns / 271 ns** |
| Point lookup, a transaction each | 3.38 µs / 3.50 µs | 1.69 µs / 1.25 µs | 602 ns / 609 ns |
| Point lookup, composite key | 1.60 µs / 1.59 µs | 1.23 µs / 637 ns | 384 ns / 374 ns |
| Full scan, per row | **21 ns / 21 ns** | 134 ns / 33 ns | 26 ns / 27 ns |
| Prefix scan of 100 rows | 11.7 µs / 11.9 µs | 7.8 µs / 5.9 µs | 3.9 µs / 3.8 µs |
| Range scan, per row | 100 ns / 101 ns | 59 ns / 41 ns | 30 ns / 29 ns |
| Update one row and commit | 3.7 ms / 4.2 ms | 4.1 ms / 4.0 ms | 4.3 ms / 3.8 ms |
| Update, 100 rows a transaction, per row | 59 µs / 62 µs | 66 µs / 65 µs | 85 µs / 81 µs |
| Delete, per row | 6.54 µs / 6.54 µs | 750 ns / 595 ns | 663 ns / 651 ns |
| Point lookups on 4 threads | 348 ns / 326 ns | 1.57 µs / 1.72 µs | **102 ns / 101 ns** |

Memory, `default` / `large`: the store peaks at 101 / 100 MiB, SQLite at
21 / 177 MiB, redb at 72 / 72 MiB.

At 2M rows a table ([`kv_2m.md`](results/kv_2m.md)) the data no longer fits
the store's default 128 MiB cache:
- **Its lookups slow down about 10x** (7.7 µs a point lookup), and
  recover with the 1 GiB cache (1.4 µs).
- **Its memory:** the store peaks at 273 MiB on the default profile and
  849 MiB with the 1 GiB cache. SQLite peaks at 21 MiB and 1.5 GB, and
  redb at 626 MiB on both.

### What it says about squeal-db

- **Level** on durable commits, which are bound by the disk flush.
- **Ahead** on the blocking SQL queries: correlated `NOT EXISTS` (a hash
  anti-join) and join + `GROUP BY` (hash grouping). Also ahead on the
  store's full scans.
- **Behind** on lookups and bulk writes, by 2-6x, in both the SQL layer
  and the store.
- **redb is the fastest key-value store here**, by 2-3x on lookups and
  more on concurrent reads.
- **Turso is close to SQLite throughout**, ahead on concurrent reads.
- **Memory now stays within its limits**, but is still 2-4x SQLite's at
  the defaults.

### Memory: what was fixed

Before is the previous reports, whose loads used 100,000-row transactions
because one transaction could not write a million rows. After is this run:

| | before | after |
|---|---:|---:|
| SQL, 200k orders: after the load / peak | 316 / 426 MiB | 163 / 223 MiB |
| SQL, 1M orders, peak: `default` / `large` | 1.4 / 2.0 GB | 459 MiB / 1.07 GB |
| Store, 200k rows a table, peak | 216 MiB | 101 MiB |
| Store, 2M rows a table, 1 GiB cache, peak | 1.68 GB | 849 MiB |

A side-by-side run of both builds showed the reads unchanged or faster.
A full-scan `GROUP BY` went from 77 to 50 ms, and the SQL footprint after
the load from 309 to 161 MiB.

What changed (see the [store](../store/README.md) and
[squeal-sql](../squeal-sql/README.md) READMEs):

1. **No version record for an insert.**
   - Only updates and deletes keep a pre-image.
   - A transaction's writes are a compact log: a few bytes an insert,
     where each one used to keep about 450 bytes.
   - A committed transaction's inserts are forgotten at once.
   - The version maps shrink once emptied.
2. **One transaction may write any number of rows.** The snapshot caps
   count only what is pinned for others, so a large load is no longer
   aborted with `SnapshotTooOld`.
3. **The page cache is bounded by the bytes its pages hold**, not by a
   page count times the page size. A decoded page is larger than its
   on-disk form.
4. **Page buffers are pooled.** The macOS allocator keeps freed memory
   rather than returning it, so reusing buffers is what keeps eviction
   churn from growing the footprint.
5. **A checkpoint shares the pages it writes** rather than copying them.
6. **`GROUP BY` hashes** where it used to sort every input row. Its
   memory is proportional to the groups, and it falls back to sorting
   when they outgrow the query's budget.
7. **A sort charges a row's real in-memory size** against the query
   budget, so it spills before it overruns it.

The cost of honest accounting is that the default 128 MiB holds fewer
pages than before. Lookups on data larger than that cache, and
whole-table sorts, are slower than they were at 1M rows.

What's left is in [TODO.md](../TODO.md).
