# store

The engine under squeal-db: an embedded, transactional, indexable blob store.
Tables are B+trees of `Tuple`s, each a typed key plus an opaque byte
payload. The store gives them:

- MVCC snapshot isolation
- a write-ahead log, with fuzzy checkpoints and crash recovery
- range and prefix scans
- id sequences
- scratch space for query execution

It knows nothing about SQL or JSON. `squeal-sql` and `sq-json` are built on
it (see the [workspace README](../README.md)).

```rust
use store::{db::Db, memfile::MemFile, tuple::{DBIdType, Tuple}};

let db = Db::<std::fs::File>::create("orders.db")?;   // or Db::<MemFile>::...
let orders = db.create_table("orders".into())?;

let txn = db.begin()?;
db.insert(orders, Tuple::new(42, b"payload"), &txn)?;
db.commit(txn)?;                       // returns once the commit is fsynced

let txn = db.begin()?;
let row = db.find(orders, DBIdType::Int(42), &txn)?;  // Some(tuple), as of txn's snapshot
let mut scan = db.table_scan(orders)?;                // or range_scan / prefix_scan
```

## Configuration

What a database is created and opened with is two structs
(`store::config`), split by when a setting can be chosen:

- **`CreateConfig`** holds what is fixed in the file when the database is
  created, plus an `OpenConfig` for running it. Every later open reads the
  fixed part from the file.
- **`OpenConfig`** holds how this process runs the database. It is chosen
  on every open, written nowhere, and changes nothing in the file. The
  same database can be opened with a small cache on one machine and a
  large one on another.

```rust
use store::config::{CreateConfig, OpenConfig};

let run = OpenConfig::default()
    .page_cache_bytes(256 << 20)
    .query_memory_bytes(32 << 20);
let db = Db::<std::fs::File>::create_with(
    "orders.db",
    &CreateConfig::default().page_size(8 * 1024).open(run.clone()),
)?;
// ... later, in any process:
let db = Db::<std::fs::File>::open_with("orders.db", &run)?;
```

`Db::create`, `open` and `open_using` use the defaults. `Db::config()`
returns what a database is running with.

| `CreateConfig` | default | |
|---|---|---|
| `page_size` | 16 KiB | bytes a page: a power of two, 4 KiB to 1 MiB |
| `max_index_key_size` | 512 | largest index key: a power of two, 64 to 8 KiB, small enough for the page size (below) |
| `open` | | an `OpenConfig`, below |

| `OpenConfig` | default | |
|---|---|---|
| `page_cache_bytes` | 128 MiB | how much of the database stays in memory |
| `temp_cache_bytes` | 64 MiB | memory for query scratch pages before they spill to `<db>.tmp` |
| `query_memory_bytes` | 64 MiB | what one query may hold before its operators spill; the store keeps it, the layer running queries enforces it |
| `lock_timeout` | 1 s | how long a page lock is waited for |
| `checkpoint_log_bytes` | 16 MiB | WAL growth that triggers a checkpoint |
| `checkpoint_dirty_pages` | half the cache | dirty pages that trigger a checkpoint |
| `snapshot_limits` | 256 MiB, 1M versions | what a long transaction may pin before it is aborted |
| `maintenance_interval` | 10 ms | how often background maintenance runs when idle |

**A page must hold four index entries.** A full tree page splits in two,
and each half must still take another entry. With fewer than four entries
to a page the tree fails an insert after two or three rows. So:
- **A database is refused** when its pages can't hold four entries of its
  own `max_index_key_size`: 4 KiB pages take keys up to 512 bytes, 8 KiB
  up to 1 KiB, 16 KiB up to 2 KiB.
- **A table is refused** when it asks for index entries bigger than
  `Db::max_index_entry_size()`, with `IndexEntryTooLarge` naming the
  largest that fits.

Row payloads are not limited this way: a row bigger than a page gets a
page of its own with an overflow chain.

A config no database could run with is refused with
`StoreError::InvalidConfig`: a zero size, or a page size that isn't a
power of two in range.

**Settings by name.** Every setting also has a text form, for command
lines and JSON options: `CreateConfig::set("page_size", "8k")`, or
`OpenConfig::from_settings([("pageCacheBytes", "256m")])`.
- **Names** are accepted in snake_case, kebab-case or camelCase.
- **Sizes** take `k`, `m` or `g`.
- **Durations** take `us`, `ms`, `s` or `m`.
- **Fixed settings:** one that is fixed at creation is refused on open,
  with a message saying so.
- **The list:** `OPEN_SETTINGS` and `CREATE_SETTINGS`, with descriptions.

## Keys and values

A key is a `DBIdType`. It is either `Int(u64)` (a generated row id) or
`Rec(IndexKey)`: an ordered list of `ValueItem`s, one per key column. A
`ValueItem` is one of:
- `Null`
- `Integer(i64)`
- `Double(f64)`
- `Datetime`
- `Str` and `Blob`, each with a declared capacity
- `Boolean`

`Rec` keys compare field by field. That is what makes composite keys,
prefix scans and range seeks work. The layers above encode their keys into
this:
- `squeal-sql`: primary keys, and index keys with the row's key appended
- `sq-json`: BSON-ordered values behind a type bracket

The payload is bytes: the store never looks inside it.

`ValueRef` is the borrowed twin of `ValueItem`. Its strings and blobs point
into a page's bytes, so a reader can examine every field of a row and copy
out only what it keeps.

## Pages

Every page has the same format (`pages/slotted.rs`):

```
[ header: next_page, sizes, lsn, flags, high_key, checksum ]
[ slot_count | capacity | slot 0 | slot 1 | ... → free ← tuple bytes packed from the end ]
```

- **The bytes on disk are the bytes in memory.** A page is one buffer.
  - Loading it parses only the slot directory.
  - Flushing it copies the buffer.
  - Insert, replace and remove edit it in place: a memmove of the slot
    directory, plus a write into the heap.
  - Dead heap bytes are compacted only when an insert would not otherwise
    fit.
- **Nothing is decoded to be read.** Slots are kept in key order. A binary
  search compares an 8-byte order prefix held per slot, and reads a key from
  its bytes only on a tie (`wire.rs`, a zero-allocation reader of the
  postcard encoding). A tuple is *lent* to the caller as a `TupleRef`
  borrowing the page (a few nanoseconds), rather than built as an owned
  copy.
- **Checksummed.** The header carries a CRC-32 of the data, checked on
  every read from disk. Pages written by older versions used FNV-1a and
  still verify. A torn or corrupted page is an error, never silently wrong
  data.
- **Versioned formats.** Every persisted structure carries a version tag,
  and every release can open files written by earlier ones.
- **Oversized tuples.** A tuple too big for a page goes on an empty page of
  its own, with an overflow chain.
- **Fixed page numbers.** Pages 0–2 are the system pages: the table
  catalog, sequences, and the free-page list. Pages are 16 KiB by default,
  configurable per database from 4 KiB to 1 MiB.

## B-link trees

Each table has two parts:

- **An index tree**, keyed by the tuple's key.
- **A chain of data pages** holding the tuples. A leaf entry maps a key to
  the data page holding its row.

The tree is a **B-link tree** (Lehman–Yao). Every page carries a `high_key`
(the upper bound of the keys it covers) and a link to its right sibling.
When a split moves the upper half of a page to a new sibling, a reader who
arrives with an out-of-date route finds its key at or above the
`high_key`, and moves right. So:

- **Readers descend without locks.** They never wait on a split in
  progress.
- **Writers descend optimistically.** Route with unlocked reads, lock only
  the leaf, then recheck under the lock that the page is still a leaf and
  still covers the key (following right-links if not). If it has room, no
  ancestor was ever locked. Locking the root on every write cost about 20%
  of throughput under 16 threads.
- **Splits are by bytes, not entry count.** Short keys pack a page full,
  whatever the table's largest key might be. The split separator becomes
  the left page's new `high_key`.
- **A data page's free space is found by a cached tail hint.** Sequential
  inserts are O(1) amortized rather than O(table size).

**Page lock order** is checked at every acquisition, before waiting:
- Index pages (top-down) come before data pages.
- At most one data page is held at a time.

A violation is an immediate `LockOrderViolation`, never a deadlock. A lock
wait gives up after one second (`LockTimeout`). A legitimate hold lasts
microseconds, so a timeout is a bug detector, not a contention knob.

## The page cache

`PageBuffer` caches pages in memory, sharded by page id (16 shards, each its
own lock).

- **Eviction is CLOCK (second chance).**
  - A FIFO ring holds the eviction candidates.
  - Each cache hit sets the page's `referenced` bit, which is one atomic
    store.
  - A sweep clears the bit and gives the page another lap, or evicts it if
    the bit was already clear.
  - Every operation is O(1) amortized.
- **Evicted but still held.** An evicted page someone still holds stays
  reachable through a `Weak` entry until they let go.
- **Dirty pages are not evicted.** They are parked until a checkpoint writes
  them. Data reaches disk only through checkpoints, which is what makes the
  WAL rule simple to uphold (see below). The cache also checkpoints when too
  many dirty pages are waiting, which bounds memory.
- **Copy-on-write pages.** A page's content is an `Arc<dyn PageTuple>`.
  - A reader clones the `Arc` and walks a stable snapshot, holding no lock.
  - A writer copies the content only if a reader still holds the old
    snapshot (`Arc::get_mut`, otherwise a deep clone).
  - Readers never block writers, and never see a half-written page.

**Scratch space** for queries (sort runs, hash-join tables, temp tables)
lives in a separate pool (`TempPool`). It has CLOCK eviction, and spills to
a private `<db>.tmp` file that is created on first need and removed when
empty. Scratch pages bypass the WAL, checkpoints and the free list
entirely, and are wiped at open, so a crash leaks nothing.

## Transactions

The short version is in [TXN_MODEL.md](../TXN_MODEL.md).

- **One clock.** A single counter (`LsnClock`) mints transaction ids, log
  sequence numbers and commit timestamps. A transaction's id is its start
  time. Every record it writes has a larger LSN, and its commit timestamp
  is larger still. There is no wall clock and no second sequence to
  reconcile.
- **One table of states.** `TransactionManager` maps ids to states:
  `Active`, `Committing`, `Committed{ts}`, `Aborting`, `Aborted`. An id
  that is absent means "committed before every active reader began": it is
  visible to everyone, and there is nothing to remember about it.
- **Snapshots.** At `begin`, a transaction records the ids that had not yet
  committed, as a sorted array in an `Arc<Snapshot>`. A version is visible
  if:
  - its writer is the reader itself, or
  - its writer has a smaller id and is not in the array.

  This check is a comparison and a binary search, with no lock. A writer
  that was mid-commit when the reader began is the only case that asks the
  shared table, and that case waits for the commit's outcome.
- **Versions.** A row on its page is the newest version. Its `pre_lsn`
  points at the previous version, held in an in-memory `VersionStore`. A
  reader that can't see the newest version walks back to one it can.
  Deletes are tombstone versions, purged once no reader can see the row.
- **Write conflicts: first committer wins.** Overwriting a row whose writer
  hadn't committed before you began is a `WriteConflict`. Per transaction,
  you choose (`ConflictPolicy`) whether that fails just the operation or
  aborts the transaction.
- **Rollback** reverts the transaction's writes from its version records.
  Dropping a `Transaction` guard without committing rolls it back.
- **Cleanup has one rule: the horizon.** A committed transaction's records
  and tombstones are reclaimed once its commit timestamp is below the oldest
  active transaction's id. An aborted one's go once it is reverted.
  - **Who does it:** the work runs on the one maintenance thread per
    database, as do abort retries and checkpoints triggered by log growth.
    Foreground threads never do maintenance.
  - **Long-lived transactions:** these would pin history, so they are capped
    (`SnapshotLimits`). Past the cap, the oldest is aborted with
    `SnapshotTooOld` rather than letting memory or log grow without bound.
- **Everything is observable.** `Db::stats()` reports:
  - active, retained and aborting transactions
  - version records
  - retained WAL bytes
  - maintenance counters, and the last error it hit

## The write-ahead log

Every insert, update and remove appends a record (`Add`, `Mod` with pre-
and post-image, or `Del`) before its page is changed. Commits and rollbacks
append a `Commit` or `Rollback` record.

- **Segments.** The log is a series of files, `<name>.wal.1`,
  `<name>.wal.2`, …, each with a validated header (database, version, page
  size).
- **Group commit.**
  - One log-writer thread drains a bounded channel in batches of up to 256
    records, lingering 200 µs for stragglers.
  - Each batch is one write and one fsync.
  - `Db::commit` waits for its own record's batch to be durable. A single
    fsync wakes every committer in the batch.
  - `commit_with(Durability::Async)` returns once queued, for bulk loads
    that can be re-run.
- **Fuzzy checkpoints never wait for a transaction.**
  1. **Capture.** Briefly excluding tree writers (microseconds), read the
     *floor* (the oldest unfinished transaction's id) and copy every dirty
     page: one consistent instant of the tree.
  2. **Sync the log, then the data.** Fsync the log; every captured
     change's record is now durable, which is the WAL rule. Then write the
     pages and fsync the data file.
  3. **Header.** Write and fsync the header, carrying the counter and the
     floor.
  4. **Roll the log.** Start a new segment, and delete the old segments
     whose records are all below the floor.

  A checkpoint runs every 16 MiB of log growth, or when too many dirty
  pages are waiting. A long transaction costs retained segments, never a
  stalled checkpoint.
- **Recovery** reads the header, then scans the segments from the floor in
  three passes:
  1. Analysis: which transactions committed.
  2. Redo: committed transactions' records.
  3. Undo: everything else.

  Replay is idempotent: a record already reflected on its page is skipped.
  The clock is reseeded past the highest LSN found, so no id is ever
  reissued.

## Memory and disk files

`Db<F: DBFile>` is generic over its file. `DBFile` is
`Read + Write + Seek + Send + Sync + Opener`, where `Opener` adds:
- positioned I/O (`pread`/`pwrite`)
- `sync`, `truncate` and `lock`
- sibling files: open, list and remove, for the WAL segments

There are four implementations:

| type | used by |
|---|---|
| `std::fs::File` | the default on native targets |
| `MemFile` | tests, the browser, "memory" mode in the CLI |
| `NamedMemFile` | tests that close and reopen by name without touching disk |
| `WasiFile` | Node.js WASI builds (`ws-napi`), where `try_clone`/`try_lock` don't work |

The engine has one code path for all of them. Two details matter:

- **Positioned I/O everywhere.** Cloned file handles share an OS seek
  cursor, and two threads seeking the same descriptor once corrupted page
  writes. `pread`/`pwrite` have no cursor to share.
- **`MemFile` models a disk.** It keeps the bytes as written and, separately,
  the bytes as of the last `sync`. `Db::<MemFile>::synced_snapshot()`
  returns only what was synced: exactly what survives a power cut. The
  crash harness is built on that.

On `wasm32-unknown-unknown` there are no threads. The log writer and the
maintenance work run inline, and time comes from `store::clock`
(`web-time`), because `std::time`'s `now()` panics there.

## Unsafe

The crate is safe Rust apart from four small, commented uses:

- **`cursor.rs`.** Three reborrows through a raw pointer so a cursor can
  lend a tuple from its current page snapshot. This is NLL's "problem case
  #3": a borrow returned from one loop iteration needn't block the next,
  but the borrow checker can't see that yet.
- **`wire.rs` / `pages/slotted.rs`.** `from_utf8_unchecked` on string bytes
  that were already validated when the page was loaded (`assume_checked`).
- **`valueitem.rs`.** Reading a `#[repr(u8)]` enum's discriminant.
- **`alloc.rs`.** The `GlobalAlloc` impl of the optional allocation-counting
  allocator (the `alloc-tracking` feature, used only by `squeal-cli`).

The other `unsafe` blocks in the crate are inside tests.

## Testing

- **Unit tests.** About 640, inline in each module: `cargo test -p store
  --release`.
- **The crash harness** (`crash_harness.rs`, and the `crash` example for
  long soaks).
  - **The run:** threads commit, roll back and checkpoint at random. The
    harness "cuts the power" at a random instant, reopens from the synced
    bytes, and repeats on the recovered database.
  - **The checks:**
    - every committed transaction is present
    - every uncommitted one is absent
    - an in-flight one is all-or-nothing
    - scans see each row exactly once
- **`examples/stress`.** A mixed concurrent workload with randomized
  commit/rollback and latency histograms, on both backends.
- **`examples/perf`**, **`examples/bulk_load`.** Throughput reports and
  multi-million-row loads.
- **`examples/wal_dump`.** Prints a WAL's records.
- **`benches/`.** Criterion micro-benchmarks for pages and page locks.

## Comparing with SQLite

The store alone against SQLite 3.53 used as a key-value store: the same
keyed 100-byte values and the same operations, with each phase's results
compared (they agree in all of them). This measures the engine without a
SQL layer on top; squeal-sql's README has the comparison with one.

Two tables of 200,000 rows:
- **`kv`:** an integer key. In SQLite, `INTEGER PRIMARY KEY`.
- **`ordered`:** a two-integer key `(grp, seq)` with 100 rows a group,
  the shape an index has. In SQLite, `PRIMARY KEY (grp, seq) WITHOUT
  ROWID`.

Both are on disk and equally durable: every commit waits for a full
flush (`F_FULLFSYNC` on macOS; SQLite in WAL mode with
`synchronous=FULL` and `fullfsync` on). SQLite runs a prepared statement
per operation, where the store is called directly. Reads run inside one
read transaction on both, except where noted.

Measured 2026-10-10 on an Apple M4 Max, release build, median of three
runs. Times are per operation. The last column is SQLite's time divided
by the store's, so above 1 means the store is faster.

| phase | store | SQLite | store vs SQLite |
|---|---:|---:|---:|
| Insert, integer keys, one transaction | 1.73 µs | 943 ns | 0.55x |
| Insert, composite keys in order, one transaction | 3.66 µs | 971 ns | 0.27x |
| Checkpoint after the load | 87.13 ms | 15.99 ms | 0.18x |
| Point lookup, integer key | 589 ns | 703 ns | **1.19x** |
| Point lookup, a transaction each | 2.96 µs | 1.39 µs | 0.47x |
| Point lookup, composite key | 1.37 µs | 941 ns | 0.69x |
| Full scan, per row | 19 ns | 46 ns | **2.42x** |
| Prefix scan of 100 rows, per scan | 10.56 µs | 6.89 µs | 0.65x |
| Range scan of 1% of the table, per row | 90 ns | 53 ns | 0.59x |
| Update one row and commit | 4.45 ms | 4.30 ms | **0.97x** |
| Insert one row and commit | 4.26 ms | 4.10 ms | **0.96x** |
| Update, 100 rows a transaction, per row | 61.54 µs | 74.25 µs | **1.21x** |
| Delete, one transaction, per row | 5.58 µs | 860 ns | 0.15x |
| Point lookups on 4 threads, per lookup | 378 ns | 855 ns | **2.26x** |

What the numbers say:

- **Ahead on a full scan, and on concurrent reads.** A table scan lends
  each row from its page without decoding it. Readers take no locks, so
  four threads each read at the single-thread cost, where SQLite's slow
  down.
- **Slightly ahead on a point lookup by integer key; about 30% behind
  on a composite key**, whose values are compared field by field.
- **Behind on range and prefix scans**, by about 40%. These walk the
  index and fetch each row from its data page, where a table scan reads
  data pages straight through.
- **Level on durable commits.** Both are bound by the disk flush. With
  100 updates a transaction the flush still dominates.
- **Behind when every lookup is its own transaction.** Beginning and
  committing a read transaction costs about 2.4 µs, against a 0.6 µs
  lookup.
- **Behind on inserts in bulk, by 2-4x.** Each insert writes a WAL
  record (through a channel to the log thread), a version record, a data
  page and an index entry.
- **Well behind on deletes**, at 5.6 µs a row. Not yet profiled.
- **Checkpoints are slower.** Every dirty page is copied, then written
  with its checksum.
- **On disk:** 59 MB against SQLite's 48 MB after a checkpoint.

To run it:

```bash
cargo run --release -p store --example kv_vs_sqlite -- 200000 4
```

The arguments are the rows per table and the number of reader threads.

## History

The root design notes record how this got here, and some describe designs
since replaced:
- `ARCHITECTURE.md` (an earlier snapshot, before slotted pages and the
  current transaction model)
- `STORE_AUDIT.md`
- `TXN_SIMPLIFICATION_*.md`
- `P6_SLOTTED_PAGE_DESIGN.md`

This README describes the code as it is.
