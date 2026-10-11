# Memory issues

These come from an audit made on 2026-10-11, after the first round of memory
fixes (see bench/README.md, "Memory: what was fixed"). At the default
settings, squeal-db's footprint is still about 4x Turso's at 1M orders:
412–425 MiB against 230 MiB.

How each finding was established:
- **measured**: the item is backed by numbers from one of these:
  - `-Zprint-type-sizes` (nightly)
  - macOS `heap` / `vmmap` snapshots of the 1M-order bench, run with
    `MallocStackLogging`
  - small probes outside the repo that load data and measure file sizes,
    or count live bytes with a counting allocator
- **code**: found by reading the code, not measured.

Fixed since the audit:
- [x] **The SQL parser leaked its grammar on every parse**: 13.7 KB a
  parse, about 40 MB in the bench, and without bound in a long-running
  process. Fixed in `552168c`.

## 1. The data is 5.8x larger than SQLite's (measured)

The bench database (1M orders, 100k customers, 3 indexes) takes 332 MB on
disk, against SQLite's 57 MB. The page cache is bounded by bytes, so each
cached megabyte holds about a sixth of the rows SQLite's does. The cost
multiplies through the cache, the WAL and I/O.

The orders table, loaded step by step:

| | size | per row |
|---|---:|---:|
| table with no primary key | 100 MB | 100 B |
| adding `primary key(id)` | +54 MB | +54 B |
| each single-integer secondary index | +73 MB | +73 B |

SQLite needs roughly 12–15 B for an index entry.

- [ ] **The primary key gets its own index tree.** The row tree is already
  keyed by the primary key, so this duplicates it: 54 B a row. Where:
  `squeal-sql/src/table.rs` (`inline_indices` and the table-level
  constraint, `is_primary: true`), and `insert_one_row_in_txn` in
  `squeal-sql/src/schema_ops/schema.rs`.
- [ ] **An index entry stores the row's primary key twice**: appended to
  the key (`values.extend_from_slice(identity.values())`) and again as
  the payload (`to_allocvec(&identity)`). Where:
  `insert_one_row_in_txn`.
- [ ] **Rows are fixed-width.** Where: `RowLayout` / `column_width` in
  `squeal-sql/src/table.rs`, and `write_to` in `store/src/valueitem.rs`.
  - Every integer and double takes 9 bytes; SQLite's varints take 1–9.
  - A string takes 9 bytes plus its declared width: `varchar(12)` holding
    "pen" takes 21 bytes. `TEXT` defaults to a width of 32.
  - A NULL takes its column's full width.
  - Every row starts with an 8-byte value count.
- [ ] **Every stored tuple carries MVCC fields**: a transaction id, a
  previous-version LSN and flags, plus an 8-byte slot entry. That is
  about 20 B a row and a further 20 B an index entry. Some could be
  dropped once a row is older than every snapshot.

The suggested direction:
1. Drop the primary-key tree.
2. Drop the duplicate primary key from index entries.
3. Use variable-width rows with varint integers and unpadded strings.

Together these should bring the data within about 2x of SQLite's, and let
the same cache hold proportionally more rows.

## 2. Allocator fragmentation: about half the footprint at the peaks (measured)

`vmmap` shows the malloc zone 46–49% fragmented at the peaks: 335 MB
dirty, but only 182 MB allocated. The macOS allocator keeps freed memory
between live allocations charged to the process.

The cause is churn of mixed-size allocations among long-lived 16 KiB
pages. Sections 4 and 6 list the main sources. Cutting the churn is the
fix. A different allocator is out (no mimalloc).

## 3. Page search arrays as large as the pages (measured)

- [ ] **Each decoded page keeps two search arrays**: a 16-byte `u128`
  prefix and a 4-byte `id_end` per slot, on top of the 8-byte slot entry
  in the buffer. Where: `store/src/pages/slotted.rs`.
  - Pages built by inserts grow the arrays with `Vec::insert`, which
    doubles capacity.
  - During the 1M-order load, that growth held about 45–90 MB, against
    about 80 MB of page buffers.
  - Pages decoded from disk get exact-size arrays: about 24 MB against
    103 MB of pages.
  - `clear()` keeps the old capacity.
- These arrays count against `page_cache_bytes`, so during writes up to
  half the cache holds arrays rather than data.
- Fix:
  - use an 8-byte prefix for integer keys;
  - shrink the arrays after a split or a bulk load;
  - release capacity in `clear()`.

## 4. Grows without bound (measured, and code)

- [ ] **The page-lock map is never pruned** (`store/src/arclock.rs`).
  - `cleanup()` has no production caller.
  - Every page ever locked for writing keeps:
    - a map entry;
    - an `Arc<ReentrantMutex>`;
    - an `Arc<Holder>` holding a `Mutex<Option<(String, Instant)>>`.
  - That is about 150–180 B per page, for the life of the `Db`.
  - Measured: 19k → 30k → 41k entries over the bench run, 3.5 MB and
    climbing.
- [ ] **The cache keeps `Weak` entries for evicted pages**
  (`store/src/buffer.rs`).
  - An entry goes only when that page is requested again, and it keeps
    the `Page` allocation (about 190 B) alive.
  - The shard maps are never shrunk.
  - A scan of a database larger than the cache leaves about 230 B per
    page evicted.
- [ ] **TempPool** (`store/src/temppool.rs`) is bounded by page count, not
  bytes. When its last page is freed it keeps the capacity of `slots`,
  `index` and `free_ids`.

## 5. Type sizes and padding (measured with `-Zprint-type-sizes`)

| type | size | what's wasted |
|---|---:|---|
| `ValueItem` | 40 B | `Str((String, u32))` carries the column's declared width in every value, which pushes the type from 32 to 40 B. Every variant has 7 bytes of padding. With `Box<str>` or `Arc<str>`, and the width read from the schema, it would be 24 B. A 6-column row costs 16 + 240 B plus one allocation per string. |
| `Tuple` | 80 B | `Option<TransactionId>` and `Option<LsnId>` take 16 B each, where a `NonZeroU64` niche would take 8. 7 bytes of end padding. 56 B is reachable. |
| `Page` | 160 B | two 16-byte `Option`s (`record_size`, `lsn`) and four `u64` counters |
| `LogMsg` / `Operation` | 224 / 216 B | `Mod` keeps two tuples inline, so every `Add` or `Commit` takes 224 B in the log channel. Boxing `Mod`'s payload would bring it to about 120. |
| `ProjectableField` / `EvalExpr` | 144 / 96 B | hash GROUP BY clones the whole SELECT list per group (below) |

## 6. Unnecessary allocations (code; churn measured in aggregate)

These matter mostly as churn, which feeds the fragmentation in section 2.

Per insert:
- [ ] **A `format!` String on every page-lock acquire**
  (`store/src/arclock.rs`, `Holder::acquired`). That is at least two per
  inserted row, used only to report a timeout. Store the `ThreadId` and
  format it on timeout instead.
- [ ] **The row bytes are built three times**: by `RowLayout::encode`,
  then `postcard::to_allocvec`, then `Arc::from` in `Tuple::new_with`.
  `SlottedPage::add` encodes each tuple again (`to_allocvec`), once for
  the data page and once for the leaf.
- [ ] **The B+tree insert builds a `probe` tuple and a leaf `entry` tuple**,
  each allocated (`store/src/tables/bplustree.rs`).
- [ ] **Each index entry costs** an `extract_field_values` Vec, an
  `IndexKey::new_from` (an Arc plus clones of its strings), and a
  `to_allocvec(&identity)`.
- [ ] **The log thread allocates per record**: `to_allocvec` plus
  `frame_record` make two Vecs per record, and each batch gets a new
  `batch` Vec that is never reused (`store/src/logger.rs`).
- [ ] **Bulk-insert batches** keep the statement text, the AST and all
  500 built rows alive at once. Each row also gets a
  `HashMap<&str, ValueItem>` (about 400 B) and a String clone per text
  column (`build_insert_rows` in `squeal-sql/src/table.rs`).

Per row read or produced:
- [ ] **`eval` always returns a clone** (`squeal-sql/src/plan/eval.rs`).
  `WHERE name = 'x'` allocates two Strings per row just to compare them.
  `MAX`/`MIN` clone the current best on every row.
- [ ] **Each operator builds a fresh row.** Projection, `UnionJoin`,
  `JoinMatcher` and nested-loop join each build a `Vec`, then
  `IndexKey::new_from_owned` copies it into an `Arc`: two allocations per
  row per operator, plus a String per text column. `WhereSource` does
  `vec![res]` per input row, rejected rows included.
- [ ] **Owned lookups and scan rows copy the tuple** (`Cursor::next`,
  `Db::find`). The lending paths (`next_ref`, `find_with`) don't.
- [ ] **Unread columns are not decoded but the row is still full width.**
  A 20-column row read for one column allocates 16 + 20 × 40 B.
- [ ] **`IndexKey::from_bytes` uses `unwrap_or(...)`**, so it builds a
  throwaway key on every call. Use `unwrap_or_else`
  (`store/src/valueitem.rs`).

Pages:
- [ ] **A checkpoint allocates about three 16 KiB buffers per dirty page**,
  outside the pool:
  - `to_bytes_snapshot` clones the buffer and zero-pads it;
  - `write_page` builds a header `Vec` and copies everything into it;
  - overflow chunks use fresh `vec!`s.

  Temp-pool reads use `vec![0; page_size]` rather than `bufpool`.
- [ ] **A page split decodes every tuple** into owned values and
  re-encodes them (`page.iter().collect::<Vec<_>>()` in
  `bplustree.rs`).
- [ ] **`SlottedPage::compact`** copies every live record into its own
  `Vec`. An in-place move would do.
- [ ] **`bufpool` is keyed by exact capacity.** Read buffers (16384 B) and
  content buffers (15776 B) land in separate classes, which halves its
  use.

## 7. Query memory the budget doesn't see (code)

- [ ] **Hash-join tables are under-charged by about 2.5x.** `MemTable`
  (`squeal-sql/src/source/hash.rs`) charges `size()` + 48 per row: about
  83 B for a 3-integer row, against about 210 really used. That 210 is
  the Arc'd values, the `HashValue`, a `MemReservation` per row, the
  hash bucket, and a `Vec<usize>` per distinct hash.
- [ ] **Merge join** (`sortjoin.rs`) charges the encoded size, and keeps
  one `MemReservation` per row.
- [ ] **Not tracked at all:**
  - subquery materialization (`plan/subquery.rs`);
  - `COUNT(DISTINCT)` sets (`plan/funcs.rs`);
  - the recursive CTE's `seen` set (`plan/logical.rs`), which also calls
    `to_bytes()` twice per row;
  - the matching rows UPDATE/DELETE buffer (`drain_with_ids`).
- [ ] **Hash GROUP BY** (`source/hashgroup.rs`) has three problems:
  - Each group deep-clones the SELECT list and HAVING, including each
    aggregate's name `String`. The expression trees could be shared,
    keeping only the accumulators per group.
  - The key is stored twice: once as the HashMap key and again as an
    `IndexKey`.
  - Every row allocates a key `Vec`, even when its group exists.
- [ ] **A sort's `row_memory`** (`source/sort.rs`) leaves out the
  16-byte slot and the up-to-2x growth slack of the current run.

## 8. Parse cache (measured, and code)

- [ ] **The cache is bounded by entry count, not bytes**
  (`sql-parser/src/cache.rs`): 512 entries a level, texts up to 8 KiB.
  - Each literal variant of a statement keeps a full AST copy: about
    3 KB each, measured.
  - Key texts are stored twice (`entries` and `by_use`).
  - A shape hit deep-clones the template AST and inserts the clone into
    the exact-text cache.
  - Sharing the template behind an `Arc` and bounding the cache by bytes
    would fix both.
- [ ] **No plan cache.** Every execution re-plans, cloning `TableStat`
  (its name, a HashMap and a Vec) per table per statement. This is
  churn, not footprint.
- [ ] **A cache-missing parse builds the grammar** (about 14 µs, 13.7 KB of
  allocations, now freed). Building it once per thread would remove
  both costs.

## 9. Defaults

- **The default page cache is 128 MiB.** SQLite's is about 2 MiB, and
  Turso's 2,000 pages, about 8 MiB.
- **Each query may take 64 MiB, and scratch is a further 64 MiB.**

Much of the gap at default settings is simply this. A smaller default
cache only makes sense once sections 1 and 3 make each cached megabyte
hold more rows.

## Suggested order

1. **Density** (section 1): the biggest lever, since it multiplies through
   the cache, the WAL and I/O.
2. **Search arrays** (3) and **the unbounded maps** (4).
3. **Per-insert and per-row churn** (6): this is what drives the
   fragmentation.
4. **`ValueItem` to 24 B and `Tuple` to 56 B** (5).
5. **Charge real heap sizes against the query budget** (7).
6. **The parse-cache byte bound** (8), then **the defaults** (9).
