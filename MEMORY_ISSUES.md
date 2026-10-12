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

## The larger problem: allocate less, move fewer bytes

Most of what follows is one habit seen from different sides: bytes are
turned into owned values at each step, and back. The structural fix is to
keep bytes as bytes (encode once, compare and sort in place, copy only
what must be kept, decode only where a value is needed). It starts with
keys: see [BYTE_KEYS_DESIGN.md](BYTE_KEYS_DESIGN.md), stage 1 of which is
done.

## 1. The data is 5.8x larger than SQLite's (measured)

The bench database (1M orders, 100k customers, 3 indexes) takes 332 MB on
disk, against SQLite's 57 MB. The page cache is bounded by bytes, so each
cached megabyte holds about a sixth of the rows SQLite's does. The cost
multiplies through the cache, the WAL and I/O.

Where an order row's ~300 B goes, measured by loading step by step, with
the per-field breakdown taken from the code:

| | per row | why |
|---|---:|---|
| row tree | ~100 B | the row body (~57–74 B fixed-width), MVCC header and slot ≈ 94 B on a data page, plus a ~21 B leaf entry pointing at it |
| primary-key tree | ~54 B | a second copy of what the row tree already is |
| each secondary index | ~73 B | key + payload on a data page, and the key again in a leaf; leaves ~69% full |

SQLite needs about 50 B for the same row and indexes.

The causes:
- **The primary key gets its own index tree.** The row tree is already
  keyed by the primary key, and already rejects a duplicate one. Only
  three things read the extra tree: foreign-key probes, covering scans
  of a primary-key range, and stats.
  Where: `squeal-sql/src/table.rs` (`is_primary: true`), and
  `insert_one_row_in_txn` / `delete_one_row_in_txn` in
  `squeal-sql/src/schema_ops/schema.rs`.
- **Every store tree keeps each entry twice**, index trees included: the
  key in the leaf, and the key plus payload on a separate data page
  (`write_version` / `write_data`, `store/src/tables/bplustree.rs`).
- **An index entry stores the row's primary key twice**: appended to the
  key, and again as the payload.
- **Rows are fixed-width** (`RowLayout` in `squeal-sql/src/table.rs`).
  - Integers take 9 B.
  - A string takes 9 B plus its declared width.
  - A NULL takes its column's full width.
  - Each row starts with an 8-byte count.

  Keys and index payloads are already compact; only row bodies are
  padded.
- **`CREATE INDEX` inserts in primary-key order**, so the index keys
  arrive at random and the leaves settle about 69% full.
- **Every tuple carries MVCC fields forever.** These are a transaction
  id, a previous-version LSN and flags, plus an 8-byte slot. Every page
  also reserves 608 B for a high key, data pages included.

### Plan

Each step is its own commit, checked by:
- the size probe;
- the full test suite;
- the SQLite comparison tests;
- crash soaks for the store steps (4, 5 and 6);
- the bench.

Old databases stay readable throughout: each new format has its own flag
or is unambiguous.

- [x] **Step 1. Stop materializing the primary-key tree** (squeal-sql).
  Done 2026-10-11.
  - The `is_primary` entry stays in `table.indices` (`SqlIndex::has_tree`
    is false for it), so positions, the catalog, `SHOW TABLE INDEX` and
    the ALTER guards are unchanged.
  - The row is written before any index entry, so a duplicate primary
    key still fails before anything is written.
  - Foreign-key checks probe the row tree with the same key.
  - The planner never offers the primary key as an index.
  - In existing databases the tree is no longer read or written. Its
    pages stay allocated until reclaiming them is added.
  - Measured on the size probe, 1M orders:

    | | before | after |
    |---|---:|---:|
    | table only | 154.5 MB | 104.5 MB (−50 B a row) |
    | with 2 indexes | 300.0 MB | 251.4 MB |

  - A/B on the 1M-order bench:

    | | before | after |
    |---|---:|---:|
    | bulk load, per row | 4.5 µs | 2.6 µs |
    | `UPDATE` 100 a txn, per row | 140 µs | 125 µs |
    | peak memory | 441 MiB | 405–412 MiB |
    | memory at the end | 383 MiB | 338–349 MiB |
    | reads | | unchanged |

  - The trade-off: a `count(*)` over a primary-key range reads the rows
    tree instead of the narrower primary-key tree, about 12% slower
    (83 → 93 ms for half of 1M rows). A full `count(*)` got 10% faster.
- [x] **Step 2. Packed rows** (squeal-sql `table.rs`, `Packed`). Done
  2026-10-11.
  - Each value is untagged, its type taken from the row's schema version.
    Integers drop their high zero bytes, strings and blobs are unpadded,
    and NULLs are a bitmap present only when one is.
  - The row starts with where each column ends (1, 2 or 4 bytes an
    offset), so a column is still found with an offset read: the O(1)
    access fixed-width rows gave.
  - A one-byte marker that no older row can start with marks the format.
    Fixed-width and tagged rows still read.
  - On the size probe, 1M orders:

    | | before | after |
    |---|---:|---:|
    | table only | 104.5 MB | 67.7 MB (−37 B a row) |
    | with 2 indexes | 251.4 MB | 202.8 MB |

  - A/B with everything cached (1 GiB cache), against fixed-width rows:
    - **orders, 6 columns:** every query is within ±1% or faster.
      Pushdown on an int is −7%, and pushdown on the last column +1%.
    - **wide, 25 columns:** pushdown on any column is −63 to −71%, and
      sums are −20 to −35%.
    - **inserts:** −3 to −11%.
    - **file:** −48%.
  - Getting there took three changes:
    - reading the tested column straight from the row bytes;
    - returning an `Option` per column;
    - jump-table integer reads.

    The first version was up to 28% slower on pushdown.
  - The 1M-order bench (default cache) against fixed-width rows:
    - every phase is the same or faster: PK lookup −21%, filtered join
      −33%, correlated `NOT EXISTS` −21%;
    - memory after reads, after writes and at the end is −6 to −8%;
    - live heap is lower throughout.
  - The bench's "after load" and peak read higher: 379 against 180 MiB,
    and 425 against 375. That is the allocator, not the rows:
    - Both builds reach the same high-water mark during `CREATE INDEX`
      (~370 MiB, 56% of it fragmentation).
    - The fixed-width build's allocator happened to return ~160 MiB to the
      system during `ANALYZE`; the packed build's didn't.
    - See section 2.
- [ ] **Step 3. No duplicate primary key in index entries.** Saves about
  6 B an entry.
  - A non-unique index writes an empty payload.
  - Its readers take the identity from the end of the key.
  - An empty payload is unambiguous, so old entries still read.
- [x] **Step 4. Build `CREATE INDEX` sorted, in bulk.** Done 2026-10-11.
  - squeal-sql sorts each partition's entries (`IndexEntries` into a
    spilling `SortSource`, within the query memory). The store builds the
    new tree bottom-up from them (`Db::bulk_load`):
    - leaves filled until a split would be due;
    - no WAL or version record per entry;
    - a checkpoint before the commit, for durability.
  - `SlottedPage::add` appends without searching when the key sorts last.
  - Sorting first and then inserting one at a time made indexes smaller
    but builds slower (3.1–4.5 s). The per-entry insert, not the leaf
    access, was the cost.
  - Measured at 1M orders:

    | per index | before | after |
    |---|---:|---:|
    | `customer_id` | 66 MB, 2.9–3.1 s | 63 MB, 1.2 s |
    | `day` | 69 MB, 3.7–4.5 s | 55 MB, 1.2 s |

    The index is smaller by its leaves only: each entry is also on a data
    page, which filled either way (step 5).
  - Most of what's left is the sort. SQLite takes 0.18 s.
  - Checked by a crash-harness thread that bulk-loads tables alongside
    the workload: a load that returned before the cut is whole after
    recovery.
  - The harness caught a race in the first version: the loader allocated
    pages without the writer permit.
    - A checkpoint could capture between a page's count and its install,
      and record a page it never wrote. Reopen then failed reading past
      the end of the file.
    - It failed at 1 in 20–150 seeds with a 24-page cache, and never
      without the bulk thread.
    - Fixed by holding the permit around each step's writes, as every tree
      write does. 1,600 seeds clean since.
- [ ] **Step 5. Keep entries in the leaf** (store B+tree; needs the
  author's go-ahead). Saves about 30 B an index entry.
  - A tree created as index-only keeps each whole entry in its leaf, with
    no data page.
  - The same mode could later hold short rows in the row tree: −21 B a
    row, and one page read fewer per lookup.
- [ ] **Step 6. Shrink the tuple header** (store page format v3). Saves
  about 10–20 B a row.
  - Drop the transaction id and previous-version LSN once a tuple is
    older than every reader.
  - 4-byte slots instead of 8.
  - No high-key reservation on data pages.

Expected, per order row with the table and its 3 trees:

| | per row |
|---|---:|
| today | ~300 B |
| after steps 1–4 | ~150 B |
| after step 5 | ~100 B |
| SQLite | ~50 B |

## 2. Allocator fragmentation: about half the footprint at the peaks (measured)

`vmmap` shows the malloc zone 46–49% fragmented at the peaks: 335 MB
dirty, but only 182 MB allocated. The macOS allocator keeps freed memory
between live allocations charged to the process.

The cause is churn of mixed-size allocations among long-lived 16 KiB
pages. Sections 4 and 6 list the main sources. Cutting the churn is the
fix. A different allocator is out (no mimalloc).

- [ ] **`CREATE INDEX` leaves the heap 56% fragmented** (measured
  2026-10-11, 1M orders).
  - About 71 MB of it is freed large blocks ("Malloc Large (empty)"):
    39 MB after the bulk load, 71 MB after the index builds.
  - Whether the allocator later returns them is luck, and the bench's
    "after load" and peak readings swing by ~200 MiB with it.
  - Suspect: the per-transaction `WriteLog` (`store/src/version.rs`). It
    is one `Vec<u8>` that doubles to ~16 MB for a million writes, leaving
    each smaller copy freed. Unconfirmed.
  - Writing it in fixed-size chunks would keep it out of the large-block
    allocator.


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

1. **Density** (section 1, with its own plan): the biggest lever, since it
   multiplies through the cache, the WAL and I/O.
2. **Search arrays** (3) and **the unbounded maps** (4).
3. **Per-insert and per-row churn** (6): this is what drives the
   fragmentation.
4. **`ValueItem` to 24 B and `Tuple` to 56 B** (5).
5. **Charge real heap sizes against the query budget** (7).
6. **The parse-cache byte bound** (8), then **the defaults** (9).
