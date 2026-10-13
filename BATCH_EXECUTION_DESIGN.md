# Retiring ValueItem from execution

Started 2026-10-12. The plan of record for how query execution represents
values: as bytes and columns, not as owned `ValueItem`s.

## The problem

`ValueItem` is an owned, self-contained value, and squeal-db uses it for
every job a value has:
- the row a scan decodes;
- a key;
- an expression's result;
- the rows an operator keeps (a sort's, a hash join's build side, a
  group's key);
- the rows handed to the client.

Owned means each string is its own `String` and each row its own `Vec`.
Anything that moves a value, keeps one or computes one allocates or copies.
Lending rows and reusing buffers (BYTE_KEYS_DESIGN.md stage 6) hid that on
the scan, filter and aggregate paths. They can't remove it: wherever a row
is kept or produced, it is still an owned `Vec<ValueItem>`.

It also sets the cost of compute. Measured on 500k orders, release build:

| query | per row | where it goes |
|---|---:|---|
| `sum(price)` | 59 ns | turning each row into `ValueItem`s ~40%, the aggregate's expression ~20%, the cursor ~15% |
| `GROUP BY item` with `count(*)`, `sum(qty)` | 129 ns | turning rows into values ~34% (half of it re-checking each string's UTF-8), expressions and aggregates ~23%, hashing the group ~10% |
| join + `GROUP BY` | ~370 ns, 1.3 allocations | |
| `ORDER BY price` | ~300 ns, 1 allocation | |
| `select *`, streamed | ~190 ns, 2 allocations | |

A vectorized engine sums a column at about 1 ns a row.

## The goal

**Allocations per query: a fixed number, nothing per row.** Time follows,
because nothing is allocated, copied or matched on a tag per value.
Every stage is measured first by allocations per query, then by time.

## The design: a representation per role

| role | representation |
|---|---|
| at rest | bytes: packed rows, keycode keys (as now) |
| in flight between operators | batches of typed columns, in buffers each operator owns and refills |
| one value looked at | `ValueRef<'a>`, a 16-byte `Copy` view |
| kept by an operator | bytes copied into the query's arena |
| at the edges (results to the client, bound parameters, the catalog) | `ValueItem` |

### Batches

- A **batch** is up to 1,024 rows held as columns (the size is to be
  tuned: a batch should stay in cache).
- A **column** is a typed vector plus a validity bitmap. It uses Arrow's
  memory layout:
  - integers and datetimes as `i64`, doubles as `f64`;
  - booleans as one byte each (a bitmap later, if it pays);
  - strings and blobs as `u32` offsets into one byte buffer.
- **UTF-8:** a string column's buffer is checked once, whole, when it is
  filled, not value by value.
- A **selection vector** (the positions of the rows still in play) lets a
  filter drop rows without copying a column. A batch is compacted only
  when few rows are left.
- **Buffers are reused.** An operator keeps its batch and refills it.
- **One engine for every query size.** A seek that finds three rows makes
  a batch of three. Rows exist only at the edge, for the result set.

### The arena

- What an operator keeps past one batch (sort rows, a hash join's build
  side, group keys, materialized results) is copied as bytes into the
  query's arena: large chunks, bump-allocated, all freed when the query
  ends.
- Kept rows are byte ranges in the arena; kept keys are keycode bytes,
  compared and hashed as bytes.
- No allocator API is needed: the arena is chunks of bytes addressed by
  offset. When `allocator_api` stabilizes, this becomes the epoch allocator,
  and collections can live in it too.
- Its chunks are counted in `QueryMemory`, and spill as today.

### Operators

- **`Source` gains batches** beside lent rows: `batches()` and
  `next_batch()`, which lends a batch the way `advance` and `row` lend a
  row.
- **Adapters both ways** (batches to rows, rows to batches) let operators
  move over one at a time. Every query keeps working at every step.
- **Scan:** a row's wanted columns are read from its packed bytes straight
  into the column vectors (no `ValueItem`). Pushed-down tests run on the
  bytes before a row is taken. Later: visibility and the row format's
  version checked once per page, not per row.
- **Filter:** the predicate over columns gives the selection.
- **Projection:** computes new columns. A plain column passes through,
  shared.
- **Aggregates:** `sum`, `count`, `min`, `max`, `avg` are loops over one
  column under the selection.
- **Hash GROUP BY:** hash a batch's key columns, find the groups, update
  typed accumulators. Group keys are kept in the arena.
- **Hash join:** the build side's rows are kept in the arena; probing a
  batch gives match lists, and the output columns are gathered.
- **Sort:** sort keys are written once as keycode bytes into the arena, and
  sorted as bytes. This is BYTE_KEYS_DESIGN.md stage 4: the query sort and
  `CREATE INDEX` share it.

### Expressions

- `EvalExpr` gets a batch evaluation. Each node computes a whole column, so
  the type is matched once per batch, not once per value.
- Kernels exist per operator and type pair. NULLs go through the validity
  bitmaps.
- A function gets a batch entry point. By default it calls its row
  evaluation per row, so every function works from the start.

### SIMD

Only once there are batches.
- Kernels are first plain loops over slices. On aarch64 (NEON is
  baseline) the compiler vectorizes most of them.
- `fearless_simd` is for what it won't vectorize, and for runtime dispatch
  to AVX2 on x86:
  - a compare to a mask, then compacting the selection;
  - hashing a batch of keys;
  - byte searching and escaping in strings and keys.

### Storage, and Parquet

- **Packed row pages** feed batches as described under Scan, with today's
  on-disk format.
- **PAX pages** (laid out by column within each page) come later. A column
  is then nearly a slice of the page, and pages are denser. It is an
  on-disk change, so it will be brought for approval.
- **Parquet:** a row group is a run of batches, and a column chunk's pages
  (plain, dictionary or RLE encoded) decode into column vectors.
  - A `ParquetSource` reads only the columns the query needs, and skips
    row groups whose min/max statistics rule them out, as partitions are
    pruned today.
  - Reading a file as a table first; writing one (`COPY ... TO`) from
    batches after.
- **Column types:** squeal-db's own, in Arrow's layout, so a Parquet
  reader's arrays move in by a cheap conversion. No `arrow` dependency for
  now (size, squeal-wasm).

## Stages

Each stage is its own commit, measured (allocations per query, then time)
with the allocation probe and the 1M-order bench, and checked by the
workspace tests and the SQLite comparison tests.

- [x] **1. Batches exist.** Done 2026-10-12.
  - `crate::batch`: `Batch` and `Column`. A string column's bytes are
    checked as UTF-8 once per batch, whole. A value not of its column's
    type (rows in an older format can hold one) turns that column into
    plain values, so a batch is always right.
  - `Source::batches`, `advance_batch` and `batch` lend a batch the way
    `advance` and `row` lend a row. The table scan and the partition reader
    give batches. The scan fills them straight from packed rows
    (`decode_row_to_batch`), without making a value.
  - Whole-table aggregates (no `GROUP BY`, no `HAVING`) of plain columns
    read batches (`source::total`): `count`, `sum`, `avg`, `min` and `max`
    each take a column at a time, in row order, so totals are what row by
    row gives. Anything else is read row by row, as before.
  - **Measured**, 500k orders, no allocations per row before or after:

    | | before | after |
    |---|---:|---:|
    | `count(*)` | 64 ns a row | 27 ns |
    | `sum(price)` | 59 ns | 28 ns |

  - **What is left in those 27 ns** is per-row work in the cursor and the
    row format: the cursor's visibility check and tuple access (~50%), and
    each row's header and offsets (~38%). That is stage 7.
- [ ] **2. Expressions and filters on batches:** batch evaluation of
  `EvalExpr`, selection vectors, projections.
- [ ] **3. The arena, and hash GROUP BY on batches** with its keys there.
- [ ] **4. Hash join on batches,** its build side in the arena.
- [ ] **5. Sort on keycode bytes in the arena,** shared with `CREATE INDEX`.
- [ ] **6. The result set:** rows to the client lent from batches.
  `ValueItem` is then only at the edges.
- [ ] **7. Per-page work in the scan:** visibility and row-format version
  checked once per page.
- [ ] **8. SIMD kernels** where the plain loops don't vectorize, measured
  one by one.
- [ ] **9. Parquet:** reading a file as a table, with column projection and
  row-group pruning; then writing.
- [ ] **10. PAX pages.** On-disk change: approval first.
