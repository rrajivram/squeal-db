# Batch execution

Started 2026-10-12. The plan of record for executing queries over batches of
rows held as columns, rather than one row at a time.

## The problem

A scan costs tens of nanoseconds a row in squeal-db. A vectorized engine
(DuckDB, DataFusion) sums a column at about 1 ns a row. Measured on 500k
orders, release build:

| query | per row | where it goes |
|---|---:|---|
| `sum(price)` | 59 ns | decoding the row ~40%, the aggregate's expression ~20%, the cursor ~15% |
| `GROUP BY item` with `count(*)`, `sum(qty)` | 129 ns | decoding ~34% (half of it re-checking each string's UTF-8), expressions and aggregates ~23%, hashing the group ~10% |
| `item, qty WHERE qty > 5` | ~110 ns | |
| join + `GROUP BY` | ~370 ns | |
| `ORDER BY price` | ~300 ns | |

The cause is not missing SIMD. It is the shape of execution:
- **One row at a time.** Every operator handles a row per call, through a
  `dyn Source`.
- **Rows as enums.** Each row becomes a `Vec<ValueItem>`, a 24–32 byte
  tagged value per column. Every value has its tag matched on every use,
  and every string is copied and checked again.
- **Expressions walked per row.** `EvalExpr` is a tree, interpreted once
  per row, per node.

The lent-row work (BYTE_KEYS_DESIGN.md stage 6) took the allocations out of
this path. It didn't change its shape.

## The design

### Batches

- A **batch** is up to 1,024 rows (to be tuned: it should stay in cache)
  held as columns.
- A **column** is a typed vector plus a validity bitmap. It uses the Arrow
  memory layout:
  - integers and datetimes as `i64`, doubles as `f64`;
  - booleans as a bitmap;
  - strings and blobs as `u32` offsets into one byte buffer.
- A **selection vector**, the positions of the rows still in play, lets a
  filter drop rows without copying any column. A batch is compacted only
  when few rows are left.
- **Buffers are reused.** Each operator keeps its batch and refills it, so
  a query allocates per operator, not per row or per batch. They are
  counted in `QueryMemory`.
- **One engine.** A seek that finds three rows makes a batch of three.
  Rows are formed only at the edge, for the result set.

### Operators

- **`BatchSource`**, next to `Source`: `next_batch()` lends a batch, as
  `advance`/`row` lend a row today.
- **Adapters** both ways (batches to rows, rows to batches), so operators
  move over one at a time, as lending rows did. Every query keeps working
  at every step.
- **Scan.** A page at a time: for each column the query reads, walk the
  page's packed rows and append that column's value to its vector. Packed
  rows have fixed-width end offsets, so this is a strided read. Pushed-down
  tests run on the column and produce the selection vector.
- **Filter.** The predicate over whole columns gives a selection.
- **Projection.** Computes new columns. A plain column passes through,
  shared, not copied.
- **Aggregates.** `sum`, `count`, `min` and `max` are a loop over one
  column under the selection.
- **Hash GROUP BY.** Hash a batch's key columns in one pass, find the
  groups in a second, then update typed accumulators.
- **Hash join.** Build columns; probe a batch, giving match lists; gather
  the output columns.
- **Sort.** Each batch's sort keys are written once as keycode bytes, and
  sorted as bytes. This is BYTE_KEYS_DESIGN.md stage 4; the query sort and
  `CREATE INDEX` share it.

### Expressions

- `EvalExpr` gets a batch evaluation. Each node computes a whole column, so
  the type is matched once per batch, not once per value: `qty > 5` is one
  loop over an `i64` vector giving a bitmap.
- Kernels exist per operator and type pair. NULLs go through the validity
  bitmaps: a result is valid where all its inputs are.
- A function gets a batch entry point. By default it falls back to calling
  its row evaluation per row, so every function works from the first stage
  and the common ones are converted as they matter.

### SIMD

Only after the batches exist.
- Kernels are first plain loops over slices. On aarch64 (NEON is
  baseline) the compiler vectorizes most of them.
- `fearless_simd` is for what it won't vectorize, and for runtime dispatch
  to AVX2 on x86:
  - a compare to a mask, then compacting the selection;
  - hashing a batch of keys;
  - byte searching and escaping in strings and keys.

### Storage, and Parquet

- **Row-major packed pages** can feed batches as described under Scan.
  That keeps today's on-disk format.
- **PAX pages**, laid out column by column within each page, come later.
  A batch could then take a column nearly straight from the page, and
  pages would be denser. It is an on-disk change, so it will be brought
  for approval.
- **Parquet** fits the model directly:
  - A row group is a run of batches. A column chunk's pages (plain,
    dictionary or RLE encoded) decode into column vectors.
  - A `ParquetSource` is a `BatchSource`. It reads only the columns the
    query needs, and skips row groups whose min/max statistics rule them
    out, as partitions are pruned today.
  - Reading a file as a table first; writing one (`COPY ... TO`) from
    batches after.
  - Dictionary-encoded strings may later stay dictionaries in the batch.
- **UTF-8** is checked when a string is written, not each time it is read.

### Open questions

- **Our own column types (Arrow layout), or the `arrow` crate.**
  - Our own: small, fits `ValueItem` and keycode bytes, builds for
    squeal-wasm, no large dependency.
  - `arrow` + `parquet`: a full Parquet reader and writer for free, but a
    heavy dependency.
  - Recommended: our own types in Arrow's layout, so moving a Parquet
    reader's arrays in is a cheap conversion, not a rewrite. Decide before
    stage 1.
- **Batch size:** 1,024 to start, measured against 2,048 at stage 3.

## Stages

Each stage is its own commit, measured with the allocation probe and the
1M-order bench, and checked by the workspace tests and the SQLite
comparison tests.

- [ ] **1. Batches exist.** The batch and column types; a table scan that
  produces them; the batches-to-rows adapter. `count(*)` and `sum` run on
  batches end to end.
- [ ] **2. Expressions and filters on batches.** Batch evaluation of
  `EvalExpr`, selection vectors, and pushed-down tests on columns.
- [ ] **3. Aggregates and hash GROUP BY on batches.**
- [ ] **4. Hash join on batches.**
- [ ] **5. Sort on keycode bytes,** shared with `CREATE INDEX`.
- [ ] **6. SIMD kernels** where the plain loops don't vectorize, measured
  one by one.
- [ ] **7. Parquet:** reading a file as a table, with column projection and
  row-group pruning; then writing.
- [ ] **8. PAX pages.** On-disk change: approval first.
