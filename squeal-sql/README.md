# squeal-sql

The SQL database, built on [`store`](../store/README.md) with
[`sql-parser`](../sql-parser/README.md)'s grammar.

## What it supports

- **Queries:**
  - `SELECT [DISTINCT]` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`,
    `LIMIT`, `OFFSET`
  - inner, left, right, full and cross joins (`ON` or `USING`)
  - `UNION [ALL]`, `INTERSECT`, `EXCEPT`
  - `WITH` (common table expressions), subqueries in `FROM`
  - `x [NOT] IN (SELECT …)`, `[NOT] EXISTS (SELECT …)` and scalar
    `(SELECT …)` in `SELECT`, `WHERE` and `HAVING`, and in `UPDATE` and
    `DELETE` (see below)
  - aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)`)
    and scalar functions
- **Writes:** `INSERT … VALUES | SELECT`, `UPDATE`, `DELETE`, `TRUNCATE`,
  and `COPY INTO t FROM @file.csv`.
- **DDL:**
  - `CREATE TABLE`, with primary key, unique, not null, default and
    foreign key constraints
  - `CREATE TABLE … AS COPY FROM @file.csv`, which infers the columns
  - `ALTER TABLE` (add, drop or rename a column; add or drop a
    constraint; rename the table)
  - `CREATE [UNIQUE] INDEX`, `DROP TABLE`, `DROP INDEX`
- **Partitioning:** `PARTITION BY RANGE | LIST`, with `ADD` and `DROP
  PARTITION`.
- **Organization:** databases and schemas (`CREATE SCHEMA`, `USE`), and
  connection-private temp tables (`temp.<name>`).
- **Transactions:** `BEGIN`, `COMMIT`, `ROLLBACK`, with snapshot isolation
  from the store.
- **Prepared statements:** `PREPARE`, `EXECUTE`, `DEALLOCATE`, with `?` /
  `:name` placeholders.
- **Introspection:**
  - `EXPLAIN`
  - `ANALYZE`
  - `SHOW TABLES` / `SCHEMAS` / `PARTITIONS` / `TABLE INDEX`
  - `DESCRIBE TABLE`
- **Types:** `INTEGER`, `DOUBLE`, `VARCHAR(n)`, `BLOB(n)`, `DATETIME`,
  `BOOLEAN`.

The full cheat sheet is `squeal_sql::help::SQL_HELP`. It is the same text
`!help` prints in the CLI and the browser demo.

## Layers

```
Connection ─ Statement ─┬─ parse (sql-parser, cached)
                        ├─ plan   (plan::logical → optim::picker)
                        └─ execute (source::* — a tree of pull-based Sources)
Database ─ Schema ─ SqlTable ─ Partition ─ store tables
```

- **`conn`**
  - `ConnectionManager` → `Connection`, which holds the current database,
    the current schema, the open transaction and temp tables.
  - `tablelock`: the shared/exclusive table locks that keep DDL from
    changing a table under a statement. They detect deadlocks and fail
    them at once.
- **`schema_ops`**
  - `Database` wraps one store `Db`, and holds many `Schema`s.
  - A schema's catalog is a store table, and its tables are
    schema-qualified store tables.
- **`table`.** `SqlTable`: the columns, indexes, foreign keys and
  partitions, and how a row is encoded.
- **`plan`.** Turning an AST into an executable tree:
  - name resolution
  - expression compilation (`eval`)
  - WHERE analysis (`conjuncts`, `sarg`)
  - built-in functions
  - the per-query memory budget
- **`optim`.** Statistics, and the choices that use them (below).
- **`source`.** The executors.
- **`stmt`.** `Statement` / `PreparedStatement`: execute a parsed
  statement, stream the results.

## How SQL maps onto the store

Every table is one or more **partitions**, and an unpartitioned table has
one. Each partition has:

- **A rows tree.** Keyed by the primary key, as a composite
  `IndexKey`. A table without one gets an auto-increment row id from a
  store sequence. The payload is the encoded row.
- **One tree per index.** Keyed by the indexed columns. For a non-unique
  index, the row's key is appended, so equal values are still distinct
  keys. The payload is the row's key.
  - A `UNIQUE` or `PRIMARY KEY` constraint is enforced by the store's own
    duplicate-key check, with no extra lookup.
  - Indexes are local to each partition, so a unique key must include the
    partition column.

**Rows are fixed-width when every column type has a width.** Each column
takes its type's bytes whatever its value, with `VARCHAR(n)` padded to `n`.
So:
- a column is found at a fixed offset, without decoding the ones before it
- an `UPDATE` never changes a row's size
- a `WHERE` test can run against a row's bytes in place, before the row is
  built (`ColumnTest`)

**Schema changes don't rewrite rows.** A table keeps an append-only list of
`SchemaVersion`s, and each row records the version it was written under.
`ALTER TABLE` pushes a new version, and old rows are projected onto the
current one as they are read.

**Foreign keys** are checked on the referencing side: on insert and update,
and when the constraint is added. Both the check and the inner side of nested-loop joins read rows
lent from the page, never copied.

## The cost-based optimizer

**Statistics** are kept per table and column:
- row count
- distinct values (a bloom-filter estimate, exact for unique columns)
- null count
- min and max
- rows per partition

A background thread updates them incrementally as rows are written, and
they persist in a store table per schema. `ANALYZE` recomputes them from
scratch. On wasm, which has no threads, they are applied inline.

**The unit of cost is bytes read.** Every choice below compares estimates
of how many bytes each alternative reads (and, for joins, builds and
spills).

**Access paths** (`optim::picker::pick_access`). For each table in `FROM`,
the candidates are:

| path | reads |
|---|---|
| `TableScan` | every row |
| `TableSeek` | key ranges of the primary key (the table's own tree) |
| `IndexScan` | every entry of an index that *covers* the query |
| `IndexSeek` | key ranges of a covering index |
| `IndexLookup` | key ranges of a non-covering index, then each row from the table |

Their costs are estimated as follows:

- **Turning WHERE into key ranges** (`plan::sarg`). Conditions of the form
  `column op constant` become equalities on the leading key columns, then
  at most one range on the next.
- **Ranges are only ever widened.** A condition is used only when its
  constant compares exactly as the evaluator would compare it. The full
  WHERE still runs on top, except for conditions the path is known to
  enforce exactly.
- **Row estimates.** Rows in range are estimated from min/max
  interpolation and distinct counts. Each seek costs one page for the
  descent. An `IndexLookup` pays one page per fetched row, because those
  rows are scattered.
- **Order.** When `ORDER BY` (with an optional `LIMIT`) can be served by a
  key's order, a path that produces it saves the sort and can stop after
  `LIMIT` rows. A path that doesn't produce the order pays for the sort.
- **Without statistics,** only choices that are safe anyway: a primary-key
  seek, or an equality on every column of a unique index.
- **Every path yields rows in the table's own layout.** That is why they're
  interchangeable without renumbering any column downstream.

**Joins** (`plan::logical`, `optim::picker::pick_join_seek`). The joins
are taken in `FROM` order. Each join chooses its algorithm by cost:

- **Hash join.** This is the default. The build side is the larger input
  if it fits the query's memory budget, else the smaller one. Spilling is
  costed in, and spills go to store scratch runs.
- **Sort-merge join.** Used when both inputs can be read in key order, or
  when the merge's output order saves a later `ORDER BY` or `GROUP BY`.
- **Index nested-loop join.** Used when few outer rows meet a large inner
  table: one seek per outer row, into the inner table's primary key or an
  index, beats reading the inner table for a hash build.

The other rewrites:

- **Implicit joins.** `FROM a, b WHERE a.x = b.y` becomes a hash join, not
  a cross join.
- **Predicate pushdown.** Conditions are pushed to the table they read,
  except below the NULL-extended side of an outer join, where pushing would
  change the answer.
- **Partition pruning.** A partitioned table reads only the partitions its
  conditions allow.

**EXPLAIN** prints the tree that would actually run. It is built from the
same `Source`s, each describing itself, with the estimated rows. `!print
stats` (CLI) shows per-operator rows and time from the last query.

**Subqueries in expressions** (`plan::subquery`) run once, while the
query holding them is planned and inside its transaction, never once per
outer row:

- **Uncorrelated.** The rows are the answer. `IN` keeps them as a hash
  set, with SQL's NULL rules. `EXISTS` and a scalar subquery become
  constants, so they can still bound an index seek.
- **Correlated through equalities.** Take `EXISTS (SELECT 1 FROM c WHERE
  c.pid = p.id AND c.qty > 5)`. The `inner = outer` conditions are taken
  out and their inner sides selected instead, which leaves an
  uncorrelated query. Its rows are grouped by those keys, and each outer
  row looks its key up. The effect is a hash semi-join, or an anti-join
  for `NOT`.
- **Anything else correlated is refused** rather than answered wrongly:
  - an outer column outside an `=` condition
  - a correlated scalar subquery
  - a correlated subquery that groups, aggregates or limits

## Executors

Execution is a pull-based tree of `Source`s:
- table, index and temp-table scans and seeks
- filter, projection and limit
- sort (external, spilling sorted runs and merging them)
- hash, sort-merge and nested-loop joins
- group and aggregate
- set operations
- appending partitions

Streaming operators hold O(1) rows. Blocking ones (sort, hash build,
grouping) reserve memory from the query's budget and spill to store
scratch runs past it, so a query's memory is bounded, not proportional to
its input.

## Known gaps

- **Correlated subqueries beyond equalities.** Correlated subqueries
  run only when linked to the outer query by `inner = outer` conditions,
  and not when they aggregate or are used as a value (see above). They
  are not allowed in `JOIN … ON` or `GROUP BY` either.
- **`UPDATE` and `DELETE` scan the whole table**, even with `WHERE id =
  …`. They don't use the optimizer's seeks yet (see TODO.md).
- **No constant folding.** `day < 10 + 7` can't bound a seek, but
  `day < 17` can.
- **Deleting a referenced row.** Nothing checks it: there is no `ON
  DELETE` action, and no restriction.
- **Join order.** Joins run in `FROM` order and are not reordered by cost.
- **Batched statements.** In a batch run as one `Statement`, a `SELECT`'s
  rows are produced lazily, after the later statements have run. So
  `begin; select …; rollback` reads after the rollback. The browser demo
  runs statements one at a time. See [TODO.md](../TODO.md).

## Comparing with SQLite

```bash
cargo run --release -p squeal-sql --example load_vs_sqlite -- 200000 4
```

The example runs one workload against squeal-db and SQLite (bundled),
both on disk and equally durable: SQLite in WAL mode, with
`synchronous=FULL` and `fullfsync` on. The workload covers:
- bulk load, index builds, `ANALYZE`
- point, index and range lookups
- scans, joins and subqueries
- durable single-row and batched writes
- concurrent readers

It prints a table of each phase's time and rate on both engines, and
checks that their answers agree. `SQ_EXPLAIN=1` also prints the plans
squeal-db chose.

## Testing

```bash
cargo test -p squeal-sql --release
```

About 760 tests. Among them:

- **`stmt/tests/oracle.rs`.** A differential test against SQLite (through
  `rusqlite`). It generates random schemas, data and queries (joins,
  aggregates, set operations, NULLs) and fails on any
  disagreement. The failure names the seed and the query. A second run
  adds random `IN`, `EXISTS` and scalar subqueries, both correlated and
  uncorrelated.
- **Partition, merge-join, index-seek and soak suites,** and race tests
  for DDL against running statements.
