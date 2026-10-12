# Planning once per statement shape

Started 2026-10-12. The plan of record for cutting what squeal-sql does per
statement.

## The problem

A point lookup by primary key takes about 7 µs, against SQLite's 1.5, and
makes about 178 allocations. Measured on 500k orders:

| where | allocations | time |
|---|---:|---:|
| planning (`LogicalPlan::build`) | 84% | most of it |
| the parse-cache hit (a deep copy of the syntax tree) | 11% | ~10% |
| reading the row (seek, decode, result) | ~3% | under a third |

Every statement is planned from nothing, even when a statement of the same
shape (the same text but for its literals) was planned a microsecond
before. Each one:
- resolves its tables;
- rebuilds their statistics, walking root to leaf of the row tree and
  every index tree, per partition;
- builds its expression trees, projections and conjunct analysis again;
- and only then picks an access path and opens a cursor.

## The design: prepare once, bind and choose per execution

Planning splits in two.

**Prepared, once per shape, and cached.** None of it depends on the
literal values:
- name resolution of the FROM items (the `Arc<SqlTable>`s and their
  fields), and the join structure;
- expression trees for projections, `ON`, `WHERE`, `GROUP BY` and
  `HAVING`, with a parameter slot (`EvalExpr::Param(i)`) where each
  literal was;
- display names, hidden `ORDER BY` columns, group positions;
- conjunct classification and which columns each table must supply.

**Per execution.** Everything that depends on values, run exactly as
today:
- bind the literal values into the slots, then fold constants;
- derive comparisons across joins, build key ranges (the 256-range cap,
  the NULL and integer/double edge cases), prune partitions;
- pick access paths, the join algorithm and the hash build side, from the
  statistics;
- `LIMIT`/`OFFSET`, `GROUP BY` validation;
- open the sources under the transaction, with a fresh `QueryMemory`.

So plan quality is unchanged: every decision still sees the actual values.
What stops being repeated is the work that never depended on them.

**What makes a cached plan valid.**
- The cache key is the shape (with integer and float literals told apart,
  since the decisions differ) and the current schema.
- Each execution checks every cached table against the catalog
  (`Arc::ptr_eq`, or a per-schema DDL generation counter). An `ALTER` or
  `DROP` makes the plan stale and it is prepared again.
- Statistics are not in the key. They are cached per table under a
  statistics epoch, and a newer epoch is simply read at execution.

**Not cached; prepared as today:**
- statements with subqueries, CTEs or derived tables (these run parts of
  themselves at plan time);
- statements on temporary tables;
- an `ORDER BY` given by position.

## Stages

Each stage is its own commit, measured with the allocation probe and the
1M-order bench, and checked by the workspace tests and the SQLite
comparison tests.

- [x] **1. Statistics computed once, not per statement.** Done 2026-10-12.
  - A table's planning statistics (`ComputedTableStat`, with the tree
    shapes `btree_range_params` walks for) are cached beside its stored
    statistics. They are dropped wherever the column-stats snapshot is
    (a stats update from writes, a reshape; `ANALYZE` rebuilds the entry),
    and tied to the table definition by `Arc::ptr_eq`, so an `ALTER`
    rebuilds them.
  - The name, per-partition counts and index stats are shared (`Arc`), so
    handing a plan its copy allocates nothing.
  - A tree that grows a level without a stats update (its message
    dropped) keeps an older depth until the next update. That only shifts
    cost estimates.
  - Point lookup: 178 → 163 allocations; the tree walks are gone from the
    profile, and the stats step fell to ~2% of a lookup's time (~9% faster
    in total).
- [ ] **2. Parameter slots.**
  - The parse cache hands back a shape's template, shared and not copied,
    with the statement's literals beside it (`parse_sql_shaped`).
  - `EvalExpr::Param(i)` reads the i-th value at execution.
  - Placeholders (`?`, `$n`, `:name`) plan as parameters. That gives
    prepared `SELECT`, `UPDATE` and `DELETE`, which today return an error,
    for free.
- [ ] **3. The prepared-plan cache**, as designed above: the prepared part
  cached per shape and schema, validated against the catalog each
  execution, with everything value-sensitive run per execution.
- [ ] **4. What is left per statement**, measured again after 3: the
  statement transaction, locks, and the result set.
