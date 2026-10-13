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
- [x] **2. Parameter values, not a copied tree.** Done 2026-10-12.
  - `sql_parser::parse_sql_shaped` hands back a shape's template, shared
    and not copied, with the statement's literals beside it. A literal has
    the span of the placeholder standing for it.
  - squeal-sql's `plan::params::Params` holds the values by span. SELECT,
    UPDATE, DELETE and EXPLAIN run from the template, and every place that
    reads a literal reads a placeholder through `Params`:
    - `EvalExpr::from_expr` plans a placeholder as its value;
    - `LIMIT`, `OFFSET` and `ORDER BY` positions.
    Other statements (INSERT, DDL) still take a bound copy.
  - So plans are exactly what they were: every decision sees the values.
    The `EvalExpr::Param(i)` slot, which a cached plan needs, comes with
    stage 3.
  - Prepared `SELECT`, `UPDATE` and `DELETE` now run, through the same
    `Params` (they used to return an error).
  - Point lookup: 163 → 152 allocations, ~5.5 µs. A text served from the
    exact-text cache costs one allocation more than before (its values'
    list, where it used to get a tree with them bound in): the
    secondary-index lookup went 188 → 189.
  - Telling integer and float literals apart in the shape key is left for
    stage 3, where it matters: values are not in the plan until then.
- [x] **3. The prepared-plan cache.** Done 2026-10-12.
  - `handle_select` is two halves:
    - `prepare_select` (no value decides it): FROM resolved, expressions
      built, conditions sorted by table (`analyze_query`).
    - `run_select` (per execution): comparisons implied through join
      equalities, `GROUP BY` validation, partitions, access paths, joins,
      and the sources opened.
  - A plain SELECT is prepared once per shape with `EvalExpr::Param` slots
    and kept in the connection's `PlanCache`. Each execution clones it,
    binds its values in (folding what then becomes constant), refreshes
    each table's statistics, and runs the second half, so every
    value-sensitive choice is made again. Plan quality is unchanged.
  - **Key:** the address of the outermost query in the parse cache's
    shared tree. The entry holds that tree, so the address can't be reused.
  - **Valid** while a process-wide catalog generation (bumped by any table
    created, dropped or altered, and by a schema made or loaded) and the
    connection's current schema are the ones it was prepared under.
  - **Not kept**, planned as before every time (decided from the syntax
    tree, so nothing runs with unbound slots):
    - WITH queries, FROM subqueries and subqueries anywhere;
    - temp tables;
    - an `ORDER BY` position.
  - Prepared statements use it too: their tree is fixed.
  - Integer and float literals need no separate keys after all: nothing
    kept depends on the values' types.
  - **Measured**, 500k orders:

    | | before | after |
    |---|---:|---:|
    | point lookup by primary key | 5.5 µs | 4.0 µs |
    | secondary-index lookup + aggregate | 20.5 µs | 13.5 µs |

  - **Where a point lookup's 4.0 µs goes now:**
    - planning per execution, ~58%: choosing the access path ~0.5 µs,
      opening the sources ~0.5 µs, cloning and binding the kept plan
      ~0.5 µs;
    - reading the row ~18%;
    - parsing (tokenizing, the shape lookup) ~14%;
    - dropping the result ~7%.
- [ ] **3b. Reusing value-blind choices.** An access path that no value
  can change (an equality on every column of the primary key, or of a
  unique index) is kept with the plan, so an execution only builds its key
  from the values. The kept plan is also made cheaper to bind: shared
  parts behind `Arc`, so only expressions are copied.
- [ ] **4. What is left per statement**, measured again after 3: the
  statement transaction, locks, and the result set.
