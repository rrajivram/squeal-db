# squeal-db

A transactional database engine in Rust: one generic, indexable,
transactional **blob store**, with SQL and MongoDB-style JSON documents built
on top of it as separate layers.

**Try it in a browser:** https://squeal-db-demo.rrajivram-seattle.workers.dev
(SQL and JSON documents, running entirely in WebAssembly).

## The thesis

Most databases fuse storage, transactions and the query language together.
squeal-db keeps them apart:

- **The store knows nothing about SQL or JSON.** It stores keyed tuples of
  bytes in B+trees. It gives them transactions (MVCC snapshot isolation), a
  write-ahead log, crash recovery and range scans. A key is a typed composite
  value, so the store can order and seek it. The payload is opaque bytes.
- **Each data model is a layer over the store.** `squeal-sql` maps tables,
  rows, indexes, foreign keys and partitions onto store tables. `sq-json`
  does the same for collections, documents and multikey indexes. Both use
  the same transactions and recovery, and both can share one database
  file. The browser demo keeps SQL tables and JSON collections in a single
  store.
- **Memory-safe by design.** It is written in Rust, with minimal `unsafe`:
  - A handful of small, commented blocks exist, all in `store` (see
    [store/README.md](store/README.md#unsafe)).
  - There are none in the SQL, JSON, parser or front-end crates.
- **In-memory and on-disk from the ground up.** The engine is generic over
  its file type (`Db<F: DBFile>`). A real file and an in-memory file are two
  implementations of one trait, not two code paths. The same WAL,
  checkpoints and recovery run on both. That is what lets the engine run in
  a browser tab, and lets tests simulate a power cut by keeping only the
  bytes that were fsynced.
- **A hand-built SQL dialect.** The grammar is written in this repo rather
  than taken from a general-purpose SQL parser crate, to keep control of the
  dialect.
  - It follows the model in LakeSail's
    [*a SQL parser in one week*](https://lakesail.com/blog/sql-parser-in-one-week/):
    each AST type derives its own parser.
  - The parsers are built from [chumsky](https://github.com/zesterer/chumsky)
    combinators.
  - A small proc-macro (`macros`) generates the derive.

## Crates

| crate | what it is |
|---|---|
| [`store`](store/README.md) | The engine: pages, page cache, B-link trees, WAL, MVCC transactions, checkpoints, recovery, file backends |
| [`sql-parser`](sql-parser/README.md) | The SQL lexer, AST and parser (chumsky combinators, derived per AST type), plus a parse cache |
| [`macros`](macros/README.md) | `#[derive(SQLParser)]`: builds a parser from the shape of an AST type |
| [`squeal-sql`](squeal-sql/README.md) | The SQL database: catalog, schemas, planner, cost-based optimizer, executors, connections |
| [`sq-json`](sq-json/README.md) | A MongoDB-style document database and a mongosh-style shell, on the same store |
| [`squeal-cli`](squeal-cli/README.md) | A terminal SQL REPL |
| [`squeal-wasm`](squeal-wasm/README.md) | The browser build (wasm-bindgen): SQL and JSON, persisted to IndexedDB; the live demo |
| [`ws-napi`](ws-napi/README.md) | A Node.js addon (napi-rs): native, or WASI with a real persistent file |
| [`bench`](bench/README.md) | Comparisons with SQLite, Turso and redb: speed and memory, under default and large memory settings |

```
  squeal-cli   squeal-wasm   ws-napi          front ends
       \           |    \       /
        \          |     \     /
         squeal-sql       sq-json             data models
             |               |
         sql-parser          |
             |               |
          macros             |
                \            /
                    store                     the engine
```

## Separation of responsibilities

**`store`** owns everything that must be right for data to survive:
- page format and checksums
- the page cache and eviction
- B+tree structure and concurrency
- transaction ids, snapshots and visibility
- write-write conflicts
- the WAL, checkpoints and crash recovery
- id sequences
- scratch space for queries (sort spills, hash-join tables)

Its unit is a `Tuple`: a key (`DBIdType`: an integer, or an `IndexKey` of
typed values), a byte payload, and the transaction metadata MVCC needs. It
has no idea what a column, a document or a query is.

**`squeal-sql`** owns what SQL means:
- the catalog (databases, schemas, tables, columns, indexes, foreign keys,
  partitions)
- how a row is encoded
- planning, optimization and execution
- statistics
- connection state (current schema, open transaction, temp tables)
- table-level locks that keep DDL from changing a table under a running
  statement

Each SQL table becomes store tables:
- one keyed by the primary key, holding the rows
- one per index, holding index keys that point back to the row's key

Each partition has its own.

**`sq-json`** owns what documents mean:
- extended JSON and BSON type ordering
- filters, updates and aggregation
- multikey index keys
- sessions

Each collection becomes a store table keyed by `_id`, plus one per index.

**`sql-parser`** owns only syntax. It produces an AST and knows nothing
about storage.

**The front ends** own only I/O: a terminal, a browser page, a Node
module.

The rule that keeps this honest: anything that needed an architectural
change went into `store` or the model layers, and the front ends were added
without one.

## Use of `Arc`

Shared ownership is how components hold each other without lifetimes
leaking into every API, and it is also a concurrency tool:

- **The database is always shared.** Every `Db` constructor returns
  `Arc<Db<F>>`. Cursors, schemas and collections each hold a clone, so a
  scan can outlive the call that started it. Inside `Db`, the page cache,
  WAL, transaction manager, generator and table map are each `Arc`'d, so
  background threads (maintenance, log writer) hold just what they need.
- **Copy-on-write pages.** A page's content is an `Arc<dyn PageTuple>`.
  - A reader clones the `Arc` and walks a stable snapshot of the page
    without holding its lock.
  - A writer calls `Arc::get_mut`, and copies the content only if some
    reader still holds the old snapshot.
  - Readers never block writers, and never see a half-written page.
- **Snapshots.** Each transaction's MVCC snapshot is an `Arc<Snapshot>`:
  - taken once at `begin`
  - shared by every scan in the transaction
  - read without a lock (the `active` flag is an atomic)
- **Cheap clones of immutable metadata.** A table's column list is
  `Arc<[Arc<Field>]>`, and table definitions are `Arc<SqlTable>`. Planning
  every statement clones them, which is a refcount bump, not a copy. Blob
  values are `Arc<[u8]>`.
- **Lifetime by refcount.** Scratch runs (sort spills, temp tables) free
  their pages when the last `Arc` holding them drops. That lets a cursor
  outlive the code that produced it without a borrow tying them together.
- **`Weak` where ownership would be a cycle.** The page cache keeps a
  `Weak` entry for an evicted page that someone still holds. The
  maintenance thread holds a `Weak` to its `Db`, so it never keeps a closed
  database alive.

## Building and testing

```bash
cargo build --release
cargo test --release                      # whole workspace, about 1,500 tests
cargo test -p store --release             # the engine alone
cargo run -p squeal-cli --release -- my.db    # SQL REPL
cargo run -p sq-json --release -- docs.db     # JSON shell
```

The SQL suite includes a differential "oracle" test. It generates random
schemas, data and queries, runs them on both squeal-db and SQLite, and
fails on any disagreement. The store has a crash harness that cuts power at
random moments under a concurrent workload and checks what recovery brings
back (see [store/README.md](store/README.md#testing)).

## How it was built

The architecture is mine: the blob-store-plus-layers design, bplus tree design 
and implementation (specifically using a crabbing approach to minimize locking),
the page and transaction models, and the parser approach. I designed and built the 
various components for the most part. In specific cases, I used Claude to 
complete some time consuming tasks (e.g. finishing up sql-parser after I 
got the initial design sketched and built). Across the board, 
Claude wrote all the tests, and debugged and fixed some hairy transactional issues. 
Also, my initial design had seprate redo and undo log files. I used Claude to clean that 
up and create one single WAL that is sequenced,

Some overall design and build goals:
1. 100% Rust
2. Minimal unsafe
3. Idiomatic rust.  Use iterators, closures and other functional concepts; minimize use of indexing
4. Multi-threaded safe
5. Separate concerns where possible
6. Support File and Memory for all operations out of the box
7. No unwraps. All errors are mapped and propogated, never hidden
8. Every commit is public - you can see exactly what I did and what Claude did
9. Extensive testing, unit and stress , all built by Claude

The project is open on GitHub and every commit is visible:

- **Commits with a `Co-Authored-By: Claude …` trailer** are ones where
  Claude wrote all the code and tests.
- **Commits without one** are ones where I did most of the work.
- **The SQL CLI, the WebAssembly and Node.js builds, and JSON document
  support** were built entirely by Claude. They needed no architectural
  changes.

## Other documents

- [TXN_MODEL.md](TXN_MODEL.md): the transaction model in under 100 words.
- `TXN_SIMPLIFICATION_PROPOSAL.md`, `TXN_SIMPLIFICATION_PLAN.md`: the
  current transaction design and how it got there.
- `ARCHITECTURE.md`, `STORE_AUDIT.md` and the other root design notes are a
  historical record. Some of what they describe has since been replaced
  (see the store README for the current state).
- [TODO.md](TODO.md): known gaps and ideas.

## License

MIT, see [LICENSE](LICENSE).
