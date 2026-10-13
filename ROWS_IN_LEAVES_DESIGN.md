# Rows in the leaves

Started 2026-10-13. **Approved 2026-10-13**, with the three decisions at the
end as recommended. It changes what is on disk. It is MEMORY_ISSUES.md section 1, step 5 ("keep entries in the
leaf"), worked out.

## The problem

Every store tree (a table's rows, and each secondary index) is two
structures:
- **an index tree** whose leaf entries are `key -> Node::Leaf(data page)`;
- **a chain of data pages**, in insert order, holding the whole tuples:
  key, transaction id, pre-image LSN, flags and row.

So:
- **Every key is stored twice**, once in its leaf entry and once in its
  tuple: about 21 bytes a row, and half of every secondary index.
- **Every lookup searches twice.** It routes to the leaf and searches it
  for the key; the entry names only a page, so it then searches that data
  page for the key again (`locate_index_entry`, `find_hinted_ref`). In a
  point lookup that second search, plus fetching the page, is about 0.2 µs
  of 1.7.
- **Every range scan probes a data page per row** (TODO.md: ~90 ns a row).
- **Data pages never split and their space is never reused.** A row that
  grows past its page moves to the tail (`relocate`), leaving a hole that
  stays. Pages emptied by deletes are never freed. That is allocator-style
  fragmentation on disk.
- **A read of a row is two reads that aren't atomic**: the leaf, then the
  data page. `relocation_lock` exists only to keep them consistent.
- **Every insert writes two pages**, the data page and the leaf.

SQLite, redb and most B+tree stores keep the row in the leaf, which is the
same "move fewer bytes" point the rest of this work is about.

## What makes the change contained

Nothing outside a tree knows where a row lives:
- **MVCC** is keyed by row key and LSN: pre-images, undo, tombstones and
  `visible_version` hold no page ids.
- **The WAL** is keyed by row key. Its records carry a `data_page` field,
  but every production write passes `None`; replay goes by key.
- **Secondary indexes** hold the row's primary key (or row id), not a page.
- **The catalog** holds each tree's root and first data page. Only full
  scans, dropping a table and free-list reconciliation walk the data
  chain.

So the change is inside `BPlusTree`, its cursors, and the page format. The
SQL layer is untouched.

## The design

### A tree whose leaves hold its tuples

- **A new tree form**, marked in the catalog (a new `TableType` tag; tags 10
  and up are free) and on each of its leaf pages (a new page flag, so a
  page says what it is on its own).
- **Its leaf pages hold the tuples themselves**, sorted by key, in the
  slotted page format every page already uses. There are no data pages and
  no `Node::Leaf` entries. Inner pages are unchanged: separator keys and
  child pointers.
- **Leaves split and fill by bytes.** Today a leaf entry has a fixed byte
  budget (`record_size`); here a leaf is full when the next tuple doesn't
  fit, as data pages are. A leaf must hold at least four tuples
  (`MIN_ENTRIES_PER_PAGE`), which sets the largest tuple kept inline.
- **Tuples too large to keep inline** keep their row in an overflow chain,
  with the tuple in the leaf holding its key, header and the chain's first
  page. (Overflow already exists, for a lone large tuple on a data page.)
- **Updates happen in place in the leaf.** A tuple that no longer fits
  splits its leaf, as an insert would. Nothing relocates, so
  `relocation_lock` goes for this form.
- **Deletes and purges free their space in the leaf**, which the slotted
  page reuses. A leaf emptied by purge is unlinked and freed (data pages
  never were).

### Reading

- **Point lookup:** route to the leaf, then one search. The tuple is
  there.
- **Range and key-range scans:** walk the leaves, with no per-row probe.
- **Full scan:** walk the leaves too, now in key order rather than insert
  order. SQL promises no order without `ORDER BY`, and the planner already
  knows a table seek gives key order.
- **MVCC is unchanged**: `visible_version` reads the tuple's `txn_id` and
  `pre_lsn` as before, wherever the tuple is.

### Writing

- **Insert:** one leaf, under the leaf lock `write_version` already takes.
  One page dirtied, not two.
- **The WAL is unchanged**: records are by key. A redo inserts into the
  leaf by key.
- **Bulk load** (`CREATE INDEX`, `COPY`) builds full leaves bottom-up,
  which is simpler than now: no data pages to fill alongside.
- **The write protocol's lock order** (table guard, index top-down, leaf,
  one data page) loses its last step.

### Old databases

- **Trees already on disk keep their form**, read and written as now. Both
  forms live side by side, told apart by the catalog tag, so there is no
  migration at open and nothing to convert.
- **New trees are created in the new form**: new tables, new indexes, and
  the trees `CREATE INDEX` and `ALTER TABLE` rebuild.
- **An explicit rebuild** (for example `VACUUM <table>`) can move an old
  table to the new form later. It is not part of this change.

### What it should give

| | now | expected |
|---|---|---|
| point lookup | route + leaf search + data-page fetch and search | route + leaf search: about 0.2 µs less |
| range scan | a data-page probe per row (~90 ns) | none |
| insert | two pages written | one |
| row tree size | ~21 B a row for the key a second time | gone |
| secondary index size | each entry stored twice | once: about half |
| space from updates and deletes | never reused; empty pages never freed | reused in the leaf; empty leaves freed |

## Persistence

It follows PERSISTENCE_VERSIONING_PROGRESS.md:
- the new `TableType` tag and leaf-page flag, each new and unambiguous;
- frozen copies of the current shapes, and fixtures proving pages, catalogs
  and WALs written before the change still open and read;
- the workspace tests, the crash soaks, and the SQLite comparison tests.

## Stages

Each stage its own commit, measured.

1. [x] **The format.** Done 2026-10-13.
   - `TableType::RowsInLeaves`, catalog tag 10 (a fixture pins it; older
     builds refuse an unknown tag at open, rather than read its leaves as
     data-page pointers).
   - Leaf pages of such a tree carry a new flag, `LEAF_ROWS` (bit 6), and a
     per-entry budget of their own (`record_size`, already in every page
     header). Inner pages keep the routing budget; a root that stops being
     a leaf takes it then (`Page::set_record_size`).
   - The tree keeps one empty data page, so its catalog entry, and what
     walks a table's pages (drop, free-list reconciliation), are the same
     for both forms.
2. [x] **Secondary indexes in the new form.** Done 2026-10-13.
   - The store: `Db::create_table_with_rows_in_leaves`. A write reads and
     changes the tuple in its one locked leaf (`write_in_leaf`); lookups
     search the leaf; range scans lend the leaf entry as the row; full
     scans walk the leaves, in key order; bulk loads fill leaves with the
     tuples. A tuple past the leaf budget is refused before its write is
     logged (`check_fits`).
   - squeal-sql creates each secondary index this way, budgeted for its
     widest entry (key, row identity, header). An index whose entries could
     pass an eighth of a page keeps the old form.
   - **Checked by:** targeted store tests (writes, rollback, bulk load,
     reopen and power cut, concurrent writers and readers); the store suite
     run with every tree in the new form (`SQ_ROWS_IN_LEAVES=1`: all pass
     but tests of data-page chains and of rows past a leaf's budget, which
     stage 3's overflow covers); the workspace suite, 20,000 SQLite oracle
     queries, a 60-second crash soak.
   - **Measured**, against the build before, back to back:

     | | before | after |
     |---|---:|---:|
     | index range, per matching row (probe, 500k) | ~690 ns | ~620 ns |
     | secondary-index lookup (probe) | ~15 µs | ~13 µs |
     | `CREATE INDEX`, per index (bench, 200k) | 160 ms | 122 ms |
     | memory after the load / peak (bench, 200k) | 105 / 177 MiB | 83 / 155 MiB |

     An index read still fetches each row from the table's tree, which
     keeps data pages until stage 3.
3. [x] **Row trees in the new form, for rows that fit inline.** Done
   2026-10-13.
   - squeal-sql creates a table's row tree this way when the widest row its
     schema allows (`SqlTable::row_tuple_size`) fits a leaf's inline budget,
     an eighth of a page. A wider table keeps data pages until overflow
     (3b).
   - **Room in a leaf.** A leaf budget is now only a cap on one tuple's
     size, not room a leaf keeps free:
     - a write that knows its tuple (insert, update, a rollback's restore,
       recovery's redo) asks the descent for room for that tuple, so
       inserts fill leaves;
     - one that doesn't (a delete's tombstone) only ever rewrites the tuple
       already there: it reads that tuple's size under the leaf's lock and,
       if the leaf lacks room for it, descends again asking for it, which
       splits the leaf. Nothing is logged before that, so the retry is free.
   - **Checked by:** the workspace suite; 20,000 SQLite oracle queries; a
     two-minute SQL crash soak; 200 seeds of the store's crash harness with
     every tree in the new form.
   - **Measured**, 500k orders, against the build before, back to back:

     | | before | after |
     |---|---:|---:|
     | point lookup, prepared | 1.72 µs | 1.25 µs |
     | point lookup, SQL text | ~1.9 µs | ~1.65 µs |
     | index range, per matching row | ~750 ns | ~505 ns |
     | secondary-index lookup | ~13 µs | ~10.9 µs |
     | bulk insert, per row | 2.5 µs, 44 allocations | 2.1 µs, 38 |
     | scans, `GROUP BY`, sort, join | | unchanged |

   - **Not yet:** a row past the inline budget (a table widened past it by
     `ALTER TABLE`, which keeps its tree) is refused (`TupleTooLarge`)
     until overflow.
3b. [ ] **Overflow:** a row past the inline budget keeps its bytes on an
   overflow chain, its leaf tuple holding the chain's first page. The
   chain's pages are freed only once no snapshot can still be reading the
   version that used them. Then every table can take the new form.
4. [ ] **Old tables rebuilt on request** (optional, later).

## Decisions for the author

1. **Approve the on-disk change**, in the stages above.
2. **Old tables keep the old form until rebuilt**, rather than being
   converted when a database opens. Recommended: no migration at open.
3. **The largest row kept inline**: what four tuples to a page allows
   (about 4 KiB on the default 16 KiB page; 1 KiB on the smallest, 4 KiB,
   pages), larger rows going to overflow.
   - **Revised in stage 2: an eighth of a page (about 2 KiB on 16 KiB).** A
     leaf splits when it has less room than its budget, so a quarter-page
     budget left leaves up to a quarter empty; an eighth costs at most an
     eighth. A tree whose rows are narrower is budgeted by their real
     width, and its leaves fill.
