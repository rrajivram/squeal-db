# System-wide forward-compatible persistence — progress tracker

Plain markdown, not `bd`, per standing preference for this repo. Tracks the
staged rollout described in this session's plan (system-wide versioned
persistence, motivated by the overflow-page decode bug fixed in `d58a756`
and the `PAGE_OVERHEAD`/`high_key` audit that followed it).

Convention per stage: implement -> fixture test proves the new decoder reads
bytes from before the change -> `cargo test --workspace` green -> ask before
committing (each stage is its own commit).

## Stages

- [x] **Stage 0 — shared infrastructure**: `store/src/versioned.rs`
      (`write_versioned`/`read_version_tag`/`unsupported_version`). No
      consumers yet (stages 3+ use it for real top-level envelopes). Unit
      tests only.
- [x] **Stage 1 — hand-rolled enums**: `TableType` (table.rs), `Node`
      (bplustree.rs), `DBIdType` (tuple.rs), `Operation` (logger.rs),
      `DataType` (squeal-sql/datatype.rs) all converted from derived
      `Serialize`/`Deserialize` (wire tag = declaration index, fragile) to
      hand-rolled codecs with explicit, fixed `u8` tags matching each
      variant's *current* declaration index (required for the fixture-decode
      proof to hold — nothing on disk has ever seen anything but today's
      order). Each has: round-trip test, unknown-tag error test, and a
      fixture-decode test using bytes captured from the pre-Stage-1 derived
      encoding (commit `bfbc240`). `cargo test --workspace` green.
      Not yet committed — pending go-ahead.
- [ ] **Stage 2 — `Tuple` envelope**: version-tag `Tuple` itself, preserving
      `slotted.rs`'s `decode_id_at` byte-prefix fast path (skip the 2-byte
      tag, then decode `DBIdType`).
- [ ] **Stage 3 — `Header` versioning + `max_index_key_size` +
      `PAGE_OVERHEAD` runtime-derived**: replaces the exact-match
      `format_version` gate with real dispatch; adds
      `max_index_key_size: DBSizeType` (default 512, validated range);
      `PageBuffer` gets a denormalized `page_overhead: usize` computed at
      open instead of the global `PAGE_OVERHEAD` const; `CREATE
      TABLE`/`CREATE INDEX` reject a too-wide key up front. This is the
      stage that actually resolves the `high_key` motivating bug.
- [ ] **Stage 4 — WAL (`LogHeader` + `LogRecord`/`Record`)**: real version
      dispatch instead of the exact-match gate; fixture is a full WAL
      segment replayed through `Db::open`'s recovery path.
- [ ] **Stage 5 — page shell (`PageHeader`/`PageDto`)**: lower priority now
      that Stage 3 resolves the urgent overhead crisis; still needed so the
      page shell itself can grow safely later.
- [ ] **Stage 6 — store-level system pages**: table catalog (page 0),
      generator state (page 1), free-page list (page 2).
- [ ] **Stage 7 — squeal-sql catalog** (`SqlTable`/`SchemaVersion`/
      `SqlIndex`/`SqlForeignKey`/`Field`), modeled on `VersionedRow`'s
      existing pattern. Also unifies `Field::default`/index-leaf `IndexKey`
      payload onto the same hand-rolled codec `VersionedRow` already uses.
      **This is the stage that fixes the bug class that started this
      effort.**
- [ ] **Stage 8 — schema registry + stats exception**: schema registry
      (trivial, it's a `String`); `TableStatStored`/`PersistedTableStat`/
      `PersistedColumnStat` get a *deliberate* exception — decode failure
      (including bloom-filter format failure) is caught and treated as "no
      stats yet," never propagated as a hard error.

## After Stage 7

Re-run this session's original repro end-to-end: recreate
`test_data/squeal.db` from `test_data/retail_data`, reopen it 5+ times.

## Explicitly out of scope

- Sort/hash-join spill pages, temp-table pages — confirmed transient, never
  re-read after a restart.
- `history.txt` — plain text, not engine state.
- Moving `high_key` into the page's content area — reopens the atomicity
  race `PageInner` already consolidated `has_overflow`/`next_page` to fix.
