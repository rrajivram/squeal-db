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
      Committed: `fcc6843`.
- [~] **Stage 2 — `Tuple` envelope — DEFERRED, folded into Stage 5.**
      Investigated giving `Tuple` its own per-record version tag as
      originally planned. Found a real blocker: `Tuple` is decoded in two
      incompatible contexts — (a) standalone from a raw byte slice
      (`slotted.rs`'s `decode_at`/`decode_id_at`), where a leading tag can
      be sniffed and dispatched on, and (b) as an element of `Vec<Tuple>`
      decoded via ONE bulk `postcard::from_bytes::<Vec<Tuple>>(bytes)` call
      — `AnyTuplePage::from_bytes` (`pages/anytuple.rs:57`), the **default,
      primary** on-disk page format (`SlottedPage` was tried and reverted
      for performance — see `page.rs`'s own comment). Inside a `Vec<T>`
      decode, each element goes through serde's generic
      `Deserializer`/`SeqAccess` machinery with no raw-byte access to sniff
      — doing this properly would mean rewriting `AnyTuplePage` (and
      checking `fixedtuple.rs`/`run.rs`) to decode tuple-by-tuple via
      `postcard::take_from_bytes` instead of one bulk call, just to support
      a per-record tag.
      **Decision (confirmed with the user)**: skip a per-`Tuple` tag
      entirely. Give the *page* itself a version tag instead (pulled
      forward from Stage 5) that declares which `Tuple` shape applies
      uniformly to every tuple it holds — no per-record ambiguity, no bulk-
      decode rewrite. `slotted.rs`'s `decode_id_at` fast-path change (skip a
      known-width prefix, then decode `DBIdType`) moves to Stage 5 too,
      keyed off the page's version instead of a per-tuple one.
- [~] **Stage 3 — `Header` versioning + `max_index_key_size` +
      `PAGE_OVERHEAD` runtime-derived — PARTLY DONE.**
      Done: `Header::decode` replaces the exact-match `format_version` gate
      with real dispatch (`magic`+`format_version` sit at a fixed byte
      offset in every version there's been, since postcard's derive
      serializes struct fields in declaration order regardless of Rust's
      in-memory layout — safe to peek before deciding how to decode the
      rest). `HeaderV3Shape` is a frozen copy of the pre-Stage-3 shape
      (including ITS OWN frozen checksum formula — the stored checksum was
      computed without `max_index_key_size` ever existing, so validating a
      v3 file against the *new* formula would spuriously report checksum
      corruption on every existing database). Added
      `max_index_key_size: DBSizeType` (`DEFAULT_MAX_INDEX_KEY_SIZE = 512`,
      range-validated `[64, 8192]`), and
      `Db::create_with_page_size_and_max_index_key_size` as the override
      entry point (`create`/`create_with_page_size` funnel through it with
      the default). Fixture tests: a v3-shaped header (real checksum, old
      formula) opens with the default key size; a v3 header with a bad
      checksum is still rejected; an unrecognized `format_version` is
      rejected. `cargo test --workspace` green.
      **Not yet done** (paused here to check in before the risky part):
      `PageBuffer` denormalizing a runtime `page_overhead: usize` (replacing
      the global `PAGE_OVERHEAD` const, threaded through ~20 call sites in
      `page.rs`/`buffer.rs`) — this is the part that actually resolves the
      `high_key` motivating bug — and the `CREATE TABLE`/`CREATE INDEX`
      DDL-time key-width rejection in squeal-sql.
- [ ] **Stage 4 — WAL (`LogHeader` + `LogRecord`/`Record`)**: real version
      dispatch instead of the exact-match gate; fixture is a full WAL
      segment replayed through `Db::open`'s recovery path.
- [ ] **Stage 5 — page shell (`PageHeader`/`PageDto`) + deferred Stage 2
      (`Tuple`)**: lower priority than it first appeared now that Stage 3
      resolves the urgent overhead crisis, but now also carries Stage 2's
      deferred work: the page's own version tag dictates which `Tuple`
      shape every tuple it holds decodes as (see Stage 2's note above),
      and `slotted.rs`'s `decode_id_at` fast path moves here too.
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
