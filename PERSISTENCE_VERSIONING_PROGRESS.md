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
- [x] **Stage 2 — `Tuple` envelope — folded into Stage 5 (done there).**
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
- [x] **Stage 3 — `Header` versioning + `max_index_key_size` +
      `PAGE_OVERHEAD` runtime-derived — DONE.**
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
      **`PAGE_OVERHEAD` rewrite — DONE.** Real scope turned out to be 77
      call sites (every `Page` constructor/decode call site, not just the
      ~20 direct `PAGE_OVERHEAD` references — most already had a `Header`/
      `PageBuffer` handle in scope, since that's where `page_size` itself
      came from, so `page_overhead()` piggybacks on the same handle rather
      than inventing new plumbing). `PAGE_OVERHEAD = size_of::<PageDto>()`
      is gone — confirmed genuinely broken by direct measurement (a 512-
      byte high_key serializes to 556 bytes vs. the old fixed 112-byte
      boundary). Replaced with `page::page_overhead(max_index_key_size)`
      (measured fixed-field cost + configured key cap + margin),
      denormalized onto `PageBuffer` like `page_size` already is.
      **Deliberate one-time breaking change** for any database with pages
      already on disk (confirmed and accepted — see page.rs's own comment):
      the header/data byte boundary moves, so old pages are a different
      physical layout, not just an old logical version. True non-breaking
      support for old page layouts needs page-level versioning (deferred
      to Stage 5, alongside Stage 2's deferred `Tuple` work).
      Regression test: `test_persistence_versioning_stage3_wide_composite_key_survives_split_and_reopen`
      inserts wide composite keys past the old 112-byte ceiling, forces a
      real split (the only thing that ever sets `high_key`), and verifies
      a close/reopen round-trip — the actual motivating bug, reproduced and
      proven fixed. `cargo test --workspace` and clippy both green.
      **DDL-time key-width rejection — DONE.** `Schema::check_key_width`
      (squeal-sql `schema.rs`) refuses, before anything is created, a
      `CREATE TABLE` whose PRIMARY KEY or any UNIQUE/PK index, or a
      `CREATE INDEX`, whose worst-case key (summed `DataType::size()` — a
      declared capacity is already an enforced ceiling on real data, via
      `IndexKey::new_from`'s `validate`) exceeds `Db::max_index_key_size()`.
      A non-unique index's key includes the appended row identity, so a wide
      PK counts against it. New `SqlIndex::key_size` (key only, no
      `ENTRY_OVERHEAD_BYTES`; `size` now builds on it). Tests:
      `schema_ops/schema/tests/key_width.rs` (6). `cargo test --workspace`
      green (402 squeal-sql + 525 store). **Stage 3 is now fully done.**
      Committed: `482e753` (parts 1-2: `8526f5e`, `5585bdf`).
- [x] **Stage 4 — WAL (`LogHeader` + `LogRecord`/`Record`) — DONE.** Design: the WAL's version lives
      once per SEGMENT (in `LogHeader`), not per record — it declares the
      shape of every record in that segment, including the embedded `Tuple`
      (same reasoning as Stage 2's "version the container, not the element").
      `Db::open` already never appends to a recovered segment (always starts a
      fresh one), so an old-version segment is only ever READ, and new writes
      are always the current version — no mixed-version segment can exist.
      Done: `read_and_validate_log_header` peeks the fixed magic+version
      prefix (6 bytes, same offset in every version), dispatches through
      `log_header_len(version)`, and returns `ValidatedLogHeader { version,
      bytes }`; `scan_log` now takes the segment's version explicitly (no
      caller can silently apply the current decoder to an old segment) and
      routes payloads through `decode_log_record(version, ..)`. Unrecognized
      versions are refused with a message naming the known range.
      `MIN_SUPPORTED_LOG_VERSION = 1` (user's decision: no cut-off recorded;
      old databases are simply recreated). Version 1 predates the framing
      rewrite and **never had a decoder**, so it is recognized but not
      readable: header and record decode return an explicit "recreate the
      database" error for it (`unreadable_v1_wal`), tested. If v1 support is
      ever wanted for real, the format must be reconstructed from git
      history first. `describe_wal` and a new `header_len_of` honor the
      file's own version (tools such as `wal_dump` keep working on old
      files); `header_len()` remains "current version".
      Live `LogHeader`/`LogRecord` are the v2 shapes today; the doc comment on
      `CURRENT_LOG_VERSION` says to freeze `...V2Shape` copies BEFORE changing
      them. Enforcement is mechanical: a permanent fixture — a real v2 segment
      (header + one record per `Operation` variant, both `DBIdType`s) pinned as
      hex in `logger.rs` — must decode with current code, plus page-size
      mismatch and torn-tail behavior on that fixture. 7 new logger tests +
      the reworked header tests. `cargo test --workspace` green (402
      squeal-sql + 532 store). Not done, by design: a full `Db::open` replay of
      the fixture — the replay path itself is unchanged and already covered
      by the existing crash-recovery tests; the new surface is the
      version-aware header/record decode, which the fixture tests directly.
- [x] **Stage 5 — page format version (`PageHeader`/`PageDto`) + the
      deferred Stage 2 (`Tuple`) — DONE. Committed: `28f0ec1`.**
      Design (per the Stage 2 decision): the PAGE is the unit of versioning.
      One page-format version fixes, uniformly for every tuple on the page,
      the header shape, the content-codec framing, AND the `Tuple` wire shape
      — no per-`Tuple` tag, no bulk-decode rewrite. Implemented as a trailing,
      fixed-width `format_version: u16` on `PageHeader`/`PageDto`
      (`CURRENT_PAGE_FORMAT_VERSION = 1`). It is the LAST field on purpose: a
      header is zero-padded to `page_overhead` on disk, so every page written
      before this stage (Stage 3 layout) reads back with those bytes as 0 =
      `LEGACY_PAGE_FORMAT_VERSION` — same layout, no migration, and a
      variable-length `high_key` ahead of it is unaffected (tested). Any
      future header field must likewise be appended, with 0 = absent/legacy.
      New/rewritten pages are stamped 1, so old pages upgrade lazily on their
      next flush. `PageHeader::check_format_version` (called from
      `read_page_header` in buffer.rs and `Page::from_bytes`) accepts 0 and 1
      and refuses anything else via `versioned::unsupported_version`; its doc
      comment says to freeze `...V1Shape` copies BEFORE changing any shape and
      then branch on the version. Fixtures: two real pre-change pages (an
      `AnyTuplePage` data page, and a `FixedTuplePage` index page with a
      real composite `high_key`), pinned as header-hex + data-hex (zero
      padding rebuilt in the test — a long zero run is too easy to mangle).
      Also: rewrite-upgrades-to-v1, unknown-version-refused, and
      `test_page_header_fixed_fields_fit_the_reserved_budget` (worst-case
      fixed fields = 62 bytes ≤ `FIXED_HEADER_BYTES` 64, so a header field
      that no longer fits fails loudly instead of eating the high_key
      reserve). `SlottedPage::decode_id_at`'s "id is `Tuple`'s first field"
      fast path is now documented as a promise of page versions 0 and 1 and
      cross-referenced both ways; nothing needed to change since no `Tuple`
      shape changed. `cargo test --workspace` green (402 squeal-sql + 538
      store), clippy clean on the touched files.
- [x] **Stage 6 — store-level system pages — DONE. Committed: `7a081fe`.** The table catalog (page 0), sequence state (page 1)
      and free-page list (page 2) were each exactly ONE pinned page with
      unversioned payloads; a catalog or free list that outgrew a page could
      not be persisted (STORE_AUDIT.md S6 just made `create_table` fail
      cleanly). Each is now a versioned, growable page CHAIN — new module
      `store/src/systempages.rs`. The head stays the fixed page 0/1/2;
      continuation pages come from the ordinary allocator and link through
      `next_page`. Every chain page starts with a reserved HEADER tuple (id
      `u64::MAX`, which no payload id can reach): `[u16 chain version][kind]
      [position u32]`, validated on read (kind/position mismatch, unknown
      version, missing header, or a cycle are all typed errors, tested).
      Payloads are per-entry and carry their OWN version tag (catalog: one
      `Table` per tuple; generator: one `(name, value)` per tuple; free list:
      chunks of 128 ids), so a payload shape can change without touching the
      chain layout. **Backward compat:** a head page with no header tuple is
      the pre-Stage-6 layout (single page, unversioned) and is read as such;
      the next checkpoint upgrades it. Verified with a test that writes the
      exact pre-change layout and reopens it, plus a pinned byte fixture for
      the `Table` payload shape (`0701740003040a`, captured from the encoder,
      not hand-written). **Growth:** `write_chain` allocates continuation
      pages as needed, re-serializing after each allocation because taking a
      page from the free list mutates the very list being written (catalog and
      generator are written first, free list last). **Chains never shrink**
      (unused continuation pages stay chained, header-only) — releasing them
      would mutate the free list mid-write; the cost is bounded by the
      historical peak (~0.1% of freed pages for the free list). Continuation
      pages are added to `reconcile_free_list`'s reachable set so a stale
      on-disk list can never hand one out. The old S6 test (create_table
      fails once the catalog page is full) is replaced: 150 tables at the
      4 KiB minimum page size now succeed, span a multi-page catalog, and
      survive reopen; likewise 1500 sequences and 4000 free pages.
      `BPlusTree::from_bytes` is now explicitly the legacy decode path;
      `from_table` is the new one. `cargo test --workspace` green (402
      squeal-sql + 548 store), clippy clean on touched files. **Observed and
      NOT caused by this stage:** `schema::tests::stats::
      test_analyze_table_resets_stale_stats_before_rebuilding` (in the
      untracked `stats.rs`) flakes ~2-3% — measured on the Stage 5 commit
      (3/120 failures) and on this tree (1/120): a race between the async
      stats collector and `analyze_table`.
- [x] **Stage 7 — squeal-sql catalog — DONE (uncommitted, pending the
      user's ok).** The bug class that started this effort. Two parts.
      **(1) The catalog row is a versioned envelope.** `SqlTable` (with
      `SchemaVersion`/`Field`/`SqlIndex`/`SqlForeignKey` nested) is stored as
      `[0x00][u16 LE version][postcard body]` (`SqlTable::encode_catalog_row`/
      `decode_catalog_row`, `CATALOG_ROW_VERSION = 1`). The leading 0x00 is
      the discriminator against pre-envelope rows, which are bare postcard
      starting with the name's length varint — 0 only for an empty name,
      which encode now refuses and no earlier build could create — so
      "first byte 0x00 = enveloped, anything else = legacy = version 1's
      body" is unambiguous by construction (tested). All four call sites
      (`create_table`, `flush_metadata`, `alter_table`, `load_tables`) go
      through it; unknown versions, truncated envelopes and empty rows are
      typed errors, not panics. The v1 body is the live derived shape, pinned
      by a 340-byte fixture of a rich table (composite PK, UNIQUE, FK,
      defaults of four types, two ALTER ADD COLUMNs) whose bytes were
      **verified identical to the untouched Stage-6 HEAD encoder** (I
      regenerated it in a scratch worktree, since the first capture happened
      after part 2 below). A schema-level test writes a legacy row into a live
      system table, loads it, flushes (which upgrades the row to the
      envelope), and loads again.
      **(2) `ValueItem` and `IndexKey` serde are hand-rolled**, byte-identical
      to the old derive (explicit tags Null0 Integer1 Double2 Datetime3 Str4
      Blob5 Boolean6, pinned from pre-change bytes). This closes the
      "derive keys a variant by declaration index" hazard for the three
      places `ValueItem` reaches disk via serde: `Field::default` in every
      catalog row, every index-leaf payload (an `IndexKey`), and every
      composite `Tuple` id. `IndexKey`'s decode deliberately does not
      re-validate capacity (tested) — it reproduces what was written.
      **Deliberately NOT done, and why:** (a) the index-leaf `IndexKey`
      payload (`schema.rs` `to_allocvec(&identity)`) was not moved to the
      other hand-rolled `to_bytes` codec as the original plan suggested: those
      payloads carry no discriminator, so a dual-read would need a sniffing
      heuristic; instead their bytes are now explicitly stable via (2).
      (b) `VersionedRow` (the user-row payload) still has no envelope version
      of its own — its first field is the schema-version index, so nothing in
      it can serve as a discriminator. It is hand-rolled and explicit already;
      a future change to that envelope needs its own strategy first (e.g. a
      page-format bump). Flagged for the user rather than half-fixed.
      **End to end (the plan's post-Stage-7 check):** rebuilt the CLI,
      loaded the whole retail dataset (10/1000/100/10000/30271 rows, 0
      failures, PK/FKs intact) into a scratch db and reopened it 6 times, all
      counts identical. `cargo test --workspace` green (409 squeal-sql + 552
      store); clippy shows nothing in touched code.
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
