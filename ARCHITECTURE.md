# squeal_db Architecture

This document describes how the `store` crate actually works today, for anyone building on top of it. It is written to be honest about gaps, dead code, and known-unfixed issues, not just the intended design — where the implementation diverges from what you'd expect, that's called out explicitly. For a detailed, dated history of bugs found and fixed (with root causes and reproductions), see `todo.txt` at the repo root; this document describes current state, not history.

## What this is

`store` is an embedded, single-process, transactional key-value storage engine written in Rust. Each table is one B+Tree keyed by a single (possibly composite) primary key. There is no query language, no secondary indexes, and no schema beyond "table name + primary key + opaque byte payload." Think of it as a hand-rolled SQLite-style storage layer, not a database server — everything runs in-process, and there is currently no client/server or replication story at all.

It is a **working, tested storage engine with real crash-recovery, best-effort snapshot isolation, and genuine `fsync`-backed durability at checkpoint/close/WAL-flush boundaries**, but it still has the rough edges of a project being actively hardened: a few dead/unused code paths, and at least one important sizing lesson learned the hard way in its WAL batching (see "Known limitations" below). Treat it as an actively-developed engine, not a finished one.

## Public API surface

Everything a consumer touches lives behind these public modules:

- `store::db` — `Db<F>` (generic over the storage backend `F`), `FileDB = Db<std::fs::File>`, the `DBFile`/`Opener` traits.
- `store::memfile` — `MemFile`, the in-memory backend.
- `store::txn` — `Transaction` (RAII guard), `TransactionId`.
- `store::tuple` — `Tuple`, `DBIdType` (the primary-key type).
- `store::cursor` — `Cursor` trait, `TableCursor`, `RangeCursor`.
- `store::table` — `TableIdType`.
- `store::valueitem` — `ValueItem`, `IndexKey` (composite-key building blocks).
- `store::error` — `StoreError`.

Core `Db<F>` operations, all through `&self`/`&Arc<Self>` (thread-safe): `create`/`create_with_page_size`/`create_with_limits`, `open`/`open_using`/`open_using_with_limits`, `close`, `checkpoint`, `create_table`, `begin`, `commit`, `rollback`, `insert`, `find`, `update`, `remove`, `table_scan`, `range_scan`, `page_count`, `delete` (removes the on-disk files by name).

## Storage backends

`Db<F>` is generic over `F: DBFile` (`Write + Read + Seek + Send + Sync + Opener`). Two backends exist:

- **`MemFile`** — an `Arc<RwLock<Vec<u8>>>`-backed in-memory "file." `MemFile::open()` always returns a fresh, empty buffer regardless of the path given — you cannot reopen a `MemFile`-backed `Db` by name; you must keep the file handles `close()` returns and pass them to `open_using` directly. Used by the test suite and the `stress`/`perf` examples for speed.
- **`std::fs::File`** — the real backend, opened via `Db::open`/`create` with `name`, `name.undo`, `name.redo` as three separate files.

Both implement positioned I/O (`pread`/`pwrite`) rather than seek-then-read/write. This is deliberate: earlier code shared OS file descriptors (via `try_clone`) across the main thread and the background writer thread, and `try_clone`'d descriptors share the OS-level seek cursor — a seek on one silently moved the read/write position for the other, corrupting page headers under concurrent access. Positioned I/O has no shared cursor at all, closing that class of bug for good (see `todo.txt` [6]).

## On-disk layout

- **Header** (page 0's logical slot, written directly at file offset 0): magic, `first_page_offset`, `page_count`, `page_size`, `last_checkpoint`. Only rewritten on `checkpoint()`/`close()` — see "Durability" below for why this matters.
- **Reserved system pages**, fixed page numbers: `SYSTEM_TABLE_PAGE = 0` (table list), `GENERATOR_TABLE_PAGE = 1` (id-generator sequences), `FREE_PAGE_TABLE_PAGE = 2` (free-page list). `FIRST_USER_PAGE = 3` is where user tables' pages begin.
- **Pages** are fixed-size (`page_size`, default 16 KiB, configurable per-`Db`). Each page has a small binary header (`PageHeader`: `next_page`, `page_data_size`, `page_used_size`, `record_size`, `lsn`, `flags`) followed by a data body.
- Two page body encodings, chosen per-page: **`AnyTuplePage`** (variable-size tuples, used for ordinary data pages) and **`FixedTuplePage`** (fixed-size records, used only for index pages — entries are capped at `MAX_ENTRY_BYTES = 64` bytes each).

## Tables, keys, and the B+Tree

Every table (`BPlusTree<F>`) has exactly one **index page chain** (the primary-key B+Tree — inner/leaf nodes, `FixedTuplePage`-encoded) and one **data page chain** (a singly-linked list of `AnyTuplePage`s holding the actual rows, walked via each page's `next_page`). An index leaf entry maps a key to the `PageId` of the data page holding that row (`Node::Leaf(PageId)`); `find_page` resolves a key by walking the index tree, then reads the row straight off that data page.

New rows are packed greedily: `write_data` fills the current tail data page until it's full (`Page::can_store`), then allocates a new page and links it in — it does not walk backward to reclaim space freed by earlier removes (see "Known limitations").

**Keys** are `DBIdType`: `Int(u64)`, `Vec(Vec<u8>)`, or `Rec(IndexKey)` for composite/multi-column keys (`IndexKey` wraps a `Vec<ValueItem>` — integers, doubles, strings, blobs, datetimes, or `Null`). `Int`/`Vec` order by a hash of their content (`DBIdType::hashed()`), not structurally — this matches the B+Tree's own iteration/storage order but means two *different* `Int`/`Vec` ids can compare `Equal` under `Ord` (a hash collision) while still being `!=` under `PartialEq`. `Rec` orders structurally (field-by-field), which is the one case range scans over multiple columns actually make sense.

**Table types**: `TableType::BtreeTable` is the only variant ever constructed. `TableType::Index` exists in the enum but nothing in the codebase ever builds one — there is no secondary-index feature. If you need to query by something other than the primary key, you build that yourself (e.g. a composite `Rec(IndexKey)` primary key, or a second table you maintain by hand).

**Overflow pages**: a single tuple too large to fit on an *empty* page (`Page::can_store`'s "empty page always accepts" exception) spills across a chain of overflow pages, up to `MAX_OVERFLOW_PAGES = 1024` (returns a generic `StoreError::UnknownError` if exceeded, not the seemingly-purpose-built `TupleTooLarge` variant, which is defined but never actually raised anywhere). This overflow mechanism is *only* valid for that single-oversized-tuple case; a multi-tuple data page must never be routed through it (a real bug where `update()` could push a fully-packed page over capacity and trigger this path was found and fixed this session — see `bplustree.rs::update`'s and `buffer.rs::handle_large_page_size`'s own comments for the full story, plus the regression test `db.rs::test_table_scan_correct_after_updating_every_row_across_multiple_data_pages`).

## The `IndexKey`/`ValueItem` reserved-capacity gap

`ValueItem::Str((String, u32))` and `ValueItem::Blob((Arc<[u8]>, u32))` carry a `u32` alongside the content — a **declared, reserved on-disk capacity** (`to_bytes` pads short content out to this length; presumably intended to support fixed-layout, in-place-updatable fields). Content exceeding its declared capacity is now rejected upfront: `IndexKey::new_from` calls `ValueItem::validate()` on every field and returns `Err(StoreError::TupleTooLarge(..))` instead of silently accepting it — `From<&[ValueItem]>` and `IndexKey::from_bytes` (which can't return `Result`) fall back to a `Null` key on validation failure rather than propagating it, so check `new_from` directly if you need the real error. Some other inconsistencies remain, though:

- `size()` is driven entirely by the *reserved* capacity, not the actual content length — since content is now validated to never exceed it, this no longer means `size()` can under-report, but it does mean `size()` always reflects the *reserved* budget, not how much of it the actual content uses.
- `PartialEq`/derived equality compares the *whole* `(String, u32)` tuple, including the reserved capacity — two values with identical content but different declared capacities are `!=`.
- `PartialOrd`/`Ord` (used for sorting and B+Tree key comparisons) compares **content only**, ignoring the capacity — so the same two values that are `!=` under `Eq` compare `Equal` under `Ord`.
- `hash()` also ignores the capacity.

Net effect: the reserved-capacity value is now an enforced per-field ceiling (good), but `Eq`/`Ord`/`hash` still disagree with each other about whether it's part of a value's identity — keep that in mind if you're using these fields inside a `HashSet`/sorted structure of your own.

**Sizing the index for your key shape is now an explicit, checked choice, not a hardcoded guess.** `BPlusTree::new` takes `index_entry_size` as a real parameter (the fixed per-entry byte budget for that table's index pages) instead of silently assuming `MAX_ENTRY_BYTES = 64` — call `Db::create_table_with_index_entry_size(name, size)` if your primary key is a composite `Rec(IndexKey)` with `Str`/`Blob` fields, computing `size` from your fields' own reserved capacities (`ValueItem::size()` per field, plus `IndexKey`'s 8-byte count prefix, plus `Tuple`'s own framing). `Db::create_table` is unchanged and still defaults to `MAX_ENTRY_BYTES`, which comfortably fits a plain `Int`/`Vec` key but not much else. `FixedTuplePage::add`/`replace` enforce this at the page level too, so an entry that's too big for whatever budget you did pick fails clearly with `TupleTooLarge(actual, budget)`, not a generic capacity error.

`ValueItem::Blob`'s `PartialOrd` also unconditionally `panic!`s ("Blobs cannot be compared") — a blob can be *part* of an `IndexKey` used for exact-match lookup, but never inside anything that gets ordered (a range scan, a sort). Comparing mismatched `ValueItem` variants (e.g. `Integer` vs `Str`) also panics.

## The write path (`PageBuffer`)

Each `Db` owns one `PageBuffer<F>`: an in-memory page cache (`HashMap<PageId, Weak/Strong<Page>>` with priority-queue-based eviction) plus one background writer thread. Most writes are **fire-and-forget**: `write_page`/`write_locked_page`/`write_header` enqueue a message on a bounded channel and return immediately — the caller has no guarantee the bytes have reached the file (or even been dequeued) by the time the call returns.

Two exceptions are synchronous:
- `PageBuffer::checkpoint()` sends a message and blocks on a reply, which drains everything queued *before* it (not anything queued after, since more can arrive while it's not the sole thing enqueuing). The writer thread's `Checkpoint` and `Shutdown` handlers now also call `file.do_sync()` after draining, right before replying/exiting — a real `fsync`/`fdatasync`, not just a `write()`. This is genuinely slower than before (checkpoint/close went from tens of microseconds to single-digit-to-tens of milliseconds on the `File` backend — see "Performance characteristics") but means a checkpoint or close is now actually durable across a power loss, not just a process crash.
- `write_page_header` (used by the overflow-page machinery) does a direct, synchronous `pwrite` — bypassing the async queue entirely. This was the direct cause of `update()` being disproportionately slow on the `File` backend before an earlier fix this session (each spuriously-triggered overflow allocation did several real synchronous disk writes; on `MemFile` the same code path is nearly free).

An `LsnClock` (one per `Db`, shared between `PageBuffer` and `Logger`) tracks two independent values: a monotonic `counter` (source of new redo LSNs) and a `last_written` watermark. `Page::set_dirty` stamps a dirtied page with the *current watermark value* (not the operation's own LSN) — the writer only flushes a page once the watermark has advanced past its stamped value, which is meant to guarantee write-ahead-logging order (a page's corresponding redo record is durable before the page itself is). On reopen, replay (`process_redo`) now seeds *both* the watermark and the counter from the highest LSN found in the prior session's redo log (a fix landed this session — previously only the watermark was seeded, and the counter restarting at 0 meant the very first new write after reopen could regress the watermark right back down, see `LsnClock::advance_counter_past`'s comment).

Backpressure: the writer thread caps how many not-yet-durable page writes it holds in memory (`max_pending_writes`, default 1024) — once hit, it stops draining its channel, and since the channel itself is bounded, callers block on `send()`. This is deliberate (an earlier version had unbounded memory growth — 13+ GB RSS observed for 2M rows before the fix, see `todo.txt` [14]).

## Concurrency model

Locking is **per-page**, via `ArcLock<PageId>` — a hand-rolled, reentrant-per-thread lock keyed by page id, backed by a `HashMap<PageId, ArcLockGuard>` where the guard's `Arc` strong-count *is* the lock state (count 1 = free). This means two transactions touching *different rows on the same page* still serialize against each other — locking granularity is the page, not the row.

Some things worth knowing if you're reasoning about throughput or latency tail:
- `ArcLock::lock()` takes a `timeout` parameter that is **completely ignored** — the actual wait is always hardcoded to 60 seconds, regardless of what's passed in. Waiting itself is a busy-poll loop (checks every 100µs), not a wake/notify primitive.
- Contended calls that can't acquire a lock in time surface as `StoreError::LockContentionError`. The codebase's own convention (and the one you should follow) is to retry with backoff — see `retry_on_contention` in `db.rs`, used throughout `insert`/`update`/`remove`'s internals and exported `pub(crate)` for reuse.
- There is exactly **one writer thread** per `Db` (draining `PageBuffer`'s write queue) and **one pair of WAL runner threads** per `Db` (redo/undo log writers, via `Logger`). All page and log writes funnel through these regardless of how many application threads are calling in. This remains a real bottleneck on the `File` backend specifically: multi-threaded insert scaling (private, non-contended key ranges per thread — see `examples/perf`) is essentially flat around 13-14k ops/s from 1 to 8 threads, dominated by real `fsync` cost in the WAL runners (see "Write-ahead log and crash recovery"). On `MemFile` (where `fsync` is a no-op) it scales more normally, ~75k ops/s at 1 thread down to ~41k at 8 (lock contention, not I/O, is the limiter there). If you need higher `File`-backend write throughput, sharding across multiple `Db` instances (each gets its own writer/WAL threads) will get you further than adding threads against one `Db`.
- **Fixed this session, but worth knowing the shape of the bug if you're auditing similar code:** `BPlusTree::insert_recursive`'s routing had a TOCTOU race — a non-root child's "is it full?" check released the child's lock before the caller actually descended into it, so a concurrent insert could fill that page in the gap and hit a dead end with no way to recover (previously a live panic — see `todo.txt` [16] for the original analysis). Fixed by having `split_if_needed` return the already-held lock (`SplitOutcome::NoSplitNeeded`) instead of dropping it, so the capacity check and the actual insert happen under one continuous hold. A related, adjacent gap in the same area — an inner-node-level `PageCapacityError` with no retry wired up anywhere (an abandoned, never-compiled draft in `insert_index`) — was fixed alongside it. Both confirmed via a targeted stress repro (small page size, interleaved keys across many threads): 19/200 and several/100 failures respectively before the fix, 0 after (see `db::tests::test_concurrent_inserts_at_small_page_size_do_not_panic_or_lose_rows`).

## Transactions and isolation

`db.begin()` returns a `Transaction` — an RAII guard that rolls back automatically on drop if never explicitly committed or rolled back. `TransactionId` is `Arc<TransactionInner { id: u64, ts: u128 }>`; equality and hashing intentionally check **both** fields, not just the numeric id. This is because the id-generator sequence is only persisted at `checkpoint()`/`close()`/table creation — a reopen after a crash (or a checkpoint with no subsequent close) can restore a stale sequence, letting a freshly-begun transaction be handed a numeric id an old, already-committed transaction also used. Comparing `ts` too means two genuinely different transactions never collide, even if their numeric ids do.

**Reads are snapshot-isolated on a best-effort basis** via `Db::find_visible_to` (used by `find`, `TableCursor`, `RangeCursor`): a version is visible to a reader only if its writer committed strictly *before* the reader began — a writer still active (or not yet begun) at the reader's `begin()` stays invisible for the reader's entire lifetime, even once it commits. This makes repeatable read hold for the common case, including across a concurrent commit landing mid-transaction, not just against a still-active concurrent write.

Getting there needed two parts working together, not just the visibility check:
- **The snapshot check itself** compares real wall-clock timestamps (`TransactionId::ts()`), not the numeric transaction id — the id generator's persisted sequence isn't guaranteed to have caught up to the true high-water mark right after a reopen (only refreshed at checkpoint/close/table-creation), so ordering by raw id across a reopen boundary can silently be wrong. `ts` has no such dependency: it's real time, so a transaction from a prior session is always chronologically before any transaction in a later one. The "snapshot at `begin()`" set itself stores full `TransactionId`s (not bare numeric ids), for the same cross-session-collision reason `TransactionInner`'s own `PartialEq` does.
- **Deferred undo discard** (`Logger::discard_or_defer_undo` / `drain_ready_undo_discards`), because the underlying undo log is a rollback log, not a retained multi-version store: a transaction's undo trail used to be discarded unconditionally the instant it committed, which meant the pre-image a still-open reader's snapshot needed could already be gone. This mirrors `TransactionManager`'s own `aborting`/`drain_aborting` pattern — `Db::commit` doesn't discard a transaction's undo trail if any other transaction is currently active (and might have it in its own snapshot); it parks the obligation, and `Db::begin`'s opportunistic drain (alongside `drain_aborting`) finishes the job once every such transaction has actually finished. Validated under sustained 16-thread load (always *some* transaction active) that this doesn't leak: each commit's waiter set is a fixed, finite snapshot that drains as those specific transactions finish, regardless of how many new ones begin afterward.

**Not full textbook snapshot isolation, still**: there's a narrow, intentionally-tolerated race where a brand-new reader can begin in the split-second between a commit's active-set snapshot and the commit actually taking effect, missing out on being counted as a waiter. If that exact race is hit, `find_visible_to` falls back to the latest committed version rather than the reader's own snapshot-consistent one — weaker than the reader's guarantee ought to be, but the one thing that must hold unconditionally (a row that exists and is committed is never reported as missing) still does. This fallback is now the rare path, not the common one.

Write-write conflicts are resolved by the page lock, not by row versioning or optimistic concurrency control — a writer blocks (up to the 60s `ArcLock` timeout, surfacing `LockContentionError` well before that under real contention) rather than detecting and rejecting a conflicting concurrent write after the fact.

Rollback is a **physical revert**: the undo log stores enough of each operation's pre-image to reconstruct the prior state (an `Add`'s undo removes the row; a `Mod`/`Del`'s undo restores the pre-image tuple), and `revert_txn_writes` replays it directly against the B+Tree, conditionally (`update_if_txn`/`remove_if_txn` check the row still belongs to the reverting transaction before touching it, so a concurrent forward write from someone else is never clobbered).

## Write-ahead log and crash recovery

Every `insert`/`update`/`remove`/`commit`/`rollback` writes both a redo record (what to reapply) and an undo record (what to revert) to two separate log files (`name.redo`, `name.undo`), sent to two dedicated writer threads (one per file) over bounded channels.

**Group commit with a linger window.** Each writer thread blocks for the first message, then lingers briefly (`LOG_BATCH_LINGER = 200µs`, stopping at the first timeout rather than always waiting the full window) hoping concurrent senders' messages join the same batch, up to `MAX_LOG_BATCH = 256` messages — then does one `write_all` for the whole batch and one `do_sync()` call. The channel itself is bounded to the same 256, which matters more than it sounds: **the batch cap must be at least as large as the channel capacity**, or a real backlog (which forms whenever `fsync` is slower than the producer — not a linger artifact, the messages are already queued) needs far more drain-and-fsync cycles to clear than necessary. This was measured directly: dropping the cap to 10 (while leaving the 256-slot channel unchanged) collapsed `File`-backend insert throughput from ~15k to ~600-1400 ops/s — roughly the same ~20x factor as the extra fsync cycles a too-small cap forces on an unchanged backlog. See `MAX_LOG_BATCH`'s own comment in `logger.rs` for the full numbers.

Sending to the channel still isn't a synchronous-write guarantee on its own (a just-sent message can still be mid-batch when a caller proceeds) — internal crash-simulation tests use an explicit `wait_for_durable_logs` poll helper rather than relying on channel timing.

`checkpoint()` flushes all pending pages synchronously, `fsync`s the main file, then truncates both log files (nothing before a checkpoint needs replaying, and the data it would have replayed is now actually durable). `close()` does the same (a clean close is, by definition, a point where everything is durable). Reopening (`open_using`) always replays whatever's left in the logs (`load_logs` → `process_redo` then `process_undo`), which is a no-op if you closed/checkpointed cleanly and a real replay if you didn't (simulating a crash). Replay is designed to be idempotent — `insert_if_needed`/`update_if_needed` check the row already reflects the write before reapplying — so re-running it against an already-consistent state is safe.

**`fsync` is wired in now**, at the two places durability actually needs it: each WAL batch (in the runner threads, right after `write_all`) and the main data file at checkpoint/close (in `PageBuffer`'s writer thread, after draining pending pages). The `Opener` trait's `do_sync` (a no-op for `MemFile`, `sync_data()`/`fdatasync` for `File`) is what gets called — it was correctly implemented all along, just never invoked anywhere; that gap is now closed. This has a real, measured cost: checkpoint/close latency on `File` went from tens of microseconds to single-digit-to-tens of milliseconds, and sustained `File`-backend write throughput is now dominated by `fsync` cost rather than CPU/lock overhead (see "Performance characteristics"). Trading throughput for actual crash-across-power-loss durability was the point — data checkpointed or cleanly closed is now recoverable even if the machine itself loses power, not just the process.

## Known limitations, honestly

A consolidated list, gathered from direct code inspection (not aspirational — these are all currently true). Several items that were open when this document was first written have since been fixed; this list reflects current state, and calls out what changed.

**Still open:**

- **No secondary indexes.** `TableType::Index` is declared, never constructed. The only index is the primary key.
- **No `drop_table`**, despite being listed as a planned operation in `lib.rs`'s own module-level doc comment. Tables can be created but never removed.
- **`IndexKey`/`ValueItem`'s reserved-capacity `u32`** is inconsistently enforced across `size()`/`Eq`/`Ord`/`hash()` and unvalidated against the 64-byte index-entry cap — see its own section above.
- **`ValueItem::Blob` can't be ordered** (`PartialOrd` panics), and comparing mismatched `ValueItem` variants panics in general.
- **Snapshot isolation has one narrow, intentional gap**: a reader beginning in the exact window between a commit's active-set snapshot and the commit taking effect can miss being counted as a waiter, and falls back to the latest committed value instead of its own snapshot-consistent one. See "Transactions and isolation" above — this is the one remaining piece of the original "dead scaffolding" gap, now deliberately narrowed rather than pervasive.
- **`ArcLock::lock()`'s timeout parameter is ignored** — always 60 seconds regardless of what's passed.
- **`File`-backend multi-threaded write throughput plateaus** around 13-14k ops/s regardless of thread count (1-8 tested) — now dominated by real `fsync` cost per WAL batch rather than lock contention. `MemFile` (where `fsync` is a no-op) scales more normally, down from ~75k ops/s at 1 thread to ~41k at 8, limited by lock contention as before.
- **The WAL batch cap must stay ≥ the channel capacity** (currently both 256) — see "Write-ahead log and crash recovery" for the ~20x regression measured when this invariant was violated. Not a limitation of correctness, but a real footgun if either constant is tuned independently later.
- **No space reclamation from removed rows** on the hot insert path — `write_data` only ever walks forward from its cached tail hint; a deleted row's page space isn't reused by later inserts (a deliberate, documented tradeoff from the `last_data_page` optimization — see `todo.txt` [13]).
- **`StoreError::TupleTooLarge` is defined but never raised** — an oversized tuple that exceeds the overflow-chain cap (`MAX_OVERFLOW_PAGES = 1024`, roughly `1024 × page_size` bytes) surfaces as a generic `UnknownError` instead.
- **`#![allow(dead_code)]` is set crate-wide** in `lib.rs`, which is part of why `TableType::Index` and `TupleTooLarge` (both above) exist without so much as a compiler warning flagging them as unused. If you're extending this codebase, don't assume "it compiles cleanly" means "everything defined is actually wired up."
- **`MemFile`-backed `Db`s cannot be reopened by name** — `MemFile::open()` always returns a fresh, empty buffer; you must reuse the exact file handles `close()` returned.

**Fixed since this document was first written** (kept here briefly so the history is visible, not silently dropped):

- ~~No `fsync` anywhere~~ — now wired into WAL batch flushes and checkpoint/close. See "Write-ahead log and crash recovery".
- ~~Snapshot isolation scaffolding disconnected~~ — `TransactionManager::snapshot()` is now consulted by `Db::find_visible_to`, backed by deferred undo discard. See "Transactions and isolation".
- ~~A confirmed, reachable panic in concurrent inserts~~ (`insert_recursive`'s "count == nodes- should not happen", plus an adjacent unhandled `PageCapacityError`) — both fixed. See "Concurrency model".

## Performance characteristics

From `examples/perf` (20,000 small rows at 64B, 2,000 large rows at 8KiB, default 16KiB page size — see that example's own `--help` for tunables). These reflect the current state: real `fsync` at checkpoint/close/WAL-batch boundaries, WAL group commit (256-message batch cap, 200µs linger), and deferred undo discard for snapshot isolation. Take these as rough orientation, not guarantees:

| operation | Mem | File |
|---|---|---|
| insert, sequential | ~50-75k ops/s | ~14-15k ops/s |
| find, random order | ~270-320k ops/s | ~30-32k ops/s |
| update, random order | ~39-51k ops/s | ~15-16k ops/s |
| remove, random subset | ~18-20k ops/s | ~15-16k ops/s |
| insert, large value (8KiB, overflow pages) | ~27-32k ops/s | ~600-1000 ops/s |
| table/range scan | tens of millions of rows/s | same |
| checkpoint | ~50-115µs | ~15-40ms |
| close | ~0.4-0.7ms | ~5-6ms |
| reopen (`open_using`) | ~13-15ms | ~17-19ms |
| multi-threaded insert scaling (1→8 threads) | ~75k → ~41k ops/s | ~14k → ~14k ops/s (flat) |

**The File-backend numbers dropped substantially from an earlier snapshot of this table** (insert/update/remove were previously ~15-47k ops/s across the board) — this is the direct, expected cost of wiring in real `fsync`, not a regression to chase down. `MemFile` (where `do_sync` is a no-op) mostly held steady or improved slightly, since it only picks up the batching side of the WAL changes, not the `fsync` cost. The one outlier worth flagging explicitly: File-backend large-value insert (which triggers overflow-page chains) dropped to ~600-1000 ops/s — each overflow page write still goes through `write_page_header`'s synchronous, unbatched `pwrite` (see "The write path" above), so an overflow-heavy insert now pays several small, unbatched, un-amortized costs per row rather than one batched WAL fsync; this path hasn't been revisited since the `fsync` wiring landed and is a reasonable next place to look if large-value write throughput matters to you.

Point lookups and scans remain fast on both backends (dominated by in-memory cache hits, not WAL I/O). Bulk sequential insert throughput is intentionally kept flat over time regardless of backend (O(1) amortized per insert via the `last_data_page` hint, not O(N) — see `todo.txt` [13]) — millions of rows sustain roughly the same rate as thousands, just at a lower absolute rate on `File` now that durability is real.

## Testing and tooling

- Unit/integration tests live inline (`#[cfg(test)] mod tests`) in nearly every source file — `cargo test -p store --lib`.
- `store/examples/stress` — concurrency/correctness stress harness (mixed insert/update/remove/find, randomized commit/rollback, latency histogram, mem+file backends). This is the tool that has found most of the concurrency bugs documented in `todo.txt`.
- `store/examples/perf` — the throughput/latency report harness referenced above (added this session).
- `store/examples/bulk_load` — single-threaded high-volume sequential/random load test, used to validate B+Tree depth and insert-throughput behavior at scale (millions of rows).
- `store/benches/page_store.rs` — a Criterion micro-benchmark (separate from the examples above).
- `todo.txt` (repo root) — a detailed, chronological record of every bug found and fixed in this codebase's development, including full root-cause writeups. Read it if you want the "why" behind design choices that might otherwise look arbitrary (e.g. positioned I/O instead of seek, the `last_data_page` hint, the `max_pending_writes` cap, the 60-second `ArcLock` timeout).
