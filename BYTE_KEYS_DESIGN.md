# Byte-comparable keys

Started 2026-10-11. The plan of record for changing how the store holds
keys, and why.

## The larger problem

squeal-db allocates too much and moves too many bytes. The memory audit
(MEMORY_ISSUES.md) found the same thing on every path:
- an insert makes about 10 allocations and builds its row's bytes three
  times;
- a query builds an owned row in every operator and clones a value in
  every expression;
- about half the footprint at the peaks is allocator fragmentation, which
  that churn produces.

The cause is one habit: bytes are turned into owned values at each step,
and back. The fix is to keep bytes as bytes:
1. encode a row or a key once, at the edge;
2. compare, search and sort the bytes in place;
3. copy bytes (into an arena) only where something must be kept;
4. decode a value only where a value is needed.

Keys come first, because everything that orders data depends on comparing
them: page search, range scans, sorts, merges and index builds. Today a
key on a page is a postcard-encoded list of values. Comparing two means
walking both value by value, and each decoded page keeps a 16-byte prefix
and a 4-byte offset per slot to make that bearable.

## The encoding

`store/src/keycode.rs`, whose header has the byte layout. In short:
- **A key's bytes compare, as plain bytes, in the key's order.**
- **A record key is its fields one after another, each self-delimiting.**
  There is no count in front and no terminator behind. So the keys that
  begin with a key `p`'s fields are exactly the bytes from `p` up to `p`
  then `FF` (`has_prefix`, `prefix_end`).
- **Each field starts with a tag that rises with ValueItem's type rank:**
  Null, Boolean, Integer, Double, Datetime, Str, Blob.
- **Integers and datetimes are variable-length and still ordered.** The tag
  carries the sign and byte count. A value under 256 takes 2 bytes, no
  more than the varint it replaces.
- **Doubles take 8 bytes, sign-flipped.** That is the order
  `f64::total_cmp` gives, which is the order keys already had: `-0.0`
  below `+0.0`, NaNs at the ends.
- **Strings and blobs are their bytes with `00` written `00 FF`, then
  `00`.** The escaping pass can be vectorized (fearless_simd) later.
- **A row id has its own tags, above every record key's.**

Its tests check the byte order against `DBIdType`'s own for every pair in
a pool of extremes, in keys of one and two fields.

### Three deliberate differences from today's order

| | today | bytes |
|---|---|---|
| a key that is a prefix of another | compares Equal | sorts before it |
| a record key against a row id | by a 32-bit hash | every record key below every row id |
| a string's reserved capacity | stored in the key, ignored by comparison | not in the key; one decoded has a capacity of its length |

- **The prefix tie:** scans don't rely on it (they pad with NULLs or
  compare field by field), and a tree's keys are all one length.
- **Record key against row id:** only `Int(u64::MAX)`, the last entry of
  an inner page, relies on the order, and it stays above everything.
- **Capacity:** nothing above the store compares on it.

## Stages

Each stage is its own commit. A stage that changes what is on disk follows
the persistence convention (PERSISTENCE_VERSIONING_PROGRESS.md):
- a fixture test proves the old bytes still read;
- the workspace tests are green;
- it is brought for approval before it is committed.

- [x] **1. The encoding** (`keycode.rs`). Nothing uses it yet, and nothing
  on disk changes.
- [ ] **2. Pages hold keys as these bytes.**
  - A tree created from now on stores each tuple's key as its `keycode`
    bytes. The tree says so in its catalog entry, and its pages in their
    content kind. A tree is all one format, because high keys and
    separators cross pages.
  - Comparisons inside the store route through one seam:
    - `IdRef::cmp` / `eq`
    - `Ord for DBIdType`
    - `order_prefix_of` / `prefix_order`
    - `wire::Reader`'s id functions

    For a new tree these are byte comparisons. The per-slot prefix becomes
    the key's first 16 bytes, with no decoding.
  - Trees written before keep their format and their code path.
  - The WAL keeps carrying keys as values: a record is replayed into
    whatever format its tree has.
  - Checked by the store's tests run against both formats, the crash
    soaks, and the SQLite comparison tests.
- [ ] **3. Scans on bytes.** A `KeyRange` becomes a pair of byte bounds
  (`prefix_end` gives the upper one). A cursor compares bytes, and stops
  decoding each key to test it.
- [ ] **4. Sorting on bytes.** `CREATE INDEX` writes each entry's key bytes
  once into one buffer, sorts positions by comparing bytes, and hands the
  bytes to the bulk loader. No row of values per entry, and no second
  sorter: the query sort moves to the same representation.
- [ ] **5. Drop the per-slot arrays** (MEMORY_ISSUES.md section 3). With
  byte keys a page is searched on its own bytes. The 16-byte prefix and
  4-byte offset per slot go, or shrink to what a cache line needs.
- [ ] **6. The same for rows:** operators pass views of packed rows on
  their pages, and keep rows by copying bytes into a per-query arena.
  Tracked in MEMORY_ISSUES.md.
