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
- [x] **2a. One order for keys, everywhere.** `DBIdType`'s `Ord` (and
  `IdRef`'s) is now the order the keycode bytes have:
  - two row ids by value;
  - two record keys field by field, a prefix before its extensions;
  - a record key below a row id.

  Nothing on disk changes, and no real tree's order does: same-length
  record keys and row ids compared this way already. The hash is no
  longer part of ordering. The keycode tests check bytes against this
  order for every kind of pair, and that equal bytes are equal keys.
- [x] **2b. Tuples on pages carry their key as these bytes.** On-disk
  change. Done 2026-10-11, approved 2026-10-12.
  - **Measured against 2a.**

    | | before | after |
    |---|---:|---:|
    | store: point lookup, composite key | 1.09 µs | 0.60 µs |
    | store: delete, per row | 6.6 µs | 5.4 µs |
    | store: range scan, per row | 85 ns | 76 ns |
    | SQL, 1M orders: secondary-index lookup | 53 µs | 47 µs |
    | SQL: `UPDATE`, 100 a transaction, per row | 136 µs | 123 µs |
    | SQL: `CREATE INDEX`, per index | 812 ms | 743 ms |
    | SQL: bulk load, per row | 2.35 µs | 2.27 µs |
    | file, 1M orders | 216 MB | 213 MB |

  - **Encoding once.** The first version re-encoded each key several
    times through a buffer and serde, and made SQL writes 7% slower. A
    tuple is now written in one pass, its key's bytes going straight in
    (`Tuple::encode_into`), and its size is counted from its fields.
  - **The form.** A tuple's id starts with a tag: 0 is a row id, 1 a record
    key as postcard values. Tag 2 is new: a length, then the record key's
    keycode bytes. Every record key written from now on uses it, in every
    tree, with two exceptions:
    - **Row ids keep tag 0.** They already compare without decoding, and
      the new form would cost two bytes more a row.
    - **A key holding a 00 byte in a string or blob keeps tag 1 for now.**
      Tag-2 keys then hold no escapes, so their strings are lent from the
      page as they are. Such keys stay correct, on the slower path.
  - **No tree-wide switch.** Both forms have the same order (2a), so they
    can share a page. That means no catalog flag, no new page kind, and no
    migration: an old database's tuples take the new form as they are
    rewritten.
  - **Comparing.**
    - Two tag-2 keys compare as bytes.
    - An old-form key against anything compares by decoding, as now.
    - The per-slot prefix is the first 16 bytes of a key's keycode bytes,
      for both forms: different prefixes decide, equal ones fall to the
      full compare.
    - A search encodes its probe once.
  - **Size.** `Tuple::size()` measures the page's encoding, since pages
    budget by it.
  - **Scans.** A `KeyRange` is turned into byte bounds once (`prefix_end`
    gives the end of "begins with these fields"). A cursor tests tag-2
    keys against them as bytes, and old-form keys field by field as now.
    Without this, every scanned key would be decoded to test it.
  - **The WAL** keeps carrying keys as values: a record is replayed into
    whatever a page holds.
  - **Checked by:**
    - a fixture of a page of old-form tuples that still reads, searches
      and takes new-form tuples beside them;
    - the store's tests;
    - the crash soaks;
    - the SQLite comparison tests.
- [ ] **3. Nothing above the store decodes a key to use it.**
  - squeal-sql's index-only scan and identity reads take key fields
    straight from the bytes.
  - Seeks and bounds are built as bytes.
- [x] **4. Sorting on bytes** — for `CREATE INDEX`, done 2026-10-14.
  - `Schema::build_index_stored`: one pass over the rows, decoding only
    the index's columns into a reused buffer; each entry's key written once
    as stored keycode bytes into one buffer (a stored identity key copied
    as it is), its data after it; positions sorted by comparing key bytes;
    and `Db::bulk_load_stored` writing each tuple's encoding straight into
    the leaves. No row of values, key as values, or `Tuple` per entry.
  - It needs a tree with its rows in its leaves, keys with no 00 byte in a
    string, and entries within the query's memory; otherwise the general
    sort (which spills) is used.
  - Index build on 500k orders: 483 -> 139 ms.
  - Still open: the query sort on the same representation (part of
    BATCH_EXECUTION_DESIGN.md stage 5).
- [ ] **5. Drop the per-slot arrays** (MEMORY_ISSUES.md section 3). With
  byte keys a page is searched on its own bytes. The 16-byte prefix and
  4-byte offset per slot go, or shrink to what a cache line needs.
- [ ] **6. The same for rows:** operators pass views of packed rows on
  their pages, and keep rows by copying bytes into a per-query arena.
  Tracked in MEMORY_ISSUES.md.
  - **First step done 2026-10-12.** Sources may lend rows, and scan,
    filter, projection, aggregate and GROUP BY do. A scan, filter or group
    now allocates nothing per row.
  - The lent row is still values (a reused buffer), not a view of the
    page's bytes. That is the next step, along with the joins and sort.
