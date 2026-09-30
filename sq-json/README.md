# sq-json

A MongoDB-style document database on `store`, with a Rust API.

```rust
use sq_json::{Client, Document, FindOptions, IndexOptions};

let client = Client::<std::fs::File>::create("docs.db")?;   // or Client::open
let orders = client.database("shop").collection("orders");
let d = |j: &str| Document::parse(j).unwrap();

orders.insert_one(d(r#"{"sku": "pen", "qty": 5, "tags": ["red"]}"#))?;
orders.create_index(d(r#"{"sku": 1}"#), IndexOptions { unique: true, name: None })?;
orders.update_one(d(r#"{"sku": "pen"}"#), d(r#"{"$inc": {"qty": 1}}"#), false)?;
let big = orders.find(d(r#"{"qty": {"$gt": 3}}"#), FindOptions::new().sort(d(r#"{"qty": -1}"#)).limit(10))?;
println!("{}", orders.explain(d(r#"{"sku": "pen"}"#))?.to_json()); // {"stage":"IXSCAN",...}

let session = client.start_session();
session.start_transaction()?;
orders.with_session(&session).delete_many(d(r#"{"qty": 0}"#))?;
session.commit_transaction()?;
```

## What's there

- **Documents** keep their field order. They read and write MongoDB extended
  JSON (`$oid`, `$date`, `$numberLong`, and similar). Values compare in BSON
  type order.
- **CRUD**: `insert_one/many`, `find`, `find_one`, `count_documents`,
  `distinct`, `update_one/many`, `replace_one` (all with upsert),
  `delete_one/many`.
- **Filters**:
  - comparison: `$eq $ne $gt $gte $lt $lte`, type-bracketed as in MongoDB;
  - sets: `$in $nin`;
  - element and array: `$exists $size $all $elemMatch`;
  - logic: `$not $and $or $nor`;
  - paths: dotted paths, which reach through arrays; null matches a missing field.
- **Updates**: `$set $unset $inc $mul $min $max $rename $push $addToSet $pull
  $pop $setOnInsert $currentDate`. `_id` is immutable.
- **find options**: sort, skip, limit, and inclusion or exclusion projection.
- **Indexes**:
  - single-field, compound, unique and multikey (arrays);
  - built from existing documents when created;
  - used for equality, `$in` and range seeks on their leading fields;
  - read in key order to satisfy ascending sorts, stopping early under a limit;
  - `_id` seeks and `_id` order use the collection's own key order;
  - `explain` reports COLLSCAN, IDHACK or IXSCAN; `explain_find` also says
    whether a sort is by index or in memory.
- **Aggregation**: `aggregate` runs pipelines:
  - stages: `$match $project $addFields/$set $unset $group $sort $skip $limit
    $unwind $count $lookup $replaceRoot/$replaceWith`;
  - expressions: field paths, `$$ROOT`, arithmetic, comparison, logic, `$cond
    $ifNull $concat $toUpper $toLower $size $arrayElemAt $in $literal`;
  - accumulators: `$sum $avg $min $max $first $last $push $addToSet $count`;
  - leading `$match`, `$sort`, `$skip` and `$limit` stages become one find,
    so they use indexes; `$lookup` is an `$in` find on the other collection.
- **Transactions**: sessions give snapshot isolation through store's MVCC. A
  write-write conflict fails with `WriteConflict`. A failed operation aborts
  the transaction, as in MongoDB.
- **Persistence**: collections and indexes live in store tables. A catalog
  table records them, so they survive a reopen.

## Differences from MongoDB

- **Atomicity outside a transaction**: each operation runs in one store
  transaction. A failing `insert_many` or `update_many` therefore changes
  nothing; MongoDB would keep the writes made before the failure.
- **Missing features**: no regex, `$facet`/`$bucket` and other stages, text, geo, TTL, sparse or
  partial indexes, collation, or `$slice` and positional projections.
- **Descending sorts**: done in memory, since an index is only read forwards.
  Descending index fields are stored ascending.
- **Size limits**: keys (`_id` and indexed values) are limited to 256
  serialized bytes (`KeyTooLarge`).
- **Multikey null keys**: a multikey index stores a null key wherever a path is
  missing along an array branch. On a unique index, this can refuse documents
  that MongoDB would accept.
- **DDL during transactions**: creating or dropping a collection or index is
  refused while any transaction is open.
