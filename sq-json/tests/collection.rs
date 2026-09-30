use sq_json::{Client, Collection, Document, Error, FindOptions, IndexOptions, Value};
use store::db::Db;
use store::memfile::MemFile;

fn d(json: &str) -> Document {
    Document::parse(json).unwrap()
}

fn client(name: &str) -> Client<MemFile> {
    Client::start(Db::<MemFile>::create(name).unwrap()).unwrap()
}

fn json(docs: &[Document]) -> String {
    docs.iter().map(Document::to_json).collect::<Vec<_>>().join(",")
}

fn stage(c: &Collection<MemFile>, filter: &str) -> String {
    let e = c.explain(d(filter)).unwrap();
    let mut s = e.get("stage").unwrap().to_json();
    if let Some(name) = e.get("indexName") {
        s = format!("{s} {}", name.to_json());
    }
    s.replace('"', "")
}

fn load(c: &Collection<MemFile>) {
    c.insert_many(
        [
            r#"{"_id": 1, "sku": "a", "qty": 5, "tags": ["red", "blue"], "dim": {"w": 10, "h": 2}}"#,
            r#"{"_id": 2, "sku": "b", "qty": 15, "tags": ["red"], "dim": {"w": 3}}"#,
            r#"{"_id": 3, "sku": "c", "qty": 25.5, "tags": [], "dim": {"w": 7, "h": 9}}"#,
            r#"{"_id": 4, "sku": "d", "tags": ["green"]}"#,
            r#"{"_id": 5, "sku": "e", "qty": "many"}"#,
        ]
        .map(d)
        .to_vec(),
    )
    .unwrap();
}

#[test]
fn test_crud_round_trip() {
    let client = client("sqjson_crud");
    let c = client.database("shop").collection("items");
    let id = c.insert_one(d(r#"{"name": "pen", "_id": 7}"#)).unwrap();
    assert_eq!(id, Value::Int(7));
    // _id moves first; a missing one is an ObjectId.
    assert_eq!(c.find_one(d("{}")).unwrap().unwrap().to_json(), r#"{"_id":7,"name":"pen"}"#);
    let oid = c.insert_one(d(r#"{"name": "ink"}"#)).unwrap();
    assert!(matches!(oid, Value::ObjectId(_)));

    let r = c.update_one(d(r#"{"name": "pen"}"#), d(r#"{"$set": {"price": 2}, "$inc": {"stock": 3}}"#), false).unwrap();
    assert_eq!((r.matched_count, r.modified_count), (1, 1));
    assert_eq!(
        c.find_one(d(r#"{"_id": 7}"#)).unwrap().unwrap().to_json(),
        r#"{"_id":7,"name":"pen","price":2,"stock":3}"#
    );
    // An update that changes nothing matches but doesn't modify.
    let r = c.update_one(d(r#"{"_id": 7}"#), d(r#"{"$set": {"price": 2}}"#), false).unwrap();
    assert_eq!((r.matched_count, r.modified_count), (1, 0));

    let r = c.replace_one(d(r#"{"_id": 7}"#), d(r#"{"name": "quill"}"#), false).unwrap();
    assert_eq!(r.modified_count, 1);
    assert_eq!(c.find_one(d(r#"{"_id": 7}"#)).unwrap().unwrap().to_json(), r#"{"_id":7,"name":"quill"}"#);
    assert!(matches!(c.update_one(d("{}"), d(r#"{"name": "x"}"#), false), Err(Error::BadValue(_))));
    assert!(matches!(c.replace_one(d("{}"), d(r#"{"$set": {"a": 1}}"#), false), Err(Error::BadValue(_))));
    assert!(matches!(
        c.update_one(d(r#"{"_id": 7}"#), d(r#"{"$set": {"_id": 8}}"#), false),
        Err(Error::ImmutableId)
    ));

    // Upserts insert from the filter's equalities plus the update.
    let r = c.update_one(d(r#"{"name": "cap", "size": 3}"#), d(r#"{"$set": {"color": "red"}}"#), true).unwrap();
    assert_eq!(r.matched_count, 0);
    let upserted = r.upserted_id.unwrap();
    let doc = c.find_one(d(r#"{"name": "cap"}"#)).unwrap().unwrap();
    assert_eq!(doc.get("_id"), Some(&upserted));
    assert_eq!(doc.get("color"), Some(&Value::String("red".into())));
    let r = c.replace_one(d(r#"{"_id": 99}"#), d(r#"{"name": "new"}"#), true).unwrap();
    assert_eq!(r.upserted_id, Some(Value::Int(99)));

    assert_eq!(c.count_documents(d("{}")).unwrap(), 4);
    assert_eq!(c.delete_one(d(r#"{"name": {"$in": ["cap", "new"]}}"#)).unwrap(), 1);
    assert_eq!(c.delete_many(d("{}")).unwrap(), 3);
    assert_eq!(c.count_documents(d("{}")).unwrap(), 0);
    // Reads and deletes of a collection that doesn't exist find nothing.
    let none = client.database("shop").collection("nothing");
    assert!(none.find_one(d("{}")).unwrap().is_none());
    assert_eq!(none.delete_many(d("{}")).unwrap(), 0);
    assert_eq!(client.database("shop").list_collection_names(), vec!["items"]);
}

#[test]
fn test_duplicate_ids_and_bad_documents_are_refused_atomically() {
    let client = client("sqjson_dups");
    let c = client.database("t").collection("c");
    c.insert_one(d(r#"{"_id": 1}"#)).unwrap();
    let e = c.insert_one(d(r#"{"_id": 1.0}"#)).unwrap_err();
    assert_eq!(e.to_string(), "E11000 duplicate key error collection: t.c index: _id_ dup key: 1.0");
    // insert_many is all or nothing.
    assert!(c.insert_many(vec![d(r#"{"_id": 2}"#), d(r#"{"_id": 1}"#)]).is_err());
    assert_eq!(c.count_documents(d("{}")).unwrap(), 1);
    assert!(c.insert_one(d(r#"{"_id": [1]}"#)).is_err());
    assert!(c.insert_one(d(r#"{"$bad": 1}"#)).is_err());
    let long = format!(r#"{{"_id": "{}"}}"#, "x".repeat(400));
    assert!(matches!(c.insert_one(d(&long)), Err(Error::KeyTooLarge(..))));
}

#[test]
fn test_find_sort_skip_limit_projection_and_distinct() {
    let client = client("sqjson_find");
    let c = client.database("t").collection("c");
    load(&c);
    let ids = |docs: Vec<Document>| docs.iter().map(|x| x.get("_id").unwrap().to_json()).collect::<Vec<_>>().join(",");
    // Comparisons are type-bracketed: "many" isn't above 10.
    assert_eq!(ids(c.find(d(r#"{"qty": {"$gt": 10}}"#), FindOptions::new()).unwrap()), "2,3");
    assert_eq!(ids(c.find(d(r#"{"tags": "red"}"#), FindOptions::new()).unwrap()), "1,2");
    assert_eq!(ids(c.find(d(r#"{"dim.h": {"$exists": false}}"#), FindOptions::new()).unwrap()), "2,4,5");
    assert_eq!(ids(c.find(d(r#"{"qty": null}"#), FindOptions::new()).unwrap()), "4");
    assert_eq!(
        ids(c.find(d(r#"{"$or": [{"sku": "a"}, {"qty": {"$gte": 25}}]}"#), FindOptions::new()).unwrap()),
        "1,3"
    );
    let sorted = FindOptions::new().sort(d(r#"{"qty": -1}"#));
    // Strings sort above numbers; missing sorts as null, lowest.
    assert_eq!(ids(c.find(d("{}"), sorted.clone()).unwrap()), "5,3,2,1,4");
    assert_eq!(ids(c.find(d("{}"), sorted.skip(1).limit(2)).unwrap()), "3,2");
    assert_eq!(ids(c.find(d("{}"), FindOptions::new().limit(2)).unwrap()), "1,2");
    let projected = c
        .find(d(r#"{"_id": {"$lte": 2}}"#), FindOptions::new().projection(d(r#"{"dim.w": 1, "_id": 0}"#)))
        .unwrap();
    assert_eq!(json(&projected), r#"{"dim":{"w":10}},{"dim":{"w":3}}"#);
    let tags = c.distinct("tags", d("{}")).unwrap();
    assert_eq!(tags.iter().map(Value::to_json).collect::<Vec<_>>(), [r#""blue""#, r#""green""#, r#""red""#]);
}

#[test]
fn test_indexes_answer_queries_with_the_same_results() {
    let client = client("sqjson_indexes");
    let c = client.database("t").collection("c");
    load(&c);
    let filters = [
        r#"{"qty": 15}"#,
        r#"{"qty": {"$gt": 5}}"#,
        r#"{"qty": {"$gte": 5, "$lt": 20}}"#,
        r#"{"qty": {"$in": [5, 25.5, "many"]}}"#,
        r#"{"qty": null}"#,
        r#"{"qty": {"$lt": "n"}}"#,
        r#"{"tags": "red"}"#,
        r#"{"tags": {"$in": ["red", "green"]}}"#,
        r#"{"tags": {"$gt": "a", "$lt": "c"}}"#,
        r#"{"dim.h": null}"#,
        r#"{"dim.w": {"$lte": 7}, "sku": {"$ne": "b"}}"#,
        r#"{"sku": "c", "qty": {"$gt": 20}}"#,
        r#"{"_id": {"$in": [2, 4, 9]}}"#,
        r#"{"_id": {"$gt": 3}}"#,
    ];
    // Index order may differ from _id order: compare in _id order.
    let found = |f: &str| {
        let sorted = FindOptions::new().sort(d(r#"{"_id": 1}"#));
        json(&c.find(d(f), sorted).unwrap())
    };
    let before: Vec<String> = filters.iter().map(|f| found(f)).collect();
    for f in &filters {
        assert!(matches!(stage(&c, f).as_str(), "COLLSCAN" | "IDHACK" | "IXSCAN _id_"), "{f}");
    }
    c.create_index(d(r#"{"qty": 1}"#), IndexOptions::default()).unwrap();
    c.create_index(d(r#"{"tags": 1}"#), IndexOptions::default()).unwrap();
    c.create_index(d(r#"{"dim.w": 1}"#), IndexOptions::default()).unwrap();
    c.create_index(d(r#"{"dim.h": 1}"#), IndexOptions::default()).unwrap();
    c.create_index(d(r#"{"sku": 1, "qty": -1}"#), IndexOptions::default()).unwrap();
    for (f, expected) in filters.iter().zip(&before) {
        assert_eq!(&found(f), expected, "{f}");
    }
    assert_eq!(stage(&c, r#"{"qty": {"$gt": 5}}"#), "IXSCAN qty_1");
    assert_eq!(stage(&c, r#"{"tags": "red"}"#), "IXSCAN tags_1");
    assert_eq!(stage(&c, r#"{"sku": "c", "qty": {"$gt": 20}}"#), "IXSCAN sku_1_qty_-1");
    assert_eq!(stage(&c, r#"{"_id": 3}"#), "IDHACK");
    assert_eq!(stage(&c, r#"{"qty": {"$ne": 3}}"#), "COLLSCAN");
    // tags holds arrays: the index is multikey, and a document matched by
    // two elements comes back once.
    let e = c.explain(d(r#"{"tags": {"$in": ["red", "blue"]}}"#)).unwrap();
    assert_eq!(e.get("multikey"), Some(&Value::Bool(true)));
    assert_eq!(c.count_documents(d(r#"{"tags": {"$in": ["red", "blue"]}}"#)).unwrap(), 2);

    // Writes keep the indexes in step.
    c.update_many(d(r#"{"qty": {"$gt": 10}}"#), d(r#"{"$inc": {"qty": 100}}"#), false).unwrap();
    assert_eq!(c.count_documents(d(r#"{"qty": {"$gt": 100}}"#)).unwrap(), 2);
    assert_eq!(c.count_documents(d(r#"{"qty": {"$gt": 10, "$lt": 100}}"#)).unwrap(), 0);
    c.update_one(d(r#"{"_id": 2}"#), d(r#"{"$push": {"tags": "gold"}}"#), false).unwrap();
    assert_eq!(c.count_documents(d(r#"{"tags": "gold"}"#)).unwrap(), 1);
    c.delete_many(d(r#"{"tags": "red"}"#)).unwrap();
    assert_eq!(c.count_documents(d(r#"{"tags": "gold"}"#)).unwrap(), 0);
    assert_eq!(c.count_documents(d(r#"{"qty": {"$gte": 0}}"#)).unwrap(), 1);

    assert_eq!(c.list_indexes().len(), 6);
    c.drop_index("tags_1").unwrap();
    assert!(matches!(c.drop_index("tags_1"), Err(Error::IndexNotFound(_))));
    assert_eq!(stage(&c, r#"{"tags": "red"}"#), "COLLSCAN");
}

#[test]
fn test_unique_indexes() {
    let client = client("sqjson_unique");
    let c = client.database("t").collection("users");
    c.create_index(d(r#"{"email": 1}"#), IndexOptions { unique: true, name: None }).unwrap();
    c.insert_one(d(r#"{"_id": 1, "email": "a@x"}"#)).unwrap();
    let e = c.insert_one(d(r#"{"_id": 2, "email": "a@x"}"#)).unwrap_err();
    assert_eq!(e.to_string(), r#"E11000 duplicate key error collection: t.users index: email_1 dup key: "a@x""#);
    // The failed insert left nothing behind.
    assert!(c.find_one(d(r#"{"_id": 2}"#)).unwrap().is_none());
    // A missing field is indexed as null: only one document may lack it.
    c.insert_one(d(r#"{"_id": 3}"#)).unwrap();
    assert!(c.insert_one(d(r#"{"_id": 4}"#)).is_err());
    // Changing the value frees the old one.
    c.update_one(d(r#"{"_id": 1}"#), d(r#"{"$set": {"email": "b@x"}}"#), false).unwrap();
    c.insert_one(d(r#"{"_id": 5, "email": "a@x"}"#)).unwrap();
    assert!(c.update_one(d(r#"{"_id": 5}"#), d(r#"{"$set": {"email": "b@x"}}"#), false).is_err());
    // Building a unique index over duplicates fails, leaving no index.
    let o = client.database("t").collection("other");
    o.insert_many(vec![d(r#"{"k": 1}"#), d(r#"{"k": 1.0}"#)]).unwrap();
    assert!(o.create_index(d(r#"{"k": 1}"#), IndexOptions { unique: true, name: None }).is_err());
    assert_eq!(o.list_indexes().len(), 1);
    // Compound multikey over two arrays at once is refused.
    let p = client.database("t").collection("pairs");
    p.create_index(d(r#"{"a": 1, "b": 1}"#), IndexOptions::default()).unwrap();
    p.insert_one(d(r#"{"a": [1, 2], "b": 3}"#)).unwrap();
    assert!(p.insert_one(d(r#"{"a": [1, 2], "b": [3, 4]}"#)).is_err());
}

#[test]
fn test_transactions_commit_abort_and_isolate() {
    let client = client("sqjson_txn");
    let c = client.database("bank").collection("accounts");
    c.insert_many(vec![d(r#"{"_id": "alice", "bal": 100}"#), d(r#"{"_id": "bob", "bal": 0}"#)]).unwrap();

    let session = client.start_session();
    session.start_transaction().unwrap();
    let s = c.with_session(&session);
    s.update_one(d(r#"{"_id": "alice"}"#), d(r#"{"$inc": {"bal": -30}}"#), false).unwrap();
    s.update_one(d(r#"{"_id": "bob"}"#), d(r#"{"$inc": {"bal": 30}}"#), false).unwrap();
    // The transaction sees its writes; others don't until commit.
    assert_eq!(s.find_one(d(r#"{"_id": "bob"}"#)).unwrap().unwrap().get("bal"), Some(&Value::Int(30)));
    assert_eq!(c.find_one(d(r#"{"_id": "bob"}"#)).unwrap().unwrap().get("bal"), Some(&Value::Int(0)));
    // No DDL while it's open.
    assert!(matches!(c.create_index(d(r#"{"bal": 1}"#), IndexOptions::default()), Err(Error::Transaction(_))));
    session.commit_transaction().unwrap();
    assert_eq!(c.find_one(d(r#"{"_id": "bob"}"#)).unwrap().unwrap().get("bal"), Some(&Value::Int(30)));

    session.start_transaction().unwrap();
    s.delete_many(d("{}")).unwrap();
    s.insert_one(d(r#"{"_id": "carol"}"#)).unwrap();
    session.abort_transaction().unwrap();
    assert_eq!(c.count_documents(d("{}")).unwrap(), 2);

    // A failed operation aborts the whole transaction.
    session.start_transaction().unwrap();
    s.insert_one(d(r#"{"_id": "dave"}"#)).unwrap();
    assert!(s.insert_one(d(r#"{"_id": "alice"}"#)).is_err());
    assert!(!session.in_transaction());
    assert!(session.commit_transaction().is_err());
    assert!(c.find_one(d(r#"{"_id": "dave"}"#)).unwrap().is_none());

    // Two transactions writing one document: the second conflicts.
    let other = client.start_session();
    session.start_transaction().unwrap();
    other.start_transaction().unwrap();
    s.update_one(d(r#"{"_id": "alice"}"#), d(r#"{"$set": {"x": 1}}"#), false).unwrap();
    let e = c.with_session(&other).update_one(d(r#"{"_id": "alice"}"#), d(r#"{"$set": {"x": 2}}"#), false);
    assert!(matches!(e, Err(Error::WriteConflict)), "{e:?}");
    session.commit_transaction().unwrap();
    assert_eq!(c.find_one(d(r#"{"_id": "alice"}"#)).unwrap().unwrap().get("x"), Some(&Value::Int(1)));
}

#[test]
fn test_collections_and_indexes_persist_across_reopen() {
    let dir = std::env::temp_dir().join(format!("sqjson_persist_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("docs.db");
    let path = path.to_str().unwrap();
    {
        let client = Client::<std::fs::File>::create(path).unwrap();
        let c = client.database("app").collection("events");
        c.create_index(d(r#"{"kind": 1}"#), IndexOptions::default()).unwrap();
        c.create_index(d(r#"{"labels": 1}"#), IndexOptions::default()).unwrap();
        for i in 0..200 {
            c.insert_one(d(&format!(r#"{{"_id": {i}, "kind": "k{}", "labels": ["x", "y{}"]}}"#, i % 7, i % 3))).unwrap();
        }
        client.database("app").collection("gone").insert_one(d("{}")).unwrap();
        client.database("app").collection("gone").drop().unwrap();
        drop(c);
        client.close().unwrap();
    }
    let client = Client::<std::fs::File>::open(path).unwrap();
    assert_eq!(client.list_database_names(), vec!["app"]);
    assert_eq!(client.database("app").list_collection_names(), vec!["events"]);
    let c = client.database("app").collection("events");
    assert_eq!(c.list_indexes().len(), 3);
    assert_eq!(stage_file(&c, r#"{"kind": "k3"}"#), "IXSCAN");
    assert_eq!(c.count_documents(d(r#"{"kind": "k3"}"#)).unwrap(), 29);
    assert_eq!(c.explain(d(r#"{"labels": "x"}"#)).unwrap().get("multikey"), Some(&Value::Bool(true)));
    assert_eq!(c.count_documents(d(r#"{"labels": {"$in": ["x", "y1"]}}"#)).unwrap(), 200);
    drop(c);
    client.close().unwrap();
    let _ = Db::<std::fs::File>::delete(path);
    let _ = std::fs::remove_dir_all(&dir);
}

fn stage_file(c: &Collection<std::fs::File>, filter: &str) -> String {
    c.explain(d(filter)).unwrap().get("stage").unwrap().to_json().replace('"', "")
}

#[test]
fn test_indexes_supply_sort_order_with_the_same_results() {
    let client = client("sqjson_index_sort");
    let plain = client.database("t").collection("plain");
    let indexed = client.database("t").collection("indexed");
    // A mix of types, ties and missing fields.
    let docs: Vec<Document> = (0..300)
        .map(|i| {
            let a = match i % 11 {
                0 => String::new(),
                1 => r#""a": null,"#.into(),
                2 => format!(r#""a": "s{}","#, i % 4),
                3 => format!(r#""a": {}.5,"#, i % 6),
                _ => format!(r#""a": {},"#, i % 6),
            };
            d(&format!(r#"{{"_id": {}, {a} "b": {}, "c": {}}}"#, (i * 7919) % 300, i % 5, i % 3))
        })
        .collect();
    plain.insert_many(docs.clone()).unwrap();
    indexed.insert_many(docs).unwrap();
    indexed.create_index(d(r#"{"a": 1}"#), IndexOptions::default()).unwrap();
    indexed.create_index(d(r#"{"b": 1, "a": 1}"#), IndexOptions::default()).unwrap();
    let cases = [
        (r#"{}"#, r#"{"a": 1}"#, Some(7), "IXSCAN a_1 index"),
        (r#"{}"#, r#"{"a": 1, "_id": 1}"#, Some(20), "IXSCAN a_1 index"),
        (r#"{"a": {"$gte": 2}}"#, r#"{"a": 1}"#, None, "IXSCAN a_1 index"),
        (r#"{"b": 3}"#, r#"{"a": 1}"#, Some(5), "IXSCAN b_1_a_1 index"),
        (r#"{"b": 3}"#, r#"{"b": 1, "a": 1}"#, None, "IXSCAN b_1_a_1 index"),
        (r#"{"b": {"$in": [1, 3]}}"#, r#"{"b": 1, "a": 1}"#, Some(12), "IXSCAN b_1_a_1 index"),
        (r#"{"b": {"$in": [1, 3]}}"#, r#"{"a": 1}"#, Some(12), "IXSCAN b_1_a_1 memory"),
        (r#"{"c": 1}"#, r#"{"_id": 1}"#, Some(9), "COLLSCAN index"),
        (r#"{"_id": {"$gt": 100}}"#, r#"{"_id": 1}"#, Some(9), "IXSCAN _id_ index"),
        (r#"{}"#, r#"{"a": -1}"#, Some(7), "COLLSCAN memory"),
        (r#"{"c": 2}"#, r#"{"a": 1}"#, None, "COLLSCAN memory"),
        (r#"{"c": 2}"#, r#"{"a": 1}"#, Some(4), "IXSCAN a_1 index"),
    ];
    for (filter, sort, limit, expected) in cases {
        let mut options = FindOptions::new().sort(d(sort)).skip(2);
        if let Some(l) = limit {
            options = options.limit(l);
        }
        let e = indexed.explain_find(d(filter), &options).unwrap();
        let mut how = e.get("stage").unwrap().to_json();
        if let Some(n) = e.get("indexName") {
            how = format!("{how} {}", n.to_json());
        }
        let how = format!("{how} {}", e.get("sort").unwrap().to_json()).replace('"', "");
        assert_eq!(how, expected, "{filter} {sort}");
        // Ties may come out in any order: compare the sort fields, then the
        // documents as sets.
        let got = indexed.find(d(filter), options.clone()).unwrap();
        let want = plain.find(d(filter), options.clone()).unwrap();
        let keys = |docs: &[Document]| {
            let fields: Vec<String> = Document::parse(sort).unwrap().iter().map(|(k, _)| k.clone()).collect();
            docs.iter()
                .map(|doc| fields.iter().map(|f| doc.get(f).map(Value::to_json).unwrap_or("null".into())).collect::<Vec<_>>().join("/"))
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(&got), keys(&want), "{filter} {sort}");
        if limit.is_none() {
            let mut g: Vec<String> = got.iter().map(Document::to_json).collect();
            let mut w: Vec<String> = want.iter().map(Document::to_json).collect();
            g.sort();
            w.sort();
            assert_eq!(g, w, "{filter} {sort}");
        }
    }
}

#[test]
fn test_aggregate_over_collections_with_lookup() {
    let client = client("sqjson_aggregate");
    let db = client.database("shop");
    let orders = db.collection("orders");
    let customers = db.collection("customers");
    customers
        .insert_many(vec![d(r#"{"_id": "c1", "name": "Ann"}"#), d(r#"{"_id": "c2", "name": "Bo"}"#)])
        .unwrap();
    orders
        .insert_many(vec![
            d(r#"{"_id": 1, "cust": "c1", "total": 10, "status": "paid"}"#),
            d(r#"{"_id": 2, "cust": "c2", "total": 5, "status": "paid"}"#),
            d(r#"{"_id": 3, "cust": "c1", "total": 7.5, "status": "paid"}"#),
            d(r#"{"_id": 4, "cust": "c3", "total": 1, "status": "open"}"#),
        ])
        .unwrap();
    orders.create_index(d(r#"{"status": 1}"#), IndexOptions::default()).unwrap();
    let pipeline = |p: &str| match sq_json::value::from_json(p).unwrap() {
        Value::Array(stages) => stages
            .into_iter()
            .map(|s| match s {
                Value::Document(d) => d,
                _ => panic!("stages are documents"),
            })
            .collect::<Vec<_>>(),
        _ => panic!("a list"),
    };
    let out = orders
        .aggregate(pipeline(
            r#"[{"$match": {"status": "paid"}},
                {"$group": {"_id": "$cust", "spent": {"$sum": "$total"}, "n": {"$sum": 1}}},
                {"$lookup": {"from": "customers", "localField": "_id", "foreignField": "_id", "as": "who"}},
                {"$unwind": "$who"},
                {"$project": {"_id": 0, "name": "$who.name", "spent": 1, "n": 1}},
                {"$sort": {"spent": -1}}]"#,
        ))
        .unwrap();
    assert_eq!(json(&out), r#"{"spent":17.5,"n":2,"name":"Ann"},{"spent":5,"n":1,"name":"Bo"}"#);
    // Leading $sort/$limit go to find; an unmatched lookup gives [].
    let out = orders
        .aggregate(pipeline(
            r#"[{"$sort": {"_id": 1}}, {"$skip": 3}, {"$limit": 5},
                {"$lookup": {"from": "customers", "localField": "cust", "foreignField": "_id", "as": "who"}},
                {"$project": {"who": 1}}]"#,
        ))
        .unwrap();
    assert_eq!(json(&out), r#"{"_id":4,"who":[]}"#);
    assert!(orders.aggregate(pipeline(r#"[{"$bogus": 1}]"#)).is_err());
}
