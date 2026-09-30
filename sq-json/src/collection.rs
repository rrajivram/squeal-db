//! A collection: reads and writes of documents, and their indexes.
//!
//! Documents live in their collection's table keyed by `_id` (encoded as
//! keys.rs describes), serialized whole. Each secondary index is a table
//! of keys — the indexed fields' values, then (unless unique) the `_id`'s,
//! so equal values still make distinct keys — each holding the document's
//! `_id`. A unique index's key is its values alone, so the store's own
//! duplicate-key check enforces it.
//!
//! Every operation runs in one store transaction: the session's when it
//! has one open, else its own, committed at the end — so a multi-document
//! write outside a transaction is still all or nothing (MongoDB would keep
//! the documents written before a failure).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLockReadGuard;
use store::cursor::Cursor;
use store::db::DBFile;
use store::error::StoreError;
use store::tuple::{DBIdType, Tuple};
use store::txn::Transaction;
use store::valueitem::IndexKey;

use crate::client::{CollState, IndexState, Inner, Session};
use crate::error::{Error, Result};
use crate::filter::Filter;
use crate::keys::{check_size, encode};
use crate::plan::{Seek, seek_for};
use crate::query::{Projection, Sort};
use crate::update::{Update, check_field_names};
use crate::value::{Document, ObjectId, Value, compare, lookup, values_equal};

/// How a find orders, trims and shapes its results.
#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    pub sort: Option<Document>,
    pub projection: Option<Document>,
    pub skip: usize,
    pub limit: Option<usize>,
}

impl FindOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn sort(mut self, sort: Document) -> Self {
        self.sort = Some(sort);
        self
    }
    pub fn projection(mut self, projection: Document) -> Self {
        self.projection = Some(projection);
        self
    }
    pub fn skip(mut self, skip: usize) -> Self {
        self.skip = skip;
        self
    }
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    pub unique: bool,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResult {
    pub matched_count: usize,
    pub modified_count: usize,
    pub upserted_id: Option<Value>,
}

/// A description of an index, as list_indexes reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub keys: Document,
    pub unique: bool,
}

/// How a query finds its documents.
enum Plan {
    CollScan,
    Id(Seek),
    Index(Arc<IndexState>, Seek),
}

pub struct Collection<F: DBFile<Item = F> + 'static = std::fs::File> {
    inner: Arc<Inner<F>>,
    ns: String,
    session: Option<Session<F>>,
}

impl<F: DBFile<Item = F> + 'static> Clone for Collection<F> {
    fn clone(&self) -> Self {
        Collection {
            inner: self.inner.clone(),
            ns: self.ns.clone(),
            session: self.session.clone(),
        }
    }
}

impl<F: DBFile<Item = F> + 'static> Collection<F> {
    pub(crate) fn new(inner: Arc<Inner<F>>, db: &str, name: &str) -> Self {
        Collection {
            inner,
            ns: format!("{db}.{name}"),
            session: None,
        }
    }

    /// "database.collection".
    pub fn namespace(&self) -> &str {
        &self.ns
    }

    /// This collection, with operations run through `session` (joining its
    /// transaction while one is open).
    pub fn with_session(&self, session: &Session<F>) -> Self {
        Collection {
            session: Some(session.clone()),
            ..self.clone()
        }
    }

    // ---- writes ----

    /// Inserts a document, giving it an ObjectId `_id` (first) if it has
    /// none; returns the `_id`.
    pub fn insert_one(&self, doc: Document) -> Result<Value> {
        Ok(self.insert_many(vec![doc])?.remove(0))
    }

    pub fn insert_many(&self, docs: Vec<Document>) -> Result<Vec<Value>> {
        let (_ddl, state) = self.write_state()?;
        self.run(|txn| {
            docs.into_iter()
                .map(|mut doc| {
                    prepare_insert(&mut doc)?;
                    self.insert_doc(&state, &doc, txn)?;
                    Ok(doc.get("_id").cloned().unwrap_or(Value::Null))
                })
                .collect()
        })
    }

    pub fn update_one(&self, filter: Document, update: Document, upsert: bool) -> Result<UpdateResult> {
        self.update(filter, operators(update)?, upsert, false)
    }

    pub fn update_many(&self, filter: Document, update: Document, upsert: bool) -> Result<UpdateResult> {
        self.update(filter, operators(update)?, upsert, true)
    }

    pub fn replace_one(&self, filter: Document, replacement: Document, upsert: bool) -> Result<UpdateResult> {
        let update = Update::parse(&replacement)?;
        if !matches!(update, Update::Replace(_)) {
            return Err(Error::BadValue("a replacement document can't contain update operators".into()));
        }
        self.update(filter, update, upsert, false)
    }

    fn update(&self, filter: Document, update: Update, upsert: bool, many: bool) -> Result<UpdateResult> {
        let filter = Filter::parse(&filter)?;
        let (_ddl, state) = if upsert {
            let (g, s) = self.write_state()?;
            (g, Some(s))
        } else {
            self.read_state()
        };
        let Some(state) = state else {
            return Ok(UpdateResult { matched_count: 0, modified_count: 0, upserted_id: None });
        };
        self.run(|txn| {
            // Found first, then changed, so no document is seen twice.
            let limit = if many { None } else { Some(1) };
            let found = self.matching(&state, &filter, limit, txn)?;
            if found.is_empty() && upsert {
                let mut doc = update.upsert_document(&filter)?;
                prepare_insert(&mut doc)?;
                self.insert_doc(&state, &doc, txn)?;
                return Ok(UpdateResult {
                    matched_count: 0,
                    modified_count: 0,
                    upserted_id: doc.get("_id").cloned(),
                });
            }
            let mut modified = 0;
            for old in &found {
                let mut new = old.clone();
                if update.apply(&mut new, false)? {
                    self.update_doc(&state, old, &new, txn)?;
                    modified += 1;
                }
            }
            Ok(UpdateResult {
                matched_count: found.len(),
                modified_count: modified,
                upserted_id: None,
            })
        })
    }

    /// Returns how many were deleted (0 or 1).
    pub fn delete_one(&self, filter: Document) -> Result<usize> {
        self.delete(filter, Some(1))
    }

    pub fn delete_many(&self, filter: Document) -> Result<usize> {
        self.delete(filter, None)
    }

    fn delete(&self, filter: Document, limit: Option<usize>) -> Result<usize> {
        let filter = Filter::parse(&filter)?;
        let (_ddl, state) = self.read_state();
        let Some(state) = state else { return Ok(0) };
        self.run(|txn| {
            let found = self.matching(&state, &filter, limit, txn)?;
            for doc in &found {
                self.delete_doc(&state, doc, txn)?;
            }
            Ok(found.len())
        })
    }

    // ---- reads ----

    pub fn find(&self, filter: Document, options: FindOptions) -> Result<Vec<Document>> {
        let filter = Filter::parse(&filter)?;
        let sort = options.sort.as_ref().map(Sort::parse).transpose()?;
        let projection = match &options.projection {
            Some(p) => Projection::parse(p)?,
            None => None,
        };
        let (_ddl, state) = self.read_state();
        let Some(state) = state else { return Ok(vec![]) };
        let mut docs = self.run(|txn| {
            // Unsorted, only the first skip + limit are needed.
            let enough = match (&sort, options.limit) {
                (None, Some(limit)) => Some(options.skip + limit),
                _ => None,
            };
            self.matching(&state, &filter, enough, txn)
        })?;
        if let Some(sort) = &sort {
            docs.sort_by(|a, b| sort.compare(a, b));
        }
        let docs = docs.into_iter().skip(options.skip).take(options.limit.unwrap_or(usize::MAX));
        Ok(match &projection {
            Some(p) => docs.map(|d| p.apply(&d)).collect(),
            None => docs.collect(),
        })
    }

    pub fn find_one(&self, filter: Document) -> Result<Option<Document>> {
        Ok(self.find(filter, FindOptions::new().limit(1))?.pop())
    }

    pub fn count_documents(&self, filter: Document) -> Result<usize> {
        Ok(self.find(filter, FindOptions::new())?.len())
    }

    /// The distinct values of `path` among matching documents (an array's
    /// elements counted separately), in MongoDB's order.
    pub fn distinct(&self, path: &str, filter: Document) -> Result<Vec<Value>> {
        let mut values = vec![];
        for doc in self.find(filter, FindOptions::new())? {
            for v in lookup(&doc, path) {
                match v {
                    Value::Array(items) => values.extend(items.iter().cloned()),
                    other => values.push(other.clone()),
                }
            }
        }
        values.sort_by(compare);
        values.dedup_by(|a, b| values_equal(a, b));
        Ok(values)
    }

    /// How a find with `filter` would run: `{stage: "COLLSCAN"}`, or
    /// `{stage: "IXSCAN", indexName, keyRanges, multikey}` (`_id_` for the
    /// collection's own `_id` order), or `{stage: "IDHACK"}` for one `_id`.
    pub fn explain(&self, filter: Document) -> Result<Document> {
        let filter = Filter::parse(&filter)?;
        let (_ddl, state) = self.read_state();
        let mut out = Document::new();
        let plan = match &state {
            Some(state) => plan(state, &filter),
            None => Plan::CollScan,
        };
        match plan {
            Plan::CollScan => out.insert("stage", Value::String("COLLSCAN".into())),
            Plan::Id(seek) if !seek.ranged && seek.ranges.len() == 1 => {
                out.insert("stage", Value::String("IDHACK".into()))
            }
            Plan::Id(seek) => {
                out.insert("stage", Value::String("IXSCAN".into()));
                out.insert("indexName", Value::String("_id_".into()));
                out.insert("keyRanges", Value::Int(seek.ranges.len() as i64));
                out.insert("multikey", Value::Bool(false));
            }
            Plan::Index(ix, seek) => {
                out.insert("stage", Value::String("IXSCAN".into()));
                out.insert("indexName", Value::String(ix.name.clone()));
                out.insert("keyRanges", Value::Int(seek.ranges.len() as i64));
                out.insert("multikey", Value::Bool(ix.multikey.load(Ordering::SeqCst)));
            }
        }
        Ok(out)
    }

    // ---- indexes and the collection itself ----

    /// Creates an index over `keys` (`{field: 1 | -1, ...}`), filling it
    /// from the documents already here; returns its name. Creating an
    /// index that already exists (same keys and options) does nothing.
    pub fn create_index(&self, keys: Document, options: IndexOptions) -> Result<String> {
        let mut fields = vec![];
        for (path, dir) in keys.iter() {
            let dir = match dir {
                Value::Int(d @ (1 | -1)) => *d as i8,
                Value::Double(d) if *d == 1.0 || *d == -1.0 => *d as i8,
                _ => return Err(Error::BadValue(format!("index direction for {path} must be 1 or -1"))),
            };
            if path.is_empty() || path.starts_with('$') {
                return Err(Error::BadValue(format!("bad index field name '{path}'")));
            }
            fields.push((path.clone(), dir));
        }
        if fields.is_empty() {
            return Err(Error::BadValue("an index needs at least one field".into()));
        }
        let name = options.name.clone().unwrap_or_else(|| {
            fields.iter().map(|(p, d)| format!("{p}_{d}")).collect::<Vec<_>>().join("_")
        });
        if name == "_id_" || fields == [("_id".to_string(), 1)] {
            return Ok("_id_".into());
        }
        let _ddl = self.inner.ddl.write();
        self.inner.check_no_transactions("create an index")?;
        let state = self.inner.create_collection(&self.ns)?;
        if let Some(ix) = state.indexes.iter().find(|ix| ix.name == name || ix.keys == fields) {
            if ix.name == name && ix.keys == fields && ix.unique == options.unique {
                return Ok(name);
            }
            return Err(Error::BadValue(format!(
                "an index named {} with keys {:?} already exists",
                ix.name, ix.keys
            )));
        }
        let (table, tid) = self.inner.new_table()?;
        let ix = Arc::new(IndexState {
            name: name.clone(),
            keys: fields,
            unique: options.unique,
            table: table.clone(),
            tid,
            multikey: Default::default(),
        });
        let filled = self.fill(&state, &ix);
        if let Err(e) = filled {
            let _ = self.inner.db.drop_table(&table);
            return Err(e);
        }
        let mut indexes = state.indexes.clone();
        indexes.push(ix);
        self.inner.publish(Arc::new(CollState {
            ns: state.ns.clone(),
            table: state.table.clone(),
            tid: state.tid,
            indexes,
        }))?;
        Ok(name)
    }

    // Writes the new index's entries for every document.
    fn fill(&self, state: &CollState, ix: &IndexState) -> Result<()> {
        let db = &self.inner.db;
        let txn = db.begin()?;
        let docs = self.matching(state, &Filter::And(vec![]), None, &txn)?;
        for doc in &docs {
            self.insert_entries(ix, doc, &txn)?;
        }
        db.commit(txn)?;
        Ok(())
    }

    pub fn drop_index(&self, name: &str) -> Result<()> {
        let _ddl = self.inner.ddl.write();
        self.inner.check_no_transactions("drop an index")?;
        let state = self
            .inner
            .state(&self.ns)
            .ok_or_else(|| Error::IndexNotFound(name.into()))?;
        let Some(ix) = state.indexes.iter().find(|ix| ix.name == name) else {
            return Err(Error::IndexNotFound(name.into()));
        };
        let indexes = state.indexes.iter().filter(|i| i.name != name).cloned().collect();
        self.inner.publish(Arc::new(CollState {
            ns: state.ns.clone(),
            table: state.table.clone(),
            tid: state.tid,
            indexes,
        }))?;
        self.inner.db.drop_table(&ix.table)?;
        Ok(())
    }

    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        let mut out = vec![];
        let Some(state) = self.inner.state(&self.ns) else { return out };
        let mut id = Document::new();
        id.insert("_id", Value::Int(1));
        out.push(IndexInfo { name: "_id_".into(), keys: id, unique: true });
        for ix in &state.indexes {
            let mut keys = Document::new();
            for (p, d) in &ix.keys {
                keys.insert(p.clone(), Value::Int(*d as i64));
            }
            out.push(IndexInfo { name: ix.name.clone(), keys, unique: ix.unique });
        }
        out
    }

    /// Drops the collection, its documents and indexes. Dropping one that
    /// doesn't exist does nothing.
    pub fn drop(&self) -> Result<()> {
        let _ddl = self.inner.ddl.write();
        self.inner.check_no_transactions("drop a collection")?;
        let Some(state) = self.inner.state(&self.ns) else { return Ok(()) };
        self.inner.unpublish(&self.ns)?;
        for ix in &state.indexes {
            self.inner.db.drop_table(&ix.table)?;
        }
        self.inner.db.drop_table(&state.table)?;
        Ok(())
    }

    // ---- plumbing ----

    // The collection's state (None if it doesn't exist), holding off DDL
    // while the guard lives.
    fn read_state(&self) -> (RwLockReadGuard<'_, ()>, Option<Arc<CollState>>) {
        let guard = self.inner.ddl.read();
        (guard, self.inner.state(&self.ns))
    }

    // As read_state, creating the collection if need be.
    fn write_state(&self) -> Result<(RwLockReadGuard<'_, ()>, Arc<CollState>)> {
        loop {
            if let (guard, Some(state)) = self.read_state() {
                return Ok((guard, state));
            }
            let _ddl = self.inner.ddl.write();
            self.inner.create_collection(&self.ns)?;
        }
    }

    // Runs `f` in the session's open transaction (which a failure aborts),
    // or in one of its own.
    fn run<T>(&self, f: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
        if let Some(session) = &self.session {
            let mut txn = session.0.txn.lock();
            if let Some(t) = txn.as_ref() {
                let result = f(t);
                if result.is_err() {
                    session.0.abort_after_error(&mut txn);
                }
                return result;
            }
        }
        let db = &self.inner.db;
        let txn = db.begin()?;
        match f(&txn) {
            Ok(v) => {
                db.commit(txn)?;
                Ok(v)
            }
            Err(e) => {
                let _ = db.rollback(txn);
                Err(e)
            }
        }
    }

    // The documents matching `filter`, up to `limit` of them.
    fn matching(&self, state: &CollState, filter: &Filter, limit: Option<usize>, txn: &Transaction) -> Result<Vec<Document>> {
        let db = &self.inner.db;
        let limit = limit.unwrap_or(usize::MAX);
        let mut out = vec![];
        if limit == 0 {
            return Ok(out);
        }
        let mut keep = |doc: Document| {
            if filter.matches(&doc) {
                out.push(doc);
            }
            out.len() < limit
        };
        match plan(state, filter) {
            Plan::CollScan => {
                let mut cursor = db.table_scan_in_txn(state.tid, txn)?;
                while let Some(tuple) = cursor.next()? {
                    if !tuple.is_tombstoned() && !keep(postcard::from_bytes(tuple.data())?) {
                        break;
                    }
                }
            }
            Plan::Id(seek) => {
                let mut cursor = db.key_ranges_scan(state.tid, Some(txn.id()), seek.ranges)?;
                while let Some(tuple) = cursor.next()? {
                    if !tuple.is_tombstoned() && !keep(postcard::from_bytes(tuple.data())?) {
                        break;
                    }
                }
            }
            Plan::Index(ix, seek) => {
                let multikey = ix.multikey.load(Ordering::SeqCst);
                let mut seen = HashSet::new();
                let mut cursor = db.key_ranges_scan(ix.tid, Some(txn.id()), seek.ranges)?;
                while let Some(entry) = cursor.next()? {
                    if entry.is_tombstoned() {
                        continue;
                    }
                    let id: Value = postcard::from_bytes(entry.data())?;
                    let key = id_key(&id)?;
                    if multikey && !seen.insert(key.to_bytes()) {
                        continue;
                    }
                    let Some(tuple) = db.find(state.tid, DBIdType::Rec(key), txn)? else {
                        continue;
                    };
                    if !keep(postcard::from_bytes(tuple.data())?) {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn insert_doc(&self, state: &CollState, doc: &Document, txn: &Transaction) -> Result<()> {
        let id = doc.get("_id").expect("prepared documents have an _id");
        let key = id_key(id)?;
        let tuple = Tuple::new_with(DBIdType::Rec(key), &postcard::to_allocvec(doc)?, None, None);
        self.inner.db.insert(state.tid, tuple, txn).map_err(|e| {
            self.duplicate(e, "_id_", std::slice::from_ref(id))
        })?;
        for ix in &state.indexes {
            self.insert_entries(ix, doc, txn)?;
        }
        Ok(())
    }

    fn update_doc(&self, state: &CollState, old: &Document, new: &Document, txn: &Transaction) -> Result<()> {
        let db = &self.inner.db;
        let id = new.get("_id").expect("stored documents have an _id");
        for ix in &state.indexes {
            let (before, _) = index_keys(ix, old)?;
            let (after, multikey) = index_keys(ix, new)?;
            let before_bytes: HashSet<Vec<u8>> = before.iter().map(|(k, _)| k.to_bytes()).collect();
            let after_bytes: HashSet<Vec<u8>> = after.iter().map(|(k, _)| k.to_bytes()).collect();
            for (key, _) in &before {
                if !after_bytes.contains(&key.to_bytes()) {
                    db.remove(ix.tid, DBIdType::Rec(key.clone()), txn)?;
                }
            }
            if multikey {
                self.inner.set_multikey(&self.ns, ix)?;
            }
            for (key, values) in after {
                if !before_bytes.contains(&key.to_bytes()) {
                    self.insert_entry(ix, key, &values, id, txn)?;
                }
            }
        }
        let tuple = Tuple::new_with(DBIdType::Rec(id_key(id)?), &postcard::to_allocvec(new)?, None, None);
        db.update(state.tid, tuple, txn)?;
        Ok(())
    }

    fn delete_doc(&self, state: &CollState, doc: &Document, txn: &Transaction) -> Result<()> {
        let db = &self.inner.db;
        for ix in &state.indexes {
            for (key, _) in index_keys(ix, doc)?.0 {
                db.remove(ix.tid, DBIdType::Rec(key), txn)?;
            }
        }
        let id = doc.get("_id").expect("stored documents have an _id");
        db.remove(state.tid, DBIdType::Rec(id_key(id)?), txn)?;
        Ok(())
    }

    fn insert_entries(&self, ix: &IndexState, doc: &Document, txn: &Transaction) -> Result<()> {
        let (keys, multikey) = index_keys(ix, doc)?;
        if multikey {
            self.inner.set_multikey(&self.ns, ix)?;
        }
        let id = doc.get("_id").expect("stored documents have an _id");
        for (key, values) in keys {
            self.insert_entry(ix, key, &values, id, txn)?;
        }
        Ok(())
    }

    fn insert_entry(&self, ix: &IndexState, key: IndexKey, values: &[Value], id: &Value, txn: &Transaction) -> Result<()> {
        let tuple = Tuple::new_with(DBIdType::Rec(key), &postcard::to_allocvec(id)?, None, None);
        self.inner
            .db
            .insert(ix.tid, tuple, txn)
            .map_err(|e| self.duplicate(e, &ix.name, values))
    }

    // A store error, as MongoDB reports a duplicate key (E11000).
    fn duplicate(&self, e: StoreError, index: &str, values: &[Value]) -> Error {
        match e {
            StoreError::DuplicateKey(_) => Error::DuplicateKey {
                ns: self.ns.clone(),
                index: index.to_string(),
                key: values.iter().map(Value::to_json).collect::<Vec<_>>().join(", "),
            },
            other => other.into(),
        }
    }
}

// The best way into `state` for `filter`: the seek fixing the most leading
// key fields, then one that also bounds a range; `_id` wins ties.
fn plan(state: &CollState, filter: &Filter) -> Plan {
    let score = |s: &Seek| (s.points, s.ranged);
    let mut best = match seek_for(filter, &["_id"], false) {
        Some(seek) => Plan::Id(seek),
        None => Plan::CollScan,
    };
    for ix in &state.indexes {
        let paths: Vec<&str> = ix.keys.iter().map(|(p, _)| p.as_str()).collect();
        let Some(seek) = seek_for(filter, &paths, ix.multikey.load(Ordering::SeqCst)) else {
            continue;
        };
        let better = match &best {
            Plan::CollScan => true,
            Plan::Id(b) | Plan::Index(_, b) => score(&seek) > score(b),
        };
        if better {
            best = Plan::Index(ix.clone(), seek);
        }
    }
    best
}

fn operators(update: Document) -> Result<Update> {
    let update = Update::parse(&update)?;
    if matches!(update, Update::Replace(_)) {
        return Err(Error::BadValue("an update document needs update operators ($set, ...); use replace_one to replace".into()));
    }
    Ok(update)
}

// Checks a document to be inserted, giving it an `_id` if it has none.
fn prepare_insert(doc: &mut Document) -> Result<()> {
    check_field_names(doc)?;
    match doc.get("_id") {
        None => doc.insert_first("_id", Value::ObjectId(ObjectId::new())),
        Some(Value::Array(_)) => {
            return Err(Error::BadValue("the _id field cannot be an array".into()));
        }
        Some(_) => {
            // `_id` goes first, as MongoDB stores it.
            let id = doc.remove("_id").expect("present");
            doc.insert_first("_id", id);
        }
    }
    Ok(())
}

fn id_key(id: &Value) -> Result<IndexKey> {
    let key = IndexKey::new_from(&encode(id))?;
    check_size(&key)?;
    Ok(key)
}

// An index key and the values it indexes.
type Entry = (IndexKey, Vec<Value>);

// A document's keys in `ix`, each with the values it indexes, and whether
// there is more than one (the index is then multikey).
fn index_keys(ix: &IndexState, doc: &Document) -> Result<(Vec<Entry>, bool)> {
    let per_field: Vec<Vec<Value>> = ix.keys.iter().map(|(p, _)| index_values(doc, p)).collect();
    if per_field.iter().filter(|vs| vs.len() > 1).count() > 1 {
        return Err(Error::BadValue(format!(
            "cannot index parallel arrays: index {} has more than one array field in this document",
            ix.name
        )));
    }
    let mut combos: Vec<Vec<Value>> = vec![vec![]];
    for values in &per_field {
        combos = combos
            .into_iter()
            .flat_map(|c| {
                values.iter().map(move |v| {
                    let mut c = c.clone();
                    c.push(v.clone());
                    c
                })
            })
            .collect();
    }
    let multikey = combos.len() > 1;
    let id = doc.get("_id").map(encode);
    let mut seen = HashSet::new();
    let keys = combos
        .into_iter()
        .filter(|values| seen.insert(postcard::to_allocvec(&values.iter().flat_map(encode).collect::<Vec<_>>()).unwrap_or_default()))
        .map(|values| {
            let mut items: Vec<_> = values.iter().flat_map(encode).collect();
            if !ix.unique && let Some(id) = &id {
                items.extend(id.iter().cloned());
            }
            let key = IndexKey::new_from_owned(items)?;
            check_size(&key)?;
            Ok((key, values))
        })
        .collect::<Result<_>>()?;
    Ok((keys, multikey))
}

// Every value an index on `path` holds for `doc`: each reached value, an
// array's elements separately, and null wherever the path is missing
// (along any branch through an array). Erring toward more keys is safe —
// a seek only narrows what the filter then checks — while a missing key
// would lose a match.
fn index_values(doc: &Document, path: &str) -> Vec<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = vec![];
    match doc.get(parts[0]) {
        Some(v) => gather(v, &parts[1..], &mut out),
        None => out.push(Value::Null),
    }
    out.sort_by(compare);
    out.dedup_by(|a, b| values_equal(a, b));
    out
}

fn gather(v: &Value, parts: &[&str], out: &mut Vec<Value>) {
    let Some(part) = parts.first() else {
        match v {
            Value::Array(items) if !items.is_empty() => out.extend(items.iter().cloned()),
            other => out.push(other.clone()),
        }
        return;
    };
    match v {
        Value::Document(d) => match d.get(part) {
            Some(x) => gather(x, &parts[1..], out),
            None => out.push(Value::Null),
        },
        Value::Array(items) => {
            if let Ok(i) = part.parse::<usize>()
                && let Some(item) = items.get(i)
            {
                gather(item, &parts[1..], out);
            }
            if items.is_empty() {
                out.push(Value::Null);
            }
            for item in items {
                gather(item, parts, out);
            }
        }
        _ => out.push(Value::Null),
    }
}
