//! The entry points: a Client over one store database, its Databases
//! (namespaces of collections), and Sessions for multi-document
//! transactions.
//!
//! Every collection lives in store tables: one for its documents, keyed by
//! `_id`, and one per secondary index. The catalog — which collections
//! exist and their indexes — is itself a store table, read into memory at
//! open. Tables are named `sqjson.<n>` rather than after the collection, so
//! a name's length or characters never matter to the store.
//!
//! Collection and index changes (DDL) take `ddl` exclusively; every other
//! operation holds it shared while it runs, so an index is never built
//! while a write is half done. A write in an open transaction can span many
//! operations, though, so DDL is refused while any transaction is open.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use store::cursor::Cursor;
use store::db::{DBFile, Db};
use store::error::StoreError;
use store::table::TableIdType;
use store::tuple::{DBIdType, Tuple};
use store::txn::Transaction;
use store::valueitem::{IndexKey, ValueItem};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::keys::KEY_BUDGET;

const CATALOG_TABLE: &str = "sqjson.catalog";
const TABLE_NUMBERS: &str = "sqjson.tables";

/// A document database in one store database file.
pub struct Client<F: DBFile<Item = F> + 'static = crate::DefaultFile> {
    pub(crate) inner: Arc<Inner<F>>,
}

pub(crate) struct Inner<F: DBFile<Item = F> + 'static> {
    pub db: Arc<Db<F>>,
    catalog_tid: TableIdType,
    /// Namespace ("db.collection") to its current state.
    collections: Mutex<HashMap<String, Arc<CollState>>>,
    pub ddl: RwLock<()>,
    /// Sessions with a transaction open (see the module comment).
    open_transactions: AtomicUsize,
}

/// A collection as operations see it: replaced whole by DDL.
pub(crate) struct CollState {
    pub ns: String,
    pub table: String,
    pub tid: TableIdType,
    pub indexes: Vec<Arc<IndexState>>,
}

pub(crate) struct IndexState {
    pub name: String,
    pub keys: Vec<(String, i8)>,
    pub unique: bool,
    pub table: String,
    pub tid: TableIdType,
    /// Some document has more than one key in this index (an array in an
    /// indexed field). Set before such keys are written and never cleared.
    pub multikey: AtomicBool,
}

// The persisted form of a collection's catalog entry.
#[derive(Serialize, Deserialize)]
struct CatalogEntry {
    table: String,
    indexes: Vec<IndexEntry>,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    name: String,
    keys: Vec<(String, i8)>,
    unique: bool,
    multikey: bool,
    table: String,
}

impl<F: DBFile<Item = F> + 'static> Client<F> {
    /// Creates a new database file at `path`.
    pub fn create(path: &str) -> Result<Self> {
        Self::start(Db::create(path)?)
    }

    /// Opens an existing database file.
    pub fn open(path: &str) -> Result<Self> {
        Self::start(Db::open(path)?)
    }

    /// Wraps a store database already open (e.g. one on MemFile).
    pub fn start(db: Arc<Db<F>>) -> Result<Self> {
        match db.get_generator().create_generator(TABLE_NUMBERS, Some(1)) {
            Ok(()) | Err(StoreError::DuplicateName(_)) => {}
            Err(e) => return Err(e.into()),
        }
        let catalog_tid = match db.table_id_by_name(CATALOG_TABLE)? {
            Some(tid) => tid,
            None => db.create_table_with_index_entry_size(CATALOG_TABLE.into(), entry_size())?,
        };
        let mut collections = HashMap::new();
        let txn = db.begin()?;
        let mut cursor = db.table_scan_in_txn(catalog_tid, &txn)?;
        while let Some(tuple) = cursor.next()? {
            let ns = catalog_name(&tuple)?;
            let entry: CatalogEntry = postcard::from_bytes(tuple.data())?;
            collections.insert(ns.clone(), Arc::new(load_state(&db, ns, entry)?));
        }
        drop(cursor);
        db.rollback(txn)?;
        Ok(Client {
            inner: Arc::new(Inner {
                db,
                catalog_tid,
                collections: Mutex::new(collections),
                ddl: RwLock::new(()),
                open_transactions: AtomicUsize::new(0),
            }),
        })
    }

    pub fn database(&self, name: &str) -> Database<F> {
        Database {
            inner: self.inner.clone(),
            name: name.to_string(),
        }
    }

    /// Databases holding at least one collection.
    pub fn list_database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .collections
            .lock()
            .keys()
            .filter_map(|ns| ns.split_once('.').map(|(db, _)| db.to_string()))
            .collect();
        names.sort();
        names.dedup();
        names
    }

    pub fn start_session(&self) -> Session<F> {
        Session(Arc::new(SessionInner {
            inner: self.inner.clone(),
            txn: Mutex::new(None),
        }))
    }

    /// Closes the database. Every Database, Collection and Session from
    /// this client must have been dropped.
    pub fn close(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| {
            Error::BadValue("close: databases, collections or sessions are still in use".into())
        })?;
        inner.db.close()?;
        Ok(())
    }

    /// The underlying store database.
    pub fn store(&self) -> &Arc<Db<F>> {
        &self.inner.db
    }
}

/// A namespace of collections.
pub struct Database<F: DBFile<Item = F> + 'static = crate::DefaultFile> {
    inner: Arc<Inner<F>>,
    name: String,
}

impl<F: DBFile<Item = F> + 'static> Database<F> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A handle to a collection; it is created by the first write (or
    /// create_index) if it doesn't exist.
    pub fn collection(&self, name: &str) -> Collection<F> {
        Collection::new(self.inner.clone(), &self.name, name)
    }

    pub fn list_collection_names(&self) -> Vec<String> {
        let prefix = format!("{}.", self.name);
        let mut names: Vec<String> = self
            .inner
            .collections
            .lock()
            .keys()
            .filter_map(|ns| ns.strip_prefix(&prefix).map(str::to_string))
            .collect();
        names.sort();
        names
    }

    /// Drops every collection in this database.
    pub fn drop(&self) -> Result<()> {
        for name in self.list_collection_names() {
            self.collection(&name).drop()?;
        }
        Ok(())
    }
}

impl<F: DBFile<Item = F> + 'static> Inner<F> {
    pub fn state(&self, ns: &str) -> Option<Arc<CollState>> {
        self.collections.lock().get(ns).cloned()
    }

    /// Fails when a transaction is open (see the module comment); call
    /// holding `ddl` exclusively.
    pub fn check_no_transactions(&self, what: &str) -> Result<()> {
        if self.open_transactions.load(Ordering::SeqCst) > 0 {
            return Err(Error::Transaction(format!(
                "cannot {what} while a transaction is in progress"
            )));
        }
        Ok(())
    }

    pub fn new_table(&self) -> Result<(String, TableIdType)> {
        let n = self.db.get_generator().gen_key(TABLE_NUMBERS)?;
        let name = format!("sqjson.{n}");
        let tid = self
            .db
            .create_table_with_index_entry_size(name.clone(), entry_size())?;
        Ok((name, tid))
    }

    /// Creates the collection `ns`; call holding `ddl` exclusively.
    pub fn create_collection(&self, ns: &str) -> Result<Arc<CollState>> {
        if let Some(state) = self.state(ns) {
            return Ok(state);
        }
        let (table, tid) = self.new_table()?;
        let state = Arc::new(CollState {
            ns: ns.to_string(),
            table,
            tid,
            indexes: vec![],
        });
        self.publish(state.clone())?;
        Ok(state)
    }

    /// Records `state` as collection `state.ns`, in the catalog table and
    /// in memory.
    pub fn publish(&self, state: Arc<CollState>) -> Result<()> {
        let mut collections = self.collections.lock();
        let entry = CatalogEntry {
            table: state.table.clone(),
            indexes: state
                .indexes
                .iter()
                .map(|ix| IndexEntry {
                    name: ix.name.clone(),
                    keys: ix.keys.clone(),
                    unique: ix.unique,
                    multikey: ix.multikey.load(Ordering::SeqCst),
                    table: ix.table.clone(),
                })
                .collect(),
        };
        let tuple = Tuple::new_with(catalog_key(&state.ns)?, &postcard::to_allocvec(&entry)?, None, None);
        let txn = self.db.begin()?;
        if collections.contains_key(&state.ns) {
            self.db.update(self.catalog_tid, tuple, &txn)?;
        } else {
            self.db.insert(self.catalog_tid, tuple, &txn)?;
        }
        self.db.commit(txn)?;
        collections.insert(state.ns.clone(), state);
        Ok(())
    }

    /// Removes collection `ns` from the catalog (its tables are the
    /// caller's to drop).
    pub fn unpublish(&self, ns: &str) -> Result<()> {
        let mut collections = self.collections.lock();
        let txn = self.db.begin()?;
        self.db.remove(self.catalog_tid, catalog_key(ns)?, &txn)?;
        self.db.commit(txn)?;
        collections.remove(ns);
        Ok(())
    }

    /// Marks an index multikey, durably, before any multikey entry is
    /// written. (If that write's transaction then aborts, the flag stays:
    /// it only makes plans more cautious.)
    pub fn set_multikey(&self, ns: &str, index: &IndexState) -> Result<()> {
        if index.multikey.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        match self.state(ns) {
            Some(state) => self.publish(state),
            None => Ok(()),
        }
    }
}

fn entry_size() -> u64 {
    (KEY_BUDGET + 64) as _
}

fn catalog_key(ns: &str) -> Result<DBIdType> {
    Ok(DBIdType::Rec(IndexKey::new_from(&[ValueItem::Str((
        ns.to_string(),
        ns.len() as u32,
    ))])?))
}

fn catalog_name(tuple: &Tuple) -> Result<String> {
    match tuple.id() {
        DBIdType::Rec(key) => match key.values().first() {
            Some(ValueItem::Str((s, _))) => Ok(s.clone()),
            _ => Err(Error::BadValue("corrupt catalog entry".into())),
        },
        _ => Err(Error::BadValue("corrupt catalog entry".into())),
    }
}

fn load_state<F: DBFile<Item = F> + 'static>(
    db: &Arc<Db<F>>,
    ns: String,
    entry: CatalogEntry,
) -> Result<CollState> {
    let tid = |name: &str| {
        db.table_id_by_name(name)?
            .ok_or_else(|| Error::BadValue(format!("catalog names a missing table {name}")))
    };
    Ok(CollState {
        tid: tid(&entry.table)?,
        indexes: entry
            .indexes
            .into_iter()
            .map(|ix| {
                Ok(Arc::new(IndexState {
                    tid: tid(&ix.table)?,
                    name: ix.name,
                    keys: ix.keys,
                    unique: ix.unique,
                    table: ix.table,
                    multikey: AtomicBool::new(ix.multikey),
                }))
            })
            .collect::<Result<_>>()?,
        table: entry.table,
        ns,
    })
}

/// A session: operations run through it (Collection::with_session) join
/// its transaction while one is open, and commit one by one otherwise.
///
/// As in MongoDB, an operation that fails inside a transaction aborts the
/// whole transaction.
pub struct Session<F: DBFile<Item = F> + 'static = crate::DefaultFile>(pub(crate) Arc<SessionInner<F>>);

impl<F: DBFile<Item = F> + 'static> Clone for Session<F> {
    fn clone(&self) -> Self {
        Session(self.0.clone())
    }
}

pub(crate) struct SessionInner<F: DBFile<Item = F> + 'static> {
    pub inner: Arc<Inner<F>>,
    pub txn: Mutex<Option<Transaction>>,
}

impl<F: DBFile<Item = F> + 'static> Session<F> {
    pub fn start_transaction(&self) -> Result<()> {
        let mut txn = self.0.txn.lock();
        if txn.is_some() {
            return Err(Error::Transaction("a transaction is already in progress".into()));
        }
        *txn = Some(self.0.inner.db.begin()?);
        self.0.inner.open_transactions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<()> {
        let txn = self.take()?;
        self.0.inner.db.commit(txn)?;
        Ok(())
    }

    pub fn abort_transaction(&self) -> Result<()> {
        let txn = self.take()?;
        self.0.inner.db.rollback(txn)?;
        Ok(())
    }

    pub fn in_transaction(&self) -> bool {
        self.0.txn.lock().is_some()
    }

    fn take(&self) -> Result<Transaction> {
        let txn = self
            .0
            .txn
            .lock()
            .take()
            .ok_or_else(|| Error::Transaction("no transaction in progress".into()))?;
        self.0.inner.open_transactions.fetch_sub(1, Ordering::SeqCst);
        Ok(txn)
    }
}

impl<F: DBFile<Item = F> + 'static> SessionInner<F> {
    /// Ends the open transaction after a failed operation.
    pub fn abort_after_error(&self, txn: &mut Option<Transaction>) {
        if let Some(t) = txn.take() {
            self.inner.open_transactions.fetch_sub(1, Ordering::SeqCst);
            let _ = self.inner.db.rollback(t);
        }
    }
}

impl<F: DBFile<Item = F> + 'static> Drop for SessionInner<F> {
    fn drop(&mut self) {
        let mut txn = self.txn.lock();
        self.abort_after_error(&mut txn);
    }
}
