use std::{
    collections::HashMap,
    collections::HashSet,
    fs::File,
    sync::{Arc, LazyLock},
};

use parking_lot::RwLock;
use store::{db::DBFile, txn::Transaction};
use uuid::Uuid;

use crate::{error::SchemaError, schema_ops::database::Database, schema_ops::schema::Schema, stmt::Statement};

#[cfg(test)]
mod tests;

// ConnectionManager itself stays generic over the storage backend (so
// tests can build one directly over MemFile — cheap, isolated, no real
// filesystem I/O). The process-wide *singleton* below is pinned to the
// one concrete backend real usage needs (File): a `static` can't be
// generic (Rust requires one fixed, concrete type per static item), but
// `Arc<ConnectionManager<File>>` is itself a concrete type, so fixing F
// here — rather than dropping ConnectionManager's own genericity — is
// enough to satisfy that.
static CON_MANAGER: LazyLock<ConMgr<File>> = LazyLock::new(|| Arc::new(ConnectionManager::new()));

pub type ConMgr<F> = Arc<ConnectionManager<F>>;

// A Connection is 1-1 with a Database (not, as before, with each Schema
// individually) — Database itself now hosts multiple Schemas, so a
// Connection only ever needs to track which one it's currently pointed
// at, not a whole map of separately-opened "schemas".
pub struct ConnectionManager<F: DBFile + 'static> {
    active_conns: RwLock<HashSet<Arc<Connection<F>>>>,
    open_databases: RwLock<HashMap<String, Arc<Database<F>>>>,
}

pub struct Connection<F: DBFile + 'static> {
    id: Uuid,
    mgr: ConMgr<F>,
    // A RwLock, not a plain field: CREATE DATABASE/USE DATABASE (see
    // Statement::execute) can repoint an *existing* connection at a
    // different database, unlike before where a Connection's database
    // was fixed at construction.
    database: RwLock<Arc<Database<F>>>,
    current_schema: RwLock<Option<Arc<Schema<F>>>>,
    // Set by BEGIN/START TRANSACTION, cleared by COMMIT/ROLLBACK (see
    // Statement::execute's handling of those). While set, statements
    // that support it (currently just INSERT — see
    // Schema::insert_rows' own `txn` parameter) run against this shared
    // transaction instead of opening and auto-committing their own.
    // DDL (CREATE TABLE/DATABASE/SCHEMA) and USE always still manage
    // their own transaction regardless of this — deliberately out of
    // scope for this first pass.
    current_txn: RwLock<Option<Transaction>>,
}

impl<F> ConnectionManager<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // Public (not just pub(crate)) so callers outside this crate can
    // build a manager over a backend other than the File-pinned process
    // singleton (see ConnectionManager::<File>::get_manager) — e.g.
    // squeal-cli building a NamedMemFile-backed manager for an
    // in-memory session.
    pub fn new() -> Self {
        Self {
            active_conns: RwLock::new(HashSet::new()),
            open_databases: RwLock::new(HashMap::new()),
        }
    }

    // Opens (or reuses an already-open) database and hands back a new
    // connection to it.
    pub fn connect(self: &Arc<Self>, db_name: &str) -> Result<Arc<Connection<F>>, SchemaError> {
        let database = self.open_or_get_database(db_name)?;
        Ok(self.new_connection(database))
    }

    // Creates a brand-new database and hands back a connection to it.
    pub fn create_and_connect(
        self: &Arc<Self>,
        db_name: &str,
    ) -> Result<Arc<Connection<F>>, SchemaError> {
        let database = self.create_database(db_name)?;
        Ok(self.new_connection(database))
    }

    fn new_connection(self: &Arc<Self>, database: Arc<Database<F>>) -> Arc<Connection<F>> {
        let conn = Arc::new(Connection::new(self.clone(), database));
        self.active_conns.write().insert(conn.clone());
        conn
    }

    fn open_or_get_database(&self, name: &str) -> Result<Arc<Database<F>>, SchemaError> {
        if let Some(db) = self.open_databases.read().get(name) {
            return Ok(db.clone());
        }
        let db = Database::<F>::open(name.to_string())?;
        self.open_databases
            .write()
            .insert(name.to_string(), db.clone());
        Ok(db)
    }

    fn create_database(&self, name: &str) -> Result<Arc<Database<F>>, SchemaError> {
        if self.open_databases.read().contains_key(name) {
            return Err(SchemaError::DatabaseInUseError(name.to_string()));
        }
        let db = Database::<F>::create(name.to_string())?;
        self.open_databases
            .write()
            .insert(name.to_string(), db.clone());
        Ok(db)
    }
}

impl<F> Default for ConnectionManager<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager<File> {
    pub fn get_manager() -> ConMgr<File> {
        CON_MANAGER.clone()
    }
}

impl<F> Connection<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new(mgr: ConMgr<F>, database: Arc<Database<F>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            mgr,
            database: RwLock::new(database),
            current_schema: RwLock::new(None),
            current_txn: RwLock::new(None),
        }
    }

    pub(crate) fn current_schema(&self) -> Option<Arc<Schema<F>>> {
        self.current_schema.read().clone()
    }

    pub(crate) fn database_name(&self) -> String {
        self.database.read().name().to_string()
    }

    pub fn use_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let schema = self.database.read().get_schema(name)?;
        self.current_schema.write().replace(schema);
        Ok(())
    }

    pub fn create_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let schema = self.database.read().create_schema(name)?;
        self.current_schema.write().replace(schema);
        Ok(())
    }

    // Repoints this connection at a different (already-open, or newly
    // opened) database — the current schema selection belonged to the
    // old database, so it's cleared, not carried over.
    pub(crate) fn use_database(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let database = self.mgr.open_or_get_database(name)?;
        *self.database.write() = database;
        self.current_schema.write().take();
        Ok(())
    }

    // Like use_database, but creates a brand-new database rather than
    // opening an existing one.
    pub(crate) fn create_database(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let database = self.mgr.create_database(name)?;
        *self.database.write() = database;
        self.current_schema.write().take();
        Ok(())
    }

    pub fn create_statement(self: Arc<Self>, sql: &str) -> Result<Statement<F>, SchemaError> {
        Statement::new(sql, self.clone())
    }

    pub(crate) fn begin_transaction(self: &Arc<Self>) -> Result<(), SchemaError> {
        let mut slot = self.current_txn.write();
        if slot.is_some() {
            return Err(SchemaError::TransactionAlreadyActive);
        }
        *slot = Some(self.database.read().begin()?);
        Ok(())
    }

    pub(crate) fn commit_transaction(self: &Arc<Self>) -> Result<(), SchemaError> {
        let txn = self
            .current_txn
            .write()
            .take()
            .ok_or(SchemaError::NoActiveTransaction)?;
        self.database.read().commit(txn)
    }

    pub(crate) fn rollback_transaction(self: &Arc<Self>) -> Result<(), SchemaError> {
        let txn = self
            .current_txn
            .write()
            .take()
            .ok_or(SchemaError::NoActiveTransaction)?;
        self.database.read().rollback(txn)
    }

    // Runs `f` with a reference to the currently-open explicit
    // transaction, if any — None in autocommit mode (no BEGIN yet
    // issued), in which case callers fall back to managing their own
    // transaction, same as before explicit transactions existed.
    pub(crate) fn with_current_txn<R>(&self, f: impl FnOnce(Option<&Transaction>) -> R) -> R {
        let guard = self.current_txn.read();
        f(guard.as_ref())
    }
}

// store::Db (via Database) doesn't implement Debug, so this can't be
// derived — a minimal manual impl (id + database name) is enough for
// {:?} logging and for Result<Arc<Connection<F>>, _>::unwrap_err() in
// tests.
impl<F> std::fmt::Debug for Connection<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("database", &self.database.read().name())
            .finish_non_exhaustive()
    }
}

impl<F> std::fmt::Display for Connection<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connection {} (database: ", self.id)?;
        write!(f, "{})", self.database.read().name())
    }
}

impl<F> std::hash::Hash for Connection<F>
where
    F: DBFile + 'static,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<F> PartialEq for Connection<F>
where
    F: DBFile + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<F> Eq for Connection<F> where F: DBFile + 'static {}
