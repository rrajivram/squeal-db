use std::{
    collections::HashMap,
    collections::HashSet,
    fs::File,
    sync::{Arc, LazyLock},
};

use parking_lot::RwLock;
use store::{db::DBFile, txn::TransactionId};
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
    #[allow(dead_code)]
    mgr: ConMgr<F>,
    database: Arc<Database<F>>,
    current_schema: RwLock<Option<Arc<Schema<F>>>>,
    // Not yet wired to anything (no transaction support lands until
    // row-level statement execution does) — carried forward from the
    // old per-schema ConnectedSchema so it doesn't need re-adding later.
    current_txn: RwLock<Option<TransactionId>>,
}

impl<F> ConnectionManager<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new() -> Self {
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
            database,
            current_schema: RwLock::new(None),
            current_txn: RwLock::new(None),
        }
    }

    pub fn use_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let schema = self.database.get_schema(name)?;
        self.current_schema.write().replace(schema);
        Ok(())
    }

    pub fn create_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let schema = self.database.create_schema(name)?;
        self.current_schema.write().replace(schema);
        Ok(())
    }

    pub fn create_statement(self: Arc<Self>, sql: &str) -> Result<Statement<F>, SchemaError> {
        Statement::new(sql, self.clone())
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
            .field("database", &self.database.name())
            .finish_non_exhaustive()
    }
}

impl<F> std::fmt::Display for Connection<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connection {} (database: ", self.id)?;
        write!(f, "{})", self.database.name())
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
