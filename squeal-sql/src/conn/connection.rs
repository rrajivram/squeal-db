use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    fs::File,
    hash::Hash,
    sync::{Arc, LazyLock},
};

use parking_lot::RwLock;
use store::{db::DBFile, txn::TransactionId};

use crate::{error::SchemaError, schema_ops::schema::Schema, stmt::Statement};

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

pub struct ConnectionManager<F: DBFile + 'static> {
    active_conns: RwLock<HashSet<Arc<Connection<F>>>>,
    open_dbs: RwLock<HashMap<String, Arc<Schema<F>>>>,
}

pub struct Connection<F: DBFile + 'static> {
    open_dbs: RwLock<HashMap<String, ConnectedSchema<F>>>,
    mgr: ConMgr<F>,
    default_schema: RwLock<Option<String>>,
}

struct ConnectedSchema<F: DBFile + 'static> {
    name: String,
    current_txn: Option<TransactionId>,
    schema: Arc<Schema<F>>,
}

impl<F> ConnectionManager<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new() -> Self {
        Self {
            active_conns: RwLock::new(HashSet::new()),
            open_dbs: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_connection(self: Arc<Self>) -> Arc<Connection<F>> {
        let conn = Arc::new(Connection::new(self.clone()));
        self.active_conns.write().insert(conn.clone());
        conn
    }

    #[allow(clippy::ptr_arg)]
    fn use_schema(self: &Arc<Self>, name: &String) -> Result<Arc<Schema<F>>, SchemaError> {
        let mut dbs = self.open_dbs.write();

        let schema = dbs
            .entry(name.clone())
            .or_insert(Schema::<F>::open(name.clone())?);
        Ok(schema.clone())
    }

    fn create_schema(self: &Arc<Self>, name: &String) -> Result<Arc<Schema<F>>, SchemaError> {
        if self.open_dbs.read().contains_key(name) {
            return Err(SchemaError::SchemaInUseError(name.clone()));
        }
        let schema = Schema::<F>::create_database(name.clone())?;
        self.open_dbs.write().insert(name.clone(), schema.clone());
        Ok(schema)
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
    fn new(mgr: ConMgr<F>) -> Self {
        Self {
            open_dbs: RwLock::new(HashMap::new()),
            mgr,
            default_schema: RwLock::new(None),
        }
    }

    pub fn use_schema(self: Arc<Self>, name: &String) -> Result<(), SchemaError> {
        if self.open_dbs.read().contains_key(name) {
            return Err(SchemaError::SchemaAlreadyInUseError(name.clone()));
        }
        let schema = self.mgr.use_schema(name)?;
        self.open_dbs
            .write()
            .insert(name.clone(), ConnectedSchema::new(name, schema));
        self.default_schema.write().replace(name.clone());
        Ok(())
    }

    pub fn create_schema(self: Arc<Self>, name: &String) -> Result<(), SchemaError> {
        let schema = self.mgr.create_schema(name)?;
        self.open_dbs
            .write()
            .insert(name.clone(), ConnectedSchema::new(name, schema));
        self.default_schema.write().replace(name.clone());
        Ok(())
    }

    pub fn create_statement(self: Arc<Self>, sql: &str) -> Result<Statement<F>, SchemaError> {
        Statement::new(sql, self.clone())
    }
}

impl<F> ConnectedSchema<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new(name: &str, schema: Arc<Schema<F>>) -> Self {
        Self {
            name: name.to_string(),
            current_txn: None,
            schema,
        }
    }
}

impl<F> Display for Connection<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Open dbs in connection:")?;
        for a in self.open_dbs.read().keys() {
            writeln!(f, "{}", a)?;
        }
        Ok(())
    }
}

impl<F> Hash for Connection<F>
where
    F: DBFile + 'static,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for d in self.open_dbs.read().keys() {
            d.hash(state);
        }
    }
}

impl<F> PartialEq for Connection<F>
where
    F: DBFile + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.open_dbs.read().keys().eq(other.open_dbs.read().keys())
    }
}

impl<F> PartialOrd for Connection<F>
where
    F: DBFile + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<F> Eq for Connection<F> where F: DBFile + 'static {}

impl<F> Ord for Connection<F>
where
    F: DBFile + 'static,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.open_dbs
            .read()
            .keys()
            .cmp(other.open_dbs.read().keys())
    }
}

impl<F> Clone for Connection<F>
where
    F: DBFile + 'static,
{
    fn clone(&self) -> Self {
        Self {
            open_dbs: RwLock::new(self.open_dbs.read().clone()),
            mgr: self.mgr.clone(),
            default_schema: RwLock::new(self.default_schema.read().clone()),
        }
    }
}

impl<F> Clone for ConnectedSchema<F>
where
    F: DBFile + 'static,
{
    fn clone(&self) -> Self {
        Self {
            current_txn: self.current_txn.clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
        }
    }
}
