use std::{
    collections::HashMap,
    collections::HashSet,
    fs::File,
    sync::{Arc, LazyLock},
};

use parking_lot::RwLock;
use store::{db::DBFile, txn::Transaction};
use uuid::Uuid;

use crate::{
    error::SchemaError,
    plan::logical::HasFields,
    schema_ops::{database::Database, schema::Schema},
    stmt::{PreparedStatement, Statement},
    table::SqlTable,
    temp::{TEMP_SCHEMA_NAME, TempTable, TempTables},
};

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
    pub(crate) database: RwLock<Arc<Database<F>>>,
    current_schema: RwLock<Option<Arc<Schema<F>>>>,
    // Set by BEGIN/START TRANSACTION, cleared by COMMIT/ROLLBACK (see
    // Statement::execute's handling of those). While set, statements
    // that support it (currently just INSERT — see
    // Schema::insert_rows' own `txn` parameter) run against this shared
    // transaction instead of opening and auto-committing their own.
    // DDL (CREATE TABLE/DATABASE/SCHEMA) and USE always still manage
    // their own transaction regardless of this — deliberately out of
    // scope for this first pass.
    pub(crate) current_txn: RwLock<Option<Transaction>>,
    // Connection-scoped temporary tables, addressed as `temp.<table>` —
    // see crate::temp's own doc comment for why this lives here rather
    // than as a real Schema. Cleared by use_database/create_database
    // (see their own comments): a temp table's Run holds pages from a
    // specific database's PageBuffer, so it can't survive this
    // connection repointing at a different one.
    temp_tables: TempTables<F>,
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

// What a FROM-clause-style reference resolves to — the one shared
// answer for every statement kind that needs to turn a parsed table
// name into something to act on (INSERT, ALTER TABLE, COPY INTO,
// SELECT's own table resolution in plan::logical), instead of each
// reimplementing the same schema-vs-temp classification (see
// Connection::resolve_table_ref's own doc comment for why that used to
// drift).
//
// Real carries the already-resolved Arc<SqlTable> (not just its name) —
// resolve_table_ref does that lookup itself, once, rather than every
// caller (INSERT and plan::logical::QueryVisitor::validate_table both
// used to) re-deriving the same "look it up, error if missing" step
// immediately after getting a schema+name pair back. The Schema handle
// is kept alongside it since the schema-level operations (insert_rows,
// add_column, copy_csv_into, ...) still live on Schema, not SqlTable.
//
// Derived (a FROM-clause subquery, `(SELECT ...) AS x` — carries
// nothing yet, see plan::logical's own TODO on actually planning one)
// lives here too, not as a separate enum one layer up: it's another
// answer to the exact same question ("what is this FROM item"), and
// keeping it here means a future non-subquery producer of the same
// shape — a VIEW, most plausibly, a stored query resolved by name the
// same way a real table is — has one obvious place to plug into instead
// of two enums to keep in sync.

pub(crate) enum TableRef<F: DBFile + 'static> {
    Real(Arc<Schema<F>>, Arc<SqlTable>),
    // Carries the (lowercased) name alongside the handle, not just the
    // handle — callers still want it for error messages/further lookups
    // the way TableRef::Real's own Arc<SqlTable> carries its name via
    // `.name`, and a TempTable has no separate "schema" object to get
    // one from otherwise.
    Temp(String, Arc<RwLock<TempTable<F>>>),
    Derived,
}

// TempTable (via store::run::Run) doesn't implement Debug, so this can't
// be derived — a minimal manual impl (variant + the name each carries)
// is enough for {:?} logging and for
// Result<(TableRef<F>, _), _>::unwrap_err() in tests.
impl<F: DBFile + 'static> std::fmt::Debug for TableRef<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableRef::Real(schema, table) => f
                .debug_tuple("Real")
                .field(&schema.name)
                .field(&table.name)
                .finish(),
            TableRef::Temp(name, _) => f.debug_tuple("Temp").field(name).finish(),
            TableRef::Derived => f.debug_tuple("Derived").finish(),
        }
    }
}

impl<F: DBFile + 'static> Clone for TableRef<F> {
    fn clone(&self) -> Self {
        match self {
            Self::Real(s, t) => Self::Real(s.clone(), t.clone()),
            Self::Temp(s, t) => Self::Temp(s.clone(), t.clone()),
            Self::Derived => Self::Derived,
        }
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
            temp_tables: TempTables::new(),
        }
    }

    pub(crate) fn temp_tables(&self) -> &TempTables<F> {
        &self.temp_tables
    }

    pub(crate) fn current_schema(&self) -> Option<Arc<Schema<F>>> {
        self.current_schema.read().clone()
    }

    // Looks up any named schema in this connection's current database —
    // not necessarily the current one, and doesn't change it (unlike
    // use_schema). For resolving a schema-qualified table reference
    // (`schema.table`) without a USE SCHEMA first.
    pub(crate) fn schema(&self, name: &str) -> Result<Arc<Schema<F>>, SchemaError> {
        self.database.read().get_schema(name)
    }

    // Resolves a possibly-qualified table (or table-plus-field) reference
    // — `table`, `schema.table`, `schema.table.field`, `temp.table`, or
    // `temp.table.field` — to either the Schema a table lives in (plus
    // its own unqualified, lowercased name) or a connection-scoped temp
    // table handle, alongside the trailing field name if the reference
    // carried one (see the shapes documented on
    // plan::logical::QueryVisitor::validate_object_name, which this is
    // the shared building block for — that function still owns the
    // shapes this can't disambiguate alone, `table.field` and a bare
    // `field`, since telling those apart from `schema.table` and a bare
    // `table` needs to know what tables are already in scope, which this
    // method — unlike validate_object_name — doesn't take).
    //
    // The bare (one-part) form resolves against this connection's
    // current schema; the two- and three-part forms look up that schema
    // by name (see `schema`, above) unless the first part is the
    // reserved `temp` name, in which case it's routed to this
    // connection's own temp-table registry instead. More than three
    // parts is always rejected — nothing in this engine lets a single
    // statement reach across databases.
    //
    // The one place every table-reference-resolving statement (INSERT,
    // ALTER TABLE, COPY INTO, SELECT's own resolution in
    // plan::logical::QueryVisitor) goes through — previously each
    // reimplemented this same schema-vs-temp-vs-too-many-parts
    // classification separately, which had already drifted once (SELECT
    // rejected a 3-part name with a different SchemaError variant than
    // everything else did for the identical situation). Those four
    // callers only ever pass a pure table reference (no trailing field)
    // and reject a `Some` field themselves — a `schema.table.field`-
    // shaped INSERT/ALTER TABLE/COPY INTO/FROM target doesn't mean
    // anything, so it's on each of them to say so, not on this method to
    // guess who's calling.
    pub(crate) fn resolve_object_name_ref(
        self: &Arc<Self>,
        name: &sql_parser::ObjectName,
    ) -> Result<(TableRef<F>, Option<String>), SchemaError> {
        let parts: Vec<&str> = name.idents().map(|i| i.value.as_str()).collect();
        let (head, field) = match parts.as_slice() {
            [_, _, field] => (&parts[..2], Some(field.to_lowercase())),
            _ => (parts.as_slice(), None),
        };
        if let Some(temp_name) = crate::temp::temp_table_name(head) {
            let table = self
                .temp_tables
                .get(&temp_name)
                .ok_or_else(|| SchemaError::BadTableName(format!("temp.{temp_name}")))?;
            if let Some(field) = &field
                && !table.has_field(field)
            {
                return Err(SchemaError::FieldNotFound(field.clone()));
            }
            return Ok((TableRef::Temp(temp_name, table), field));
        }
        let (schema, table_name) = match head {
            [table] => (
                self.current_schema().ok_or(SchemaError::NoSchemaSelected)?,
                table.to_lowercase(),
            ),
            [schema, table] => (self.schema(schema)?, table.to_lowercase()),
            _ => {
                return Err(SchemaError::UserError(format!(
                    "{:?} has too many parts — only <table>, <schema>.<table>, \
                     <schema>.<table>.<field>, or temp.<table>[.<field>] is supported",
                    name.to_dotted()
                )));
            }
        };
        let table = schema
            .get_table(&table_name)
            .ok_or(SchemaError::BadTableName(table_name.to_string()))?;
        if let Some(field) = &field
            && !table.has_field(field)
        {
            return Err(SchemaError::FieldNotFound(field.clone()));
        }
        Ok((TableRef::Real(schema, table), field))
    }

    #[allow(dead_code)]
    pub(crate) fn database_name(&self) -> String {
        self.database.read().name().to_string()
    }

    pub fn use_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        reject_temp_schema_name(name)?;
        let schema = self.database.read().get_schema(name)?;
        self.current_schema.write().replace(schema);
        Ok(())
    }

    pub fn create_schema(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        reject_temp_schema_name(name)?;
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
        self.temp_tables.clear();
        Ok(())
    }

    // Like use_database, but creates a brand-new database rather than
    // opening an existing one.
    pub(crate) fn create_database(self: &Arc<Self>, name: &str) -> Result<(), SchemaError> {
        let database = self.mgr.create_database(name)?;
        *self.database.write() = database;
        self.current_schema.write().take();
        self.temp_tables.clear();
        Ok(())
    }

    pub fn create_statement(self: Arc<Self>, sql: &str) -> Result<Statement<F>, SchemaError> {
        Statement::new(sql, self.clone())
    }

    pub fn list_schemas(self: &Arc<Self>) -> Result<Vec<String>, SchemaError> {
        self.database.read().list_schemas()
    }

    // A single INSERT/UPDATE/DELETE/SELECT statement, "?"-parameterized
    // and re-executable with different bound values (see
    // PreparedStatement::set_field/execute) instead of being re-parsed
    // from scratch every time — only INSERT is actually runnable right
    // now (see PreparedStatement::execute's own doc comment).
    pub fn create_prepared_statement(
        self: Arc<Self>,
        sql: &str,
    ) -> Result<PreparedStatement<F>, SchemaError> {
        PreparedStatement::new(sql, self.clone())
    }

    // Cleanly shuts down the database this connection is pointed at —
    // flushes every loaded schema's metadata and truncates the WAL (see
    // Database::close), and removes both this connection and the
    // database from the owning ConnectionManager's registries so a
    // later connect() to the same name reopens fresh rather than
    // reusing one that's already shut down. Consumes `self`, matching
    // Database::close's own "closing takes ownership" shape.
    //
    // Fails (via Database::close's own error) if any OTHER connection
    // still references the same database: closing out from under a
    // connection someone else is still using would be silently
    // destructive, not a case this should paper over.
    pub fn close(self: Arc<Self>) -> Result<(), SchemaError> {
        let mgr = self.mgr.clone();
        mgr.active_conns.write().remove(&self);
        let db_name = self.database.read().name().to_string();
        // Drop this connection's own Arc<Database> clone (held in its
        // `database` field) before checking the reference count below —
        // Database::close needs unique ownership, so every other
        // reference has to be gone first.
        drop(self);

        // Held across the strong-count check and the possible reinsert
        // below, not just the remove: without it, a concurrent connect()
        // to the same name could interleave between them and either see
        // the database briefly missing or race the reinsert.
        let mut open_databases = mgr.open_databases.write();
        let database = open_databases.remove(&db_name).ok_or_else(|| {
            SchemaError::UnknownError(format!("database {db_name:?} is not open"))
        })?;
        // Database::close's own Arc::try_unwrap doesn't hand the Arc
        // back on failure — it just drops the failed clone and reports
        // an error, which would leave `database` gone from this map
        // even though some other connection still legitimately has it
        // open. Checked here instead, before ever calling close, so a
        // failure is a clean no-op: the registry never disagrees with
        // reality.
        if Arc::strong_count(&database) > 1 {
            open_databases.insert(db_name, database);
            return Err(SchemaError::UnknownError(
                "cannot close: another connection still has this database open".into(),
            ));
        }
        drop(open_databases);
        database.close()?;
        Ok(())
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

// `temp` is reserved for connection-scoped temporary tables (see
// crate::temp's own doc comment) — it's never a real Schema, so both
// USE SCHEMA temp and CREATE SCHEMA temp are rejected outright rather
// than either silently doing nothing useful or (for CREATE) colliding
// with the reserved name.
fn reject_temp_schema_name(name: &str) -> Result<(), SchemaError> {
    if name.eq_ignore_ascii_case(TEMP_SCHEMA_NAME) {
        return Err(SchemaError::UserError(format!(
            "{TEMP_SCHEMA_NAME:?} is reserved for temporary tables (temp.<table>) and cannot be used as a real schema"
        )));
    }
    Ok(())
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
