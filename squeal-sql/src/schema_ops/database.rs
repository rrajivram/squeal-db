use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::to_allocvec;
use store::{
    db::{DBFile, Db},
    generator::Generator,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    constant::{DEFAULT_SCHEMA_NAME, MAX_TABLE_NAME_LEN, SYSTEM_SCHEMAS_TABLE},
    error::SchemaError,
    schema_ops::schema::Schema,
};

// One Database wraps exactly one store-level Db<F> and hosts multiple
// Schemas — replaces the old model where Schema itself owned a Db<F>
// (a hidden 1-schema-per-database assumption). Every Schema this
// Database creates/loads shares this same `db`, so store-level table
// names have to be schema-qualified (see Schema::qualify) to avoid two
// schemas' same-named tables colliding in the shared, flat namespace.
pub struct Database<F: DBFile> {
    name: String,
    db: Arc<Db<F>>,
    generator: Arc<Generator>,
    schemas_table: TableIdType,
    schemas: RwLock<HashMap<String, Arc<Schema<F>>>>,
}

#[cfg(test)]
mod tests;

impl<F: DBFile> Database<F> {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

// store::Db doesn't implement Debug, so this can't be derived — a
// minimal manual impl (name only) is enough for {:?} logging and for
// Result<Arc<Database<F>>, _>::unwrap_err() in tests.
impl<F: DBFile> std::fmt::Debug for Database<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<F> Database<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub fn create(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::create(&name)?;
        let schemas_table = db.create_table(SYSTEM_SCHEMAS_TABLE.into())?;
        let database = Arc::new(Self {
            name,
            generator: db.get_generator(),
            db,
            schemas_table,
            schemas: RwLock::new(HashMap::new()),
        });
        database.create_schema(DEFAULT_SCHEMA_NAME)?;
        Ok(database)
    }

    pub fn open(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::open(&name)?;
        let schemas_table = db.table_id_by_name(SYSTEM_SCHEMAS_TABLE)?.ok_or_else(|| {
            SchemaError::UnknownError("Unable to load system schemas!".into())
        })?;
        let database = Arc::new(Self {
            name,
            generator: db.get_generator(),
            db,
            schemas_table,
            schemas: RwLock::new(HashMap::new()),
        });
        // Best-effort: "default" is auto-created by Database::create, but
        // nothing stops it from having been dropped out-of-band since —
        // a missing/failed load here must not fail opening the database
        // as a whole, only leave "default" unloaded until (if ever)
        // explicitly looked up or recreated.
        let _ = database.get_schema(DEFAULT_SCHEMA_NAME);
        Ok(database)
    }

    pub fn close(self: Arc<Self>) -> Result<(F, F, F), SchemaError> {
        for schema in self.schemas.read().values() {
            schema.flush_metadata()?;
        }
        let db = self.db.clone();
        // Drop every reference this Database holds to `db` — including,
        // transitively, each loaded Schema's own clone — before closing
        // it: Db::close (below) does its own Arc::try_unwrap internally
        // and requires unique ownership.
        let database = Arc::try_unwrap(self).map_err(|_| {
            SchemaError::UnknownError(
                "Database::close: other Arc<Database> references still exist".into(),
            )
        })?;
        drop(database);
        Ok(db.close()?)
    }

    pub fn create_schema(self: &Arc<Self>, name: &str) -> Result<Arc<Schema<F>>, SchemaError> {
        if self.schemas.read().contains_key(name) {
            return Err(SchemaError::SchemaInUseError(name.to_string()));
        }
        // Also guards against recreating a schema that exists in storage
        // but isn't currently loaded (e.g. created earlier, then dropped
        // from the in-memory map on a prior close/reopen without being
        // explicitly re-loaded).
        if self
            .db
            .table_id_by_name(Schema::<F>::system_table_name(name))?
            .is_some()
        {
            return Err(SchemaError::SchemaInUseError(name.to_string()));
        }

        let txn = self.db.begin()?;
        let ik = IndexKey::new_from(&[ValueItem::Str((
            name.to_string(),
            MAX_TABLE_NAME_LEN as u32,
        ))])?;
        self.db.insert(
            self.schemas_table,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &to_allocvec(&name.to_string())?,
                Some(txn.id()),
                None,
            ),
            &txn,
        )?;
        // Schema::create's own db.create_table call is DDL, not undone by
        // rollback(txn) — same known, accepted tradeoff as create_table's
        // index creation below: the up-front existence checks above close
        // the one collision-driven failure mode that's actually reachable
        // here, so this only leaks on a genuinely unexpected failure.
        let schema = match Schema::<F>::create(name.to_string(), self.db.clone()) {
            Ok(schema) => schema,
            Err(e) => {
                self.db.rollback(txn)?;
                return Err(e);
            }
        };
        self.db.commit(txn)?;
        self.schemas.write().insert(name.to_string(), schema.clone());
        Ok(schema)
    }

    pub fn get_schema(self: &Arc<Self>, name: &str) -> Result<Arc<Schema<F>>, SchemaError> {
        if let Some(schema) = self.schemas.read().get(name) {
            return Ok(schema.clone());
        }
        let schema = Schema::<F>::load(name.to_string(), self.db.clone())?;
        self.schemas.write().insert(name.to_string(), schema.clone());
        Ok(schema)
    }

    pub(crate) fn schema_exists(self: &Arc<Self>, name: &str) -> bool {
        self.schemas.read().contains_key(name)
    }

    // Exposed so Connection can hold an explicit transaction open across
    // several statements (BEGIN ... COMMIT/ROLLBACK) — everything else
    // in Database/Schema still opens and finishes its own, own-statement-
    // scoped transaction directly against `db`.
    pub(crate) fn begin(&self) -> Result<store::txn::Transaction, SchemaError> {
        Ok(self.db.begin()?)
    }

    pub(crate) fn commit(&self, txn: store::txn::Transaction) -> Result<(), SchemaError> {
        Ok(self.db.commit(txn)?)
    }

    pub(crate) fn rollback(&self, txn: store::txn::Transaction) -> Result<(), SchemaError> {
        Ok(self.db.rollback(txn)?)
    }
}
