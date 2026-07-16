use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    generator::Generator,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    table::{SqlTable, TableBuilder},
};

#[derive(Clone)]
pub struct Schema<F: DBFile> {
    name: String,
    db: Arc<Db<F>>,
    tables: Arc<RwLock<HashMap<String, SqlTable>>>,
    sys_tables: Arc<RwLock<HashMap<String, TableIdType>>>,
    generator: Arc<Generator>,
}

const SYSTEM_SCHEMA: &str = "sql_system.schema";

impl<F> Schema<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub fn create(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::create(&name)?;
        let mut s = Self {
            name,
            db: db.clone(),
            generator: db.get_generator(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_tables: Arc::new(RwLock::new(HashMap::new())),
        };
        s.setup_schema()?;
        Ok(Arc::new(s))
    }

    fn setup_schema(&mut self) -> Result<(), SchemaError> {
        let id = self.db.create_table(SYSTEM_SCHEMA.into())?;
        self.sys_tables.write().insert(SYSTEM_SCHEMA.into(), id);
        Ok(())
    }

    fn close(self: Arc<Self>) -> Result<(F, F, F), SchemaError> {
        let tid = *self.sys_tables.read().get(SYSTEM_SCHEMA).unwrap();
        let tx = self.db.begin()?;
        for (n, t) in self.tables.read().iter() {
            let ik = IndexKey::new_from(&[ValueItem::Str((n.clone(), 128))])?;
            self.db.update(
                tid,
                Tuple::new_with(DBIdType::Rec(ik), &to_allocvec(t)?, Some(tx.id()), None),
                &tx,
            )?;
        }
        self.db.commit(tx)?;
        let schema =
            Arc::try_unwrap(self).map_err(|_| SchemaError::UnknownError("Unknown error".into()))?;
        let (f, u, r) = schema.db.close()?;
        Ok((f, u, r))
    }

    fn load_schema(&mut self) -> Result<(), SchemaError> {
        let tid = self
            .db
            .table_id_by_name(SYSTEM_SCHEMA)?
            .ok_or(SchemaError::UnknownError(
                "Unable to load system tables!".into(),
            ))?;
        let mut cursor = self.db.table_scan(tid)?;
        while let Some(tuple) = cursor.next()? {
            let table = from_bytes::<SqlTable>(tuple.data())?;
            self.tables.write().insert(table.name.clone(), table);
        }
        Ok(())
    }

    pub fn open(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::open(&name)?;
        let mut s = Self {
            name: name.clone(),
            db: db.clone(),
            generator: db.get_generator(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_tables: Arc::new(RwLock::new(HashMap::new())),
        };
        s.load_schema()?;
        Ok(Arc::new(s))
    }

    pub fn builder(self: Arc<Self>) -> TableBuilder<F> {
        TableBuilder::new(self.clone())
    }

    //    pub fn create_table(self: Arc<Self>, table: Table) -> Result<(), SchemaError> {}
}
