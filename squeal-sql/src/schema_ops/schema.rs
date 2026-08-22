use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use sqlparser::{dialect::GenericDialect, parser::Parser};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

use crate::{constant::MAX_TABLE_NAME_LEN, error::SchemaError, table::SqlTable};

#[derive(Clone)]
pub struct Schema<F: DBFile> {
    name: String,
    // Shared with the owning Database and every sibling Schema — the
    // underlying store has one flat table namespace, so every store-level
    // table/index this schema creates must go through `qualify()` first
    // to avoid colliding with another schema's tables of the same name.
    db: Arc<Db<F>>,
    tables: Arc<RwLock<HashMap<String, SqlTable>>>,
    sys_table_id: TableIdType,
    dialect: Arc<GenericDialect>,
}

#[cfg(test)]
mod tests;

// store::Db doesn't implement Debug, so this can't be derived — a
// minimal manual impl (name only) is enough for {:?} logging and for
// Result<Arc<Schema<F>>, _>::unwrap_err() in tests.
impl<F: DBFile> std::fmt::Debug for Schema<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<F> Schema<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // Schema is never constructed standalone — only Database creates or
    // loads one (see Database::create_schema/get_schema), since a Schema
    // has to share its Database's single Db<F> rather than owning one.
    pub(crate) fn create(name: String, db: Arc<Db<F>>) -> Result<Arc<Self>, SchemaError> {
        let sys_table_id = db.create_table(Self::system_table_name(&name))?;
        Ok(Arc::new(Self {
            name,
            db,
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_table_id,
            dialect: Arc::new(GenericDialect),
        }))
    }

    pub(crate) fn load(name: String, db: Arc<Db<F>>) -> Result<Arc<Self>, SchemaError> {
        let sys_table_id = db
            .table_id_by_name(Self::system_table_name(&name))?
            .ok_or_else(|| SchemaError::SchemaNotFound(name.clone()))?;
        let mut s = Self {
            name,
            db,
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_table_id,
            dialect: Arc::new(GenericDialect),
        };
        s.load_tables()?;
        Ok(Arc::new(s))
    }

    pub(crate) fn system_table_name(schema_name: &str) -> String {
        format!("{schema_name}.{}", crate::constant::SYSTEM_TABLES_SUFFIX)
    }

    // Every store-level table/index name this schema creates or looks up
    // must go through this — the underlying Db<F> is shared by every
    // schema in the Database and has one flat table namespace.
    fn qualify(&self, name: &str) -> String {
        format!("{}.{}", self.name, name)
    }

    fn load_tables(&mut self) -> Result<(), SchemaError> {
        let mut cursor = self.db.table_scan(self.sys_table_id)?;
        while let Some(tuple) = cursor.next()? {
            let table = from_bytes::<SqlTable>(tuple.data())?;
            self.tables.write().insert(table.name.clone(), table);
        }
        Ok(())
    }

    // Re-persists every known table's current metadata row — called by
    // Database::close before it releases its Db<F>. Rows are keyed by
    // this schema's own unqualified table names; `sys_table_id` is
    // already schema-scoped (one system table per schema), so no
    // qualification is needed for the row key itself, only for
    // store-level table/index names (see `qualify`).
    pub(crate) fn flush_metadata(&self) -> Result<(), SchemaError> {
        let tx = self.db.begin()?;
        for (n, t) in self.tables.read().iter() {
            let ik = IndexKey::new_from(&[ValueItem::Str((n.clone(), MAX_TABLE_NAME_LEN as u32))])?;
            self.db.update(
                self.sys_table_id,
                Tuple::new_with(DBIdType::Rec(ik), &to_allocvec(t)?, Some(tx.id()), None),
                &tx,
            )?;
        }
        self.db.commit(tx)?;
        Ok(())
    }

    pub fn execute(self: &Arc<Self>, sql: String) -> Result<(), SchemaError> {
        let dialect = GenericDialect;
        let statements = Parser::parse_sql(&dialect, sql.as_str())?;
        statements.iter().try_for_each(|s| self.exec_statement(s))?;

        Ok(())
    }

    fn exec_statement(
        self: &Arc<Self>,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<(), SchemaError> {
        match stmt {
            sqlparser::ast::Statement::CreateTable(c) => {
                self.create_table(SqlTable::from_sql(self, c.clone())?)?;
            }
            sqlparser::ast::Statement::AlterCollation(_) => {
                todo!()
            }
            _ => {}
        }
        Ok(())
    }

    fn create_table(self: &Arc<Self>, table: SqlTable) -> Result<(), SchemaError> {
        let mut table = table;

        // Resolve every index's target (schema-qualified) name up front
        // and fail before creating anything if one's already taken, so
        // the loop below only runs once none of them can collide —
        // closes the one failure mode that was actually reachable here
        // (see drop_table's own doc comment for why
        // create_table_with_index_entry_size isn't undone by
        // self.db.rollback(txn): it's DDL, not a row-level, undo-logged
        // operation the way insert/update/remove are).
        let mut index_names = Vec::with_capacity(table.indices.len());
        for (count, i) in table.indices.iter().enumerate() {
            let iname = i
                .name
                .clone()
                .unwrap_or_else(|| format!("{}{}", table.name, count));
            let qualified = self.qualify(&iname);
            if self.db.table_id_by_name(&qualified)?.is_some() {
                return Err(SchemaError::BadTableName(format!(
                    "Index name {iname} is already in use"
                )));
            }
            index_names.push(qualified);
        }

        let txn = self.db.begin()?;
        let ik = IndexKey::new_from(&[ValueItem::Str((
            table.name.clone(),
            MAX_TABLE_NAME_LEN as u32,
        ))])?;
        self.db.insert(
            self.sys_table_id,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &to_allocvec(&table)?,
                Some(txn.id()),
                None,
            ),
            &txn,
        )?;
        // Tracks names as they're actually created (not just planned), so a
        // later failure in this same loop — a transient I/O error or lock
        // contention on some create_table_with_index_entry_size call, now
        // that a name collision can't happen here anymore — can be cleaned
        // up via drop_table instead of leaking. Safe to call unconditionally
        // here: nothing else can have discovered these tables yet (the
        // SqlTable row isn't in self.tables, and table_exists/get_table
        // can't see it either, until the whole create_table call succeeds).
        let mut created_names: Vec<String> = Vec::with_capacity(table.indices.len());
        let res: Result<(), SchemaError> =
            table
                .indices
                .iter_mut()
                .zip(index_names)
                .try_for_each(|(i, qualified)| {
                    let size = i.size();
                    let iid = self
                        .db
                        .create_table_with_index_entry_size(qualified.clone(), size as u64)?;
                    i.db_table_id = iid;
                    created_names.push(qualified);
                    Ok(())
                });
        if res.is_err() {
            for name in &created_names {
                self.db.drop_table(name)?;
            }
            self.db.rollback(txn)?;
            return res;
        }
        self.db.commit(txn)?;
        self.tables.write().insert(table.name.clone(), table);
        Ok(())
    }

    pub(crate) fn table_exists(self: &Arc<Self>, name: &str) -> bool {
        let name = name.to_lowercase();
        self.tables.read().contains_key(&name)
    }

    pub(crate) fn get_table(self: &Arc<Self>, name: &str) -> Option<SqlTable> {
        self.tables.read().get(&name.to_lowercase()).cloned()
    }
}
