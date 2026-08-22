use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
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

    // The name of the auto-increment sequence backing a primary-key-less
    // table's row ids — only created (see create_table) and consulted
    // (see insert_rows) when the table has no PRIMARY KEY of its own to
    // key rows by instead.
    fn rowid_seq_name(&self, table_name: &str) -> String {
        format!("{}.rowid", self.qualify(table_name))
    }

    // Statement dispatch (parsing SQL, matching on the statement kind)
    // lives in Statement::execute (stmt.rs) — this is just the actual
    // table-creation work, called from there for the CreateTable case.
    //
    // Besides the table's own metadata row (in this schema's system
    // table) and each index's own backing table, this also creates the
    // table's *row-storage* backing table — a BPlusTree exactly like an
    // index's, just holding full rows instead of indexed columns — and,
    // for a table with no PRIMARY KEY, the auto-increment sequence used
    // to key rows in it.
    pub(crate) fn create_table(self: &Arc<Self>, table: SqlTable) -> Result<(), SchemaError> {
        let mut table = table;
        let row_table_name = self.qualify(&table.name);
        let rowid_seq_name = self.rowid_seq_name(&table.name);
        let has_primary_key = table.primary_key().is_some();

        // Resolve every store-level name this needs up front — the row
        // table itself, plus each index's backing table — and fail
        // before creating anything if any is already taken, so the
        // creation step below only runs once none of them can collide —
        // closes the one failure mode that was actually reachable here
        // (see drop_table's own doc comment for why
        // create_table_with_index_entry_size isn't undone by
        // self.db.rollback(txn): it's DDL, not a row-level, undo-logged
        // operation the way insert/update/remove are).
        if self.db.table_id_by_name(&row_table_name)?.is_some() {
            return Err(SchemaError::BadTableName(format!(
                "Table name {} is already in use",
                table.name
            )));
        }
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
        // Tracks names/generators as they're actually created (not just
        // planned), so a later failure in this same sequence — a
        // transient I/O error or lock contention on some
        // create_table_with_index_entry_size call, now that a name
        // collision can't happen here anymore — can be cleaned up
        // instead of leaking. Safe to call unconditionally here: nothing
        // else can have discovered these tables yet (the SqlTable row
        // isn't in self.tables, and table_exists/get_table can't see it
        // either, until the whole create_table call succeeds).
        let mut created_names: Vec<String> = Vec::with_capacity(table.indices.len() + 1);
        let mut created_generator = false;
        let res: Result<(), SchemaError> = (|| {
            let row_table_id = self
                .db
                .create_table_with_index_entry_size(row_table_name.clone(), table.row_size() as u64)?;
            created_names.push(row_table_name.clone());
            table.db_table_id = row_table_id;

            if !has_primary_key {
                self.db
                    .get_generator()
                    .create_generator(&rowid_seq_name, Some(0))?;
                created_generator = true;
            }

            table
                .indices
                .iter_mut()
                .zip(&index_names)
                .try_for_each(|(i, qualified)| {
                    let size = i.size();
                    let iid = self
                        .db
                        .create_table_with_index_entry_size(qualified.clone(), size as u64)?;
                    i.db_table_id = iid;
                    created_names.push(qualified.clone());
                    Ok(())
                })
        })();
        if res.is_err() {
            for name in &created_names {
                self.db.drop_table(name)?;
            }
            if created_generator {
                self.db.get_generator().remove_generator(&rowid_seq_name)?;
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

    // The DBIdType a full row (in table-field order) should be keyed by
    // in the table's own row-storage backing table: the PRIMARY KEY's
    // values if it has one, else the next value from its auto-increment
    // rowid sequence (see create_table).
    fn row_key(&self, table: &SqlTable, row: &[ValueItem]) -> Result<DBIdType, SchemaError> {
        match table.primary_key() {
            Some(pk) => {
                let values = table.extract_field_values(&pk.fields, row);
                Ok(DBIdType::Rec(IndexKey::new_from(&values)?))
            }
            None => {
                let id = self
                    .db
                    .get_generator()
                    .gen_key(self.rowid_seq_name(&table.name))?;
                Ok(DBIdType::Int(id))
            }
        }
    }

    // Statement dispatch for Insert lives in Statement::execute, same
    // split as create_table — this does the actual work: writing each
    // full row into the table's own backing table, and a corresponding
    // entry into every index's backing table (so PRIMARY KEY/UNIQUE
    // constraints are enforced immediately, by the same duplicate-key
    // rejection store already gives every BPlusTree table for free —
    // not deferred to some later index-build step). All in one
    // transaction per call: a violation on any row, or any index entry,
    // rolls the whole batch back — undo-logged row-level inserts, unlike
    // create_table's own DDL, so a plain rollback(txn) is enough here.
    pub(crate) fn insert_rows(
        self: &Arc<Self>,
        table_name: &str,
        rows: Vec<Vec<ValueItem>>,
    ) -> Result<usize, SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;

        let txn = self.db.begin()?;
        let res: Result<usize, SchemaError> = (|| {
            let mut count = 0usize;
            for row in &rows {
                let row_key = self.row_key(&table, row)?;
                let row_data = IndexKey::new_from(row)?;
                self.db.insert(
                    table.db_table_id,
                    Tuple::new_with(
                        row_key.clone(),
                        &to_allocvec(&row_data)?,
                        Some(txn.id()),
                        None,
                    ),
                    &txn,
                )?;

                // What every index's entry points back to: the row's own
                // key, itself encoded as an IndexKey — the PRIMARY KEY's
                // own IndexKey when there is one (so an index lookup can
                // go straight to DBIdType::Rec of it), or a single-value
                // IndexKey wrapping the auto-generated id otherwise.
                let identity = match &row_key {
                    DBIdType::Rec(ik) => ik.clone(),
                    DBIdType::Int(n) => IndexKey::new_from(&[ValueItem::Integer(*n as i64)])?,
                };
                for index in &table.indices {
                    let values = table.extract_field_values(&index.fields, row);
                    let index_key = DBIdType::Rec(IndexKey::new_from(&values)?);
                    self.db.insert(
                        index.db_table_id,
                        Tuple::new_with(
                            index_key,
                            &to_allocvec(&identity)?,
                            Some(txn.id()),
                            None,
                        ),
                        &txn,
                    )?;
                }
                count += 1;
            }
            Ok(count)
        })();

        match res {
            Ok(count) => {
                self.db.commit(txn)?;
                Ok(count)
            }
            Err(e) => {
                self.db.rollback(txn)?;
                Err(e)
            }
        }
    }
}
