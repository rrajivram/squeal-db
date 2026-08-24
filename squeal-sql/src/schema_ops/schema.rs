use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    txn::Transaction,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    constant::MAX_TABLE_NAME_LEN,
    error::SchemaError,
    rslt::resultset::ResultSet,
    table::{Field, SqlForeignKey, SqlTable, VersionedRow},
};

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
        // Self-referential foreign keys (ref_table == this table) were
        // already fully validated inside TableBuilder::build(), which
        // has this table's own shape in hand but no access to the rest
        // of the schema — anything referencing a *different* table can
        // only be checked here, once that table's own SqlTable is
        // available via self.get_table.
        for fk in &table.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(&table.name) {
                continue;
            }
            let ref_table = self.get_table(&fk.ref_table).ok_or_else(|| {
                SchemaError::UserError(format!(
                    "Foreign key references unknown table: {:?}",
                    fk.ref_table
                ))
            })?;
            let column_datatype = table
                .fields()
                .iter()
                .find(|f| f.name == fk.column)
                .expect("TableBuilder::build already checked this column exists")
                .datatype;
            ref_table.validate_foreign_key_target(&fk.ref_column, column_datatype)?;
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
            let row_table_id = self.db.create_table_with_index_entry_size(
                row_table_name.clone(),
                table.row_size() as u64,
            )?;
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
    // not deferred to some later index-build step).
    //
    // `txn`: Some when a connection has an explicit BEGIN open (see
    // Connection::with_current_txn) — rows go into that shared
    // transaction, and committing/rolling it back is the caller's job
    // (via COMMIT/ROLLBACK), not this call's. None (autocommit) opens
    // and finishes its own transaction here, same as before explicit
    // transactions existed: a violation on any row, or any index entry,
    // rolls the whole batch back — undo-logged row-level inserts, unlike
    // create_table's own DDL, so a plain rollback(txn) is enough.
    pub(crate) fn insert_rows(
        self: &Arc<Self>,
        table_name: &str,
        rows: Vec<Vec<ValueItem>>,
        txn: Option<&Transaction>,
    ) -> Result<usize, SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;

        match txn {
            Some(txn) => self.insert_rows_in_txn(&table, &rows, txn),
            None => {
                let txn = self.db.begin()?;
                match self.insert_rows_in_txn(&table, &rows, &txn) {
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
    }

    fn insert_rows_in_txn(
        self: &Arc<Self>,
        table: &SqlTable,
        rows: &[Vec<ValueItem>],
        txn: &Transaction,
    ) -> Result<usize, SchemaError> {
        let mut count = 0usize;
        for row in rows {
            self.check_foreign_keys(table, row, txn)?;
            let row_key = self.row_key(table, row)?;
            // Stamped with the table's CURRENT version — every new
            // insert is always written in the table's latest shape;
            // only rows written before an ALTER TABLE carry an older
            // version (see VersionedRow, SqlTable::reproject).
            let row_data = VersionedRow {
                version: table.version(),
                values: IndexKey::new_from(row)?,
            };
            self.db.insert(
                table.db_table_id,
                Tuple::new_with(
                    row_key.clone(),
                    &to_allocvec(&row_data)?,
                    Some(txn.id()),
                    None,
                ),
                txn,
            )?;

            // What every index's entry points back to: the row's own
            // key, itself encoded as an IndexKey — the PRIMARY KEY's own
            // IndexKey when there is one (so an index lookup can go
            // straight to DBIdType::Rec of it), or a single-value
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
                    Tuple::new_with(index_key, &to_allocvec(&identity)?, Some(txn.id()), None),
                    txn,
                )?;
            }
            count += 1;
        }
        Ok(count)
    }

    // For each of `table`'s foreign keys, checks `row`'s value for that
    // key's column against the referenced table's target index — a
    // NULL value is always allowed through (standard SQL: a foreign key
    // only constrains non-NULL values), matching the same read-your-
    // own-writes transaction `txn` the row itself is being inserted
    // under, so a batch can reference an earlier row in the very same
    // INSERT statement (not a later one — that row doesn't exist yet).
    fn check_foreign_keys(
        self: &Arc<Self>,
        table: &SqlTable,
        row: &[ValueItem],
        txn: &Transaction,
    ) -> Result<(), SchemaError> {
        for fk in &table.foreign_keys {
            let pos = table
                .fields()
                .iter()
                .position(|f| f.name == fk.column)
                .expect("a table's own foreign_keys always reference one of its own fields");
            let value = &row[pos];
            if *value == ValueItem::Null {
                continue;
            }
            let ref_table = self.get_table(&fk.ref_table).ok_or_else(|| {
                SchemaError::UnknownError(format!(
                    "foreign key on {:?}.{} references unknown table {:?}",
                    table.name, fk.column, fk.ref_table
                ))
            })?;
            let index = ref_table.unique_index_on(&fk.ref_column).ok_or_else(|| {
                SchemaError::UnknownError(format!(
                    "foreign key on {:?}.{} references {:?}.{}, which is no longer a \
                     PRIMARY KEY/UNIQUE column",
                    table.name, fk.column, fk.ref_table, fk.ref_column
                ))
            })?;
            let key = DBIdType::Rec(IndexKey::new_from(std::slice::from_ref(value))?);
            if self.db.find(index.db_table_id, key, txn)?.is_none() {
                return Err(SchemaError::UserError(format!(
                    "insert on table {:?} violates foreign key {:?}: no row in {:?} with {} = {value:?}",
                    table.name,
                    fk.name.as_deref().unwrap_or("<unnamed>"),
                    fk.ref_table,
                    fk.ref_column
                )));
            }
        }
        Ok(())
    }

    // Statement dispatch for SELECT lives in Statement::execute, same
    // split as create_table/insert_rows — this is the "SELECT * FROM
    // <table>" launchpad: every stored row in the table's own
    // row-storage backing table (see create_table), decoded back out of
    // the same IndexKey encoding insert_rows wrote them in, in whatever
    // order table_scan yields (no ORDER BY support yet — see
    // Statement::execute's own parsing). `txn` mirrors insert_rows' own
    // parameter: Some(_) when the caller has an explicit transaction open
    // (see Connection::current_txn) scans under that transaction — via
    // Db::table_scan_in_txn — so the connection can see its own
    // not-yet-committed inserts, same as a real DB's read-your-own-writes;
    // None scans under a fresh transaction and so only ever sees committed
    // state, same as every other autocommit statement.
    pub(crate) fn select_all(
        self: &Arc<Self>,
        table_name: &str,
        txn: Option<&Transaction>,
    ) -> Result<ResultSet, SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let columns = table.fields().iter().map(|f| f.name.clone()).collect();
        let mut rows = Vec::new();
        let mut cursor = match txn {
            Some(txn) => self.db.table_scan_in_txn(table.db_table_id, txn)?,
            None => self.db.table_scan(table.db_table_id)?,
        };
        while let Some(tuple) = cursor.next()? {
            let row = from_bytes::<VersionedRow>(tuple.data())?;
            rows.push(table.reproject(&row)?);
        }
        Ok(ResultSet::new(columns, rows))
    }

    // Statement dispatch for ALTER TABLE lives in Statement::execute,
    // same split as create_table/insert_rows/select_all — these three
    // do the actual metadata mutation (see SqlTable::alter_add_column/
    // alter_drop_column/alter_rename_column for the validation and
    // version-history bookkeeping itself) plus persisting it. Mirrors
    // create_table's own ordering: persist to disk and commit *before*
    // updating the in-memory table, so a failure partway through never
    // leaves the in-memory and on-disk shapes disagreeing.
    pub(crate) fn add_column(
        self: &Arc<Self>,
        table_name: &str,
        field: Field,
    ) -> Result<(), SchemaError> {
        self.alter_table(table_name, |t| t.alter_add_column(field))
    }

    pub(crate) fn drop_column(
        self: &Arc<Self>,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), SchemaError> {
        // SqlTable::alter_drop_column only guards against `column_name`
        // being this table's own LOCAL foreign-key column — it has no
        // way to see another table's (or, for a self-referential key,
        // this same table's own) foreign key pointing at it as a
        // *target*, so that side of the check has to happen here.
        self.check_not_a_foreign_key_target(table_name, column_name)?;
        self.alter_table(table_name, |t| t.alter_drop_column(column_name))
    }

    pub(crate) fn rename_column(
        self: &Arc<Self>,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        self.check_not_a_foreign_key_target(table_name, old_name)?;
        self.alter_table(table_name, |t| t.alter_rename_column(old_name, new_name))
    }

    fn check_not_a_foreign_key_target(
        self: &Arc<Self>,
        table_name: &str,
        column_name: &str,
    ) -> Result<(), SchemaError> {
        let name = table_name.to_lowercase();
        for (other_name, other) in self.tables.read().iter() {
            if let Some(fk) = other
                .foreign_keys
                .iter()
                .find(|fk| fk.ref_table.eq_ignore_ascii_case(&name) && fk.ref_column == column_name)
            {
                return Err(SchemaError::UserError(format!(
                    "Column {column_name:?} on table {table_name:?} is referenced by foreign key \
                     {:?} on table {other_name:?} — drop that foreign key first",
                    fk.name.clone().unwrap_or_else(|| "<unnamed>".into())
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn add_foreign_key(
        self: &Arc<Self>,
        table_name: &str,
        fk: SqlForeignKey,
    ) -> Result<(), SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let ref_table = self.get_table(&fk.ref_table).ok_or_else(|| {
            SchemaError::UserError(format!(
                "Foreign key references unknown table: {:?}",
                fk.ref_table
            ))
        })?;
        let column_datatype = table
            .fields()
            .iter()
            .find(|f| f.name == fk.column)
            .ok_or_else(|| {
                SchemaError::UserError(format!(
                    "Table {table_name:?} has no column named {:?}",
                    fk.column
                ))
            })?
            .datatype;
        ref_table.validate_foreign_key_target(&fk.ref_column, column_datatype)?;
        let index = ref_table
            .unique_index_on(&fk.ref_column)
            .expect("validate_foreign_key_target above already confirmed this exists");

        // Existing rows must already satisfy the constraint — no NOT
        // VALID escape hatch in this engine yet (see
        // foreign_key_holder's own doc comment on what ALTER TABLE ADD
        // FOREIGN KEY deliberately doesn't support). A plain autocommit
        // read: nothing here writes anything, so there's no partial
        // state to roll back on a violation.
        let txn = self.db.begin()?;
        let mut cursor = self.db.table_scan_in_txn(table.db_table_id, &txn)?;
        while let Some(tuple) = cursor.next()? {
            let versioned = from_bytes::<VersionedRow>(tuple.data())?;
            let row = table.reproject(&versioned)?;
            let pos = table
                .fields()
                .iter()
                .position(|f| f.name == fk.column)
                .expect("checked present above");
            let value = &row[pos];
            if *value != ValueItem::Null {
                let key = DBIdType::Rec(IndexKey::new_from(std::slice::from_ref(value))?);
                if self.db.find(index.db_table_id, key, &txn)?.is_none() {
                    return Err(SchemaError::UserError(format!(
                        "cannot add foreign key: existing row in {table_name:?} has {} = \
                         {value:?}, which does not exist in {:?}.{}",
                        fk.column, fk.ref_table, fk.ref_column
                    )));
                }
            }
        }

        self.alter_table(table_name, |t| t.alter_add_foreign_key(fk))
    }

    pub(crate) fn drop_foreign_key(
        self: &Arc<Self>,
        table_name: &str,
        constraint_name: &str,
    ) -> Result<(), SchemaError> {
        self.alter_table(table_name, |t| t.alter_drop_foreign_key(constraint_name))
    }

    // Statement dispatch for COPY INTO lives in Statement::execute, same
    // split as everything else — this does the actual load. `path` is
    // read as a CSV file (always assumed — see stmt.rs's
    // parse_copy_into, which rejects FILE_FORMAT overrides), first row
    // skipped as a header, every row after that mapped POSITIONALLY
    // onto the table's current column order (like an implicit-column-
    // list INSERT). Permissive, not atomic, on purpose: a row that
    // fails to parse or violates a constraint is skipped and counted,
    // not fatal to the whole load — matches real COPY INTO's own
    // per-row reporting, unlike this engine's own multi-row INSERT
    // (which rolls the whole batch back on any single failure). Each
    // row is its own independent insert_rows call (autocommit), so an
    // earlier row's success survives a later row's failure.
    pub(crate) fn copy_csv_into(
        self: &Arc<Self>,
        table_name: &str,
        path: &str,
    ) -> Result<(usize, usize), SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let file = std::fs::File::open(path)
            .map_err(|e| SchemaError::UserError(format!("could not open {path:?}: {e}")))?;
        let mut reader = csv::ReaderBuilder::new().has_headers(true).from_reader(file);
        let fields = table.fields();

        let mut loaded = 0usize;
        let mut failed = 0usize;
        for record in reader.records() {
            let row = record
                .map_err(|e| SchemaError::UserError(e.to_string()))
                .and_then(|record| csv_record_to_row(&record, fields));
            let row = match row {
                Ok(row) => row,
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            match self.insert_rows(table_name, vec![row], None) {
                Ok(_) => loaded += 1,
                Err(_) => failed += 1,
            }
        }
        Ok((loaded, failed))
    }

    fn alter_table(
        self: &Arc<Self>,
        table_name: &str,
        apply: impl FnOnce(&mut SqlTable) -> Result<(), SchemaError>,
    ) -> Result<(), SchemaError> {
        let name = table_name.to_lowercase();
        let mut table = self.get_table(&name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        apply(&mut table)?;

        let txn = self.db.begin()?;
        let ik = IndexKey::new_from(&[ValueItem::Str((name.clone(), MAX_TABLE_NAME_LEN as u32))])?;
        self.db.update(
            self.sys_table_id,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &to_allocvec(&table)?,
                Some(txn.id()),
                None,
            ),
            &txn,
        )?;
        self.db.commit(txn)?;
        self.tables.write().insert(name, table);
        Ok(())
    }
}

// One CSV row -> one full row's worth of ValueItems, in `fields`' own
// (i.e. the table's current) order — positional, so the CSV's own
// column count must match exactly; there's no header-name-based
// mapping (see Schema::copy_csv_into's own doc comment).
fn csv_record_to_row(
    record: &csv::StringRecord,
    fields: &[Arc<Field>],
) -> Result<Vec<ValueItem>, SchemaError> {
    if record.len() != fields.len() {
        return Err(SchemaError::UserError(format!(
            "expected {} field(s), got {}",
            fields.len(),
            record.len()
        )));
    }
    fields
        .iter()
        .zip(record.iter())
        .map(|(f, cell)| csv_field_to_value_item(cell, f.datatype, f.nullable))
        .collect()
}

// An empty CSV field means NULL (matching Snowflake's own default CSV
// NULL handling) — an error for a NOT NULL column, same as any other
// NULL-into-NOT-NULL rejection. Blob has no CSV representation this
// engine knows how to parse (no literal Blob syntax anywhere else in
// this crate either — see expr_to_value_item's own gap).
fn csv_field_to_value_item(
    cell: &str,
    datatype: crate::datatype::DataType,
    nullable: bool,
) -> Result<ValueItem, SchemaError> {
    use crate::datatype::DataType;
    if cell.is_empty() {
        return if nullable {
            Ok(ValueItem::Null)
        } else {
            Err(SchemaError::UserError(
                "empty CSV field for a NOT NULL column".into(),
            ))
        };
    }
    match datatype {
        DataType::Integer => cell
            .parse()
            .map(ValueItem::Integer)
            .map_err(|_| SchemaError::UserError(format!("invalid integer: {cell:?}"))),
        DataType::Double => cell
            .parse()
            .map(ValueItem::Double)
            .map_err(|_| SchemaError::UserError(format!("invalid double: {cell:?}"))),
        // A bare number is accepted as-is (an already-computed literal
        // value); anything else is tried as "YYYY-MM-DD"/"HH:MM:SS"/
        // the two combined — see crate::datetime's own doc comment.
        DataType::Datetime => cell
            .parse()
            .ok()
            .or_else(|| crate::datetime::parse_datetime(cell))
            .map(ValueItem::Datetime)
            .ok_or_else(|| SchemaError::UserError(format!("invalid datetime: {cell:?}"))),
        DataType::Str(cap) => Ok(ValueItem::Str((cell.to_string(), cap))),
        DataType::Blob(_) | DataType::Unsupported => Err(SchemaError::UserError(format!(
            "CSV loading into a {datatype:?} column is not supported yet"
        ))),
    }
}
