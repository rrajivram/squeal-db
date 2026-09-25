use std::{collections::HashMap, sync::Arc};

use parking_lot::{Mutex, RwLock};
use postcard::{from_bytes, to_allocvec};
use store::clock::Instant;
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    error::StoreError,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    txn::Transaction,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    constant::MAX_TABLE_NAME_LEN,
    error::SchemaError,
    optim::table_stats::{SchemaStats, TableStat},
    rslt::resultset::ResultSet,
    table::{Field, SqlForeignKey, SqlIndex, SqlTable, VersionedRow},
};

// Default per-column-value sampling rate for a schema's SchemaStats (see
// optim::table_stats) — 1.0 means every logged row is applied (no
// skipping). Not yet exposed as something a caller can tune; a
// reasonable placeholder until something drives that decision (e.g. a
// SQL-level setting).
const DEFAULT_STATS_SAMPLING_RATE: f64 = 1.0;

#[derive(Clone)]
pub struct Schema<F: DBFile> {
    pub(crate) name: String,
    // Shared with the owning Database and every sibling Schema — the
    // underlying store has one flat table namespace, so every store-level
    // table/index this schema creates must go through `qualify()` first
    // to avoid colliding with another schema's tables of the same name.
    pub(crate) db: Arc<Db<F>>,
    tables: Arc<RwLock<HashMap<String, Arc<SqlTable>>>>,
    sys_table_id: TableIdType,
    // One SchemaStats per Schema, persisted to its own store table (see
    // stats_table_name) and shut down alongside it — see
    // persist_and_shutdown_stats. `Option` so close can `.take()` it out
    // for SchemaStats::shutdown, which consumes `self` to join its
    // background thread; `None` afterward means a closed schema (nothing
    // else re-populates it — Schema itself is on its way out too).
    stats: Arc<Mutex<Option<SchemaStats<F>>>>,
    stats_table_id: TableIdType,
}

#[cfg(test)]
mod tests;

// store::Db doesn't implement Debug, so this can't be derived — a
// minimal manual impl (name only) is enough for {:?} logging and for
// Result<Arc<Schema<F>>, _>::unwrap_err() in tests.
/// Rows per transaction for COPY INTO (phase 7).
const COPY_BATCH_ROWS: usize = 1000;

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
        let stats_table_id = db.create_table(Self::stats_table_name(&name))?;
        let schema = Arc::new(Self {
            name,
            db,
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_table_id,
            stats: Arc::new(Mutex::new(None)),
            stats_table_id,
        });
        // SchemaStats::new needs the Arc<Schema<F>> this struct doesn't
        // exist as until the constructor above returns — a brand-new
        // schema has no tables yet either way, so there's nothing for it
        // to find via schema.list_tables() at this point regardless.
        let stats = SchemaStats::new(schema.clone(), DEFAULT_STATS_SAMPLING_RATE)?;
        *schema.stats.lock() = Some(stats);
        Ok(schema)
    }

    pub(crate) fn load(name: String, db: Arc<Db<F>>) -> Result<Arc<Self>, SchemaError> {
        let sys_table_id = db
            .table_id_by_name(Self::system_table_name(&name))?
            .ok_or_else(|| SchemaError::SchemaNotFound(name.clone()))?;
        // Backward compatibility: a schema created before SchemaStats
        // existed has no stats table yet. Create one now (instead of
        // failing to open) and start fresh, exactly like a brand-new
        // schema would — `existed` is what tells SchemaStats::load
        // apart from SchemaStats::new below.
        let (stats_table_id, existed) = match db.table_id_by_name(Self::stats_table_name(&name))? {
            Some(id) => (id, true),
            None => (db.create_table(Self::stats_table_name(&name))?, false),
        };
        let mut s = Self {
            name,
            db,
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_table_id,
            stats: Arc::new(Mutex::new(None)),
            stats_table_id,
        };
        s.load_tables()?;
        let schema = Arc::new(s);
        let stats = if existed {
            SchemaStats::load(schema.clone(), DEFAULT_STATS_SAMPLING_RATE, stats_table_id)?
        } else {
            SchemaStats::new(schema.clone(), DEFAULT_STATS_SAMPLING_RATE)?
        };
        *schema.stats.lock() = Some(stats);
        Ok(schema)
    }

    pub(crate) fn system_table_name(schema_name: &str) -> String {
        format!("{schema_name}.{}", crate::constant::SYSTEM_TABLES_SUFFIX)
    }

    pub(crate) fn stats_table_name(schema_name: &str) -> String {
        format!("{schema_name}.{}", crate::constant::SYSTEM_STATS_SUFFIX)
    }

    // Persists every table's current stats (see SchemaStats::persist) and
    // stops its background collector thread (SchemaStats::shutdown, which
    // consumes it — hence the `.take()`). Called by Database::close
    // alongside flush_metadata, before the underlying Db<F> is released.
    // A no-op if already shut down (or somehow never started).
    pub(crate) fn persist_and_shutdown_stats(&self) -> Result<(), SchemaError> {
        if let Some(stats) = self.stats.lock().take() {
            stats.persist(self.stats_table_id)?;
            stats.shutdown();
        }
        Ok(())
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
            let table = SqlTable::decode_catalog_row(tuple.data())?;
            self.tables
                .write()
                .insert(table.name.clone(), Arc::new(table));
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
                Tuple::new_with(
                    DBIdType::Rec(ik),
                    &t.encode_catalog_row()?,
                    Some(tx.id()),
                    None,
                ),
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
    // Persistence versioning Stage 3: rejects DDL up front whose worst-case
    // key (summed declared column widths — a column's declared capacity is
    // already a true ceiling on real data, see IndexKey::new_from's
    // validate) would exceed this database's configured max_index_key_size,
    // the ceiling page_overhead() reserves header space for. Loud and early
    // by design: this is the one point where refusing is still cheap.
    fn check_key_width(&self, what: &str, key_size: usize) -> Result<(), SchemaError> {
        let cap = self.db.max_index_key_size() as usize;
        if key_size > cap {
            return Err(SchemaError::UserError(format!(
                "{what} would be up to {key_size} bytes wide, exceeding this database's \
                 max index key size of {cap} bytes — use narrower or fewer columns"
            )));
        }
        Ok(())
    }

    pub(crate) fn create_table(self: &Arc<Self>, table: SqlTable) -> Result<(), SchemaError> {
        let mut table = table;
        let row_table_name = self.qualify(&table.name);
        let rowid_seq_name = self.rowid_seq_name(&table.name);
        let has_primary_key = table.primary_key().is_some();

        // The row-storage tree is keyed by the PRIMARY KEY (or a small
        // generated rowid), and every index tree by its own key — each is
        // a tree whose split can stamp a high_key, so each must fit.
        let identity_size = table.identity_size();
        self.check_key_width(
            &format!("PRIMARY KEY of table {:?}", table.name),
            identity_size,
        )?;
        for i in &table.indices {
            self.check_key_width(
                &format!(
                    "index {:?} on table {:?}",
                    i.name.as_deref().unwrap_or("(unnamed)"),
                    table.name
                ),
                i.key_size(identity_size),
            )?;
        }

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
        // The catalog row is inserted LAST, inside the block below, once the
        // row-storage table's and every index's store id have been assigned.
        // It used to be written first, with those ids still the "none"
        // placeholder, and never rewritten — so a crash before a clean close
        // (flush_metadata) left a durable catalog row pointing at table id 0.
        // Writing it last also means a failure anywhere in here (including
        // this insert) goes through the same cleanup that drops whatever
        // store tables were already created.
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
        // Computed before the mutable borrow below (table.indices.iter_mut())
        // makes an immutable one unavailable — every one of these
        // indices is always PRIMARY KEY/UNIQUE today (see SqlIndex::
        // size's own doc comment), so this never actually adds anything
        // here, but it's the same real value CREATE INDEX's own sizing
        // uses, not a stand-in.
        let identity_size = table.identity_size();
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
                    let size = i.size(identity_size);
                    let iid = self
                        .db
                        .create_table_with_index_entry_size(qualified.clone(), size as u64)?;
                    i.db_table_id = iid;
                    created_names.push(qualified.clone());
                    Ok::<(), SchemaError>(())
                })?;

            self.db.insert(
                self.sys_table_id,
                Tuple::new_with(
                    DBIdType::Rec(ik),
                    &table.encode_catalog_row()?,
                    Some(txn.id()),
                    None,
                ),
                &txn,
            )?;
            Ok(())
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
        let table = Arc::new(table);
        // Best-effort, same reasoning as log_stat: the table is already
        // durably created (committed above), so a stats-registration
        // hiccup here (Bloom construction failing — effectively never,
        // given fixed parameters) must not fail table creation itself.
        // update_table_stats also tolerates a table missing from
        // SchemaStats entirely (see its own comment), so skipping this
        // on error just means that table's stats never start collecting
        // rather than anything unsound.
        if let Some(stats) = self.stats.lock().as_ref()
            && let Err(e) = stats.add_table(table.clone())
        {
            log::warn!("failed to register {:?} with SchemaStats: {e}", table.name);
        }
        self.tables.write().insert(table.name.clone(), table);
        Ok(())
    }

    pub(crate) fn table_exists(self: &Arc<Self>, name: &str) -> bool {
        let name = name.to_lowercase();
        self.tables.read().contains_key(&name)
    }

    // Returns the shared Arc, not a deep clone — get_table used to
    // return an owned SqlTable, so every lookup (at least once per
    // statement — INSERT/SELECT/ALTER/COPY INTO all call this) deep-
    // cloned every SchemaVersion's and SqlIndex's own fields list, even
    // though the vast majority of callers only ever read from it.
    // Arc::clone here is O(1) regardless of how many versions/indices/
    // fields the table has; a caller that genuinely needs to mutate its
    // own copy still can via Arc::make_mut (see alter_table).
    pub(crate) fn get_table(self: &Arc<Self>, name: &str) -> Option<Arc<SqlTable>> {
        self.tables.read().get(&name.to_lowercase()).cloned()
    }

    pub(crate) fn list_tables(self: &Arc<Self>) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
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
            Some(txn) => self.insert_rows_in_txn(&table, rows, txn),
            None => {
                let txn = self.db.begin()?;
                match self.insert_rows_in_txn(&table, rows, &txn) {
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

    // Takes `rows` by value, not `&[Vec<ValueItem>]`: each row's own
    // Vec<ValueItem> is moved into its row-storage IndexKey inside
    // insert_one_row_in_txn (see IndexKey::new_from_owned's own doc
    // comment for why that avoids cloning every field).
    fn insert_rows_in_txn(
        self: &Arc<Self>,
        table: &SqlTable,
        rows: Vec<Vec<ValueItem>>,
        txn: &Transaction,
    ) -> Result<usize, SchemaError> {
        let mut count = 0usize;
        for row in rows {
            self.insert_one_row_in_txn(table, row, txn)?;
            count += 1;
        }
        Ok(count)
    }

    // One row's worth of what insert_rows_in_txn used to do inline —
    // pulled out so UPDATE (see update_rows_in_txn) can reuse the exact
    // same index-writing/versioning logic for the "write the new
    // version" half of a delete-then-insert, instead of a second,
    // parallel implementation that could silently drift from plain
    // INSERT's.
    //
    // Everything that still needs to read `row`'s values (FK checks,
    // the row key, each index's own key extraction) runs first, while
    // `row` is still just borrowed — it's moved into row_data only at
    // the very end.
    fn insert_one_row_in_txn(
        self: &Arc<Self>,
        table: &SqlTable,
        row: Vec<ValueItem>,
        txn: &Transaction,
    ) -> Result<(), SchemaError> {
        self.check_foreign_keys(table, &row, txn)?;
        let row_key = self.row_key(table, &row)?;

        // What every index's entry points back to: the row's own
        // key, itself encoded as an IndexKey — the PRIMARY KEY's own
        // IndexKey when there is one (so an index lookup can go
        // straight to DBIdType::Rec of it), or a single-value
        // IndexKey wrapping the auto-generated id otherwise. Built
        // (and every index entry inserted) before `row` is moved
        // into row_data below — extract_field_values below still
        // needs to borrow it.
        let identity = match &row_key {
            DBIdType::Rec(ik) => ik.clone(),
            DBIdType::Int(n) => IndexKey::new_from(&[ValueItem::Integer(*n as i64)])?,
        };
        for index in &table.indices {
            let mut values = table.extract_field_values(&index.fields, &row);
            // A PRIMARY KEY/UNIQUE index's own declared fields are
            // already unique by definition — that's what makes the
            // backing BPlusTree's own duplicate-key rejection enforce
            // the constraint. A plain index has no such guarantee, so
            // its key has to carry the row's own identity too, or two
            // rows sharing the same indexed value would collide as
            // the same physical key (see Schema::create_index's own
            // doc comment, which this has to stay in lockstep with).
            if !index.is_primary && !index.is_unique {
                values.extend_from_slice(identity.values());
            }
            let index_key = DBIdType::Rec(IndexKey::new_from(&values)?);
            self.db.insert(
                index.db_table_id,
                Tuple::new_with(index_key, &to_allocvec(&identity)?, Some(txn.id()), None),
                txn,
            )?;
        }

        // Stamped with the table's CURRENT version — every new
        // insert is always written in the table's latest shape;
        // only rows written before an ALTER TABLE carry an older
        // version (see VersionedRow, SqlTable::reproject). Moves
        // `row` (see new_from_owned) — must be the last thing that
        // touches it.
        let row_data = VersionedRow {
            version: table.version(),
            values: IndexKey::new_from_owned(row)?,
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
        // Best-effort, matching SchemaStats' own approximate nature
        // (bloom-filter uniqueness, a skippable send — see log_stat's
        // own doc comment): logged right after this row's own insert
        // succeeds, not after the whole batch/transaction commits, so
        // a row whose transaction later rolls back may still count
        // once here. Fine for stats used to guide query planning, not
        // worth threading commit/rollback awareness into an already-
        // lossy background collector for.
        if let Some(stats) = self.stats.lock().as_ref() {
            stats.log_stat(table.db_table_id, row_data.values.clone());
        }
        Ok(())
    }

    // The mirror of insert_one_row_in_txn: removes `row_key`'s entry
    // from the main table AND every index's own backing table. `row`
    // must be the row's CURRENT full field values (table-field order)
    // — needed to recompute each index's key the same way
    // insert_one_row_in_txn built it, since an index's key is derived
    // from the row's data, not stored anywhere that could be looked up
    // by row_key alone.
    //
    // `row_key` must be the row's own already-known identity, never
    // recomputed here via Schema::row_key — for a table with no
    // PRIMARY KEY, row_key mints a *fresh* auto-increment id on every
    // call (see its own doc comment), which would delete nothing and
    // silently leave the actual row behind. Callers of this function
    // (update_rows_matching, delete_rows_matching) get the real one from
    // Statement::execute's own TableSource-backed scan (Source::last_id),
    // not by recomputing it.
    fn delete_one_row_in_txn(
        self: &Arc<Self>,
        table: &SqlTable,
        row: &[ValueItem],
        row_key: &DBIdType,
        txn: &Transaction,
    ) -> Result<(), SchemaError> {
        let identity = match row_key {
            DBIdType::Rec(ik) => ik.clone(),
            DBIdType::Int(n) => IndexKey::new_from(&[ValueItem::Integer(*n as i64)])?,
        };
        for index in &table.indices {
            let mut values = table.extract_field_values(&index.fields, row);
            if !index.is_primary && !index.is_unique {
                values.extend_from_slice(identity.values());
            }
            let index_key = DBIdType::Rec(IndexKey::new_from(&values)?);
            self.db.remove(index.db_table_id, index_key, txn)?;
        }
        self.db.remove(table.db_table_id, row_key.clone(), txn)?;
        Ok(())
    }

    // UPDATE's own entry point. `matches` is every row the WHERE clause
    // selected — Statement::execute finds them by running the exact same
    // TableSource(+WhereSource) pipeline SELECT itself uses (see
    // source::Source::last_id's own doc comment for why: so UPDATE/
    // DELETE automatically inherit whatever pushdown/optimization that
    // pipeline gains in the future, instead of a second, parallel scan
    // implementation of their own), pairing each row's real physical key
    // (Tuple::id, read straight off the scan) with its current field
    // values. `assignments` is (target field position, its already-
    // built new-value EvalExpr), one pair per SET item — evaluated here,
    // per matching row, against that row's own current values (so
    // `SET age = age + 1` reads the row it's updating, not some other
    // one). `txn` is always a real, already-active transaction by the
    // time this is called — Statement::execute guarantees one is open
    // for exactly as long as both the scan and these writes need to
    // share one snapshot (see its own with_active_txn).
    //
    // Implemented as delete-then-insert per row, not an in-place
    // store::Db::update — a PRIMARY KEY column can be part of a SET
    // assignment, which moves the row to a different physical key, and
    // any changed *indexed* column needs its old index entry removed
    // and a new one written regardless; delete-then-insert reuses
    // insert_one_row_in_txn's already-correct index-maintenance for the
    // "write the new version" half instead of a second, more delicate
    // in-place-index-update implementation that only pays for itself
    // when nothing indexed actually changed.
    pub(crate) fn update_rows_matching(
        self: &Arc<Self>,
        table_name: &str,
        matches: Vec<(DBIdType, IndexKey)>,
        mut assignments: Vec<(usize, crate::plan::eval::EvalExpr)>,
        txn: &Transaction,
    ) -> Result<usize, SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let fields = table.fields();
        let mut count = 0usize;
        for (old_key, current) in matches {
            let mut new_row = current.values().to_vec();
            for (pos, expr) in &mut assignments {
                let raw = expr.eval(std::slice::from_ref(&current), 0)?;
                let field = &fields[*pos];
                let item = crate::table::coerce_selected_value(raw, field.datatype)?;
                if item == ValueItem::Null && !field.nullable {
                    return Err(SchemaError::UserError(format!(
                        "Column {:?} cannot be null",
                        field.name
                    )));
                }
                new_row[*pos] = item;
            }
            self.delete_one_row_in_txn(&table, current.values(), &old_key, txn)?;
            self.insert_one_row_in_txn(&table, new_row, txn)?;
            count += 1;
        }
        Ok(count)
    }

    // DELETE's own entry point — see update_rows_matching's own doc
    // comment on where `matches`/`txn` come from and why.
    pub(crate) fn delete_rows_matching(
        self: &Arc<Self>,
        table_name: &str,
        matches: Vec<(DBIdType, IndexKey)>,
        txn: &Transaction,
    ) -> Result<usize, SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let mut count = 0usize;
        for (key, row) in matches {
            self.delete_one_row_in_txn(&table, row.values(), &key, txn)?;
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
    #[allow(unused)]
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
        let start = Instant::now();
        let mut count = 0;
        let mut cursor = match txn {
            Some(txn) => self.db.table_scan_in_txn(table.db_table_id, txn)?,
            None => self.db.table_scan(table.db_table_id)?,
        };
        while let Some(tuple) = cursor.next()? {
            let row = from_bytes::<VersionedRow>(tuple.data())?;
            rows.push(table.reproject(&row)?.values().to_vec());
            count += 1;
        }
        let message = format!("{} rows in {} ms", count, start.elapsed().as_millis());
        Ok(ResultSet::new(columns, rows, message))
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
            let value = &row.values()[pos];
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

    // CREATE [UNIQUE] INDEX <name> ON <table>(<columns>) against a table
    // that (unlike CREATE TABLE's own inline PRIMARY KEY/UNIQUE) may
    // already have rows in it — so this has to both create the index's
    // own backing store table AND backfill it, not just create an empty
    // one.
    //
    // For a UNIQUE index, the key is just the indexed columns' own
    // values — same as PRIMARY KEY/UNIQUE has always worked, relying on
    // the backing BPlusTree's own duplicate-key rejection to enforce the
    // constraint for free. A plain, non-unique index has no such
    // guarantee, so two rows sharing the same indexed value would
    // otherwise collide as the same physical key — there's no multi-
    // value-per-key facility in the store layer to fall back on. The fix
    // doesn't need one: every row already has a unique identity (its own
    // PRIMARY KEY, or the generated rowid when there isn't one — see
    // Schema::row_key), so appending that to a non-unique index's key
    // makes it unique by construction, the same way a composite PRIMARY
    // KEY already works today. insert_rows_in_txn's own per-index loop
    // has to make the identical choice for every row inserted after this
    // index exists — the two must stay in lockstep.
    pub(crate) fn create_index(
        self: &Arc<Self>,
        table_name: &str,
        name: String,
        column_names: &[String],
        is_unique: bool,
    ) -> Result<(), SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        if column_names.is_empty() {
            return Err(SchemaError::UserError(
                "CREATE INDEX needs at least one column".into(),
            ));
        }
        let mut fields = Vec::with_capacity(column_names.len());
        for cname in column_names {
            let f = table
                .fields()
                .iter()
                .find(|f| &f.name == cname)
                .cloned()
                .ok_or_else(|| {
                    SchemaError::UserError(format!(
                        "Table {table_name:?} has no column named {cname:?}"
                    ))
                })?;
            fields.push(f);
        }
        if table
            .indices
            .iter()
            .any(|i| i.name.as_deref() == Some(name.as_str()))
        {
            return Err(SchemaError::UserError(format!(
                "Duplicate index name: {name}"
            )));
        }
        let qualified = self.qualify(&name);
        if self.db.table_id_by_name(&qualified)?.is_some() {
            return Err(SchemaError::BadTableName(format!(
                "Index name {name} is already in use"
            )));
        }

        let identity_size = table.identity_size();
        let sizing_index = SqlIndex {
            name: Some(name.clone()),
            db_table_id: TableIdType::none(),
            is_primary: false,
            is_unique,
            fields: fields.clone().into(),
        };
        self.check_key_width(
            &format!("index {name:?} on table {table_name:?}"),
            sizing_index.key_size(identity_size),
        )?;
        let index_table_id = self.db.create_table_with_index_entry_size(
            qualified.clone(),
            sizing_index.size(identity_size) as u64,
        )?;

        let backfill: Result<(), SchemaError> = (|| {
            let txn = self.db.begin()?;
            let mut cursor = self.db.table_scan_in_txn(table.db_table_id, &txn)?;
            while let Some(tuple) = cursor.next()? {
                let versioned = from_bytes::<VersionedRow>(tuple.data())?;
                let row = table.reproject(&versioned)?;
                let identity = match tuple.id() {
                    DBIdType::Rec(ik) => ik.clone(),
                    DBIdType::Int(n) => IndexKey::new_from(&[ValueItem::Integer(*n as i64)])?,
                };
                let mut values = table.extract_field_values(&fields, row.values());
                if !is_unique {
                    values.extend_from_slice(identity.values());
                }
                let index_key = DBIdType::Rec(IndexKey::new_from(&values)?);
                self.db
                    .insert(
                        index_table_id,
                        Tuple::new_with(index_key, &to_allocvec(&identity)?, Some(txn.id()), None),
                        &txn,
                    )
                    .map_err(|e| match e {
                        StoreError::DuplicateKey(_) if is_unique => {
                            SchemaError::UserError(format!(
                                "cannot create unique index {name:?}: table {table_name:?} has \
                             duplicate values for column(s) {column_names:?}"
                            ))
                        }
                        other => other.into(),
                    })?;
            }
            self.db.commit(txn)?;
            Ok(())
        })();
        if let Err(e) = backfill {
            let _ = self.db.drop_table(&qualified);
            return Err(e);
        }

        let index = SqlIndex {
            name: Some(name),
            db_table_id: index_table_id,
            is_primary: false,
            is_unique,
            fields: fields.into(),
        };
        if let Err(e) = self.alter_table(table_name, |t| t.alter_add_index(index)) {
            let _ = self.db.drop_table(&qualified);
            return Err(e);
        }
        Ok(())
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
    // (which rolls the whole batch back on any single failure).
    //
    // TXN_SIMPLIFICATION_PLAN.md phase 7: rows are loaded COPY_BATCH_ROWS
    // per transaction (one fsync per batch instead of one per row, which
    // pinned a single-connection load to the disk's fsync rate); a batch
    // that fails is replayed row by row so the bad row(s) are skipped and
    // counted while the rest of that batch still loads.
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
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        let fields = table.fields();

        let mut loaded = 0usize;
        let mut failed = 0usize;
        let mut batch: Vec<Vec<ValueItem>> = Vec::with_capacity(COPY_BATCH_ROWS);
        let flush =
            |batch: &mut Vec<Vec<ValueItem>>, loaded: &mut usize, failed: &mut usize| {
                if batch.is_empty() {
                    return;
                }
                let rows = std::mem::take(batch);
                match self.insert_rows(table_name, rows.clone(), None) {
                    Ok(n) => *loaded += n,
                    Err(_) => {
                        for row in rows {
                            match self.insert_rows(table_name, vec![row], None) {
                                Ok(_) => *loaded += 1,
                                Err(_) => *failed += 1,
                            }
                        }
                    }
                }
            };
        for record in reader.records() {
            let row = record
                .map_err(|e| SchemaError::UserError(e.to_string()))
                .and_then(|record| csv_record_to_row(&record, fields));
            match row {
                Ok(row) => batch.push(row),
                Err(_) => failed += 1,
            }
            if batch.len() >= COPY_BATCH_ROWS {
                flush(&mut batch, &mut loaded, &mut failed);
            }
        }
        flush(&mut batch, &mut loaded, &mut failed);
        Ok((loaded, failed))
    }

    // Entry point for `ANALYZE TABLE <name>` / `ANALYZE TABLES` (stmt.rs).
    // Unlike insert_rows_in_txn's incidental, sampled log_stat calls (fed
    // by live traffic, lossy under load by design), this is a deliberate,
    // exhaustive rebuild: drop whatever was collected before, start fresh
    // (same shape a brand-new table gets), then replay every row
    // currently in the table through record_row_sync — the synchronous
    // counterpart to log_stat that can't silently drop rows the way the
    // background collector's capacity-1 channel would under a tight
    // scan loop (see record_row_sync's own comment).
    pub(crate) fn analyze_table(self: &Arc<Self>, table_name: &str) -> Result<(), SchemaError> {
        let table = self.get_table(table_name).ok_or_else(|| {
            SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
        })?;
        let guard = self.stats.lock();
        // None means this schema is already closing (persist_and_
        // shutdown_stats took it) — nothing left to analyze into.
        let Some(stats) = guard.as_ref() else {
            return Ok(());
        };
        stats.drop_table_stats(table.db_table_id);
        stats.add_table(table.clone())?;
        let mut cursor = self.db.table_scan(table.db_table_id)?;
        while let Some(tuple) = cursor.next()? {
            let row = from_bytes::<VersionedRow>(tuple.data())?;
            stats.record_row_sync(table.db_table_id, table.reproject(&row)?);
        }
        Ok(())
    }

    pub(crate) fn get_table_stats(
        self: Arc<Self>,
        table: TableIdType,
    ) -> Result<Option<TableStat>, SchemaError> {
        Ok(self
            .stats
            .lock()
            .as_ref()
            .and_then(|t| t.get_table_stats(table)))
    }

    // Backing data for `!show table stats` (squeal-cli, via
    // Connection::table_stats_report) — one row per (table, column) with
    // tracked stats, plus one placeholder row for a table with none
    // (e.g. every column is a lone PRIMARY KEY/UNIQUE field — see
    // SchemaStats::table_data). Columns: table, column, row_count,
    // unique, nulls, min, max. `min`/`max` pass the underlying ValueItem
    // straight through rather than pre-formatting to a string — whatever
    // renders this (ResultSet::rows_as_strings today) already knows how
    // to display any ValueItem variant.
    pub(crate) fn table_stats_rows(self: &Arc<Self>) -> Vec<Vec<ValueItem>> {
        let guard = self.stats.lock();
        let Some(stats) = guard.as_ref() else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        for name in self.list_tables() {
            let Some(table) = self.get_table(&name) else {
                continue;
            };
            let Some(stat) = stats.get_table_stats(table.db_table_id) else {
                continue;
            };
            if stat.col_stats.is_empty() {
                rows.push(vec![
                    ValueItem::Str((name, MAX_TABLE_NAME_LEN as u32)),
                    ValueItem::Str(("(no tracked columns)".into(), MAX_TABLE_NAME_LEN as u32)),
                    ValueItem::Integer(stat.row_count as i64),
                    ValueItem::Null,
                    ValueItem::Null,
                    ValueItem::Null,
                    ValueItem::Null,
                ]);
                continue;
            }
            let mut cols: Vec<_> = stat.col_stats.iter().collect();
            cols.sort_by_key(|(field_index, _)| **field_index);
            for (_, c) in cols {
                rows.push(vec![
                    ValueItem::Str((name.clone(), MAX_TABLE_NAME_LEN as u32)),
                    ValueItem::Str((c.name.clone(), MAX_TABLE_NAME_LEN as u32)),
                    ValueItem::Integer(stat.row_count as i64),
                    ValueItem::Integer(c.unique as i64),
                    ValueItem::Integer(c.null as i64),
                    c.min.clone(),
                    c.max.clone(),
                ]);
            }
        }
        rows
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
        // Arc::make_mut: get_table's Arc is shared with the map's own
        // entry (and possibly other readers), so mutating through it
        // directly isn't an option — this clones the underlying SqlTable
        // only if some other reference is still alive (copy-on-write),
        // giving `apply` a private copy to mutate. Same net effect as
        // the old always-owned `table`: the map's own entry is
        // untouched until the write below explicitly replaces it, so a
        // failure partway through (e.g. self.db.update erroring) still
        // never leaves it half-mutated.
        apply(Arc::make_mut(&mut table))?;

        let txn = self.db.begin()?;
        let ik = IndexKey::new_from(&[ValueItem::Str((name.clone(), MAX_TABLE_NAME_LEN as u32))])?;
        self.db.update(
            self.sys_table_id,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &table.encode_catalog_row()?,
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
        DataType::Boolean => cell
            .parse()
            .map(ValueItem::Boolean)
            .map_err(|_| SchemaError::UserError(format!("invalid boolean: {cell:?}"))),
        DataType::Blob(_) | DataType::Unsupported | DataType::Null => Err(SchemaError::UserError(
            format!("CSV loading into a {datatype:?} column is not supported yet"),
        )),
    }
}
