use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption, CreateTable};
use store::db::DBFile;
use store::table::TableIdType;
use store::valueitem::{IndexKey, ValueItem};

use crate::constant::MAX_TABLE_NAME_LEN;
use crate::datatype::DataType;
use crate::error::SchemaError;
use crate::schema_ops::schema::Schema;

// A store-level Tuple wraps whatever's actually stored (row or index
// entry) with its own key (DBIdType — a Rec(IndexKey) key costs more
// than the raw field bytes alone: an enum tag, the IndexKey's own
// Vec-length prefix, and one enum tag per ValueItem), Option<TransactionId>,
// Option<UndoId>, a flags byte, and postcard's own length-prefix on the
// data field — none of which the raw sum of ValueItem::size() calls
// below accounts for. Since store's own historical default entry
// budget for a plain Int key is 64 bytes (MAX_ENTRY_BYTES), padding by
// the same order of magnitude keeps composite Rec keys (which cost
// more per field than a bare Int) comfortably covered without needing
// to hand-derive postcard's exact encoding overhead.
const ENTRY_OVERHEAD_BYTES: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlTable {
    pub(crate) name: String,
    // Append-only history of this table's column layout — index 0 is the
    // shape CREATE TABLE built, each ALTER TABLE (see alter_add_column/
    // alter_drop_column/alter_rename_column) pushes one more, never
    // mutating an earlier entry. This is what lets ALTER avoid rewriting
    // every existing row: an old row stays encoded against whichever
    // version was current when it was written (see VersionedRow), and
    // gets reprojected onto the table's current version — the last
    // entry here — only at read time (see Schema::select_all).
    pub(crate) versions: Vec<SchemaVersion>,
    pub(crate) indices: Vec<SqlIndex>,
    pub(crate) foreign_keys: Vec<SqlForeignKey>,
    // The table's own row-storage backing table — distinct from each
    // index's own db_table_id below. Set by Schema::create_table once
    // the backing table actually exists; TableIdType::none() until then
    // (mirrors SqlIndex::db_table_id's own convention).
    pub(crate) db_table_id: TableIdType,
    // The next never-yet-used Field::id — starts at the initial column
    // count (see TableBuilder::build) and only ever increases, one per
    // alter_add_column, even across a column that's since been dropped:
    // ids are never reused, so a dropped-then-re-added column of the
    // same name still gets a fresh id and can't be confused with the
    // original by reproject.
    pub(crate) next_field_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub(crate) fields: Vec<Arc<Field>>,
}

// What Schema::insert_rows_in_txn actually writes as a row's stored
// payload, replacing the bare IndexKey it used before ALTER TABLE
// existed: `values` alone is positional with no record of which
// SchemaVersion that position order matches, so once a table can have
// more than one version, decoding needs to know which one a given row
// was encoded under. New inserts always stamp SqlTable::version()
// (the current/latest version); older rows keep whatever version was
// current when they were written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionedRow {
    pub(crate) version: u32,
    pub(crate) values: IndexKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlIndex {
    pub(crate) name: Option<String>,
    pub(crate) db_table_id: TableIdType,
    pub(crate) is_primary: bool,
    pub(crate) is_unique: bool,
    pub(crate) fields: Vec<Arc<Field>>,
}

// A single-column foreign key — this table's `column` must, for every
// non-NULL value, match some row's `ref_column` in `ref_table` (see
// Schema::insert_rows_in_txn's own enforcement and
// Schema::add_foreign_key's existing-row backfill check). Not
// versioned, unlike Field/SchemaVersion — like SqlIndex, it's simpler
// to just require dropping the constraint before renaming/dropping
// either column it touches (see SqlTable::fk_referencing_column) than
// to track its own history across a rename.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlForeignKey {
    pub(crate) name: Option<String>,
    pub(crate) column: String,
    pub(crate) ref_table: String,
    pub(crate) ref_column: String,
}

#[derive(Debug, Clone, Default)]
struct IndexHolder {
    name: Option<String>,
    is_primary: bool,
    is_unique: bool,
    fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    // A permanent identity, distinct from `name` and from this field's
    // position in any particular SchemaVersion — assigned once (see
    // SqlTable::next_field_id) and never reused or changed afterward,
    // including across a RENAME COLUMN. This is what lets
    // SqlTable::reproject bridge a renamed column between an old row's
    // stored version and the table's current one: matching by `name`
    // alone can't, since the whole point of a rename is that the name
    // differs between those two versions.
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) datatype: DataType,
    pub(crate) nullable: bool,
    // A literal value, not a re-evaluated expression — captured once,
    // at CREATE TABLE / ALTER TABLE ADD COLUMN time. Used two ways: (1)
    // an ordinary INSERT that omits this column falls back to it (see
    // rows_from_insert), same as any other database's DEFAULT; (2)
    // reprojecting a row written under an older SchemaVersion that
    // predates this column falls back to it too (see
    // SqlTable::reproject) — the "backfill" ALTER TABLE ADD COLUMN
    // needs without rewriting existing rows.
    pub(crate) default: Option<ValueItem>,
}

pub struct TableBuilder {
    name: Option<String>,
    fields: Vec<Field>,
    indices: Vec<IndexHolder>,
    foreign_keys: Vec<SqlForeignKey>,
}

impl Field {
    // `id` starts at 0, a placeholder — Field::new/TryFrom<&ColumnDef>
    // build a field's *content* without knowing its permanent id yet,
    // since that depends on which table it ends up in and (for ALTER
    // TABLE ADD COLUMN) that table's current next_field_id, neither of
    // which is available this early. Callers that actually place a
    // field into a table (TableBuilder::build, SqlTable::alter_add_column)
    // must call with_id afterward to assign the real one.
    pub fn new(
        name: String,
        datatype: DataType,
        nullable: bool,
        default: Option<ValueItem>,
    ) -> Result<Field, SchemaError> {
        match datatype {
            DataType::Blob(l) | DataType::Str(l) if l > 4 * 1024 * 1024 => {
                return Err(SchemaError::UserError("Max field size is 4MB.".into()));
            }
            _ => {}
        }
        if let Some(d) = &default
            && *d == ValueItem::Null
            && !nullable
        {
            return Err(SchemaError::UserError(format!(
                "Column {name:?} is NOT NULL and cannot DEFAULT to NULL"
            )));
        }
        Ok(Self {
            id: 0,
            name,
            nullable,
            datatype,
            default,
        })
    }

    pub(crate) fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            name: None,
            fields: vec![],
            indices: vec![],
            foreign_keys: vec![],
        }
    }

    pub fn with_name(&mut self, name: String) -> &mut Self {
        self.name = Some(name);
        self
    }

    pub fn with_field(&mut self, field: &Field) -> &mut Self {
        self.fields.push(field.clone());
        self
    }

    pub fn with_foreign_key(
        &mut self,
        column: String,
        name: Option<String>,
        ref_table: String,
        ref_column: String,
    ) -> &mut Self {
        self.foreign_keys.push(SqlForeignKey {
            name,
            column,
            ref_table,
            ref_column,
        });
        self
    }

    pub fn with_index(
        &mut self,
        fields: &[String],
        name: Option<String>,
        is_primary: bool,
        is_unique: bool,
    ) -> &mut Self {
        self.indices.push(IndexHolder {
            name,
            is_primary,
            is_unique,
            fields: fields.to_vec(),
        });
        self
    }

    pub fn build(self) -> Result<SqlTable, SchemaError> {
        if self.name.is_none() {
            return Err(SchemaError::BadTableName("Table name missing".into()));
        }
        if let Some(name) = &self.name
            && name.len() > MAX_TABLE_NAME_LEN
        {
            return Err(SchemaError::BadTableName(format!(
                "Table name cannot be longer than {}",
                MAX_TABLE_NAME_LEN
            )));
        }
        // Built incrementally (not a one-shot `.collect()`) so a duplicate
        // name can be caught here instead of silently overwriting the
        // earlier field the way collecting straight into a HashMap did.
        let mut field_names: HashMap<String, &Field> = HashMap::with_capacity(self.fields.len());
        for f in &self.fields {
            if f.name.len() >= 128 {
                return Err(SchemaError::UserError(
                    "Max field name length is 128".into(),
                ));
            }
            if field_names.insert(f.name.clone(), f).is_some() {
                return Err(SchemaError::UserError(format!(
                    "Duplicate field name: {}",
                    f.name
                )));
            }
        }
        for i in &self.indices {
            if i.is_primary && !i.is_unique {
                return Err(SchemaError::UserError(format!(
                    "Primary index must be unique. {:?}",
                    i
                )));
            }
            if i.is_primary || i.is_unique {
                for f in &i.fields {
                    if let Some(f) = field_names.get(f)
                        && f.nullable
                    {
                        return Err(SchemaError::UserError(
                            "Unique or primary keys cannot be nullable".into(),
                        ));
                    }
                }
            }
            if !i.fields.iter().all(|i| field_names.contains_key(i)) {
                return Err(SchemaError::UserError(format!(
                    "Index {:?}: has field names not in table",
                    i
                )));
            }
        }
        // Only the LOCAL side is checkable here — from_sql doesn't know
        // about any other table. A foreign key referencing another
        // table gets its ref_table/ref_column validated by
        // Schema::create_table instead, before this table is actually
        // persisted; a self-referential one (ref_table == this table)
        // is fully validated below, once this table's own fields/
        // indices are built.
        let mut fk_names: HashMap<String, ()> = HashMap::with_capacity(self.foreign_keys.len());
        for fk in &self.foreign_keys {
            if !field_names.contains_key(&fk.column) {
                return Err(SchemaError::UserError(format!(
                    "Foreign key references unknown column: {}",
                    fk.column
                )));
            }
            if let Some(name) = &fk.name
                && fk_names.insert(name.clone(), ()).is_some()
            {
                return Err(SchemaError::UserError(format!(
                    "Duplicate foreign key constraint name: {name}"
                )));
            }
        }

        // Ids assigned by position here, 0..N — this is the one place a
        // brand-new table's fields get their permanent identity (see
        // Field::id); every later ADD COLUMN continues from
        // next_field_id instead of restarting at 0.
        let fields = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| Arc::new(f.clone().with_id(i as u32)))
            .collect::<Vec<_>>();
        let next_field_id = fields.len() as u32;

        let mut table = SqlTable {
            name: self.name.as_ref().unwrap().clone(),
            versions: vec![SchemaVersion { fields }],
            indices: vec![],
            foreign_keys: vec![],
            db_table_id: TableIdType::none(),
            next_field_id,
        };
        for i in &self.indices {
            let mut index = SqlIndex {
                name: i.name.clone(),
                is_primary: i.is_primary,
                is_unique: i.is_unique,
                fields: vec![],
                db_table_id: TableIdType::none(),
            };
            for index_f in &i.fields {
                if let Some(f) = table.fields().iter().find(|f| f.name == *index_f) {
                    index.fields.push(f.clone());
                }
            }
            table.indices.push(index);
        }
        for fk in &self.foreign_keys {
            // Case-insensitive: table names are lowercased everywhere
            // else in this crate (see e.g. Statement's own table-name
            // handling), so "REFERENCES Users" naming this same table
            // by a different case must still count as self-referential.
            if fk.ref_table.eq_ignore_ascii_case(&table.name) {
                let column_datatype = table
                    .fields()
                    .iter()
                    .find(|f| f.name == fk.column)
                    .expect("checked present in field_names above")
                    .datatype;
                table.validate_foreign_key_target(&fk.ref_column, column_datatype)?;
            }
            table.foreign_keys.push(fk.clone());
        }
        Ok(table)
    }
}

impl TryFrom<&ColumnDef> for Field {
    type Error = SchemaError;

    fn try_from(value: &ColumnDef) -> Result<Self, SchemaError> {
        // SQL columns are nullable by default; NOT NULL is what opts a
        // column out, not the other way around.
        let nullable = !value
            .options
            .iter()
            .any(|o| matches!(o.option, ColumnOption::NotNull));
        let datatype: DataType = value.data_type.clone().into();
        let default = value
            .options
            .iter()
            .find_map(|o| match &o.option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            })
            .map(|expr| expr_to_value_item(expr, datatype))
            .transpose()?;
        // Routed through Field::new (not a struct literal) so its size-cap
        // and NOT-NULL-vs-DEFAULT-NULL validation actually run for
        // SQL-parsed columns.
        Field::new(value.name.value.clone(), datatype, nullable, default)
    }
}

// A column's own inline PRIMARY KEY / UNIQUE option (e.g. `id INTEGER
// PRIMARY KEY`), as opposed to a table-level constraint clause (`PRIMARY
// KEY(id)`). sqlparser represents both with the same PrimaryKeyConstraint/
// UniqueConstraint types, but the inline form's own `columns` field is
// always empty — the column is implicit ("this one"), not named — so it
// has to be supplied here from the ColumnDef itself rather than reused via
// the same `From<&PrimaryKeyConstraint>`/`From<&UniqueConstraint>` impls
// the table-level constraints go through.
fn inline_indices(column: &ColumnDef) -> Vec<IndexHolder> {
    column
        .options
        .iter()
        .filter_map(|o| match &o.option {
            ColumnOption::PrimaryKey(c) => Some(IndexHolder {
                name: c.name.clone().map(|n| n.to_string()),
                is_primary: true,
                is_unique: true,
                fields: vec![column.name.value.clone()],
            }),
            ColumnOption::Unique(c) => Some(IndexHolder {
                name: c.name.clone().map(|n| n.to_string()),
                is_primary: false,
                is_unique: true,
                fields: vec![column.name.value.clone()],
            }),
            _ => None,
        })
        .collect()
}

// Converts a parsed FOREIGN KEY constraint (table-level or, via
// `implicit_column`, the inline column form — see inline_foreign_key)
// into a SqlForeignKey, rejecting every form this crate doesn't support
// yet: composite (multi-column) keys, ON DELETE/ON UPDATE (moot
// anyway — this engine has no UPDATE/DELETE to cascade/restrict
// against), MATCH, characteristics (e.g. DEFERRABLE), and MySQL's
// separate index_name. Shared with Statement::execute's ALTER TABLE ADD
// CONSTRAINT/ADD FOREIGN KEY parsing (see stmt.rs's parse_alter_table),
// so both entry points reject exactly the same unsupported forms.
pub(crate) fn foreign_key_from_constraint(
    fk: &sqlparser::ast::ForeignKeyConstraint,
    implicit_column: Option<&str>,
) -> Result<SqlForeignKey, SchemaError> {
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "FOREIGN KEY only supports a single-column reference right now — {what} is not \
             supported yet"
        )))
    };
    if fk.index_name.is_some() {
        return unsupported("a MySQL-style index name");
    }
    if fk.on_delete.is_some() || fk.on_update.is_some() {
        return unsupported("ON DELETE/ON UPDATE");
    }
    if fk.match_kind.is_some() {
        return unsupported("MATCH FULL/PARTIAL/SIMPLE");
    }
    if fk.characteristics.is_some() {
        return unsupported("constraint characteristics (e.g. DEFERRABLE)");
    }
    let column = match (implicit_column, fk.columns.as_slice()) {
        (Some(c), []) => c.to_string(),
        (None, [c]) => c.value.clone(),
        _ => return unsupported("multiple columns"),
    };
    let ref_column = match fk.referred_columns.as_slice() {
        [c] => c.value.clone(),
        _ => return unsupported("multiple referenced columns"),
    };
    Ok(SqlForeignKey {
        // Lowercased, matching every other identifier in this crate
        // (table/column names) — DROP CONSTRAINT's own name lookup
        // lowercases too (see stmt.rs's parse_alter_table), so this has
        // to agree or a mixed-case constraint name would never match.
        name: fk.name.clone().map(|n| n.to_string().to_lowercase()),
        column,
        ref_table: fk.foreign_table.to_string().to_lowercase(),
        ref_column,
    })
}

// Mirrors inline_indices: a column's own inline REFERENCES (e.g.
// `customer_id INTEGER REFERENCES customers(id)`) parses to the same
// ForeignKeyConstraint as a table-level FOREIGN KEY(...) clause, but
// with an empty `columns` — the column is implicit ("this one").
fn inline_foreign_key(column: &ColumnDef) -> Option<Result<SqlForeignKey, SchemaError>> {
    column.options.iter().find_map(|o| match &o.option {
        ColumnOption::ForeignKey(fk) => {
            Some(foreign_key_from_constraint(fk, Some(&column.name.value)))
        }
        _ => None,
    })
}

impl SqlTable {
    pub(crate) fn from_sql<F>(db: &Arc<Schema<F>>, value: CreateTable) -> Result<Self, SchemaError>
    where
        F: DBFile + 'static,
        F: DBFile<Item = F>,
    {
        let name = value.name.to_string().to_lowercase();
        if db.table_exists(&name) {
            return Err(SchemaError::BadTableName(format!("Table {name} exists.")));
        }
        // A Vec, not a HashMap: declaration order must survive into the
        // built table (SELECT *, positional INSERT rely on it) — build()
        // is what rejects a duplicate name now, instead of a HashMap
        // silently keeping whichever column happened to collect last.
        let fields: Vec<Field> = value
            .columns
            .iter()
            .map(Field::try_from)
            .collect::<Result<_, _>>()?;
        let mut indices = value
            .constraints
            .iter()
            .filter_map(|t| {
                let index: Option<IndexHolder> = match t {
                    sqlparser::ast::TableConstraint::Unique(unique_constraint) => {
                        Some(unique_constraint.into())
                    }
                    sqlparser::ast::TableConstraint::PrimaryKey(primary_key_constraint) => {
                        Some(primary_key_constraint.into())
                    }
                    _ => None,
                };
                index
            })
            .collect::<Vec<_>>();
        for c in &value.columns {
            indices.extend(inline_indices(c));
        }
        let mut foreign_keys = value
            .constraints
            .iter()
            .filter_map(|t| match t {
                sqlparser::ast::TableConstraint::ForeignKey(fk) => {
                    Some(foreign_key_from_constraint(fk, None))
                }
                _ => None,
            })
            .collect::<Result<Vec<_>, _>>()?;
        for c in &value.columns {
            if let Some(fk) = inline_foreign_key(c) {
                foreign_keys.push(fk?);
            }
        }
        let mut tb = TableBuilder::new();
        tb.with_name(name);
        for f in &fields {
            tb.with_field(f);
        }
        for i in indices {
            tb.with_index(&i.fields, i.name, i.is_primary, i.is_unique);
        }
        for fk in foreign_keys {
            tb.with_foreign_key(fk.column, fk.name, fk.ref_table, fk.ref_column);
        }

        tb.build()
    }

    pub(crate) fn primary_key(&self) -> Option<&SqlIndex> {
        self.indices.iter().find(|i| i.is_primary)
    }

    // A single-column PRIMARY KEY or UNIQUE index on exactly `column` —
    // what a foreign key's ref_column must resolve to (standard SQL
    // requirement: you can only reference a column with a uniqueness
    // guarantee, otherwise "the referenced row" isn't well-defined).
    // Deliberately doesn't match a multi-column index whose fields
    // merely *include* `column` — v1 foreign keys are single-column
    // only, so the reference has to be resolvable from that one column
    // alone.
    pub(crate) fn unique_index_on(&self, column: &str) -> Option<&SqlIndex> {
        self.indices
            .iter()
            .find(|i| (i.is_primary || i.is_unique) && i.fields.len() == 1 && i.fields[0].name == column)
    }

    // Validates `ref_column` as a foreign key target on THIS table
    // (i.e. `self` is the referenced table): it must exist, be backed
    // by a single-column PRIMARY KEY/UNIQUE index (see unique_index_on),
    // and match `column_datatype` — a referencing column can't
    // meaningfully compare against a target of a different type.
    // Shared by TableBuilder::build (self-referential FKs, validated
    // locally) and Schema::create_table/add_foreign_key (FKs against a
    // different table, validated once that table's own SqlTable is in
    // hand).
    pub(crate) fn validate_foreign_key_target(
        &self,
        ref_column: &str,
        column_datatype: DataType,
    ) -> Result<(), SchemaError> {
        let field = self
            .fields()
            .iter()
            .find(|f| f.name == ref_column)
            .ok_or_else(|| {
                SchemaError::UserError(format!(
                    "Table {:?} has no column named {ref_column:?}",
                    self.name
                ))
            })?;
        if self.unique_index_on(ref_column).is_none() {
            return Err(SchemaError::UserError(format!(
                "Column {ref_column:?} on table {:?} must be a PRIMARY KEY or UNIQUE column \
                 to be a foreign key target",
                self.name
            )));
        }
        if field.datatype != column_datatype {
            return Err(SchemaError::UserError(format!(
                "Foreign key column type {column_datatype:?} does not match referenced column \
                 {ref_column:?}'s type {:?}",
                field.datatype
            )));
        }
        Ok(())
    }

    // Does `self` have any foreign key whose LOCAL column is `name`?
    // Used by alter_drop_column/alter_rename_column, same "drop the
    // constraint first" restriction as an indexed column.
    fn fk_referencing_local_column(&self, name: &str) -> Option<&SqlForeignKey> {
        self.foreign_keys.iter().find(|fk| fk.column == name)
    }

    // The table's current column layout — the last entry in `versions`.
    // Every place that used to read a flat `fields` list (row_size,
    // field_position, rows_from_insert, SELECT's own column list, ...)
    // goes through this now; it's always what "the table's columns"
    // means outside of decoding an old row (see reproject, the one
    // place that deliberately looks at an *older* version instead).
    pub(crate) fn fields(&self) -> &[Arc<Field>] {
        &self
            .versions
            .last()
            .expect("a table always has at least one schema version")
            .fields
    }

    // This table's current version number — 0 for a table that's never
    // been ALTERed, incrementing by one per ALTER TABLE. Stamped onto
    // every row written from here on (see VersionedRow) so a later
    // reproject knows which version's field layout the row's positional
    // values match.
    pub(crate) fn version(&self) -> u32 {
        (self.versions.len() - 1) as u32
    }

    fn fields_at(&self, version: u32) -> Option<&[Arc<Field>]> {
        self.versions.get(version as usize).map(|v| v.fields.as_slice())
    }

    // Decodes a stored row back into ValueItems in the table's CURRENT
    // field order, regardless of which (possibly older) version it was
    // written under — the read side of ALTER TABLE's whole point: a row
    // written before an ADD/DROP/RENAME COLUMN is never rewritten, so
    // this has to bridge whatever version it actually has to whatever
    // version the table is on now, every time it's read.
    //   - a field present in both versions: carried over positionally
    //     from the stored row.
    //   - a field only in the current version (added after this row was
    //     written): falls back to that field's own default, or NULL —
    //     the same "backfill" a real ALTER TABLE ADD COLUMN gives you
    //     without rewriting existing rows.
    //   - a field only in the row's stored version (dropped since):
    //     simply not carried over — the current version doesn't ask for
    //     it.
    pub(crate) fn reproject(&self, row: &VersionedRow) -> Result<Vec<ValueItem>, SchemaError> {
        let stored_fields = self.fields_at(row.version).ok_or_else(|| {
            SchemaError::UnknownError(format!(
                "row was written under schema version {} but table {:?} has no such version",
                row.version, self.name
            ))
        })?;
        let stored_values = row.values.values();
        if stored_values.len() != stored_fields.len() {
            return Err(SchemaError::UnknownError(format!(
                "row has {} value(s) but schema version {} of table {:?} declared {} field(s)",
                stored_values.len(),
                row.version,
                self.name,
                stored_fields.len()
            )));
        }
        Ok(self
            .fields()
            .iter()
            .map(|f| {
                // Matched by Field::id, not name — a renamed column's
                // stored (old) name and current name legitimately
                // differ, but its id never changes across a rename, so
                // this still finds it.
                match stored_fields.iter().position(|sf| sf.id == f.id) {
                    Some(pos) => stored_values[pos].clone(),
                    None => f.default.clone().unwrap_or(ValueItem::Null),
                }
            })
            .collect())
    }

    // Appends a new schema version with `field` added to the end of the
    // current column order. A NOT NULL column needs a DEFAULT here even
    // though CREATE TABLE doesn't require one for a NOT NULL column —
    // CREATE TABLE has no pre-existing rows to backfill; ALTER TABLE
    // might, and reproject needs *something* to hand back for every row
    // written before this column existed.
    pub(crate) fn alter_add_column(&mut self, field: Field) -> Result<(), SchemaError> {
        if self.fields().iter().any(|f| f.name == field.name) {
            return Err(SchemaError::UserError(format!(
                "Duplicate field name: {}",
                field.name
            )));
        }
        if !field.nullable && field.default.is_none() {
            return Err(SchemaError::UserError(format!(
                "Column {:?} is NOT NULL — ADD COLUMN on a table that may already have rows \
                 needs a DEFAULT to backfill existing rows with",
                field.name
            )));
        }
        let mut fields = self.fields().to_vec();
        fields.push(Arc::new(field.with_id(self.next_field_id)));
        self.next_field_id += 1;
        self.versions.push(SchemaVersion { fields });
        Ok(())
    }

    // Appends a new schema version with `name` removed. Refuses a
    // column that's part of any index (including the PRIMARY KEY, which
    // is just an index with is_primary set) — dropping it first, per
    // the equivalent restriction on renaming below, keeps every index's
    // own `fields` list (an Arc<Field> shared with the table's own
    // version-0 field, not re-derived per version) trivially still
    // correct without needing its own rewrite-on-ALTER logic.
    pub(crate) fn alter_drop_column(&mut self, name: &str) -> Result<(), SchemaError> {
        if !self.fields().iter().any(|f| f.name == name) {
            return Err(SchemaError::UserError(format!(
                "Table {:?} has no column named {name:?}",
                self.name
            )));
        }
        if let Some(idx) = self.index_referencing(name) {
            return Err(SchemaError::UserError(format!(
                "Column {name:?} is used by index {:?} — drop the index first",
                idx.name.clone().unwrap_or_else(|| "<unnamed>".into())
            )));
        }
        if let Some(fk) = self.fk_referencing_local_column(name) {
            return Err(SchemaError::UserError(format!(
                "Column {name:?} is used by foreign key {:?} — drop the foreign key first",
                fk.name.clone().unwrap_or_else(|| "<unnamed>".into())
            )));
        }
        let fields: Vec<Arc<Field>> = self
            .fields()
            .iter()
            .filter(|f| f.name != name)
            .cloned()
            .collect();
        if fields.is_empty() {
            return Err(SchemaError::UserError(
                "Cannot drop the last remaining column".into(),
            ));
        }
        self.versions.push(SchemaVersion { fields });
        Ok(())
    }

    // Appends a new schema version with `old_name` renamed to
    // `new_name` — same physical position and value, so unlike ADD/DROP
    // this never needs reproject's default/omit handling; every version
    // back to whichever one first introduced the column still decodes
    // fine, just under its old name at write time. Refuses a column
    // used by an index for the same reason alter_drop_column does.
    pub(crate) fn alter_rename_column(
        &mut self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        if !self.fields().iter().any(|f| f.name == old_name) {
            return Err(SchemaError::UserError(format!(
                "Table {:?} has no column named {old_name:?}",
                self.name
            )));
        }
        if self.fields().iter().any(|f| f.name == new_name) {
            return Err(SchemaError::UserError(format!(
                "Duplicate field name: {new_name}"
            )));
        }
        if let Some(idx) = self.index_referencing(old_name) {
            return Err(SchemaError::UserError(format!(
                "Column {old_name:?} is used by index {:?} — drop the index first",
                idx.name.clone().unwrap_or_else(|| "<unnamed>".into())
            )));
        }
        if let Some(fk) = self.fk_referencing_local_column(old_name) {
            return Err(SchemaError::UserError(format!(
                "Column {old_name:?} is used by foreign key {:?} — drop the foreign key first",
                fk.name.clone().unwrap_or_else(|| "<unnamed>".into())
            )));
        }
        let fields: Vec<Arc<Field>> = self
            .fields()
            .iter()
            .map(|f| {
                if f.name == old_name {
                    Arc::new(Field {
                        id: f.id,
                        name: new_name.to_string(),
                        datatype: f.datatype,
                        nullable: f.nullable,
                        default: f.default.clone(),
                    })
                } else {
                    f.clone()
                }
            })
            .collect();
        self.versions.push(SchemaVersion { fields });
        Ok(())
    }

    // Structural validation only (local column exists, no duplicate
    // constraint name) — Schema::add_foreign_key is responsible for
    // everything that needs to look outside this one table: does
    // ref_table/ref_column exist and qualify as a target (see
    // validate_foreign_key_target), and does every existing row's
    // `fk.column` value already have a match there.
    pub(crate) fn alter_add_foreign_key(&mut self, fk: SqlForeignKey) -> Result<(), SchemaError> {
        if !self.fields().iter().any(|f| f.name == fk.column) {
            return Err(SchemaError::UserError(format!(
                "Table {:?} has no column named {:?}",
                self.name, fk.column
            )));
        }
        if let Some(name) = &fk.name
            && self.foreign_keys.iter().any(|f| f.name.as_deref() == Some(name.as_str()))
        {
            return Err(SchemaError::UserError(format!(
                "Duplicate foreign key constraint name: {name}"
            )));
        }
        self.foreign_keys.push(fk);
        Ok(())
    }

    pub(crate) fn alter_drop_foreign_key(&mut self, name: &str) -> Result<(), SchemaError> {
        let pos = self
            .foreign_keys
            .iter()
            .position(|fk| fk.name.as_deref() == Some(name))
            .ok_or_else(|| {
                SchemaError::UserError(format!(
                    "Table {:?} has no foreign key constraint named {name:?}",
                    self.name
                ))
            })?;
        self.foreign_keys.remove(pos);
        Ok(())
    }

    fn index_referencing(&self, field_name: &str) -> Option<&SqlIndex> {
        self.indices
            .iter()
            .find(|i| i.fields.iter().any(|f| f.name == field_name))
    }

    // Every field's byte budget, summed (plus ENTRY_OVERHEAD_BYTES —
    // see its own comment) — the row-storage table's own
    // index_entry_size, mirroring SqlIndex::size() (indexed fields only)
    // but over the whole row, since the full row is what's stored there.
    pub(crate) fn row_size(&self) -> usize {
        self.fields().iter().map(|f| f.datatype.size()).sum::<usize>() + ENTRY_OVERHEAD_BYTES
    }

    // The position of `field` within this table's own declared field
    // order — used to pull a PRIMARY KEY/index's values back out of a
    // full row (built in that same order by rows_from_insert).
    fn field_position(&self, field: &Field) -> Option<usize> {
        self.fields().iter().position(|f| f.name == field.name)
    }

    // Extracts just the values for `fields` (e.g. a PRIMARY KEY or other
    // index's own field list) out of a full row, in `fields`' order.
    pub(crate) fn extract_field_values(
        &self,
        fields: &[Arc<Field>],
        row: &[ValueItem],
    ) -> Vec<ValueItem> {
        fields
            .iter()
            .map(|f| {
                let pos = self
                    .field_position(f)
                    .expect("index/primary-key fields are always a subset of the table's own fields");
                row[pos].clone()
            })
            .collect()
    }

    // Builds full rows (one Vec<ValueItem> per VALUES row, in this
    // table's own field order — not `insert`'s column order) from an
    // INSERT statement's AST. Columns omitted from an explicit column
    // list are filled with Null (rejected below if that column isn't
    // nullable).
    pub(crate) fn rows_from_insert(
        &self,
        insert: &sqlparser::ast::Insert,
    ) -> Result<Vec<Vec<ValueItem>>, SchemaError> {
        let target_fields: Vec<&Arc<Field>> = if insert.columns.is_empty() {
            self.fields().iter().collect()
        } else {
            insert
                .columns
                .iter()
                .map(|c| {
                    let name = c.to_string().to_lowercase();
                    self.fields().iter().find(|f| f.name == name).ok_or_else(|| {
                        SchemaError::UserError(format!(
                            "Table {:?} has no column named {name:?}",
                            self.name
                        ))
                    })
                })
                .collect::<Result<_, _>>()?
        };

        let query = insert.source.as_ref().ok_or_else(|| {
            SchemaError::UserError("INSERT without a VALUES clause is not supported".into())
        })?;
        let values = match query.body.as_ref() {
            sqlparser::ast::SetExpr::Values(v) => v,
            _ => {
                return Err(SchemaError::UserError(
                    "Only INSERT ... VALUES (...) is supported".into(),
                ));
            }
        };

        let mut rows = Vec::with_capacity(values.rows.len());
        for row in &values.rows {
            let exprs = &row.content;
            if exprs.len() != target_fields.len() {
                return Err(SchemaError::UserError(format!(
                    "Expected {} value(s), got {}",
                    target_fields.len(),
                    exprs.len()
                )));
            }
            let mut by_name: HashMap<&str, ValueItem> = HashMap::with_capacity(exprs.len());
            for (field, expr) in target_fields.iter().zip(exprs) {
                let item = expr_to_value_item(expr, field.datatype)?;
                if item == ValueItem::Null && !field.nullable {
                    return Err(SchemaError::UserError(format!(
                        "Column {:?} cannot be null",
                        field.name
                    )));
                }
                by_name.insert(field.name.as_str(), item);
            }
            let fields = self.fields();
            let mut full_row = Vec::with_capacity(fields.len());
            for f in fields {
                match by_name.remove(f.name.as_str()) {
                    Some(v) => full_row.push(v),
                    // DEFAULT applies to any omitted column, not just a
                    // backfilled pre-ALTER row (see Field::default) —
                    // checked before the plain-NULL fallback so a NOT
                    // NULL column with a DEFAULT still works via an
                    // explicit column list that leaves it out.
                    None if f.default.is_some() => {
                        full_row.push(f.default.clone().expect("checked Some above"))
                    }
                    None if f.nullable => full_row.push(ValueItem::Null),
                    None => {
                        return Err(SchemaError::UserError(format!(
                            "Column {:?} has no value and is not nullable",
                            f.name
                        )));
                    }
                }
            }
            rows.push(full_row);
        }
        Ok(rows)
    }
}

// Converts a single VALUES-clause literal into a ValueItem matching
// `datatype` — only plain literal expressions are supported (no
// function calls, subqueries, arithmetic, ...). Reserved-capacity
// validation for Str/Blob (does the literal actually fit within the
// column's declared length) happens later, in IndexKey::new_from.
fn expr_to_value_item(
    expr: &sqlparser::ast::Expr,
    datatype: DataType,
) -> Result<ValueItem, SchemaError> {
    let value = match expr {
        sqlparser::ast::Expr::Value(v) => &v.value,
        _ => {
            return Err(SchemaError::UserError(format!(
                "unsupported expression in VALUES: {expr}"
            )));
        }
    };
    match (value, datatype) {
        (sqlparser::ast::Value::Null, _) => Ok(ValueItem::Null),
        (sqlparser::ast::Value::Number(s, _), DataType::Integer) => s
            .parse()
            .map(ValueItem::Integer)
            .map_err(|_| SchemaError::UserError(format!("invalid integer literal: {s}"))),
        (sqlparser::ast::Value::Number(s, _), DataType::Double) => s
            .parse()
            .map(ValueItem::Double)
            .map_err(|_| SchemaError::UserError(format!("invalid double literal: {s}"))),
        (sqlparser::ast::Value::Number(s, _), DataType::Datetime) => s
            .parse()
            .map(ValueItem::Datetime)
            .map_err(|_| SchemaError::UserError(format!("invalid datetime literal: {s}"))),
        (
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s),
            DataType::Str(cap),
        ) => Ok(ValueItem::Str((s.clone(), cap))),
        _ => Err(SchemaError::UserError(format!(
            "value {value} does not match column type {datatype:?}"
        ))),
    }
}

impl SqlIndex {
    // Plus ENTRY_OVERHEAD_BYTES — see its own comment on SqlTable::row_size.
    pub(crate) fn size(&self) -> usize {
        self.fields.iter().map(|f| f.datatype.size()).sum::<usize>() + ENTRY_OVERHEAD_BYTES
    }
}

impl From<&sqlparser::ast::UniqueConstraint> for IndexHolder {
    fn from(value: &sqlparser::ast::UniqueConstraint) -> Self {
        Self {
            fields: value.columns.iter().map(|c| c.column.to_string()).collect(),
            is_primary: false,
            is_unique: true,
            name: value.name.clone().map(|n| n.to_string()),
        }
    }
}

impl From<&sqlparser::ast::PrimaryKeyConstraint> for IndexHolder {
    fn from(value: &sqlparser::ast::PrimaryKeyConstraint) -> Self {
        Self {
            fields: value.columns.iter().map(|c| c.column.to_string()).collect(),
            is_primary: true,
            is_unique: true,
            name: value.name.clone().map(|n| n.to_string()),
        }
    }
}
