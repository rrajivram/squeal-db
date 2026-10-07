use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use serde::de::{self, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use sql_parser::ddl::{
    ColumnDef, ColumnOption, CreateTable, ForeignKeyReference, TableConstraint, TableConstraintKind,
};
use sql_parser::ident::Ident;
use store::db::DBFile;
use store::table::TableIdType;
use store::valueitem::{IndexKey, ValueItem, ValueRef};

use crate::constant::{DEFAULT_VAR_SIZE, MAX_TABLE_NAME_LEN};
use crate::datatype::DataType;
use crate::error::SchemaError;
use crate::partition::{Partition, PartitionBound, PartitionKind, Partitioning};
use crate::schema_ops::schema::Schema;

// A store-level Tuple wraps whatever's actually stored (row or index
// entry) with its own key (DBIdType — a Rec(IndexKey) key costs more
// than the raw field bytes alone: an enum tag, the IndexKey's own
// Vec-length prefix, and one enum tag per ValueItem), Option<TransactionId>,
// Option<LsnId>, a flags byte, and postcard's own length-prefix on the
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
    // What this table's statistics are keyed by (see optim::table_stats):
    // the id of the rows tree of the first partition it was created with.
    // It stays the same when partitions come and go. Set by
    // Schema::create_table; TableIdType::none() until then.
    pub(crate) id: TableIdType,
    // The next never-yet-used Field::id — starts at the initial column
    // count (see TableBuilder::build) and only ever increases, one per
    // alter_add_column, even across a column that's since been dropped:
    // ids are never reused, so a dropped-then-re-added column of the
    // same name still gets a fresh id and can't be confused with the
    // original by reproject.
    pub(crate) next_field_id: u32,
    // How the rows are split between `partitions`; None for a table that
    // is not partitioned. See crate::partition.
    pub(crate) partitioning: Option<Partitioning>,
    // Where the rows are: never empty. A table that is not partitioned has
    // one, holding every row. Each has its own rows tree and its own tree
    // per index (set by Schema::create_table once they exist).
    pub(crate) partitions: Vec<Partition>,
    // The next never-yet-used Partition::id.
    pub(crate) next_partition_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    // An Arc-wrapped slice, not a Vec: this list is built once (see
    // TableBuilder::build/alter_add_column/alter_drop_column/
    // alter_rename_column, the only places that ever construct a new
    // SchemaVersion) and read many times after — most rows insert/
    // reproject against the table's current version, and SqlTable
    // itself is now cloned via Arc (see Schema::get_table), so cloning
    // *this* is on the same hot path. Cloning an Arc<[_]> is a refcount
    // bump; cloning a Vec<_> reallocates and copies its whole spine
    // every time, even though each element (Arc<Field>) was already
    // cheap to clone on its own.
    pub(crate) fields: Arc<[Arc<Field>]>,
    // Where each column of a fixed-width row is (see RowLayout), worked
    // out from `fields` the first time it is asked for. Not persisted.
    #[serde(skip)]
    layout: OnceLock<Option<RowLayout>>,
}

impl SchemaVersion {
    pub(crate) fn new(fields: Arc<[Arc<Field>]>) -> Self {
        Self {
            fields,
            layout: OnceLock::new(),
        }
    }

    // None if some column's type has no width (see column_width): rows of
    // this version are then written the old way.
    fn layout(&self) -> Option<&RowLayout> {
        self.layout
            .get_or_init(|| RowLayout::of(&self.fields))
            .as_ref()
    }
}

// A fixed-width row: every column takes the bytes its datatype does,
// whatever its value — a number its nine, a string or blob its declared
// capacity (ValueRef::write_to pads those already), and a NULL its
// column's too, a zero tag and zero padding. So where a column is follows
// from the SchemaVersion the row was written under, and a reader goes
// straight to the ones it wants (SqlTable::decode_row) instead of
// stepping over every value before them. An UPDATE never changes a row's
// size, either.
//
// The row's bytes are those of IndexKey::to_bytes — a u64 count, then
// each value as ValueRef::decode reads it — except for the NULLs'
// padding, which a reader stepping from value to value would misread. So
// such a row says what it is: FIXED_ROW is set in its count, where a row
// written the old way (a NULL its one byte) never has it, and an old
// reader fails on the count rather than misreading the row.
#[derive(Debug, Clone)]
struct RowLayout {
    // Each column's offset in the row's bytes, and one past the last.
    offsets: Vec<usize>,
}

const FIXED_ROW: u64 = 1 << 63;
const ROW_COUNT_BYTES: usize = size_of::<u64>();

// The bytes a column of `datatype` takes in a fixed-width row: any of
// its values', as ValueItem::size counts them. None for what isn't a
// column type a value is stored as.
fn column_width(datatype: DataType) -> Option<usize> {
    match datatype {
        DataType::Integer | DataType::Double | DataType::Datetime => Some(9),
        DataType::Boolean => Some(2),
        DataType::Str(n) | DataType::Blob(n) => Some(9 + n as usize),
        DataType::Null | DataType::Unsupported => None,
    }
}

impl RowLayout {
    fn of(fields: &[Arc<Field>]) -> Option<Self> {
        let mut offsets = Vec::with_capacity(fields.len() + 1);
        let mut at = ROW_COUNT_BYTES;
        for f in fields {
            offsets.push(at);
            at += column_width(f.datatype)?;
        }
        offsets.push(at);
        Some(Self { offsets })
    }

    fn columns(&self) -> usize {
        self.offsets.len() - 1
    }

    fn len(&self) -> usize {
        self.offsets[self.columns()]
    }

    // Column `pos` of a row of this layout.
    fn column<'a>(&self, row: &'a [u8], pos: usize) -> &'a [u8] {
        &row[self.offsets[pos]..self.offsets[pos + 1]]
    }

    // The row's bytes, or None if a value doesn't fit its column's width
    // (a type the column isn't, a string past its capacity): no fixed-
    // width row can hold it.
    fn encode(&self, fields: &[Arc<Field>], values: &[ValueItem]) -> Option<Vec<u8>> {
        if values.len() != self.columns() {
            return None;
        }
        let mut out = Vec::with_capacity(self.len());
        out.extend_from_slice(&(values.len() as u64 | FIXED_ROW).to_le_bytes());
        for (pos, (value, field)) in values.iter().zip(fields).enumerate() {
            // A string or blob is written with the column's capacity,
            // whatever the value came with.
            let value = match (value.as_ref(), field.datatype) {
                (ValueRef::Str(s, _), DataType::Str(n)) if s.len() <= n as usize => {
                    ValueRef::Str(s, n)
                }
                (ValueRef::Blob(b, _), DataType::Blob(n)) if b.len() <= n as usize => {
                    ValueRef::Blob(b, n)
                }
                (v, _) => v,
            };
            value.write_to(&mut out);
            let end = self.offsets[pos + 1];
            if out.len() > end || (out.len() < end && value != ValueRef::Null) {
                return None;
            }
            out.resize(end, 0);
        }
        Some(out)
    }

    // How many columns a row's bytes say it has, if it is a fixed-width
    // row (FIXED_ROW).
    fn fixed_count(row: &[u8]) -> Option<u64> {
        let count = u64::from_le_bytes(row.get(..ROW_COUNT_BYTES)?.try_into().ok()?);
        (count & FIXED_ROW != 0).then_some(count & !FIXED_ROW)
    }
}

// What Schema::insert_rows_in_txn actually writes as a row's stored
// payload, replacing the bare IndexKey it used before ALTER TABLE
// existed: `values` alone is positional with no record of which
// SchemaVersion that position order matches, so once a table can have
// more than one version, decoding needs to know which one a given row
// was encoded under. New inserts always stamp SqlTable::version()
// (the current/latest version); older rows keep whatever version was
// current when they were written.
#[derive(Debug, Clone)]
pub(crate) struct VersionedRow {
    pub(crate) version: u32,
    pub(crate) values: IndexKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlIndex {
    pub(crate) name: Option<String>,
    pub(crate) is_primary: bool,
    pub(crate) is_unique: bool,
    // Same reasoning as SchemaVersion::fields — built once, cloned
    // whenever the containing SqlTable is.
    pub(crate) fields: Arc<[Arc<Field>]>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
    #[serde(skip)]
    pub(crate) is_ephemeral: bool,
}

pub struct TableBuilder {
    name: Option<String>,
    fields: Vec<Field>,
    indices: Vec<IndexHolder>,
    foreign_keys: Vec<SqlForeignKey>,
}

impl Serialize for VersionedRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("VersionedRow", 2)?;
        s.serialize_field("ver", &self.version)?;
        s.serialize_field("data", &self.values.to_bytes())?;
        s.end()
    }
}

// A stored row as written (VersionedRow's Serialize), its values still
// in place: decoding one reads the version and borrows the values' bytes
// (IndexKey::to_bytes's form) rather than copying them out. The values
// were written as a Vec<u8>, which postcard lays out exactly as it does
// bytes — a length, then the bytes — so they borrow straight off the
// tuple. See SqlTable::decode_row.
pub(crate) struct StoredRow<'a> {
    pub(crate) version: u32,
    pub(crate) values: &'a [u8],
}

// Written as VersionedRow is: the version, then the values' bytes.
impl Serialize for StoredRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct Bytes<'a>(&'a [u8]);
        impl Serialize for Bytes<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(self.0)
            }
        }
        let mut s = serializer.serialize_struct("VersionedRow", 2)?;
        s.serialize_field("ver", &self.version)?;
        s.serialize_field("data", &Bytes(self.values))?;
        s.end()
    }
}

// Field names given to deserialize_struct below only matter to
// self-describing formats (JSON, ...) that key on them; postcard (the
// only format this crate actually feeds VersionedRow through) ignores
// them entirely and always drives visit_seq, reading fields in
// declaration order — same as the Serialize side's serialize_struct.
const VERSIONED_ROW_FIELDS: &[&str] = &["ver", "data"];

struct StoredRowVisitor;

impl<'de> Visitor<'de> for StoredRowVisitor {
    type Value = StoredRow<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("struct VersionedRow")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let version = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let values = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        Ok(StoredRow { version, values })
    }
}

impl<'de> Deserialize<'de> for StoredRow<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct("VersionedRow", VERSIONED_ROW_FIELDS, StoredRowVisitor)
    }
}

impl<'de> Deserialize<'de> for VersionedRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let row = StoredRow::deserialize(deserializer)?;
        Ok(VersionedRow {
            version: row.version,
            values: IndexKey::from_bytes(row.values).map_err(de::Error::custom)?,
        })
    }
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
            is_ephemeral: false,
        })
    }

    pub(crate) fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }
}

impl From<String> for Field {
    fn from(value: String) -> Self {
        Self {
            id: 0,
            datatype: DataType::Str(DEFAULT_VAR_SIZE as u32),
            default: None,
            name: value,
            nullable: true,
            is_ephemeral: true,
        }
    }
}

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
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
            versions: vec![SchemaVersion::new(fields.into())],
            indices: vec![],
            foreign_keys: vec![],
            id: TableIdType::none(),
            next_field_id,
            partitioning: None,
            partitions: vec![],
            next_partition_id: 1,
        };
        for i in &self.indices {
            // Built as a Vec (needs .push() while resolving each name
            // below), converted to the stored Arc<[_]> only once it's
            // complete — same pattern as alter_add_column/
            // alter_drop_column/alter_rename_column.
            let mut fields: Vec<Arc<Field>> = vec![];
            for index_f in &i.fields {
                if let Some(f) = table.fields().iter().find(|f| f.name == *index_f) {
                    fields.push(f.clone());
                }
            }
            table.indices.push(SqlIndex {
                name: i.name.clone(),
                is_primary: i.is_primary,
                is_unique: i.is_unique,
                fields: fields.into(),
            });
        }
        table.partitions = vec![Partition::whole(table.indices.len())];
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
            .any(|o| matches!(o, ColumnOption::NotNull(_, _)));
        let datatype: DataType = value.data_type.clone().into();
        let default = value
            .options
            .iter()
            .find_map(|o| match o {
                ColumnOption::Default(_, expr) => Some(expr),
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
// KEY(id)`). The inline form's grammar has no CONSTRAINT-name capture at
// all (see ddl::ColumnOption) — unlike a table-level constraint, whose
// name comes from index_from_constraint — so `name` here is always None.
fn inline_indices(column: &ColumnDef) -> Vec<IndexHolder> {
    column
        .options
        .iter()
        .filter_map(|o| match o {
            ColumnOption::PrimaryKey(_, _) => Some(IndexHolder {
                name: None,
                is_primary: true,
                is_unique: true,
                fields: vec![column.name.value.clone()],
            }),
            ColumnOption::Unique(_) => Some(IndexHolder {
                name: None,
                is_primary: false,
                is_unique: true,
                fields: vec![column.name.value.clone()],
            }),
            _ => None,
        })
        .collect()
}

// Converts a parsed FOREIGN KEY reference (table-level or, via
// inline_foreign_key, the inline column form) into a SqlForeignKey,
// rejecting every form this crate doesn't support yet: composite
// (multi-column) keys, and REFERENCES without an explicit target column.
// Everything sqlparser's ForeignKeyConstraint used to carry beyond that
// (ON DELETE/ON UPDATE, MATCH, characteristics, a MySQL-style index name)
// has no equivalent in sql-parser's grammar at all — that SQL simply
// fails to parse now instead of parsing and then being rejected here.
// Shared with Statement::execute's ALTER TABLE ADD CONSTRAINT parsing
// (see stmt.rs's parse_alter_table), so both entry points agree.
pub(crate) fn foreign_key_from_constraint(
    reference: &ForeignKeyReference,
    local_columns: &[Ident],
    name: Option<&Ident>,
) -> Result<SqlForeignKey, SchemaError> {
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "FOREIGN KEY only supports a single-column reference right now — {what} is not \
             supported yet"
        )))
    };
    let column = match local_columns {
        [c] => c.value.clone(),
        _ => return unsupported("multiple columns"),
    };
    let ref_column = match &reference.column {
        Some((_, cols, _)) if cols.len() == 1 => cols.items().next().unwrap().value.clone(),
        Some(_) => return unsupported("multiple referenced columns"),
        None => return unsupported("REFERENCES without an explicit target column"),
    };
    Ok(SqlForeignKey {
        // Lowercased, matching every other identifier in this crate
        // (table/column names) — DROP CONSTRAINT's own name lookup
        // lowercases too (see stmt.rs's parse_alter_table), so this has
        // to agree or a mixed-case constraint name would never match.
        name: name.map(|n| n.value.to_lowercase()),
        column,
        ref_table: reference.table.to_dotted().to_lowercase(),
        ref_column,
    })
}

// Mirrors inline_indices: a column's own inline REFERENCES (e.g.
// `customer_id INTEGER REFERENCES customers(id)`) — the column is
// implicit ("this one"), and never named (no CONSTRAINT-name capture in
// the inline grammar, same as inline_indices).
fn inline_foreign_key(column: &ColumnDef) -> Option<Result<SqlForeignKey, SchemaError>> {
    column.options.iter().find_map(|o| match o {
        ColumnOption::References(reference) => Some(foreign_key_from_constraint(
            reference,
            std::slice::from_ref(&column.name),
            None,
        )),
        _ => None,
    })
}

// Persistence versioning Stage 7: how a `SqlTable` is stored as a row of its
// schema's system table. Layout: `[0x00][u16 LE version][postcard body]`.
//
// The leading 0x00 is the discriminator against rows written before this
// envelope existed: those are bare postcard of `SqlTable`, whose first field
// is `name: String`, i.e. a length varint that is 0x00 only for an EMPTY
// name — which `encode_catalog_row` refuses to write and no earlier build
// could create. So a first byte of 0x00 means "enveloped", anything else
// means "legacy, no envelope, read as version 1's body".
//
// The version-1 body is the live derived shape of `SqlTable`/`SchemaVersion`/
// `Field`/`SqlIndex`/`SqlForeignKey` (`Field::default` and any embedded
// `ValueItem` go through store's explicitly-tagged serde). It is pinned by
// tests/catalog_versioning.rs against bytes captured from the pre-envelope
// encoder: to change any of those structs, FIRST freeze the current shapes as
// `...V1Shape` copies and branch on the version here, THEN edit the live ones.
//
// Version 2 (partitions): a table's trees moved from `SqlTable::db_table_id`
// and each `SqlIndex::db_table_id` into its partitions (see
// crate::partition), and `SqlTable` gained `id`, `partitioning`,
// `partitions` and `next_partition_id`. Version 1 is frozen below as
// `SqlTableV1Shape`/`SqlIndexV1Shape`; `SchemaVersion`, `Field` and
// `SqlForeignKey` did not change and are shared by both. The version-2 body
// is pinned the same way, by a row captured when it was introduced.
pub(crate) const CATALOG_ROW_VERSION: u16 = 2;

// A version-1 (or legacy) catalog row's body. Frozen: never edit.
#[derive(Serialize, Deserialize)]
pub(crate) struct SqlTableV1Shape {
    name: String,
    versions: Vec<SchemaVersion>,
    indices: Vec<SqlIndexV1Shape>,
    foreign_keys: Vec<SqlForeignKey>,
    db_table_id: TableIdType,
    next_field_id: u32,
}

#[derive(Serialize, Deserialize)]
struct SqlIndexV1Shape {
    name: Option<String>,
    db_table_id: TableIdType,
    is_primary: bool,
    is_unique: bool,
    fields: Arc<[Arc<Field>]>,
}

// A version-1 table is one that is not partitioned: its trees become its
// one partition's.
impl From<SqlTableV1Shape> for SqlTable {
    fn from(v1: SqlTableV1Shape) -> Self {
        let mut whole = Partition::whole(0);
        whole.set_rows(v1.db_table_id);
        for (i, index) in v1.indices.iter().enumerate() {
            whole.set_index(i, index.db_table_id);
        }
        SqlTable {
            name: v1.name,
            versions: v1.versions,
            indices: v1
                .indices
                .into_iter()
                .map(|i| SqlIndex {
                    name: i.name,
                    is_primary: i.is_primary,
                    is_unique: i.is_unique,
                    fields: i.fields,
                })
                .collect(),
            foreign_keys: v1.foreign_keys,
            id: v1.db_table_id,
            next_field_id: v1.next_field_id,
            partitioning: None,
            partitions: vec![whole],
            next_partition_id: 1,
        }
    }
}

#[cfg(test)]
impl SqlTableV1Shape {
    // What a build before partitions wrote for `table`, which must be one
    // that is not partitioned.
    pub(crate) fn from_table(table: &SqlTable) -> Self {
        assert!(table.partitioning.is_none() && table.partitions.len() == 1);
        let part = &table.partitions[0];
        Self {
            name: table.name.clone(),
            versions: table.versions.clone(),
            indices: table
                .indices
                .iter()
                .enumerate()
                .map(|(i, index)| SqlIndexV1Shape {
                    name: index.name.clone(),
                    db_table_id: part.index(i),
                    is_primary: index.is_primary,
                    is_unique: index.is_unique,
                    fields: index.fields.clone(),
                })
                .collect(),
            foreign_keys: table.foreign_keys.clone(),
            db_table_id: part.rows(),
            next_field_id: table.next_field_id,
        }
    }
}

impl SqlTable {
    pub(crate) fn encode_catalog_row(&self) -> Result<Vec<u8>, SchemaError> {
        if self.name.is_empty() {
            return Err(SchemaError::UserError(
                "a table name cannot be empty".into(),
            ));
        }
        Ok(crate::envelope::seal(
            CATALOG_ROW_VERSION,
            &postcard::to_allocvec(self)?,
        ))
    }

    pub(crate) fn decode_catalog_row(bytes: &[u8]) -> Result<SqlTable, SchemaError> {
        use crate::envelope::{Opened, open, unsupported};
        match open(bytes, "catalog")? {
            Opened::Versioned { version: 2, body } => Ok(postcard::from_bytes(body)?),
            // Legacy: bare postcard of the version-1 shape, no envelope.
            Opened::Versioned { version: 1, body } | Opened::Legacy(body) => {
                Ok(postcard::from_bytes::<SqlTableV1Shape>(body)?.into())
            }
            Opened::Versioned { version, .. } => Err(unsupported("catalog", version)),
        }
    }
}

impl SqlTable {
    pub(crate) fn from_sql<F>(db: &Arc<Schema<F>>, value: CreateTable) -> Result<Self, SchemaError>
    where
        F: DBFile + 'static,
        F: DBFile<Item = F>,
    {
        let name = value.name.to_dotted().to_lowercase();
        if db.table_exists(&name) {
            return Err(SchemaError::BadTableName(format!("Table {name} exists.")));
        }
        // A Vec, not a HashMap: declaration order must survive into the
        // built table (SELECT *, positional INSERT rely on it) — build()
        // is what rejects a duplicate name now, instead of a HashMap
        // silently keeping whichever column happened to collect last.
        let fields: Vec<Field> = value
            .columns()
            .map(Field::try_from)
            .collect::<Result<_, _>>()?;
        let mut indices = value
            .constraints()
            .filter_map(index_from_constraint)
            .collect::<Vec<_>>();
        for c in value.columns() {
            indices.extend(inline_indices(c));
        }
        let mut foreign_keys = value
            .constraints()
            .filter_map(|t| match &t.kind {
                TableConstraintKind::ForeignKey(_, _, _, columns, _, reference) => {
                    let cols: Vec<Ident> = columns.items().cloned().collect();
                    Some(foreign_key_from_constraint(
                        reference,
                        &cols,
                        t.name.as_ref().map(|(_, n)| n),
                    ))
                }
                _ => None,
            })
            .collect::<Result<Vec<_>, _>>()?;
        for c in value.columns() {
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

        let mut table = tb.build()?;
        if let Some(by) = &value.partition_by {
            let kind = match by.kind {
                either::Either::Left(_) => PartitionKind::Range,
                either::Either::Right(_) => PartitionKind::List,
            };
            let column = by.column.value.to_lowercase();
            let field = table
                .fields()
                .iter()
                .find(|f| f.name == column)
                .cloned()
                .ok_or_else(|| {
                    SchemaError::UserError(format!(
                        "PARTITION BY: table {:?} has no column named {column:?}",
                        table.name
                    ))
                })?;
            if !matches!(
                field.datatype,
                DataType::Integer
                    | DataType::Double
                    | DataType::Datetime
                    | DataType::Str(_)
                    | DataType::Boolean
            ) {
                return Err(SchemaError::UserError(format!(
                    "PARTITION BY: column {column:?} of type {:?} cannot be partitioned by",
                    field.datatype
                )));
            }
            let partitioning = Partitioning {
                kind,
                field_id: field.id,
            };
            let indices = table.indices.len();
            let partitions = by
                .partitions
                .items()
                .enumerate()
                .map(|(i, def)| {
                    Ok(Partition::new(
                        i as u32,
                        partition_name(def)?,
                        PartitionBound::from_sql(def, kind, field.datatype)?,
                        indices,
                    ))
                })
                .collect::<Result<Vec<_>, SchemaError>>()?;
            partitioning.check(&partitions)?;
            table.next_partition_id = partitions.len() as u32;
            table.partitions = partitions;
            table.partitioning = Some(partitioning);
            table.check_partition_keys()?;
        }
        Ok(table)
    }

    // The trees of a table that has one partition: its rows tree, and its
    // tree for the index at `index` in `indices`.
    #[cfg(test)]
    pub(crate) fn rows_tree(&self) -> TableIdType {
        self.partitions[0].rows()
    }

    #[cfg(test)]
    pub(crate) fn index_tree(&self, index: usize) -> TableIdType {
        self.partitions[0].index(index)
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        self.partitioning.is_some()
    }

    // The partition column and its position in a row of the table's
    // current layout; None for a table that is not partitioned.
    pub(crate) fn partition_column(&self) -> Option<(usize, &Arc<Field>)> {
        let by = self.partitioning.as_ref()?;
        self.fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.id == by.field_id)
    }

    // The partition (a position in `partitions`) that `row`, a full row in
    // the table's current layout, belongs in. An error when no partition
    // takes its partition column's value.
    pub(crate) fn partition_for(&self, row: &[ValueItem]) -> Result<usize, SchemaError> {
        let (Some(by), Some((pos, field))) = (&self.partitioning, self.partition_column()) else {
            return Ok(0);
        };
        by.route(&self.partitions, &row[pos]).ok_or_else(|| {
            SchemaError::UserError(format!(
                "table {:?} has no partition for {} = {}",
                self.name, field.name, row[pos]
            ))
        })
    }

    // Each partition enforces a PRIMARY KEY/UNIQUE constraint over its own
    // rows only (indexes are local — see crate::partition). That enforces
    // it over the whole table only if equal keys always land in the same
    // partition, i.e. the key includes the partition column.
    pub(crate) fn check_partition_keys(&self) -> Result<(), SchemaError> {
        let Some((_, column)) = self.partition_column() else {
            return Ok(());
        };
        for index in &self.indices {
            if (index.is_primary || index.is_unique)
                && !index.fields.iter().any(|f| f.id == column.id)
            {
                return Err(SchemaError::UserError(format!(
                    "table {:?} is partitioned by {:?}: its {} must include that column",
                    self.name,
                    column.name,
                    if index.is_primary {
                        "PRIMARY KEY".to_string()
                    } else {
                        format!(
                            "unique index {:?}",
                            index.name.as_deref().unwrap_or("<unnamed>")
                        )
                    }
                )));
            }
        }
        Ok(())
    }

    // The byte footprint of a row's own identity (its PRIMARY KEY, or
    // the generated rowid when there isn't one) — what a non-unique
    // index's key has to grow by to stay unique (see alter_add_index's
    // own doc comment on why: every backing table here is a BPlusTree
    // with its own duplicate-key rejection, and a plain, non-unique
    // index has to survive two rows sharing the same indexed value).
    pub(crate) fn identity_size(&self) -> usize {
        match self.primary_key() {
            Some(pk) => pk.fields.iter().map(|f| f.datatype.size()).sum::<usize>(),
            None => store::valueitem::ValueItem::Integer(0).size(),
        }
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
        self.unique_index_position(column).map(|i| &self.indices[i])
    }

    // unique_index_on's index as a position in `indices` — what finds its
    // tree in each partition.
    pub(crate) fn unique_index_position(&self, column: &str) -> Option<usize> {
        self.indices.iter().position(|i| {
            (i.is_primary || i.is_unique) && i.fields.len() == 1 && i.fields[0].name == column
        })
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

    // Same fields as fields(), but a cheap Arc clone (refcount bump) of
    // the current version's own backing slice, for a caller (e.g.
    // Source::fields) that needs an owned, shareable handle rather than
    // a borrow tied to &self — not a fresh Arc<[_]> built by re-collecting
    // the slice, which would allocate every call for no reason.
    pub(crate) fn fields_arc(&self) -> Arc<[Arc<Field>]> {
        self.versions
            .last()
            .expect("a table always has at least one schema version")
            .fields
            .clone()
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
        self.versions.get(version as usize).map(|v| &v.fields[..])
    }

    // A stored row (a tuple's data), decoded and reprojected (see
    // reproject). `wanted`, by position in the current version: the
    // columns to fill; the others come out NULL. A row written under the
    // current version is read in place (StoredRow, IndexKey::refs) and
    // only the wanted columns are copied out; an older one is decoded
    // whole and reprojected.
    pub(crate) fn decode_row(
        &self,
        data: &[u8],
        wanted: Option<&[bool]>,
    ) -> Result<IndexKey, SchemaError> {
        let row: StoredRow = postcard::from_bytes(data)?;
        if let Some(count) = RowLayout::fixed_count(row.values) {
            return self.decode_fixed_row(&row, count, wanted);
        }
        if row.version != self.version() {
            return self.reproject(&VersionedRow {
                version: row.version,
                values: IndexKey::from_bytes(row.values)?,
            });
        }
        let fields = self.fields().len();
        let mut refs = IndexKey::refs(row.values)?;
        if refs.remaining() != fields as u64 {
            return Err(SchemaError::UnknownError(format!(
                "row has {} value(s) but schema version {} of table {:?} declared {} field(s)",
                refs.remaining(),
                row.version,
                self.name,
                fields
            )));
        }
        let mut values = Vec::with_capacity(fields);
        for pos in 0..fields {
            let value = match wanted {
                Some(w) if !w[pos] => refs.skip_value().map(|r| r.map(|()| ValueItem::Null)),
                _ => refs.next().map(|r| r.map(|v| v.to_owned())),
            };
            match value {
                Some(value) => values.push(value?),
                None => break,
            }
        }
        Ok(IndexKey::new_from_owned(values)?)
    }

    // decode_row, of a fixed-width row (see RowLayout): each wanted column
    // read from where its SchemaVersion says it is.
    fn decode_fixed_row(
        &self,
        row: &StoredRow,
        count: u64,
        wanted: Option<&[bool]>,
    ) -> Result<IndexKey, SchemaError> {
        let layout = self
            .versions
            .get(row.version as usize)
            .and_then(SchemaVersion::layout)
            .filter(|l| l.columns() as u64 == count && l.len() == row.values.len())
            .ok_or_else(|| {
                SchemaError::UnknownError(format!(
                    "row of {count} fixed-width value(s) in {} byte(s) doesn't match schema \
                     version {} of table {:?}",
                    row.values.len(),
                    row.version,
                    self.name
                ))
            })?;
        let current = row.version == self.version();
        let mut values = Vec::with_capacity(layout.columns());
        for pos in 0..layout.columns() {
            values.push(match wanted {
                Some(w) if current && !w[pos] => ValueItem::Null,
                _ => ValueRef::decode(layout.column(row.values, pos))?.0.to_owned(),
            });
        }
        let values = IndexKey::new_from_owned(values)?;
        if current {
            return Ok(values);
        }
        self.reproject(&VersionedRow {
            version: row.version,
            values,
        })
    }

    /// A row's stored bytes (a tuple's data), written under the current
    /// version: fixed-width (see RowLayout) where its values fit their
    /// columns, as they do unless something got past INSERT's typing —
    /// and the old way, each value its own size, otherwise.
    pub(crate) fn encode_row(&self, row: &IndexKey) -> Result<Vec<u8>, SchemaError> {
        let version = &self.versions[self.version() as usize];
        let values = version
            .layout()
            .and_then(|l| l.encode(&version.fields, row.values()))
            .unwrap_or_else(|| row.to_bytes());
        Ok(postcard::to_allocvec(&StoredRow {
            version: self.version(),
            values: &values,
        })?)
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
    pub(crate) fn reproject(&self, row: &VersionedRow) -> Result<IndexKey, SchemaError> {
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
        // The common case: a row written under the table's current
        // version (every row not predating the table's last ALTER
        // TABLE) already has its values in exactly the shape the loop
        // below would reconstruct one field at a time — stored_fields
        // and self.fields() are the literal same SchemaVersion here.
        // Cloning row.values is an Arc refcount bump (IndexKey wraps
        // Arc<[ValueItem]>), not a real allocation, and skips both the
        // O(fields^2) id lookup below and new_from's redundant
        // re-validation of data that's already known valid.
        if row.version == self.version() {
            return Ok(row.values.clone());
        }
        let values: Vec<ValueItem> = self
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
            .collect();
        // Every value here is either a clone of an already-validated
        // stored value or an already-validated Field::default (see
        // Field::new's own size-cap check) — new_from's validation can't
        // meaningfully fail on either, but going through it anyway (not
        // a raw IndexKey construction) keeps this the one place that
        // decides what "a valid IndexKey" means.
        Ok(IndexKey::new_from(&values)?)
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
        self.versions.push(SchemaVersion::new(fields.into()));
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
        if self.partition_column().is_some_and(|(_, f)| f.name == name) {
            return Err(SchemaError::UserError(format!(
                "Column {name:?} is what table {:?} is partitioned by",
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
        self.versions.push(SchemaVersion::new(fields.into()));
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
                        is_ephemeral: f.is_ephemeral,
                    })
                } else {
                    f.clone()
                }
            })
            .collect();
        self.versions.push(SchemaVersion::new(fields.into()));
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
            && self
                .foreign_keys
                .iter()
                .any(|f| f.name.as_deref() == Some(name.as_str()))
        {
            return Err(SchemaError::UserError(format!(
                "Duplicate foreign key constraint name: {name}"
            )));
        }
        self.foreign_keys.push(fk);
        Ok(())
    }

    // Structural validation only (no duplicate index name) — Schema::
    // create_index is responsible for everything that needs to look
    // outside this one table's own field list: resolving each column
    // name to its Arc<Field>, checking the store-level backing-table
    // name isn't taken, creating and backfilling that backing table
    // before this ever gets called (the fully-built SqlIndex it hands
    // in is just appended here; its trees go into the partitions).
    pub(crate) fn alter_add_index(&mut self, index: SqlIndex) -> Result<(), SchemaError> {
        if let Some(name) = &index.name
            && self
                .indices
                .iter()
                .any(|i| i.name.as_deref() == Some(name.as_str()))
        {
            return Err(SchemaError::UserError(format!(
                "Duplicate index name: {name}"
            )));
        }
        self.indices.push(index);
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

    // The row-storage table's own index_entry_size: the width of its key
    // (the PRIMARY KEY's columns, or the rowid — see Schema::row_key) plus
    // ENTRY_OVERHEAD_BYTES, as SqlIndex::size() does for an index. Not the
    // row's width: store keeps a row on its table's data pages, and the
    // tree's entries hold only the key and that page's id. (Sizing by the
    // whole row starved wide tables of fanout — a varchar(4000) column
    // left 4 entries a 16 KB page — and broke tables wider than that.)
    pub(crate) fn row_entry_size(&self) -> usize {
        self.identity_size() + ENTRY_OVERHEAD_BYTES
    }

    // A row's width as the cost model counts it (see optim::picker's
    // table_row): every field's budget plus ENTRY_OVERHEAD_BYTES — what
    // the row table's entry budget was before row_entry_size, and what the
    // planner's constants are calibrated against. Taken from the schema,
    // not the tree, so plans don't depend on how a table's tree was sized.
    pub(crate) fn costed_row_size(&self) -> usize {
        self.fields()
            .iter()
            .map(|f| f.datatype.size())
            .sum::<usize>()
            + ENTRY_OVERHEAD_BYTES
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
                let pos = self.field_position(f).expect(
                    "index/primary-key fields are always a subset of the table's own fields",
                );
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
        insert: &sql_parser::dml::Insert,
    ) -> Result<Vec<Vec<ValueItem>>, SchemaError> {
        build_insert_rows(&self.name, self.fields(), insert)
    }
}

// The actual logic behind SqlTable::rows_from_insert, extracted to a
// free function taking just a name (for error messages) and a field
// list rather than a whole SqlTable — so a temp table (crate::temp::
// TempTable, which has fields but no SqlTable at all: no versions,
// indices, or store-backed db_table_id) can validate/build its own
// INSERT rows through the exact same rules (column-list resolution,
// NOT NULL, DEFAULT, arity checking) instead of a second, parallel
// implementation that could silently drift from this one.
pub(crate) fn build_insert_rows(
    table_name: &str,
    fields: &[Arc<Field>],
    insert: &sql_parser::dml::Insert,
) -> Result<Vec<Vec<ValueItem>>, SchemaError> {
    let target_fields: Vec<&Arc<Field>> = match &insert.columns {
        None => fields.iter().collect(),
        Some((_, cols, _)) => cols
            .items()
            .map(|c| {
                let name = c.value.to_lowercase();
                fields.iter().find(|f| f.name == name).ok_or_else(|| {
                    SchemaError::UserError(format!(
                        "Table {table_name:?} has no column named {name:?}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?,
    };

    let value_rows = match &insert.source {
        sql_parser::dml::InsertSource::Values(_, rows) => rows,
        // INSERT ... SELECT goes through rows_from_select instead (see
        // its own doc comment) — Statement::execute branches on
        // insert.source before ever calling this function, so reaching
        // here with a Select source would be this crate's own bug, not
        // a user-facing case.
        sql_parser::dml::InsertSource::Select(_) => {
            return Err(SchemaError::InternalSchemaError(
                "build_insert_rows called with an INSERT ... SELECT — should have gone \
                 through rows_from_select instead"
                    .into(),
            ));
        }
    };

    let mut rows = Vec::with_capacity(value_rows.len());
    for row in value_rows.items() {
        let exprs: Vec<&sql_parser::Expr> = row.exprs().collect();
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
        rows.push(fill_full_row(fields, by_name)?);
    }
    Ok(rows)
}

// Shared tail of both build_insert_rows and rows_from_select: given
// `by_name` (one entry per column the caller actually supplied a value
// for), produces one full row in `fields`' own declared order — DEFAULT
// for any omitted column that has one (checked before the plain-NULL
// fallback so a NOT NULL column with a DEFAULT still works via an
// explicit column list that leaves it out), Null for any other omitted
// nullable column, and an error for an omitted NOT NULL column with no
// DEFAULT.
fn fill_full_row(
    fields: &[Arc<Field>],
    mut by_name: HashMap<&str, ValueItem>,
) -> Result<Vec<ValueItem>, SchemaError> {
    let mut full_row = Vec::with_capacity(fields.len());
    for f in fields {
        match by_name.remove(f.name.as_str()) {
            Some(v) => full_row.push(v),
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
    Ok(full_row)
}

// INSERT ... SELECT's own row-shaping — the SELECT side has already run
// by the time this is called (see Statement::execute; this function
// stays free of Connection/LogicalPlan so table.rs doesn't need to know
// how a query executes, only how to shape its output into this table's
// row order). Resolves insert's column list exactly like
// build_insert_rows does, coerces each selected value to its target
// column's declared type (coerce_selected_value re-wraps a Str/Blob
// with THIS column's own reserved capacity — a selected value carries
// whatever capacity its own source column had, never this one's), and
// fills every omitted column with DEFAULT/NULL exactly like the VALUES
// path (fill_full_row).
pub(crate) fn rows_from_select(
    table_name: &str,
    fields: &[Arc<Field>],
    insert: &sql_parser::dml::Insert,
    selected_rows: Vec<IndexKey>,
) -> Result<Vec<Vec<ValueItem>>, SchemaError> {
    let target_fields: Vec<&Arc<Field>> = match &insert.columns {
        None => fields.iter().collect(),
        Some((_, cols, _)) => cols
            .items()
            .map(|c| {
                let name = c.value.to_lowercase();
                fields.iter().find(|f| f.name == name).ok_or_else(|| {
                    SchemaError::UserError(format!(
                        "Table {table_name:?} has no column named {name:?}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?,
    };

    let mut rows = Vec::with_capacity(selected_rows.len());
    for selected in selected_rows {
        let values = selected.values();
        if values.len() != target_fields.len() {
            return Err(SchemaError::UserError(format!(
                "SELECT returned {} column(s) but INSERT expects {}",
                values.len(),
                target_fields.len()
            )));
        }
        let mut by_name: HashMap<&str, ValueItem> = HashMap::with_capacity(values.len());
        for (field, value) in target_fields.iter().zip(values) {
            let item = coerce_selected_value(value.clone(), field.datatype)?;
            if item == ValueItem::Null && !field.nullable {
                return Err(SchemaError::UserError(format!(
                    "Column {:?} cannot be null",
                    field.name
                )));
            }
            by_name.insert(field.name.as_str(), item);
        }
        rows.push(fill_full_row(fields, by_name)?);
    }
    Ok(rows)
}

// Re-typing for a specific target column — same spirit as
// expr_to_value_item (which does this for a VALUES literal), but the
// input here is already a concrete ValueItem the SELECT side produced,
// not an AST node to parse. Deliberately as strict as expr_to_value_item
// (no int-to-double widening, no implicit casts): a value whose variant
// doesn't already match `datatype` is rejected with a clear message
// rather than silently coerced.
pub(crate) fn coerce_selected_value(
    value: ValueItem,
    datatype: DataType,
) -> Result<ValueItem, SchemaError> {
    Ok(match (value, datatype) {
        (ValueItem::Null, _) => ValueItem::Null,
        (ValueItem::Integer(i), DataType::Integer) => ValueItem::Integer(i),
        (ValueItem::Double(d), DataType::Double) => ValueItem::Double(d),
        (ValueItem::Datetime(d), DataType::Datetime) => ValueItem::Datetime(d),
        (ValueItem::Str((s, _)), DataType::Str(cap)) => ValueItem::Str((s, cap)),
        (ValueItem::Blob((b, _)), DataType::Blob(cap)) => ValueItem::Blob((b, cap)),
        (ValueItem::Boolean(b), DataType::Boolean) => ValueItem::Boolean(b),
        (v, dt) => {
            return Err(SchemaError::UserError(format!(
                "value {v:?} does not match column type {dt:?}"
            )));
        }
    })
}

// Converts a single VALUES-clause literal into a ValueItem matching
// `datatype` — only plain literal expressions are supported (no
// function calls, subqueries, arithmetic, ...). Reserved-capacity
// validation for Str/Blob (does the literal actually fit within the
// column's declared length) happens later, in IndexKey::new_from.
pub(crate) fn expr_to_value_item(
    expr: &sql_parser::Expr,
    datatype: DataType,
) -> Result<ValueItem, SchemaError> {
    use sql_parser::{
        expr::{Expr, UnaryOp},
        literal::Literal,
    };
    // A signed number — `-4.5`, `+3` — parses as a unary operator applied
    // to the number literal. Negated from its raw text, not by negating
    // the parsed value, so i64::MIN (whose magnitude doesn't fit an i64)
    // still works.
    if let Expr::Unary { op, expr: inner } = expr
        && let Expr::Literal(Literal::Number(n)) = inner.as_ref()
        && matches!(op, UnaryOp::Minus | UnaryOp::Plus)
    {
        let negative = matches!(op, UnaryOp::Minus);
        return match datatype {
            DataType::Integer => {
                let text = if negative {
                    format!("-{}", n.raw)
                } else {
                    n.raw.clone()
                };
                text.parse::<i64>()
                    .map(ValueItem::Integer)
                    .map_err(|_| SchemaError::UserError(format!("invalid integer literal: {text}")))
            }
            DataType::Double => Ok(ValueItem::Double(if negative {
                -n.as_f64()
            } else {
                n.as_f64()
            })),
            _ => Err(SchemaError::UserError(format!(
                "value {expr:?} does not match column type {datatype:?}"
            ))),
        };
    }
    let literal = match expr {
        Expr::Literal(l) => l,
        _ => {
            return Err(SchemaError::UserError(format!(
                "unsupported expression in VALUES: {expr:?}"
            )));
        }
    };
    match (literal, datatype) {
        (Literal::Null(_), _) => Ok(ValueItem::Null),
        (Literal::Number(n), DataType::Integer) => n
            .as_i64()
            .map(ValueItem::Integer)
            .ok_or_else(|| SchemaError::UserError(format!("invalid integer literal: {}", n.raw))),
        (Literal::Number(n), DataType::Double) => Ok(ValueItem::Double(n.as_f64())),
        (Literal::Number(n), DataType::Datetime) => n
            .as_i64()
            .map(|i| ValueItem::Datetime(i as u64))
            .ok_or_else(|| SchemaError::UserError(format!("invalid datetime literal: {}", n.raw))),
        // A quoted literal against a DATETIME column — "2020-04-13",
        // "12:53:24", or the two combined (space or "T" separated) —
        // see crate::datetime's own doc comment for the exact forms
        // and how they're encoded. A bare number (the arm above) is
        // still accepted too, as a literal, already-computed value.
        // Only single-quoted strings are literals here (unlike the old
        // sqlparser-based grammar, which also accepted double-quoted
        // strings as values) — sql-parser's grammar treats a
        // double-quoted token exclusively as a quoted identifier, the
        // standard SQL reading, so `VALUES ("x")` now parses as a column
        // reference and correctly falls through to the "unsupported
        // expression" error above instead of being silently accepted.
        (Literal::String(s), DataType::Datetime) => crate::datetime::parse_datetime(&s.value)
            .map(ValueItem::Datetime)
            .ok_or_else(|| {
                SchemaError::UserError(format!("invalid datetime literal: {:?}", s.value))
            }),
        (Literal::String(s), DataType::Str(cap)) => Ok(ValueItem::Str((s.value.clone(), cap))),
        (Literal::Boolean(b), DataType::Boolean) => Ok(ValueItem::Boolean(b.value())),
        _ => Err(SchemaError::UserError(format!(
            "value {literal:?} does not match column type {datatype:?}"
        ))),
    }
}

impl SqlIndex {
    // Plus ENTRY_OVERHEAD_BYTES — see its own comment on SqlTable::
    // row_size. `identity_size` (SqlTable::identity_size's own return
    // value) only actually adds anything for a plain, non-unique index —
    // a PRIMARY KEY/UNIQUE index's own declared fields are already the
    // whole key (that's what makes the backing BPlusTree's own duplicate-
    // key rejection enforce the constraint); every other caller can just
    // pass 0 to say "this index doesn't need it."
    pub(crate) fn size(&self, identity_size: usize) -> usize {
        self.key_size(identity_size) + ENTRY_OVERHEAD_BYTES
    }

    // Worst-case width of just this index's tree KEY (indexed columns, plus
    // the row identity appended to make a non-unique index's key unique) —
    // what a B-link `high_key` can end up holding, and so what
    // Db::max_index_key_size bounds. Excludes ENTRY_OVERHEAD_BYTES, which is
    // per-entry storage slack, not part of the key itself.
    pub(crate) fn key_size(&self, identity_size: usize) -> usize {
        let extra = if self.is_primary || self.is_unique {
            0
        } else {
            identity_size
        };
        self.fields.iter().map(|f| f.datatype.size()).sum::<usize>() + extra
    }
}

// A table-level UNIQUE/PRIMARY KEY constraint (e.g. `CONSTRAINT uq
// UNIQUE (a, b)`) as an IndexHolder — None for every other constraint kind
// (FOREIGN KEY is handled separately, by from_sql's own foreign_key loop;
// CHECK has no index representation at all). Unlike the inline
// (column-level) form (see inline_indices), a table-level constraint's own
// optional `CONSTRAINT name` clause carries through as `name`.
fn index_from_constraint(constraint: &TableConstraint) -> Option<IndexHolder> {
    let name = constraint.name.as_ref().map(|(_, n)| n.value.clone());
    match &constraint.kind {
        TableConstraintKind::Unique(_, _, cols, _) => Some(IndexHolder {
            fields: cols.items().map(|c| c.value.clone()).collect(),
            is_primary: false,
            is_unique: true,
            name,
        }),
        TableConstraintKind::PrimaryKey(_, _, _, cols, _) => Some(IndexHolder {
            fields: cols.items().map(|c| c.value.clone()).collect(),
            is_primary: true,
            is_unique: true,
            name,
        }),
        _ => None,
    }
}

// A partition's name as declared, lowercased like every other name here.
pub(crate) fn partition_name(def: &sql_parser::ddl::PartitionDef) -> Result<String, SchemaError> {
    let name = def.name.value.to_lowercase();
    if name.is_empty() || name.len() > MAX_TABLE_NAME_LEN {
        return Err(SchemaError::UserError(format!(
            "a partition name must be 1 to {MAX_TABLE_NAME_LEN} characters long"
        )));
    }
    Ok(name)
}
