use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption, CreateTable};
use store::db::DBFile;
use store::table::TableIdType;
use store::valueitem::ValueItem;

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
    pub(crate) fields: Vec<Arc<Field>>,
    pub(crate) indices: Vec<SqlIndex>,
    // The table's own row-storage backing table — distinct from each
    // index's own db_table_id below. Set by Schema::create_table once
    // the backing table actually exists; TableIdType::none() until then
    // (mirrors SqlIndex::db_table_id's own convention).
    pub(crate) db_table_id: TableIdType,
}

/* #[derive(Debug, Clone)]
pub struct ForeignKey<'a,'b> {

}
 */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlIndex {
    pub(crate) name: Option<String>,
    pub(crate) db_table_id: TableIdType,
    pub(crate) is_primary: bool,
    pub(crate) is_unique: bool,
    pub(crate) fields: Vec<Arc<Field>>,
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
    pub(crate) name: String,
    pub(crate) datatype: DataType,
    pub(crate) nullable: bool,
}

pub struct TableBuilder {
    name: Option<String>,
    fields: Vec<Field>,
    indices: Vec<IndexHolder>,
}

impl Field {
    pub fn new(name: String, datatype: DataType, nullable: bool) -> Result<Field, SchemaError> {
        match datatype {
            DataType::Blob(l) | DataType::Str(l) if l > 4 * 1024 * 1024 => {
                return Err(SchemaError::UserError("Max field size is 4MB.".into()));
            }
            _ => {}
        }
        Ok(Self {
            name,
            nullable,
            datatype,
        })
    }
}

impl TableBuilder {
    pub(crate) fn new() -> Self {
        Self {
            name: None,
            fields: vec![],
            indices: vec![],
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

        let fields = self
            .fields
            .iter()
            .map(|f| Arc::new(f.clone()))
            .collect::<Vec<_>>();

        let mut table = SqlTable {
            name: self.name.as_ref().unwrap().clone(),
            fields,
            indices: vec![],
            db_table_id: TableIdType::none(),
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
                if let Some(f) = table.fields.iter().find(|f| f.name == *index_f) {
                    index.fields.push(f.clone());
                }
            }
            table.indices.push(index);
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
        // Routed through Field::new (not a struct literal) so its size-cap
        // validation actually runs for SQL-parsed columns.
        Field::new(
            value.name.value.clone(),
            value.data_type.clone().into(),
            nullable,
        )
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
        let mut tb = TableBuilder::new();
        tb.with_name(name);
        for f in &fields {
            tb.with_field(f);
        }
        for i in indices {
            tb.with_index(&i.fields, i.name, i.is_primary, i.is_unique);
        }

        tb.build()
    }

    pub(crate) fn primary_key(&self) -> Option<&SqlIndex> {
        self.indices.iter().find(|i| i.is_primary)
    }

    // Every field's byte budget, summed (plus ENTRY_OVERHEAD_BYTES —
    // see its own comment) — the row-storage table's own
    // index_entry_size, mirroring SqlIndex::size() (indexed fields only)
    // but over the whole row, since the full row is what's stored there.
    pub(crate) fn row_size(&self) -> usize {
        self.fields.iter().map(|f| f.datatype.size()).sum::<usize>() + ENTRY_OVERHEAD_BYTES
    }

    // The position of `field` within this table's own declared field
    // order — used to pull a PRIMARY KEY/index's values back out of a
    // full row (built in that same order by rows_from_insert).
    fn field_position(&self, field: &Field) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field.name)
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
            self.fields.iter().collect()
        } else {
            insert
                .columns
                .iter()
                .map(|c| {
                    let name = c.to_string().to_lowercase();
                    self.fields.iter().find(|f| f.name == name).ok_or_else(|| {
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
            let mut full_row = Vec::with_capacity(self.fields.len());
            for f in &self.fields {
                match by_name.remove(f.name.as_str()) {
                    Some(v) => full_row.push(v),
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
