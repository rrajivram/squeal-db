use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption, CreateTable};
use store::db::DBFile;
use store::table::TableIdType;

use crate::constant::MAX_TABLE_NAME_LEN;
use crate::datatype::DataType;
use crate::error::SchemaError;
use crate::schema::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlTable {
    pub(crate) name: String,
    pub(crate) fields: Vec<Arc<Field>>,
    pub(crate) indices: Vec<SqlIndex>,
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
}

impl SqlIndex {
    pub(crate) fn size(&self) -> usize {
        self.fields.iter().map(|f| f.datatype.size()).sum()
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
