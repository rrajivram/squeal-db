use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use store::db::DBFile;

use crate::datatype::DataType;
use crate::error::SchemaError;
use crate::schema::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlTable {
    pub(crate) name: String,
    fields: Vec<Arc<Field>>,
    indices: Vec<Index>,
}

/* #[derive(Debug, Clone)]
pub struct ForeignKey<'a,'b> {

}
 */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    name: Option<String>,
    is_primary: bool,
    is_unique: bool,
    fields: Vec<Arc<Field>>,
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
    name: String,
    datatype: DataType,
    nullable: bool,
}

pub struct TableBuilder<F: DBFile> {
    db: Arc<Schema<F>>,
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

impl<F: DBFile> TableBuilder<F>
where
    F: DBFile + 'static,
{
    pub(crate) fn new(db: Arc<Schema<F>>) -> Self {
        Self {
            db,
            name: None,
            fields: vec![],
            indices: vec![],
        }
    }

    pub fn with_name(&mut self, name: String) -> &mut Self {
        self.name = Some(name);
        self
    }

    pub fn with_field(&mut self, field: Field) -> &mut Self {
        self.fields.push(field);
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
        let field_names = self
            .fields
            .iter()
            .map(|f| (f.name.clone(), f))
            .collect::<HashMap<_, _>>();
        if !field_names.iter().all(|f| f.0.len() < 128) {
            return Err(SchemaError::UserError(
                "Max field name length is 128".into(),
            ));
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
            let mut index = Index {
                name: i.name.clone(),
                is_primary: i.is_primary,
                is_unique: i.is_unique,
                fields: vec![],
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
