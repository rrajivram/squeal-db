use std::{fmt::Debug, sync::Arc};

use store::valueitem::IndexKey;

use crate::{error::SchemaError, plan::eval::EvalExpr, table::Field};

pub mod group;
pub(crate) mod join;
pub mod limit;
pub mod proj;
pub(crate) mod run;
pub mod sort;
pub mod table;
pub mod where_source;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ProjectableField {
    pub(crate) field: Arc<Field>,
    pub(crate) display_name: String,
    pub(crate) source_id: usize,
    pub(crate) field_id: usize,
    pub(crate) expr: EvalExpr,
}

pub trait Source: Debug {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError>;
    fn fields(&self) -> Arc<[ProjectableField]>;
    fn reset(&mut self) -> Result<(), SchemaError>;
}

// Shared by every other Source's own test module (join, limit, proj,
// where_source, ...) — a minimal in-memory Source backed by a plain
// Vec<Vec<ValueItem>>, so those tests can exercise combining/filtering/
// limiting logic in isolation without a real Db/SqlTable/Connection.
#[cfg(test)]
pub(crate) mod test_support {
    use store::valueitem::{IndexKey, ValueItem};

    use super::{Arc, Debug, ProjectableField, SchemaError, Source};
    use crate::table::Field;

    #[derive(Debug)]
    pub(crate) struct VecSource {
        rows: Vec<Vec<ValueItem>>,
        pos: usize,
        fields: Arc<[ProjectableField]>,
    }

    impl VecSource {
        pub(crate) fn new(field_names: &[&str], rows: Vec<Vec<ValueItem>>) -> Self {
            let fields = field_names
                .iter()
                .enumerate()
                .map(|(i, name)| ProjectableField::from_field(Arc::new(Field::from(*name)), 0, i))
                .collect::<Vec<_>>();
            Self {
                rows,
                pos: 0,
                fields: Arc::from(fields),
            }
        }
    }

    impl Source for VecSource {
        fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
            if self.pos >= self.rows.len() {
                return Ok(None);
            }
            let row = self.rows[self.pos].clone();
            self.pos += 1;
            Ok(Some(IndexKey::new_from_owned(row)?))
        }

        fn fields(&self) -> Arc<[ProjectableField]> {
            self.fields.clone()
        }

        fn reset(&mut self) -> Result<(), SchemaError> {
            self.pos = 0;
            Ok(())
        }
    }

    // Test-only convenience: drain every remaining row as plain
    // Vec<ValueItem>, so a test can assert on values directly instead of
    // destructuring IndexKey each time.
    pub(crate) fn drain(source: &mut dyn Source) -> Vec<Vec<ValueItem>> {
        let mut out = vec![];
        while let Some(row) = source.next().unwrap() {
            out.push(row.values().to_vec());
        }
        out
    }
}
