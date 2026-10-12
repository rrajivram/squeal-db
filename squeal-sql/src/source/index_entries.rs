// The rows of one partition as the entries of an index about to be built
// over them (see Schema::create_index): each row's values of the index's
// columns, then its identity — the PRIMARY KEY's values, or the generated
// row id. Read once, by the sort that puts the entries in key order, so
// the index is built by appending to its last leaf: leaves fill, where
// entries inserted in the rows' order land anywhere and leave leaves
// about two-thirds full.
use std::sync::Arc;

use store::{
    cursor::{Cursor, TableCursor},
    db::{DBFile, Db},
    table::TableIdType,
    tuple::DBIdType,
    txn::Transaction,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    datatype::DataType,
    error::SchemaError,
    source::{ProjectableField, Source, planinfo::PlanNode},
    table::{Field, SqlTable},
};

pub(crate) struct IndexEntries<F: DBFile + 'static> {
    table: Arc<SqlTable>,
    cursor: TableCursor<F>,
    columns: Vec<Arc<Field>>,
    // The columns decode_row fills: the index's.
    wanted: Vec<bool>,
    fields: Arc<[ProjectableField]>,
}

impl<F: DBFile + 'static> std::fmt::Debug for IndexEntries<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexEntries")
            .field("table", &self.table.name)
            .field("columns", &self.columns.len())
            .finish_non_exhaustive()
    }
}

impl<F> IndexEntries<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    /// The entries of an index on `columns` for the rows in `rows`, as
    /// `txn` sees them.
    pub(crate) fn new(
        db: &Arc<Db<F>>,
        table: Arc<SqlTable>,
        rows: TableIdType,
        txn: &Transaction,
        columns: &[Arc<Field>],
    ) -> Result<Self, SchemaError> {
        let current = table.fields();
        let wanted = current
            .iter()
            .map(|f| columns.iter().any(|c| c.id == f.id))
            .collect();
        let identity: Vec<Arc<Field>> = match table.primary_key() {
            Some(pk) => pk.fields.to_vec(),
            None => vec![Arc::new(Field::new(
                "rowid".into(),
                DataType::Integer,
                false,
                None,
            )?)],
        };
        let fields = columns
            .iter()
            .chain(&identity)
            .enumerate()
            .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            cursor: db.table_scan_in_txn(rows, txn)?,
            table,
            columns: columns.to_vec(),
            wanted,
            fields,
        })
    }
}

impl<F> Source for IndexEntries<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        PlanNode::new("IndexEntries").detail(self.table.name.clone())
    }

    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let Some(tuple) = self.cursor.next_ref()? else {
            return Ok(None);
        };
        let row = self.table.decode_row(tuple.data(), Some(&self.wanted))?;
        let mut values = self.table.extract_field_values(&self.columns, row.values());
        match tuple.id().to_owned() {
            DBIdType::Rec(ik) => values.extend_from_slice(ik.values()),
            DBIdType::Int(n) => values.push(ValueItem::Integer(n as i64)),
        }
        Ok(Some(IndexKey::new_from_owned(values)?))
    }

    // Read once: a scan under the build's transaction, which a rewind
    // would have to take again.
    fn reset(&mut self) -> Result<(), SchemaError> {
        Err(SchemaError::InternalSchemaError(
            "an index build's entries are read once".into(),
        ))
    }
}
