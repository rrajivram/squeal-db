use store::valueitem::{IndexKey, ValueItem};

use crate::{error::SchemaError, source::Source};

// Neither this nor ResultType below needs a DBFile type parameter —
// rows are materialized eagerly (see Schema::select_all), so a
// ResultSet is just plain data by the time a Statement produces one,
// with no live connection/backend reference to carry along.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    columns: Vec<String>,
    rows: Vec<Vec<ValueItem>>,
}

pub struct StreamingResultSet {
    begin: Box<dyn Source>,
}

impl StreamingResultSet {
    pub(crate) fn new(begin: Box<dyn Source>) -> Self {
        Self { begin }
    }

    pub fn columns(&self) -> Vec<String> {
        self.begin
            .fields()
            .iter()
            .map(|f| f.display_name.clone())
            .collect()
    }

    pub fn next_result(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        self.begin.as_mut().next()
    }

    pub fn next_result_as_strings(&mut self) -> Result<Option<Vec<String>>, SchemaError> {
        Ok(self
            .begin
            .as_mut()
            .next()?
            .map(|i| i.values().iter().map(|n| n.to_string()).collect::<Vec<_>>()))
    }
}

impl ResultSet {
    pub(crate) fn new(columns: Vec<String>, rows: Vec<Vec<ValueItem>>) -> Self {
        Self { columns, rows }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[Vec<ValueItem>] {
        &self.rows
    }

    // Every row rendered as display strings, in column order — the
    // shape any tabular renderer (the CLI's comfy-table today, maybe
    // others later) wants directly, so the ValueItem -> String mapping
    // lives in exactly one place rather than being reimplemented by
    // each consumer.
    pub fn rows_as_strings(&self) -> Vec<Vec<String>> {
        self.rows
            .iter()
            .map(|row| row.iter().map(value_item_to_string).collect())
            .collect()
    }
}

fn value_item_to_string(v: &ValueItem) -> String {
    match v {
        ValueItem::Null => "NULL".to_string(),
        ValueItem::Integer(i) => i.to_string(),
        ValueItem::Double(d) => d.to_string(),
        ValueItem::Datetime(d) => d.to_string(),
        ValueItem::Str((s, _)) => s.clone(),
        ValueItem::Blob((b, _)) => format!("<blob, {} bytes>", b.len()),
    }
}

#[derive(Debug)]
pub enum ResultType {
    // Rows affected — currently only INSERT produces this; UPDATE/DELETE
    // will too once they exist.
    Count(usize),
    // A query result set — currently only SELECT * FROM <table>
    // produces this (see Schema::select_all); grows as real relational
    // algebra support (WHERE, JOIN, projections, ...) lands.
    Result(ResultSet),
    // A human-readable outcome for statements with no row count or rows
    // of their own (CREATE TABLE/DATABASE/SCHEMA, USE DATABASE/SCHEMA,
    // BEGIN/COMMIT/ROLLBACK, ...) — e.g. "Table 'users' created".
    ResultString(String),
    StreamingResult(StreamingResultSet),
}

impl std::fmt::Debug for StreamingResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = write!(f, "Streaming Results");
        Ok(())
    }
}

impl PartialEq for StreamingResultSet {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}
