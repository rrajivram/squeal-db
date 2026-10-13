//! Rows in flight as columns (see BATCH_EXECUTION_DESIGN.md).
//!
//! A batch is up to BATCH_ROWS rows of a source's row layout, held column
//! by column: integers, datetimes and doubles as plain vectors, a string
//! column as one buffer of bytes and where each value ends. A source that
//! gives batches (Source::advance_batch) refills one batch, so reading a
//! table this way allocates nothing per row: its buffers grow to the size
//! of a batch and stay. A value is looked at through `ValueRef`, a view.

use store::valueitem::{ValueItem, ValueRef};

use crate::{datatype::DataType, error::SchemaError};

/// Rows a batch holds at most.
pub(crate) const BATCH_ROWS: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct Batch {
    pub(crate) columns: Vec<Column>,
    len: usize,
}

impl Batch {
    /// An empty batch for rows of these column types; a column that isn't
    /// `wanted` is never filled (it reads as NULL).
    pub(crate) fn new(types: impl Iterator<Item = DataType>, wanted: Option<&[bool]>) -> Self {
        Batch {
            columns: types
                .enumerate()
                .map(|(i, t)| match wanted {
                    Some(w) if !w[i] => Column::absent(),
                    _ => Column::new(t),
                })
                .collect(),
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len >= BATCH_ROWS
    }

    /// Empties the batch, keeping its buffers.
    pub(crate) fn clear(&mut self) {
        for c in &mut self.columns {
            c.clear();
        }
        self.len = 0;
    }

    /// Counts the row whose values were just pushed to every column.
    pub(crate) fn row_pushed(&mut self) {
        self.len += 1;
    }

    /// Makes the batch readable once filled: a string column's bytes are
    /// checked as UTF-8 here, once for all its values.
    pub(crate) fn finish(&mut self) -> Result<(), SchemaError> {
        for c in &mut self.columns {
            c.finish()?;
        }
        Ok(())
    }
}

/// One column of a batch.
#[derive(Debug)]
pub(crate) struct Column {
    pub(crate) data: ColumnData,
    // Per row: whether the value is NULL (the vector holds a placeholder
    // there), and how many are.
    null: Vec<bool>,
    nulls: usize,
}

#[derive(Debug)]
pub(crate) enum ColumnData {
    /// Not read: every value is NULL.
    Absent,
    Integer(Vec<i64>),
    Datetime(Vec<u64>),
    Double(Vec<f64>),
    Boolean(Vec<bool>),
    /// Each value is `text[start..end]`, where `ends` holds each value's
    /// end and its start is the previous one's. `filling` takes the bytes
    /// until `finish` checks them and moves them into `text`; the buffer
    /// passes between the two, never copied.
    Str {
        ends: Vec<u32>,
        filling: Vec<u8>,
        text: String,
        capacity: u32,
    },
    Blob {
        ends: Vec<u32>,
        bytes: Vec<u8>,
        capacity: u32,
    },
    /// A column holding a value not of its type (a row in an older
    /// format can): its values one by one.
    Values(Vec<ValueItem>),
}

impl Column {
    fn new(t: DataType) -> Self {
        let data = match t {
            DataType::Integer => ColumnData::Integer(vec![]),
            DataType::Datetime => ColumnData::Datetime(vec![]),
            DataType::Double => ColumnData::Double(vec![]),
            DataType::Boolean => ColumnData::Boolean(vec![]),
            DataType::Str(n) => ColumnData::Str {
                ends: vec![],
                filling: vec![],
                text: String::new(),
                capacity: n,
            },
            DataType::Blob(n) => ColumnData::Blob {
                ends: vec![],
                bytes: vec![],
                capacity: n,
            },
            DataType::Null | DataType::Unsupported => ColumnData::Values(vec![]),
        };
        Column {
            data,
            null: vec![],
            nulls: 0,
        }
    }

    fn absent() -> Self {
        Column {
            data: ColumnData::Absent,
            null: vec![],
            nulls: 0,
        }
    }

    pub(crate) fn is_absent(&self) -> bool {
        matches!(self.data, ColumnData::Absent)
    }

    /// How many of the column's values are NULL.
    pub(crate) fn nulls(&self) -> usize {
        self.nulls
    }

    pub(crate) fn is_null(&self, row: usize) -> bool {
        self.null.get(row).copied().unwrap_or(true)
    }

    fn clear(&mut self) {
        self.null.clear();
        self.nulls = 0;
        match &mut self.data {
            ColumnData::Absent => {}
            ColumnData::Integer(v) => v.clear(),
            ColumnData::Datetime(v) => v.clear(),
            ColumnData::Double(v) => v.clear(),
            ColumnData::Boolean(v) => v.clear(),
            ColumnData::Str {
                ends,
                filling,
                text,
                ..
            } => {
                ends.clear();
                // The checked buffer back for refilling.
                *filling = std::mem::take(text).into_bytes();
                filling.clear();
            }
            ColumnData::Blob { ends, bytes, .. } => {
                ends.clear();
                bytes.clear();
            }
            ColumnData::Values(v) => v.clear(),
        }
    }

    fn finish(&mut self) -> Result<(), SchemaError> {
        if let ColumnData::Str {
            ends,
            filling,
            text,
            ..
        } = &mut self.data
        {
            let bytes = std::mem::take(filling);
            *text = String::from_utf8(bytes).map_err(|_| not_utf8())?;
            // Valid as a whole, and so each value is when each starts and
            // ends on a character's boundary.
            if !ends.iter().all(|&e| text.is_char_boundary(e as usize)) {
                return Err(not_utf8());
            }
        }
        Ok(())
    }

    pub(crate) fn push_null(&mut self) {
        self.null.push(true);
        self.nulls += 1;
        match &mut self.data {
            ColumnData::Absent => {}
            ColumnData::Integer(v) => v.push(0),
            ColumnData::Datetime(v) => v.push(0),
            ColumnData::Double(v) => v.push(0.0),
            ColumnData::Boolean(v) => v.push(false),
            ColumnData::Str { ends, filling, .. } => ends.push(filling.len() as u32),
            ColumnData::Blob { ends, bytes, .. } => ends.push(bytes.len() as u32),
            ColumnData::Values(v) => v.push(ValueItem::Null),
        }
    }

    /// Appends a value as a packed row holds it (see table.rs's Packed):
    /// an integer zigzagged and trimmed, a datetime trimmed, a double's 8
    /// bytes, a boolean's byte, a string's or blob's bytes. Err for bytes
    /// that can't be one.
    #[inline(always)]
    pub(crate) fn push_packed(&mut self, bytes: &[u8]) -> Result<(), SchemaError> {
        match &mut self.data {
            ColumnData::Absent => return Ok(()),
            ColumnData::Integer(v) => {
                let z = trimmed(bytes).ok_or_else(damaged)?;
                v.push((z >> 1) as i64 ^ -((z & 1) as i64));
            }
            ColumnData::Datetime(v) => v.push(trimmed(bytes).ok_or_else(damaged)?),
            ColumnData::Double(v) => {
                let b: [u8; 8] = bytes.try_into().map_err(|_| damaged())?;
                v.push(f64::from_le_bytes(b));
            }
            ColumnData::Boolean(v) => v.push(bytes.first().is_some_and(|b| *b != 0)),
            ColumnData::Str { ends, filling, .. } => {
                filling.extend_from_slice(bytes);
                ends.push(filling.len() as u32);
            }
            ColumnData::Blob { ends, bytes: b, .. } => {
                b.extend_from_slice(bytes);
                ends.push(b.len() as u32);
            }
            // A packed row's values are always their column's type.
            ColumnData::Values(_) => return Err(damaged()),
        }
        self.null.push(false);
        Ok(())
    }

    /// Appends `value`, of whatever type: a value not of the column's type
    /// turns the column into one of values (rare: rows in an older format).
    pub(crate) fn push(&mut self, value: ValueRef<'_>) {
        if matches!(value, ValueRef::Null) {
            self.push_null();
            return;
        }
        let fits = match (&mut self.data, value) {
            (ColumnData::Absent, _) => return,
            (ColumnData::Integer(v), ValueRef::Integer(i)) => {
                v.push(i);
                true
            }
            (ColumnData::Datetime(v), ValueRef::Datetime(d)) => {
                v.push(d);
                true
            }
            (ColumnData::Double(v), ValueRef::Double(d)) => {
                v.push(d);
                true
            }
            (ColumnData::Boolean(v), ValueRef::Boolean(b)) => {
                v.push(b);
                true
            }
            (ColumnData::Str { ends, filling, capacity, .. }, ValueRef::Str(s, c))
                if c == *capacity =>
            {
                filling.extend_from_slice(s.as_bytes());
                ends.push(filling.len() as u32);
                true
            }
            (ColumnData::Blob { ends, bytes, capacity }, ValueRef::Blob(b, c)) if c == *capacity => {
                bytes.extend_from_slice(b);
                ends.push(bytes.len() as u32);
                true
            }
            (ColumnData::Values(v), value) => {
                v.push(value.to_owned());
                true
            }
            _ => false,
        };
        if fits {
            self.null.push(false);
            return;
        }
        // The values so far, then this one.
        let mut values: Vec<ValueItem> = (0..self.null.len())
            .map(|i| self.unfinished(i).to_owned())
            .collect();
        values.push(value.to_owned());
        self.data = ColumnData::Values(values);
        self.null.push(false);
    }

    /// Row `row`'s value.
    #[inline(always)]
    pub(crate) fn get(&self, row: usize) -> ValueRef<'_> {
        if self.is_null(row) {
            return ValueRef::Null;
        }
        match &self.data {
            ColumnData::Absent => ValueRef::Null,
            ColumnData::Integer(v) => ValueRef::Integer(v[row]),
            ColumnData::Datetime(v) => ValueRef::Datetime(v[row]),
            ColumnData::Double(v) => ValueRef::Double(v[row]),
            ColumnData::Boolean(v) => ValueRef::Boolean(v[row]),
            ColumnData::Str {
                ends,
                text,
                capacity,
                ..
            } => ValueRef::Str(&text[span(ends, row)], *capacity),
            ColumnData::Blob {
                ends,
                bytes,
                capacity,
            } => ValueRef::Blob(&bytes[span(ends, row)], *capacity),
            ColumnData::Values(v) => v[row].as_ref(),
        }
    }

    // Row `row`'s value while the column is being filled (a string's
    // bytes not yet checked: read lossily, for the rare switch to values).
    fn unfinished(&self, row: usize) -> ValueItem {
        if self.is_null(row) {
            return ValueItem::Null;
        }
        match &self.data {
            ColumnData::Str {
                ends,
                filling,
                capacity,
                ..
            } => {
                let s = String::from_utf8_lossy(&filling[span(ends, row)]).into_owned();
                ValueItem::Str((s, *capacity))
            }
            _ => self.get(row).to_owned(),
        }
    }
}

fn span(ends: &[u32], row: usize) -> std::ops::Range<usize> {
    let start = if row == 0 { 0 } else { ends[row - 1] as usize };
    start..ends[row] as usize
}

// A number written trimmed: its little-endian bytes, high zero bytes
// dropped.
#[inline(always)]
fn trimmed(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 8 {
        return None;
    }
    let mut b = [0u8; 8];
    b[..bytes.len()].copy_from_slice(bytes);
    Some(u64::from_le_bytes(b))
}

fn damaged() -> SchemaError {
    SchemaError::UnknownError(
        "packed row damaged: a column runs past the row, or a value doesn't fit its type".into(),
    )
}

fn not_utf8() -> SchemaError {
    SchemaError::UnknownError("a stored string is not UTF-8".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_batch_holds_each_type_and_nulls_and_is_refilled() {
        let types = [DataType::Integer, DataType::Double, DataType::Str(8), DataType::Integer];
        let mut b = Batch::new(types.into_iter(), Some(&[true, true, true, false]));
        for round in 0..2 {
            b.clear();
            for i in 0..3i64 {
                let c = &mut b.columns;
                c[0].push(ValueRef::Integer(i * 10 + round));
                if i == 1 {
                    c[1].push_null();
                } else {
                    c[1].push(ValueRef::Double(i as f64 / 2.0));
                }
                c[2].push(ValueRef::Str(["é", "", "abc"][i as usize], 8));
                c[3].push(ValueRef::Integer(7));
                b.row_pushed();
            }
            b.finish().unwrap();
            assert_eq!(b.len(), 3);
            assert_eq!(b.columns[0].get(2), ValueRef::Integer(20 + round));
            assert_eq!(b.columns[1].get(1), ValueRef::Null);
            assert_eq!(b.columns[1].nulls(), 1);
            assert_eq!(b.columns[2].get(0), ValueRef::Str("é", 8));
            assert_eq!(b.columns[2].get(1), ValueRef::Str("", 8));
            assert_eq!(b.columns[2].get(2), ValueRef::Str("abc", 8));
            // Not read: NULL.
            assert_eq!(b.columns[3].get(0), ValueRef::Null);
        }
    }

    #[test]
    fn test_a_value_of_another_type_turns_the_column_into_values() {
        let mut b = Batch::new([DataType::Integer].into_iter(), None);
        b.columns[0].push(ValueRef::Integer(1));
        b.columns[0].push(ValueRef::Double(2.5));
        b.columns[0].push_null();
        b.finish().unwrap();
        assert_eq!(b.columns[0].get(0), ValueRef::Integer(1));
        assert_eq!(b.columns[0].get(1), ValueRef::Double(2.5));
        assert_eq!(b.columns[0].get(2), ValueRef::Null);
    }

    #[test]
    fn test_packed_values_decode() {
        let mut b = Batch::new([DataType::Integer, DataType::Double].into_iter(), None);
        // zigzag: -2 is 3.
        b.columns[0].push_packed(&[3]).unwrap();
        b.columns[0].push_packed(&[]).unwrap();
        b.columns[1].push_packed(&1.5f64.to_le_bytes()).unwrap();
        assert!(b.columns[1].push_packed(&[1, 2]).is_err());
        assert_eq!(b.columns[0].get(0), ValueRef::Integer(-2));
        assert_eq!(b.columns[0].get(1), ValueRef::Integer(0));
        assert_eq!(b.columns[1].get(0), ValueRef::Double(1.5));
    }
}
