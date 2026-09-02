use std::{fmt::Display, sync::Arc};

use log::warn;
use serde::{Deserialize, Serialize};
use store::valueitem::ValueItem;

use crate::constant::DEFAULT_VAR_SIZE;

/// A column's declared type — distinct from `ValueItem`, which describes a
/// stored *value*. There is deliberately no `Null` variant: null isn't a
/// type, it's the absence of a value, so whether a column accepts it is a
/// nullability concern (a separate flag on `Field`), not a `DataType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Double,
    Datetime,
    Str(u32),
    Blob(u32),
    Null,
    Unsupported,
    // Appended last, not inserted among the existing variants — this enum
    // derives Serialize/Deserialize and serde's derive keys variants by
    // declaration index, so inserting here would silently rotate every
    // already-serialized DataType (persisted inside Field, see table.rs).
    Boolean,
}

impl DataType {
    /// The `DataType` a value would need to satisfy — `None` for
    /// `ValueItem::Null`, since a null value carries no type information of
    /// its own to project.
    pub fn of(value: &ValueItem) -> Option<DataType> {
        match value {
            ValueItem::Null => None,
            ValueItem::Integer(_) => Some(DataType::Integer),
            ValueItem::Double(_) => Some(DataType::Double),
            ValueItem::Datetime(_) => Some(DataType::Datetime),
            ValueItem::Str((_, cap)) => Some(DataType::Str(*cap)),
            ValueItem::Blob((_, cap)) => Some(DataType::Blob(*cap)),
            ValueItem::Boolean(_) => Some(DataType::Boolean),
        }
    }

    /// Whether `value` is a legal value for a column declared as `self`.
    /// `ValueItem::Null` always matches — enforcing NOT NULL is a
    /// nullability concern, not this method's job.
    pub fn matches(&self, value: &ValueItem) -> bool {
        match (self, value) {
            (_, ValueItem::Null) => true,
            (DataType::Integer, ValueItem::Integer(_)) => true,
            (DataType::Double, ValueItem::Double(_)) => true,
            (DataType::Datetime, ValueItem::Datetime(_)) => true,
            (DataType::Str(cap), ValueItem::Str((_, vcap))) => vcap <= cap,
            (DataType::Blob(cap), ValueItem::Blob((_, vcap))) => vcap <= cap,
            (DataType::Boolean, ValueItem::Boolean(_)) => true,
            _ => false,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            DataType::Integer => ValueItem::Integer(0).size(),
            DataType::Double => ValueItem::Double(0.).size(),
            DataType::Datetime => ValueItem::Datetime(0).size(),
            DataType::Str(l) => ValueItem::Str(("".into(), *l)).size(),
            DataType::Blob(l) => ValueItem::Blob((Arc::new([0u8]), *l)).size(),
            DataType::Boolean => ValueItem::Boolean(false).size(),
            DataType::Null => 0,
            DataType::Unsupported => 0,
        }
    }
}

// The length argument out of a parenthesized `(n)` / `(n, m)` type suffix
// (VARCHAR(n), CHAR(n), ...) — None for the bare, unparenthesized form.
fn args1_len(args: &sql_parser::datatype::Args1) -> Option<u32> {
    args.as_ref()
        .and_then(|(_, n, _)| n.as_i64())
        .map(|n| n as u32)
}

impl From<sql_parser::datatype::DataType> for DataType {
    fn from(value: sql_parser::datatype::DataType) -> Self {
        use sql_parser::datatype::DataType as SqlDataType;
        match value {
            SqlDataType::TinyInt(_)
            | SqlDataType::SmallInt(_)
            | SqlDataType::BigInt(_)
            | SqlDataType::Integer(_)
            | SqlDataType::Int8(_)
            | SqlDataType::Int16(_)
            | SqlDataType::Int32(_)
            | SqlDataType::Int64(_)
            | SqlDataType::Int(_)
            | SqlDataType::Uint8(_)
            | SqlDataType::Uint16(_)
            | SqlDataType::Uint32(_)
            | SqlDataType::Uint64(_) => DataType::Integer,
            SqlDataType::Float32(_)
            | SqlDataType::Float64(_)
            | SqlDataType::Float(_)
            | SqlDataType::Double(_)
            | SqlDataType::Real(_)
            | SqlDataType::DoublePrecision(_, _) => DataType::Double,
            SqlDataType::Datetime(_)
            | SqlDataType::Timestamp(_)
            | SqlDataType::Date(_)
            | SqlDataType::Time(_) => DataType::Datetime,
            SqlDataType::Text(_) | SqlDataType::String(_) => DataType::Str(DEFAULT_VAR_SIZE as u32),
            SqlDataType::Varchar(_, args)
            | SqlDataType::Char(_, args)
            | SqlDataType::Character(_, args) => DataType::Str(args1_len(&args).unwrap_or(32)),
            SqlDataType::Bytea(_) | SqlDataType::Binary(_) => {
                DataType::Blob(DEFAULT_VAR_SIZE as u32)
            }
            SqlDataType::Boolean(_) => DataType::Boolean,
            other @ (SqlDataType::Decimal(_, _) | SqlDataType::Numeric(_, _)) => {
                warn!("unsupported datatype: {:?}", other);
                DataType::Unsupported
            }
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DataType::Integer => "int".into(),
            DataType::Double => "float".into(),
            DataType::Datetime => "datetime".into(),
            DataType::Str(n) => format!("varchar({n})"),
            DataType::Blob(n) => format!("blob({n})"),
            DataType::Boolean => "boolean".into(),
            DataType::Null => "(null)".into(),
            DataType::Unsupported => "*err*".into(),
        };
        write!(f, "{s}")?;
        Ok(())
    }
}
