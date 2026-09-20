use std::{fmt::Display, sync::Arc};

use log::warn;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, SeqAccess, Visitor},
};
use store::valueitem::ValueItem;

use crate::constant::DEFAULT_VAR_SIZE;

/// A column's declared type — distinct from `ValueItem`, which describes a
/// stored *value*. There is deliberately no `Null` variant: null isn't a
/// type, it's the absence of a value, so whether a column accepts it is a
/// nullability concern (a separate flag on `Field`), not a `DataType`.
///
/// `Serialize`/`Deserialize` are hand-rolled, not derived: this enum is
/// persisted inside `Field` (see table.rs), and a derived enum's wire tag is
/// its declaration index, which would silently rotate every already-
/// persisted `DataType` if a variant were ever inserted anywhere but the
/// end. The tags below are fixed forever at their historical declaration-
/// index values (Integer=0 .. Boolean=7); a future variant picks an unused
/// tag (10+ is free) rather than reordering these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Double,
    Datetime,
    Str(u32),
    Blob(u32),
    Null,
    Unsupported,
    Boolean,
}

impl Serialize for DataType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DataType::Integer => (0u8,).serialize(serializer),
            DataType::Double => (1u8,).serialize(serializer),
            DataType::Datetime => (2u8,).serialize(serializer),
            DataType::Str(n) => (3u8, *n).serialize(serializer),
            DataType::Blob(n) => (4u8, *n).serialize(serializer),
            DataType::Null => (5u8,).serialize(serializer),
            DataType::Unsupported => (6u8,).serialize(serializer),
            DataType::Boolean => (7u8,).serialize(serializer),
        }
    }
}

struct DataTypeVisitor;

impl<'de> Visitor<'de> for DataTypeVisitor {
    type Value = DataType;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "a tagged DataType")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DataType, A::Error> {
        let tag: u8 = seq
            .next_element()?
            .ok_or_else(|| DeError::custom("missing DataType tag"))?;
        match tag {
            0 => Ok(DataType::Integer),
            1 => Ok(DataType::Double),
            2 => Ok(DataType::Datetime),
            3 => Ok(DataType::Str(seq.next_element()?.ok_or_else(|| {
                DeError::custom("DataType::Str: missing length")
            })?)),
            4 => Ok(DataType::Blob(seq.next_element()?.ok_or_else(|| {
                DeError::custom("DataType::Blob: missing length")
            })?)),
            5 => Ok(DataType::Null),
            6 => Ok(DataType::Unsupported),
            7 => Ok(DataType::Boolean),
            other => Err(DeError::custom(format!("unknown DataType tag {other}"))),
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_tuple(2, DataTypeVisitor)
    }
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

#[cfg(test)]
mod tests {
    use super::DataType;

    fn all_variants() -> [DataType; 8] {
        [
            DataType::Integer,
            DataType::Double,
            DataType::Datetime,
            DataType::Str(10),
            DataType::Blob(20),
            DataType::Null,
            DataType::Unsupported,
            DataType::Boolean,
        ]
    }

    #[test]
    fn test_round_trip() {
        for v in all_variants() {
            let bytes = postcard::to_allocvec(&v).unwrap();
            let back: DataType = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn test_unknown_tag_errors() {
        assert!(postcard::from_bytes::<DataType>(&[99]).is_err());
    }

    // Fixtures captured from the pre-Stage-1 `#[derive(Serialize,
    // Deserialize)]` encoding (commit bfbc240), before DataType grew a
    // hand-rolled codec.
    #[test]
    fn test_decodes_pre_stage1_derived_fixtures() {
        let fixtures: [(&[u8], DataType); 8] = [
            (&[0], DataType::Integer),
            (&[1], DataType::Double),
            (&[2], DataType::Datetime),
            (&[3, 10], DataType::Str(10)),
            (&[4, 20], DataType::Blob(20)),
            (&[5], DataType::Null),
            (&[6], DataType::Unsupported),
            (&[7], DataType::Boolean),
        ];
        for (bytes, expected) in fixtures {
            assert_eq!(postcard::from_bytes::<DataType>(bytes).unwrap(), expected);
        }
    }
}
