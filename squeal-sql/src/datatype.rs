use std::sync::Arc;

use log::warn;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharacterLength;
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
    Unsupported,
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
            DataType::Unsupported => 0,
        }
    }
}

impl From<sqlparser::ast::DataType> for DataType {
    fn from(value: sqlparser::ast::DataType) -> Self {
        match value {
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                DataType::Integer
            }
            sqlparser::ast::DataType::Double(_) => DataType::Double,
            sqlparser::ast::DataType::Datetime(_) | sqlparser::ast::DataType::Date => {
                DataType::Datetime
            }
            sqlparser::ast::DataType::String(l) => {
                DataType::Str(l.unwrap_or(DEFAULT_VAR_SIZE as u64) as u32)
            }
            sqlparser::ast::DataType::Text => DataType::Str(DEFAULT_VAR_SIZE as u32),
            sqlparser::ast::DataType::Varchar(l) => {
                let l = l.unwrap_or(CharacterLength::IntegerLength {
                    length: 32,
                    unit: None,
                });
                match l {
                    CharacterLength::IntegerLength { length, unit: _ } => {
                        DataType::Str(length as u32)
                    }
                    _ => DataType::Str(32),
                }
            }
            sqlparser::ast::DataType::Blob(l) => {
                DataType::Blob(l.unwrap_or(DEFAULT_VAR_SIZE as u64) as u32)
            }
            _ => {
                warn!("unsupported datatype: {:?}", value);
                DataType::Unsupported
            }
        }
    }
}
