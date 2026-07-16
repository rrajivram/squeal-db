use serde::{Deserialize, Serialize};
use store::valueitem::ValueItem;

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
}
