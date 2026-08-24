//! SQL data types as they appear in DDL (`INT`, `VARCHAR(128)`,
//! `DECIMAL(10, 2)`, ...).

use either::Either;
use macros::SQLParser;

use crate::{
    keyword as kw,
    literal::NumberLiteral,
    token::{Comma, LeftParenthesis, RightParenthesis},
};

/// An optional parenthesized argument list like `(128)` or `(10, 2)`.
pub type Args1 = Option<(LeftParenthesis, NumberLiteral, RightParenthesis)>;
pub type Args2 = Option<(
    LeftParenthesis,
    NumberLiteral,
    Option<(Comma, NumberLiteral)>,
    RightParenthesis,
)>;

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum DataType {
    // Multi-word / parameterized types.
    DoublePrecision(kw::Double, kw::Precision),
    Varchar(kw::Varchar, Args1),
    Char(kw::Char, Args1),
    Character(kw::Character, Args1),
    Decimal(kw::Decimal, Args2),
    Numeric(kw::Numeric, Args2),
    // Plain keyword types.
    TinyInt(kw::Tinyint),
    SmallInt(kw::Smallint),
    BigInt(kw::Bigint),
    Integer(kw::Integer),
    Int8(kw::Int8),
    Int16(kw::Int16),
    Int32(kw::Int32),
    Int64(kw::Int64),
    Int(kw::Int),
    Uint8(kw::Uint8),
    Uint16(kw::Uint16),
    Uint32(kw::Uint32),
    Uint64(kw::Uint64),
    Float32(kw::Float32),
    Float64(kw::Float64),
    Float(kw::Float),
    Double(kw::Double),
    Real(kw::Real),
    Boolean(Either<kw::Boolean, kw::Bool>),
    Text(kw::Text),
    String(kw::String),
    Bytea(kw::Bytea),
    Binary(kw::Binary),
    Datetime(kw::Datetime),
    Timestamp(kw::Timestamp),
    Date(kw::Date),
    Time(kw::Time),
}
