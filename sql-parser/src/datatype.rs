use either::Either;
use macros::SQLParser;

use crate::{
    keyword::{Double, Int, Integer, Null, Uint64, Varchar},
    token::{LeftParenthesis, RightParenthesis},
};

#[derive(Debug, Clone, SQLParser)]
pub enum DataType {
    Null(Null),
    Int(Int),
    Double(Double),
    Datetime(Either<Uint64, String>),
    Varchar(Varchar, LeftParenthesis, Integer, RightParenthesis),
}
