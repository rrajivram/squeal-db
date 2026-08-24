//! Literal values: numbers, strings, booleans, NULL.

use chumsky::{Parser, extra::ParserExtra, label::LabelError};
use either::Either;
use macros::SQLParser;

use crate::{
    keyword as kw,
    parser::{SQLParser, TokenInput, token},
    span::TokenSpan,
    token::{StringStyle, Token},
};

/// A single-quoted string literal, with the `''` escape already resolved.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StringLiteral {
    pub span: TokenSpan,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumberLiteral {
    pub span: TokenSpan,
    pub raw: String,
    pub value: NumberValue,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumberValue {
    Integer(i64),
    Float(f64),
}

impl NumberLiteral {
    pub fn as_f64(&self) -> f64 {
        match self.value {
            NumberValue::Integer(i) => i as f64,
            NumberValue::Float(f) => f,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self.value {
            NumberValue::Integer(i) => Some(i),
            NumberValue::Float(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SQLParser)]
pub struct BooleanLiteral(pub Either<kw::True, kw::False>);

impl BooleanLiteral {
    pub fn value(&self) -> bool {
        self.0.is_left()
    }
}

#[derive(Debug, Clone, PartialEq, SQLParser)]
pub enum Literal {
    Number(NumberLiteral),
    String(StringLiteral),
    Boolean(BooleanLiteral),
    Null(kw::Null),
}

impl<'src, I, E, A> SQLParser<'src, I, E, A> for StringLiteral
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: A) -> impl Parser<'src, I, Self, E> + Clone {
        token("string literal", |t| match &t.token {
            Token::String {
                raw,
                kind: StringStyle::SingleQuoted(_),
            } => Some(StringLiteral {
                span: t.span,
                value: raw.replace("''", "'"),
            }),
            _ => None,
        })
    }
}

impl<'src, I, E, A> SQLParser<'src, I, E, A> for NumberLiteral
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: A) -> impl Parser<'src, I, Self, E> + Clone {
        token("number", |t| match &t.token {
            Token::Number { raw } => {
                let value = match raw.parse::<i64>() {
                    Ok(i) => NumberValue::Integer(i),
                    // not an i64 (has a '.', or overflows) — fall back to f64
                    Err(_) => NumberValue::Float(raw.parse::<f64>().ok()?),
                };
                Some(NumberLiteral {
                    span: t.span,
                    raw: (*raw).to_string(),
                    value,
                })
            }
            _ => None,
        })
    }
}
