//! A SQL parser built on [chumsky].
//!
//! Pipeline: [`lexer::tokenize`] turns source text into keyword-classified,
//! span-carrying tokens; the [`parser::SQLParser`] trait (mostly implemented
//! via `#[derive(SQLParser)]` from the `macros` crate) turns tokens into the
//! AST; [`parse_sql`] is the front door.

// Lets the derive macro emit `::sql_parser::...` paths that work both inside
// this crate and from dependent crates.
extern crate self as sql_parser;

pub mod combo;
pub mod datatype;
pub mod ddl;
pub mod dml;
pub mod expr;
pub mod ident;
pub mod keyword;
pub mod lexer;
pub mod literal;
pub mod parser;
pub mod query;
pub mod span;
pub mod statement;
pub mod token;
pub mod utils;

use chumsky::{IterParser, Parser, error::Rich, extra, prelude::end};

pub use crate::{
    expr::Expr,
    ident::{Ident, ObjectName},
    parser::SQLParser,
    span::TokenSpan,
    statement::Statement,
};
use crate::{
    parser::punct,
    token::{Punctuation, TokenStruct},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    /// Byte range in the source text, when known.
    pub span: Option<TokenSpan>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.span {
            Some(s) => write!(f, "{} at {}..{}", self.message, s.start, s.end),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a string of zero or more semicolon-separated SQL statements.
///
/// ```
/// use sql_parser::{parse_sql, Statement};
///
/// let stmts = parse_sql("SELECT name, count(*) FROM users GROUP BY name; COMMIT;").unwrap();
/// assert_eq!(stmts.len(), 2);
/// assert!(matches!(stmts[0], Statement::Select(_)));
///
/// let err = parse_sql("SELECT FROM").unwrap_err();
/// assert!(err[0].span.is_some());
/// ```
pub fn parse_sql(src: &str) -> Result<Vec<Statement>, Vec<ParseError>> {
    let tokens = lexer::tokenize(src).map_err(|errs| {
        errs.into_iter()
            .map(|e| ParseError {
                message: e.to_string(),
                span: Some(TokenSpan::from(*e.span())),
            })
            .collect::<Vec<_>>()
    })?;

    type TokInput<'src> = &'src [TokenStruct<'src>];
    type TokExtra<'src> = extra::Err<Rich<'src, TokenStruct<'src>>>;

    let semi = punct::<TokInput, TokExtra>(Punctuation::Semicolon);
    let parser = <Statement as SQLParser<TokInput, TokExtra>>::parser(())
        .separated_by(semi.repeated().at_least(1))
        .allow_trailing()
        .collect::<Vec<_>>()
        .then_ignore(end());

    let result: Result<_, Vec<Rich<TokenStruct>>> = parser
        .parse(&tokens[..])
        .into_result();
    result.map_err(|errs| {
        errs.into_iter()
            .map(|e| {
                // Parser error spans index into the token list; translate
                // back to byte offsets in the source.
                let span = token_index_span_to_source(&tokens, e.span().start, e.span().end, src);
                ParseError {
                    message: e.to_string(),
                    span,
                }
            })
            .collect()
    })
}

fn token_index_span_to_source(
    tokens: &[TokenStruct],
    start: usize,
    end: usize,
    src: &str,
) -> Option<TokenSpan> {
    if tokens.is_empty() {
        return None;
    }
    let start_byte = match tokens.get(start) {
        Some(t) => t.span.start,
        None => src.len(),
    };
    let end_byte = if end > start {
        tokens
            .get(end - 1)
            .map(|t| t.span.end)
            .unwrap_or(src.len())
    } else {
        start_byte
    };
    Some(TokenSpan {
        start: start_byte,
        end: end_byte,
    })
}

/// Parse exactly one statement (a trailing semicolon is allowed).
pub fn parse_one(src: &str) -> Result<Statement, Vec<ParseError>> {
    let mut stmts = parse_sql(src)?;
    match stmts.len() {
        1 => Ok(stmts.pop().unwrap()),
        n => Err(vec![ParseError {
            message: format!("expected exactly one statement, found {n}"),
            span: None,
        }]),
    }
}
