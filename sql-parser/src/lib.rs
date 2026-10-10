//! A SQL parser built on [chumsky].
//!
//! Pipeline: [`lexer::tokenize`] turns source text into keyword-classified,
//! span-carrying tokens; the [`parser::SQLParser`] trait (mostly implemented
//! via `#[derive(SQLParser)]` from the `macros` crate) turns tokens into the
//! AST; [`parse_sql`] is the front door.

// Lets the derive macro emit `::sql_parser::...` paths that work both inside
// this crate and from dependent crates.
extern crate self as sql_parser;

mod bind;
mod cache;
pub mod combo;
pub mod datatype;
pub mod ddl;
pub mod dml;
pub mod expr;
pub mod ident;
pub mod keyword;
pub mod lexer;
pub mod literal;
mod params;
pub mod parser;
pub mod query;
pub mod span;
pub mod statement;
pub mod token;
pub mod utils;
mod values;
pub mod visitor;

use chumsky::{
    IterParser, Parser,
    error::{EmptyErr, Rich},
    extra,
    prelude::end,
};

pub use crate::cache::{CacheStats, cache_stats, parse_sql_cached};
pub use crate::{
    expr::{Expr, Placeholder},
    ident::{Ident, ObjectName},
    parser::{SQLParser, SqlCtx},
    query::Query,
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

    // First with errors that carry nothing: chumsky builds an error for
    // every alternative it tries, and a Rich one — its expected tokens, a
    // label string each — cost most of a large INSERT's parse even when
    // the text parses. The grammar is the same either way, so a text that
    // parses gives the same statements; only a failure is parsed again,
    // with Rich errors, for its message.
    if let Some(stmts) = values::fast_insert(&tokens) {
        return Ok(stmts);
    }
    if let Ok(stmts) = parse_tokens::<EmptyErr>(&tokens).into_result() {
        return Ok(stmts);
    }
    let result: Result<_, Vec<Rich<TokenStruct>>> = parse_tokens(&tokens).into_result();
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

// Statements separated by semicolons, with errors of type `Error`.
pub(crate) fn parse_tokens<'src, Error>(
    tokens: &'src [TokenStruct<'src>],
) -> chumsky::ParseResult<Vec<Statement>, Error>
where
    Error: chumsky::error::Error<'src, &'src [TokenStruct<'src>]>
        + chumsky::label::LabelError<'src, &'src [TokenStruct<'src>], String>
        + 'src,
{
    type TokInput<'src> = &'src [TokenStruct<'src>];
    let semi = punct::<TokInput, extra::Err<Error>>(Punctuation::Semicolon);
    let parser = <Statement as SQLParser<TokInput, extra::Err<Error>>>::parser(())
        .separated_by(semi.repeated().at_least(1))
        .allow_trailing()
        .collect::<Vec<_>>()
        .then_ignore(end());
    parser.parse(tokens)
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

#[cfg(test)]
mod error_type_tests {
    use super::*;

    // parse_sql takes the cheap-error parse when it succeeds: it must give
    // exactly the statements the Rich-error parse does.
    #[test]
    fn test_cheap_and_rich_errors_parse_alike() {
        let corpus = [
            "select a, b + 1 as c from t where x = 1 and (y < 2 or z is not null) order by a desc limit 3 offset 1",
            "select distinct t.* from t join u on t.id = u.id left join v using (k) where v.n like 'a%'",
            "with recursive r (n) as (select 1 union all select n + 1 from r where n < 5) select n from r",
            "select x from t where x in (select y from u where u.k = t.k) and not exists (select 1 from w)",
            "select (select max(a) from t), case when b then 1 when c then 2 else 3 end, cast(d as varchar(4)) from t",
            "select count(distinct a), sum(b) from t group by c having count(*) > 1 intersect select 1, 2",
            "insert into t (a, b) values (1, 'x'), (-2, null), (3.5, 'it''s')",
            "insert into t select * from u where u.a between 1 and 9",
            "update t set a = a + 1, b = 'q' where id = ?",
            "delete from t where id in (1, 2, 3)",
            "create table t (id integer not null, n varchar(10) default 'x', primary key(id), foreign key (n) references u(n))",
            "create table p (id integer not null, d integer) partition by range (d) (partition a values less than (10), partition b values less than maxvalue)",
            "alter table t add column c double",
            "create unique index if not exists ix on t (a desc, b)",
            "drop table if exists t, u",
            "begin; commit; rollback",
            "explain select * from t",
            "prepare q (integer) as select * from t where id = $1",
            "show tables; describe table t; analyze tables",
            "copy into t from @data/file.csv",
        ];
        for sql in corpus {
            let tokens = lexer::tokenize(sql).unwrap();
            let cheap = parse_tokens::<EmptyErr>(&tokens).into_result();
            let rich = parse_tokens::<Rich<TokenStruct>>(&tokens).into_result();
            match (cheap, rich) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "{sql}"),
                (Err(_), Err(_)) => panic!("corpus statement does not parse: {sql}"),
                (a, b) => panic!("{sql}: cheap {:?}, rich {:?}", a.is_ok(), b.is_ok()),
            }
        }
    }
}
