//! A fast path for the one statement whose size is its data: `INSERT ...
//! VALUES (..), (..), ...` with rows of plain literals.
//!
//! The grammar parses each value as a whole expression — a dozen
//! precedence layers tried and a few allocations for what is one token —
//! which is nearly all of a bulk INSERT's parse. Here the statement's head
//! and its first row go through the grammar as ever (`insert into t (a, b)
//! values (1, 'x')`), and every further row is built straight from its
//! tokens, into exactly what the grammar builds (a test parses both ways
//! and compares). A row with anything but literals — a number, a signed
//! number, a single-quoted string, NULL, TRUE, FALSE — and the whole
//! statement is the grammar's again.

use chumsky::error::EmptyErr;
use either::Either;

use crate::{
    Expr, Statement,
    dml::{InsertSource, ValuesRow},
    expr::UnaryOp,
    keyword::{self as kw, Keyword},
    literal::{BooleanLiteral, Literal, NumberLiteral, NumberValue, StringLiteral},
    parse_tokens,
    token::{Comma, LeftParenthesis, Punctuation, RightParenthesis, StringStyle, Token, TokenStruct},
    utils::Seq,
};

// Below this many rows after the first, the grammar is fast enough.
const MIN_ROWS: usize = 8;

fn is(t: &TokenStruct, p: Punctuation) -> bool {
    matches!(&t.token, Token::Punctuation(q) if *q == p)
}

fn keyword(t: &TokenStruct) -> Option<Keyword> {
    match &t.token {
        Token::Word { keyword, .. } => *keyword,
        _ => None,
    }
}

/// `tokens` as one INSERT of literal rows, or None for anything else (and
/// for whatever of it the grammar would refuse).
pub(crate) fn fast_insert<'src>(tokens: &'src [TokenStruct<'src>]) -> Option<Vec<Statement>> {
    if keyword(tokens.first()?) != Some(Keyword::Insert) {
        return None;
    }
    // VALUES, outside any parentheses (a column list comes before it).
    let mut depth = 0usize;
    let mut values = None;
    for (i, t) in tokens.iter().enumerate() {
        if is(t, Punctuation::LeftParenthesis) {
            depth += 1;
        } else if is(t, Punctuation::RightParenthesis) {
            depth = depth.checked_sub(1)?;
        } else if depth == 0 && keyword(t) == Some(Keyword::Values) {
            values = Some(i);
            break;
        } else if is(t, Punctuation::Semicolon) {
            return None;
        }
    }
    // The first row: to its closing parenthesis.
    let open = values? + 1;
    if !is(tokens.get(open)?, Punctuation::LeftParenthesis) {
        return None;
    }
    let mut depth = 0usize;
    let mut close = None;
    for (i, t) in tokens.iter().enumerate().skip(open) {
        if is(t, Punctuation::LeftParenthesis) {
            depth += 1;
        } else if is(t, Punctuation::RightParenthesis) {
            depth -= 1;
            if depth == 0 {
                close = Some(i);
                break;
            }
        }
    }
    let close = close?;

    // The rows after it, each `, ( literal {, literal} )`, then only
    // semicolons.
    let mut rows = vec![];
    let mut at = close + 1;
    while at < tokens.len() && is(&tokens[at], Punctuation::Comma) {
        let comma = Comma {
            span: tokens[at].span,
        };
        let (row, next) = row(tokens, at + 1)?;
        rows.push((comma, row));
        at = next;
    }
    if rows.len() < MIN_ROWS || !tokens[at..].iter().all(|t| is(t, Punctuation::Semicolon)) {
        return None;
    }

    // The head and first row, by the grammar; the rest appended.
    let mut statements = parse_tokens::<EmptyErr>(&tokens[..=close])
        .into_result()
        .ok()?;
    let [Statement::Insert(insert)] = &mut statements[..] else {
        return None;
    };
    let InsertSource::Values(_, seq) = &mut insert.source else {
        return None;
    };
    if !seq.tail.is_empty() {
        return None;
    }
    seq.tail = rows;
    Some(statements)
}

// One row starting at `tokens[at]`, and the index after its `)`.
fn row<'src>(tokens: &'src [TokenStruct<'src>], at: usize) -> Option<(ValuesRow, usize)> {
    let open = tokens.get(at)?;
    if !is(open, Punctuation::LeftParenthesis) {
        return None;
    }
    let (head, mut at) = literal(tokens, at + 1)?;
    let mut tail = vec![];
    loop {
        let t = tokens.get(at)?;
        if is(t, Punctuation::RightParenthesis) {
            let row = ValuesRow(
                LeftParenthesis { span: open.span },
                Seq {
                    head: Box::new(head),
                    tail,
                },
                RightParenthesis { span: t.span },
            );
            return Some((row, at + 1));
        }
        if !is(t, Punctuation::Comma) {
            return None;
        }
        let (value, next) = literal(tokens, at + 1)?;
        tail.push((Comma { span: t.span }, value));
        at = next;
    }
}

// One literal value starting at `tokens[at]`, as the expression grammar
// builds it, and the index after it.
fn literal<'src>(tokens: &'src [TokenStruct<'src>], at: usize) -> Option<(Expr, usize)> {
    let t = tokens.get(at)?;
    let literal = match &t.token {
        Token::Number { raw } => Literal::Number(number(t, raw)?),
        Token::String {
            raw,
            kind: StringStyle::SingleQuoted(_),
        } => Literal::String(StringLiteral {
            span: t.span,
            value: raw.replace("''", "'"),
        }),
        // `-5`: the sign is a unary operator over the number.
        Token::Punctuation(Punctuation::Minus) => {
            let n = tokens.get(at + 1)?;
            let Token::Number { raw } = &n.token else {
                return None;
            };
            let expr = Expr::Unary {
                op: UnaryOp::Minus,
                expr: Box::new(Expr::Literal(Literal::Number(number(n, raw)?))),
            };
            return Some((expr, at + 2));
        }
        Token::Word {
            keyword: Some(Keyword::Null),
            ..
        } => Literal::Null(kw::Null { span: t.span }),
        Token::Word {
            keyword: Some(Keyword::True),
            ..
        } => Literal::Boolean(BooleanLiteral(Either::Left(kw::True { span: t.span }))),
        Token::Word {
            keyword: Some(Keyword::False),
            ..
        } => Literal::Boolean(BooleanLiteral(Either::Right(kw::False { span: t.span }))),
        _ => return None,
    };
    Some((Expr::Literal(literal), at + 1))
}

// As NumberLiteral's own parser reads one.
fn number(t: &TokenStruct, raw: &str) -> Option<NumberLiteral> {
    let value = match raw.parse::<i64>() {
        Ok(i) => NumberValue::Integer(i),
        Err(_) => NumberValue::Float(raw.parse::<f64>().ok()?),
    };
    Some(NumberLiteral {
        span: t.span,
        raw: raw.to_string(),
        value,
    })
}

#[cfg(test)]
mod tests {
    use chumsky::error::EmptyErr;

    use super::fast_insert;
    use crate::{lexer, parse_tokens};

    fn rows(n: usize, row: impl Fn(usize) -> String) -> String {
        (0..n).map(row).collect::<Vec<_>>().join(", ")
    }

    // What it builds is what the grammar builds.
    #[test]
    fn test_the_fast_path_gives_the_grammars_statements() {
        let cases = [
            format!("insert into t values {}", rows(20, |i| format!("({i}, 'r{i}')"))),
            format!(
                "insert into s.t (a, b, c, d, e) values {};",
                rows(40, |i| format!("({i}, -{i}, {i}.5, 'it''s {i}', null)"))
            ),
            format!(
                "INSERT INTO t (a,b) VALUES {} ;;",
                rows(12, |i| format!("( true , {} )", if i % 2 == 0 { "false" } else { "NULL" }))
            ),
            format!(
                "insert into t values {}",
                rows(10, |i| format!("(-.5, 99999999999999999999, 0.25, '', {i})"))
            ),
        ];
        for sql in &cases {
            let tokens = lexer::tokenize(sql).unwrap();
            let fast = fast_insert(&tokens).unwrap_or_else(|| panic!("no fast path: {sql}"));
            let grammar = parse_tokens::<EmptyErr>(&tokens).into_result().unwrap();
            assert_eq!(fast, grammar, "{sql}");
        }
    }

    // Anything it doesn't know is the grammar's — or nobody's.
    #[test]
    fn test_the_fast_path_declines_what_it_does_not_cover() {
        let ten = rows(10, |i| format!("({i}, 1)"));
        for sql in [
            // An expression in a row.
            format!("insert into t values {ten}, (1 + 1, 2)"),
            format!("insert into t values {ten}, (upper('a'), 2)"),
            format!("insert into t values {ten}, ((1), 2)"),
            format!("insert into t values {ten}, (\"a\", 2)"),
            format!("insert into t values {ten}, (?, 2)"),
            // Too few rows to bother.
            "insert into t values (1, 2), (3, 4)".to_string(),
            // Not one INSERT ... VALUES.
            format!("insert into t values {ten}; select 1"),
            "insert into t select * from u".to_string(),
            format!("select 1; insert into t values {ten}"),
            // Not SQL: left for the grammar to refuse, with its message.
            format!("insert into t values {ten}, (1, 2"),
            format!("insert into t values {ten}, (1,, 2)"),
            format!("insert into t values {ten}, ()"),
            format!("insert into t values {ten} (1, 2)"),
            format!("insert into t values {ten},"),
            format!("insert t values {ten}"),
        ] {
            let tokens = match lexer::tokenize(&sql) {
                Ok(t) => t,
                Err(_) => continue,
            };
            assert!(fast_insert(&tokens).is_none(), "{sql}");
        }
        // And the whole of parse_sql agrees with the grammar on each.
        assert!(crate::parse_sql(&format!("insert into t values {ten}, (1, 2")).is_err());
        assert_eq!(
            crate::parse_sql(&format!("insert into t values {ten}, (1 + 1, 2)"))
                .unwrap()
                .len(),
            1
        );
    }
}
