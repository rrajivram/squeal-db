//! The lexer: turns source text into a flat list of [`TokenStruct`]s.
//!
//! Keywords are recognized here (case-insensitively) so the token-level
//! parsers can match on `Keyword` variants instead of comparing strings.
//! Whitespace and comments are emitted as [`Token::Space`] and stripped by
//! [`tokenize`], which is what the statement parsers consume.

use chumsky::{
    IterParser, Parser,
    error::Rich,
    extra,
    prelude::{any, choice, end, just, one_of},
    text,
};

use crate::{
    keyword::Keyword,
    span::TokenSpan,
    token::{Operator, Punctuation, StringStyle, Token, TokenStruct},
};

type LexError<'src> = extra::Err<Rich<'src, char>>;

fn word<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    text::ident().map_with(|s: &str, e| TokenStruct {
        token: Token::Word {
            raw: s,
            keyword: Keyword::get(s),
        },
        span: TokenSpan::from(e.span()),
    })
}

fn number<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    let digits = any()
        .filter(|c: &char| c.is_ascii_digit())
        .repeated()
        .at_least(1);
    // `123`, `1.5`, `1.`, `.5` — no sign (unary minus belongs to the
    // expression parser, otherwise `1-2` would lex as two numbers).
    choice((
        digits
            .then(just('.').then(digits.or_not()).or_not())
            .ignored(),
        just('.').then(digits).ignored(),
    ))
    .to_slice()
    .map_with(|s: &str, e| TokenStruct {
        token: Token::Number { raw: s },
        span: TokenSpan::from(e.span()),
    })
}

fn string<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    // A doubled quote inside the string is the SQL escape for the quote
    // character itself ('it''s'), so it must be consumed before the closing
    // quote can end the literal. Unescaping happens at literal-parse time.
    let quoted = |quote: char| {
        choice((
            just([quote, quote]).ignored(),
            any().filter(move |c: &char| *c != quote).ignored(),
        ))
        .repeated()
        .to_slice()
        .delimited_by(just(quote), just(quote))
    };
    choice((
        quoted('\'').map_with(|s: &str, e| TokenStruct {
            token: Token::String {
                raw: s,
                kind: StringStyle::SingleQuoted(Some('\'')),
            },
            span: TokenSpan::from(e.span()),
        }),
        quoted('"').map_with(|s: &str, e| TokenStruct {
            token: Token::String {
                raw: s,
                kind: StringStyle::DoubleQuoted(Some('"')),
            },
            span: TokenSpan::from(e.span()),
        }),
    ))
}

// `@<path>` — a COPY INTO stage reference. Lexed as one token (not `@`
// punctuation followed by a run of Word/Slash/Period/Minus tokens) because a
// real filesystem path can contain characters (`-`, `_`, multiple `.`) that
// would otherwise fragment into ambiguous punctuation/word tokens with no
// reliable way to losslessly reassemble the original path from them —
// notably `-` lexes as Punctuation::Minus, indistinguishable at the token
// level from a subtraction operator. Reuses Token::String's Unquoted style
// (see StringStyle's own doc comment) rather than a bespoke token variant,
// since it's exactly that: a bare, unescaped run of text, just introduced by
// `@` instead of a quote character. The `@` itself is not part of the
// captured span's text (only used to trigger the rule) — sql-parser's own
// `StagePath` type re-adds it to `span` bookkeeping but stores the path
// with it already stripped, matching what a filesystem path actually needs.
fn stage_path<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    just('@')
        .ignore_then(
            any()
                .filter(|c: &char| !Token::is_whitespace(*c) && *c != ';')
                .repeated()
                .at_least(1)
                .to_slice(),
        )
        .map_with(|s: &str, e| TokenStruct {
            token: Token::String {
                raw: s,
                kind: StringStyle::Unquoted,
            },
            span: TokenSpan::from(e.span()),
        })
}

fn operator<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    choice((
        just("<>").to(Operator::NotEq),
        just("!=").to(Operator::NotEqBang),
        just("<=").to(Operator::LtEq),
        just(">=").to(Operator::GtEq),
        just("||").to(Operator::Concat),
        just("::").to(Operator::DoubleColon),
    ))
    .map_with(|op, e| TokenStruct {
        token: Token::Operator(op),
        span: TokenSpan::from(e.span()),
    })
}

fn punctuation<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    any().try_map(|c: char, span| match Punctuation::from_char(c) {
        Some(p) => Ok(TokenStruct {
            token: Token::Punctuation(p),
            span: TokenSpan::from(span),
        }),
        None => Err(Rich::custom(span, format!("'{c}' is not punctuation"))),
    })
}

fn whitespace<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    one_of(" \r\n\t")
        .repeated()
        .at_least(1)
        .map_with(|(), e| TokenStruct {
            token: Token::Space,
            span: TokenSpan::from(e.span()),
        })
}

fn comment<'src>() -> impl Parser<'src, &'src str, TokenStruct<'src>, LexError<'src>> + Clone {
    let line = just("--")
        .then(any().filter(|c: &char| *c != '\n').repeated())
        .ignored();
    let block = any()
        .and_is(just("*/").not())
        .repeated()
        .delimited_by(just("/*"), just("*/"))
        .ignored();
    line.or(block).map_with(|(), e| TokenStruct {
        token: Token::Space,
        span: TokenSpan::from(e.span()),
    })
}

pub fn lexer<'src>() -> impl Parser<'src, &'src str, Vec<TokenStruct<'src>>, LexError<'src>> {
    // Order matters: comments before punctuation (`--`, `/*`), operators
    // before punctuation (`<=` vs `<`), numbers before punctuation (`.5`).
    choice((
        comment(),
        operator(),
        number(),
        word(),
        string(),
        stage_path(),
        whitespace(),
        punctuation(),
    ))
    .repeated()
    .collect()
    .then_ignore(end())
}

/// Lex `src` into tokens with whitespace and comments removed — the input the
/// statement parsers expect.
pub fn tokenize(src: &str) -> Result<Vec<TokenStruct<'_>>, Vec<Rich<'_, char>>> {
    lexer()
        .parse(src)
        .into_result()
        .map(|tokens| {
            tokens
                .into_iter()
                .filter(|t| t.token != Token::Space)
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_create_table() {
        let toks =
            tokenize("create table table_name (id int, name varchar(128)) 128").unwrap();
        assert!(toks.iter().all(|t| t.token != Token::Space));
        assert!(matches!(
            toks[0].token,
            Token::Word {
                keyword: Some(Keyword::Create),
                ..
            }
        ));
        assert_eq!(toks.last().unwrap().token, Token::Number { raw: "128" });
    }

    #[test]
    fn test_number() {
        for (src, raw) in [("123", "123"), ("1.5", "1.5"), (".5", ".5"), ("1.", "1.")] {
            let toks = tokenize(src).unwrap();
            assert_eq!(toks.len(), 1, "{src}");
            assert_eq!(toks[0].token, Token::Number { raw }, "{src}");
        }
        // no sign in the lexer: `1-2` is number, minus, number
        let toks = tokenize("1-2").unwrap();
        assert_eq!(toks.len(), 3);
        assert_eq!(toks[1].token, Token::Punctuation(Punctuation::Minus));
    }

    #[test]
    fn test_string() {
        let toks = tokenize("'abcd  ef'").unwrap();
        assert_eq!(
            toks[0].token,
            Token::String {
                raw: "abcd  ef",
                kind: StringStyle::SingleQuoted(Some('\''))
            }
        );
        let toks = tokenize("'it''s'").unwrap();
        assert_eq!(toks.len(), 1);
        assert_eq!(
            toks[0].token,
            Token::String {
                raw: "it''s",
                kind: StringStyle::SingleQuoted(Some('\''))
            }
        );
        assert!(tokenize("'unterminated").is_err());
    }

    #[test]
    fn test_operators_and_comments() {
        let toks = tokenize("a <= b -- trailing\n/* block */ c <> 1").unwrap();
        assert!(toks
            .iter()
            .any(|t| t.token == Token::Operator(Operator::LtEq)));
        assert!(toks
            .iter()
            .any(|t| t.token == Token::Operator(Operator::NotEq)));
        assert_eq!(toks.len(), 6);
    }
}
