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
///
/// By hand (`lex_fast`), which is several times faster than the combinators
/// above and gives exactly their tokens (a test checks it against them);
/// when the text doesn't lex, the combinators say why.
pub fn tokenize(src: &str) -> Result<Vec<TokenStruct<'_>>, Vec<Rich<'_, char>>> {
    match lex_fast(src) {
        Some(tokens) => Ok(tokens),
        None => tokenize_combinators(src),
    }
}

// The combinator lexer's tokens, minus whitespace and comments.
fn tokenize_combinators(src: &str) -> Result<Vec<TokenStruct<'_>>, Vec<Rich<'_, char>>> {
    lexer().parse(src).into_result().map(|tokens| {
        tokens
            .into_iter()
            .filter(|t| t.token != Token::Space)
            .collect()
    })
}

// `lexer()` by hand: the same alternatives tried in the same order, each
// as the combinators define it, so it gives the same tokens. Whitespace and
// comments are dropped as they are lexed. None when the text doesn't lex.
fn lex_fast(src: &str) -> Option<Vec<TokenStruct<'_>>> {
    const OPERATORS: [(&str, Operator); 6] = [
        ("<>", Operator::NotEq),
        ("!=", Operator::NotEqBang),
        ("<=", Operator::LtEq),
        (">=", Operator::GtEq),
        ("||", Operator::Concat),
        ("::", Operator::DoubleColon),
    ];
    let bytes = src.as_bytes();
    let mut tokens = Vec::with_capacity(src.len() / 3);
    let mut at = 0;
    let span = |start: usize, end: usize| TokenSpan { start, end };
    let digits_from = |mut i: usize| {
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        i
    };
    while at < src.len() {
        let rest = &src[at..];
        let c = rest.chars().next()?;
        // Comments: `-- ...` to the end of the line; `/* ... */` only when
        // closed (else `/` is punctuation, as in the combinators).
        if rest.starts_with("--") {
            at += rest.find('\n').unwrap_or(rest.len());
            continue;
        }
        if rest.starts_with("/*")
            && let Some(close) = rest[2..].find("*/")
        {
            at += 2 + close + 2;
            continue;
        }
        if let Some((text, op)) = OPERATORS.iter().find(|(t, _)| rest.starts_with(t)) {
            tokens.push(TokenStruct {
                token: Token::Operator(*op),
                span: span(at, at + text.len()),
            });
            at += text.len();
            continue;
        }
        // Numbers: `123`, `1.5`, `1.`, `.5`.
        let number_end = if c.is_ascii_digit() {
            let mut end = digits_from(at);
            if bytes.get(end) == Some(&b'.') {
                end = digits_from(end + 1);
            }
            Some(end)
        } else if c == '.' && bytes.get(at + 1).is_some_and(|b| b.is_ascii_digit()) {
            Some(digits_from(at + 1))
        } else {
            None
        };
        if let Some(end) = number_end {
            tokens.push(TokenStruct {
                token: Token::Number { raw: &src[at..end] },
                span: span(at, end),
            });
            at = end;
            continue;
        }
        if unicode_ident::is_xid_start(c) || c == '_' {
            let len = rest
                .char_indices()
                .find(|(i, ch)| *i > 0 && !unicode_ident::is_xid_continue(*ch))
                .map_or(rest.len(), |(i, _)| i);
            let raw = &rest[..len];
            tokens.push(TokenStruct {
                token: Token::Word {
                    raw,
                    keyword: Keyword::get(raw),
                },
                span: span(at, at + len),
            });
            at += len;
            continue;
        }
        // Strings: '...' and "...", a doubled quote standing for one.
        if c == '\'' || c == '"' {
            let quote = c as u8;
            let mut i = at + 1;
            loop {
                match bytes.get(i) {
                    // Unterminated: nothing else lexes a quote.
                    None => return None,
                    Some(b) if *b == quote && bytes.get(i + 1) == Some(&quote) => i += 2,
                    Some(b) if *b == quote => break,
                    Some(_) => i += 1,
                }
            }
            tokens.push(TokenStruct {
                token: Token::String {
                    raw: &src[at + 1..i],
                    kind: if quote == b'\'' {
                        StringStyle::SingleQuoted(Some('\''))
                    } else {
                        StringStyle::DoubleQuoted(Some('"'))
                    },
                },
                span: span(at, i + 1),
            });
            at = i + 1;
            continue;
        }
        // `@path`: at least one character, up to whitespace or `;`.
        if c == '@' {
            let len = rest[1..]
                .char_indices()
                .find(|(_, ch)| Token::is_whitespace(*ch) || *ch == ';')
                .map_or(rest.len() - 1, |(i, _)| i);
            if len > 0 {
                tokens.push(TokenStruct {
                    token: Token::String {
                        raw: &rest[1..1 + len],
                        kind: StringStyle::Unquoted,
                    },
                    span: span(at, at + 1 + len),
                });
                at += 1 + len;
                continue;
            }
        }
        if Token::is_whitespace(c) {
            at += c.len_utf8();
            continue;
        }
        let p = Punctuation::from_char(c)?;
        tokens.push(TokenStruct {
            token: Token::Punctuation(p),
            span: span(at, at + c.len_utf8()),
        });
        at += c.len_utf8();
    }
    Some(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The hand lexer against the combinators, on random text made of the
    // characters every rule turns on: the same tokens, or both failing.
    #[test]
    fn test_the_hand_lexer_gives_the_combinators_tokens() {
        let pieces = [
            "a",
            "_",
            "Z9",
            "é",
            "ß",
            "1",
            "23",
            ".",
            "..",
            "'",
            "''",
            "\"",
            " ",
            "\n",
            "\t",
            "\r",
            "-",
            "--",
            "/",
            "*",
            "/*",
            "*/",
            "@",
            "@x/y.csv",
            ";",
            "<",
            ">",
            "=",
            "!",
            "|",
            ":",
            "(",
            ")",
            ",",
            "select",
            "FROM",
            "where",
            "?",
            "$",
            "#",
            "[",
            "\\",
            "~",
            "€",
            "0.5",
            "1.",
            ".7",
            "x1",
            "\u{1F600}",
            "%",
            "&",
            "^",
            "{",
            "}",
        ];
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200_000 {
            let len = (next() % 12) as usize;
            let text: String = (0..len)
                .map(|_| pieces[(next() % pieces.len() as u64) as usize])
                .collect();
            let fast = lex_fast(&text);
            let slow = tokenize_combinators(&text).ok();
            assert_eq!(fast, slow, "{text:?}");
        }
        for text in [
            "select * from t where a <> 'it''s' -- c\n and b::int >= .5 || \"q\"\"x\"",
            "copy into t from @/tmp/a-b_c.csv;",
            "/* unclosed",
            "x /* a */ y",
            "@",
            "@ x",
            "'unterminated",
        ] {
            assert_eq!(lex_fast(text), tokenize_combinators(text).ok(), "{text:?}");
        }
    }

    #[test]
    fn test_lexer_create_table() {
        let toks = tokenize("create table table_name (id int, name varchar(128)) 128").unwrap();
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
        assert!(
            toks.iter()
                .any(|t| t.token == Token::Operator(Operator::LtEq))
        );
        assert!(
            toks.iter()
                .any(|t| t.token == Token::Operator(Operator::NotEq))
        );
        assert_eq!(toks.len(), 6);
    }
}
