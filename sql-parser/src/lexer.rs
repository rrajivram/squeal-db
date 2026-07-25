use std::{collections::HashMap, ops::RangeFrom};

use chumsky::{
    IterParser, Parser,
    error::Rich,
    extra::{self, SimpleState},
    input::{Input, MapExtra, SliceInput, StrInput, ValueInput},
    prelude::{any, choice, custom, end, just},
    span::SimpleSpan,
    text,
};

use crate::{
    keyword::Keyword,
    span::TokenSpan,
    token::{Punctuation, StringStyle, Token, TokenStruct},
};

type LexExtra<'src, I> = extra::Full<Rich<'src, char>, SimpleState<HashMap<I, u64>>, ()>;

pub fn word<'src, I>() -> impl Parser<'src, I, TokenStruct<'src>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>,
{
    any()
        .filter(|c: &char| c.is_alphanumeric())
        .repeated()
        .at_least(1)
        .map_with(|(), e| {
            let keyword = Keyword::get(e.slice());
            TokenStruct {
                token: Token::Word {
                    raw: e.slice(),
                    keyword,
                },
                span: TokenSpan::from(e.span()),
            }
        })
}

pub fn string<'src, I>() -> impl Parser<'src, I, TokenStruct<'src>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>
        + StrInput<'src>,
{
    let ident = text::ident().padded();
    // text::ident() stops at the first whitespace, so quoted content (which
    // should allow interior spaces) can't be built on top of it — instead,
    // take any character up to the closing quote as-is and recover the
    // matched &str via to_slice() (the same "grab the raw span" idiom word()
    // uses via e.slice(), just expressed as a combinator here since the
    // quotes need to be excluded from what's captured).
    let quoted = |quote: char| {
        any()
            .filter(move |c: &char| *c != quote)
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
                kind: StringStyle::DoubleQuoted(Some('\"')),
            },
            span: TokenSpan::from(e.span()),
        }),
        ident.map_with(|s: &str, e| TokenStruct {
            token: Token::Word {
                raw: s,
                keyword: Keyword::get(s),
            },
            span: TokenSpan::from(e.span()),
        }),
    ))
}

#[allow(clippy::collapsible_match)]
pub fn number<'src, I>() -> impl Parser<'src, I, TokenStruct<'src>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>,
{
    custom(|input| {
        let mut neg: bool = false;
        let begin = input.cursor();
        let mut part1 = String::new();
        let mut period = false;
        let mut part2 = String::new();
        let mut is_err = false;
        let mut err_msg = String::new();
        while let Some(c) = input.next() {
            match c {
                _ if c.is_ascii_digit() => {
                    if period {
                        part2.push(c);
                    } else {
                        part1.push(c);
                    }
                }
                '.' => {
                    if period {
                        is_err = true;
                        err_msg = "Unexpected '.' ".into();
                        break;
                    } else {
                        period = true;
                    }
                }
                '-' => {
                    if !neg && !period && (part1.is_empty() && part2.is_empty()) {
                        neg = true;
                    } else {
                        is_err = true;
                        err_msg = "Unexpected '-'".into();
                        break;
                    }
                }
                ',' => {
                    //ignore if before period
                    if period {
                        is_err = true;
                        err_msg = "Ubexpected ',' ".into();
                    }
                }
                _ => {
                    break;
                }
            }
        }

        if !is_err && !neg && !period && part1.is_empty() && part2.is_empty() {
            is_err = true;
            err_msg = "expected a number".into();
        }

        if is_err {
            Err(Rich::custom(
                input.span_from(RangeFrom {
                    start: &input.cursor(),
                }),
                err_msg,
            ))
        } else {
            Ok(TokenStruct {
                token: Token::Decimal {
                    is_neg: neg,
                    part1: part1.parse::<u64>().unwrap_or(0),
                    period,
                    part2: part2.parse().unwrap_or(0),
                },
                span: TokenSpan::from(input.span_from(RangeFrom { start: &begin })),
            })
        }
    })
}

pub fn whitespace<'src, I>(c: char) -> impl Parser<'src, I, TokenStruct<'src>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>,
{
    just(c).repeated().at_least(1).map_with(|_, e| TokenStruct {
        token: Token::Space,
        span: TokenSpan::from(e.span()),
    })
}

pub fn punctuation<'src, I>() -> impl Parser<'src, I, TokenStruct<'src>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>,
{
    any().try_map_with(|c: char, e| match Punctuation::from_char(c) {
        Some(p) => Ok(TokenStruct {
            token: Token::Punctuation(p),
            span: TokenSpan::from(e.span()),
        }),
        None => Err(Rich::custom(e.span(), format!("'{c}' is not punctuation"))),
    })
}

pub fn lexer<'src, I>() -> impl Parser<'src, I, Vec<TokenStruct<'src>>, LexExtra<'src, I>>
where
    I: Input<'src, Token = char, Span = SimpleSpan>
        + ValueInput<'src>
        + SliceInput<'src, Slice = &'src str>
        + StrInput<'src>,
{
    choice((
        number(),
        string(),
        whitespace(' '),
        whitespace('\r'),
        whitespace('\n'),
        whitespace('\t'),
        punctuation(),
    ))
    .repeated()
    .collect()
    .then_ignore(end())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_lexer1() {
        let l = lexer()
            //.lazy()
            .parse("create table table1 (id int, name varchar(128)) 128");
        println!("{:?}", l);
    }

    #[test]
    fn test_number() {
        let n = number().parse("-123423.2234234");
        println!("{:?}", n);
    }

    #[test]
    fn test_string() {
        let n = string().parse("\'abcd  ef\'");
        println!("{:?}", n);
        let n = string().parse("\"abcdef\"");
        println!("{:?}", n);
        let n = string().parse("\'abcdef");
        println!("{:?}", n);
        let n = string().parse("abcdef\"");
        println!("{:?}", n);
        let n = string().parse("create");
        println!("{:?}", n);
    }
}
