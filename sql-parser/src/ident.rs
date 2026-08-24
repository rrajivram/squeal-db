//! Identifiers: bare words that are not reserved keywords, or double-quoted
//! strings (the SQL-standard way to use a reserved word or odd characters as
//! a name).

use chumsky::{Parser, extra::ParserExtra, label::LabelError};

use crate::{
    parser::{SQLParser, TokenInput, token},
    span::TokenSpan,
    token::{StringStyle, Token, Period},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    pub span: TokenSpan,
    pub value: String,
    /// True when written as a `"quoted identifier"` — such names bypass the
    /// keyword check and are case-sensitive to consumers that care.
    pub quoted: bool,
}

/// A possibly-qualified name: `t`, `schema.t`, `db.schema.t`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName {
    pub parts: Seq<Ident, Period>,
}

impl ObjectName {
    pub fn span(&self) -> TokenSpan {
        TokenSpan {
            start: self.parts.head.span.start,
            end: self.parts.last().span.end,
        }
    }

    pub fn idents(&self) -> impl Iterator<Item = &Ident> {
        self.parts.items()
    }

    /// The dotted textual form, e.g. `schema.table`.
    pub fn to_dotted(&self) -> String {
        self.idents()
            .map(|i| i.value.as_str())
            .collect::<Vec<_>>()
            .join(".")
    }
}

impl<'src, I, E> SQLParser<'src, I, E> for Ident
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        token("identifier", |t| match &t.token {
            // A bare word is an identifier unless it's a *reserved* keyword;
            // non-reserved keywords (NAME, DATA, YEAR, ...) are fine names.
            Token::Word { raw, keyword } if !keyword.is_some_and(|k| k.reserved()) => {
                Some(Ident {
                    span: t.span,
                    value: (*raw).to_string(),
                    quoted: false,
                })
            }
            Token::String {
                raw,
                kind: StringStyle::DoubleQuoted(_),
            } => Some(Ident {
                span: t.span,
                value: raw.replace("\"\"", "\""),
                quoted: true,
            }),
            _ => None,
        })
    }
}

impl<'src, I, E> SQLParser<'src, I, E> for ObjectName
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        Seq::<Ident, Period>::parser(()).map(|parts| ObjectName { parts })
    }
}
