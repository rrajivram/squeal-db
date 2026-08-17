use chumsky::{
    extra::ParserExtra,
    input::{ExactSizeInput, Input, InputRef, ValueInput},
    label::LabelError,
    prelude::custom,
    util::MaybeRef,
};

use crate::{
    keyword::Keyword,
    parser::SQLParser,
    span::TokenSpan,
    token::{Period, Token, TokenStruct},
    utils::Seq,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    pub span: TokenSpan,
    pub val: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Variable {
    pub span: TokenSpan,
    pub val: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName {
    pub span: TokenSpan,
    pub val: Seq<Ident, Period>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    pub span: TokenSpan,
    pub val: String,
}

impl<'src, I, E> SQLParser<'src, I, E> for Ident
where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
        custom(move |inp| parse_ident(inp, |_| true))
    }
}

impl<'src, I, E> SQLParser<'src, I, E> for TableIdent
where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
        fn matcher(k: &Option<Keyword>) -> bool {
            k.is_none()
        }
        custom(move |inp| parse_ident(inp, matcher).map(TableIdent::from))
    }
}

/* impl<'src, I, E> SQLParser<'src, I, E> for ObjectName
where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
        fn matcher(k: &Option<Keyword>) -> bool {
            k.is_none()
        }
        todo!()
        //custom(move |inp| parse_ident(inp, matcher).map(ObjectName))
    }
} */

fn parse_ident<'src, I, E, F>(
    inp: &mut InputRef<'src, '_, I, E>,
    matcher: F,
) -> Result<Ident, E::Error>
where
    F: Fn(&Option<Keyword>) -> bool,
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    let before = inp.cursor();
    match inp.next() {
        Some(tok) => match &tok.token {
            Token::Word { keyword, .. } if matcher(keyword) => Ok(Ident::from(&tok)),
            _ => Err(LabelError::expected_found(
                [],
                Some(MaybeRef::Val(tok)),
                inp.span_since(&before),
            )),
        },
        None => Err(LabelError::expected_found(
            [],
            None,
            inp.span_since(&before),
        )),
    }
}

impl<'src> From<&TokenStruct<'src>> for Ident {
    fn from(value: &TokenStruct<'src>) -> Self {
        Self {
            span: value.span,
            val: value.token.clone().into(),
        }
    }
}

impl From<Ident> for TableIdent {
    fn from(value: Ident) -> Self {
        Self {
            span: value.span,
            val: value.val,
        }
    }
}
/*
impl From<Ident> for ObjectName {
    fn from(value: Ident) -> Self {
        Self {
            span: value.span,
            val: value.val,
        }
    }
}
 */
