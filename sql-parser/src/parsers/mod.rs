use std::ops::RangeFrom;

use chumsky::{
    Parser,
    error::Rich,
    extra::ParserExtra,
    input::{ExactSizeInput, Input, InputRef, ValueInput},
    label::LabelError,
    prelude::{any, custom, just},
};

use crate::{
    keyword::{Create, Keyword, Table},
    parser::SQLParser,
    statement::{ColumnDefList, Statement},
    token::{Token, TokenStruct},
};

pub(crate) struct ColumnDefParser;
/*
impl<'src, I, E> SQLParser<'src, I, E> for ColumnDefList
where
    I: Input<'src>,
    E: ParserExtra<'src, I>,
{
    fn parse(_args: ()) -> impl Parser<'src, I, Self, E> {
        todo!()
    }
}
 */
impl<'src, I, E> SQLParser<'src, I, E> for Statement
where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parse(_args: ()) -> impl Parser<'src, I, Self, E> {
        /*
        just(Token::Word {
            raw: "",
            keyword: Some(Keyword::Create),
        })
        .then(
            just(Token::Word {
                raw: "",
                keyword: Some(Keyword::Table),
            })
            .then(just(Token::String { raw: name, kind: _ })),
        )
        */
        custom(move |stmt: &mut chumsky::input::InputRef<'src, '_, I, E>| {
            let begin = stmt.cursor();
            if let Some(c) = stmt.next()
                && matches!(
                    c.token,
                    Token::Word {
                        raw: _,
                        keyword: Some(Keyword::Create)
                    }
                )
                && let Some(t) = stmt.next()
                && matches!(
                    t.token,
                    Token::Word {
                        raw: _,
                        keyword: Some(Keyword::Table)
                    }
                )
            {
                return Ok(Statement::CreateTable {
                    create: Create::new(c.span),
                    table: Table::new(t.span),
                    name: None,
                    columns: None,
                });
            }
            Err(E::Error::expected_found(
                vec!["Create".into()],
                None,
                stmt.span_from(RangeFrom { start: &begin }),
            ))
        })
    }
}

fn parse_columndef<'src, I, E>(
    input: &mut InputRef<'src, '_, I, E>,
) -> Result<ColumnDefList, E::Error>
where
    I: Input<'src>,
    E: ParserExtra<'src, I>,
{
    let begin = input.cursor();

    todo!()
}

#[cfg(test)]
mod tests {
    use chumsky::extra;

    use crate::lexer::lexer;

    use super::*;

    type TestExtra<'src> = extra::Err<Rich<'src, TokenStruct<'src>>>;

    #[test]
    fn test_simple() {
        let t = &lexer().parse("Create table ").into_result().unwrap()[..];
        // let s = <Statement as SQLParser<_, TestExtra>>::parse(()).parse(t);
        let s = <Statement as SQLParser<_, TestExtra>>::parse(()).parse(t);
        println!("{:?}", s);
    }
}
