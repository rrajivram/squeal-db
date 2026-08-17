use chumsky::{
    Parser,
    extra::ParserExtra,
    input::{ExactSizeInput, Input, ValueInput},
    label::LabelError,
    prelude::any,
};

use crate::{
    keyword::{Create, Keyword, Table},
    literal::StringLiteral,
    parser::SQLParser,
    statement::Statement,
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
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        // just(Token::Word { .. }) can't work here: `just` matches an exact
        // value against the input's own token type, which is TokenStruct
        // (not Token) per this impl's own `I::Token = TokenStruct<'src>`
        // bound — filtering on `.token` (as the string check below already
        // does) is the right shape, not a literal match.
        any()
            .filter(|t: &TokenStruct| {
                matches!(
                    t.token,
                    Token::Word {
                        keyword: Some(Keyword::Create),
                        ..
                    }
                )
            })
            .map(|t: TokenStruct| Create::new(t.span))
            .then(
                any()
                    .filter(|t: &TokenStruct| {
                        matches!(
                            t.token,
                            Token::Word {
                                keyword: Some(Keyword::Table),
                                ..
                            }
                        )
                    })
                    .map(|t: TokenStruct| Table::new(t.span))
                    .then(
                        any()
                            .filter(|t: &TokenStruct| {
                                matches!(t.token, Token::Word { keyword: None, .. })
                            })
                            .map(|t: TokenStruct| StringLiteral {
                                value: String::from(t.token),
                            }),
                    ),
            )
            // .then().then() nests, it doesn't flatten: this produces
            // (Create, (Table, StringLiteral)), not a flat 3-tuple.
            .map(|(c, (t, tn))| Statement::CreateTable {
                create: c,
                table: t,
                name: Some(tn),
                columns: None,
            })
        /*
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
        })*/
    }
}

#[cfg(test)]
mod tests {
    use chumsky::extra;

    use crate::lexer::lexer;

    use super::*;
    use chumsky::error::Rich;

    type TestExtra<'src> = extra::Err<Rich<'src, TokenStruct<'src>>>;

    #[test]
    fn test_simple() {
        let t = &lexer()
            .parse("Create table table_name")
            .into_result()
            .unwrap()[..];
        // let s = <Statement as SQLParser<_, TestExtra>>::parse(()).parse(t);
        let s = <Statement as SQLParser<_, TestExtra>>::parser(()).parse(t);
        println!("{:?}", s);
    }
}
