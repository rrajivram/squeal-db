use chumsky::{
    extra::ParserExtra,
    input::{ExactSizeInput, Input, ValueInput},
    label::LabelError,
};

use crate::{combo::sequence, parser::SQLParser, token::TokenStruct};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Seq<T, S> {
    pub head: Box<T>,
    pub tail: Vec<(S, T)>,
}

impl<'src, I, E, T, S> SQLParser<'src, I, E> for Seq<T, S>
where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>,
    T: SQLParser<'src, I, E>,
    S: SQLParser<'src, I, E>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(args: ()) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
        sequence(T::parser(args), S::parser(args))
    }
}
