use chumsky::{
    IterParser, Parser,
    extra::ParserExtra,
    label::LabelError,
};

use crate::{parser::TokenInput, utils::Seq};

pub fn sequence<'src, I, E, T, S, PT, PS>(
    item: PT,
    sep: PS,
) -> impl Parser<'src, I, Seq<T, S>, E> + Clone
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
    PT: Parser<'src, I, T, E> + Clone,
    PS: Parser<'src, I, S, E> + Clone,
{
    item.clone()
        .then(sep.then(item).repeated().collect::<Vec<(_, _)>>())
        .map(|(head, tail)| Seq {
            head: Box::new(head),
            tail,
        })
}
