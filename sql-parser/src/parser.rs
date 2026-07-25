use chumsky::{Parser, extra::ParserExtra, input::Input, label::LabelError};

pub trait SQLParser<'a, I, E, A = ()>: Sized
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E>;
}
