use chumsky::{
    extra::ParserExtra,
    label::LabelError,
};

use crate::{combo::sequence, parser::{SQLParser, TokenInput}};

/// A non-empty, separator-interleaved sequence (`a, b, c` / `a.b.c`) that
/// keeps the separator tokens (and their spans) it was parsed with.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Seq<T, S> {
    pub head: Box<T>,
    pub tail: Vec<(S, T)>,
}

impl<T, S> Seq<T, S> {
    pub fn items(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&*self.head).chain(self.tail.iter().map(|(_, t)| t))
    }

    pub fn len(&self) -> usize {
        1 + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn last(&self) -> &T {
        self.tail.last().map(|(_, t)| t).unwrap_or(&self.head)
    }
}

impl<'src, I, E, T, S, A> SQLParser<'src, I, E, A> for Seq<T, S>
where
    I: TokenInput<'src>,
    T: SQLParser<'src, I, E, A>,
    S: SQLParser<'src, I, E, A>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
    A: Clone,
{
    fn parser(args: A) -> impl chumsky::Parser<'src, I, Self, E> + Clone {
        sequence(T::parser(args.clone()), S::parser(args))
    }
}
