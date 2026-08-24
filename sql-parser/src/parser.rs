//! The [`SQLParser`] trait: every AST node knows how to build a chumsky
//! parser for itself over a stream of [`TokenStruct`]s.
//!
//! Composite shapes get blanket impls here — `Option<T>` is "maybe T",
//! `Vec<T>` is "zero or more T", `Either<L, R>` is "L, else R", tuples are
//! sequences. Because of these, the `#[derive(SQLParser)]` macro (and
//! hand-written parsers) only ever compose `T::parser(())` calls; no
//! per-shape logic is needed anywhere else.

use chumsky::{
    IterParser, Parser,
    extra::ParserExtra,
    input::{ExactSizeInput, Input, ValueInput},
    label::LabelError,
    prelude::any,
    recursive::{Indirect, Recursive},
    util::MaybeRef,
};
use either::Either;

use crate::{
    span::TokenSpan,
    token::{Operator, Punctuation, Token, TokenStruct},
};

/// Bound alias for "a chumsky input that yields our tokens by value".
/// Supertraits make `I: TokenInput<'src>` imply all of them.
pub trait TokenInput<'src>:
    Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>
{
}

impl<'src, I> TokenInput<'src> for I where
    I: Input<'src, Token = TokenStruct<'src>> + ValueInput<'src> + ExactSizeInput<'src>
{
}

/// Match a single token: `f` returns `Some(value)` to accept it, `None` to
/// reject with an "expected `label`" error. The building block for all
/// hand-written token-level parsers.
pub fn token<'src, I, E, T, F>(
    label: impl Into<String>,
    f: F,
) -> impl Parser<'src, I, T, E> + Clone
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
    F: Fn(&TokenStruct<'src>) -> Option<T> + Clone,
{
    let label = label.into();
    any().try_map(move |t: TokenStruct<'src>, span| match f(&t) {
        Some(v) => Ok(v),
        None => Err(LabelError::expected_found(
            [label.clone()],
            Some(MaybeRef::Val(t)),
            span,
        )),
    })
}

/// Match one punctuation mark, yielding its source span.
pub fn punct<'src, I, E>(p: Punctuation) -> impl Parser<'src, I, TokenSpan, E> + Clone
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    token(String::from(p.to_char()), move |t| match t.token {
        Token::Punctuation(q) if q == p => Some(t.span),
        _ => None,
    })
}

/// Match one multi-character operator, yielding its source span.
pub fn oper<'src, I, E>(op: Operator) -> impl Parser<'src, I, TokenSpan, E> + Clone
where
    I: TokenInput<'src>,
    E: ParserExtra<'src, I>,
    E::Error: LabelError<'src, I, String>,
{
    token(op.as_str(), move |t| match t.token {
        Token::Operator(o) if o == op => Some(t.span),
        _ => None,
    })
}

pub trait SQLParser<'a, I, E, A = ()>: Sized
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone;
}

/// The shared recursion context threaded through every derived parser as its
/// args. It holds the `Recursive` handles for the two mutually recursive
/// grammar roots — expressions and queries — so that `Expr -> Query -> Expr`
/// cycles (subqueries) reference one shared definition instead of recursing
/// at parser-construction time.
///
/// [`SqlCtx::build`] declares both handles, then defines each body with the
/// context (so the bodies see the handles), and returns the tied knot.
pub struct SqlCtx<'src, I, E>
where
    I: Input<'src>,
    E: ParserExtra<'src, I>,
{
    pub expr: Recursive<Indirect<'src, 'src, I, crate::expr::Expr, E>>,
    pub query: Recursive<Indirect<'src, 'src, I, crate::query::Query, E>>,
}

// Derived Clone would demand I: Clone + E: Clone; the handles themselves are
// unconditionally cheap to clone (shared Rc).
impl<'src, I, E> Clone for SqlCtx<'src, I, E>
where
    I: Input<'src>,
    E: ParserExtra<'src, I>,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            query: self.query.clone(),
        }
    }
}

impl<'src, I, E> SqlCtx<'src, I, E>
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    pub fn build() -> Self {
        let mut expr = Recursive::declare();
        let mut query = Recursive::declare();
        let ctx = Self {
            expr: expr.clone(),
            query: query.clone(),
        };
        expr.define(crate::expr::expr_body(ctx.clone()));
        query.define(crate::query::Query::body_parser(ctx.clone()));
        ctx
    }
}

impl<'a, I, E, A, T> SQLParser<'a, I, E, A> for Option<T>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
    T: SQLParser<'a, I, E, A>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args).or_not()
    }
}

impl<'a, I, E, A, T> SQLParser<'a, I, E, A> for Vec<T>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
    T: SQLParser<'a, I, E, A>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args).repeated().collect()
    }
}

impl<'a, I, E, A, T> SQLParser<'a, I, E, A> for Box<T>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
    T: SQLParser<'a, I, E, A>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        T::parser(args).map(Box::new)
    }
}

impl<'a, I, E, A, L, R> SQLParser<'a, I, E, A> for Either<L, R>
where
    I: Input<'a>,
    E: ParserExtra<'a, I>,
    E::Error: LabelError<'a, I, String>,
    A: Clone,
    L: SQLParser<'a, I, E, A>,
    R: SQLParser<'a, I, E, A>,
{
    fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
        L::parser(args.clone())
            .map(Either::Left)
            .or(R::parser(args).map(Either::Right))
    }
}

macro_rules! tuple_impl {
    ($($t:ident.$idx:tt),+ => |$flat:pat_param| $build:expr) => {
        impl<'a, I, E, A, $($t),+> SQLParser<'a, I, E, A> for ($($t,)+)
        where
            I: Input<'a>,
            E: ParserExtra<'a, I>,
            E::Error: LabelError<'a, I, String>,
            A: Clone,
            $($t: SQLParser<'a, I, E, A>,)+
        {
            fn parser(args: A) -> impl Parser<'a, I, Self, E> + Clone {
                tuple_impl!(@chain args, $($t),+).map(|$flat| $build)
            }
        }
    };
    (@chain $args:ident, $head:ident $(, $rest:ident)+) => {
        $head::parser($args.clone()) $( .then($rest::parser($args.clone())) )+
    };
}

tuple_impl!(T1.0, T2.1 => |(a, b)| (a, b));
tuple_impl!(T1.0, T2.1, T3.2 => |((a, b), c)| (a, b, c));
tuple_impl!(T1.0, T2.1, T3.2, T4.3 => |(((a, b), c), d)| (a, b, c, d));
tuple_impl!(T1.0, T2.1, T3.2, T4.3, T5.4 => |((((a, b), c), d), e)| (a, b, c, d, e));
tuple_impl!(T1.0, T2.1, T3.2, T4.3, T5.4, T6.5 => |(((((a, b), c), d), e), f)| (a, b, c, d, e, f));
