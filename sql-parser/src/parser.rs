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
    recursive::{Direct, Recursive, recursive},
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
/// args. It holds the `Recursive` handles for the mutually recursive grammar
/// roots — expressions, queries and statements — so that `Expr -> Query ->
/// Expr` cycles (subqueries) reference one shared definition instead of
/// recursing at parser-construction time.
///
/// [`SqlCtx::build`] ties the knot. A handle given to a grammar body must not
/// own what it points at: a body owning its own root is a reference cycle,
/// and every grammar built would leak (13.7 KB a parse, with chumsky's
/// `declare`/`define`). So the roots are nested `recursive()` calls —
/// statements own queries, queries own expressions — and a body's handles
/// to itself and to the roots enclosing it are chumsky's weak ones.
pub struct SqlCtx<'src, I, E>
where
    I: Input<'src>,
    E: ParserExtra<'src, I>,
{
    pub expr: Recursive<Direct<'src, 'src, I, crate::expr::Expr, E>>,
    pub query: Recursive<Direct<'src, 'src, I, crate::query::Query, E>>,
    /// Statements recurse too: `EXPLAIN <statement>`.
    pub stmt: Recursive<Direct<'src, 'src, I, crate::statement::Statement, E>>,
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
            stmt: self.stmt.clone(),
        }
    }
}

impl<'src, I, E> SqlCtx<'src, I, E>
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    /// The grammar, with owning handles to all three roots: the statement
    /// root owns the rest, so a parser holding it holds the whole grammar,
    /// and dropping the last one frees it.
    pub fn build() -> Self {
        // Owning handles to the expression and query roots, taken out of
        // the nesting that builds them.
        let mut roots = None;
        let stmt = recursive(|stmt| {
            let mut owned_expr = None;
            let query = recursive(|query| {
                let expr = recursive(|expr| {
                    crate::expr::expr_body(Self {
                        expr,
                        query: query.clone(),
                        stmt: stmt.clone(),
                    })
                });
                owned_expr = Some(expr.clone());
                crate::query::Query::body_parser(Self {
                    expr,
                    query,
                    stmt: stmt.clone(),
                })
            });
            let expr = owned_expr.expect("built with the query grammar");
            roots = Some((expr.clone(), query.clone()));
            crate::statement::Statement::body_parser(Self { expr, query, stmt })
        });
        let (expr, query) = roots.expect("built with the statement grammar");
        Self { expr, query, stmt }
    }

    /// Wraps `parser` so that it owns the whole grammar: the expression and
    /// query roots' handles to the roots enclosing them are weak, so a
    /// parser holding only `expr` or `query` must keep `stmt` alive too.
    pub(crate) fn owning<O>(
        self,
        parser: impl Parser<'src, I, O, E> + Clone,
    ) -> impl Parser<'src, I, O, E> + Clone {
        parser.map(move |out| {
            let _keep = &self.stmt;
            out
        })
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
