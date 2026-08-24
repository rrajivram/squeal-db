//! Expressions, parsed with hand-layered precedence (loosest binds last):
//!
//! ```text
//! or          :=  and (OR and)*
//! and         :=  not (AND not)*
//! not         :=  NOT* predicate
//! predicate   :=  sum [ cmp sum | IS [NOT] NULL | [NOT] IN (query | list)
//!                     | [NOT] BETWEEN sum AND sum | [NOT] LIKE/ILIKE sum ]
//! sum         :=  product ((+ | - | ||) product)*
//! product     :=  unary ((* | / | %) unary)*
//! unary       :=  (+ | -)* casted
//! casted      :=  atom (:: datatype)*
//! atom        :=  literal | CAST(..) | CASE .. END | EXISTS (query)
//!               | func(..) | column | placeholder | ( query ) | ( expr )
//! ```
//!
//! BETWEEN bounds sit at `sum` level so `a BETWEEN 1 AND 2 AND b` parses as
//! `(a BETWEEN 1 AND 2) AND b`.
//!
//! Expressions and queries are mutually recursive (scalar subqueries,
//! `IN (SELECT ..)`, `EXISTS`, and back through `WHERE`). The body here is
//! built by [`expr_body`] against the shared [`SqlCtx`] recursion handles;
//! `Expr::parser(ctx)` returns the `ctx.expr` handle, and `SqlCtx::build`
//! ties the knot exactly once.

use chumsky::{
    IterParser, Parser,
    extra::ParserExtra,
    label::LabelError,
    prelude::choice,
};
use either::Either;

use crate::{
    datatype::DataType,
    ident::{Ident, ObjectName},
    keyword as kw,
    literal::{Literal, NumberLiteral, NumberValue},
    parser::{SQLParser, SqlCtx, TokenInput, oper, punct},
    query::Query,
    span::TokenSpan,
    token::{Operator, Punctuation},
};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Column(ObjectName),
    Placeholder(Placeholder),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `x [NOT] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Like {
        expr: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
        pattern: Box<Expr>,
    },
    Function {
        name: Ident,
        distinct: bool,
        args: Vec<FunctionArg>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    /// A scalar subquery: `(SELECT ...)` used as a value.
    Subquery(Box<Query>),
    /// `EXISTS (SELECT ...)`; `NOT EXISTS` parses as `Unary { Not, Exists }`.
    Exists {
        query: Box<Query>,
    },
    Nested(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    /// `count(*)`
    Wildcard(TokenSpan),
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Placeholder {
    /// `?`
    Anonymous(TokenSpan),
    /// `$1`
    Positional(TokenSpan, i64),
    /// `:name`
    Named(TokenSpan, String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Concat,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

enum Suffix {
    Cmp(BinaryOp, Expr),
    IsNull(bool),
    In(bool, Vec<Expr>),
    InQuery(bool, Box<Query>),
    Between(bool, Expr, Expr),
    Like(bool, bool, Expr),
}

impl Suffix {
    fn apply(self, expr: Expr) -> Expr {
        let expr = Box::new(expr);
        match self {
            Suffix::Cmp(op, right) => Expr::Binary {
                left: expr,
                op,
                right: Box::new(right),
            },
            Suffix::IsNull(negated) => Expr::IsNull { expr, negated },
            Suffix::In(negated, list) => Expr::InList {
                expr,
                list,
                negated,
            },
            Suffix::InQuery(negated, query) => Expr::InSubquery {
                expr,
                query,
                negated,
            },
            Suffix::Between(negated, low, high) => Expr::Between {
                expr,
                negated,
                low: Box::new(low),
                high: Box::new(high),
            },
            Suffix::Like(negated, case_insensitive, pattern) => Expr::Like {
                expr,
                negated,
                case_insensitive,
                pattern: Box::new(pattern),
            },
        }
    }
}

impl<'src, I, E> SQLParser<'src, I, E, SqlCtx<'src, I, E>> for Expr
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(args: SqlCtx<'src, I, E>) -> impl Parser<'src, I, Self, E> + Clone {
        args.expr
    }
}

impl<'src, I, E> SQLParser<'src, I, E> for Expr
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    fn parser(_args: ()) -> impl Parser<'src, I, Self, E> + Clone {
        SqlCtx::build().expr
    }
}

/// The full expression grammar, self-references and subqueries going through
/// the `ctx` handles. Fed into `ctx.expr` by [`SqlCtx::build`].
pub(crate) fn expr_body<'src, I, E>(
    ctx: SqlCtx<'src, I, E>,
) -> impl Parser<'src, I, Expr, E> + Clone
where
    I: TokenInput<'src> + 'src,
    E: ParserExtra<'src, I> + 'src,
    E::Error: LabelError<'src, I, String>,
{
    let expr = ctx.expr.clone();
    let query = ctx.query.clone();
    let lparen = punct(Punctuation::LeftParenthesis);
    let rparen = punct(Punctuation::RightParenthesis);
    let comma = punct(Punctuation::Comma);

    let literal = Literal::parser(ctx.clone()).map(Expr::Literal);

    let placeholder = choice((
        punct(Punctuation::QuestionMark).map(Placeholder::Anonymous),
        punct(Punctuation::Dollar)
            .then(NumberLiteral::parser(()))
            .try_map(|(dollar, n), span| match n.value {
                NumberValue::Integer(i) => Ok(Placeholder::Positional(
                    TokenSpan {
                        start: dollar.start,
                        end: n.span.end,
                    },
                    i,
                )),
                NumberValue::Float(_) => Err(LabelError::expected_found(
                    [String::from("integer placeholder index")],
                    None,
                    span,
                )),
            }),
        punct(Punctuation::Colon)
            .then(Ident::parser(()))
            .map(|(colon, id)| {
                Placeholder::Named(
                    TokenSpan {
                        start: colon.start,
                        end: id.span.end,
                    },
                    id.value,
                )
            }),
    ))
    .map(Expr::Placeholder);

    let function_arg = choice((
        punct(Punctuation::Asterisk).map(FunctionArg::Wildcard),
        expr.clone().map(FunctionArg::Expr),
    ));
    let function = Ident::parser(())
        .then_ignore(lparen.clone())
        .then(kw::Distinct::parser(()).or_not())
        .then(function_arg.separated_by(comma.clone()).collect::<Vec<_>>())
        .then_ignore(rparen.clone())
        .map(|((name, distinct), args)| Expr::Function {
            name,
            distinct: distinct.is_some(),
            args,
        });

    let cast = kw::Cast::parser(())
        .ignore_then(lparen.clone())
        .ignore_then(expr.clone())
        .then_ignore(kw::As::parser(()))
        .then(DataType::parser(ctx.clone()))
        .then_ignore(rparen.clone())
        .map(|(e, data_type)| Expr::Cast {
            expr: Box::new(e),
            data_type,
        });

    let case_expr = kw::Case::parser(())
        .ignore_then(expr.clone().or_not())
        .then(
            kw::When::parser(())
                .ignore_then(expr.clone())
                .then_ignore(kw::Then::parser(()))
                .then(expr.clone())
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>(),
        )
        .then(kw::Else::parser(()).ignore_then(expr.clone()).or_not())
        .then_ignore(kw::End::parser(()))
        .map(|((operand, when_then), else_expr)| Expr::Case {
            operand: operand.map(Box::new),
            when_then,
            else_expr: else_expr.map(Box::new),
        });

    let exists = kw::Exists::parser(())
        .ignore_then(
            query
                .clone()
                .delimited_by(lparen.clone(), rparen.clone()),
        )
        .map(|q| Expr::Exists { query: Box::new(q) });

    let subquery = query
        .clone()
        .delimited_by(lparen.clone(), rparen.clone())
        .map(|q| Expr::Subquery(Box::new(q)));

    let column = ObjectName::parser(()).map(Expr::Column);

    let nested = expr
        .clone()
        .delimited_by(lparen.clone(), rparen.clone())
        .map(|e| Expr::Nested(Box::new(e)));

    // `function` before `column`: both begin with an identifier, and the
    // function branch backtracks if no '(' follows. `subquery` before
    // `nested`: both begin with '(', and a query must continue with
    // SELECT/WITH, which no expression can start with.
    let atom = choice((
        literal,
        cast,
        case_expr,
        exists,
        function,
        column,
        placeholder,
        subquery,
        nested,
    ))
    .boxed();

    let casted = atom
        .foldl(
            oper(Operator::DoubleColon)
                .ignore_then(DataType::parser(ctx.clone()))
                .repeated(),
            |e, data_type| Expr::Cast {
                expr: Box::new(e),
                data_type,
            },
        )
        .boxed();

    let unary = choice((
        punct(Punctuation::Plus).to(UnaryOp::Plus),
        punct(Punctuation::Minus).to(UnaryOp::Minus),
    ))
    .repeated()
    .foldr(casted, |op, e| Expr::Unary {
        op,
        expr: Box::new(e),
    })
    .boxed();

    let product = unary
        .clone()
        .foldl(
            choice((
                punct(Punctuation::Asterisk).to(BinaryOp::Multiply),
                punct(Punctuation::Slash).to(BinaryOp::Divide),
                punct(Punctuation::Percent).to(BinaryOp::Modulo),
            ))
            .then(unary)
            .repeated(),
            |l, (op, r)| binary(l, op, r),
        )
        .boxed();

    let sum = product
        .clone()
        .foldl(
            choice((
                punct(Punctuation::Plus).to(BinaryOp::Plus),
                punct(Punctuation::Minus).to(BinaryOp::Minus),
                oper(Operator::Concat).to(BinaryOp::Concat),
            ))
            .then(product)
            .repeated(),
            |l, (op, r)| binary(l, op, r),
        )
        .boxed();

    let cmp_op = choice((
        oper(Operator::LtEq).to(BinaryOp::LtEq),
        oper(Operator::GtEq).to(BinaryOp::GtEq),
        oper(Operator::NotEq).to(BinaryOp::NotEq),
        oper(Operator::NotEqBang).to(BinaryOp::NotEq),
        punct(Punctuation::Equals).to(BinaryOp::Eq),
        punct(Punctuation::LessThan).to(BinaryOp::Lt),
        punct(Punctuation::GreaterThan).to(BinaryOp::Gt),
    ));

    let negation = kw::Not::parser(()).or_not().map(|n| n.is_some());
    let suffix = choice((
        cmp_op.then(sum.clone()).map(|(op, r)| Suffix::Cmp(op, r)),
        kw::Is::parser(())
            .ignore_then(negation.clone())
            .then_ignore(kw::Null::parser(()))
            .map(Suffix::IsNull),
        negation
            .clone()
            .then_ignore(kw::In::parser(()))
            .then(
                choice((
                    query.clone().map(Either::Left),
                    expr.clone()
                        .separated_by(comma)
                        .at_least(1)
                        .collect::<Vec<_>>()
                        .map(Either::Right),
                ))
                .delimited_by(lparen, rparen),
            )
            .map(|(neg, source)| match source {
                Either::Left(q) => Suffix::InQuery(neg, Box::new(q)),
                Either::Right(list) => Suffix::In(neg, list),
            }),
        negation
            .clone()
            .then_ignore(kw::Between::parser(()))
            .then(sum.clone())
            .then_ignore(kw::And::parser(()))
            .then(sum.clone())
            .map(|((neg, low), high)| Suffix::Between(neg, low, high)),
        negation
            .then(choice((
                kw::Like::parser(()).to(false),
                kw::Ilike::parser(()).to(true),
            )))
            .then(sum.clone())
            .map(|((neg, ci), pattern)| Suffix::Like(neg, ci, pattern)),
    ));

    let predicate = sum
        .then(suffix.or_not())
        .map(|(e, suffix)| match suffix {
            Some(s) => s.apply(e),
            None => e,
        })
        .boxed();

    let not_expr = kw::Not::parser(())
        .repeated()
        .foldr(predicate, |_, e| Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(e),
        })
        .boxed();

    let and_expr = not_expr
        .clone()
        .foldl(
            kw::And::parser(()).ignore_then(not_expr).repeated(),
            |l, r| binary(l, BinaryOp::And, r),
        )
        .boxed();

    and_expr
        .clone()
        .foldl(
            kw::Or::parser(()).ignore_then(and_expr).repeated(),
            |l, r| binary(l, BinaryOp::Or, r),
        )
        .boxed()
}
