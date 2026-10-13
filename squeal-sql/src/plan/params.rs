//! The values of a statement's placeholders.
//!
//! A statement runs from a syntax tree with placeholders in it in two
//! cases: a prepared statement (`?`, `$1`, `:name`), and any statement the
//! parse cache handed back as its shape's template, with its literals
//! beside it (see sql_parser::parse_sql_shaped). Either way a placeholder
//! is found here by where it is in the text: each value is kept with its
//! placeholder's span, and a literal has the span of the placeholder
//! standing for it.

use sql_parser::{
    Expr, Placeholder,
    literal::{Literal, NumberValue},
    span::TokenSpan,
};
use std::sync::{Arc, OnceLock};

use store::valueitem::ValueItem;

use crate::error::SchemaError;

// Each value with where its placeholder starts in the text, in that order.
// Shared: a plan and its statement hold the same list.
// `slots`: placeholders plan as parameter slots (EvalExpr::Param) rather
// than as their values — preparing a plan for every execution of a shape.
#[derive(Debug, Clone)]
pub(crate) struct Params(Arc<[(usize, ValueItem)]>, bool);

impl Params {
    /// No placeholders.
    pub(crate) fn none() -> Self {
        static NONE: OnceLock<Arc<[(usize, ValueItem)]>> = OnceLock::new();
        Params(NONE.get_or_init(|| Arc::from(Vec::new())).clone(), false)
    }

    /// A shape's literals, in source order.
    pub(crate) fn from_literals(literals: &[Literal]) -> Self {
        Params(
            literals
                .iter()
                .map(|l| (literal_span(l).start, literal_value(l)))
                .collect(),
            false,
        )
    }

    /// Values bound to `placeholders` (in source order) one for one.
    pub(crate) fn bound(
        placeholders: &[&Placeholder],
        values: &[Option<ValueItem>],
    ) -> Result<Self, SchemaError> {
        let mut out = Vec::with_capacity(placeholders.len());
        for (i, p) in placeholders.iter().enumerate() {
            let value = values.get(i).cloned().flatten().ok_or_else(|| {
                SchemaError::UserError(format!("parameter {} was not bound", i + 1))
            })?;
            out.push((placeholder_span(p).start, value));
        }
        out.sort_by_key(|(at, _)| *at);
        Ok(Params(out.into(), false))
    }

    /// The same values, with placeholders planned as slots.
    pub(crate) fn as_slots(&self) -> Self {
        Params(self.0.clone(), true)
    }

    pub(crate) fn slots(&self) -> bool {
        self.1
    }

    /// Which value the placeholder `p` is: its slot.
    pub(crate) fn index_of(&self, p: &Placeholder) -> Result<usize, SchemaError> {
        let at = placeholder_span(p).start;
        self.0.binary_search_by_key(&at, |(s, _)| *s).map_err(|_| {
            SchemaError::UserError(
                "a placeholder has no value: run the statement prepared, binding each one".into(),
            )
        })
    }

    /// The value of the placeholder `p`.
    pub(crate) fn get(&self, p: &Placeholder) -> Result<&ValueItem, SchemaError> {
        Ok(&self.0[self.index_of(p)?].1)
    }

    /// The value in slot `i`.
    pub(crate) fn value(&self, i: usize) -> Result<&ValueItem, SchemaError> {
        self.0.get(i).map(|(_, v)| v).ok_or_else(|| {
            SchemaError::InternalSchemaError(format!("no value for parameter slot {i}"))
        })
    }

    /// `e` as an integer, when it is an integer literal or a placeholder
    /// holding one (a `LIMIT`, an `ORDER BY` position).
    pub(crate) fn integer(&self, e: &Expr) -> Option<i64> {
        match e {
            Expr::Literal(Literal::Number(n)) => n.as_i64(),
            Expr::Placeholder(p) => match self.get(p).ok()? {
                ValueItem::Integer(i) => Some(*i),
                _ => None,
            },
            _ => None,
        }
    }
}

fn placeholder_span(p: &Placeholder) -> TokenSpan {
    match p {
        Placeholder::Anonymous(s) | Placeholder::Positional(s, _) | Placeholder::Named(s, _) => *s,
    }
}

fn literal_span(l: &Literal) -> TokenSpan {
    match l {
        Literal::Number(n) => n.span,
        Literal::String(s) => s.span,
        Literal::Boolean(b) => b.0.as_ref().either(|t| t.span, |f| f.span),
        Literal::Null(n) => n.span,
    }
}

/// A literal's value, as a column holds it.
pub(crate) fn literal_value(l: &Literal) -> ValueItem {
    match l {
        Literal::Boolean(b) => ValueItem::Boolean(b.value()),
        Literal::Null(_) => ValueItem::Null,
        Literal::String(s) => ValueItem::Str((s.value.clone(), s.value.len() as u32)),
        Literal::Number(n) => match n.value {
            NumberValue::Float(f) => ValueItem::Double(f),
            NumberValue::Integer(i) => ValueItem::Integer(i),
        },
    }
}
