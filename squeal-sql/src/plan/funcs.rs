use std::{
    collections::HashSet,
    fmt::Debug,
    sync::{Arc, atomic::AtomicUsize},
};

use parking_lot::RwLock;
use sql_parser::{Expr, Ident, expr::FunctionArg};
use store::{
    db::DBFile,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::eval::{EvalExpr, ExprWrapper},
};

pub(crate) trait FuncTrait: Debug {
    fn name(&self) -> String;
    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError>;
    fn is_aggregate(&self) -> bool;
    fn reset(&mut self) -> Result<(), SchemaError>;
    fn fields(&self) -> Vec<usize>;
    /// The function's current accumulated value, without feeding it
    /// another row the way `eval` would. Only needed for a grand-total
    /// aggregate (no GROUP BY at all) over a completely empty input:
    /// there's no row to call `eval` with, but the query still owes
    /// exactly one output row reporting each aggregate's freshly-`reset`
    /// state (e.g. `COUNT(*)` over an empty table is 0, not "no rows").
    fn current(&self) -> ValueItem;
}

#[derive(Debug, Clone)]
pub(crate) enum FuncArgs {
    Wildcard,
    // A function argument is a sub-expression like any other Unary/Binary
    // operand — no display name of its own (see EvalExpr::from_expr's own
    // doc comment on why that lives one level up, not here).
    Field(Box<EvalExpr>),
}

// Which SQL function a call resolved to, one variant per concrete
// implementation — not a `Box<dyn FuncTrait>`. A trait object can't be
// Clone (Clone isn't object-safe: it returns `Self`, whose size the
// vtable can't know), but EvalExpr — which holds a FuncObj — needs to be
// Clone (see e.g. CrateHeap building per-row copies of sort expressions).
// A closed enum sidesteps that entirely: every variant's inner type
// (Count, eventually Sum/Avg/...) derives Clone on its own, so this can
// too, and dispatch is a plain match instead of a vtable call.
#[derive(Debug, Clone)]
pub(crate) enum FuncObj {
    Count(Count),
    Avg(Avg),
    Upper(Upper),
}

impl<'a, F> TryFrom<&'a ExprWrapper<'a, F>> for FuncObj
where
    F: DBFile + 'static,
{
    type Error = SchemaError;
    fn try_from(value: &ExprWrapper<'a, F>) -> Result<Self, Self::Error> {
        if let Expr::Function {
            name,
            distinct,
            args,
            over: _,
        } = value.expr
        {
            let mut func_args = vec![];
            for arg in args {
                func_args.push(match arg {
                    FunctionArg::Wildcard(_) => FuncArgs::Wildcard,
                    FunctionArg::Expr(e) => FuncArgs::Field(EvalExpr::from_expr(e, value.tables)?),
                });
            }
            Self::get_fn(name, distinct, func_args)
        } else {
            panic!("Shouldnt be here");
        }
    }
}

impl FuncObj {
    fn get_fn(name: &Ident, distinct: &bool, args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        match name.value.as_str() {
            "count" => Ok(FuncObj::Count(Count::new(args, *distinct, None)?)),
            "avg" => {
                if *distinct {
                    return Err(SchemaError::UnsupportedFeature(
                        "avg(distinct ...) is not supported".into(),
                    ));
                }
                Ok(FuncObj::Avg(Avg::new(args, None)?))
            }
            "upper" => {
                if *distinct {
                    return Err(SchemaError::UnsupportedFeature(
                        "upper(distinct ...) is not supported — upper is a scalar function".into(),
                    ));
                }
                Ok(FuncObj::Upper(Upper::new(args)?))
            }
            _ => Err(SchemaError::UnknownFunction(name.value.clone())),
        }
    }
}

impl FuncTrait for FuncObj {
    fn name(&self) -> String {
        match self {
            FuncObj::Count(c) => c.name(),
            FuncObj::Avg(a) => a.name(),
            FuncObj::Upper(u) => u.name(),
        }
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        match self {
            FuncObj::Count(c) => c.eval(args),
            FuncObj::Avg(a) => a.eval(args),
            FuncObj::Upper(u) => u.eval(args),
        }
    }

    fn is_aggregate(&self) -> bool {
        match self {
            FuncObj::Count(c) => c.is_aggregate(),
            FuncObj::Avg(a) => a.is_aggregate(),
            FuncObj::Upper(u) => u.is_aggregate(),
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        match self {
            FuncObj::Count(c) => c.reset(),
            FuncObj::Avg(a) => a.reset(),
            FuncObj::Upper(u) => u.reset(),
        }
    }

    fn fields(&self) -> Vec<usize> {
        match self {
            FuncObj::Count(c) => c.fields(),
            FuncObj::Avg(a) => a.fields(),
            FuncObj::Upper(u) => u.fields(),
        }
    }

    fn current(&self) -> ValueItem {
        match self {
            FuncObj::Count(c) => c.current(),
            FuncObj::Avg(a) => a.current(),
            FuncObj::Upper(u) => u.current(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Count {
    name: String,
    values: HashSet<ValueItem>,
    distinct: bool,
    args: FuncArgs,
    count: usize,
}

impl Count {
    pub(crate) fn new(
        args: Vec<FuncArgs>,
        distinct: bool,
        name: Option<String>,
    ) -> Result<Self, SchemaError> {
        let name = if let Some(name) = name {
            name.clone()
        } else {
            "count".to_string()
        };
        if args.len() != 1 {
            return Err(SchemaError::UnknownFunction(format!(
                "count taking {} arguments",
                args.len()
            )));
        }
        let mut args = args;
        Ok(Self {
            values: HashSet::new(),
            args: args.pop().unwrap(),
            distinct,
            count: 0,
            name,
        })
    }
}

impl FuncTrait for Count {
    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        let res = if self.distinct {
            let item = match &mut self.args {
                FuncArgs::Field(exp) => exp.eval(args, 0)?,
                FuncArgs::Wildcard => ValueItem::Integer(1),
            };
            self.values.insert(item);
            self.values.len()
        } else {
            self.count += 1;
            self.count
        };
        Ok(ValueItem::Integer(res as i64))
    }

    fn is_aggregate(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        "count".into()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.count = 0;
        self.values.clear();
        Ok(())
    }

    fn fields(&self) -> Vec<usize> {
        vec![] // We already validate that only one non-wildcard argument is provided. And this is specifically called only to indetify non-agg fields, which does not apply here
    }

    fn current(&self) -> ValueItem {
        let res = if self.distinct {
            self.values.len()
        } else {
            self.count
        };
        ValueItem::Integer(res as i64)
    }
}

impl Clone for Count {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            args: self.args.clone(),
            distinct: self.distinct,
            count: self.count,
            name: self.name.clone(),
        }
    }
}

// Second aggregate variant, added specifically to prove FuncObj's closed
// enum dispatches correctly across more than one aggregate (name/eval/
// is_aggregate/reset/fields/current all gained a real second match arm,
// not just Count's).  Ignores NULLs the way SQL's own AVG does — neither
// counted nor summed — and reports NULL (not 0.0/NaN) if every input was
// NULL or there were no rows at all.
#[derive(Debug, Clone)]
pub(crate) struct Avg {
    name: String,
    args: FuncArgs,
    sum: f64,
    count: usize,
}

impl Avg {
    pub(crate) fn new(args: Vec<FuncArgs>, name: Option<String>) -> Result<Self, SchemaError> {
        let name = name.unwrap_or_else(|| "avg".to_string());
        if args.len() != 1 {
            return Err(SchemaError::UnknownFunction(format!(
                "avg taking {} arguments",
                args.len()
            )));
        }
        let mut args = args;
        let arg = args.pop().unwrap();
        if matches!(arg, FuncArgs::Wildcard) {
            return Err(SchemaError::UnsupportedFeature(
                "avg(*) is not valid — avg requires a single expression argument".into(),
            ));
        }
        Ok(Self {
            name,
            args: arg,
            sum: 0.0,
            count: 0,
        })
    }
}

impl FuncTrait for Avg {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        let FuncArgs::Field(exp) = &mut self.args else {
            unreachable!("avg(*) is rejected in Avg::new")
        };
        match exp.eval(args, 0)? {
            ValueItem::Integer(i) => {
                self.sum += i as f64;
                self.count += 1;
            }
            ValueItem::Double(d) => {
                self.sum += d;
                self.count += 1;
            }
            ValueItem::Null => {}
            other => {
                return Err(SchemaError::InvalidOperationOnOperand(
                    "avg".into(),
                    format!("non-numeric operand {other:?}"),
                ));
            }
        }
        Ok(self.current())
    }

    fn is_aggregate(&self) -> bool {
        true
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        self.sum = 0.0;
        self.count = 0;
        Ok(())
    }

    fn fields(&self) -> Vec<usize> {
        vec![] // Same reasoning as Count::fields — always an aggregate, so
        // EvalExpr::get_non_agg_fields never actually calls this.
    }

    fn current(&self) -> ValueItem {
        if self.count == 0 {
            ValueItem::Null
        } else {
            ValueItem::Double(self.sum / self.count as f64)
        }
    }
}

// Scalar variant — the category `get_funcs()`/`empty_group_row()` didn't
// used to handle correctly (see their own doc comments). `reset()`/
// `current()` deliberately `unreachable!()` rather than no-op: a no-op
// would hide a regression where those call sites stop filtering by
// `is_aggregate()`, whereas this turns that regression into an immediate
// test failure.
#[derive(Debug, Clone)]
pub(crate) struct Upper {
    args: FuncArgs,
}

impl Upper {
    pub(crate) fn new(args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        if args.len() != 1 {
            return Err(SchemaError::UnknownFunction(format!(
                "upper taking {} arguments",
                args.len()
            )));
        }
        let mut args = args;
        let arg = args.pop().unwrap();
        if matches!(arg, FuncArgs::Wildcard) {
            return Err(SchemaError::UnsupportedFeature(
                "upper(*) is not valid — upper requires a single expression argument".into(),
            ));
        }
        Ok(Self { args: arg })
    }
}

impl FuncTrait for Upper {
    fn name(&self) -> String {
        "upper".into()
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        let FuncArgs::Field(exp) = &mut self.args else {
            unreachable!("upper(*) is rejected in Upper::new")
        };
        match exp.eval(args, 0)? {
            ValueItem::Str((s, _)) => {
                let upper = s.to_uppercase();
                let len = upper.len() as u32;
                Ok(ValueItem::Str((upper, len)))
            }
            ValueItem::Null => Ok(ValueItem::Null),
            other => Err(SchemaError::InvalidOperationOnOperand(
                "upper".into(),
                format!("non-string operand {other:?}"),
            )),
        }
    }

    fn is_aggregate(&self) -> bool {
        false
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        unreachable!(
            "upper is a scalar function — FuncTrait::reset() must never be called on it; \
             EvalExpr::get_funcs() should have filtered it out before \
             GroupSource::reset_aggregates() ran"
        )
    }

    fn fields(&self) -> Vec<usize> {
        match &self.args {
            FuncArgs::Wildcard => vec![],
            FuncArgs::Field(e) => e.get_non_agg_fields(),
        }
    }

    fn current(&self) -> ValueItem {
        unreachable!(
            "upper is a scalar function — FuncTrait::current() must never be called on it; \
             GroupSource::empty_group_row() should have checked is_aggregate() first"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal_arg(v: ValueItem) -> FuncArgs {
        FuncArgs::Field(Box::new(EvalExpr::Literal(v)))
    }

    #[test]
    fn test_avg_averages_repeated_evals_of_the_same_value() {
        let mut avg = Avg::new(vec![literal_arg(ValueItem::Integer(10))], None).unwrap();
        avg.eval(&[]).unwrap();
        avg.eval(&[]).unwrap();
        assert_eq!(avg.eval(&[]).unwrap(), ValueItem::Double(10.0));
    }

    #[test]
    fn test_avg_ignores_nulls() {
        let mut avg = Avg::new(vec![literal_arg(ValueItem::Null)], None).unwrap();
        avg.eval(&[]).unwrap();
        avg.eval(&[]).unwrap();
        assert_eq!(
            avg.current(),
            ValueItem::Null,
            "avg of only NULLs must be NULL, not 0"
        );
    }

    #[test]
    fn test_avg_over_zero_rows_reports_null() {
        let avg = Avg::new(vec![literal_arg(ValueItem::Integer(0))], None).unwrap();
        assert_eq!(avg.current(), ValueItem::Null);
    }

    #[test]
    fn test_avg_rejects_non_numeric_operand() {
        let mut avg =
            Avg::new(vec![literal_arg(ValueItem::Str(("x".into(), 1)))], None).unwrap();
        assert!(avg.eval(&[]).is_err());
    }

    #[test]
    fn test_avg_rejects_wildcard_argument() {
        let err = Avg::new(vec![FuncArgs::Wildcard], None).unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedFeature(_)), "got {err:?}");
    }

    #[test]
    fn test_upper_uppercases_a_string() {
        let mut upper =
            Upper::new(vec![literal_arg(ValueItem::Str(("hello".into(), 5)))]).unwrap();
        assert_eq!(upper.eval(&[]).unwrap(), ValueItem::Str(("HELLO".into(), 5)));
    }

    #[test]
    fn test_upper_passes_through_null() {
        let mut upper = Upper::new(vec![literal_arg(ValueItem::Null)]).unwrap();
        assert_eq!(upper.eval(&[]).unwrap(), ValueItem::Null);
    }

    #[test]
    fn test_upper_rejects_non_string_operand() {
        let mut upper = Upper::new(vec![literal_arg(ValueItem::Integer(5))]).unwrap();
        assert!(upper.eval(&[]).is_err());
    }

    #[test]
    fn test_upper_is_not_an_aggregate() {
        let upper = Upper::new(vec![literal_arg(ValueItem::Str(("x".into(), 1)))]).unwrap();
        assert!(!upper.is_aggregate());
    }
}
