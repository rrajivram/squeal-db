use std::{collections::HashSet, fmt::Debug};

use sql_parser::{Expr, Ident, expr::FunctionArg};
use store::{
    db::DBFile,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::eval::{EvalExpr, ExprWrapper},
};

#[allow(unused)]
pub(crate) trait FuncTrait: Debug {
    fn name(&self) -> String;
    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError>;
    fn is_aggregate(&self) -> bool;
    fn reset(&mut self) -> Result<(), SchemaError>;
    /// This call's own argument list, exactly as given — `Wildcard` for
    /// `count(*)`, `Field(expr)` for everything else, in order. The one
    /// thing every function actually needs to hand its caller (`fields()`
    /// derives from it below; EvalExpr::describe — EXPLAIN's rendering —
    /// recurses into each argument's own `describe()` from it too, which
    /// is what lets a call nested inside another function's argument list
    /// — `concat(name, upper(name))` — render as itself instead of
    /// collapsing to the column it ultimately reads).
    fn args(&self) -> Vec<&FuncArgs>;
    /// Every raw column position this call's arguments read — used by
    /// EvalExpr::get_non_agg_fields, which already checks is_aggregate()
    /// itself before ever calling this (see its own Self::Function arm),
    /// so the default here doesn't need to. A function only needs to
    /// override this if its arguments aren't its own real column reads —
    /// none currently do.
    fn fields(&self) -> Vec<usize> {
        self.args()
            .iter()
            .flat_map(|a| match a {
                FuncArgs::Wildcard => vec![],
                FuncArgs::Field(e) => e.get_non_agg_fields(),
            })
            .collect()
    }
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

// ---- macros: the three shapes most new SQL functions fit ----
//
// Not every function fits one of these (COUNT's DISTINCT tracking, SUM's
// int/double promotion, CONCAT's variable arity all still need a real
// FuncTrait impl, same as they would without any macro) — these cover the
// repetitive PART of adding a function, not all of it:
//
//   func_obj!      the FuncObj enum + its FuncTrait dispatch (every
//                   function needs this, regardless of shape)
//   scalar_fn!      a one-argument scalar (non-aggregate) function —
//                   UPPER, LOWER, and anything else of the shape
//                   "take one value, return one value, no state"
//   extremum_fn!    a one-argument aggregate that just keeps the
//                   best-so-far value under some ordering — MIN, MAX

// See its own call site (just below) for what adding a function to this
// list looks like. `$ty` is a separate parameter from `$variant` (rather
// than reusing the variant name as the type) only because Rust doesn't
// let a macro derive one identifier from another — every real call below
// still has them matching.
macro_rules! func_obj {
    ($($variant:ident($ty:ty)),+ $(,)?) => {
        #[derive(Debug, Clone)]
        pub(crate) enum FuncObj {
            $($variant($ty),)+
        }

        impl FuncTrait for FuncObj {
            fn name(&self) -> String {
                match self { $(FuncObj::$variant(f) => f.name(),)+ }
            }
            fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
                match self { $(FuncObj::$variant(f) => f.eval(args),)+ }
            }
            fn is_aggregate(&self) -> bool {
                match self { $(FuncObj::$variant(f) => f.is_aggregate(),)+ }
            }
            fn reset(&mut self) -> Result<(), SchemaError> {
                match self { $(FuncObj::$variant(f) => f.reset(),)+ }
            }
            fn args(&self) -> Vec<&FuncArgs> {
                match self { $(FuncObj::$variant(f) => f.args(),)+ }
            }
            fn current(&self) -> ValueItem {
                match self { $(FuncObj::$variant(f) => f.current(),)+ }
            }
        }
    };
}

// A complete scalar (non-aggregate, one-argument) function: arg-count and
// wildcard-rejection validation, and the FuncTrait boilerplate every
// scalar function shares (is_aggregate: false; reset/current:
// unreachable!, since GroupSource/EvalExpr are only ever supposed to call
// those on an aggregate — see Upper's own original comment, preserved
// below). `$body` gets the evaluated argument bound to `$val` and must
// return `Result<ValueItem, SchemaError>` — everything specific to what
// the function actually does.
//
// Usage: scalar_fn!(Lower, "lower", |v: ValueItem| match v { ... });
macro_rules! scalar_fn {
    ($name:ident, $sql_name:literal, |$val:ident: ValueItem| $body:expr) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $name {
            args: FuncArgs,
        }

        impl $name {
            pub(crate) fn new(args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
                if args.len() != 1 {
                    return Err(SchemaError::UnknownFunction(format!(
                        "{} taking {} arguments",
                        $sql_name,
                        args.len()
                    )));
                }
                let mut args = args;
                let arg = args.pop().unwrap();
                if matches!(arg, FuncArgs::Wildcard) {
                    return Err(SchemaError::UnsupportedFeature(format!(
                        "{}(*) is not valid — {} requires a single expression argument",
                        $sql_name, $sql_name
                    )));
                }
                Ok(Self { args: arg })
            }
        }

        impl FuncTrait for $name {
            fn name(&self) -> String {
                $sql_name.into()
            }

            fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
                let FuncArgs::Field(exp) = &mut self.args else {
                    unreachable!(concat!($sql_name, "(*) is rejected in new()"))
                };
                let $val = exp.eval(args, 0)?;
                $body
            }

            fn is_aggregate(&self) -> bool {
                false
            }

            fn reset(&mut self) -> Result<(), SchemaError> {
                unreachable!(concat!(
                    $sql_name,
                    " is a scalar function — FuncTrait::reset() must never be called on it; \
                     EvalExpr::get_funcs() should have filtered it out before \
                     GroupSource::reset_aggregates() ran"
                ))
            }

            fn args(&self) -> Vec<&FuncArgs> {
                vec![&self.args]
            }

            fn current(&self) -> ValueItem {
                unreachable!(concat!(
                    $sql_name,
                    " is a scalar function — FuncTrait::current() must never be called on it; \
                     GroupSource::empty_group_row() should have checked is_aggregate() first"
                ))
            }
        }
    };
}

// A complete one-argument aggregate that just keeps the best-so-far value
// under some ordering, ignoring NULLs (same convention Avg already uses)
// and reporting NULL if every input was NULL or there were no rows.
// `$pick` decides which of two non-NULL values to keep — `ValueItem::min`/
// `ValueItem::max` (the std Ord ones: an owned receiver resolves there,
// not to IndexKey/ValueItem's own differently-named lower_bound/
// upper_bound — see valueitem.rs's own doc comment on that rename).
//
// Usage: extremum_fn!(Max, "max", ValueItem::max);
macro_rules! extremum_fn {
    ($name:ident, $sql_name:literal, $pick:expr) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $name {
            args: FuncArgs,
            best: Option<ValueItem>,
        }

        impl $name {
            pub(crate) fn new(args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
                if args.len() != 1 {
                    return Err(SchemaError::UnknownFunction(format!(
                        "{} taking {} arguments",
                        $sql_name,
                        args.len()
                    )));
                }
                let mut args = args;
                let arg = args.pop().unwrap();
                if matches!(arg, FuncArgs::Wildcard) {
                    return Err(SchemaError::UnsupportedFeature(format!(
                        "{}(*) is not valid — {} requires a single expression argument",
                        $sql_name, $sql_name
                    )));
                }
                Ok(Self { args: arg, best: None })
            }
        }

        impl FuncTrait for $name {
            fn name(&self) -> String {
                $sql_name.into()
            }

            fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
                let FuncArgs::Field(exp) = &mut self.args else {
                    unreachable!(concat!($sql_name, "(*) is rejected in new()"))
                };
                let v = exp.eval(args, 0)?;
                if !matches!(v, ValueItem::Null) {
                    self.best = Some(match self.best.take() {
                        Some(cur) => $pick(cur, v),
                        None => v,
                    });
                }
                Ok(self.current())
            }

            fn is_aggregate(&self) -> bool {
                true
            }

            fn reset(&mut self) -> Result<(), SchemaError> {
                self.best = None;
                Ok(())
            }

            fn args(&self) -> Vec<&FuncArgs> {
                vec![&self.args]
            }

            fn current(&self) -> ValueItem {
                self.best.clone().unwrap_or(ValueItem::Null)
            }
        }
    };
}

// Which SQL function a call resolved to, one variant per concrete
// implementation — not a `Box<dyn FuncTrait>`. A trait object can't be
// Clone (Clone isn't object-safe: it returns `Self`, whose size the
// vtable can't know), but EvalExpr — which holds a FuncObj — needs to be
// Clone (see e.g. CrateHeap building per-row copies of sort expressions).
// A closed enum sidesteps that entirely: every variant's inner type
// derives Clone on its own, so this can too, and dispatch is a plain
// match instead of a vtable call.
//
// Adding a function used to mean: add the variant here, AND add one
// match arm to each of the 6 methods below, by hand, in the same order,
// with the same name — six chances to typo a variant name or forget an
// arm, caught only by a compile error (if you're lucky) or a silent
// wrong dispatch (if you're not: `fields()` is a `Vec<usize>` return,
// same type in every arm, so a copy-pasted arm returning the WRONG
// variant's fields compiles fine and just answers wrong). func_obj!
// below generates the enum and all 6 arms from one list, so a new
// function is exactly one line here — no dispatch code to write or keep
// in sync, and every variant is still forced to implement FuncTrait in
// full (the macro only writes `Variant(f) => f.method(..)`; there's no
// way to leave a method out).
func_obj! {
    Count(Count),
    Avg(Avg),
    Sum(Sum),
    Min(Min),
    Max(Max),
    Upper(Upper),
    Lower(Lower),
    Concat(Concat),
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
    // The one place SQL function NAMES map to a constructor — everything
    // else (dispatch, the struct/FuncTrait boilerplate for a function
    // that fits one of the macro shapes below) is generated. DISTINCT is
    // rejected for every function here except COUNT, which is the only
    // one it changes the meaning of; MIN(DISTINCT x)/MAX(DISTINCT x) are
    // well-defined SQL (a no-op) but rejected anyway for the same reason
    // AVG/UPPER already do — silently ignoring a keyword the user wrote
    // is worse than telling them it does nothing here.
    fn get_fn(name: &Ident, distinct: &bool, args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        let reject_distinct = |what: &str| -> Result<(), SchemaError> {
            if *distinct {
                Err(SchemaError::UnsupportedFeature(format!(
                    "{what}(distinct ...) is not supported"
                )))
            } else {
                Ok(())
            }
        };
        match name.value.as_str() {
            "count" => Ok(FuncObj::Count(Count::new(args, *distinct, None)?)),
            "avg" => {
                reject_distinct("avg")?;
                Ok(FuncObj::Avg(Avg::new(args, None)?))
            }
            "sum" => {
                reject_distinct("sum")?;
                Ok(FuncObj::Sum(Sum::new(args)?))
            }
            "min" => {
                reject_distinct("min")?;
                Ok(FuncObj::Min(Min::new(args)?))
            }
            "max" => {
                reject_distinct("max")?;
                Ok(FuncObj::Max(Max::new(args)?))
            }
            "upper" => {
                reject_distinct("upper — upper is a scalar function")?;
                Ok(FuncObj::Upper(Upper::new(args)?))
            }
            "lower" => {
                reject_distinct("lower — lower is a scalar function")?;
                Ok(FuncObj::Lower(Lower::new(args)?))
            }
            "concat" => {
                reject_distinct("concat — concat is a scalar function")?;
                Ok(FuncObj::Concat(Concat::new(args)?))
            }
            _ => Err(SchemaError::UnknownFunction(name.value.clone())),
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

    fn args(&self) -> Vec<&FuncArgs> {
        vec![&self.args]
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

    fn args(&self) -> Vec<&FuncArgs> {
        vec![&self.args]
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
// `current()` (generated by scalar_fn! below) deliberately `unreachable!()`
// rather than no-op: a no-op would hide a regression where those call
// sites stop filtering by `is_aggregate()`, whereas this turns that
// regression into an immediate test failure.
scalar_fn!(Upper, "upper", |v: ValueItem| match v {
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
});

scalar_fn!(Lower, "lower", |v: ValueItem| match v {
    ValueItem::Str((s, _)) => {
        let lower = s.to_lowercase();
        let len = lower.len() as u32;
        Ok(ValueItem::Str((lower, len)))
    }
    ValueItem::Null => Ok(ValueItem::Null),
    other => Err(SchemaError::InvalidOperationOnOperand(
        "lower".into(),
        format!("non-string operand {other:?}"),
    )),
});

extremum_fn!(Min, "min", ValueItem::min);
extremum_fn!(Max, "max", ValueItem::max);

// Not built on numeric_fold_fn-style sharing with Avg: SUM keeps its
// input's own numeric shape (an all-Integer column sums to an Integer,
// checked for overflow the same way the `+` operator is — see plan::
// eval's `numeric` helper — and only promotes to Double the moment a
// Double is involved), where Avg always divides down to a Double
// regardless. Different enough result-typing that forcing both through
// one macro would either complicate the macro for no shared benefit or
// quietly change Avg's already-tested behavior — so this one is its own
// small hand-written FuncTrait impl, same as it would be without a macro
// at all.
#[derive(Debug, Clone)]
pub(crate) struct Sum {
    args: FuncArgs,
    total: ValueItem, // Integer(0) until a Double is seen, then Double
    any: bool,
}

impl Sum {
    pub(crate) fn new(args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        if args.len() != 1 {
            return Err(SchemaError::UnknownFunction(format!(
                "sum taking {} arguments",
                args.len()
            )));
        }
        let mut args = args;
        let arg = args.pop().unwrap();
        if matches!(arg, FuncArgs::Wildcard) {
            return Err(SchemaError::UnsupportedFeature(
                "sum(*) is not valid — sum requires a single expression argument".into(),
            ));
        }
        Ok(Self {
            args: arg,
            total: ValueItem::Integer(0),
            any: false,
        })
    }
}

impl FuncTrait for Sum {
    fn name(&self) -> String {
        "sum".into()
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        let FuncArgs::Field(exp) = &mut self.args else {
            unreachable!("sum(*) is rejected in Sum::new")
        };
        match exp.eval(args, 0)? {
            ValueItem::Integer(i) => {
                self.any = true;
                self.total = match self.total {
                    ValueItem::Integer(t) => ValueItem::Integer(t.checked_add(i).ok_or_else(|| {
                        SchemaError::InvalidOperationOnOperand("sum".into(), "integer overflow".into())
                    })?),
                    ValueItem::Double(t) => ValueItem::Double(t + i as f64),
                    _ => unreachable!("total is always Integer or Double"),
                };
            }
            ValueItem::Double(d) => {
                self.any = true;
                self.total = ValueItem::Double(match self.total {
                    ValueItem::Integer(t) => t as f64 + d,
                    ValueItem::Double(t) => t + d,
                    _ => unreachable!("total is always Integer or Double"),
                });
            }
            ValueItem::Null => {}
            other => {
                return Err(SchemaError::InvalidOperationOnOperand(
                    "sum".into(),
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
        self.total = ValueItem::Integer(0);
        self.any = false;
        Ok(())
    }

    fn args(&self) -> Vec<&FuncArgs> {
        vec![&self.args]
    }

    fn current(&self) -> ValueItem {
        if self.any {
            self.total.clone()
        } else {
            ValueItem::Null
        }
    }
}

// Variadic, unlike every function above — doesn't fit scalar_fn!'s
// exactly-one-argument shape, so it's hand-written; still only needs to
// implement FuncTrait once, same as any macro-generated function, and
// still gets its FuncObj dispatch for free from func_obj!.
//
// NULL propagates (CONCAT('a', NULL) is NULL, not 'a') to match this
// crate's own `||` binary operator (see plan::eval::binary's early NULL
// check) rather than picking a real-world dialect (engines disagree:
// Postgres/MySQL treat a NULL argument as empty string, ANSI/SQL Server
// propagate it) — consistency with the operator already in this codebase
// beats an arbitrary choice between the two. Non-string arguments are
// coerced via ValueItem's own Display (e.g. `concat('n=', 5)` -> `'n=5'`),
// the common cross-dialect behavior.
#[derive(Debug, Clone)]
pub(crate) struct Concat {
    args: Vec<FuncArgs>,
}

impl Concat {
    pub(crate) fn new(args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        if args.len() < 2 {
            return Err(SchemaError::UnknownFunction(format!(
                "concat taking {} arguments — needs at least 2",
                args.len()
            )));
        }
        if args.iter().any(|a| matches!(a, FuncArgs::Wildcard)) {
            return Err(SchemaError::UnsupportedFeature(
                "concat(*) is not valid — concat requires expression arguments".into(),
            ));
        }
        Ok(Self { args })
    }
}

impl FuncTrait for Concat {
    fn name(&self) -> String {
        "concat".into()
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        let mut out = String::new();
        for a in &mut self.args {
            let FuncArgs::Field(exp) = a else {
                unreachable!("concat(*) is rejected in Concat::new")
            };
            match exp.eval(args, 0)? {
                ValueItem::Null => return Ok(ValueItem::Null),
                ValueItem::Str((s, _)) => out.push_str(&s),
                other => out.push_str(&other.to_string()),
            }
        }
        let len = out.len() as u32;
        Ok(ValueItem::Str((out, len)))
    }

    fn is_aggregate(&self) -> bool {
        false
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        unreachable!(
            "concat is a scalar function — FuncTrait::reset() must never be called on it; \
             EvalExpr::get_funcs() should have filtered it out before \
             GroupSource::reset_aggregates() ran"
        )
    }

    fn args(&self) -> Vec<&FuncArgs> {
        self.args.iter().collect()
    }

    fn current(&self) -> ValueItem {
        unreachable!(
            "concat is a scalar function — FuncTrait::current() must never be called on it; \
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
        let mut avg = Avg::new(vec![literal_arg(ValueItem::Str(("x".into(), 1)))], None).unwrap();
        assert!(avg.eval(&[]).is_err());
    }

    #[test]
    fn test_avg_rejects_wildcard_argument() {
        let err = Avg::new(vec![FuncArgs::Wildcard], None).unwrap_err();
        assert!(
            matches!(err, SchemaError::UnsupportedFeature(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn test_upper_uppercases_a_string() {
        let mut upper = Upper::new(vec![literal_arg(ValueItem::Str(("hello".into(), 5)))]).unwrap();
        assert_eq!(
            upper.eval(&[]).unwrap(),
            ValueItem::Str(("HELLO".into(), 5))
        );
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

    // ---- lower: same shape as upper, proves scalar_fn! generalizes ----

    #[test]
    fn test_lower_lowercases_a_string() {
        let mut lower = Lower::new(vec![literal_arg(ValueItem::Str(("HeLLo".into(), 5)))]).unwrap();
        assert_eq!(lower.eval(&[]).unwrap(), ValueItem::Str(("hello".into(), 5)));
    }

    #[test]
    fn test_lower_passes_through_null() {
        let mut lower = Lower::new(vec![literal_arg(ValueItem::Null)]).unwrap();
        assert_eq!(lower.eval(&[]).unwrap(), ValueItem::Null);
    }

    #[test]
    fn test_lower_rejects_non_string_operand() {
        let mut lower = Lower::new(vec![literal_arg(ValueItem::Integer(5))]).unwrap();
        assert!(lower.eval(&[]).is_err());
    }

    #[test]
    fn test_lower_is_not_an_aggregate() {
        let lower = Lower::new(vec![literal_arg(ValueItem::Str(("X".into(), 1)))]).unwrap();
        assert!(!lower.is_aggregate());
    }

    // ---- min/max ----

    #[test]
    fn test_min_tracks_the_smallest_value_seen() {
        let mut min = Min::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        for v in [5, 2, 8, 1, 9] {
            min.args = literal_arg(ValueItem::Integer(v));
            min.eval(&[]).unwrap();
        }
        assert_eq!(min.current(), ValueItem::Integer(1));
    }

    #[test]
    fn test_max_tracks_the_largest_value_seen() {
        let mut max = Max::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        for v in [5, 2, 8, 1, 9] {
            max.args = literal_arg(ValueItem::Integer(v));
            max.eval(&[]).unwrap();
        }
        assert_eq!(max.current(), ValueItem::Integer(9));
    }

    #[test]
    fn test_min_and_max_ignore_nulls() {
        let mut min = Min::new(vec![literal_arg(ValueItem::Null)]).unwrap();
        min.eval(&[]).unwrap();
        min.args = literal_arg(ValueItem::Integer(7));
        min.eval(&[]).unwrap();
        min.args = literal_arg(ValueItem::Null);
        assert_eq!(min.eval(&[]).unwrap(), ValueItem::Integer(7));
    }

    #[test]
    fn test_min_and_max_over_zero_rows_report_null() {
        let min = Min::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        assert_eq!(min.current(), ValueItem::Null);
        let max = Max::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        assert_eq!(max.current(), ValueItem::Null);
    }

    #[test]
    fn test_min_max_reset_clears_the_tracked_value() {
        let mut max = Max::new(vec![literal_arg(ValueItem::Integer(5))]).unwrap();
        max.eval(&[]).unwrap();
        assert_eq!(max.current(), ValueItem::Integer(5));
        max.reset().unwrap();
        assert_eq!(max.current(), ValueItem::Null);
    }

    #[test]
    fn test_min_max_work_on_strings_too() {
        let mut min = Min::new(vec![literal_arg(ValueItem::Str(("pear".into(), 4)))]).unwrap();
        min.eval(&[]).unwrap();
        min.args = literal_arg(ValueItem::Str(("apple".into(), 5)));
        min.eval(&[]).unwrap();
        min.args = literal_arg(ValueItem::Str(("banana".into(), 6)));
        assert_eq!(min.eval(&[]).unwrap(), ValueItem::Str(("apple".into(), 5)));
    }

    #[test]
    fn test_min_and_max_are_aggregates() {
        let min = Min::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        assert!(min.is_aggregate());
        let max = Max::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        assert!(max.is_aggregate());
    }

    // ---- sum ----

    #[test]
    fn test_sum_of_integers_stays_an_integer() {
        let mut sum = Sum::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        for v in [1, 2, 3] {
            sum.args = literal_arg(ValueItem::Integer(v));
            sum.eval(&[]).unwrap();
        }
        assert_eq!(sum.current(), ValueItem::Integer(6));
    }

    #[test]
    fn test_sum_promotes_to_double_once_a_double_is_seen() {
        let mut sum = Sum::new(vec![literal_arg(ValueItem::Integer(1))]).unwrap();
        sum.eval(&[]).unwrap();
        sum.args = literal_arg(ValueItem::Double(1.5));
        assert_eq!(sum.eval(&[]).unwrap(), ValueItem::Double(2.5));
    }

    #[test]
    fn test_sum_ignores_nulls() {
        let mut sum = Sum::new(vec![literal_arg(ValueItem::Null)]).unwrap();
        sum.eval(&[]).unwrap();
        sum.args = literal_arg(ValueItem::Integer(4));
        assert_eq!(sum.eval(&[]).unwrap(), ValueItem::Integer(4));
    }

    #[test]
    fn test_sum_over_zero_rows_reports_null() {
        let sum = Sum::new(vec![literal_arg(ValueItem::Integer(0))]).unwrap();
        assert_eq!(sum.current(), ValueItem::Null);
    }

    #[test]
    fn test_sum_rejects_non_numeric_operand() {
        let mut sum = Sum::new(vec![literal_arg(ValueItem::Str(("x".into(), 1)))]).unwrap();
        assert!(sum.eval(&[]).is_err());
    }

    #[test]
    fn test_sum_overflow_is_an_error_not_a_panic() {
        let mut sum = Sum::new(vec![literal_arg(ValueItem::Integer(i64::MAX))]).unwrap();
        sum.eval(&[]).unwrap();
        sum.args = literal_arg(ValueItem::Integer(1));
        assert!(sum.eval(&[]).is_err());
    }

    // ---- concat ----

    fn args2(a: ValueItem, b: ValueItem) -> Vec<FuncArgs> {
        vec![literal_arg(a), literal_arg(b)]
    }

    #[test]
    fn test_concat_joins_strings() {
        let mut c = Concat::new(args2(
            ValueItem::Str(("foo".into(), 3)),
            ValueItem::Str(("bar".into(), 3)),
        ))
        .unwrap();
        assert_eq!(c.eval(&[]).unwrap(), ValueItem::Str(("foobar".into(), 6)));
    }

    #[test]
    fn test_concat_coerces_non_string_arguments() {
        let mut c = Concat::new(args2(ValueItem::Str(("n=".into(), 2)), ValueItem::Integer(5))).unwrap();
        assert_eq!(c.eval(&[]).unwrap(), ValueItem::Str(("n=5".into(), 3)));
    }

    #[test]
    fn test_concat_propagates_null() {
        let mut c = Concat::new(args2(ValueItem::Str(("a".into(), 1)), ValueItem::Null)).unwrap();
        assert_eq!(c.eval(&[]).unwrap(), ValueItem::Null);
    }

    #[test]
    fn test_concat_handles_more_than_two_arguments() {
        let mut c = Concat::new(vec![
            literal_arg(ValueItem::Str(("a".into(), 1))),
            literal_arg(ValueItem::Str(("b".into(), 1))),
            literal_arg(ValueItem::Str(("c".into(), 1))),
        ])
        .unwrap();
        assert_eq!(c.eval(&[]).unwrap(), ValueItem::Str(("abc".into(), 3)));
    }

    #[test]
    fn test_concat_rejects_fewer_than_two_arguments() {
        assert!(Concat::new(vec![literal_arg(ValueItem::Str(("a".into(), 1)))]).is_err());
    }

    #[test]
    fn test_concat_rejects_wildcard_arguments() {
        let err = Concat::new(vec![FuncArgs::Wildcard, literal_arg(ValueItem::Integer(1))]).unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedFeature(_)));
    }

    #[test]
    fn test_concat_is_not_an_aggregate() {
        let c = Concat::new(args2(ValueItem::Str(("a".into(), 1)), ValueItem::Str(("b".into(), 1)))).unwrap();
        assert!(!c.is_aggregate());
    }
}
