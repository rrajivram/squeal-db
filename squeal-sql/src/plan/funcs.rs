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
            _ => Err(SchemaError::UnknownFunction(name.value.clone())),
        }
    }
}

impl FuncTrait for FuncObj {
    fn name(&self) -> String {
        match self {
            FuncObj::Count(c) => c.name(),
        }
    }

    fn eval(&mut self, args: &[IndexKey]) -> Result<ValueItem, SchemaError> {
        match self {
            FuncObj::Count(c) => c.eval(args),
        }
    }

    fn is_aggregate(&self) -> bool {
        match self {
            FuncObj::Count(c) => c.is_aggregate(),
        }
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        match self {
            FuncObj::Count(c) => c.reset(),
        }
    }

    fn fields(&self) -> Vec<usize> {
        match self {
            FuncObj::Count(c) => c.fields(),
        }
    }

    fn current(&self) -> ValueItem {
        match self {
            FuncObj::Count(c) => c.current(),
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
