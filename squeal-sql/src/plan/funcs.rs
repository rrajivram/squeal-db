use std::{collections::HashSet, fmt::Debug, sync::Arc};

use sql_parser::{Expr, Ident, expr::FunctionArg};
use store::{
    db::DBFile,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::eval::{EvalExpr, ExprWrapper},
};

pub(crate) trait FuncTrait: Debug + Clone {
    fn name(&self) -> String;
    fn eval(&mut self, args: &[IndexKey], index: usize) -> Result<ValueItem, SchemaError>;
    fn is_aggregator() -> bool;
    fn reset(&mut self) -> Result<(), SchemaError>;
}

#[derive(Debug, Clone)]
pub(crate) enum FuncArgs {
    Wildcard,
    // A function argument is a sub-expression like any other Unary/Binary
    // operand — no display name of its own (see EvalExpr::from_expr's own
    // doc comment on why that lives one level up, not here).
    Field(Box<EvalExpr>),
}

#[derive(Debug, Clone)]
pub(crate) enum FuncObj {
    Count(Count),
}

impl FuncObj {
    pub(crate) fn is_aggregate(&self) -> bool {
        matches!(self, Self::Count(_))
    }
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
            Ok(Self::get_fn(name, distinct, func_args)?)
        } else {
            panic!("Shouldnt be here");
        }
    }
}

impl FuncObj {
    fn get_fn(name: &Ident, distinct: &bool, args: Vec<FuncArgs>) -> Result<Self, SchemaError> {
        match name.value.as_str() {
            "count" => Ok(Self::Count(Count::new(args, *distinct, None)?)),
            _ => Err(SchemaError::UnknownFunction(name.value.clone())),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Count {
    name: String,
    values: HashSet<IndexKey>,
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
    fn eval(&mut self, args: &[IndexKey], index: usize) -> Result<ValueItem, SchemaError> {
        if self.distinct {
            todo!()
        } else {
            self.count += 1;
        }
        todo!()
    }

    fn is_aggregator() -> bool {
        true
    }

    fn name(&self) -> String {
        "count".into()
    }

    fn reset(&mut self) -> Result<(), SchemaError> {
        Ok(())
    }
}
