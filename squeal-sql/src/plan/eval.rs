use sql_parser::{
    Expr,
    expr::{BinaryOp, FunctionArg, UnaryOp},
};
use store::{
    db::DBFile,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::{
        funcs::{FuncArgs, FuncObj},
        logical::TableQuery,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CrateValueItem(ValueItem);

pub(crate) struct ExprWrapper<'a, F: DBFile + 'static> {
    pub(crate) expr: &'a Expr,
    pub(crate) tables: &'a [TableQuery<F>],
}

#[derive(Debug, Clone)]
pub enum EvalExpr {
    Literal(ValueItem),
    // A flat offset into UnionJoin's single combined row (see its own
    // extend_from_slice loop) — not a (table_id, field_id) pair. Every
    // source's fields get concatenated in FROM-clause order into one
    // IndexKey, so `data` handed to eval() below is always length 1
    // regardless of how many tables are in play; resolving a column
    // reference to its absolute position has to happen once, at build
    // time in from_expr/validate_field (where each table's own field
    // count is still known), not here.
    Value(usize),
    Unary {
        op: UnaryOp,
        field: Box<EvalExpr>,
    },
    Binary {
        lhs: Box<EvalExpr>,
        op: BinaryOp,
        rhs: Box<EvalExpr>,
    },
    Function(FuncObj),
}

impl EvalExpr {
    pub(crate) fn has_aggregate(&self) -> bool {
        match self {
            Self::Unary { field, .. } => field.has_aggregate(),
            Self::Binary { lhs, rhs, .. } => lhs.has_aggregate() || rhs.has_aggregate(),
            Self::Function(f) => f.is_aggregate(),
            _ => false,
        }
    }
    pub(crate) fn eval(&self, data: &[IndexKey], _index: usize) -> Result<ValueItem, SchemaError> {
        let v = match self {
            Self::Literal(v) => v,
            Self::Value(pos) => &data[0][*pos],
            Self::Unary { op, field } => {
                let v = field.eval(data, _index)?;
                &CrateValueItem::unary(&v, op)?
            }
            Self::Binary { lhs, op, rhs } => {
                let lhs = lhs.eval(data, _index)?;
                let rhs = rhs.eval(data, _index)?;
                &CrateValueItem::binary(&lhs, &rhs, op)?
            }
            // Not implemented yet — function evaluation (COUNT, ...) is
            // its own separate, unfinished feature.
            Self::Function { .. } => todo!(),
        };
        Ok(v.clone())
    }

    // Builds just the evaluation tree for `expr` — no display name, no
    // Field/source_id/field_id bookkeeping. Those only mean something for
    // a top-level SELECT-list item (see plan::logical::QueryVisitor::
    // handle_expr, the one place that wraps a returned EvalExpr in a
    // ProjectedField), not for every nested sub-expression a Unary/Binary
    // node carries — a `1` inside `a+1` has no name of its own to display.
    pub(crate) fn from_expr<F: DBFile + 'static>(
        expr: &Expr,
        tables: &[TableQuery<F>],
    ) -> Result<Box<EvalExpr>, SchemaError> {
        let eval_expr = match expr {
            Expr::Unary { op, expr } => Self::Unary {
                op: *op,
                field: Self::from_expr(expr, tables)?,
            },
            Expr::Binary { left, op, right } => Self::Binary {
                lhs: Self::from_expr(left, tables)?,
                op: *op,
                rhs: Self::from_expr(right, tables)?,
            },
            Expr::Literal(l) => match l {
                sql_parser::literal::Literal::Boolean(b) => {
                    Self::Literal(ValueItem::Boolean(b.value()))
                }
                sql_parser::literal::Literal::Null(_) => Self::Literal(ValueItem::Null),
                sql_parser::literal::Literal::String(s) => {
                    Self::Literal(ValueItem::Str((s.value.clone(), s.value.len() as u32)))
                }
                sql_parser::literal::Literal::Number(f) => match f.value {
                    sql_parser::literal::NumberValue::Float(f) => {
                        Self::Literal(ValueItem::Double(f))
                    }
                    sql_parser::literal::NumberValue::Integer(i) => {
                        Self::Literal(ValueItem::Integer(i))
                    }
                },
            },
            Expr::Column(c) => {
                let idents = c.idents().collect::<Vec<_>>();
                if idents.len() > 1 {
                    // A qualified column (`a.id`) is qualified by a FROM-item
                    // alias/name already in scope, not by a schema — unlike a
                    // table reference, so this can't go through
                    // Connection::resolve_object_name_ref (which only knows
                    // schema.table[.field] shapes and would treat `a` as a
                    // schema name, failing with SchemaNotFound). Only a
                    // single qualifier is supported for now (`alias.field`),
                    // matching what SelectItem::QualifiedWildcard's own
                    // `ob.len() == 1` case above supports.
                    let field = idents.last().unwrap().value.clone();
                    let qualifier = &idents[..idents.len() - 1];
                    if qualifier.len() != 1 {
                        return Err(SchemaError::UserError(format!(
                            "{:?} has too many parts",
                            c.to_dotted()
                        )));
                    }
                    let table_name = &qualifier[0].value;
                    let table_id = tables.iter().position(|t| {
                        table_name.eq_ignore_ascii_case(&t.alias)
                            || table_name.eq_ignore_ascii_case(&t.table)
                    });
                    let Some(table_id) = table_id else {
                        return Err(SchemaError::BadTableName(table_name.clone()));
                    };
                    let table = &tables[table_id];
                    let field_id = table
                        .fields
                        .iter()
                        .position(|f| f.name.eq_ignore_ascii_case(&field));
                    let Some(field_id) = field_id else {
                        return Err(SchemaError::FieldNotFound(field));
                    };
                    Self::Value(Self::flat_position(tables, table_id, field_id))
                } else {
                    let field = idents[0].value.clone();
                    Self::validate_field(&field, tables)?
                }
            }
            Expr::Function { .. } => {
                Self::Function(FuncObj::try_from(&ExprWrapper { expr, tables })?)
            }

            _ => panic!("Oops here {:?}", expr),
        };
        Ok(Box::new(eval_expr))
    }

    fn validate_field<F: DBFile + 'static>(
        field: &str,
        tables: &[TableQuery<F>],
    ) -> Result<EvalExpr, SchemaError> {
        let mut found = false;
        let mut fid = 0;
        let mut tid = 0;
        for (sid, t) in tables.iter().enumerate() {
            let f = t
                .fields
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(field));
            if f.is_some() && found {
                return Err(SchemaError::AmbiguousFieldError(field.into()));
            }
            found = f.is_some();
            if let Some(fd) = f {
                fid = fd;
                tid = sid;
            }
        }
        if !found {
            return Err(SchemaError::FieldNotFound(field.into()));
        }
        Ok(EvalExpr::Value(Self::flat_position(tables, tid, fid)))
    }

    // Where (table_id, field_id) actually lands in UnionJoin's combined
    // row: every field of table 0, then every field of table 1, and so
    // on (see UnionJoin::next's own extend_from_slice loop) — so it's
    // just the summed field count of every table before this one, plus
    // this table's own field_id.
    pub(crate) fn flat_position<F: DBFile + 'static>(
        tables: &[TableQuery<F>],
        table_id: usize,
        field_id: usize,
    ) -> usize {
        tables[..table_id]
            .iter()
            .map(|t| t.fields.len())
            .sum::<usize>()
            + field_id
    }
}

impl CrateValueItem {
    fn unary(item: &ValueItem, op: &UnaryOp) -> Result<ValueItem, SchemaError> {
        let v = match item {
            ValueItem::Blob(_) | ValueItem::Str(_) | ValueItem::Datetime(_) => {
                operand_error_msg(&format!("{:?}", op), " blob/string/date")?
            }
            ValueItem::Boolean(b) => {
                if let UnaryOp::Not = op {
                    ValueItem::Boolean(!b)
                } else {
                    operand_error_msg(&format!("{:?}", op), "boolean")?
                }
            }
            ValueItem::Double(d) => {
                if let UnaryOp::Minus = op {
                    ValueItem::Double(-d)
                } else if let UnaryOp::Plus = op {
                    ValueItem::Double(*d)
                } else {
                    operand_error_msg("not", "double")?
                }
            }
            ValueItem::Integer(d) => {
                if let UnaryOp::Minus = op {
                    ValueItem::Integer(-d)
                } else if let UnaryOp::Plus = op {
                    ValueItem::Integer(*d)
                } else {
                    operand_error_msg("not", "integer")?
                }
            }
            ValueItem::Null => ValueItem::Null,
        };
        Ok(v)
    }

    fn binary(lhs: &ValueItem, rhs: &ValueItem, op: &BinaryOp) -> Result<ValueItem, SchemaError> {
        // NULL propagates through every operator (matches unary's own
        // `ValueItem::Null => ValueItem::Null` passthrough) rather than
        // erroring or being compared as a value in its own right. This is
        // deliberately simpler than SQL's full three-valued AND/OR logic
        // (`NULL AND FALSE` is `FALSE` there, not `NULL`) — nothing else in
        // this engine has NULL-aware boolean logic yet (there's no WHERE
        // clause support at all), so that nuance is left for whenever that
        // lands rather than half-implemented here.
        if matches!(lhs, ValueItem::Null) || matches!(rhs, ValueItem::Null) {
            return Ok(ValueItem::Null);
        }
        match op {
            BinaryOp::Plus => numeric(
                lhs,
                rhs,
                op,
                i64::checked_add,
                |a, b| a + b,
                "integer overflow",
            ),
            BinaryOp::Minus => numeric(
                lhs,
                rhs,
                op,
                i64::checked_sub,
                |a, b| a - b,
                "integer overflow",
            ),
            BinaryOp::Multiply => numeric(
                lhs,
                rhs,
                op,
                i64::checked_mul,
                |a, b| a * b,
                "integer overflow",
            ),
            BinaryOp::Divide => numeric(
                lhs,
                rhs,
                op,
                i64::checked_div,
                |a, b| a / b,
                "division by zero",
            ),
            BinaryOp::Modulo => numeric(
                lhs,
                rhs,
                op,
                i64::checked_rem,
                |a, b| a % b,
                "division by zero",
            ),
            BinaryOp::Concat => match (lhs, rhs) {
                (ValueItem::Str((a, _)), ValueItem::Str((b, _))) => {
                    let s = format!("{a}{b}");
                    let len = s.len() as u32;
                    Ok(ValueItem::Str((s, len)))
                }
                _ => operand_error_msg(&format!("{op:?}"), "non-string operand"),
            },
            BinaryOp::Eq => Ok(ValueItem::Boolean(values_equal(lhs, rhs))),
            BinaryOp::NotEq => Ok(ValueItem::Boolean(!values_equal(lhs, rhs))),
            BinaryOp::Lt => Ok(ValueItem::Boolean(compare(lhs, rhs, op)?.is_lt())),
            BinaryOp::LtEq => Ok(ValueItem::Boolean(compare(lhs, rhs, op)?.is_le())),
            BinaryOp::Gt => Ok(ValueItem::Boolean(compare(lhs, rhs, op)?.is_gt())),
            BinaryOp::GtEq => Ok(ValueItem::Boolean(compare(lhs, rhs, op)?.is_ge())),
            BinaryOp::And => match (lhs, rhs) {
                (ValueItem::Boolean(a), ValueItem::Boolean(b)) => Ok(ValueItem::Boolean(*a && *b)),
                _ => operand_error_msg(&format!("{op:?}"), "non-boolean operand"),
            },
            BinaryOp::Or => match (lhs, rhs) {
                (ValueItem::Boolean(a), ValueItem::Boolean(b)) => Ok(ValueItem::Boolean(*a || *b)),
                _ => operand_error_msg(&format!("{op:?}"), "non-boolean operand"),
            },
        }
    }
}

// Shared by Plus/Minus/Multiply/Divide/Modulo: same-type Integer/Double
// pairs use their own op directly, a mixed Integer/Double pair promotes the
// Integer side to f64 (standard numeric promotion), anything else is a
// type error. Integer overflow and integer division/modulo by zero both
// come back as `None` from the `checked_*` ops (rather than panicking, the
// way plain `/`/`%` would) and get turned into the same
// InvalidOperationOnOperand error every other invalid-operand case here
// uses, with `int_err` supplying the reason. Double arithmetic never needs
// this: dividing by 0.0 yields IEEE-754 infinity/NaN rather than panicking,
// and ValueItem::Double already tolerates both (see store's own
// serialization tests).
fn numeric(
    lhs: &ValueItem,
    rhs: &ValueItem,
    op: &BinaryOp,
    int_op: impl Fn(i64, i64) -> Option<i64>,
    float_op: impl Fn(f64, f64) -> f64,
    int_err: &str,
) -> Result<ValueItem, SchemaError> {
    match (lhs, rhs) {
        (ValueItem::Integer(a), ValueItem::Integer(b)) => {
            int_op(*a, *b).map(ValueItem::Integer).ok_or_else(|| {
                SchemaError::InvalidOperationOnOperand(format!("{op:?}"), int_err.into())
            })
        }
        (ValueItem::Double(a), ValueItem::Double(b)) => Ok(ValueItem::Double(float_op(*a, *b))),
        (ValueItem::Integer(a), ValueItem::Double(b)) => {
            Ok(ValueItem::Double(float_op(*a as f64, *b)))
        }
        (ValueItem::Double(a), ValueItem::Integer(b)) => {
            Ok(ValueItem::Double(float_op(*a, *b as f64)))
        }
        _ => operand_error_msg(&format!("{op:?}"), "non-numeric operand"),
    }
}

// Eq/NotEq's own notion of equality — deliberately more lenient than
// `compare` below: comparing two genuinely incompatible types (a Str and an
// Integer, say) is `false`, not an error, matching how `==` behaves in most
// general-purpose languages. The one type-pair `compare` treats as
// comparable-with-promotion (mixed Integer/Double) is handled the same way
// here for consistency — `1 = 1.0` and `1 < 1.5` should agree on whether
// Integer/Double are the "same kind of thing", not use two different
// rules. Everything else falls back to ValueItem's own derived, structural
// (and panic-free) PartialEq.
fn values_equal(lhs: &ValueItem, rhs: &ValueItem) -> bool {
    match (lhs, rhs) {
        (ValueItem::Integer(a), ValueItem::Double(b)) => (*a as f64) == *b,
        (ValueItem::Double(a), ValueItem::Integer(b)) => *a == (*b as f64),
        // Content only, not the reserved on-disk capacity riding along
        // with it — same reason `compare` below (and ValueItem's own
        // PartialOrd) ignore it: a `'raj'` literal (capacity 3, its own
        // length) and a `raj` read out of a `varchar(10)` column
        // (capacity 10) are the same *value* despite derived PartialEq on
        // the whole tuple calling them unequal because 3 != 10.
        (ValueItem::Str((a, _)), ValueItem::Str((b, _))) => a == b,
        (ValueItem::Blob((a, _)), ValueItem::Blob((b, _))) => a == b,
        _ => lhs == rhs,
    }
}

// Lt/LtEq/Gt/GtEq's shared ordering logic. Unlike ValueItem's own
// PartialOrd (which panics on a Blob or on mismatched variants — see
// store::valueitem), this returns a proper Result: a query evaluator
// hitting a bad `<` in a WHERE clause should fail that statement, not crash
// the whole engine. Only the types that have an unambiguous order are
// handled (with the same Integer/Double promotion `numeric` and
// `values_equal` use); Blob, mismatched types, and anything else fall
// through to the same InvalidOperationOnOperand every other invalid case
// here produces.
fn compare(
    lhs: &ValueItem,
    rhs: &ValueItem,
    op: &BinaryOp,
) -> Result<std::cmp::Ordering, SchemaError> {
    match (lhs, rhs) {
        (ValueItem::Integer(a), ValueItem::Integer(b)) => Ok(a.cmp(b)),
        (ValueItem::Datetime(a), ValueItem::Datetime(b)) => Ok(a.cmp(b)),
        (ValueItem::Str((a, _)), ValueItem::Str((b, _))) => Ok(a.cmp(b)),
        (ValueItem::Boolean(a), ValueItem::Boolean(b)) => Ok(a.cmp(b)),
        (ValueItem::Double(a), ValueItem::Double(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| SchemaError::InvalidOperationOnOperand(format!("{op:?}"), "NaN".into())),
        (ValueItem::Integer(a), ValueItem::Double(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| SchemaError::InvalidOperationOnOperand(format!("{op:?}"), "NaN".into())),
        (ValueItem::Double(a), ValueItem::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| SchemaError::InvalidOperationOnOperand(format!("{op:?}"), "NaN".into())),
        _ => Err(SchemaError::InvalidOperationOnOperand(
            format!("{op:?}"),
            "mismatched or unorderable operand types".into(),
        )),
    }
}

fn operand_error_msg(v1: &str, v2: &str) -> Result<ValueItem, SchemaError> {
    Err(SchemaError::InvalidOperationOnOperand(v1.into(), v2.into()))
}
