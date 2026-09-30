//! Turning WHERE conditions into index seeks.
//!
//! A condition of the form `column op constant` (either way round, op one
//! of = < <= > >=) limits which values of that column can match. All such
//! conditions on one column combine into one ColumnRange; `key_range` then
//! turns the ranges on an index's (or the table's) key columns into a
//! store KeyRange: equalities on the longest run of leading key columns,
//! then at most one range on the next.
//!
//! A seek may only ever be WIDER than the condition, never narrower — the
//! full WHERE is still applied on top of it (see optim::picker). So a
//! condition is only used when its constant can be compared with the
//! column's stored values exactly as the evaluator would compare them:
//! same type, or an integer/double pair converted without rounding. A
//! mismatched type is an error at evaluation time, which a seek would hide
//! by never producing the rows; such a condition is left out.

use std::{cmp::Ordering, collections::HashMap, ops::Bound};

use sql_parser::expr::{BinaryOp, UnaryOp};
use store::{cursor::KeyRange, valueitem::ValueItem};

use crate::{datatype::DataType, plan::eval::EvalExpr};

/// The values of one column that can satisfy every usable condition on it.
/// NULL is the lowest value in the key order, and no comparison is true of
/// it, so any condition makes the lower bound at least `Excluded(NULL)`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnRange {
    pub lower: Bound<ValueItem>,
    pub upper: Bound<ValueItem>,
}

impl ColumnRange {
    // `column = v`, with lower == upper == Included(v).
    fn equal_value(&self) -> Option<&ValueItem> {
        match (&self.lower, &self.upper) {
            (Bound::Included(a), Bound::Included(b)) if a.cmp(b) == Ordering::Equal => Some(a),
            _ => None,
        }
    }

    fn intersect(self, other: ColumnRange) -> ColumnRange {
        ColumnRange {
            lower: tighter(self.lower, other.lower, Ordering::Greater),
            upper: tighter(self.upper, other.upper, Ordering::Less),
        }
    }
}

// The more restrictive of two bounds on the same side: `wins` is the
// ordering of the value that restricts more (Greater for a lower bound).
// On equal values Excluded is the more restrictive.
fn tighter(a: Bound<ValueItem>, b: Bound<ValueItem>, wins: Ordering) -> Bound<ValueItem> {
    use Bound::*;
    match (a, b) {
        (Unbounded, x) | (x, Unbounded) => x,
        (a, b) => {
            let (va, vb) = match (&a, &b) {
                (Included(x) | Excluded(x), Included(y) | Excluded(y)) => (x, y),
                _ => unreachable!(),
            };
            match va.cmp(vb) {
                Ordering::Equal => {
                    if matches!(a, Excluded(_)) {
                        a
                    } else {
                        b
                    }
                }
                o if o == wins => a,
                _ => b,
            }
        }
    }
}

/// Every column (position within `types`, the row the filters read) that
/// the conditions limit, and to what. `filters` are the WHERE conjuncts
/// local to one table (see optim::picker::ItemNeeds::filters).
pub(crate) fn column_ranges(
    filters: &[EvalExpr],
    types: &[DataType],
) -> HashMap<usize, ColumnRange> {
    let mut ranges: HashMap<usize, ColumnRange> = HashMap::new();
    for f in filters {
        let Some((column, op, value)) = column_comparison(f) else {
            continue;
        };
        let Some(datatype) = types.get(column) else {
            continue;
        };
        if let Some(range) = condition_range(*datatype, op, &value) {
            let merged = match ranges.remove(&column) {
                Some(existing) => existing.intersect(range),
                None => range,
            };
            ranges.insert(column, merged);
        }
    }
    ranges
}

/// The KeyRange covering every row the ranges allow, for a key made of
/// `key_columns` (positions, in key order): equalities on its leading
/// columns, then the range on the next one. None when the conditions don't
/// limit the key's first column at all.
pub(crate) fn key_range(
    key_columns: &[usize],
    ranges: &HashMap<usize, ColumnRange>,
) -> Option<KeyRange> {
    let mut prefix = vec![];
    for c in key_columns {
        let Some(range) = ranges.get(c) else {
            break;
        };
        match range.equal_value() {
            Some(v) => prefix.push(v.clone()),
            None => {
                return Some(KeyRange {
                    prefix,
                    lower: range.lower.clone(),
                    upper: range.upper.clone(),
                });
            }
        }
    }
    if prefix.is_empty() {
        return None;
    }
    Some(KeyRange::prefix(prefix))
}

/// True when no value can lie within the range's bounds (`x = 1 AND x = 2`,
/// or `x = 5.5` on an integer column).
pub(crate) fn is_empty(range: &KeyRange) -> bool {
    match (&range.lower, &range.upper) {
        (Bound::Included(lo), Bound::Included(hi)) => lo > hi,
        (Bound::Included(lo) | Bound::Excluded(lo), Bound::Included(hi) | Bound::Excluded(hi)) => {
            lo >= hi
        }
        _ => false,
    }
}

/// How many distinct integer (or datetime) values the range's bounds
/// allow, when both are such values — on a unique key, an upper limit on
/// the rows it can match.
pub(crate) fn values_in_range(range: &KeyRange) -> Option<u128> {
    let int = |v: &ValueItem| match v {
        ValueItem::Integer(i) => Some(*i as i128),
        ValueItem::Datetime(d) => Some(*d as i128),
        _ => None,
    };
    let lo = match &range.lower {
        Bound::Included(v) => int(v)?,
        Bound::Excluded(v) => int(v)? + 1,
        Bound::Unbounded => return None,
    };
    let hi = match &range.upper {
        Bound::Included(v) => int(v)?,
        Bound::Excluded(v) => int(v)? - 1,
        Bound::Unbounded => return None,
    };
    Some(if hi < lo { 0 } else { (hi - lo + 1) as u128 })
}

/// The seek's condition for EXPLAIN — `name = 'x' AND n > 5` — given the
/// key's column names, in key order.
pub(crate) fn describe_key_range(range: &KeyRange, key_names: &[String]) -> String {
    let lit = |v: &ValueItem| EvalExpr::Literal(v.clone()).describe(&[]);
    let name = |i: usize| key_names.get(i).cloned().unwrap_or_else(|| format!("#{i}"));
    if is_empty(range) {
        return format!("{} matches nothing", name(range.prefix.len()));
    }
    let mut parts: Vec<String> = range
        .prefix
        .iter()
        .enumerate()
        .map(|(i, v)| format!("{} = {}", name(i), lit(v)))
        .collect();
    let next = name(range.prefix.len());
    match &range.lower {
        Bound::Included(v) => parts.push(format!("{next} >= {}", lit(v))),
        Bound::Excluded(ValueItem::Null) => {
            if matches!(range.upper, Bound::Unbounded) {
                parts.push(format!("{next} IS NOT NULL"));
            }
        }
        Bound::Excluded(v) => parts.push(format!("{next} > {}", lit(v))),
        Bound::Unbounded => {}
    }
    match &range.upper {
        Bound::Included(v) => parts.push(format!("{next} <= {}", lit(v))),
        Bound::Excluded(v) => parts.push(format!("{next} < {}", lit(v))),
        Bound::Unbounded => {}
    }
    parts.join(" AND ")
}

/// `column op constant` (or `constant op column`, flipped) as (column
/// position, op, constant), for op one of = < <= > >=.
pub(crate) fn column_comparison(e: &EvalExpr) -> Option<(usize, BinaryOp, ValueItem)> {
    let EvalExpr::Binary { lhs, op, rhs } = e else {
        return None;
    };
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
    ) {
        return None;
    }
    match (lhs.as_ref(), rhs.as_ref()) {
        (EvalExpr::Value(c), other) => Some((*c, *op, constant(other)?)),
        (other, EvalExpr::Value(c)) => Some((*c, flipped(op)?, constant(other)?)),
        _ => None,
    }
}

// A constant operand: a literal, or a negated numeric literal (`-5`).
fn constant(e: &EvalExpr) -> Option<ValueItem> {
    match e {
        EvalExpr::Literal(v) => Some(v.clone()),
        EvalExpr::Unary {
            op: UnaryOp::Minus,
            field,
        } => match field.as_ref() {
            EvalExpr::Literal(ValueItem::Integer(i)) => i.checked_neg().map(ValueItem::Integer),
            EvalExpr::Literal(ValueItem::Double(d)) => Some(ValueItem::Double(-d)),
            _ => None,
        },
        _ => None,
    }
}

// `constant op column` as `column op' constant`.
fn flipped(op: &BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => return None,
    })
}

const NOT_NULL: Bound<ValueItem> = Bound::Excluded(ValueItem::Null);

fn empty() -> ColumnRange {
    ColumnRange {
        lower: NOT_NULL,
        upper: NOT_NULL,
    }
}

// The range `column op value` allows, in the column's stored type; None
// when the condition can't be used for a seek (see the module comment).
fn condition_range(datatype: DataType, op: BinaryOp, value: &ValueItem) -> Option<ColumnRange> {
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
    ) {
        return None;
    }
    // A comparison with NULL is never true.
    if matches!(value, ValueItem::Null) {
        return Some(empty());
    }
    match (datatype, value) {
        (DataType::Integer, ValueItem::Integer(_))
        | (DataType::Str(_), ValueItem::Str(_))
        | (DataType::Datetime, ValueItem::Datetime(_))
        | (DataType::Boolean, ValueItem::Boolean(_)) => Some(plain_range(op, value.clone())),
        (DataType::Integer, ValueItem::Double(d)) => integer_column_range(op, *d),
        (DataType::Double, ValueItem::Double(d)) if !d.is_nan() => {
            Some(double_column_range(op, *d))
        }
        (DataType::Double, ValueItem::Integer(i)) if i.unsigned_abs() <= 1 << 53 => {
            Some(double_column_range(op, *i as f64))
        }
        _ => None,
    }
}

fn plain_range(op: BinaryOp, v: ValueItem) -> ColumnRange {
    let (lower, upper) = match op {
        BinaryOp::Eq => (Bound::Included(v.clone()), Bound::Included(v)),
        BinaryOp::Lt => (NOT_NULL, Bound::Excluded(v)),
        BinaryOp::LtEq => (NOT_NULL, Bound::Included(v)),
        BinaryOp::Gt => (Bound::Excluded(v), Bound::Unbounded),
        BinaryOp::GtEq => (Bound::Included(v), Bound::Unbounded),
        _ => unreachable!("filtered by condition_range"),
    };
    ColumnRange { lower, upper }
}

// An integer column against a double constant: the same set of integers,
// as integer bounds (`x > 5.5` is `x >= 6`), so the seek compares like with
// like. The evaluator compares integer and double exactly (see
// crate::numeric), and so does this.
fn integer_column_range(op: BinaryOp, d: f64) -> Option<ColumnRange> {
    if d.is_nan() {
        // The evaluator errors on NaN; leave it to the scan to report.
        return None;
    }
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    let int = |x: f64| ValueItem::Integer(x as i64);
    let whole = d.fract() == 0.0;
    let range = match op {
        BinaryOp::Eq if whole && (-TWO_63..TWO_63).contains(&d) => plain_range(op, int(d)),
        BinaryOp::Eq => empty(),
        BinaryOp::Gt | BinaryOp::GtEq => {
            if d >= TWO_63 {
                empty()
            } else if d < -TWO_63 {
                ColumnRange {
                    lower: NOT_NULL,
                    upper: Bound::Unbounded,
                }
            } else if whole && op == BinaryOp::Gt {
                plain_range(BinaryOp::Gt, int(d))
            } else {
                plain_range(BinaryOp::GtEq, int(d.ceil()))
            }
        }
        BinaryOp::Lt | BinaryOp::LtEq => {
            if d < -TWO_63 {
                empty()
            } else if d >= TWO_63 {
                ColumnRange {
                    lower: NOT_NULL,
                    upper: Bound::Unbounded,
                }
            } else if whole && op == BinaryOp::Lt {
                plain_range(BinaryOp::Lt, int(d))
            } else {
                plain_range(BinaryOp::LtEq, int(d.floor()))
            }
        }
        _ => unreachable!("filtered by condition_range"),
    };
    Some(range)
}

// A double column. The key order is f64::total_cmp, which puts -0.0 before
// +0.0, while SQL compares them equal — so a bound at zero is widened to
// take in both zeros or narrowed to leave out both, whichever the condition
// means.
fn double_column_range(op: BinaryOp, d: f64) -> ColumnRange {
    let mut r = plain_range(op, ValueItem::Double(d));
    if d == 0.0 {
        let (neg, pos) = (ValueItem::Double(-0.0), ValueItem::Double(0.0));
        r.lower = match r.lower {
            Bound::Included(_) => Bound::Included(neg.clone()),
            Bound::Excluded(ValueItem::Double(_)) => Bound::Excluded(pos.clone()),
            other => other,
        };
        r.upper = match r.upper {
            Bound::Included(_) => Bound::Included(pos),
            Bound::Excluded(_) => Bound::Excluded(neg),
            other => other,
        };
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(i: usize) -> Box<EvalExpr> {
        Box::new(EvalExpr::Value(i))
    }
    fn lit(v: ValueItem) -> Box<EvalExpr> {
        Box::new(EvalExpr::Literal(v))
    }
    fn cond(l: Box<EvalExpr>, op: BinaryOp, r: Box<EvalExpr>) -> EvalExpr {
        EvalExpr::Binary { lhs: l, op, rhs: r }
    }
    fn int(i: i64) -> ValueItem {
        ValueItem::Integer(i)
    }
    fn dbl(d: f64) -> ValueItem {
        ValueItem::Double(d)
    }
    fn s(v: &str) -> ValueItem {
        ValueItem::Str((v.into(), 10))
    }
    const TYPES: [DataType; 4] = [
        DataType::Integer,
        DataType::Str(10),
        DataType::Double,
        DataType::Integer,
    ];

    fn ranges(filters: &[EvalExpr]) -> HashMap<usize, ColumnRange> {
        column_ranges(filters, &TYPES)
    }
    fn range(lower: Bound<ValueItem>, upper: Bound<ValueItem>) -> ColumnRange {
        ColumnRange { lower, upper }
    }
    use Bound::{Excluded, Included, Unbounded};

    #[test]
    fn test_each_comparison_and_either_operand_order() {
        let r = ranges(&[cond(col(0), BinaryOp::Gt, lit(int(5)))]);
        assert_eq!(r[&0], range(Excluded(int(5)), Unbounded));
        // 5 > x is x < 5, and excludes NULL.
        let r = ranges(&[cond(lit(int(5)), BinaryOp::Gt, col(0))]);
        assert_eq!(r[&0], range(NOT_NULL, Excluded(int(5))));
        let r = ranges(&[cond(col(1), BinaryOp::Eq, lit(s("x")))]);
        assert_eq!(r[&1], range(Included(s("x")), Included(s("x"))));
        // Negative literals.
        let neg = Box::new(EvalExpr::Unary {
            op: UnaryOp::Minus,
            field: lit(int(3)),
        });
        let r = ranges(&[cond(col(0), BinaryOp::GtEq, neg)]);
        assert_eq!(r[&0], range(Included(int(-3)), Unbounded));
    }

    #[test]
    fn test_conditions_on_one_column_intersect() {
        let r = ranges(&[
            cond(col(0), BinaryOp::Gt, lit(int(5))),
            cond(col(0), BinaryOp::GtEq, lit(int(5))),
            cond(col(0), BinaryOp::LtEq, lit(int(9))),
            cond(col(0), BinaryOp::Lt, lit(int(20))),
        ]);
        assert_eq!(r[&0], range(Excluded(int(5)), Included(int(9))));
    }

    #[test]
    fn test_unusable_conditions_are_left_out() {
        let r = ranges(&[
            // type mismatch: an evaluation error the scan must report
            cond(col(0), BinaryOp::Eq, lit(s("x"))),
            // not a comparison with a constant
            cond(col(0), BinaryOp::Eq, col(3)),
            cond(col(0), BinaryOp::NotEq, lit(int(1))),
            // NaN errors in the evaluator
            cond(col(2), BinaryOp::Gt, lit(dbl(f64::NAN))),
        ]);
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn test_null_matches_nothing() {
        let r = ranges(&[cond(col(0), BinaryOp::Eq, lit(ValueItem::Null))]);
        assert_eq!(r[&0], empty());
    }

    #[test]
    fn test_an_integer_column_against_doubles_is_exact() {
        let one = |op, d| ranges(&[cond(col(0), op, lit(dbl(d)))])[&0].clone();
        assert_eq!(
            one(BinaryOp::Eq, 5.0),
            range(Included(int(5)), Included(int(5)))
        );
        assert_eq!(one(BinaryOp::Eq, 5.5), empty());
        assert_eq!(one(BinaryOp::Gt, 5.5), range(Included(int(6)), Unbounded));
        assert_eq!(one(BinaryOp::Gt, 5.0), range(Excluded(int(5)), Unbounded));
        assert_eq!(one(BinaryOp::Lt, 5.5), range(NOT_NULL, Included(int(5))));
        assert_eq!(one(BinaryOp::Lt, -5.5), range(NOT_NULL, Included(int(-6))));
        assert_eq!(one(BinaryOp::GtEq, 1e19), empty());
        assert_eq!(one(BinaryOp::LtEq, 1e19), range(NOT_NULL, Unbounded));
    }

    #[test]
    fn test_a_double_column_covers_both_zeros() {
        let one = |op, d| ranges(&[cond(col(2), op, lit(dbl(d)))])[&2].clone();
        assert_eq!(
            one(BinaryOp::Eq, 0.0),
            range(Included(dbl(-0.0)), Included(dbl(0.0)))
        );
        assert_eq!(
            one(BinaryOp::Eq, -0.0),
            range(Included(dbl(-0.0)), Included(dbl(0.0)))
        );
        assert_eq!(
            one(BinaryOp::Gt, -0.0),
            range(Excluded(dbl(0.0)), Unbounded)
        );
        assert_eq!(one(BinaryOp::Lt, 0.0), range(NOT_NULL, Excluded(dbl(-0.0))));
        // An integer constant converts only when exact.
        let r = ranges(&[cond(col(2), BinaryOp::Eq, lit(int(3)))]);
        assert_eq!(r[&2], range(Included(dbl(3.0)), Included(dbl(3.0))));
        assert!(ranges(&[cond(col(2), BinaryOp::Eq, lit(int(i64::MAX)))]).is_empty());
    }

    #[test]
    fn test_key_range_takes_leading_equalities_then_one_range() {
        let r = ranges(&[
            cond(col(0), BinaryOp::Eq, lit(int(1))),
            cond(col(1), BinaryOp::Gt, lit(s("m"))),
            cond(col(3), BinaryOp::Eq, lit(int(7))),
        ]);
        // key (0, 1, 3): 0 is equal, 1 a range, and 3 after a range is unused
        assert_eq!(
            key_range(&[0, 1, 3], &r),
            Some(KeyRange {
                prefix: vec![int(1)],
                lower: Excluded(s("m")),
                upper: Unbounded
            })
        );
        // key (0, 3): both equalities
        assert_eq!(
            key_range(&[0, 3], &r),
            Some(KeyRange::prefix(vec![int(1), int(7)]))
        );
        // key (3, 0) likewise; key (2, ...) not limited by anything
        assert_eq!(
            key_range(&[3, 0], &r),
            Some(KeyRange::prefix(vec![int(7), int(1)]))
        );
        assert_eq!(key_range(&[2, 0], &r), None);
    }

    #[test]
    fn test_empty_ranges_and_counting_values() {
        let r = |lower, upper| KeyRange {
            prefix: vec![],
            lower,
            upper,
        };
        assert!(is_empty(&r(NOT_NULL, NOT_NULL)));
        assert!(is_empty(&r(Included(int(5)), Excluded(int(5)))));
        assert!(is_empty(&r(Included(int(6)), Included(int(5)))));
        assert!(!is_empty(&r(Included(int(5)), Included(int(5)))));
        assert!(!is_empty(&r(NOT_NULL, Excluded(int(5)))));
        assert_eq!(
            values_in_range(&r(Included(int(10)), Excluded(int(13)))),
            Some(3)
        );
        assert_eq!(
            values_in_range(&r(Excluded(int(10)), Excluded(int(11)))),
            Some(0)
        );
        assert_eq!(values_in_range(&r(Included(int(10)), Unbounded)), None);
        assert_eq!(
            describe_key_range(&r(NOT_NULL, NOT_NULL), &["id".into()]),
            "id matches nothing"
        );
    }

    #[test]
    fn test_describe() {
        let r = KeyRange {
            prefix: vec![int(1)],
            lower: Excluded(s("m")),
            upper: Included(s("q")),
        };
        assert_eq!(
            describe_key_range(&r, &["a".into(), "b".into()]),
            "a = 1 AND b > 'm' AND b <= 'q'"
        );
        let r = KeyRange {
            prefix: vec![],
            lower: NOT_NULL,
            upper: Unbounded,
        };
        assert_eq!(describe_key_range(&r, &["a".into()]), "a IS NOT NULL");
        let r = KeyRange {
            prefix: vec![],
            lower: NOT_NULL,
            upper: Excluded(int(3)),
        };
        assert_eq!(describe_key_range(&r, &["a".into()]), "a < 3");
    }
}
