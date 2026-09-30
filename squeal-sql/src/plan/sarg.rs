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

/// The values one column may take: a set of points (`x = 1`, `x IN (1, 3)`
/// — sorted, distinct, never NULL; empty when nothing can match) or a range.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColumnSet {
    Points(Vec<ValueItem>),
    Range(ColumnRange),
}

impl ColumnSet {
    fn from_range(r: ColumnRange) -> ColumnSet {
        if let Some(v) = r.equal_value() {
            return ColumnSet::Points(vec![v.clone()]);
        }
        // `x = 0` on a double column: both zeros (see double_column_range).
        if let (Bound::Included(ValueItem::Double(a)), Bound::Included(ValueItem::Double(b))) =
            (&r.lower, &r.upper)
            && *a == 0.0
            && *b == 0.0
        {
            return ColumnSet::points(vec![ValueItem::Double(-0.0), ValueItem::Double(0.0)]);
        }
        let as_key = KeyRange {
            prefix: vec![],
            lower: r.lower.clone(),
            upper: r.upper.clone(),
        };
        if is_empty(&as_key) {
            return ColumnSet::Points(vec![]);
        }
        ColumnSet::Range(r)
    }

    fn points(mut v: Vec<ValueItem>) -> ColumnSet {
        v.sort();
        v.dedup_by(|a, b| Ord::cmp(&*a, &*b) == Ordering::Equal);
        ColumnSet::Points(v)
    }

    fn intersect(self, other: ColumnSet) -> ColumnSet {
        use ColumnSet::*;
        match (self, other) {
            (Points(a), Points(b)) => Points(
                a.into_iter()
                    .filter(|x| b.iter().any(|y| x.cmp(y) == Ordering::Equal))
                    .collect(),
            ),
            (Points(p), Range(r)) | (Range(r), Points(p)) => {
                Points(p.into_iter().filter(|v| r.contains(v)).collect())
            }
            (Range(a), Range(b)) => ColumnSet::from_range(a.intersect(b)),
        }
    }
}

impl ColumnRange {
    fn contains(&self, v: &ValueItem) -> bool {
        let above = match &self.lower {
            Bound::Included(lo) => v >= lo,
            Bound::Excluded(lo) => v > lo,
            Bound::Unbounded => true,
        };
        let below = match &self.upper {
            Bound::Included(hi) => v <= hi,
            Bound::Excluded(hi) => v < hi,
            Bound::Unbounded => true,
        };
        above && below
    }
}

/// Every column (position within `types`, the row the filters read) that
/// the conditions limit, and to what. `filters` are the WHERE conjuncts
/// local to one table (see optim::picker::ItemNeeds::filters).
pub(crate) fn column_sets(filters: &[EvalExpr], types: &[DataType]) -> HashMap<usize, ColumnSet> {
    let mut sets: HashMap<usize, ColumnSet> = HashMap::new();
    for f in filters {
        let Some((column, set)) = condition_set(f, types) else {
            continue;
        };
        let merged = match sets.remove(&column) {
            Some(existing) => existing.intersect(set),
            None => set,
        };
        sets.insert(column, merged);
    }
    sets
}

/// The column a WHERE conjunct limits, and to what — a comparison with a
/// constant, or an OR of equalities on one column (what `IN` becomes).
/// None when it can't be used for a seek (see the module comment).
pub(crate) fn condition_set(f: &EvalExpr, types: &[DataType]) -> Option<(usize, ColumnSet)> {
    let mut terms = vec![];
    or_terms(f, &mut terms);
    if let [single] = terms.as_slice() {
        let (column, op, value) = column_comparison(single)?;
        let range = condition_range(*types.get(column)?, op, &value)?;
        return Some((column, ColumnSet::from_range(range)));
    }
    let mut column = None;
    let mut points = vec![];
    for t in terms {
        let (c, op, value) = column_comparison(t)?;
        if op != BinaryOp::Eq || column.is_some_and(|col| col != c) {
            return None;
        }
        column = Some(c);
        let range = condition_range(*types.get(c)?, op, &value)?;
        match ColumnSet::from_range(range) {
            ColumnSet::Points(p) => points.extend(p),
            ColumnSet::Range(_) => return None,
        }
    }
    Some((column?, ColumnSet::points(points)))
}

/// The stored values of a `datatype` column equal to `value`, by the
/// evaluator's `=`: none for NULL (or a double constant no integer equals),
/// both zeros for a double zero. None when the types can't be compared (see
/// the module comment).
pub(crate) fn equal_points(datatype: DataType, value: &ValueItem) -> Option<Vec<ValueItem>> {
    match ColumnSet::from_range(condition_range(datatype, BinaryOp::Eq, value)?) {
        ColumnSet::Points(p) => Some(p),
        ColumnSet::Range(_) => None,
    }
}

fn or_terms<'a>(e: &'a EvalExpr, out: &mut Vec<&'a EvalExpr>) {
    match e {
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::Or,
            rhs,
        } => {
            or_terms(lhs, out);
            or_terms(rhs, out);
        }
        other => out.push(other),
    }
}

/// At most this many key ranges from one seek; a longer product of IN
/// lists stops using further key columns.
const MAX_RANGES: usize = 256;

/// The key ranges covering every row the sets allow, for a key made of
/// `key_columns` (positions, in key order): each combination of the points
/// on its leading columns, then the range on the next one — ascending and
/// non-overlapping. Also how many key columns that used: every condition on
/// those columns is exactly what the ranges read. None when the conditions
/// don't limit the key's first column at all; no ranges when nothing can
/// match.
pub(crate) fn key_ranges(
    key_columns: &[usize],
    sets: &HashMap<usize, ColumnSet>,
) -> Option<(Vec<KeyRange>, usize)> {
    let mut prefixes: Vec<Vec<ValueItem>> = vec![vec![]];
    let mut used = 0;
    for c in key_columns {
        match sets.get(c) {
            None => break,
            Some(ColumnSet::Points(points)) => {
                if prefixes.len() * points.len() > MAX_RANGES {
                    break;
                }
                prefixes = prefixes
                    .iter()
                    .flat_map(|p| {
                        points.iter().map(move |v| {
                            let mut p = p.clone();
                            p.push(v.clone());
                            p
                        })
                    })
                    .collect();
                used += 1;
                if prefixes.is_empty() {
                    break;
                }
            }
            Some(ColumnSet::Range(r)) => {
                let ranges = prefixes
                    .into_iter()
                    .map(|prefix| KeyRange {
                        prefix,
                        lower: r.lower.clone(),
                        upper: r.upper.clone(),
                    })
                    .collect();
                return Some((ranges, used + 1));
            }
        }
    }
    if used == 0 {
        return None;
    }
    Some((prefixes.into_iter().map(KeyRange::prefix).collect(), used))
}

/// The seek's condition for EXPLAIN — `city IN ('a', 'b') AND n > 5` —
/// given the key's column names, in key order. `ranges` is what key_ranges
/// produced: every combination of some values on the leading columns, all
/// with the same bounds on the next.
pub(crate) fn describe_key_ranges(ranges: &[KeyRange], key_names: &[String]) -> String {
    let Some(first) = ranges.first() else {
        return "matches nothing".into();
    };
    // Read for NULLS LAST (see optim::picker): a column's non-NULL values,
    // then its NULLs.
    if let [values, nulls] = ranges
        && values.lower == Bound::Excluded(ValueItem::Null)
        && values.upper == Bound::Unbounded
        && nulls.prefix.len() == values.prefix.len() + 1
        && nulls.prefix[..values.prefix.len()] == values.prefix[..]
        && nulls.prefix.last() == Some(&ValueItem::Null)
    {
        let fixed = describe_key_range(&KeyRange::prefix(values.prefix.clone()), key_names);
        return if fixed.is_empty() {
            "in index order, NULLs last".into()
        } else {
            format!("{fixed}, NULLs last")
        };
    }
    if ranges.len() == 1 {
        return describe_key_range(first, key_names);
    }
    let lit = |v: &ValueItem| EvalExpr::Literal(v.clone()).describe(&[]);
    let name = |i: usize| key_names.get(i).cloned().unwrap_or_else(|| format!("#{i}"));
    let mut parts = vec![];
    for i in 0..first.prefix.len() {
        let mut values: Vec<&ValueItem> = vec![];
        for r in ranges {
            if !values
                .iter()
                .any(|v| v.cmp(&&r.prefix[i]) == Ordering::Equal)
            {
                values.push(&r.prefix[i]);
            }
        }
        parts.push(match values.as_slice() {
            [v] => format!("{} = {}", name(i), lit(v)),
            vs => format!(
                "{} IN ({})",
                name(i),
                vs.iter().map(|v| lit(v)).collect::<Vec<_>>().join(", ")
            ),
        });
    }
    let bounds = describe_key_range(
        &KeyRange {
            prefix: vec![],
            lower: first.lower.clone(),
            upper: first.upper.clone(),
        },
        &key_names[first.prefix.len().min(key_names.len())..],
    );
    if !bounds.is_empty() {
        parts.push(bounds);
    }
    parts.join(" AND ")
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
    fn or(l: EvalExpr, r: EvalExpr) -> EvalExpr {
        EvalExpr::Binary {
            lhs: Box::new(l),
            op: BinaryOp::Or,
            rhs: Box::new(r),
        }
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

    fn sets(filters: &[EvalExpr]) -> HashMap<usize, ColumnSet> {
        column_sets(filters, &TYPES)
    }
    fn range(lower: Bound<ValueItem>, upper: Bound<ValueItem>) -> ColumnSet {
        ColumnSet::Range(ColumnRange { lower, upper })
    }
    fn pts(v: Vec<ValueItem>) -> ColumnSet {
        ColumnSet::Points(v)
    }
    use Bound::{Excluded, Included, Unbounded};

    #[test]
    fn test_each_comparison_and_either_operand_order() {
        let r = sets(&[cond(col(0), BinaryOp::Gt, lit(int(5)))]);
        assert_eq!(r[&0], range(Excluded(int(5)), Unbounded));
        // 5 > x is x < 5, and excludes NULL.
        let r = sets(&[cond(lit(int(5)), BinaryOp::Gt, col(0))]);
        assert_eq!(r[&0], range(NOT_NULL, Excluded(int(5))));
        let r = sets(&[cond(col(1), BinaryOp::Eq, lit(s("x")))]);
        assert_eq!(r[&1], pts(vec![s("x")]));
        // Negative literals.
        let neg = Box::new(EvalExpr::Unary {
            op: UnaryOp::Minus,
            field: lit(int(3)),
        });
        let r = sets(&[cond(col(0), BinaryOp::GtEq, neg)]);
        assert_eq!(r[&0], range(Included(int(-3)), Unbounded));
    }

    #[test]
    fn test_conditions_on_one_column_intersect() {
        let r = sets(&[
            cond(col(0), BinaryOp::Gt, lit(int(5))),
            cond(col(0), BinaryOp::GtEq, lit(int(5))),
            cond(col(0), BinaryOp::LtEq, lit(int(9))),
            cond(col(0), BinaryOp::Lt, lit(int(20))),
        ]);
        assert_eq!(r[&0], range(Excluded(int(5)), Included(int(9))));
        // Contradictions leave nothing.
        let r = sets(&[
            cond(col(0), BinaryOp::Eq, lit(int(1))),
            cond(col(0), BinaryOp::Eq, lit(int(2))),
        ]);
        assert_eq!(r[&0], pts(vec![]));
        let r = sets(&[
            cond(col(0), BinaryOp::Gt, lit(int(5))),
            cond(col(0), BinaryOp::Lt, lit(int(3))),
        ]);
        assert_eq!(r[&0], pts(vec![]));
    }

    #[test]
    fn test_an_or_of_equalities_is_a_set_of_points() {
        // x IN (3, 1, 3)
        let r = sets(&[or(
            or(
                cond(col(0), BinaryOp::Eq, lit(int(3))),
                cond(col(0), BinaryOp::Eq, lit(int(1))),
            ),
            cond(col(0), BinaryOp::Eq, lit(int(3))),
        )]);
        assert_eq!(r[&0], pts(vec![int(1), int(3)]));
        // ... AND x > 2
        let r = sets(&[
            or(
                cond(col(0), BinaryOp::Eq, lit(int(3))),
                cond(col(0), BinaryOp::Eq, lit(int(1))),
            ),
            cond(col(0), BinaryOp::Gt, lit(int(2))),
        ]);
        assert_eq!(r[&0], pts(vec![int(3)]));
        // x IN (1, NULL): NULL matches nothing
        let r = sets(&[or(
            cond(col(0), BinaryOp::Eq, lit(int(1))),
            cond(col(0), BinaryOp::Eq, lit(ValueItem::Null)),
        )]);
        assert_eq!(r[&0], pts(vec![int(1)]));
        // Not usable: two columns, or a term that isn't an equality.
        assert!(
            sets(&[or(
                cond(col(0), BinaryOp::Eq, lit(int(1))),
                cond(col(3), BinaryOp::Eq, lit(int(1))),
            )])
            .is_empty()
        );
        assert!(
            sets(&[or(
                cond(col(0), BinaryOp::Eq, lit(int(1))),
                cond(col(0), BinaryOp::Gt, lit(int(9))),
            )])
            .is_empty()
        );
    }

    #[test]
    fn test_unusable_conditions_are_left_out() {
        let r = sets(&[
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
        let r = sets(&[cond(col(0), BinaryOp::Eq, lit(ValueItem::Null))]);
        assert_eq!(r[&0], pts(vec![]));
    }

    #[test]
    fn test_an_integer_column_against_doubles_is_exact() {
        let one = |op, d| sets(&[cond(col(0), op, lit(dbl(d)))])[&0].clone();
        assert_eq!(one(BinaryOp::Eq, 5.0), pts(vec![int(5)]));
        assert_eq!(one(BinaryOp::Eq, 5.5), pts(vec![]));
        assert_eq!(one(BinaryOp::Gt, 5.5), range(Included(int(6)), Unbounded));
        assert_eq!(one(BinaryOp::Gt, 5.0), range(Excluded(int(5)), Unbounded));
        assert_eq!(one(BinaryOp::Lt, 5.5), range(NOT_NULL, Included(int(5))));
        assert_eq!(one(BinaryOp::Lt, -5.5), range(NOT_NULL, Included(int(-6))));
        assert_eq!(one(BinaryOp::GtEq, 1e19), pts(vec![]));
        assert_eq!(one(BinaryOp::LtEq, 1e19), range(NOT_NULL, Unbounded));
    }

    #[test]
    fn test_a_double_column_covers_both_zeros() {
        let one = |op, d| sets(&[cond(col(2), op, lit(dbl(d)))])[&2].clone();
        assert_eq!(one(BinaryOp::Eq, 0.0), pts(vec![dbl(-0.0), dbl(0.0)]));
        assert_eq!(one(BinaryOp::Eq, -0.0), pts(vec![dbl(-0.0), dbl(0.0)]));
        assert_eq!(
            one(BinaryOp::Gt, -0.0),
            range(Excluded(dbl(0.0)), Unbounded)
        );
        assert_eq!(one(BinaryOp::Lt, 0.0), range(NOT_NULL, Excluded(dbl(-0.0))));
        // An integer constant converts only when exact.
        let r = sets(&[cond(col(2), BinaryOp::Eq, lit(int(3)))]);
        assert_eq!(r[&2], pts(vec![dbl(3.0)]));
        assert!(sets(&[cond(col(2), BinaryOp::Eq, lit(int(i64::MAX)))]).is_empty());
    }

    #[test]
    fn test_key_ranges_take_leading_points_then_one_range() {
        let r = sets(&[
            cond(col(0), BinaryOp::Eq, lit(int(1))),
            cond(col(1), BinaryOp::Gt, lit(s("m"))),
            cond(col(3), BinaryOp::Eq, lit(int(7))),
        ]);
        // key (0, 1, 3): 0 is a point, 1 a range, and 3 after a range is unused
        assert_eq!(
            key_ranges(&[0, 1, 3], &r),
            Some((
                vec![KeyRange {
                    prefix: vec![int(1)],
                    lower: Excluded(s("m")),
                    upper: Unbounded
                }],
                2
            ))
        );
        assert_eq!(
            key_ranges(&[0, 3], &r),
            Some((vec![KeyRange::prefix(vec![int(1), int(7)])], 2))
        );
        assert_eq!(
            key_ranges(&[3, 0], &r),
            Some((vec![KeyRange::prefix(vec![int(7), int(1)])], 2))
        );
        assert_eq!(key_ranges(&[2, 0], &r), None);
    }

    #[test]
    fn test_key_ranges_expand_every_combination_in_order() {
        let r = HashMap::from([
            (0, pts(vec![int(1), int(2)])),
            (3, pts(vec![int(7), int(8)])),
        ]);
        let (ranges, used) = key_ranges(&[0, 3], &r).unwrap();
        assert_eq!(used, 2);
        let prefixes: Vec<_> = ranges.iter().map(|k| k.prefix.clone()).collect();
        assert_eq!(
            prefixes,
            vec![
                vec![int(1), int(7)],
                vec![int(1), int(8)],
                vec![int(2), int(7)],
                vec![int(2), int(8)],
            ]
        );
        assert_eq!(
            describe_key_ranges(&ranges, &["a".into(), "b".into()]),
            "a IN (1, 2) AND b IN (7, 8)"
        );
        // Read for NULLS LAST.
        let split = vec![
            KeyRange {
                prefix: vec![int(1)],
                lower: NOT_NULL,
                upper: Unbounded,
            },
            KeyRange::prefix(vec![int(1), ValueItem::Null]),
        ];
        assert_eq!(
            describe_key_ranges(&split, &["a".into(), "b".into()]),
            "a = 1, NULLs last"
        );
        // Nothing can match: no ranges at all.
        let r = HashMap::from([(0, pts(vec![]))]);
        assert_eq!(key_ranges(&[0], &r), Some((vec![], 1)));
        assert_eq!(describe_key_ranges(&[], &["a".into()]), "matches nothing");
        // Past MAX_RANGES the next column isn't used.
        let many: Vec<_> = (0..300).map(int).collect();
        let r = HashMap::from([(0, pts(vec![int(1), int(2)])), (3, pts(many))]);
        let (ranges, used) = key_ranges(&[0, 3], &r).unwrap();
        assert_eq!((ranges.len(), used), (2, 1));
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
        let both = vec![
            KeyRange {
                prefix: vec![int(1)],
                lower: Excluded(int(5)),
                upper: Unbounded,
            },
            KeyRange {
                prefix: vec![int(2)],
                lower: Excluded(int(5)),
                upper: Unbounded,
            },
        ];
        assert_eq!(
            describe_key_ranges(&both, &["a".into(), "b".into()]),
            "a IN (1, 2) AND b > 5"
        );
    }
}
