// (Dead-code allowed module-wide: the accessors beyond `equi` — local_to,
// local_expr, and_all, ... — are the interface for pushing predicates down to
// scans and for cost-based join ordering. Nothing calls them until that
// lands; the tests below pin their behavior meanwhile.)
#![allow(dead_code)]

// Splits a WHERE expression into its AND-ed conjuncts and classifies each by
// which tables it touches. This is the raw material for planning decisions
// that used to be tangled into one expression evaluated over the whole
// combined row:
//   - Local(t): reads only table t. Can be evaluated as t is scanned (and
//     rebased to t's own row layout with `Conjunct::local_expr`).
//   - Equi: `t1.col = t2.col`, same type, different tables — a join edge.
//   - Multi: reads two or more tables but is not such an equality (a theta
//     condition, an OR across tables, a mixed-type comparison...). It can
//     only be evaluated once every table it names has been joined in.
//   - Constant: reads no columns (`1 = 1`).
//
// It works over the FLAT row every expression is resolved against: the
// tables' columns concatenated in FROM order (see EvalExpr::Value), so a
// table is just a `start..start + width` slice of positions. The analysis is
// pure (no schema or database), and does not change the plan by itself.
//
// Local is not the same as pushable. A predicate on a table that an outer
// join NULL-extends cannot be applied at that table's scan:
// `a LEFT JOIN b ON .. WHERE b.x > 50` drops the rows where b is missing,
// but filtering b first turns a's rows whose only matches fail the predicate
// into unmatched rows, which the join then emits NULL-extended. So each
// table carries whether some join in its chain NULL-extends it
// (`null_supplied_tables` computes that), and a Local conjunct is `pushable`
// only when its table is not. (A null-rejecting predicate on such a table
// could instead turn the outer join into an inner one; that is a further
// optimization, not done here.)
use sql_parser::expr::BinaryOp;

use crate::{datatype::DataType, plan::eval::EvalExpr, source::join::JoinType};

// What the analysis needs to know about one table of the FROM clause.
#[derive(Debug, Clone)]
pub(crate) struct TableShape {
    // Column types, in order.
    pub columns: Vec<DataType>,
    // Some join in this table's chain fills it with NULLs for unmatched rows
    // (see null_supplied_tables).
    pub null_supplied: bool,
}

// For one FROM item's chain `t0 J1 t1 J2 t2 ...` (joins given in order), which
// of its tables an outer join NULL-extends: LEFT NULL-extends the joined-in
// table, RIGHT everything joined so far, FULL both; INNER and CROSS neither.
// The result has one entry per table, `joins.len() + 1` of them.
pub(crate) fn null_supplied_tables(joins: &[JoinType]) -> Vec<bool> {
    let mut supplied = vec![false; joins.len() + 1];
    for (i, jt) in joins.iter().enumerate() {
        let joined_in = i + 1;
        match jt {
            JoinType::Left => supplied[joined_in] = true,
            JoinType::Right => supplied[..joined_in].fill(true),
            JoinType::Full => supplied[..=joined_in].fill(true),
            JoinType::Inner | JoinType::Cross => {}
        }
    }
    supplied
}

// One column of a table, by flat position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    pub table: usize,
    pub pos: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ConjunctKind {
    Constant,
    Local(usize),
    // `left` is the column of the lower-numbered table.
    Equi { left: ColumnRef, right: ColumnRef },
    Multi,
}

#[derive(Debug, Clone)]
pub(crate) struct Conjunct {
    pub expr: EvalExpr,
    // Every table the expression reads, ascending, without repeats.
    pub tables: Vec<usize>,
    pub kind: ConjunctKind,
    // May be applied at its table's scan, below any join, without changing the
    // query's result: a Local conjunct on a table no outer join NULL-extends,
    // or a Constant (true or false the same everywhere).
    pub pushable: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct Conjuncts {
    items: Vec<Conjunct>,
    // Flat position where each table's columns begin.
    starts: Vec<usize>,
}

impl Conjuncts {
    // A predicate over a position past every table is classified Multi (the
    // safe answer: it is only ever evaluated over the full combined row).
    pub(crate) fn analyze(where_expr: Option<&EvalExpr>, shapes: &[TableShape]) -> Self {
        let tables: Vec<&Vec<DataType>> = shapes.iter().map(|t| &t.columns).collect();
        let mut starts = Vec::with_capacity(tables.len());
        let mut next = 0;
        for t in &tables {
            starts.push(next);
            next += t.len();
        }
        let mut exprs = vec![];
        if let Some(w) = where_expr {
            split(w, &mut exprs);
        }
        let table_of = |pos: usize| -> Option<usize> {
            (0..tables.len()).find(|t| pos >= starts[*t] && pos < starts[*t] + tables[*t].len())
        };
        let items = exprs
            .into_iter()
            .map(|expr| {
                let positions = expr.column_positions();
                let touched: Option<Vec<usize>> = positions.iter().map(|p| table_of(*p)).collect();
                let (tables_read, kind) = match touched {
                    None => (vec![], ConjunctKind::Multi),
                    Some(mut ts) => {
                        ts.sort_unstable();
                        ts.dedup();
                        let kind = match ts.as_slice() {
                            [] => ConjunctKind::Constant,
                            [t] => ConjunctKind::Local(*t),
                            _ => equi_edge(expr, &starts, &tables)
                                .map(|(left, right)| ConjunctKind::Equi { left, right })
                                .unwrap_or(ConjunctKind::Multi),
                        };
                        (ts, kind)
                    }
                };
                let pushable = match &kind {
                    ConjunctKind::Constant => true,
                    ConjunctKind::Local(t) => !shapes[*t].null_supplied,
                    _ => false,
                };
                Conjunct {
                    expr: expr.clone(),
                    tables: tables_read,
                    kind,
                    pushable,
                }
            })
            .collect();
        Self { items, starts }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Conjunct> {
        self.items.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    // Flat position where `table`'s columns begin.
    pub(crate) fn table_start(&self, table: usize) -> usize {
        self.starts[table]
    }

    // Conjuncts that read only `table` — whether or not they may be applied
    // at its scan (see `pushable_to`).
    pub(crate) fn local_to(&self, table: usize) -> impl Iterator<Item = &Conjunct> {
        self.items
            .iter()
            .filter(move |c| c.kind == ConjunctKind::Local(table))
    }

    // The conjuncts that can be applied at `table`'s scan: empty for a table
    // an outer join NULL-extends.
    pub(crate) fn pushable_to(&self, table: usize) -> impl Iterator<Item = &Conjunct> {
        self.local_to(table).filter(|c| c.pushable)
    }

    // Every conjunct that must stay above the joins (the complement of what
    // can be pushed; Constants are counted as pushable).
    pub(crate) fn not_pushable(&self) -> impl Iterator<Item = &Conjunct> {
        self.items.iter().filter(|c| !c.pushable)
    }

    // The join edges: (conjunct, left column, right column).
    pub(crate) fn equi(&self) -> impl Iterator<Item = (&Conjunct, ColumnRef, ColumnRef)> {
        self.items.iter().filter_map(|c| match &c.kind {
            ConjunctKind::Equi { left, right } => Some((c, *left, *right)),
            _ => None,
        })
    }

    pub(crate) fn of_kind(&self, pred: impl Fn(&ConjunctKind) -> bool) -> impl Iterator<Item = &Conjunct> {
        self.items.iter().filter(move |c| pred(&c.kind))
    }

    // AND the given conjuncts back into one expression (None if there are
    // none) — e.g. the residual filter left after some were taken out.
    pub(crate) fn and_all<'a>(items: impl IntoIterator<Item = &'a Conjunct>) -> Option<EvalExpr> {
        items.into_iter().map(|c| c.expr.clone()).reduce(|l, r| EvalExpr::Binary {
            lhs: Box::new(l),
            op: BinaryOp::And,
            rhs: Box::new(r),
        })
    }
}

impl Conjunct {
    // This conjunct rewritten to read `table`'s OWN row (columns numbered
    // from 0) instead of the combined row, so it can be evaluated against
    // that table's rows directly. Only meaningful for Local conjuncts; None
    // for anything else, and for an expression containing a function call
    // (whose arguments are not rewritten yet).
    pub(crate) fn local_expr(&self, conjuncts: &Conjuncts) -> Option<EvalExpr> {
        let ConjunctKind::Local(t) = self.kind else {
            return None;
        };
        self.expr.shifted(conjuncts.table_start(t))
    }
}

// The top-level AND-chain of an expression, one entry per conjunct, in the
// order written.
fn split<'a>(expr: &'a EvalExpr, out: &mut Vec<&'a EvalExpr>) {
    match expr {
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => {
            split(lhs, out);
            split(rhs, out);
        }
        other => out.push(other),
    }
}

// `col = col` across two different tables whose columns have the same type.
// (Comparing different types is an error in the comparison itself, so such
// an equality is not a join edge.)
fn equi_edge(
    expr: &EvalExpr,
    starts: &[usize],
    tables: &[&Vec<DataType>],
) -> Option<(ColumnRef, ColumnRef)> {
    let EvalExpr::Binary {
        lhs,
        op: BinaryOp::Eq,
        rhs,
    } = expr
    else {
        return None;
    };
    let (EvalExpr::Value(a), EvalExpr::Value(b)) = (lhs.as_ref(), rhs.as_ref()) else {
        return None;
    };
    let locate = |pos: usize| -> Option<(ColumnRef, DataType)> {
        let t = (0..tables.len()).find(|t| pos >= starts[*t] && pos < starts[*t] + tables[*t].len())?;
        Some((ColumnRef { table: t, pos }, tables[t][pos - starts[t]]))
    };
    let ((ca, ta), (cb, tb)) = (locate(*a)?, locate(*b)?);
    if ca.table == cb.table || !same_type(&ta, &tb) {
        return None;
    }
    Some(if ca.table < cb.table { (ca, cb) } else { (cb, ca) })
}

// The same-type rule comparisons enforce (see plan::eval's same_type); a
// varchar(5) and a varchar(20) are both strings.
fn same_type(a: &DataType, b: &DataType) -> bool {
    matches!(
        (a, b),
        (DataType::Integer, DataType::Integer)
            | (DataType::Double, DataType::Double)
            | (DataType::Datetime, DataType::Datetime)
            | (DataType::Str(_), DataType::Str(_))
            | (DataType::Boolean, DataType::Boolean)
    )
}

#[cfg(test)]
mod tests {
    use store::valueitem::ValueItem;

    use super::*;

    fn v(p: usize) -> EvalExpr {
        EvalExpr::Value(p)
    }
    fn lit(i: i64) -> EvalExpr {
        EvalExpr::Literal(ValueItem::Integer(i))
    }
    fn bin(l: EvalExpr, op: BinaryOp, r: EvalExpr) -> EvalExpr {
        EvalExpr::Binary {
            lhs: Box::new(l),
            op,
            rhs: Box::new(r),
        }
    }
    fn and(l: EvalExpr, r: EvalExpr) -> EvalExpr {
        bin(l, BinaryOp::And, r)
    }

    // t0: (0 int, 1 int, 2 str)   t1: (3 int, 4 double)   t2: (5 int, 6 str(9))
    fn shape_with(null_supplied: [bool; 3]) -> Vec<TableShape> {
        [
            vec![DataType::Integer, DataType::Integer, DataType::Str(5)],
            vec![DataType::Integer, DataType::Double],
            vec![DataType::Integer, DataType::Str(9)],
        ]
        .into_iter()
        .zip(null_supplied)
        .map(|(columns, null_supplied)| TableShape { columns, null_supplied })
        .collect()
    }

    fn shape() -> Vec<TableShape> {
        shape_with([false; 3])
    }

    fn analyze(e: &EvalExpr) -> Conjuncts {
        Conjuncts::analyze(Some(e), &shape())
    }

    fn kinds(c: &Conjuncts) -> Vec<ConjunctKind> {
        c.iter().map(|c| c.kind.clone()).collect()
    }

    #[test]
    fn test_no_where_means_no_conjuncts() {
        let c = Conjuncts::analyze(None, &shape());
        assert!(c.is_empty());
        assert_eq!(Conjuncts::and_all(c.iter()).map(|_| ()), None);
    }

    #[test]
    fn test_the_and_chain_flattens_in_written_order_whatever_its_shape() {
        let (a, b, c, d) = (
            bin(v(0), BinaryOp::Gt, lit(1)),
            bin(v(1), BinaryOp::Lt, lit(2)),
            bin(v(3), BinaryOp::Eq, lit(3)),
            bin(v(5), BinaryOp::NotEq, lit(4)),
        );
        let left_deep = and(and(and(a.clone(), b.clone()), c.clone()), d.clone());
        let right_deep = and(a.clone(), and(b.clone(), and(c.clone(), d.clone())));
        for e in [left_deep, right_deep] {
            let cs = analyze(&e);
            assert_eq!(cs.len(), 4);
            assert_eq!(
                kinds(&cs),
                [
                    ConjunctKind::Local(0),
                    ConjunctKind::Local(0),
                    ConjunctKind::Local(1),
                    ConjunctKind::Local(2)
                ]
            );
        }
    }

    #[test]
    fn test_classification_of_each_shape() {
        let cases: Vec<(EvalExpr, ConjunctKind, Vec<usize>)> = vec![
            // constant
            (bin(lit(1), BinaryOp::Eq, lit(1)), ConjunctKind::Constant, vec![]),
            // local: a column against a literal, and two columns of one table
            (bin(v(0), BinaryOp::Gt, lit(5)), ConjunctKind::Local(0), vec![0]),
            (bin(v(0), BinaryOp::Eq, v(1)), ConjunctKind::Local(0), vec![0]),
            // a computed expression over one table is still local
            (
                bin(bin(v(3), BinaryOp::Plus, lit(1)), BinaryOp::Lt, lit(9)),
                ConjunctKind::Local(1),
                vec![1],
            ),
            // equi-join: same-type columns of two tables (either order)
            (
                bin(v(0), BinaryOp::Eq, v(3)),
                ConjunctKind::Equi {
                    left: ColumnRef { table: 0, pos: 0 },
                    right: ColumnRef { table: 1, pos: 3 },
                },
                vec![0, 1],
            ),
            (
                bin(v(5), BinaryOp::Eq, v(1)),
                ConjunctKind::Equi {
                    left: ColumnRef { table: 0, pos: 1 },
                    right: ColumnRef { table: 2, pos: 5 },
                },
                vec![0, 2],
            ),
            // strings of different varchar lengths are the same type
            (
                bin(v(2), BinaryOp::Eq, v(6)),
                ConjunctKind::Equi {
                    left: ColumnRef { table: 0, pos: 2 },
                    right: ColumnRef { table: 2, pos: 6 },
                },
                vec![0, 2],
            ),
            // multi-table but not an equi-join
            (bin(v(0), BinaryOp::Lt, v(3)), ConjunctKind::Multi, vec![0, 1]),
            (bin(bin(v(0), BinaryOp::Plus, lit(0)), BinaryOp::Eq, v(3)), ConjunctKind::Multi, vec![0, 1]),
            (
                bin(bin(v(0), BinaryOp::Eq, v(3)), BinaryOp::Or, bin(v(0), BinaryOp::Eq, lit(1))),
                ConjunctKind::Multi,
                vec![0, 1],
            ),
            // different types: not a join edge (the comparison itself errors)
            (bin(v(0), BinaryOp::Eq, v(4)), ConjunctKind::Multi, vec![0, 1]),
            (bin(v(2), BinaryOp::Eq, v(3)), ConjunctKind::Multi, vec![0, 1]),
            // three tables in one condition
            (
                bin(bin(v(0), BinaryOp::Plus, v(3)), BinaryOp::Eq, v(5)),
                ConjunctKind::Multi,
                vec![0, 1, 2],
            ),
        ];
        for (expr, kind, tables) in cases {
            let cs = analyze(&expr);
            assert_eq!(cs.len(), 1, "{expr:?}");
            let c = cs.iter().next().unwrap();
            assert_eq!(c.kind, kind, "{expr:?}");
            assert_eq!(c.tables, tables, "{expr:?}");
        }
    }

    #[test]
    fn test_an_or_or_not_over_one_table_is_one_local_conjunct_not_split() {
        let e = bin(bin(v(0), BinaryOp::Eq, lit(1)), BinaryOp::Or, bin(v(1), BinaryOp::Eq, lit(2)));
        let cs = analyze(&e);
        assert_eq!(kinds(&cs), [ConjunctKind::Local(0)]);
    }

    #[test]
    fn test_a_position_outside_every_table_is_conservatively_multi() {
        let cs = analyze(&bin(v(99), BinaryOp::Eq, lit(1)));
        assert_eq!(kinds(&cs), [ConjunctKind::Multi]);
    }

    #[test]
    fn test_accessors_group_conjuncts_by_kind() {
        let e = and(
            and(bin(v(0), BinaryOp::Gt, lit(1)), bin(v(0), BinaryOp::Eq, v(3))),
            and(bin(v(4), BinaryOp::Gt, EvalExpr::Literal(ValueItem::Double(1.0))), bin(v(0), BinaryOp::Lt, v(5))),
        );
        let cs = analyze(&e);
        assert_eq!(cs.local_to(0).count(), 1);
        assert_eq!(cs.local_to(1).count(), 1);
        assert_eq!(cs.local_to(2).count(), 0);
        let edges: Vec<_> = cs.equi().map(|(_, l, r)| (l.table, r.table)).collect();
        assert_eq!(edges, [(0, 1)]);
        assert_eq!(cs.of_kind(|k| *k == ConjunctKind::Multi).count(), 1);
    }

    #[test]
    fn test_and_all_recombines_the_conjuncts_it_is_given() {
        let e = and(
            and(bin(v(0), BinaryOp::Gt, lit(1)), bin(v(0), BinaryOp::Eq, v(3))),
            bin(v(5), BinaryOp::Lt, lit(9)),
        );
        let cs = analyze(&e);
        // Everything back together evaluates over the same columns, in order.
        let all = Conjuncts::and_all(cs.iter()).unwrap();
        assert_eq!(all.describe(&[]), e.describe(&[]));
        // Taking the join edge out leaves the other two.
        let residual = Conjuncts::and_all(cs.of_kind(|k| !matches!(k, ConjunctKind::Equi { .. }))).unwrap();
        assert_eq!(
            residual.describe(&[]),
            and(bin(v(0), BinaryOp::Gt, lit(1)), bin(v(5), BinaryOp::Lt, lit(9))).describe(&[])
        );
        assert!(Conjuncts::and_all(std::iter::empty()).is_none());
    }

    #[test]
    fn test_a_local_conjunct_can_be_rebased_to_its_own_tables_row() {
        // t1 starts at flat position 3: `#3 > 5 AND #4 < 2.0` becomes `#0 > 5 AND #1 < 2.0`.
        let e = and(
            bin(v(3), BinaryOp::Gt, lit(5)),
            bin(v(4), BinaryOp::Lt, EvalExpr::Literal(ValueItem::Double(2.0))),
        );
        let cs = analyze(&e);
        let rebased: Vec<String> = cs
            .local_to(1)
            .map(|c| c.local_expr(&cs).unwrap().describe(&[]))
            .collect();
        assert_eq!(rebased, ["(#0 > 5)", "(#1 < 2)"]);
    }

    #[test]
    fn test_only_local_conjuncts_can_be_rebased() {
        let cs = analyze(&bin(v(0), BinaryOp::Eq, v(3)));
        assert!(cs.iter().next().unwrap().local_expr(&cs).is_none());
    }

    // ---- outer joins: Local is not the same as pushable ----

    #[test]
    fn test_which_tables_each_join_type_null_extends() {
        use JoinType::*;
        let cases: Vec<(Vec<JoinType>, Vec<bool>)> = vec![
            (vec![], vec![false]),
            (vec![Inner], vec![false, false]),
            (vec![Cross], vec![false, false]),
            (vec![Left], vec![false, true]),
            (vec![Right], vec![true, false]),
            (vec![Full], vec![true, true]),
            // a later join only adds to what earlier ones marked
            (vec![Inner, Left], vec![false, false, true]),
            (vec![Left, Inner], vec![false, true, false]),
            (vec![Inner, Right], vec![true, true, false]),
            (vec![Left, Right], vec![true, true, false]),
            (vec![Right, Left], vec![true, false, true]),
            (vec![Full, Inner], vec![true, true, false]),
            (vec![Inner, Full], vec![true, true, true]),
        ];
        for (joins, want) in cases {
            assert_eq!(null_supplied_tables(&joins), want, "{joins:?}");
        }
    }

    #[test]
    fn test_a_local_conjunct_is_pushable_unless_its_table_is_null_supplied() {
        // one predicate on each of t0, t1, t2
        let e = and(
            and(bin(v(0), BinaryOp::Gt, lit(1)), bin(v(3), BinaryOp::Gt, lit(2))),
            bin(v(5), BinaryOp::Gt, lit(3)),
        );
        // t1 is NULL-extended (as by `t0 LEFT JOIN t1`)
        let cs = Conjuncts::analyze(Some(&e), &shape_with([false, true, false]));
        assert_eq!(cs.pushable_to(0).count(), 1);
        assert_eq!(cs.pushable_to(1).count(), 0, "local to t1, but t1 is null-supplied");
        assert_eq!(cs.local_to(1).count(), 1, "still classified Local");
        assert_eq!(cs.pushable_to(2).count(), 1);
        let stay: Vec<_> = cs.not_pushable().map(|c| c.kind.clone()).collect();
        assert_eq!(stay, [ConjunctKind::Local(1)]);
    }

    #[test]
    fn test_joins_and_multi_table_conditions_and_constants_pushability() {
        let e = and(
            and(bin(v(0), BinaryOp::Eq, v(3)), bin(v(0), BinaryOp::Lt, v(5))),
            bin(lit(1), BinaryOp::Eq, lit(1)),
        );
        let cs = Conjuncts::analyze(Some(&e), &shape_with([true, true, true]));
        let pushable: Vec<bool> = cs.iter().map(|c| c.pushable).collect();
        // an equi edge and a multi-table condition are never pushed to a scan;
        // a constant is the same anywhere, so it is pushable even here.
        assert_eq!(pushable, [false, false, true]);
    }

    #[test]
    fn test_a_predicate_stays_pushable_when_no_outer_join_touches_its_table() {
        let cs = Conjuncts::analyze(Some(&bin(v(3), BinaryOp::Gt, lit(1))), &shape());
        assert!(cs.iter().next().unwrap().pushable);
    }
}
