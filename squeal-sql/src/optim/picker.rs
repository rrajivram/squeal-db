//! Choosing how to read each FROM item (see AccessPath): the whole table,
//! a range of its primary key, or all or a range of one of its indexes.
//!
//! Every path produces rows in the TABLE's layout — every column in table
//! order — so choosing one changes no column position anywhere else in the
//! plan (see IndexSource). An index that lacks some of the columns fills
//! them with NULL, which is never read: without a row lookup, an index is
//! only chosen when it holds every column the query reads from that item.
//! A seek applies only part of WHERE, and never all of it exactly, so the
//! whole WHERE is still applied on top.

use std::{collections::BTreeSet, ops::Bound};

use sql_parser::expr::BinaryOp;
use store::{cursor::KeyRange, db::DBFile, valueitem::ValueItem};

use crate::{
    datatype::DataType,
    optim::table_stats::{ColumnStat, ComputedTableStat},
    plan::{
        conjuncts::{Conjuncts, TableShape},
        eval::EvalExpr,
        funcs::{FuncArgs, FuncTrait},
        logical::TableQuery,
        sarg::{column_comparison, column_ranges, is_empty, key_range, values_in_range},
    },
    source::join::JoinType,
    table::SqlTable,
};

/// What one FROM item (one entry of the flattened table list — the base
/// table of each FROM item followed by its joined relations) has to
/// provide. Keyed by the item's position, not by table id, so the two
/// sides of a self-join (`t a JOIN t b`) are analyzed separately.
#[derive(Debug, Default, Clone)]
pub(crate) struct ItemNeeds {
    /// Every column of this item the query reads anywhere — SELECT list
    /// (hidden ORDER BY columns included), WHERE, GROUP BY, join ON
    /// conditions — as positions within the item's own row.
    pub columns: BTreeSet<usize>,
    /// The WHERE conjuncts that read only this item and may be applied
    /// before any join (see Conjunct::pushable), rewritten to positions
    /// within the item's own row — what a seek can use (see plan::sarg).
    pub filters: Vec<EvalExpr>,
}

/// One ItemNeeds per entry of `flat_tables`.
///
/// `exprs`: expressions whose column positions index the combined row of
/// every FROM item (`flat_tables` concatenated) — the SELECT list and
/// GROUP BY. `wh_expr` is in that same space. Each join's ON condition is
/// read from `tables`, where its positions start at its own FROM item's
/// base table (see QueryVisitor::get_tables).
pub(crate) fn analyze_query<F: DBFile + 'static>(
    tables: &[TableQuery<F>],
    flat_tables: &[TableQuery<F>],
    shapes: &[TableShape],
    exprs: &[&EvalExpr],
    wh_expr: Option<&EvalExpr>,
) -> Vec<ItemNeeds> {
    let mut starts = Vec::with_capacity(flat_tables.len());
    let mut next = 0;
    for t in flat_tables {
        starts.push(next);
        next += t.fields.len();
    }
    let mut needs = vec![ItemNeeds::default(); flat_tables.len()];
    let mut add = |pos: usize| {
        if let Some(item) = (0..starts.len())
            .rev()
            .find(|&i| starts[i] <= pos && pos < starts[i] + flat_tables[i].fields.len())
        {
            needs[item].columns.insert(pos - starts[item]);
        }
    };
    for e in exprs.iter().copied().chain(wh_expr) {
        every_position(e).into_iter().for_each(&mut add);
    }
    let mut item = 0;
    for t in tables {
        let base = starts[item];
        for j in &t.joins {
            for p in every_position(&j.on_expr) {
                add(base + p);
            }
        }
        item += 1 + t.joins.len();
    }

    let conjuncts = Conjuncts::analyze(wh_expr, shapes);
    for (t, n) in needs.iter_mut().enumerate() {
        n.filters = conjuncts
            .pushable_to(t)
            .filter_map(|c| c.local_expr(&conjuncts))
            .collect();
    }
    let item_of = |pos: usize| {
        (0..starts.len())
            .find(|&i| starts[i] <= pos && pos < starts[i] + flat_tables[i].fields.len())
    };
    for (pos, op, value) in derived_comparisons(tables, &starts, &conjuncts) {
        let Some(item) = item_of(pos) else {
            continue;
        };
        if shapes[item].null_supplied {
            continue;
        }
        needs[item].filters.push(EvalExpr::Binary {
            lhs: Box::new(EvalExpr::Value(pos - starts[item])),
            op,
            rhs: Box::new(EvalExpr::Literal(value)),
        });
    }
    needs
}

// Comparisons implied by the query that it doesn't state: `a.x = b.y AND
// a.x > 5` means `b.y > 5` in every row of the result. Only equalities that
// hold in every result row carry a comparison across — WHERE's, and an
// inner join's ON — and the comparisons themselves come from WHERE (flat
// positions). Only for seeks: WHERE itself still runs as written, so these
// can only narrow what is read to rows the result could contain anyway.
fn derived_comparisons<F: DBFile + 'static>(
    tables: &[TableQuery<F>],
    starts: &[usize],
    conjuncts: &Conjuncts,
) -> Vec<(usize, BinaryOp, ValueItem)> {
    let mut equal: Vec<(usize, usize)> = vec![];
    let mut stated = vec![];
    for c in conjuncts.iter() {
        match &c.expr {
            EvalExpr::Binary {
                lhs,
                op: BinaryOp::Eq,
                rhs,
            } => {
                if let (EvalExpr::Value(a), EvalExpr::Value(b)) = (lhs.as_ref(), rhs.as_ref()) {
                    equal.push((*a, *b));
                    continue;
                }
                stated.extend(column_comparison(&c.expr));
            }
            e => stated.extend(column_comparison(e)),
        }
    }
    let mut item = 0;
    for t in tables {
        let base = starts[item];
        for j in &t.joins {
            if matches!(j.join_type, JoinType::Inner) {
                let mut on = vec![];
                and_terms(&j.on_expr, &mut on);
                for e in on {
                    if let EvalExpr::Binary {
                        lhs,
                        op: BinaryOp::Eq,
                        rhs,
                    } = e
                        && let (EvalExpr::Value(a), EvalExpr::Value(b)) =
                            (lhs.as_ref(), rhs.as_ref())
                    {
                        equal.push((base + a, base + b));
                    }
                }
            }
        }
        item += 1 + t.joins.len();
    }
    // Every column equal to each stated one, through chains of equalities.
    let mut derived = vec![];
    for (pos, op, value) in &stated {
        let mut reached = vec![*pos];
        let mut i = 0;
        while i < reached.len() {
            let p = reached[i];
            for &(a, b) in &equal {
                let other = if a == p {
                    b
                } else if b == p {
                    a
                } else {
                    continue;
                };
                if !reached.contains(&other) {
                    reached.push(other);
                }
            }
            i += 1;
        }
        for q in reached.into_iter().skip(1) {
            derived.push((q, *op, value.clone()));
        }
    }
    derived
}

fn and_terms<'a>(e: &'a EvalExpr, out: &mut Vec<&'a EvalExpr>) {
    match e {
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => {
            and_terms(lhs, out);
            and_terms(rhs, out);
        }
        other => out.push(other),
    }
}

// Every column position `e` reads, through every function argument —
// aggregates nested inside other calls included (`abs(sum(x))` reads x),
// which EvalExpr::column_positions does not promise. Missing one here would
// let an index that lacks that column be chosen, and the query would read
// the NULL the index scan pads it with.
fn every_position(e: &EvalExpr) -> Vec<usize> {
    match e {
        EvalExpr::None | EvalExpr::Literal(_) => vec![],
        EvalExpr::Value(i) => vec![*i],
        EvalExpr::Unary { field, .. } => every_position(field),
        EvalExpr::Binary { lhs, rhs, .. } => {
            let mut v = every_position(lhs);
            v.extend(every_position(rhs));
            v
        }
        EvalExpr::Function(f) => f
            .args()
            .iter()
            .flat_map(|a| match a {
                FuncArgs::Field(e) => every_position(e),
                FuncArgs::Wildcard => vec![],
            })
            .collect(),
    }
}

/// How to read one FROM item that is a real table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AccessPath {
    /// Every row, from the table.
    TableScan,
    /// The rows within a range of the table's own key — its PRIMARY KEY:
    /// the table's tree is keyed by it, so this needs no index at all.
    TableSeek(KeyRange),
    /// Every entry of a covering index (see IndexSource).
    IndexScan(usize),
    /// A range of a covering index.
    IndexSeek(usize, KeyRange),
    /// A range of an index that does NOT cover the query, fetching each
    /// entry's row from the table.
    IndexLookup(usize, KeyRange),
}

/// The chosen path, and how many rows it is expected to produce (None
/// without statistics).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Access {
    pub path: AccessPath,
    pub rows: Option<usize>,
}

/// How to read `table` given what the query needs from it.
///
/// With statistics, the cheapest path by estimated bytes read. Scans read
/// every row sequentially: per row, a tree entry and the row (table) or an
/// entry and the row identity (index). Seeks read only the rows the key
/// range allows, estimated from column statistics, plus one page for the
/// descent; an IndexLookup also reads one page per row from the table,
/// since the rows it fetches are scattered.
///
/// Without statistics, only what is safe without them: a seek on the
/// table's primary key (it reads a subset of what a scan reads), or an
/// index whose every key column is fixed by an equality on a unique index
/// (one row).
pub(crate) fn pick_access(
    table: &SqlTable,
    stats: Option<&ComputedTableStat>,
    needs: &ItemNeeds,
    page_size: usize,
) -> Access {
    let fields = table.fields();
    let position = |name: &str| {
        fields
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(name))
    };
    let columns_of = |index: &crate::table::SqlIndex| -> Vec<usize> {
        index
            .fields
            .iter()
            .filter_map(|f| position(&f.name))
            .collect()
    };
    let types: Vec<DataType> = fields.iter().map(|f| f.datatype).collect();
    let ranges = column_ranges(&needs.filters, &types);
    let primary = table.indices.iter().position(|i| i.is_primary);
    let pk_columns = primary
        .map(|p| columns_of(&table.indices[p]))
        .unwrap_or_default();
    let table_seek = if pk_columns.is_empty() {
        None
    } else {
        key_range(&pk_columns, &ranges)
    };

    let Some((stats, self_index, index_stats)) =
        stats.and_then(|s| Some((s, s.self_index.as_ref()?, s.indices.as_ref()?)))
    else {
        if let Some(range) = table_seek {
            return Access {
                path: AccessPath::TableSeek(range),
                rows: None,
            };
        }
        for (i, index) in table.indices.iter().enumerate() {
            let key = columns_of(index);
            if let Some(range) = key_range(&key, &ranges)
                && (index.is_primary || index.is_unique)
                && range.prefix.len() == key.len()
            {
                let covered: BTreeSet<usize> = key.iter().chain(&pk_columns).copied().collect();
                let path = if needs.columns.is_subset(&covered) {
                    AccessPath::IndexSeek(i, range)
                } else {
                    AccessPath::IndexLookup(i, range)
                };
                return Access {
                    path,
                    rows: Some(1),
                };
            }
        }
        return Access {
            path: AccessPath::TableScan,
            rows: None,
        };
    };

    let rows = stats.table_stat.row_count;
    let estimate = |key: &[usize], range: &KeyRange, unique: bool| -> usize {
        if is_empty(range) {
            return 0;
        }
        if unique && range.prefix.len() == key.len() {
            return 1;
        }
        let mut fraction = 1.0;
        for c in &key[..range.prefix.len()] {
            fraction /= stats
                .table_stat
                .col_stats
                .get(c)
                .map_or(10.0, |s| s.unique.max(1) as f64);
        }
        if let Some(c) = key.get(range.prefix.len())
            && !matches!(
                (&range.lower, &range.upper),
                (Bound::Unbounded, Bound::Unbounded)
            )
        {
            fraction *= range_fraction(stats.table_stat.col_stats.get(c), range);
        }
        let mut est = ((rows as f64 * fraction).ceil() as usize)
            .max(1)
            .min(rows.max(1));
        // A range on a unique key's last column matches at most one row per
        // value in it: `id >= 10 AND id < 13` is at most 3 rows, with or
        // without statistics on id.
        if unique
            && range.prefix.len() + 1 == key.len()
            && let Some(n) = values_in_range(range)
        {
            est = est.min(n as usize);
        }
        est
    };
    // No PRIMARY KEY: the identity is the generated row id, one integer.
    let identity_bytes = primary.map_or(8, |p| index_stats[p].row_size);
    let table_row = self_index.row_size + stats.row_size;

    // A seek on the primary key reads a subset of what scanning the same
    // table reads, so it is never the worse of the two — whatever the
    // (possibly stale) statistics say.
    let (mut best, mut best_cost) = match table_seek {
        Some(range) => {
            let est = estimate(&pk_columns, &range, true);
            let cost = (est * table_row + page_size).min(rows * table_row);
            (
                Access {
                    path: AccessPath::TableSeek(range),
                    rows: Some(est),
                },
                cost,
            )
        }
        None => (
            Access {
                path: AccessPath::TableScan,
                rows: Some(rows),
            },
            rows * table_row,
        ),
    };
    let mut consider = |path: AccessPath, est: usize, cost: usize| {
        if cost < best_cost {
            best = Access {
                path,
                rows: Some(est),
            };
            best_cost = cost;
        }
    };
    for (i, (index, stat)) in table.indices.iter().zip(index_stats).enumerate() {
        let key = columns_of(index);
        let covered: BTreeSet<usize> = key.iter().chain(&pk_columns).copied().collect();
        let covering = needs.columns.is_subset(&covered);
        let entry = stat.row_size + identity_bytes;
        match (key_range(&key, &ranges), covering) {
            (Some(range), true) => {
                let est = estimate(&key, &range, index.is_primary || index.is_unique);
                consider(
                    AccessPath::IndexSeek(i, range),
                    est,
                    est * entry + page_size,
                );
            }
            (Some(range), false) => {
                let est = estimate(&key, &range, index.is_primary || index.is_unique);
                consider(
                    AccessPath::IndexLookup(i, range),
                    est,
                    est * (entry + page_size) + page_size,
                );
            }
            (None, true) => consider(AccessPath::IndexScan(i), rows, rows * entry),
            (None, false) => {}
        }
    }
    best
}

// The share of a column's non-NULL values a range keeps: by its bounds'
// position between the column's min and max, for numbers and datetimes;
// a third for anything else, or without statistics.
fn range_fraction(stat: Option<&ColumnStat>, range: &KeyRange) -> f64 {
    const GUESS: f64 = 1.0 / 3.0;
    let Some(stat) = stat else {
        return GUESS;
    };
    let number = |v: &ValueItem| match v {
        ValueItem::Integer(i) => Some(*i as f64),
        ValueItem::Double(d) => Some(*d),
        ValueItem::Datetime(d) => Some(*d as f64),
        _ => None,
    };
    let (Some(min), Some(max)) = (number(&stat.min), number(&stat.max)) else {
        return GUESS;
    };
    let bound = |b: &Bound<ValueItem>, default: f64| match b {
        Bound::Included(v) | Bound::Excluded(v) => number(v),
        Bound::Unbounded => Some(default),
    };
    let lower = match &range.lower {
        Bound::Excluded(ValueItem::Null) => Some(min),
        b => bound(b, min),
    };
    let (Some(lo), Some(hi)) = (lower, bound(&range.upper, max)) else {
        return GUESS;
    };
    if max <= min {
        return if lo <= min && min <= hi { 1.0 } else { 0.0 };
    }
    ((hi.min(max) - lo.max(min)) / (max - min)).clamp(0.0, 1.0)
}
