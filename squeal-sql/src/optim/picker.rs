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
        sarg::{
            ColumnSet, column_comparison, column_sets, condition_set, is_empty, key_ranges,
            values_in_range,
        },
    },
    source::{join::JoinType, nestloop::JoinSeek},
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
    /// The first `stated` are WHERE's own; the rest are comparisons
    /// implied through join equalities (see derived_comparisons), for seeks
    /// only.
    pub filters: Vec<EvalExpr>,
    pub stated: usize,
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
        n.stated = n.filters.len();
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
        EvalExpr::Subquery(s) => s.operands().flat_map(every_position).collect(),
    }
}

/// How to read one FROM item that is a real table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AccessPath {
    /// Every row, from the table, in no particular order.
    TableScan,
    /// The rows within ranges of the table's own key — its PRIMARY KEY:
    /// the table's tree is keyed by it, so this needs no index at all. One
    /// unrestricted range reads the whole table in key order.
    TableSeek(Vec<KeyRange>),
    /// Every entry of a covering index (see IndexSource).
    IndexScan(usize),
    /// Ranges of a covering index.
    IndexSeek(usize, Vec<KeyRange>),
    /// Ranges of an index that does NOT cover the query, fetching each
    /// entry's row from the table.
    IndexLookup(usize, Vec<KeyRange>),
}

/// The chosen path, and what the rest of the plan can rely on about it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Access {
    pub path: AccessPath,
    /// Expected rows (None without statistics).
    pub rows: Option<usize>,
    /// Columns (positions within the table's row) whose WHERE conditions
    /// the path reads exactly — every row it produces satisfies them, and
    /// no row it skips could have — so they need not be checked again.
    pub enforced: Vec<usize>,
    /// The rows come out in the order asked for (see OrderWanted).
    pub sorted: bool,
    /// Estimated bytes read (None without statistics).
    pub cost: Option<f64>,
}

/// An order the query wants its rows in — ORDER BY over plain columns of
/// the one table — and how many it keeps (LIMIT).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrderWanted {
    /// Columns (positions within the table's row), ascending, each with
    /// whether NULLs come first.
    pub columns: Vec<(usize, bool)>,
    pub limit: Option<usize>,
    /// Only a read that produces the order will do — for a merge join,
    /// which needs its input read in order, where a later sort doesn't
    /// count (see QueryVisitor::merge_plan). Otherwise a read that doesn't
    /// may win, paying for the sort.
    pub required: bool,
}

// Whether a key's order is the wanted one (see pick_access's order_of).
enum Order {
    No,
    Yes,
    // Yes, reading the first free key column's NULLs after its other values.
    NullsLast,
}

// A path under consideration: its key columns (what its rows are ordered
// by, when they are ordered at all) and ranges.
struct Candidate {
    path: AccessPath,
    rows: usize,
    // Bytes read, before any sort or LIMIT adjustment.
    cost: usize,
    key: Vec<usize>,
    ordered: bool,
    used: usize,
}

/// How to read `table` given what the query needs from it.
///
/// With statistics, the cheapest path by estimated bytes read. Scans read
/// every row sequentially: per row, a tree entry and the row (table) or an
/// entry and the row identity (index). Seeks read only the rows the key
/// ranges allow, estimated from column statistics, plus one page per range
/// for the descent; an IndexLookup also reads one page per row from the
/// table, since the rows it fetches are scattered. (A page, or the whole
/// index or table when that is smaller.) When an order is wanted,
/// a path that doesn't produce it pays for a sort, and one that does can
/// stop after LIMIT rows.
///
/// Without statistics, only what is safe without them: a seek on the
/// table's primary key (it reads a subset of what a scan reads), or an
/// index whose every key column is fixed by an equality on a unique index.
pub(crate) fn pick_access(
    table: &SqlTable,
    stats: Option<&ComputedTableStat>,
    needs: &ItemNeeds,
    pages: PageCosts,
    order: Option<&OrderWanted>,
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
    let sets = column_sets(&needs.filters, &types);
    let primary = table.indices.iter().position(|i| i.is_primary);
    let pk_columns = primary
        .map(|p| columns_of(&table.indices[p]))
        .unwrap_or_default();
    let table_seek = if pk_columns.is_empty() {
        None
    } else {
        key_ranges(&pk_columns, &sets)
    };
    let enforced = |key: &[usize], used: usize| key[..used].to_vec();
    // Whether rows ordered by `key` (within `ranges`, None for all of it)
    // come out in the wanted order: after the key columns every range fixes
    // to one value, the wanted columns must follow the key. A wanted column
    // the ranges fix is constant, so it can be skipped. NULLs sort first in
    // a key, so a NULLS LAST column must be one no NULL can reach (NOT
    // NULL, or limited by a condition, which never matches NULL).
    //
    // One exception: the first such column may hold NULLs wanted last when
    // the ranges don't limit it — read its non-NULL values, then its NULLs
    // (see ordered_ranges).
    let order_of = |key: &[usize], ranges: Option<&[KeyRange]>| -> Order {
        let Some(order) = order else {
            return Order::No;
        };
        let fixed = match ranges {
            None => 0,
            Some(rs) => (0..rs.first().map_or(0, |r| r.prefix.len()))
                .take_while(|&i| rs.iter().all(|r| r.prefix[i] == rs[0].prefix[i]))
                .count(),
        };
        let mut rest = key[fixed..].iter();
        let mut answer = Order::Yes;
        let mut first = true;
        for (c, nulls_first) in &order.columns {
            if key[..fixed].contains(c) {
                continue;
            }
            if rest.next() != Some(c) {
                return Order::No;
            }
            if !nulls_first && fields[*c].nullable && !sets.contains_key(c) {
                let one_open_range = ranges.is_none_or(|rs| {
                    matches!(rs, [r] if r.prefix.len() == fixed
                        && r.lower == Bound::Unbounded
                        && r.upper == Bound::Unbounded)
                });
                if !first || !one_open_range {
                    return Order::No;
                }
                answer = Order::NullsLast;
            }
            first = false;
        }
        answer
    };
    // A candidate's key ranges (None: all of it, unordered or in key order)
    // and whether it gives the wanted order — with NULLs-last split into two
    // ranges read in turn: the non-NULL values of the first free key
    // column, then its NULLs.
    let ordered_ranges =
        |key: &[usize], ranges: Option<Vec<KeyRange>>| -> (Option<Vec<KeyRange>>, bool) {
            match order_of(key, ranges.as_deref()) {
                Order::No => (ranges, false),
                Order::Yes => (ranges, true),
                Order::NullsLast => {
                    let prefix = ranges
                        .and_then(|rs| rs.into_iter().next())
                        .map_or(vec![], |r| r.prefix);
                    let mut null = prefix.clone();
                    null.push(ValueItem::Null);
                    let split = vec![
                        KeyRange {
                            prefix,
                            lower: Bound::Excluded(ValueItem::Null),
                            upper: Bound::Unbounded,
                        },
                        KeyRange::prefix(null),
                    ];
                    (Some(split), true)
                }
            }
        };
    let gives_order = |key: &[usize], ranges: Option<&[KeyRange]>| -> bool {
        !matches!(order_of(key, ranges), Order::No)
    };

    let Some((stats, self_index, index_stats)) =
        stats.and_then(|s| Some((s, s.self_index.as_ref()?, s.indices.as_ref()?)))
    else {
        if let Some((ranges, used)) = table_seek {
            let sorted = gives_order(&pk_columns, Some(&ranges));
            return Access {
                path: AccessPath::TableSeek(ranges),
                rows: None,
                enforced: enforced(&pk_columns, used),
                sorted,
                cost: None,
            };
        }
        for (i, index) in table.indices.iter().enumerate() {
            let key = columns_of(index);
            if let Some((ranges, used)) = key_ranges(&key, &sets)
                && (index.is_primary || index.is_unique)
                && used == key.len()
                && ranges.iter().all(|r| r.prefix.len() == key.len())
            {
                let covered: BTreeSet<usize> = key.iter().chain(&pk_columns).copied().collect();
                let sorted = gives_order(&key, Some(&ranges));
                let rows = Some(ranges.len());
                let path = if needs.columns.is_subset(&covered) {
                    AccessPath::IndexSeek(i, ranges)
                } else {
                    AccessPath::IndexLookup(i, ranges)
                };
                return Access {
                    path,
                    rows,
                    enforced: enforced(&key, used),
                    sorted,
                    cost: None,
                };
            }
        }
        return Access {
            path: AccessPath::TableScan,
            rows: None,
            enforced: vec![],
            sorted: false,
            cost: None,
        };
    };

    let rows = stats.table_stat.row_count;
    let estimate = |key: &[usize], ranges: &[KeyRange], unique: bool| -> usize {
        let one = |range: &KeyRange| -> usize {
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
            // A range on a unique key's last column matches at most one row
            // per value in it: `id >= 10 AND id < 13` is at most 3 rows,
            // with or without statistics on id.
            if unique
                && range.prefix.len() + 1 == key.len()
                && let Some(n) = values_in_range(range)
            {
                est = est.min(n as usize);
            }
            est
        };
        ranges.iter().map(one).sum::<usize>().min(rows.max(1))
    };
    // No PRIMARY KEY: the identity is the generated row id, one integer.
    let identity_bytes = primary.map_or(8, |p| index_stats[p].row_size);
    let table_row = self_index.row_size + stats.row_size;

    // Ties go to the earliest candidate, so a primary key seek (whose cost
    // is capped at the scan's) comes first.
    let mut candidates = vec![];
    if let Some((ranges, used)) = table_seek {
        let est = estimate(&pk_columns, &ranges, true);
        // A seek on the primary key reads a subset of what scanning the
        // same table reads, so it is never the worse of the two — whatever
        // the (possibly stale) statistics say.
        let descent = pages.touch(rows * table_row);
        let cost =
            (est * (table_row + TREE_ROW_BYTES) + descent * ranges.len()).min(rows * table_row);
        candidates.push(Candidate {
            ordered: gives_order(&pk_columns, Some(&ranges)),
            path: AccessPath::TableSeek(ranges),
            rows: est,
            cost,
            key: pk_columns.clone(),
            used,
        });
    } else if !pk_columns.is_empty() && gives_order(&pk_columns, None) {
        // The whole table, in primary key order: through its key, so each
        // row pays the cursor's per-row cost (see TREE_ROW_BYTES).
        candidates.push(Candidate {
            path: AccessPath::TableSeek(vec![KeyRange::prefix(vec![])]),
            rows,
            cost: rows * (table_row + TREE_ROW_BYTES),
            key: pk_columns.clone(),
            ordered: true,
            used: 0,
        });
    }
    candidates.push(Candidate {
        path: AccessPath::TableScan,
        rows,
        cost: rows * table_row,
        key: vec![],
        ordered: false,
        used: 0,
    });
    for (i, (index, stat)) in table.indices.iter().zip(index_stats).enumerate() {
        let key = columns_of(index);
        let unique = index.is_primary || index.is_unique;
        let covered: BTreeSet<usize> = key.iter().chain(&pk_columns).copied().collect();
        let covering = needs.columns.is_subset(&covered);
        let entry = stat.row_size + identity_bytes;
        // A descent, or a row fetched from the table, touches a page (see
        // PageCosts).
        let descent = pages.touch(rows * entry);
        // Finding the row, then reading it.
        let fetch = pages.touch(rows * table_row) + table_row;
        match (key_ranges(&key, &sets), covering) {
            (Some((ranges, used)), true) => {
                let est = estimate(&key, &ranges, unique);
                let (ranges, ordered) = ordered_ranges(&key, Some(ranges));
                let ranges = ranges.expect("ranges in, ranges out");
                let descents = ranges.len();
                candidates.push(Candidate {
                    ordered,
                    path: AccessPath::IndexSeek(i, ranges),
                    rows: est,
                    cost: est * (entry + TREE_ROW_BYTES) + descent * descents,
                    key: key.clone(),
                    used,
                });
            }
            (Some((ranges, used)), false) => {
                let est = estimate(&key, &ranges, unique);
                let (ranges, ordered) = ordered_ranges(&key, Some(ranges));
                let ranges = ranges.expect("ranges in, ranges out");
                let descents = ranges.len();
                candidates.push(Candidate {
                    ordered,
                    path: AccessPath::IndexLookup(i, ranges),
                    rows: est,
                    cost: est * (entry + TREE_ROW_BYTES + fetch) + descent * descents,
                    key: key.clone(),
                    used,
                });
            }
            (None, true) => {
                let (ranges, ordered) = ordered_ranges(&key, None);
                candidates.push(Candidate {
                    ordered,
                    // Split for NULLS LAST: the same entries, in two reads.
                    path: match ranges {
                        Some(ranges) => AccessPath::IndexSeek(i, ranges),
                        None => AccessPath::IndexScan(i),
                    },
                    rows,
                    cost: rows * (entry + TREE_ROW_BYTES),
                    key: key.clone(),
                    used: 0,
                });
            }
            // All of a non-covering index, fetching every row: only ever
            // worth it for its order, with a LIMIT to stop early.
            (None, false) if gives_order(&key, None) => {
                let (ranges, _) = ordered_ranges(&key, None);
                candidates.push(Candidate {
                    path: AccessPath::IndexLookup(
                        i,
                        ranges.unwrap_or_else(|| vec![KeyRange::prefix(vec![])]),
                    ),
                    rows,
                    cost: rows * (entry + TREE_ROW_BYTES + fetch),
                    key: key.clone(),
                    ordered: true,
                    used: 0,
                });
            }
            (None, false) => {}
        }
    }

    // What each candidate costs once the order and LIMIT are accounted for.
    let all_enforced = |c: &Candidate| {
        let e = enforced(&c.key, c.used);
        sets.keys().all(|col| e.contains(col))
    };
    let total = |c: &Candidate| -> f64 {
        let cost = c.cost as f64;
        match order {
            None => cost,
            Some(o) if !c.ordered && o.required => f64::INFINITY,
            Some(_) if !c.ordered => {
                let n = c.rows as f64;
                cost + n * (n + 1.0).log2() * SORT_BYTES_PER_COMPARE
            }
            // In order: with LIMIT n, reading stops after n rows — sooner
            // than that estimate only if WHERE rejects none of them.
            Some(o) => match o.limit {
                Some(n) if all_enforced(c) && c.rows > 0 => {
                    cost * (n as f64 / c.rows as f64).min(1.0)
                }
                _ => cost,
            },
        }
    };
    let best = candidates
        .into_iter()
        .map(|c| (total(&c), c))
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, c)| c)
        .expect("the table scan is always a candidate");
    Access {
        enforced: enforced(&best.key, best.used),
        sorted: best.ordered,
        rows: Some(best.rows),
        cost: Some(best.cost as f64),
        path: best.path,
    }
}

// What each row read through a tree cursor costs on top of its bytes —
// any read but a table scan (seeks, index reads, a table read in key
// order): each entry is resolved to its row on its own, where a scan walks
// data pages in order. Measured: scans cost ~1.2 ns per unit of row size;
// cursor reads the same plus 50-150 units per row (key-order reads of
// narrow tables nearly twice a scan; the orders table's primary key index
// slower to count than the table itself).
const TREE_ROW_BYTES: usize = 100;

// Touching a page outside a sequential scan — a descent's step, or finding
// a row by its key — when the tree it is in fits the page cache: no page
// read, but a cache lookup, locks, a binary search, a visibility check.
// In this model's units, set by a scan's cost per row: measured on a
// cached 200k-row table, fetching rows through an index (~1.5 us a row)
// overtakes scanning the whole table (~40 ns a row) at ~3% of the rows,
// which this puts the crossover at for a table of that shape. Uncached,
// it is a page read (page_size).
const CACHED_PAGE_TOUCH: usize = 4096;

/// What touching one page costs, given its tree's size: a cached lookup
/// while the tree fits the page cache, else a page read (or the whole tree,
/// when that is less).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PageCosts {
    pub page_size: usize,
    pub cache_bytes: usize,
}

impl PageCosts {
    pub(crate) fn of<F: DBFile + 'static>(db: &store::db::Db<F>) -> Self {
        Self {
            page_size: db.get_page_data_size(),
            cache_bytes: db.cache_bytes(),
        }
    }

    // One page of a tree `tree_bytes` big.
    fn touch(&self, tree_bytes: usize) -> usize {
        if tree_bytes <= self.cache_bytes {
            CACHED_PAGE_TOUCH.min(tree_bytes)
        } else {
            self.page_size.min(tree_bytes)
        }
    }
}

// What sorting costs, in the same units as reading: per comparison.
// Measured: sorting 30k rows in memory added 1-2 ms to a 32 ms scan of
// them (~4 MB of reading in these units), about 0.4 per comparison.
pub(crate) const SORT_BYTES_PER_COMPARE: f64 = 0.5;

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

/// Whether to join `table` (the inner side) by seeking it once per outer
/// row, and by which key: its primary key or an index, whose leading
/// columns the join's equalities `pairs` — (outer position, inner column,
/// outer column's type) — fix. Only same-type pairs count, so an outer
/// value always converts to the inner column's type exactly.
///
/// Worth it when `outer_rows` seeks — each a descent plus the rows one key
/// matches (and, through an index, a fetch per row) — cost less than
/// `hash_rows` rows of the inner table read for a hash join, plus building
/// the hash table (`hash_extra`, spilling included). Needs
/// statistics for the inner table and an outer row estimate.
#[allow(clippy::too_many_arguments)]
pub(crate) fn pick_join_seek(
    table: &SqlTable,
    stats: Option<&ComputedTableStat>,
    pairs: &[(usize, usize, DataType)],
    // ON comparisons of an outer column with an inner one: (outer
    // position, inner column, the op as `inner op outer`, outer type). One
    // may bound a range on the key column after the equalities.
    ranges: &[(usize, usize, BinaryOp, DataType)],
    outer_rows: Option<usize>,
    // Trees each outer row descends: one per partition sought. The rows
    // that match are in one of them or another, so they are fetched once.
    trees: usize,
    hash_rows: usize,
    hash_extra: f64,
    pages: PageCosts,
) -> Option<JoinSeek> {
    let outer_rows = outer_rows?;
    let stats = stats?;
    let self_index = stats.self_index.as_ref()?;
    let index_stats = stats.indices.as_ref()?;
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
    let same_type = |a: DataType, b: DataType| {
        a == b
            || matches!(
                (a, b),
                (DataType::Str(_), DataType::Str(_)) | (DataType::Blob(_), DataType::Blob(_))
            )
    };
    // The outer positions fixing the longest run of `key`'s leading columns.
    let keys_for = |key: &[usize]| -> Vec<(usize, usize)> {
        key.iter()
            .map_while(|col| {
                pairs
                    .iter()
                    .find(|(_, c, t)| c == col && same_type(*t, fields[*col].datatype))
                    .map(|(o, c, _)| (*o, *c))
            })
            .collect()
    };
    let rows = stats.table_stat.row_count;
    let primary = table.indices.iter().position(|i| i.is_primary);
    let identity_bytes = primary.map_or(8, |p| index_stats[p].row_size);
    let table_row = self_index.row_size + stats.row_size;
    let fetch = pages.touch(rows * table_row) + table_row;
    let per_key = |key: &[usize], used: &[(usize, usize)], unique: bool| -> usize {
        if unique && used.len() == key.len() {
            return 1;
        }
        let mut fraction = 1.0;
        for (_, c) in used {
            fraction /= stats
                .table_stat
                .col_stats
                .get(c)
                .map_or(10.0, |s| s.unique.max(1) as f64);
        }
        ((rows as f64 * fraction).ceil() as usize).max(1)
    };

    let mut best: Option<(usize, JoinSeek)> = None;
    let mut consider =
        |index: Option<usize>, key: Vec<usize>, unique: bool, entry: usize, row: usize| {
            let used = keys_for(&key);
            let range = key.get(used.len()).and_then(|col| {
                ranges
                    .iter()
                    .find(|(_, c, _, t)| c == col && same_type(*t, fields[*col].datatype))
                    .map(|(o, _, op, _)| (*o, *op))
            });
            if used.is_empty() && range.is_none() {
                return;
            }
            let mut rows_per_key = per_key(&key, &used, unique && range.is_none());
            // A range keeps a third, as filtered_rows guesses for one.
            if range.is_some() {
                rows_per_key = rows_per_key.div_ceil(3).max(1);
            }
            let descent = pages.touch(rows * entry);
            let cost = outer_rows * (trees * descent + rows_per_key * (row + TREE_ROW_BYTES));
            if best.as_ref().is_none_or(|(c, _)| cost < *c) {
                best = Some((
                    cost,
                    JoinSeek {
                        index,
                        keys: used,
                        rows_per_key,
                        range,
                    },
                ));
            }
        };
    if let Some(p) = primary {
        consider(
            None,
            columns_of(&table.indices[p]),
            true,
            self_index.row_size,
            table_row,
        );
    }
    for (i, (index, stat)) in table.indices.iter().zip(index_stats).enumerate() {
        if index.is_primary {
            continue; // the table's own tree is the better way in
        }
        let entry = stat.row_size + identity_bytes;
        consider(
            Some(i),
            columns_of(index),
            index.is_unique,
            entry,
            entry + fetch,
        );
    }
    let (cost, seek) = best?;
    // With no equality, the other way is no hash join but every pair of
    // rows: the seek wins.
    if pairs.is_empty() {
        return Some(seek);
    }
    ((cost as f64) < (hash_rows * table_row) as f64 + hash_extra).then_some(seek)
}

/// Rows left of `rows` once `filters` (conditions on `table`'s own row)
/// have been applied: a condition limiting a column to some values keeps
/// that many of its distinct values' share; anything else, a third.
pub(crate) fn filtered_rows(
    table: &SqlTable,
    stats: Option<&ComputedTableStat>,
    rows: usize,
    filters: &[&EvalExpr],
) -> usize {
    let types: Vec<DataType> = table.fields().iter().map(|f| f.datatype).collect();
    let mut fraction = 1.0;
    for f in filters {
        fraction *= match condition_set(f, &types) {
            Some((column, ColumnSet::Points(points))) => {
                let distinct = stats
                    .and_then(|s| s.table_stat.col_stats.get(&column))
                    .map_or(10.0, |c| c.unique.max(1) as f64);
                (points.len() as f64 / distinct).min(1.0)
            }
            _ => 1.0 / 3.0,
        };
    }
    ((rows as f64 * fraction).ceil() as usize).max(if rows > 0 { 1 } else { 0 })
}
