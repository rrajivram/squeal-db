//! Choosing how to read each FROM item: a plain table scan, or a scan of
//! one of its indexes when that index holds every column the query needs
//! (a covering index) and is cheaper to read than the table.
//!
//! An index scan produces rows in the TABLE's layout — every column in
//! table order, NULL wherever the index has no value — so choosing one
//! changes no column position anywhere else in the plan (see IndexSource).
//! The NULLs are never read: the index is only chosen when it covers
//! everything the query reads from that item.

use std::collections::BTreeSet;

use store::db::DBFile;

use crate::{
    optim::table_stats::ComputedTableStat,
    plan::{
        conjuncts::{Conjuncts, TableShape},
        eval::EvalExpr,
        funcs::{FuncArgs, FuncTrait},
        logical::TableQuery,
    },
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
    /// within the item's own row. Not used to choose a path yet: they are
    /// what an index seek (a range scan instead of a full one) will use.
    #[allow(dead_code)]
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
    needs
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

/// The index to scan instead of `table`, if any: the cheapest index that
/// covers `needs.columns` and costs less to read than the table itself.
///
/// Cost is bytes read per row. A table scan reads its B-tree entry and the
/// row (data page). An index scan reads the index entry and the row
/// identity the entry points to — which is also what lets an index cover
/// the primary key columns: a PRIMARY KEY table's rows are identified by
/// their key values (see Schema's index maintenance). Both costs scale
/// with the row count alike, so it cancels out.
pub(crate) fn pick_index(
    table: &SqlTable,
    stats: &ComputedTableStat,
    needs: &ItemNeeds,
) -> Option<usize> {
    let self_index = stats.self_index.as_ref()?;
    let index_stats = stats.indices.as_ref()?;
    let fields = table.fields();
    let position = |name: &str| {
        fields
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(name))
    };
    let primary = table.indices.iter().position(|i| i.is_primary);
    let pk_columns: Vec<usize> = primary
        .map(|p| {
            table.indices[p]
                .fields
                .iter()
                .filter_map(|f| position(&f.name))
                .collect()
        })
        .unwrap_or_default();
    // No PRIMARY KEY: the identity is the generated row id, one integer.
    let identity_bytes = primary.map_or(8, |p| index_stats[p].row_size);

    let table_cost = self_index.row_size + stats.row_size;
    let mut best: Option<(usize, usize)> = None;
    for (i, (index, stat)) in table.indices.iter().zip(index_stats).enumerate() {
        let covered: BTreeSet<usize> = index
            .fields
            .iter()
            .filter_map(|f| position(&f.name))
            .chain(pk_columns.iter().copied())
            .collect();
        if !needs.columns.is_subset(&covered) {
            continue;
        }
        let cost = stat.row_size + identity_bytes;
        if cost < table_cost && best.is_none_or(|(_, c)| cost < c) {
            best = Some((i, cost));
        }
    }
    best.map(|(i, _)| i)
}
