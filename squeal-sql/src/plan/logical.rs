use std::{marker::PhantomData, sync::Arc};

use parking_lot::RwLock;
use sql_parser::{
    Expr, Query,
    expr::BinaryOp,
    query::{
        Alias, FromClause, GroupByClause, JoinConstraint, JoinOperator, OrderByClause, SelectItem,
        SetOperand, TableFactor,
    },
    token::Comma,
    utils::Seq,
    visitor::{Visit, Visitor},
};
use store::{clock::Instant, db::DBFile, txn::Transaction};

use crate::{
    conn::connection::{Connection, DerivedSource, TableRef},
    constant::DEFAULT_QUERY_MEMORY_LIMIT,
    datatype::DataType,
    error::SchemaError,
    optim::{
        picker::{
            Access, AccessPath, ItemNeeds, OrderWanted, analyze_query, filtered_rows, pick_access,
            pick_join_seek,
        },
        table_stats::{ComputedTableStat, compute_table_stats},
    },
    plan::{
        conjuncts::{ConjunctKind, Conjuncts, TableShape, null_supplied_tables},
        eval::EvalExpr,
        memory::QueryMemory,
        sarg::condition_set,
    },
    rslt::resultset::StreamingResultSet,
    source::{
        ProjectableField, Source,
        aggr::AggregatingSource,
        group::GroupSource,
        index::IndexSource,
        join::{JoinSource, JoinType, UnionJoin},
        limit::Limit,
        nestloop::NestedLoopJoin,
        planinfo::PlanNode,
        proj::Projection,
        run::RunSource,
        sort::SortSource,
        table::TableSource,
        where_source::WhereSource,
    },
    table::{Field, SqlTable},
    temp::TempTable,
};

pub(crate) struct LogicalPlan<F: DBFile> {
    // The current tail of the step chain, not a list of steps: each
    // add_step call wraps the previous tail inside the new step's own
    // `depends` (see Source::chain), so at any point everything added
    // so far is reachable from just this one Box — a linear pull
    // cascade, where calling .next() on the tail recursively pulls
    // through every step behind it down to the original leaf source.
    // None until the first add_step call.
    tail: Option<Box<dyn Source>>,
    // The statement's own transaction (see QueryVisitor::stmt_txn), handed
    // to the StreamingResultSet by execute().
    stmt_txn: Option<Transaction>,
    // This query's own memory budget — separate from PageBuffer (a
    // shared, whole-database page cache every query reads through, not
    // something to partition per query). Handed out via `memory()` so a
    // step that needs to buffer state (no such step exists yet — see
    // QueryMemory's own doc comment) can be constructed with its own
    // clone of the Arc *before* being passed to add_step, the same way
    // a caller already builds a step (e.g. TableSource::new) with
    // whatever else it needs before handing it off.
    mem: Arc<QueryMemory>,
    start: Instant,
    _phanton: PhantomData<F>,
}

// Common interface between the two concrete shapes a resolved
// FROM-clause table reference can take (see ResolvedTable) — a real,
// durable SqlTable or a connection-scoped TempTable. This doesn't
// remove the need to branch on which one a given reference resolved to
// (Rust has no way around that with two unrelated concrete types), but
// it moves that branch to exactly one place (ResolvedTable's own
// dispatch, just below) instead of every call site that wants a
// reference's fields re-deriving the same Real-vs-Temp match.
//
// Deliberately NOT generic over F, unlike OpenSource below: Arc<SqlTable>
// doesn't involve F at all, so a method with no argument that mentions F
// (nothing here does) leaves the compiler unable to infer which F a
// generic trait's impl was meant, even though there's only one that
// could ever apply for a given receiver type.
pub(crate) trait HasFields {
    fn resolved_fields(&self) -> Arc<[Arc<Field>]>;
    fn has_field(&self, field: &str) -> bool;
}

impl HasFields for Arc<SqlTable> {
    fn resolved_fields(&self) -> Arc<[Arc<Field>]> {
        self.fields_arc()
    }

    fn has_field(&self, field: &str) -> bool {
        self.fields_arc()
            .iter()
            .any(|f| field.eq_ignore_ascii_case(&f.name))
    }
}

impl<F> HasFields for Arc<RwLock<TempTable<F>>>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn resolved_fields(&self) -> Arc<[Arc<Field>]> {
        self.read().fields()
    }

    fn has_field(&self, field: &str) -> bool {
        self.read()
            .fields()
            .iter()
            .any(|f| field.eq_ignore_ascii_case(&f.name))
    }
}

// Generic over F, unlike HasFields above: every call site passes
// `conn: &Arc<Connection<F>>`, which is what actually pins down F for
// the compiler — there's no inference ambiguity here the way there
// would be for a method with no F-mentioning argument.
trait OpenSource<F: DBFile + 'static> {
    /// `txn`: the transaction every source of one statement reads under —
    /// the connection's explicit BEGIN block, else the statement's own
    /// (see QueryVisitor::stmt_txn). Only borrowed for the call.
    fn open_source(
        &self,
        conn: &Arc<Connection<F>>,
        stat: Option<ComputedTableStat>,
        txn: Option<&Transaction>,
    ) -> Result<Box<dyn Source>, SchemaError>;
}

impl<F> OpenSource<F> for Arc<SqlTable>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn open_source(
        &self,
        conn: &Arc<Connection<F>>,
        stats: Option<ComputedTableStat>,
        txn: Option<&Transaction>,
    ) -> Result<Box<dyn Source>, SchemaError> {
        let ts = TableSource::new(conn.database.read().db.clone(), self.clone(), txn, stats)?;
        Ok(Box::new(ts) as Box<dyn Source>)
    }
}

impl<F> OpenSource<F> for Arc<RwLock<TempTable<F>>>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // No transaction/visibility involvement at all, unlike the real-
    // table case above — a Run isn't MVCC-shared state (see RunCursor's
    // own doc comment), so there's nothing here that needs
    // `with_current_txn`.
    fn open_source(
        &self,
        _conn: &Arc<Connection<F>>,
        _stat: Option<ComputedTableStat>,
        _txn: Option<&Transaction>,
    ) -> Result<Box<dyn Source>, SchemaError> {
        let guard = self.read();
        let cursor = guard.cursor()?;
        Ok(Box::new(RunSource::new(
            cursor,
            &guard
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ProjectableField::from_field(f.clone(), 0, i))
                .collect::<Vec<_>>(),
        )))
    }
}

// Dispatch for TableRef itself (see its own doc comment in
// conn::connection — it now carries what used to be a separate
// ResolvedTable enum here, since it's the same "what is this FROM item"
// question either way). Real/Temp delegate to HasFields/OpenSource
// above; Derived has neither a field list nor a Source to build yet —
// planning a subquery is real, unbuilt work, not a one-line stub, so
// this stays a todo!() until that exists rather than pretending an
// empty/placeholder answer would be meaningful.
#[allow(unused)]
impl<F> TableRef<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn resolved_fields(&self) -> Arc<[Arc<Field>]> {
        match self {
            TableRef::Real(_, t) => t.resolved_fields(),
            TableRef::Temp(_, t) => t.resolved_fields(),
            TableRef::Derived(_, d) => Arc::from(
                d.fields()
                    .iter()
                    .map(|f| f.field.clone())
                    .collect::<Vec<_>>(),
            ),
        }
    }

    fn open_source(
        &self,
        conn: &Arc<Connection<F>>,
        stat: Option<ComputedTableStat>,
        txn: Option<&Transaction>,
    ) -> Result<Box<dyn Source>, SchemaError> {
        match self {
            TableRef::Real(_, t) => t.open_source(conn, stat, txn),
            TableRef::Temp(_, t) => t.open_source(conn, stat, txn),
            TableRef::Derived(name, d) => d.take(name),
        }
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
enum Frame<T> {
    Empty,
    Some(T),
}

#[allow(unused)]
pub(crate) struct TableQuery<F: DBFile + 'static> {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) alias: String,
    pub(crate) fields: Arc<[Arc<Field>]>,
    // Resolved once, by resolve_table_ref (which does the schema/table-
    // name lookup itself) — carried alongside the name so
    // post_visit_select doesn't have to re-resolve or re-look-up
    // anything, nor treat "we already validated this" and "so of course
    // this lookup will succeed" as two separate, unwrap-worthy facts.
    pub(crate) resolved: TableRef<F>,
    pub(crate) joins: Vec<JoinRelation<F>>,
    pub(crate) stats: Option<ComputedTableStat>,
}

pub(crate) struct JoinRelation<F: DBFile + 'static> {
    pub(crate) join_type: JoinType,
    pub(crate) relation: TableQuery<F>,
    pub(crate) on_expr: EvalExpr,
}

struct QueryVisitor<F: DBFile> {
    conn: Arc<Connection<F>>,
    // Box<dyn Source>, not a generic Vec<S> — a Vec needs one uniform
    // element type, but different table references (and later, joins/
    // other step kinds) produce different concrete Source
    // implementations. This is also the exact type LogicalPlan::tail
    // already stores an owned step as (see its own comment) — Box
    // already owns the heap-allocated Source, so building this Vec here
    // and handing each entry to LogicalPlan::add_step below doesn't
    // need anything more than that.
    steps: Vec<Option<Box<dyn Source>>>,
    // How many Query nodes deep the walk currently is. Only the outermost
    // query is planned from the walk's post_visit_query; a FROM subquery is
    // planned by get_table when it reaches that FROM item (it needs the
    // outer query's transaction/budget, and its output becomes a table
    // reference), so the walk must not plan it a second time.
    depth: usize,
    mem: Arc<QueryMemory>,
    // TXN_SIMPLIFICATION_PLAN.md phase 7: outside an explicit BEGIN block,
    // one transaction for the whole statement, so a multi-table SELECT
    // reads every table at one snapshot (each TableCursor used to begin
    // its own). Moves into the StreamingResultSet, which owns it for as
    // long as the client holds the result; dropping it ends the
    // transaction.
    stmt_txn: Option<Transaction>,
}

// The WHERE equalities that turn a comma join into a real join: each
// conjunct of the form `col = col` whose two columns live in DIFFERENT
// top-level FROM items, keyed by the later of the two items (the one being
// joined in when the items are folded left to right).
//
// Only same-type columns qualify (a varchar(5) and a varchar(20) count as
// the same type, and so do an integer and a double — both compare by value,
// and the join matcher/hasher agree; see plan::conjuncts' same_type).
// Comparing any other pair of types is an error in the comparison itself
// (see plan::eval's same_type), so such an equality is left as a cross join
// + filter, which is what reports that error — a join would just find no
// matches and hide it. The equalities also stay in
// the WHERE filter afterwards: WHERE treats a NULL operand as "no match"
// while the join matches NULL keys to each other, and re-checking keeps the
// result exactly what cross-join-then-filter gave.
#[derive(Default)]
struct WhereJoins {
    // (later item, position a, position b) — a and b are flat positions.
    equalities: Vec<(usize, usize, usize)>,
}

impl WhereJoins {
    // `conjuncts` were analyzed over the flattened tables; `table_item[t]` is
    // the top-level FROM item flat table `t` belongs to.
    fn find(conjuncts: &Conjuncts, table_item: &[usize]) -> Self {
        let equalities = conjuncts
            .equi()
            .filter_map(|(_, left, right)| {
                let (ia, ib) = (table_item[left.table], table_item[right.table]);
                // Both columns in one FROM item: already joined inside it.
                (ia != ib).then_some((ia.max(ib), left.pos, right.pos))
            })
            .collect();
        Self { equalities }
    }

    fn is_empty(&self) -> bool {
        self.equalities.is_empty()
    }

    // The ON expression joining item `k` to items 0..k: every equality keyed
    // to `k`, AND-ed. Positions are already relative to the combined
    // (items 0..k, then k) row, which is the flat layout itself.
    fn on_expr_for(&self, k: usize) -> Option<EvalExpr> {
        self.equalities
            .iter()
            .filter(|(item, _, _)| *item == k)
            .map(|(_, a, b)| EvalExpr::Binary {
                lhs: Box::new(EvalExpr::Value(*a)),
                op: BinaryOp::Eq,
                rhs: Box::new(EvalExpr::Value(*b)),
            })
            .reduce(|l, r| EvalExpr::Binary {
                lhs: Box::new(l),
                op: BinaryOp::And,
                rhs: Box::new(r),
            })
    }
}

// The AND-ed terms of a condition.
fn conjunct_terms<'a>(e: &'a EvalExpr, out: &mut Vec<&'a EvalExpr>) {
    match e {
        EvalExpr::Binary {
            lhs,
            op: BinaryOp::And,
            rhs,
        } => {
            conjunct_terms(lhs, out);
            conjunct_terms(rhs, out);
        }
        other => out.push(other),
    }
}

struct SourceHolder {
    source: Box<dyn Source>,
    // How many columns at the END of `source`'s rows are not part of the
    // SELECT list: ORDER BY columns handle_select had to add so the sort
    // can see them. plan_query drops them again after sorting.
    hidden: usize,
    // The rows already come out in ORDER BY's order.
    sorted: bool,
}

impl<F> Visitor for QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    type Break = SchemaError;

    fn pre_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<Self::Break> {
        self.depth += 1;
        if self.depth == 1 {
            self.steps.push(None);
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        _tf: &sql_parser::query::TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        std::ops::ControlFlow::Continue(())
    }
    fn pre_visit_select(
        &mut self,
        _select: &sql_parser::query::SelectCore,
    ) -> std::ops::ControlFlow<Self::Break> {
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &sql_parser::Expr) -> std::ops::ControlFlow<Self::Break> {
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<Self::Break> {
        let outermost = self.depth == 1;
        self.depth -= 1;
        if !outermost {
            return std::ops::ControlFlow::Continue(());
        }
        match self.plan_query(query) {
            Ok(step) => self.steps.push(Some(step)),
            Err(e) => return std::ops::ControlFlow::Break(e),
        }
        std::ops::ControlFlow::Continue(())
    }
}

impl<F> QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new(conn: Arc<Connection<F>>, mem: Arc<QueryMemory>) -> Result<Self, SchemaError> {
        let stmt_txn = if conn.with_current_txn(|t| t.is_some()) {
            None
        } else {
            Some(conn.database.read().begin()?)
        };
        Ok(Self {
            conn,
            steps: vec![],
            depth: 0,
            mem,
            stmt_txn,
        })
    }

    // Plans one query into the Source that produces its rows: the SELECT,
    // then ORDER BY / LIMIT. Used for the statement's own (outermost) query
    // and for every FROM subquery.
    fn plan_query(&mut self, query: &Query) -> Result<Box<dyn Source>, SchemaError> {
        let SetOperand::Select(select) = &query.body else {
            return Err(SchemaError::UnsupportedFeature(
                "a query body other than a plain SELECT".into(),
            ));
        };
        // Resolved once, up front, and used by BOTH the ORDER BY and
        // no-ORDER-BY paths below — previously this was only ever
        // computed (and therefore ORDER BY only ever applied) inside
        // the `if let Some(limit) = ...` branch, so a bare `ORDER BY`
        // with no `LIMIT` at all silently did nothing: the query
        // still succeeded, just returned rows in scan order.
        let limit_count = match &query.limit {
            Some(limit) => match limit.count_i64() {
                Some(n) if n < 0 => return Err(SchemaError::InvalidLimitValue(n)),
                Some(n) => Some(n as usize),
                // A LIMIT that isn't a literal count (e.g. a bound
                // parameter) can't be resolved here — pre-existing
                // behavior, unchanged: treated as no limit rather
                // than erroring.
                None => None,
            },
            None => None,
        };
        let SourceHolder {
            source,
            hidden,
            sorted,
        } = self.handle_select(select, query.order_by.as_ref(), limit_count)?;
        let mut step = source;

        // Already in ORDER BY's order (read through a key that gives it —
        // see optim::picker::OrderWanted): no sort, just the LIMIT.
        if sorted {
            if let Some(limit_count) = limit_count {
                step = Box::new(Limit::new(step, limit_count));
            }
        } else if let Some(order) = &query.order_by {
            step = Box::new(SortSource::create_from(
                step,
                order,
                limit_count,
                self.conn.database.read().db.clone(),
                self.mem.clone(),
            )?);
        } else if let Some(limit_count) = limit_count {
            step = Box::new(Limit::new(step, limit_count));
        }
        if hidden > 0 {
            // Keep only the SELECT-list columns: each passes through by
            // position, under its own display name.
            let fields = step.fields();
            let visible = fields[..fields.len() - hidden]
                .iter()
                .enumerate()
                .map(|(i, f)| ProjectableField {
                    expr: EvalExpr::Value(i),
                    ..f.clone()
                })
                .collect();
            step = Box::new(Projection::new(step, visible));
        }
        Ok(step)
    }

    /// Opens a FROM item under the statement's transaction: the explicit
    /// block if one is open, else `stmt_txn`.
    fn open(
        &self,
        item: &TableRef<F>,
        stats: Option<ComputedTableStat>,
    ) -> Result<Box<dyn Source>, SchemaError> {
        self.conn.with_current_txn(|explicit| {
            item.open_source(&self.conn, stats, explicit.or(self.stmt_txn.as_ref()))
        })
    }

    fn handle_select(
        &mut self,
        select: &sql_parser::query::SelectCore,
        order_by: Option<&OrderByClause>,
        limit: Option<usize>,
    ) -> Result<SourceHolder, SchemaError> {
        let distinct = select.distinct.is_some();
        let tables = self.get_tables(&select.from)?;
        // `tables` mirrors the FROM clause: one entry per top-level item,
        // each possibly carrying its own joined-in relations nested
        // inside `.joins`. Every column-reference resolver (wildcard/
        // qualified-wildcard expansion, validate_field, flat_position)
        // only ever walks a flat `&[TableQuery<F>]` list matching
        // UnionJoin's own combined-row layout — none of them descend
        // into `.joins` — so without flattening first, a joined table's
        // columns are invisible to `SELECT *`, an explicit qualified
        // reference (`SELECT t2.val ...`), WHERE, and GROUP BY alike,
        // even though the physical JoinSource that actually produces the
        // row includes them. flat_tables is what every resolution call
        // below uses instead — `tables` itself is kept only for the
        // physical-source building loop right below, which needs the
        // nested `.joins` structure to know what to actually construct.
        // (ORDER BY is different: it resolves against the already-
        // projected SELECT-list output, not the raw FROM-clause tables
        // at all — see SortSource::create_from's own doc comment.)
        let flat_tables = Self::flatten_tables(&tables);
        let proj = self.get_projections(&select.projection, &flat_tables)?;
        let mut projected_fields = proj.into_iter().flatten().collect::<Vec<_>>();
        // ORDER BY may name a column the SELECT list leaves out (`SELECT
        // id FROM t ORDER BY created`). The sort only sees the projected
        // row, so each such column is projected too, as a hidden column
        // after the SELECT list, and dropped again after the sort (see
        // plan_query). Only plain column references: anything the sort
        // can't resolve anyway is left for it to report.
        let mut hidden = 0;
        for item in order_by.iter().flat_map(|o| o.items.items()) {
            if !matches!(item.expr, Expr::Column(_)) {
                continue;
            }
            if let Err(SchemaError::FieldNotFound(name)) =
                SortSource::<F>::resolve_order_by_index(&item.expr, &projected_fields)
            {
                if distinct {
                    // Which of several source rows would a DISTINCT row
                    // take its sort value from? Same rule as Postgres.
                    return Err(SchemaError::UnsupportedFeature(format!(
                        "ORDER BY {name} with SELECT DISTINCT: an ORDER BY column must appear in the SELECT list"
                    )));
                }
                projected_fields.push(self.handle_expr(&item.expr, &None, &flat_tables)?);
                hidden += 1;
            }
        }
        let has_aggregation = projected_fields.iter().any(|f| f.expr.has_aggregate());
        let projected_field_count = projected_fields.len();

        let wh_expr = if let Some(wh) = &select.where_clause {
            Some(*EvalExpr::from_expr(&wh.expr, &flat_tables)?)
        } else {
            None
        };

        // Each top-level FROM item's joins fold into a single left-deep
        // chain: the running `combined` source becomes the LEFT input to
        // the next join, so a query like `t1 JOIN t2 ON ... JOIN t3 ON
        // ...` produces ONE combined (t1++t2)++t3 source, not two
        // independent t1⋈t2 / t1⋈t3 pairings later cross-producted by
        // UnionJoin. That used to not just multiply the row count
        // wrong — since flatten_tables' logical column numbering
        // (t1, t2, t3 concatenated) already assumed this left-deep
        // layout, the mismatch against the OLD independent-pairings
        // layout made a plain SELECT read the wrong physical column
        // outright (confirmed via direct repro: an integer column came
        // back holding a string value from an unrelated table).
        let shapes = Self::table_shapes(&tables, &flat_tables);
        // GROUP BY is resolved again (and validated) by validate_aggreations;
        // here it only contributes the columns it reads.
        let group_by = match &select.group_by {
            Some(g) => g
                .exprs
                .items()
                .map(|e| EvalExpr::from_expr(e, &flat_tables).map(|e| *e))
                .collect::<Result<Vec<_>, _>>()?,
            None => vec![],
        };
        let reads: Vec<&EvalExpr> = projected_fields
            .iter()
            .map(|f| &f.expr)
            .chain(&group_by)
            .collect();
        let needs = analyze_query(&tables, &flat_tables, &shapes, &reads, wh_expr.as_ref());
        // The order ORDER BY wants, when one table's key could provide it:
        // a single table, nothing grouping or deduplicating rows, and every
        // ORDER BY item an ascending plain column of it.
        let order_wanted = match (order_by, tables.as_slice()) {
            (Some(order), [only]) if only.joins.is_empty() && !has_aggregation && !distinct => {
                order
                    .items
                    .items()
                    .map(|item| {
                        let ascending = item.direction.map(|d| d.is_left()).unwrap_or(true);
                        let nulls_first = item.nulls.map(|(_, n)| n.is_left()).unwrap_or(false);
                        let i =
                            SortSource::<F>::resolve_order_by_index(&item.expr, &projected_fields)
                                .ok()?;
                        match projected_fields[i].expr {
                            EvalExpr::Value(column) if ascending => Some((column, nulls_first)),
                            _ => None,
                        }
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|columns| OrderWanted { columns, limit })
            }
            _ => None,
        };
        // needs is in flat order: each FROM item, then its joined relations.
        let mut accesses = vec![];
        // Per flat item: its WHERE conditions were applied at its scan (or
        // read exactly by its seek), so the top-level WHERE can leave them.
        let mut pushed = vec![];
        let all_needs = &needs;
        let mut needs = needs.iter();
        let mut sources = vec![];
        // Per top-level item: its estimated rows (when known), its row's
        // column types, and its first flat index.
        let mut item_rows = vec![];
        let mut item_types = vec![];
        let mut item_start = vec![];
        for table in tables.iter() {
            item_start.push(accesses.len());
            let needs_t = needs.next();
            let (combined, access) = self.open_item(table, needs_t, order_wanted.as_ref())?;
            let (mut combined, rows) =
                self.push_filters(combined, table, needs_t, access.as_ref())?;
            // Rows so far on the join's outer side, when known — what decides
            // whether seeking the next table per row beats a hash join.
            let mut outer_rows = rows;
            accesses.push(access);
            pushed.push(true);
            // The column types of the joined-so-far row, which each join's
            // ON positions index (see get_tables).
            let mut left_types: Vec<DataType> = table.fields.iter().map(|f| f.datatype).collect();
            for j in &table.joins {
                let needs_j = needs.next();
                if let Some(join) = self.nested_loop_join(
                    &mut combined,
                    j.join_type,
                    &j.relation,
                    &j.on_expr,
                    needs_j,
                    outer_rows,
                    &left_types,
                )? {
                    outer_rows = outer_rows.map(|r| r * join.1);
                    combined = join.0;
                    accesses.push(None);
                    // Its rows come from the join itself: WHERE checks them.
                    pushed.push(false);
                } else {
                    let (relation, access) = self.open_item(&j.relation, needs_j, None)?;
                    let (relation, _) =
                        self.push_filters(relation, &j.relation, needs_j, access.as_ref())?;
                    accesses.push(access);
                    pushed.push(true);
                    let outer = std::mem::replace(&mut combined, Box::new(UnionJoin::new(vec![])?));
                    combined = Box::new(JoinSource::new(
                        outer,
                        relation,
                        j.on_expr.clone(),
                        j.join_type,
                        self.conn.database.read().db.clone(),
                        self.mem.clone(),
                    )?);
                    outer_rows = None;
                }
                left_types.extend(j.relation.fields.iter().map(|f| f.datatype));
            }
            sources.push(combined);
            item_rows.push(outer_rows);
            item_types.push(left_types);
        }
        // Top-level FROM items (`FROM a, b, c`) are implicitly cross joined,
        // but a WHERE equality between two of them (`a.id = b.id`) is
        // really an inner equi-join: fold the items left to right, joining
        // each to everything before it with a hash join on whichever WHERE
        // equalities link it to those, and only fall back to a cross join
        // where nothing links them. The combined row layout is unchanged
        // (items in FROM order), so every resolved column position stays
        // valid. See WhereJoins.
        let links = if sources.len() > 1 {
            // Which tables an outer join in their FROM item NULL-extends,
            // in flat order (each item is a table followed by its joins).
            // flat_tables is each top-level item followed by its joins.
            let table_item: Vec<usize> = tables
                .iter()
                .enumerate()
                .flat_map(|(i, t)| std::iter::repeat_n(i, 1 + t.joins.len()))
                .collect();
            let conjuncts = Conjuncts::analyze(wh_expr.as_ref(), &shapes);
            WhereJoins::find(&conjuncts, &table_item)
        } else {
            WhereJoins::default()
        };
        let union: Box<dyn Source> = if let [_] = sources.as_slice() {
            sources.pop().unwrap()
        } else if links.is_empty() {
            // UnionJoin only does real work (cross-producting) when there's
            // more than one top-level FROM item to combine, and no FROM at
            // all (`SELECT 1+2`, sources empty) is the degenerate case; the
            // overwhelmingly common single-item case skips it entirely
            // rather than re-flattening one source's rows for nothing.
            Box::new(UnionJoin::new(sources)?)
        } else {
            let mut items = sources.into_iter();
            let mut combined = items.next().expect("more than one source");
            let mut rows = item_rows[0];
            let mut types = item_types[0].clone();
            for (k, next) in items.enumerate() {
                let k = k + 1;
                match links.on_expr_for(k) {
                    Some(on_expr) => {
                        // An item that is one table can be joined by seeking
                        // it per row, like an ON join (its own scan, opened
                        // above, then goes unused).
                        let flat = item_start[k];
                        let seek = if tables[k].joins.is_empty() {
                            self.nested_loop_join(
                                &mut combined,
                                JoinType::Inner,
                                &tables[k],
                                &on_expr,
                                all_needs.get(flat),
                                rows,
                                &types,
                            )?
                        } else {
                            None
                        };
                        if let Some((join, per_row)) = seek {
                            combined = join;
                            rows = rows.map(|r| r * per_row);
                            accesses[flat] = None;
                            pushed[flat] = false;
                        } else {
                            combined = Box::new(JoinSource::new(
                                combined,
                                next,
                                on_expr,
                                JoinType::Inner,
                                self.conn.database.read().db.clone(),
                                self.mem.clone(),
                            )?);
                            rows = None;
                        }
                    }
                    None => {
                        combined = Box::new(UnionJoin::new(vec![combined, next])?);
                        rows = None;
                    }
                }
                types.extend(item_types[k].iter().copied());
            }
            combined
        };
        // WHERE, less the conditions already applied to one table: those
        // local to it and applicable before any join, which push_filters
        // checked at its scan (or its seek read exactly). Every row that
        // table contributes satisfies them, and joins never change a row's
        // values.
        let wh_expr = wh_expr.and_then(|wh| {
            let conjuncts = Conjuncts::analyze(Some(&wh), &shapes);
            let checked: Vec<_> = conjuncts
                .iter()
                .filter(|c| {
                    let ConjunctKind::Local(t) = c.kind else {
                        return true;
                    };
                    let applied = c.pushable
                        && pushed.get(t) == Some(&true)
                        && c.local_expr(&conjuncts).is_some();
                    !applied
                })
                .collect();
            Conjuncts::and_all(checked)
        });
        let for_proj: Box<dyn Source> = if let Some(wh_expr) = wh_expr {
            Box::new(WhereSource::new(union, wh_expr)?)
        } else {
            union
        };

        // A GROUP BY (or a bare aggregate with no GROUP BY at all, i.e.
        // one implicit group over the whole table) has to accumulate
        // over *raw* rows, one row at a time, before ever evaluating the
        // SELECT list — an aggregate's own output column changes on
        // every row it's fed, so evaluating the SELECT list eagerly per
        // raw row first (what DISTINCT's path below does, and what an
        // earlier version of this tried to reuse for GROUP BY too) can
        // never collapse anything: no two rows in the same group would
        // ever compare equal on their already-evaluated aggregate
        // column. See GroupSource's own doc comment.
        let projected: Box<dyn Source> = if has_aggregation {
            let key_positions =
                self.validate_aggreations(&projected_fields, &flat_tables, &select.group_by)?;
            let grouped_source: Box<dyn Source> = if key_positions.is_empty() {
                for_proj
            } else {
                Box::new(SortSource::with_fields(
                    for_proj,
                    self.conn.database.read().db.clone(),
                    self.mem.clone(),
                    &key_positions,
                )?)
            };
            Box::new(GroupSource::new(
                grouped_source,
                projected_fields.clone(),
                key_positions,
            ))
        } else {
            Box::new(Projection::new(for_proj, projected_fields.clone()))
        };
        // DISTINCT has to dedup on the *projected* row, not the raw table
        // row: AggregatingSource compares whole rows, so it only sees
        // duplicates once every column it's comparing has actually been
        // narrowed down to the SELECT list first. Sorting/deduping the
        // wide raw row and projecting afterward (an earlier version of
        // this) is why every row came back unchanged — two rows with the
        // same `name` but a different value in any other column of the
        // table (an id column, say) still compare unequal on the raw
        // row, so nothing ever collapsed.
        let source = if distinct {
            let sorted = SortSource::with_fields(
                projected,
                self.conn.database.read().db.clone(),
                self.mem.clone(),
                &(0..projected_field_count).collect::<Vec<_>>(),
            )?;
            Box::new(AggregatingSource::new(Box::new(sorted))?)
        } else {
            projected
        };

        let sorted = order_wanted.is_some() && matches!(accesses.as_slice(), [Some(a)] if a.sorted);
        Ok(SourceHolder {
            source,
            hidden,
            sorted,
        })
    }

    fn get_projections(
        &self,
        proj: &Seq<SelectItem, Comma>,
        tables: &[TableQuery<F>],
    ) -> Result<Vec<Vec<ProjectableField>>, SchemaError> {
        let mut res = vec![];
        for i in proj.items() {
            res.push(self.get_proj(i, tables)?);
        }
        Ok(res)
    }

    fn get_proj(
        &self,
        proj: &SelectItem,
        tables: &[TableQuery<F>],
    ) -> Result<Vec<ProjectableField>, SchemaError> {
        match proj {
            SelectItem::Expr { expr, alias } => Ok(vec![self.handle_expr(expr, alias, tables)?]),
            SelectItem::Wildcard(_) => {
                let mut v = vec![];
                for (sid, t) in tables.iter().enumerate() {
                    for (fid, f) in t.fields.iter().enumerate() {
                        v.push(ProjectableField::new_with_field(
                            f.name.clone(),
                            f.clone(),
                            sid,
                            fid,
                            EvalExpr::Value(EvalExpr::flat_position(tables, sid, fid)),
                        ));
                    }
                }
                Ok(v)
            }
            SelectItem::QualifiedWildcard(ob, _, _) => {
                let mut v = vec![];
                let ob = ob.idents().map(|n| n.value.as_str()).collect::<Vec<_>>();
                if ob.len() == 1
                    && let Some(pos) = tables.iter().position(|n| {
                        ob[0].eq_ignore_ascii_case(&n.alias) || ob[0].eq_ignore_ascii_case(&n.table)
                    })
                {
                    for (fid, f) in tables[pos].fields.iter().enumerate() {
                        v.push(ProjectableField::new_with_field(
                            f.name.clone(),
                            f.clone(),
                            pos,
                            fid,
                            EvalExpr::Value(EvalExpr::flat_position(tables, pos, fid)),
                        ));
                    }
                    return Ok(v);
                }
                Err(SchemaError::BadTableName(format!("{:?}", ob)))
            }
        }
    }

    fn handle_expr(
        &self,
        expr: &Expr,
        alias: &Option<Alias>,
        tables: &[TableQuery<F>],
    ) -> Result<ProjectableField, SchemaError> {
        let eval_expr = EvalExpr::from_expr(expr, tables)?;
        // The one place a display name/Field actually matter — a SELECT-
        // list item needs a result-set column header — so this is where
        // ProjectedField gets built now, not inside from_expr's own
        // recursion (see EvalExpr::from_expr's own doc comment). An
        // explicit alias always wins; a bare column reference falls back
        // to its own name (`select id from t` still heads its column
        // "id"); anything else (an expression, a function call, ...) has
        // no name of its own yet.
        let display_name = match alias {
            Some(alias) => alias.name.value.clone(),
            None => match expr {
                Expr::Column(c) => c
                    .idents()
                    .last()
                    .map(|i| i.value.clone())
                    .unwrap_or_else(|| "none".into()),
                _ => "none".into(),
            },
        };
        Ok(ProjectableField::new_with_field(
            display_name.clone(),
            Arc::new(Field::from(display_name)),
            0,
            0,
            *eval_expr,
        ))
    }

    // Expands each top-level FROM item's nested `.joins` into its own
    // flat entry, matching the physical row layout UnionJoin/JoinSource
    // actually produce (this table's fields, then its joined relation's
    // fields, in order) — see handle_select's own comment on why every
    // column-reference resolver needs this instead of the nested
    // `tables` list. One level of flattening is enough: `get_tables`
    // only ever pushes joins onto the FIRST table of a FROM item
    // (mirroring TableWithJoins' own shape — a Join's own `relation` is
    // just a TableFactor, never something with joins of its own), so a
    // joined-in relation's own `.joins` is always empty already.
    // Each flat table's column types, and whether an outer join in its
    // FROM item NULL-extends it — what Conjuncts::analyze classifies WHERE
    // conjuncts against.
    fn table_shapes(tables: &[TableQuery<F>], flat_tables: &[TableQuery<F>]) -> Vec<TableShape> {
        let supplied = tables.iter().flat_map(|t| {
            null_supplied_tables(&t.joins.iter().map(|j| j.join_type).collect::<Vec<_>>())
        });
        flat_tables
            .iter()
            .zip(supplied)
            .map(|(t, null_supplied)| TableShape {
                columns: t.fields.iter().map(|f| f.datatype).collect(),
                null_supplied,
            })
            .collect()
    }

    // Opens one FROM item. A real table is read the way optim::picker
    // chooses (a scan or seek of the table or one of its indexes); anything
    // else is opened as is.
    // Applies a FROM item's own WHERE conditions (ItemNeeds::filters, the
    // stated ones) right at its scan, below any join, leaving out those its
    // seek reads exactly (Access::enforced). Returns the source and how
    // many rows it is expected to produce, when known.
    fn push_filters(
        &self,
        source: Box<dyn Source>,
        item: &TableQuery<F>,
        needs: Option<&ItemNeeds>,
        access: Option<&Access>,
    ) -> Result<(Box<dyn Source>, Option<usize>), SchemaError> {
        let rows = access.and_then(|a| a.rows);
        let Some(needs) = needs else {
            return Ok((source, rows));
        };
        let types: Vec<DataType> = item.fields.iter().map(|f| f.datatype).collect();
        let remaining: Vec<&EvalExpr> = needs.filters[..needs.stated]
            .iter()
            .filter(|f| {
                let enforced = access.is_some_and(|a| {
                    condition_set(f, &types).is_some_and(|(column, _)| a.enforced.contains(&column))
                });
                !enforced
            })
            .collect();
        let rows = match (&item.resolved, rows) {
            (TableRef::Real(_, table), Some(rows)) => {
                Some(filtered_rows(table, item.stats.as_ref(), rows, &remaining))
            }
            _ => rows,
        };
        let filter = remaining
            .into_iter()
            .cloned()
            .reduce(|l, r| EvalExpr::Binary {
                lhs: Box::new(l),
                op: BinaryOp::And,
                rhs: Box::new(r),
            });
        Ok(match filter {
            Some(filter) => (Box::new(WhereSource::new(source, filter)?), rows),
            None => (source, rows),
        })
    }

    // Joins `j`'s table onto `outer` by seeking it once per outer row (see
    // source::nestloop), when optim::picker::pick_join_seek finds that
    // cheaper than a hash join; returns the join and its rows per outer row.
    // `outer` is taken only when it does. `left_types` are the outer row's
    // column types.
    #[allow(clippy::too_many_arguments)]
    fn nested_loop_join(
        &self,
        outer: &mut Box<dyn Source>,
        join_type: JoinType,
        relation: &TableQuery<F>,
        on_expr: &EvalExpr,
        needs: Option<&ItemNeeds>,
        outer_rows: Option<usize>,
        left_types: &[DataType],
    ) -> Result<Option<(Box<dyn Source>, usize)>, SchemaError> {
        let (JoinType::Inner | JoinType::Left, TableRef::Real(_, table), Some(needs)) =
            (join_type, &relation.resolved, needs)
        else {
            return Ok(None);
        };
        let Some(reader) = self
            .conn
            .with_current_txn(|explicit| explicit.or(self.stmt_txn.as_ref()).map(|t| t.id()))
        else {
            return Ok(None);
        };
        // The ON equalities between the outer row and this table.
        let left_width = left_types.len();
        let mut terms = vec![];
        conjunct_terms(on_expr, &mut terms);
        let pairs: Vec<(usize, usize, DataType)> = terms
            .iter()
            .filter_map(|t| match t {
                EvalExpr::Binary {
                    lhs,
                    op: BinaryOp::Eq,
                    rhs,
                } => match (lhs.as_ref(), rhs.as_ref()) {
                    (EvalExpr::Value(a), EvalExpr::Value(b))
                        if *a < left_width && *b >= left_width =>
                    {
                        Some((*a, *b - left_width, left_types[*a]))
                    }
                    (EvalExpr::Value(b), EvalExpr::Value(a))
                        if *a < left_width && *b >= left_width =>
                    {
                        Some((*a, *b - left_width, left_types[*a]))
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        let db = self.conn.database.read().db.clone();
        let page_size = db.get_page_data_size();
        // What a hash join would read of this table: however it would be
        // read on its own.
        let hash_rows = pick_access(table, relation.stats.as_ref(), needs, page_size, None)
            .rows
            .unwrap_or(usize::MAX / 2);
        let Some(seek) = pick_join_seek(
            table,
            relation.stats.as_ref(),
            &pairs,
            outer_rows,
            hash_rows,
            page_size,
        ) else {
            return Ok(None);
        };
        let rows_per_key = seek.rows_per_key;
        let outer = std::mem::replace(outer, Box::new(UnionJoin::new(vec![])?));
        let join = NestedLoopJoin::new(
            outer,
            db,
            reader,
            table.clone(),
            seek,
            on_expr.clone(),
            join_type,
        )?;
        Ok(Some((Box::new(join), rows_per_key)))
    }

    // Also returns the chosen access (None for anything but a real table),
    // for what the rest of the plan can rely on (see picker::Access).
    fn open_item(
        &self,
        item: &TableQuery<F>,
        needs: Option<&ItemNeeds>,
        order: Option<&OrderWanted>,
    ) -> Result<(Box<dyn Source>, Option<Access>), SchemaError> {
        let (TableRef::Real(_, table), Some(needs)) = (&item.resolved, needs) else {
            return Ok((self.open(&item.resolved, item.stats.clone())?, None));
        };
        let db = self.conn.database.read().db.clone();
        let access = pick_access(
            table,
            item.stats.as_ref(),
            needs,
            db.get_page_data_size(),
            order,
        );
        let stats = item.stats.clone();
        let chosen = access.clone();
        let source = self.conn.with_current_txn(|explicit| {
            let txn = explicit.or(self.stmt_txn.as_ref());
            let source: Box<dyn Source> = match access.path {
                AccessPath::TableScan => return item.resolved.open_source(&self.conn, stats, txn),
                AccessPath::TableSeek(range) => Box::new(TableSource::seek(
                    db,
                    table.clone(),
                    txn,
                    stats,
                    range,
                    access.rows,
                )?),
                AccessPath::IndexScan(i) => {
                    Box::new(IndexSource::new(&self.conn, table, i, txn, stats)?)
                }
                AccessPath::IndexSeek(i, range) => Box::new(IndexSource::seek(
                    &self.conn,
                    table,
                    i,
                    txn,
                    stats,
                    range,
                    false,
                    access.rows,
                )?),
                AccessPath::IndexLookup(i, range) => Box::new(IndexSource::seek(
                    &self.conn,
                    table,
                    i,
                    txn,
                    stats,
                    range,
                    true,
                    access.rows,
                )?),
            };
            Ok(source)
        })?;
        Ok((source, Some(chosen)))
    }

    fn flatten_tables(tables: &[TableQuery<F>]) -> Vec<TableQuery<F>> {
        let mut flat = vec![];
        for t in tables {
            // Base table's own fields come first in the physical row —
            // see handle_select's source-building loop: JoinSource's
            // combine() always emits left (this table) ++ right (the
            // joined relation).
            flat.push(t.clone());
            for j in &t.joins {
                flat.push(j.relation.clone());
            }
        }
        flat
    }

    fn get_tables(&mut self, from: &Option<FromClause>) -> Result<Vec<TableQuery<F>>, SchemaError> {
        if from.is_none() {
            return Ok(vec![]);
        }
        let from = from.as_ref().unwrap();
        let mut tables = vec![];
        let items = from.tables.items();
        for qtable in items {
            let mut table = self.get_table(&qtable.relation)?;
            // Every join in this FROM item folds into a single left-deep
            // chain at execution time (see handle_select's
            // source-building loop: each JoinSource's own output becomes
            // the NEXT join's left input) — so a later join's ON clause
            // can reference ANY table already joined so far, not just
            // the base `table`, and its column positions must be
            // resolved against that FULL running list (matching the
            // running left side's actual field width at that point), not
            // just a throwaway [table, relation] pair. Getting this
            // wrong doesn't just reject a valid reference to an earlier
            // joined table — it can silently miscompute BOTH sides'
            // positions for a join that only references the base table
            // too, since a position encoded relative to a 2-table
            // [table, relation] resolution can come out numerically
            // valid-but-wrong once interpreted against the wider running
            // left side.
            let mut joined_so_far = vec![table.clone()];
            for j in &qtable.joins {
                let join_type = match &j.operator {
                    JoinOperator::Cross(_, _) => JoinType::Cross,
                    JoinOperator::FullOuter(_, _, _) => JoinType::Full,
                    JoinOperator::Inner(_, _) => JoinType::Inner,
                    JoinOperator::LeftOuter(_, _, _) => JoinType::Left,
                    JoinOperator::Plain(_) => JoinType::Inner,
                    JoinOperator::RightOuter(_, _, _) => JoinType::Right,
                };
                let relation = self.get_table(&j.relation)?;
                let on_expr = if let Some(constraint) = &j.constraint {
                    match constraint {
                        JoinConstraint::On(_, expr) => {
                            if matches!(join_type, JoinType::Cross) {
                                return Err(SchemaError::UserError(
                                    "Cross joins cannot have ON clause".into(),
                                ));
                            }
                            let mut resolve_against = joined_so_far.clone();
                            resolve_against.push(relation.clone());
                            let proj = self.handle_expr(expr, &None, &resolve_against)?;
                            proj.expr
                        }
                        JoinConstraint::Using(_, _, _, _) => {
                            return Err(SchemaError::UnsupportedFeature(
                                "joins with USING. Use ON instead.".into(),
                            ));
                        }
                    }
                } else {
                    if !matches!(join_type, JoinType::Cross) {
                        return Err(SchemaError::UserError(
                            "Non cross joins need a join constraint".into(),
                        ));
                    }
                    EvalExpr::None
                };
                joined_so_far.push(relation.clone());
                table.joins.push(JoinRelation {
                    join_type,
                    relation,
                    on_expr,
                })
            }
            tables.push(table);
        }
        Ok(tables)
    }

    fn get_table(&mut self, factor: &TableFactor) -> Result<TableQuery<F>, SchemaError> {
        let tq = if let TableFactor::Table { name, alias } = &factor {
            let (table, field) = self.conn.resolve_object_name_ref(name)?;
            crate::stmt::reject_qualified_field("a FROM target", field)?;
            if let TableRef::Real(schema, sqltable) = &table {
                TableQuery {
                    alias: alias
                        .clone()
                        .map(|a| a.name.value.clone())
                        .unwrap_or(sqltable.name.clone()),
                    resolved: table.clone(),
                    fields: sqltable.fields_arc(),
                    schema: schema.name.clone(),
                    table: sqltable.name.clone(),
                    joins: vec![],
                    stats: compute_table_stats(&self.conn, &schema.name, sqltable)?,
                }
            } else if let TableRef::Temp(schema, temptable) = &table {
                TableQuery {
                    schema: schema.clone(),
                    table: temptable.read().name.clone(),
                    alias: alias
                        .clone()
                        .map(|a| a.name.value.clone())
                        .unwrap_or(temptable.read().name.clone()),
                    fields: temptable.resolved_fields(),
                    resolved: table.clone(),
                    joins: vec![],
                    stats: None,
                }
            } else {
                todo!()
            }
        } else if let TableFactor::Derived { query, alias, .. } = factor {
            // Planned right here, as a self-contained inner query whose
            // rows the outer query then reads like a table. It shares this
            // statement's transaction (one snapshot) and memory budget.
            let alias = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .ok_or_else(|| {
                    SchemaError::UserError("every table in FROM needs an alias".into())
                })?;
            let inner = self.plan_query(query)?;
            let fields: Arc<[Arc<Field>]> =
                inner.fields().iter().map(|f| f.field.clone()).collect();
            let stats = inner.table_stats();
            TableQuery {
                schema: String::new(),
                table: alias.clone(),
                alias: alias.clone(),
                fields,
                resolved: TableRef::Derived(alias, DerivedSource::new(inner)),
                joins: vec![],
                stats,
            }
        } else {
            unreachable!("TableFactor is Table or Derived")
        };
        Ok(tq)
    }
    // Returns the raw table field positions to group by — empty means
    // "no GROUP BY clause at all," i.e. one implicit group over the
    // whole table (a bare aggregate like `SELECT COUNT(*) FROM t`), a
    // legitimate case in its own right, not something to skip grouping
    // for (see GroupSource, which treats an empty key list the same
    // way: one group covering every row, even zero rows).
    fn validate_aggreations(
        &self,
        fields: &[ProjectableField],
        tables: &[TableQuery<F>],
        group: &Option<GroupByClause>,
    ) -> Result<Vec<usize>, SchemaError> {
        let group_by = if let Some(group) = group {
            let mut v = vec![];
            for f in group.exprs.items() {
                v.push(self.handle_expr(f, &None, tables)?);
            }
            v
        } else {
            vec![]
        };
        let non_agg_fields = fields
            .iter()
            .flat_map(|f| f.expr.get_non_agg_fields())
            .collect::<Vec<_>>();
        if !non_agg_fields.is_empty() && group_by.is_empty() {
            let mut names = String::new();
            for n in fields {
                if !n.expr.has_aggregate() {
                    names += format!("{},", n.display_name.clone()).as_str();
                }
            }
            return Err(SchemaError::GroupByMissingField(names));
        }
        let group_by_positions = group_by
            .iter()
            .filter_map(|g| match &g.expr {
                EvalExpr::Value(u) => Some(*u),
                _ => None,
            })
            .collect::<Vec<_>>();
        // Standard SQL rule, and the reverse of what this checked
        // before: every non-aggregate SELECT column must appear in
        // GROUP BY (so its value is well-defined once rows collapse
        // into groups) — but a GROUP BY column need NOT appear in the
        // SELECT list at all (`GROUP BY category` alone, selecting only
        // `count(*)`, is perfectly valid).
        for n in fields {
            if n.expr.has_aggregate() {
                continue;
            }
            for pos in n.expr.get_non_agg_fields() {
                if !group_by_positions.contains(&pos) {
                    return Err(SchemaError::GroupByMissingField(n.display_name.clone()));
                }
            }
        }
        Ok(group_by_positions)
    }
}

#[allow(unused)]
impl<F> LogicalPlan<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(conn: Arc<Connection<F>>) -> Self {
        Self::with_memory_limit(conn, DEFAULT_QUERY_MEMORY_LIMIT)
    }

    pub(crate) fn with_memory_limit(conn: Arc<Connection<F>>, limit: usize) -> Self {
        Self {
            tail: None,
            stmt_txn: None,
            mem: QueryMemory::new(limit),
            start: Instant::now(),
            _phanton: PhantomData,
        }
    }

    pub(crate) fn build(conn: Arc<Connection<F>>, query: &Query) -> Result<Self, SchemaError> {
        let start = Instant::now();
        let mem = QueryMemory::new(DEFAULT_QUERY_MEMORY_LIMIT);
        let mut visitor = QueryVisitor::new(conn.clone(), mem.clone())?;
        if let std::ops::ControlFlow::Break(e) = query.visit(&mut visitor) {
            return Err(e);
        }
        let stmt_txn = visitor.stmt_txn.take();
        let mut this = Self {
            tail: None,
            stmt_txn,
            mem,
            start,
            _phanton: PhantomData,
        };
        assert!(visitor.steps.len() == 2);
        if let Some(Some(step)) = visitor.steps.into_iter().last() {
            //this.add_step(step);
            this.tail = Some(step)
        }
        Ok(this)
    }

    // Clone of this query's memory budget handle — grab this *before*
    // constructing a step that needs to reserve against it (see
    // QueryMemory::try_reserve), then build the step with it, then pass
    // the step to add_step. Not threaded through add_step/Source::chain
    // itself: most steps (any plain streaming Source) never touch it at
    // all, so forcing it through the trait for every step wouldn't earn
    // its keep until there's a real buffering step to design that
    // wiring against.
    pub(crate) fn memory(&self) -> Arc<QueryMemory> {
        self.mem.clone()
    }

    // The plan this query would run, as a tree — without running it. Built
    // from the same Source tree execute() would hand to the result set, so
    // EXPLAIN cannot drift from what really executes.
    pub(crate) fn explain(&self) -> Result<PlanNode, SchemaError> {
        self.tail
            .as_ref()
            .map(|t| t.plan())
            .ok_or(SchemaError::InternalSchemaError("Nothing in plan".into()))
    }

    pub(crate) fn execute(&mut self) -> Result<StreamingResultSet, SchemaError> {
        let tail = self
            .tail
            .take()
            .ok_or(SchemaError::InternalSchemaError("Nothing in plan".into()))?;
        Ok(StreamingResultSet::new(tail, self.start).owning_transaction(self.stmt_txn.take()))
    }
}

impl<F: DBFile + 'static> Clone for TableQuery<F> {
    fn clone(&self) -> Self {
        Self {
            alias: self.alias.clone(),
            fields: self.fields.clone(),
            joins: self.joins.clone(),
            resolved: self.resolved.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl<F: DBFile + 'static> Clone for JoinRelation<F> {
    fn clone(&self) -> Self {
        Self {
            join_type: self.join_type,
            on_expr: self.on_expr.clone(),
            relation: self.relation.clone(),
        }
    }
}
