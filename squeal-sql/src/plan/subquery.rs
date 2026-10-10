//! Subqueries inside expressions: `x [NOT] IN (SELECT ...)`, `[NOT] EXISTS
//! (SELECT ...)` and a scalar `(SELECT ...)` used as a value.
//!
//! Each is run once, while the query holding it is planned, inside that
//! query's transaction — never once per outer row:
//!
//! - **Uncorrelated** (reads nothing of the outer query): its rows are the
//!   answer. IN keeps them as a hash set; EXISTS keeps whether there was
//!   one; a scalar subquery keeps its one value.
//! - **Correlated through equalities** — `EXISTS (SELECT 1 FROM c WHERE
//!   c.pid = p.id AND c.qty > 5)`: the conditions comparing an inner
//!   expression with an outer one are taken out and their inner sides
//!   selected instead, which leaves an uncorrelated query (`SELECT c.pid
//!   FROM c WHERE c.qty > 5`). Its rows are grouped by those keys, and
//!   each outer row looks up its own key — a hash semi-join (or, for NOT,
//!   anti-join) in place of a nested loop.
//!
//! Anything else correlated (an outer column outside such an equality, a
//! correlated scalar subquery, or one that groups, aggregates or limits)
//! is refused rather than answered wrongly.
//!
//! How the planned answer reaches `EvalExpr::from_expr`, which compiles an
//! expression with no access to the planner: `register` files each answer
//! under its `Query` node's address for the current thread, for as long as
//! the returned guard lives — the planning of the one SELECT (or UPDATE /
//! DELETE) that contains it.

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use sql_parser::{
    Expr, Query,
    expr::{BinaryOp, FunctionArg},
    query::{FromClause, SelectItem, SetOperand, WhereClause},
    span::TokenSpan,
    token::Comma,
    utils::Seq,
};
use store::{db::DBFile, valueitem::ValueItem};

use crate::{
    error::SchemaError,
    plan::{eval::EvalExpr, logical::TableQuery},
    source::Source,
};

/// What a subquery is asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Exists,
    In,
    Scalar,
}

// The rows that share one correlation key (all of them, uncorrelated).
#[derive(Debug, Default)]
struct Group {
    // IN: the non-NULL values, normalized (see `normalize`).
    values: HashSet<ValueItem>,
    // IN: whether a NULL was among them.
    has_null: bool,
    // Scalar: the one value.
    value: Option<ValueItem>,
}

/// A subquery's answer: its rows, grouped by correlation key (the empty
/// key when uncorrelated). A key with no group had no rows.
#[derive(Debug)]
pub(crate) struct SubqueryData {
    kind: Kind,
    groups: HashMap<Vec<ValueItem>, Group>,
    rows: usize,
}

/// A subquery in a compiled expression: what it asks, the expressions of
/// the outer row that pick its group (none when uncorrelated), and for IN
/// the value tested.
#[derive(Clone)]
pub struct SubqueryExpr {
    pub(crate) kind: Kind,
    pub(crate) lhs: Option<Box<EvalExpr>>,
    pub(crate) keys: Vec<EvalExpr>,
    pub(crate) data: Arc<SubqueryData>,
}

impl std::fmt::Debug for SubqueryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubqueryExpr")
            .field("kind", &self.kind)
            .field("lhs", &self.lhs)
            .field("keys", &self.keys)
            .field("rows", &self.data.rows)
            .finish()
    }
}

impl SubqueryExpr {
    // Every expression of the outer row this reads, in order.
    pub(crate) fn operands(&self) -> impl Iterator<Item = &EvalExpr> {
        self.lhs.iter().map(|b| &**b).chain(&self.keys)
    }

    pub(crate) fn operands_mut(&mut self) -> impl Iterator<Item = &mut EvalExpr> {
        self.lhs.iter_mut().map(|b| &mut **b).chain(&mut self.keys)
    }

    pub(crate) fn describe(&self, names: &[String]) -> String {
        let keyed = if self.keys.is_empty() {
            String::new()
        } else {
            let keys = self.keys.iter().map(|k| k.describe(names)).collect::<Vec<_>>();
            format!(" by {}", keys.join(", "))
        };
        let what = format!("subquery: {} row(s){keyed}", self.data.rows);
        match (&self.kind, &self.lhs) {
            (Kind::In, Some(lhs)) => format!("{} IN ({what})", lhs.describe(names)),
            (Kind::Exists, _) => format!("EXISTS ({what})"),
            _ => format!("({what})"),
        }
    }

    /// The answer for one outer row, given its operands' values in
    /// `operands` order (see `eval_with`).
    pub(crate) fn answer(&self, mut operands: Vec<ValueItem>) -> ValueItem {
        let key: Vec<ValueItem> = operands.split_off(usize::from(self.lhs.is_some()));
        // `=` with a NULL is never true: a NULL key matches no inner row.
        let group = if key.iter().any(|v| matches!(v, ValueItem::Null)) {
            None
        } else {
            self.data.groups.get(&key.into_iter().map(normalize).collect::<Vec<_>>())
        };
        match self.kind {
            Kind::Exists => ValueItem::Boolean(group.is_some()),
            Kind::Scalar => group.and_then(|g| g.value.clone()).unwrap_or(ValueItem::Null),
            // SQL's three-valued IN: FALSE over no rows, NULL for a NULL
            // value, TRUE on a match, else NULL if the rows held a NULL.
            Kind::In => {
                let Some(group) = group else {
                    return ValueItem::Boolean(false);
                };
                let v = operands.pop().unwrap_or(ValueItem::Null);
                if matches!(v, ValueItem::Null) {
                    ValueItem::Null
                } else if group.values.contains(&normalize(v)) {
                    ValueItem::Boolean(true)
                } else if group.has_null {
                    ValueItem::Null
                } else {
                    ValueItem::Boolean(false)
                }
            }
        }
    }
}

// A value as a hash key, so that values `=` calls equal hash alike: a
// whole double is the integer it equals (`1 = 1.0`).
fn normalize(v: ValueItem) -> ValueItem {
    match v {
        ValueItem::Double(d)
            if d.fract() == 0.0 && d >= i64::MIN as f64 && d < i64::MAX as f64 =>
        {
            ValueItem::Integer(d as i64)
        }
        v => v,
    }
}

// ---- registry ----------------------------------------------------------

struct Resolved {
    data: Arc<SubqueryData>,
    // The outer side of each correlation equality, compiled by from_expr
    // against the outer query's tables.
    keys: Vec<Expr>,
}

thread_local! {
    static RESOLVED: RefCell<HashMap<usize, Resolved>> = RefCell::new(HashMap::new());
}

fn address(q: &Query) -> usize {
    q as *const Query as usize
}

/// Unregisters, when dropped, the answers `resolve_all` registered.
#[must_use]
pub(crate) struct Registered(Vec<usize>);

impl Drop for Registered {
    fn drop(&mut self) {
        RESOLVED.with(|r| {
            let mut r = r.borrow_mut();
            for a in &self.0 {
                r.remove(a);
            }
        });
    }
}

/// The planned answer to the subquery `q`, compiled against `tables` (the
/// outer query's), if it was resolved (see `resolve_all`).
pub(crate) fn compile<F: DBFile + 'static>(
    q: &Query,
    kind: Kind,
    lhs: Option<&Expr>,
    tables: &[TableQuery<F>],
) -> Result<EvalExpr, SchemaError> {
    let found = RESOLVED.with(|r| {
        r.borrow()
            .get(&address(q))
            .map(|res| (res.data.clone(), res.keys.clone()))
    });
    let Some((data, keys)) = found else {
        return Err(SchemaError::UnsupportedFeature(
            "a subquery here: subqueries may appear in a query's SELECT list, WHERE and HAVING, and in the WHERE of UPDATE and DELETE".into(),
        ));
    };
    if data.kind != kind {
        return Err(SchemaError::InternalSchemaError(format!(
            "subquery planned as {:?}, used as {kind:?}",
            data.kind
        )));
    }
    // Uncorrelated EXISTS and scalar subqueries are constants: as literals
    // they can bound a seek (`id = (SELECT max(id) FROM t)`).
    if keys.is_empty() && kind != Kind::In {
        let answer = SubqueryExpr {
            kind,
            lhs: None,
            keys: vec![],
            data,
        }
        .answer(vec![]);
        return Ok(EvalExpr::Literal(answer));
    }
    Ok(EvalExpr::Subquery(SubqueryExpr {
        kind,
        lhs: match lhs {
            Some(e) => Some(EvalExpr::from_expr(e, tables)?),
            None => None,
        },
        keys: keys
            .iter()
            .map(|k| EvalExpr::from_expr(k, tables).map(|e| *e))
            .collect::<Result<_, _>>()?,
        data,
    }))
}

// ---- resolution ----------------------------------------------------------

/// What resolving a subquery needs from the planner of the query holding
/// it: planning a query (at that query's snapshot), and the tables a FROM
/// clause names, flattened as expressions resolve against them.
pub(crate) trait SubqueryPlanner<F: DBFile + 'static> {
    fn plan(&mut self, query: &Query) -> Result<Box<dyn Source>, SchemaError>;
    fn scope(&mut self, from: &Option<FromClause>) -> Result<Vec<TableQuery<F>>, SchemaError>;
}

/// Runs every subquery in `exprs` (not those nested inside another
/// subquery: that one's own planning resolves them) and registers the
/// answers for `compile` until the guard drops. `outer` is the tables the
/// expressions read.
pub(crate) fn resolve_all<F: DBFile + 'static>(
    planner: &mut dyn SubqueryPlanner<F>,
    exprs: &[&Expr],
    outer: &[TableQuery<F>],
) -> Result<Registered, SchemaError> {
    let mut found = vec![];
    for e in exprs {
        subqueries(e, &mut found);
    }
    let mut registered = Registered(vec![]);
    for (q, kind) in found {
        let resolved = resolve(planner, q, kind, outer)?;
        let a = address(q);
        RESOLVED.with(|r| r.borrow_mut().insert(a, resolved));
        registered.0.push(a);
    }
    Ok(registered)
}

fn resolve<F: DBFile + 'static>(
    planner: &mut dyn SubqueryPlanner<F>,
    q: &Query,
    kind: Kind,
    outer: &[TableQuery<F>],
) -> Result<Resolved, SchemaError> {
    match planner.plan(q) {
        Ok(source) => Ok(Resolved {
            data: Arc::new(materialize(source, kind, 0)?),
            keys: vec![],
        }),
        // It names something it doesn't have — perhaps the outer query's.
        Err(e @ (SchemaError::FieldNotFound(_) | SchemaError::BadTableName(_))) => {
            match correlated(planner, q, kind, outer)? {
                Some(r) => Ok(r),
                None => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

// Reads a planned subquery's rows: the value column first (IN, scalar),
// then `keys` correlation keys.
fn materialize(
    mut source: Box<dyn Source>,
    kind: Kind,
    keys: usize,
) -> Result<SubqueryData, SchemaError> {
    let columns = source.fields().len();
    let values = match kind {
        Kind::Exists => columns - keys,
        Kind::In | Kind::Scalar => 1,
    };
    if kind != Kind::Exists && columns != values + keys {
        let what = if kind == Kind::In { "IN" } else { "used as a value" };
        return Err(SchemaError::UserError(format!(
            "a subquery {what} must return one column, not {}",
            columns - keys
        )));
    }
    let mut data = SubqueryData {
        kind,
        groups: HashMap::new(),
        rows: 0,
    };
    while let Some(row) = source.next()? {
        let row = row.values();
        let key = &row[values..];
        if key.iter().any(|v| matches!(v, ValueItem::Null)) {
            continue;
        }
        data.rows += 1;
        let group = data
            .groups
            .entry(key.iter().cloned().map(normalize).collect())
            .or_default();
        match kind {
            // One row answers an uncorrelated EXISTS.
            Kind::Exists if keys == 0 => break,
            Kind::Exists => {}
            Kind::In => match &row[0] {
                ValueItem::Null => group.has_null = true,
                v => {
                    group.values.insert(normalize(v.clone()));
                }
            },
            Kind::Scalar => {
                if group.value.is_some() {
                    return Err(SchemaError::UserError(
                        "a subquery used as a value returned more than one row".into(),
                    ));
                }
                group.value = Some(row[0].clone());
            }
        }
    }
    Ok(data)
}

// Decorrelates `q` (see this module's doc comment), or None if it reads
// nothing of the outer query after all (its error is then the real one).
fn correlated<F: DBFile + 'static>(
    planner: &mut dyn SubqueryPlanner<F>,
    q: &Query,
    kind: Kind,
    outer: &[TableQuery<F>],
) -> Result<Option<Resolved>, SchemaError> {
    let SetOperand::Select(core) = &q.body else {
        return Ok(None);
    };
    let inner = planner.scope(&core.from)?;
    // A column the subquery's own tables don't have but the outer query's
    // do. (A name both have is the subquery's own, as in SQL.)
    let is_outer = |c: &Expr| {
        EvalExpr::from_expr(c, &inner).is_err() && EvalExpr::from_expr(c, outer).is_ok()
    };
    let reads_outer = |e: &Expr| {
        let mut cols = vec![];
        columns(e, &mut cols);
        cols.into_iter().any(is_outer)
    };

    let mut kept = vec![];
    let mut inner_keys = vec![];
    let mut outer_keys = vec![];
    if let Some(w) = &core.where_clause {
        let mut conjuncts = vec![];
        split_and(&w.expr, &mut conjuncts);
        for c in conjuncts {
            if !reads_outer(c) {
                kept.push(c.clone());
                continue;
            }
            // `inner = outer`, either way round: each side reads only its
            // own query's columns, and the outer side reads some.
            let only_outer = |e: &Expr| {
                let mut cols = vec![];
                columns(e, &mut cols);
                !cols.is_empty() && cols.into_iter().all(is_outer)
            };
            let pair = match strip(c) {
                Expr::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } if only_outer(left) && !reads_outer(right) => Some((right, left)),
                Expr::Binary {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } if only_outer(right) && !reads_outer(left) => Some((left, right)),
                _ => None,
            };
            let Some((i, o)) = pair else {
                return Err(SchemaError::UnsupportedFeature(
                    "a correlated subquery that reads the outer query other than in `inner = outer` WHERE conditions".into(),
                ));
            };
            inner_keys.push((**i).clone());
            outer_keys.push((**o).clone());
        }
    }
    if outer_keys.is_empty() {
        return Ok(None);
    }

    let items: Vec<&SelectItem> = core.projection.items().collect();
    let refuse = |what: &str| {
        Err(SchemaError::UnsupportedFeature(format!(
            "a correlated subquery {what}"
        )))
    };
    if kind == Kind::Scalar {
        return refuse("used as a value");
    }
    if q.with.is_some() || !q.compounds.is_empty() {
        return refuse("with WITH, UNION, INTERSECT or EXCEPT");
    }
    if q.limit.is_some() || q.offset.is_some() {
        return refuse("with LIMIT or OFFSET");
    }
    if core.group_by.is_some() || core.having.is_some() {
        return refuse("with GROUP BY or HAVING");
    }
    for item in &items {
        if let SelectItem::Expr { expr, .. } = item {
            if reads_outer(expr) {
                return refuse("that selects an outer column");
            }
            if has_aggregate(expr) {
                return refuse("that aggregates");
            }
        }
    }

    // The same query, uncorrelated: the outer equalities gone, their inner
    // sides selected after (IN) or instead of (EXISTS) its SELECT list.
    let mut q2 = q.clone();
    q2.order_by = None;
    let SetOperand::Select(core2) = &mut q2.body else {
        unreachable!("matched above")
    };
    core2.where_clause = match kept.into_iter().reduce(|a, b| Expr::Binary {
        left: Box::new(a),
        op: BinaryOp::And,
        right: Box::new(b),
    }) {
        Some(expr) => Some(WhereClause {
            where_token: core.where_clause.as_ref().expect("had conjuncts").where_token,
            expr,
        }),
        None => None,
    };
    let mut select: Vec<SelectItem> = match kind {
        Kind::In => {
            if items.len() != 1 || !matches!(items[0], SelectItem::Expr { .. }) {
                return Err(SchemaError::UserError(
                    "a subquery IN must return one column".into(),
                ));
            }
            vec![items[0].clone()]
        }
        _ => vec![],
    };
    select.extend(
        inner_keys
            .into_iter()
            .map(|expr| SelectItem::Expr { expr, alias: None }),
    );
    let mut select = select.into_iter();
    let head = select.next().expect("at least one key");
    let comma = Comma {
        span: TokenSpan { start: 0, end: 0 },
    };
    core2.projection = Seq {
        head: Box::new(head),
        tail: select.map(|s| (comma, s)).collect(),
    };
    let source = planner.plan(&q2)?;
    let keys = outer_keys.len();
    Ok(Some(Resolved {
        data: Arc::new(materialize(source, kind, keys)?),
        keys: outer_keys,
    }))
}

// ---- walking expressions ------------------------------------------------

fn strip(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(n) => strip(n),
        e => e,
    }
}

fn split_and<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match strip(e) {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            split_and(left, out);
            split_and(right, out);
        }
        e => out.push(e),
    }
}

// The direct children of `e` that are expressions, and the subqueries it
// holds directly — not descending into a subquery's own query.
fn visit<'a>(e: &'a Expr, sub: &mut dyn FnMut(&'a Query, Kind), child: &mut dyn FnMut(&'a Expr)) {
    match e {
        Expr::Literal(_) | Expr::Column(_) | Expr::Placeholder(_) => {}
        Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr) => child(expr),
        Expr::Binary { left, right, .. } => {
            child(left);
            child(right);
        }
        Expr::InList { expr, list, .. } => {
            child(expr);
            list.iter().for_each(child);
        }
        Expr::InSubquery { expr, query, .. } => {
            child(expr);
            sub(query, Kind::In);
        }
        Expr::Exists { query } => sub(query, Kind::Exists),
        Expr::Subquery(query) => sub(query, Kind::Scalar),
        Expr::Between {
            expr, low, high, ..
        } => {
            child(expr);
            child(low);
            child(high);
        }
        Expr::Like { expr, pattern, .. } => {
            child(expr);
            child(pattern);
        }
        Expr::Function { args, .. } => {
            for a in args {
                if let FunctionArg::Expr(e) = a {
                    child(e);
                }
            }
        }
        Expr::QuantifiedComparison { left, .. } => child(left),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.iter().for_each(|o| child(o));
            for (w, t) in when_then {
                child(w);
                child(t);
            }
            else_expr.iter().for_each(|o| child(o));
        }
    }
}

fn subqueries<'a>(e: &'a Expr, out: &mut Vec<(&'a Query, Kind)>) {
    let mut children = vec![];
    visit(e, &mut |q, k| out.push((q, k)), &mut |c| children.push(c));
    for c in children {
        subqueries(c, out);
    }
}

fn columns<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Column(_) = e {
        out.push(e);
        return;
    }
    let mut children = vec![];
    visit(e, &mut |_, _| {}, &mut |c| children.push(c));
    for c in children {
        columns(c, out);
    }
}

fn has_aggregate(e: &Expr) -> bool {
    if let Expr::Function { name, over: None, .. } = e
        && ["count", "sum", "avg", "min", "max"]
            .iter()
            .any(|a| name.value.eq_ignore_ascii_case(a))
    {
        return true;
    }
    let mut children = vec![];
    visit(e, &mut |_, _| {}, &mut |c| children.push(c));
    children.into_iter().any(has_aggregate)
}
