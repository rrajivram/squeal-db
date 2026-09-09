use std::{marker::PhantomData, sync::Arc};

use parking_lot::RwLock;
use sql_parser::{
    Expr, Query,
    keyword::No,
    query::{
        self, Alias, FromClause, GroupByClause, OrderByClause, SelectItem, SetOperand, TableFactor,
    },
    token::Comma,
    utils::Seq,
    visitor::{Visit, Visitor},
};
use store::db::DBFile;

use crate::{
    conn::connection::{Connection, TableRef},
    constant::DEFAULT_QUERY_MEMORY_LIMIT,
    ds::stack::Stack,
    error::SchemaError,
    plan::{eval::EvalExpr, funcs::FuncTrait, memory::QueryMemory},
    rslt::resultset::StreamingResultSet,
    source::{
        ProjectableField, Source, aggr::AggregatingSource, group::GroupSource, join::UnionJoin,
        limit::Limit, proj::Projection, run::RunSource, sort::SortSource, table::TableSource,
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
    // This query's own memory budget — separate from PageBuffer (a
    // shared, whole-database page cache every query reads through, not
    // something to partition per query). Handed out via `memory()` so a
    // step that needs to buffer state (no such step exists yet — see
    // QueryMemory's own doc comment) can be constructed with its own
    // clone of the Arc *before* being passed to add_step, the same way
    // a caller already builds a step (e.g. TableSource::new) with
    // whatever else it needs before handing it off.
    mem: Arc<QueryMemory>,
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
    fn open_source(&self, conn: &Arc<Connection<F>>) -> Result<Box<dyn Source>, SchemaError>;
}

impl<F> OpenSource<F> for Arc<SqlTable>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn open_source(&self, conn: &Arc<Connection<F>>) -> Result<Box<dyn Source>, SchemaError> {
        conn.with_current_txn::<Result<Box<dyn Source>, SchemaError>>(|txn| {
            let ts = TableSource::new(conn.database.read().db.clone(), self.clone(), txn)?;
            Ok(Box::new(ts) as Box<dyn Source>)
        })
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
    fn open_source(&self, _conn: &Arc<Connection<F>>) -> Result<Box<dyn Source>, SchemaError> {
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
            TableRef::Derived => todo!(),
        }
    }

    fn open_source(&self, conn: &Arc<Connection<F>>) -> Result<Box<dyn Source>, SchemaError> {
        match self {
            TableRef::Real(_, t) => t.open_source(conn),
            TableRef::Temp(_, t) => t.open_source(conn),
            TableRef::Derived => todo!(),
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
}

struct QueryVisitor<F: DBFile> {
    tables: Stack<Frame<TableQuery<F>>>, // None means frame is done
    projections: Stack<Frame<SelectItem>>,
    conn: Arc<Connection<F>>,
    // Box<dyn Source>, not a generic Vec<S> — a Vec needs one uniform
    // element type, but different table references (and later, joins/
    // other step kinds) produce different concrete Source
    // implementations. This is also the exact type LogicalPlan::tail
    // already stores an owned step as (see its own comment) — Box
    // already owns the heap-allocated Source, so building this Vec here
    // and handing each entry to LogicalPlan::add_step below doesn't
    // need anything more than that.
    steps: Vec<Box<dyn Source>>,
    limit: Option<usize>,
    order: Option<OrderByClause>,
    mem: Arc<QueryMemory>,
}

struct SourceHolder<F: DBFile + 'static> {
    source: Box<dyn Source>,
    tables: Vec<TableQuery<F>>,
}

impl<F> Visitor for QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    type Break = SchemaError;

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<Self::Break> {
        if let Some(limit) = &query.limit
            && let Some(order) = &query.order_by
        {
            self.limit = limit.count_i64().map(|l| l as usize);
            self.order = Some(order.clone());
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
        self.tables.push(Frame::Empty);
        self.projections.push(Frame::Empty);
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &sql_parser::Expr) -> std::ops::ControlFlow<Self::Break> {
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<Self::Break> {
        if let SetOperand::Select(select) = &query.body {
            let holder = self.handle_select(select);
            if let Err(e) = holder {
                return std::ops::ControlFlow::Break(e);
            }
            let holder = holder.unwrap();
            let step = holder.source;
            let tables = holder.tables;

            if let Some(limit) = &query.limit
                && let Some(limit) = limit.count_i64()
            {
                if limit < 0 {
                    return std::ops::ControlFlow::Break(SchemaError::InvalidLimitValue(limit));
                } else {
                    if let Some(order) = &query.order_by {
                        let order = SortSource::create_from(
                            step,
                            order,
                            &tables,
                            Some(limit as usize),
                            self.conn.database.read().db.clone(),
                            self.mem.clone(),
                        );
                        if let Err(e) = order {
                            return std::ops::ControlFlow::Break(e);
                        }
                        let step = order.unwrap();
                        self.steps.push(Box::new(step));
                    } else {
                        self.steps.push(Box::new(Limit::new(step, limit as usize)));
                    }
                    return std::ops::ControlFlow::Continue(());
                }
            } else {
                self.steps.push(step);
            }
        }

        std::ops::ControlFlow::Continue(())
    }
}

impl<F> QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new(conn: Arc<Connection<F>>, mem: Arc<QueryMemory>) -> Self {
        Self {
            conn,
            steps: vec![],
            tables: Stack::new(),
            projections: Stack::new(),
            limit: None,
            order: None,
            mem,
        }
    }

    fn handle_select(
        &mut self,
        select: &sql_parser::query::SelectCore,
    ) -> Result<SourceHolder<F>, SchemaError> {
        let distinct = select.distinct.is_some();
        let tables = self.get_tables(&select.from)?;
        let proj = self.get_projections(&select.projection, &tables)?;
        let projected_fields = proj.into_iter().flatten().collect::<Vec<_>>();
        let has_aggregation = projected_fields.iter().any(|f| f.expr.has_aggregate());
        let projected_field_count = projected_fields.len();

        let wh_expr = if let Some(wh) = &select.where_clause {
            Some(*EvalExpr::from_expr(&wh.expr, &tables)?)
        } else {
            None
        };

        let mut sources = vec![];
        for table in tables.iter() {
            sources.push(table.resolved.open_source(&self.conn)?);
        }
        let union = UnionJoin::new(sources)?;
        let for_proj: Box<dyn Source> = if let Some(wh_expr) = wh_expr {
            Box::new(WhereSource::new(Box::new(union), wh_expr)?)
        } else {
            Box::new(union)
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
                self.validate_aggreations(&projected_fields, &tables, &select.group_by)?;
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

        Ok(SourceHolder { source, tables })
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
                            EvalExpr::Value(EvalExpr::flat_position(&tables, sid, fid)),
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
                            EvalExpr::Value(EvalExpr::flat_position(&tables, pos, fid)),
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

    fn get_tables(&self, from: &Option<FromClause>) -> Result<Vec<TableQuery<F>>, SchemaError> {
        if from.is_none() {
            return Ok(vec![]);
        }
        let from = from.as_ref().unwrap();
        let mut tables = vec![];
        let items = from.tables.items();
        for qtable in items {
            let tq = if let TableFactor::Table { name, alias } = &qtable.relation {
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
                    }
                } else {
                    todo!()
                }
            } else {
                todo!()
            };
            tables.push(tq);
        }
        Ok(tables)
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
            mem: QueryMemory::new(limit),
            _phanton: PhantomData,
        }
    }

    pub(crate) fn build(conn: Arc<Connection<F>>, query: &Query) -> Result<Self, SchemaError> {
        let mem = QueryMemory::new(DEFAULT_QUERY_MEMORY_LIMIT);
        let mut visitor = QueryVisitor::new(conn.clone(), mem.clone());
        if let std::ops::ControlFlow::Break(e) = query.visit(&mut visitor) {
            return Err(e);
        }
        let mut this = Self {
            tail: None,
            mem,
            _phanton: PhantomData,
        };
        assert!(visitor.steps.len() == 1);
        for step in visitor.steps.into_iter().rev() {
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

    pub(crate) fn execute(&mut self) -> Result<StreamingResultSet, SchemaError> {
        let tail = self
            .tail
            .take()
            .ok_or(SchemaError::InternalSchemaError("Nothing in plan".into()))?;
        Ok(StreamingResultSet::new(tail))
    }
}
