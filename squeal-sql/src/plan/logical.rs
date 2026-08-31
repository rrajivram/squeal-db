use std::sync::Arc;

use parking_lot::RwLock;
use sql_parser::{
    Expr, ObjectName, Query,
    query::{Alias, FromClause, SelectItem, TableFactor},
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
    plan::memory::QueryMemory,
    rslt::resultset::StreamingResultSet,
    source::{ProjectedField, Source, proj::WildcardProj, run::RunSource, table::TableSource},
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
    conn: Arc<Connection<F>>,
    // This query's own memory budget — separate from PageBuffer (a
    // shared, whole-database page cache every query reads through, not
    // something to partition per query). Handed out via `memory()` so a
    // step that needs to buffer state (no such step exists yet — see
    // QueryMemory's own doc comment) can be constructed with its own
    // clone of the Arc *before* being passed to add_step, the same way
    // a caller already builds a step (e.g. TableSource::new) with
    // whatever else it needs before handing it off.
    mem: Arc<QueryMemory>,
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
            guard
                .fields()
                .iter()
                .map(|f| ProjectedField::from(f.clone()))
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

#[derive(Debug, Clone)]
enum Frame<T> {
    Empty,
    Some(T),
}

struct TableQuery<F: DBFile + 'static> {
    schema: String,
    table: String,
    alias: String,
    fields: Arc<[Arc<Field>]>,
    // Resolved once, by resolve_table_ref (which does the schema/table-
    // name lookup itself) — carried alongside the name so
    // post_visit_select doesn't have to re-resolve or re-look-up
    // anything, nor treat "we already validated this" and "so of course
    // this lookup will succeed" as two separate, unwrap-worthy facts.
    resolved: TableRef<F>,
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
}

impl<F> Visitor for QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    type Break = SchemaError;

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

    fn post_visit_select(
        &mut self,
        select: &sql_parser::query::SelectCore,
    ) -> std::ops::ControlFlow<Self::Break> {
        let _distinct = select.distinct.is_some();
        let tables = self.get_tables(&select.from);
        if let Err(e) = tables {
            return std::ops::ControlFlow::Break(e);
        }
        let tables = tables.unwrap();
        let proj = self.get_projections(&select.projection, &tables);
        if let Err(e) = proj {
            return std::ops::ControlFlow::Break(e);
        }
        let proj = proj.unwrap();
        for table in tables.into_iter() {
            match table.resolved.open_source(&self.conn) {
                Ok(source) => {
                    self.steps.push(source);
                }
                Err(e) => return std::ops::ControlFlow::Break(e),
            }
        }
        let proj = Box::new(WildcardProj::new(&proj));
        self.steps.push(proj);

        std::ops::ControlFlow::Continue(())

        /*         let _distinct = select
                   .distinct
                   .map(|_d| Some(true))
                   .or(Some(Some(false)))
                   .flatten()
                   .unwrap();
               /*         let proj = select.projection.head;
                      match proj {
                          sql_parser::query::SelectItem::QualifiedWildcard(object_name, period, asterisk) => todo!(),
                          sql_parser::query::SelectItem::Wildcard(asterisk) => todo!(),
                          sql_parser::query::SelectItem::Expr { expr, alias } => todo!(),
                      }
               */
               let Some(Frame::Some(table)) = self.tables.pop() else {
                   return std::ops::ControlFlow::Break(SchemaError::UnknownError(
                       "No table found".into(),
                   ));
               };
               let source: Result<Box<dyn Source>, SchemaError> = match table.resolved {
                   ResolvedTable::Real(t) => self
                       .conn
                       .with_current_txn::<Result<Box<dyn Source>, SchemaError>>(|txn| {
                           let ts = TableSource::new(self.conn.database.read().db.clone(), t, txn)?;
                           Ok(Box::new(ts))
                       }),
                   // No transaction/visibility involvement at all, unlike the
                   // real-table case above — a Run isn't MVCC-shared state (see
                   // RunCursor's own doc comment), so there's nothing here that
                   // needs `with_current_txn`.
                   ResolvedTable::Temp(t) => {
                       let guard = t.read();
                       guard.cursor().map(|cursor| {
                           Box::new(RunSource::new(cursor, guard.fields())) as Box<dyn Source>
                       })
                   }
               };
               match source {
                   Ok(source) => {
                       self.steps.push(source);
                       std::ops::ControlFlow::Continue(())
                   }
                   Err(e) => std::ops::ControlFlow::Break(e),
               }
        */
    }
}

impl<F> QueryVisitor<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new(conn: Arc<Connection<F>>) -> Self {
        Self {
            conn,
            steps: vec![],
            tables: Stack::new(),
            projections: Stack::new(),
        }
    }

    fn get_projections(
        &self,
        proj: &Seq<SelectItem, Comma>,
        tables: &[TableQuery<F>],
    ) -> Result<Vec<Vec<ProjectedField>>, SchemaError> {
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
    ) -> Result<Vec<ProjectedField>, SchemaError> {
        match proj {
            SelectItem::Expr { expr, alias } => Ok(vec![self.handle_expr(expr, alias, tables)?]),
            SelectItem::Wildcard(_) => {
                let mut v = vec![];
                for t in tables {
                    for f in t.fields.iter() {
                        v.push(ProjectedField::from(f.clone()));
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
                    for f in tables[pos].fields.iter() {
                        v.push(ProjectedField::from(f.clone()));
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
    ) -> Result<ProjectedField, SchemaError> {
        let dummy = "(None)".to_string();
        let res = match expr {
            Expr::Column(c) => {
                let idents = c.idents().collect::<Vec<_>>();
                if idents.len() > 1 {
                    let (_t, f) = self.conn.resolve_object_name_ref(c)?;
                    if f.is_none() {
                        return Err(SchemaError::FieldNotFound("(None)".into()));
                    }
                    f.unwrap()
                } else {
                    let field = idents[0].value.clone();
                    self.validate_field(&field, tables)?;
                    field
                }
            }
            Expr::Literal(_) => dummy.clone(),
            _ => dummy.clone(),
        };
        if let Some(alias) = alias {
            return Ok(ProjectedField::from(alias.name.value.clone()));
        }
        Ok(ProjectedField::from(res))
    }

    fn validate_field(&self, field: &str, tables: &[TableQuery<F>]) -> Result<(), SchemaError> {
        let mut found = false;
        for t in tables {
            let f = t.fields.iter().any(|f| f.name.eq_ignore_ascii_case(field));
            if f && found {
                return Err(SchemaError::AmbiguousFieldError(field.into()));
            }
            found = f;
        }
        if !found {
            return Err(SchemaError::FieldNotFound(field.into()));
        }
        Ok(())
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
                let (table, _) = self.conn.resolve_object_name_ref(name)?;
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
}

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
            conn,
            mem: QueryMemory::new(limit),
        }
    }

    pub(crate) fn build(conn: Arc<Connection<F>>, query: &Query) -> Result<Self, SchemaError> {
        let mut visitor = QueryVisitor::new(conn.clone());
        if let std::ops::ControlFlow::Break(e) = query.visit(&mut visitor) {
            return Err(e);
        }
        let mut this = Self {
            conn,
            tail: None,
            mem: QueryMemory::new(DEFAULT_QUERY_MEMORY_LIMIT),
        };
        for step in visitor.steps.into_iter().rev() {
            this.add_step(step);
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

    // Takes an already-owned Box<dyn Source> rather than being generic
    // over S: Source — a caller building a concrete step (e.g.
    // TableSource::new) boxes it at the call site (Box::new(step)); this
    // way there's exactly one owned-Source type (Box<dyn Source>) used
    // throughout LogicalPlan/QueryVisitor, instead of a generic one here
    // and a trait-object one for `tail`/`steps` that can't convert into
    // each other without a blanket `impl Source for Box<dyn Source>`
    // this crate doesn't have.
    pub(crate) fn add_step(&mut self, mut step: Box<dyn Source>) {
        // Takes the *previous* tail (not the first-ever step) as this
        // step's dependency — that's what makes this a chain (1 depends
        // on nothing, 2 depends on 1, 3 depends on 2, ...) instead of
        // every step fanning out from the same original source.
        step.chain(self.tail.take());
        self.tail = Some(step);
    }

    pub(crate) fn execute(&mut self) -> Result<StreamingResultSet, SchemaError> {
        let tail = self
            .tail
            .take()
            .ok_or(SchemaError::InternalSchemaError("Nothing in plan".into()))?;
        Ok(StreamingResultSet::new(tail))
    }
}

pub(crate) fn extract_object_name(obj_name: &ObjectName) -> Vec<String> {
    let mut res = vec![obj_name.parts.head.value.clone()];
    if obj_name.parts.len() > 1 {
        for (_p, n) in &obj_name.parts.tail {
            res.push(n.value.clone())
        }
    }
    res
}
