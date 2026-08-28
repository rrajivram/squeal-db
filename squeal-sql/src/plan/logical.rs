use std::{collections::VecDeque, sync::Arc};

use parking_lot::RwLock;
use sql_parser::{
    ObjectName, Query,
    query::TableFactor,
    visitor::{Visit, Visitor},
};
use store::db::DBFile;

use crate::{
    conn::connection::Connection,
    constant::DEFAULT_QUERY_MEMORY_LIMIT,
    error::SchemaError,
    plan::memory::QueryMemory,
    rslt::resultset::StreamingResultSet,
    source::{Source, run::RunSource, table::TableSource},
    table::SqlTable,
    temp::{self, TempTable},
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

// Either shape a resolved FROM-clause table reference can take: a real,
// durable table living in some Schema, or a connection-scoped temp
// table (see crate::temp's own doc comment for why temp tables aren't
// backed by a Schema at all). post_visit_select branches on this to
// build the matching Source (TableSource vs RunSource).
enum ResolvedTable<F: DBFile + 'static> {
    Real(Arc<SqlTable>),
    Temp(Arc<RwLock<TempTable<F>>>),
}

struct TableQuery<F: DBFile + 'static> {
    schema: String,
    table: String,
    alias: String,
    // Resolved once, in validate_table (which already has a live
    // Arc<Schema<F>> in hand right when it confirms the table exists) —
    // carried alongside the name so post_visit_select doesn't have to
    // re-resolve the schema and re-look-up the table a second time, nor
    // treat "we already validated this" and "so of course this lookup
    // will succeed" as two separate, unwrap-worthy facts.
    resolved: ResolvedTable<F>,
}

struct QueryVisitor<F: DBFile> {
    tables: VecDeque<TableQuery<F>>,
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

    fn post_visit_table_factor(
        &mut self,
        tf: &sql_parser::query::TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        match tf {
            TableFactor::Derived {
                lparen: _,
                query: _,
                rparen: _,
                alias: _,
            } => {}
            TableFactor::Table { name: _, alias: _ } => {
                let res = self.validate_table(tf);
                if let Err(e) = res {
                    return std::ops::ControlFlow::Break(e);
                }
                let res = res.unwrap();
                self.tables.push_back(res);
            }
        }
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_select(
        &mut self,
        select: &sql_parser::query::SelectCore,
    ) -> std::ops::ControlFlow<Self::Break> {
        let _distinct = select
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
        let Some(table) = self.tables.pop_front() else {
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
            tables: VecDeque::new(),
        }
    }
    fn validate_table(&self, tf: &TableFactor) -> Result<TableQuery<F>, SchemaError> {
        if let TableFactor::Table { name, alias } = tf {
            let name_parts = self.extract_object(name);
            // Table name can only have two parts : schema and name .
            if name_parts.len() > 2 {
                let bad_name = name_parts.join(".");
                return Err(SchemaError::BadTableName(bad_name));
            }
            let (schema, name) = if name_parts.len() == 1 {
                (None, name_parts[0].clone())
            } else {
                (Some(name_parts[0].clone()), name_parts[1].clone())
            };
            let alias = if let Some(alias) = alias {
                alias.name.value.clone()
            } else {
                name.clone()
            };
            if let Some(schema_name) = schema {
                if schema_name.eq_ignore_ascii_case(temp::TEMP_SCHEMA_NAME) {
                    let resolved = self.conn.temp_tables().get(&name).ok_or_else(|| {
                        SchemaError::BadTableName(format!("temp table {name:?} does not exist"))
                    })?;
                    return Ok(TableQuery {
                        schema: temp::TEMP_SCHEMA_NAME.to_string(),
                        table: name,
                        alias,
                        resolved: ResolvedTable::Temp(resolved),
                    });
                }
                let schema = self
                    .conn
                    .database
                    .read()
                    .get_schema(&schema_name)
                    .map_err(|_| SchemaError::SchemaNotFound(schema_name.clone()))?;
                let resolved = schema
                    .get_table(&name)
                    .ok_or_else(|| SchemaError::BadTableName(name.clone()))?;
                Ok(TableQuery {
                    schema: schema_name,
                    table: name,
                    alias,
                    resolved: ResolvedTable::Real(resolved),
                })
            } else {
                let schema = self
                    .conn
                    .current_schema()
                    .ok_or(SchemaError::NoSchemaSelected)?;
                let resolved = schema
                    .get_table(&name)
                    .ok_or_else(|| SchemaError::BadTableName(name.clone()))?;
                Ok(TableQuery {
                    schema: schema.name.clone(),
                    table: name,
                    alias,
                    resolved: ResolvedTable::Real(resolved),
                })
            }
        } else {
            Err(SchemaError::UnknownError(format!("Unknown TF: {:?}", tf)))
        }
    }

    fn extract_object(&self, obj_name: &ObjectName) -> Vec<String> {
        let mut res = vec![obj_name.parts.head.value.clone()];
        if obj_name.parts.len() > 1 {
            for (_p, n) in &obj_name.parts.tail {
                res.push(n.value.clone())
            }
        }
        res
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
