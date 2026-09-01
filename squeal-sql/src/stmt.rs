use std::{fmt::Display, sync::Arc};

use either::Either;
use store::{db::DBFile, valueitem::ValueItem};
use uuid::Uuid;

use crate::{
    conn::connection::{Connection, TableRef},
    constant::DEFAULT_VAR_SIZE,
    error::SchemaError,
    plan::logical::LogicalPlan,
    rslt::resultset::{ResultSet, ResultType},
    schema_ops::schema::Schema,
    table::SqlTable,
    temp,
};

pub struct Statement<F: DBFile> {
    id: uuid::Uuid,
    sql: String,
    stmts: Vec<sql_parser::Statement>,
    conn: Arc<Connection<F>>,
    // Option<_>, not a bare ResultType: a result — streaming ones
    // especially, see ResultType::StreamingResult — can't be cheaply
    // cloned to hand out while keeping an intact copy behind for a
    // later re-read, so get_results/get_nextresult *take* the slot
    // instead (see their own doc comments); None here means "already
    // retrieved", not "never had a result".
    results: Vec<Option<ResultType>>,
    current_result: Option<usize>,
}

pub struct PreparedStatement<F: DBFile> {
    stmt: Statement<F>,
    // The original, still placeholder-bearing statement — kept
    // separate from (a clone of) `stmt.stmts[0]` specifically because
    // execute() overwrites `stmt.stmts` with a *substituted* (no longer
    // placeholder-bearing) copy each time it runs. Substituting always
    // starts from this template, never from `stmt.stmts[0]` itself —
    // otherwise a second execute() call would find no placeholders left
    // to replace (they were already replaced by the first call) and
    // silently reuse the first call's bound values instead of the
    // newly-set ones.
    template: sql_parser::Statement,
    // Bound values, indexed by the "?" placeholder's position (0-based,
    // in the order it appears across the statement's own AST — see
    // sql_parser::Statement::parameter_count) — None until set_field is
    // called for that index. Deliberately persists across execute()
    // calls rather than being cleared: a caller can either rebind every
    // field before each execute() or leave values as-is to repeat the
    // same execution.
    params: Vec<Option<ValueItem>>,
}

#[cfg(test)]
mod tests;

// Connection (via Database, via store::Db) doesn't implement Debug, so
// this can't be derived — a minimal manual impl (id + sql) is enough
// for {:?} logging and for Result<Statement<F>, _>::unwrap_err() in
// tests.
impl<F: DBFile> std::fmt::Debug for Statement<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("id", &self.id)
            .field("sql", &self.sql)
            .finish_non_exhaustive()
    }
}

impl<F: DBFile> std::fmt::Debug for PreparedStatement<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("stmt", &self.stmt)
            .finish_non_exhaustive()
    }
}

impl<F> PreparedStatement<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(sql: &str, conn: Arc<Connection<F>>) -> Result<Self, SchemaError> {
        let stmt = Statement::new(sql, conn)?;
        if stmt.stmts.len() != 1 {
            return Err(SchemaError::TooManyPreparedStatement);
        }
        let st = &stmt.stmts[0];
        match st {
            sql_parser::Statement::Insert(_)
            | sql_parser::Statement::Delete(_)
            | sql_parser::Statement::Update(_)
            | sql_parser::Statement::Select(_) => {}
            _ => return Err(SchemaError::BadPreparedStatement(format!("{st:?}"))),
        }
        let param_count = st.parameter_count();
        let template = st.clone();
        Ok(Self {
            stmt,
            template,
            params: vec![None; param_count],
        })
    }

    /// The number of "?" placeholders this statement has — set_field
    /// accepts any index in `0..parameter_count()`.
    pub fn parameter_count(&self) -> usize {
        self.params.len()
    }

    pub fn set_field(&mut self, index: usize, value: ValueItem) -> Result<(), SchemaError> {
        let count = self.params.len();
        let slot = self.params.get_mut(index).ok_or_else(|| {
            SchemaError::UserError(format!(
                "parameter index {index} out of range — this statement has {count} placeholder(s)"
            ))
        })?;
        *slot = Some(value);
        Ok(())
    }

    // Only INSERT is actually executable right now: DELETE/UPDATE
    // aren't dispatched by Statement::execute at all yet (there's no
    // row-mutation support in this engine yet, prepared or otherwise),
    // and Query is permanently limited to "SELECT * FROM <table>" with
    // no WHERE clause (rejected outright by parse_select_star) — the
    // only place a "?" could ever legally appear in a Query is a WHERE
    // clause, so a prepared SELECT can never actually have anything to
    // bind. Both are accepted at `new()` (matching the parse-time
    // validation already written) so a caller can construct and bind
    // one ahead of when those become real, but execute() has to be
    // honest about not being able to run them yet.
    pub fn execute(&mut self) -> Result<ResultType, SchemaError> {
        let insert = match &self.template {
            sql_parser::Statement::Insert(insert) => insert.clone(),
            sql_parser::Statement::Update(_) => {
                return Err(SchemaError::UserError(
                    "prepared UPDATE is not executable yet — UPDATE isn't supported by this \
                     engine at all yet"
                        .into(),
                ));
            }
            sql_parser::Statement::Delete(_) => {
                return Err(SchemaError::UserError(
                    "prepared DELETE is not executable yet — DELETE isn't supported by this \
                     engine at all yet"
                        .into(),
                ));
            }
            _ => {
                return Err(SchemaError::UserError(
                    "prepared SELECT is not executable yet — SELECT has no WHERE clause support \
                     yet for a placeholder to bind into"
                        .into(),
                ));
            }
        };
        let substituted = substitute_insert_placeholders(insert, &self.params)?;
        // Swapped into the underlying Statement and run through its own
        // ordinary execute() — reuses every bit of INSERT's existing
        // validation/dispatch (rows_from_insert's type/NOT NULL checks,
        // FK enforcement, transaction handling, ...) unchanged, since a
        // fully-substituted Insert is indistinguishable from one a
        // caller typed as a literal statement.
        self.stmt.stmts = vec![sql_parser::Statement::Insert(substituted)];
        self.stmt.results.clear();
        self.stmt.current_result = None;
        self.stmt.execute()?;
        Ok(self
            .stmt
            .results
            .first_mut()
            .and_then(Option::take)
            .expect("Insert's own execute() arm always pushes exactly one result"))
    }
}

// Mutable iteration over a Seq's items — sql_parser::utils::Seq exposes
// `head`/`tail` as public fields but only an immutable `.items()`
// iterator; placeholder substitution needs to mutate each Expr in place,
// so this reaches into those fields directly instead.
fn seq_items_mut<T, S>(seq: &mut sql_parser::utils::Seq<T, S>) -> impl Iterator<Item = &mut T> {
    std::iter::once(seq.head.as_mut()).chain(seq.tail.iter_mut().map(|(_, t)| t))
}

// Clones `insert`'s VALUES rows, replacing each "?" placeholder — in
// the same left-to-right, row-major order sql_parser::Statement::
// placeholders enumerates them in — with a literal Expr built from the
// correspondingly-bound param. Errors if a placeholder's slot was never
// bound (set_field never called for that index); can't error the other
// way (more bound params than placeholders) since params.len() is fixed
// at PreparedStatement::new time to exactly the placeholder count.
fn substitute_insert_placeholders(
    mut insert: sql_parser::dml::Insert,
    params: &[Option<ValueItem>],
) -> Result<sql_parser::dml::Insert, SchemaError> {
    let mut next = 0usize;
    if let sql_parser::dml::InsertSource::Values(_, rows) = &mut insert.source {
        for row in seq_items_mut(rows) {
            for expr in seq_items_mut(&mut row.1) {
                substitute_placeholder_expr(expr, params, &mut next)?;
            }
        }
    }
    Ok(insert)
}

fn substitute_placeholder_expr(
    expr: &mut sql_parser::Expr,
    params: &[Option<ValueItem>],
    next: &mut usize,
) -> Result<(), SchemaError> {
    if matches!(expr, sql_parser::Expr::Placeholder(_)) {
        let value = params.get(*next).and_then(|o| o.clone()).ok_or_else(|| {
            SchemaError::UserError(format!("parameter {} was not bound", *next + 1))
        })?;
        *next += 1;
        *expr = value_item_to_expr(&value)?;
    }
    Ok(())
}

// The inverse of expr_to_value_item — builds a literal Expr node from a
// bound ValueItem so a substituted Insert round-trips through the same
// validation (rows_from_insert -> expr_to_value_item) an ordinary,
// literally-typed INSERT already goes through. The span on each
// synthesized literal is a dummy (0..0): these nodes never came from
// source text, and nothing downstream reads a literal's own span.
fn value_item_to_expr(v: &ValueItem) -> Result<sql_parser::Expr, SchemaError> {
    use sql_parser::{
        expr::Expr,
        literal::{Literal, NumberLiteral, NumberValue, StringLiteral},
        span::TokenSpan,
    };
    let dummy_span = TokenSpan { start: 0, end: 0 };
    let literal = match v {
        ValueItem::Null => Literal::Null(sql_parser::keyword::Null::new(dummy_span)),
        ValueItem::Integer(i) => Literal::Number(NumberLiteral {
            span: dummy_span,
            raw: i.to_string(),
            value: NumberValue::Integer(*i),
        }),
        ValueItem::Double(d) => Literal::Number(NumberLiteral {
            span: dummy_span,
            raw: d.to_string(),
            value: NumberValue::Float(*d),
        }),
        ValueItem::Datetime(d) => Literal::Number(NumberLiteral {
            span: dummy_span,
            raw: d.to_string(),
            value: NumberValue::Integer(*d as i64),
        }),
        ValueItem::Str((s, _)) => Literal::String(StringLiteral {
            span: dummy_span,
            value: s.clone(),
        }),
        ValueItem::Blob(_) => {
            return Err(SchemaError::UserError(
                "binding a Blob value into a prepared statement is not supported yet".into(),
            ));
        }
    };
    Ok(Expr::Literal(literal))
}

impl<F> Statement<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(sql: &str, conn: Arc<Connection<F>>) -> Result<Self, SchemaError> {
        let stmts = sql_parser::parse_sql(sql)?;
        Self::semantic_validate(&stmts)?;
        Ok(Self {
            id: Uuid::new_v4(),
            sql: sql.to_string(),
            conn,
            stmts,
            results: vec![],
            current_result: None,
        })
    }

    // Static checks derivable purely from the parsed AST — no schema
    // lookup, so this can't (and doesn't try to) check things like "does
    // this table/column actually exist." Runs over *every* statement in
    // the batch up front, before any of them execute — so a structurally
    // broken statement later in a multi-statement batch is caught before
    // earlier ones in the same batch have already run, not partway
    // through. Some of what it checks (name length, duplicate columns,
    // VALUES arity) is also re-checked deeper in execute() — intentional
    // duplication, not an oversight: this is what buys the "whole batch
    // validated before any of it runs" guarantee execute()'s own
    // per-statement checks can't provide on their own.
    fn semantic_validate(stmts: &[sql_parser::Statement]) -> Result<(), SchemaError> {
        for stmt in stmts {
            match stmt {
                sql_parser::Statement::CreateTable(c) => validate_create_table(c)?,
                sql_parser::Statement::CreateDatabase(c) => {
                    let what = if c.kind.is_left() {
                        "database name"
                    } else {
                        "schema name"
                    };
                    validate_identifier(what, &c.name.value)?;
                }
                sql_parser::Statement::Insert(insert) => validate_insert(insert)?,
                sql_parser::Statement::Select(query) => {
                    //parse_select_star(query)?;
                }
                sql_parser::Statement::AlterTable(alter) => {
                    parse_alter_table(alter)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn execute(&mut self) -> Result<(), SchemaError> {
        for stmt in &self.stmts {
            match stmt {
                sql_parser::Statement::ShowSchemas(_) => {
                    let schemas = self
                        .conn
                        .list_schemas()?
                        .iter()
                        .map(|n| vec![ValueItem::try_from(n).unwrap()])
                        .collect::<Vec<_>>();
                    self.results.push(Some(ResultType::Result(ResultSet::new(
                        vec!["Schema name".into()],
                        schemas,
                    ))));
                }
                sql_parser::Statement::ShowTables(_) => {
                    let tables = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?
                        .list_tables()
                        .iter()
                        .map(|n| vec![ValueItem::try_from(n).unwrap()])
                        .collect();
                    self.results.push(Some(ResultType::Result(ResultSet::new(
                        vec!["Table name".into()],
                        tables,
                    ))));
                }
                sql_parser::Statement::CreateTable(c) => {
                    let parts: Vec<&str> = c.name.idents().map(|i| i.value.as_str()).collect();
                    if let Some(temp_name) = temp::temp_table_name(&parts) {
                        let fields = temp::fields_from_create_table(c)?;
                        let db = self.conn.database.read().db.clone();
                        self.conn
                            .temp_tables()
                            .create(&db, temp_name.clone(), fields)?;
                        self.results.push(Some(ResultType::ResultString(format!(
                            "Table 'temp.{temp_name}' created"
                        ))));
                    } else {
                        let schema = self
                            .conn
                            .current_schema()
                            .ok_or(SchemaError::NoSchemaSelected)?;
                        let table_name = c.name.to_dotted();
                        schema.create_table(SqlTable::from_sql(&schema, c.clone())?)?;
                        self.results.push(Some(ResultType::ResultString(format!(
                            "Table '{table_name}' created"
                        ))));
                    }
                }
                sql_parser::Statement::CreateDatabase(c) => {
                    let name = c.name.value.to_lowercase();
                    let message = if c.kind.is_left() {
                        match self.conn.create_database(&name) {
                            Ok(()) => format!("Database '{name}' created"),
                            // IF NOT EXISTS on a name that's already open must
                            // still land the connection on it, same end state
                            // as the "didn't exist yet" path above — not a
                            // silent no-op that leaves the old database
                            // selected.
                            Err(SchemaError::DatabaseInUseError(_))
                                if c.if_not_exists.is_some() =>
                            {
                                self.conn.use_database(&name)?;
                                format!("Database '{name}' already exists")
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        match self.conn.create_schema(&name) {
                            Ok(()) => format!("Schema '{name}' created"),
                            Err(SchemaError::SchemaInUseError(_)) if c.if_not_exists.is_some() => {
                                self.conn.use_schema(&name)?;
                                format!("Schema '{name}' already exists")
                            }
                            Err(e) => return Err(e),
                        }
                    };
                    self.results.push(Some(ResultType::ResultString(message)));
                }
                sql_parser::Statement::Use(u) => match &u.kind {
                    Some(Either::Left(_)) => {
                        let name = u.name.to_dotted().to_lowercase();
                        self.conn.use_database(&name)?;
                        self.results.push(Some(ResultType::ResultString(format!(
                            "Using database '{name}'"
                        ))));
                    }
                    Some(Either::Right(_)) => {
                        let name = u.name.to_dotted().to_lowercase();
                        self.conn.use_schema(&name)?;
                        self.results.push(Some(ResultType::ResultString(format!(
                            "Using schema '{name}'"
                        ))));
                    }
                    // Bare `USE name` (no DATABASE/SCHEMA keyword) has no
                    // equivalent concept here yet — silently ignored, same
                    // as sqlparser's other USE targets (catalog/warehouse/
                    // role/...) always were.
                    None => {}
                },
                sql_parser::Statement::Insert(insert) => {
                    let (table_ref, field) = self.conn.resolve_object_name_ref(&insert.table)?;
                    reject_qualified_field("INSERT INTO", field)?;
                    match table_ref {
                        TableRef::Temp(temp_name, handle) => {
                            let fields = handle.read().fields();
                            let rows =
                                crate::table::build_insert_rows(&temp_name, &fields, insert)?;
                            let count = handle.write().insert_rows(rows)?;
                            self.results.push(Some(ResultType::Count(count)));
                        }
                        TableRef::Real(schema, table) => {
                            let rows = table.rows_from_insert(insert)?;
                            let count = self.conn.with_current_txn(|txn| {
                                schema.insert_rows(&table.name, rows, txn)
                            })?;
                            self.results.push(Some(ResultType::Count(count)));
                        }
                        // resolve_table_ref never actually produces this —
                        // it only resolves names, and a derived table has
                        // none — but TableRef is one enum shared with
                        // plan::logical's own FROM-item resolution (see its
                        // own doc comment), so every match on it has to stay
                        // exhaustive.
                        TableRef::Derived => {
                            return Err(SchemaError::InternalSchemaError(
                                "resolve_table_ref unexpectedly returned Derived".into(),
                            ));
                        }
                    }
                }
                sql_parser::Statement::StartTransaction(_) => {
                    self.conn.begin_transaction()?;
                    self.results
                        .push(Some(ResultType::ResultString("Transaction started".into())));
                }
                sql_parser::Statement::Commit(_) => {
                    self.conn.commit_transaction()?;
                    self.results.push(Some(ResultType::ResultString(
                        "Transaction committed".into(),
                    )));
                }
                sql_parser::Statement::Rollback(_) => {
                    self.conn.rollback_transaction()?;
                    self.results.push(Some(ResultType::ResultString(
                        "Transaction rolled back".into(),
                    )));
                }
                sql_parser::Statement::Select(query) => {
                    /*                     let table_name_obj = parse_select_star(query)?;
                    let (schema, table_name) = resolve_table(&self.conn, &table_name_obj)?;
                    let table = schema.get_table(&table_name).ok_or_else(|| {
                        SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
                    })?;
                    let mut plan = LogicalPlan::new(self.conn.clone());
                    self.conn
                        .with_current_txn::<Result<(), SchemaError>>(|txn| {
                            let ts =
                                TableSource::new(self.conn.database.read().db.clone(), table, txn)?;
                            plan.add_step(Box::new(ts));
                            Ok(())
                        })?;
                     */
                    let mut plan = LogicalPlan::build(self.conn.clone(), query)?;
                    let result = plan.execute()?;
                    self.results.push(Some(ResultType::StreamingResult(result)));
                }
                sql_parser::Statement::AlterTable(alter) => {
                    let (table_name_obj, op) = parse_alter_table(alter)?;
                    let (table_ref, field) = self.conn.resolve_object_name_ref(&table_name_obj)?;
                    reject_qualified_field("ALTER TABLE", field)?;
                    let (schema, table) = expect_real(table_ref, "ALTER TABLE")?;
                    let table_name = &table.name;
                    match op {
                        AlterColumnOp::Add(field) => schema.add_column(table_name, field)?,
                        AlterColumnOp::Drop(name) => schema.drop_column(table_name, &name)?,
                        AlterColumnOp::Rename(old, new) => {
                            schema.rename_column(table_name, &old, &new)?
                        }
                        AlterColumnOp::AddForeignKey(fk) => {
                            schema.add_foreign_key(table_name, fk)?
                        }
                        AlterColumnOp::DropForeignKey(name) => {
                            schema.drop_foreign_key(table_name, &name)?
                        }
                    }
                    self.results.push(Some(ResultType::ResultString(format!(
                        "Table {table_name:?} altered"
                    ))));
                }
                sql_parser::Statement::CopyInto(c) => {
                    let (table_name_obj, path) = parse_copy_into(c);
                    let (table_ref, field) = self.conn.resolve_object_name_ref(&table_name_obj)?;
                    reject_qualified_field("COPY INTO", field)?;
                    let (schema, table) = expect_real(table_ref, "COPY INTO")?;
                    let (loaded, failed) = schema.copy_csv_into(&table.name, &path)?;
                    self.results.push(Some(ResultType::ResultString(format!(
                        "{loaded} row(s) loaded, {failed} row(s) failed"
                    ))));
                }
                sql_parser::Statement::DescribeTable(d) => {
                    let (table_ref, _) = self.conn.resolve_object_name_ref(&d.name)?;
                    let fields = ["Column", "Type", "Nullable", "Default"];
                    let fields_arc = match table_ref {
                        TableRef::Real(_s, t) => t.fields_arc(),
                        TableRef::Temp(_s, t) => t.read().fields(),
                        _ => {
                            return Err(SchemaError::UnknownError(
                                "Can't describe derived table".into(),
                            ));
                        }
                    };
                    let rows = fields_arc
                        .iter()
                        .map(|f| {
                            let default = f
                                .default
                                .as_ref()
                                .map(|v| v.to_string())
                                .unwrap_or("".into());
                            let nullable = f.nullable.to_string();
                            let t = f.datatype.to_string();
                            let name = f.name.clone();
                            let sz = DEFAULT_VAR_SIZE as u32;
                            vec![
                                ValueItem::Str((name, sz)),
                                ValueItem::Str((t, sz)),
                                ValueItem::Str((nullable, sz)),
                                ValueItem::Str((default, sz)),
                            ]
                        })
                        .collect::<Vec<_>>();
                    self.results.push(Some(ResultType::Result(ResultSet::new(
                        fields.iter().map(|s| s.to_string()).collect(),
                        rows,
                    ))));
                }
                _ => {}
            }
        }
        Ok(())
    }

    // Returns the "current" result, initializing the cursor to the first
    // one on first call. Each result can only be retrieved once — this
    // *takes* the slot rather than cloning it, because a StreamingResult
    // holds a live cursor that fundamentally can't be cloned (see
    // ResultType's own doc comment), and every other variant follows the
    // same rule for consistency rather than letting some results be
    // re-readable and others not. Concretely: calling this again at the
    // same position (without an intervening get_nextresult) finds the
    // slot already emptied and returns None — this is no longer
    // idempotent the way it used to be when results were cheap to clone.
    // None also covers the ordinary "no results at all" and "advanced
    // past the last one" cases.
    pub fn get_results(&mut self) -> Result<Option<ResultType>, SchemaError> {
        let i = *self.current_result.get_or_insert(0);
        Ok(self.results.get_mut(i).and_then(Option::take))
    }

    // Advances the cursor to the next result and takes it (see
    // get_results on why take, not clone), or returns None if there
    // isn't one — the cursor position is left unchanged in that case,
    // but the result it points at may already have been taken by an
    // earlier get_results/get_nextresult call.
    pub fn get_nextresult(&mut self) -> Result<Option<ResultType>, SchemaError> {
        let next = self.current_result.map_or(0, |i| i + 1);
        match self.results.get_mut(next) {
            Some(slot) => {
                self.current_result = Some(next);
                Ok(slot.take())
            }
            None => Ok(None),
        }
    }
}

fn validate_identifier(what: &str, name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::UserError(format!("{what} cannot be empty")));
    }
    if name.len() > crate::constant::MAX_TABLE_NAME_LEN {
        return Err(SchemaError::UserError(format!(
            "{what} cannot be longer than {} characters",
            crate::constant::MAX_TABLE_NAME_LEN
        )));
    }
    Ok(())
}

// Table names specifically use BadTableName (not UserError, unlike
// every other identifier this module validates) — matching
// TableBuilder::build's own existing convention, since that's the
// other place an over-length/missing table name gets caught (for a
// name that reaches execute() without going through semantic_validate
// first, e.g. any future caller that skips it).
fn validate_table_name(name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::BadTableName(
            "table name cannot be empty".into(),
        ));
    }
    if name.len() > crate::constant::MAX_TABLE_NAME_LEN {
        return Err(SchemaError::BadTableName(format!(
            "table name cannot be longer than {} characters",
            crate::constant::MAX_TABLE_NAME_LEN
        )));
    }
    Ok(())
}

fn validate_create_table(c: &sql_parser::ddl::CreateTable) -> Result<(), SchemaError> {
    validate_table_name(&c.name.to_dotted())?;

    let parts: Vec<&str> = c.name.idents().map(|i| i.value.as_str()).collect();
    if temp::temp_table_name(&parts).is_some() && c.constraints().next().is_some() {
        return Err(SchemaError::UserError(
            "CREATE TABLE temp.<table> doesn't support constraints (PRIMARY KEY/UNIQUE/FOREIGN \
             KEY/CHECK) yet"
                .into(),
        ));
    }

    let mut seen = std::collections::HashSet::new();
    for col in c.columns() {
        let name = col.name.value.to_lowercase();
        validate_identifier("column name", &name)?;
        if !seen.insert(name.clone()) {
            return Err(SchemaError::UserError(format!(
                "duplicate column name: {name}"
            )));
        }
    }

    for constraint in c.constraints() {
        let fields: Vec<String> = match &constraint.kind {
            sql_parser::ddl::TableConstraintKind::Unique(_, _, cols, _) => {
                cols.items().map(|c| c.value.to_lowercase()).collect()
            }
            sql_parser::ddl::TableConstraintKind::PrimaryKey(_, _, _, cols, _) => {
                cols.items().map(|c| c.value.to_lowercase()).collect()
            }
            // Reuses the same conversion from_sql itself calls later —
            // rejects composite keys/a missing target column here too,
            // not just once execute() actually gets there, and its
            // `column` is what needs checking against `seen` below,
            // same as Unique/PrimaryKey's own local columns.
            sql_parser::ddl::TableConstraintKind::ForeignKey(_, _, _, cols, _, reference) => {
                let local: Vec<sql_parser::Ident> = cols.items().cloned().collect();
                vec![crate::table::foreign_key_from_constraint(reference, &local, None)?.column]
            }
            sql_parser::ddl::TableConstraintKind::Check(..) => vec![],
        };
        for f in fields {
            if !seen.contains(&f) {
                return Err(SchemaError::UserError(format!(
                    "constraint references unknown column: {f}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_insert(insert: &sql_parser::dml::Insert) -> Result<(), SchemaError> {
    let Some((_, cols, _)) = &insert.columns else {
        return Ok(());
    };

    let mut seen = std::collections::HashSet::new();
    for col in cols.items() {
        let name = col.value.to_lowercase();
        validate_identifier("column name", &name)?;
        if !seen.insert(name.clone()) {
            return Err(SchemaError::UserError(format!(
                "duplicate column name in INSERT column list: {name}"
            )));
        }
    }

    // Row width vs. an *explicit* column list is checkable without a
    // schema lookup; against the implicit "every field, in declared
    // order" form (no column list) it isn't — that's deferred to
    // rows_from_insert, which has the actual table to check against.
    if let sql_parser::dml::InsertSource::Values(_, rows) = &insert.source {
        for row in rows.items() {
            if row.1.len() != cols.len() {
                return Err(SchemaError::UserError(format!(
                    "INSERT column list has {} column(s) but a VALUES row has {}",
                    cols.len(),
                    row.1.len()
                )));
            }
        }
    }
    Ok(())
}

// Connection::resolve_table_ref accepts a trailing field
// (schema.table.field) since that shape is meaningful for a column
// reference — but INSERT/ALTER TABLE/COPY INTO always name a plain
// table, with no column position a trailing field could qualify. Called
// right after resolve_table_ref by each of those, instead of leaving a
// `.field`-qualified table target to fail confusingly deeper in
// (e.g. as a bogus table name once the field got silently balled into
// it, or an "unknown table" from a name it was never meant to be).
pub(crate) fn reject_qualified_field(what: &str, field: Option<String>) -> Result<(), SchemaError> {
    if let Some(field) = field {
        return Err(SchemaError::UserError(format!(
            "{what} does not accept a field-qualified table reference (.{field})"
        )));
    }
    Ok(())
}

// ALTER TABLE and COPY INTO's own target can only ever be a real,
// durable table — neither has anything to do against a temp table yet
// (schema-level operations like add_column/copy_csv_into only exist on
// Schema, not TempTable), and Derived can't actually happen here at all
// (resolve_table_ref only ever resolves names; a derived table has
// none) but still has to be matched since TableRef is one enum shared
// with plan::logical's own FROM-item resolution (see its own doc
// comment).
fn expect_real<F>(
    table_ref: TableRef<F>,
    what: &str,
) -> Result<(Arc<Schema<F>>, Arc<SqlTable>), SchemaError>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    match table_ref {
        TableRef::Real(schema, table) => Ok((schema, table)),
        TableRef::Temp(name, _) => Err(SchemaError::UserError(format!(
            "{what} is not supported on temp tables (temp.{name})"
        ))),
        TableRef::Derived => Err(SchemaError::InternalSchemaError(format!(
            "resolve_table_ref unexpectedly returned Derived for {what}"
        ))),
    }
}

// Deliberately strict launchpad for real relational algebra support:
// accepts exactly "SELECT * FROM <table>" and rejects (with a specific
// message, not silent ignoring) anything else — WITH/CTEs, ORDER BY,
// LIMIT/OFFSET, set operations, explicit column lists, WHERE, DISTINCT,
// HAVING, GROUP BY, joins/multiple FROM tables, and subqueries/aliases
// in FROM.
fn parse_select_star(query: &sql_parser::Query) -> Result<sql_parser::ObjectName, SchemaError> {
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "SELECT only supports \"SELECT * FROM <table>\" right now — {what} is not supported yet"
        )))
    };
    if query.with.is_some() {
        return unsupported("CTEs (WITH)");
    }
    if query.order_by.is_some() {
        return unsupported("ORDER BY");
    }
    if query.limit.is_some() {
        return unsupported("LIMIT");
    }
    if query.offset.is_some() {
        return unsupported("OFFSET");
    }
    if !query.compounds.is_empty() {
        return unsupported("set operations (UNION/INTERSECT/EXCEPT)");
    }
    let select = match &query.body {
        sql_parser::query::SetOperand::Select(core) => core,
        sql_parser::query::SetOperand::Paren(..) => {
            return unsupported("a parenthesized query");
        }
    };
    if select.projection.len() != 1
        || !matches!(
            select.projection.items().next(),
            Some(sql_parser::query::SelectItem::Wildcard(_))
        )
    {
        return unsupported("explicit column lists (only SELECT *)");
    }
    if select.where_clause.is_some() {
        return unsupported("WHERE");
    }
    if select.distinct.is_some() {
        return unsupported("DISTINCT");
    }
    if select.having.is_some() {
        return unsupported("HAVING");
    }
    if select.group_by.is_some() {
        return unsupported("GROUP BY");
    }
    let Some(from) = &select.from else {
        return unsupported("SELECT without FROM");
    };
    if from.tables.len() != 1 {
        return unsupported("JOINs / multiple FROM tables");
    }
    let t = &from.tables.head;
    if !t.joins.is_empty() {
        return unsupported("JOINs / multiple FROM tables");
    }
    match &t.relation {
        sql_parser::query::TableFactor::Table { name, alias: None } => Ok(name.clone()),
        sql_parser::query::TableFactor::Table { alias: Some(_), .. } => {
            unsupported("aliases in FROM")
        }
        sql_parser::query::TableFactor::Derived { .. } => unsupported("subqueries in FROM"),
    }
}

// What Statement::execute's AlterTable arm actually dispatches on —
// parse_alter_table's own output, carrying just enough to call the
// matching Schema::add_column/drop_column/rename_column/
// add_foreign_key/drop_foreign_key. Column names are already lowercased
// here, matching every other identifier in this crate (see e.g.
// rows_from_insert's own column lookups).
enum AlterColumnOp {
    Add(crate::table::Field),
    Drop(String),
    Rename(String, String),
    AddForeignKey(crate::table::SqlForeignKey),
    DropForeignKey(String),
}

// Deliberately strict, same spirit as parse_select_star: accepts exactly
// one ADD COLUMN / DROP COLUMN / RENAME COLUMN / ADD [CONSTRAINT <name>]
// FOREIGN KEY / DROP CONSTRAINT operation per ALTER TABLE statement.
// Everything sqlparser used to let through and this rejected by hand
// (IF EXISTS, ONLY, SET LOCATION, ON CLUSTER, ICEBERG/DYNAMIC/EXTERNAL
// table types, dropping multiple columns at once, MySQL's FIRST/AFTER
// column position, ...) has no equivalent in sql-parser's grammar at
// all now — that SQL simply fails to parse instead of parsing and then
// being rejected here.
fn parse_alter_table(
    alter: &sql_parser::ddl::AlterTable,
) -> Result<(sql_parser::ObjectName, AlterColumnOp), SchemaError> {
    use sql_parser::ddl::{AlterTableOp, TableConstraintKind};
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "ALTER TABLE only supports a single ADD COLUMN / DROP COLUMN / RENAME COLUMN / \
             ADD FOREIGN KEY / DROP CONSTRAINT right now — {what} is not supported yet"
        )))
    };
    let table_name = alter.name.clone();
    let op = match &alter.operation {
        AlterTableOp::AddColumn(_, _, column_def) => {
            AlterColumnOp::Add(crate::table::Field::try_from(column_def)?)
        }
        AlterTableOp::DropColumn(_, _, name) => AlterColumnOp::Drop(name.value.to_lowercase()),
        AlterTableOp::RenameColumn(_, _, old, _, new) => {
            AlterColumnOp::Rename(old.value.to_lowercase(), new.value.to_lowercase())
        }
        AlterTableOp::AddConstraint(_, constraint) => match &constraint.kind {
            TableConstraintKind::ForeignKey(_, _, _, columns, _, reference) => {
                let cols: Vec<sql_parser::Ident> = columns.items().cloned().collect();
                AlterColumnOp::AddForeignKey(crate::table::foreign_key_from_constraint(
                    reference,
                    &cols,
                    constraint.name.as_ref().map(|(_, n)| n),
                )?)
            }
            _ => return unsupported("this ALTER TABLE ADD CONSTRAINT kind"),
        },
        AlterTableOp::DropConstraint(_, _, name) => {
            AlterColumnOp::DropForeignKey(name.value.to_lowercase())
        }
        AlterTableOp::RenameTo(..) => return unsupported("RENAME TABLE"),
    };
    Ok((table_name, op))
}

// COPY INTO's grammar (sql_parser::ddl::CopyInto) is already exactly as
// strict as this crate needs — "COPY INTO <table> FROM @<path>" and
// nothing else parses at all — so unlike parse_select_star/
// parse_alter_table there's no further validation to do here; this just
// extracts the two pieces schema::copy_csv_into needs.
fn parse_copy_into(c: &sql_parser::ddl::CopyInto) -> (sql_parser::ObjectName, String) {
    (c.table.clone(), c.path.path.clone())
}

impl<F> Display for Statement<F>
where
    F: DBFile + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Statement: id: {}, sql: {}", self.id, self.sql)?;
        Ok(())
    }
}

#[cfg(test)]
mod dummy_tests {

    use std::{ops::ControlFlow, sync::Arc};

    use sql_parser::{
        Expr, ObjectName, Statement,
        literal::Literal,
        query::{Query, SelectCore, TableFactor},
        visitor::{Visit, Visitor},
    };
    use store::named_memfile::NamedMemFile;

    use crate::{
        conn::connection::{Connection, ConnectionManager},
        error::SchemaError,
        rslt::resultset::ResultType,
    };

    struct V;
    impl Visitor for V {
        type Break = ();

        fn post_visit_expr(&mut self, _expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
            println!("post visit expr: {:?}", _expr);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            println!("post visit query: {:?}", _query);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
            println!("post visit rela: {:?}", _relation);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_select(&mut self, _select: &SelectCore) -> ControlFlow<Self::Break> {
            println!("post visit select: {:?}", _select);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
            println!("post visit stmt: {:?}", _statement);
            println!("");
            ControlFlow::Continue(())
        }
        fn post_visit_table_factor(
            &mut self,
            _table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            println!("post visit table_factor: {:?}", _table_factor);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_literal(
            &mut self,
            _lit: &sql_parser::literal::Literal,
        ) -> ControlFlow<Self::Break> {
            println!("post visit value: {:?}", _lit);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
            println!("pre visit expr: {:?}", _expr);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            println!("pre visit query: {:?}", _query);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
            println!("pre visit rela: {:?}", _relation);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_select(&mut self, _select: &SelectCore) -> ControlFlow<Self::Break> {
            println!("pre visit select: {:?}", _select);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
            println!("pre visit statement: {:?}", _statement);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            _table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit t_factor: {:?}", _table_factor);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_literal(&mut self, _lit: &Literal) -> ControlFlow<Self::Break> {
            println!("pre visit valye: {:?}", _lit);
            println!("");
            ControlFlow::Continue(())
        }
    }

    fn exec(sql: &str) {
        let stmt = sql_parser::parse_sql(sql).unwrap();
        let mut v = V;
        stmt[0].visit(&mut v);
        println!("\n");
        println!("{:?}", stmt[0]);
    }

    fn get_conn() -> Arc<Connection<NamedMemFile>> {
        let mgr = Arc::new(ConnectionManager::<NamedMemFile>::new());
        let conn = mgr.create_and_connect("test").unwrap();
        conn.use_schema(crate::constant::DEFAULT_SCHEMA_NAME)
            .unwrap();
        conn
    }

    fn exec_sql_safe(conn: Arc<Connection<NamedMemFile>>, sql: &str) -> Result<(), SchemaError> {
        let mut stmt = conn.create_statement(sql)?;
        stmt.execute()?;
        let mut next = stmt.get_results()?;
        while let Some(res) = next {
            match res {
                ResultType::Count(n) => println!("{n} rows affected"),
                ResultType::ResultString(s) => println!("Output : {s}"),
                ResultType::Result(r) => {
                    print_row(r.columns());
                    for r in r.rows_as_strings() {
                        print_row(&r);
                    }
                }
                ResultType::StreamingResult(r) => {
                    let mut r = r;
                    print_row(&r.columns());
                    while let Some(row) = &r.next_result_as_strings().unwrap() {
                        print_row(row);
                    }
                }
            }
            next = stmt.get_nextresult()?;
        }
        Ok(())
    }

    fn print_row(row: &[String]) {
        for r in row {
            print!("{r}|")
        }
        println!()
    }

    fn exec_sql(conn: Arc<Connection<NamedMemFile>>, sql: &str) {
        let r = exec_sql_safe(conn, sql);
        if let Err(r) = r {
            println!("Error: {:?}", r);
        } else {
            println!("Success!!");
        }
    }

    #[test]
    fn test1() {
        exec("select t.a,t.b from t");
    }

    #[test]
    fn test2() {
        let conn = get_conn();
        exec_sql(conn.clone(), "create table t1 (id int, name varchar(10))");
        exec_sql(conn.clone(), "insert into t1 values(1,'raj')");
        exec_sql(conn.clone(), "insert into t1 values(2,'kav')");
        // SELECT only supports "SELECT * FROM <table>" right now (see
        // parse_select_star) — no column lists or aggregates like
        // COUNT(*) yet.
        exec_sql(conn, "select *,* from t1");
    }
}
