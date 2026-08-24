use std::{fmt::Display, sync::Arc};

// GenericDialect doesn't recognize the DATABASE/SCHEMA keywords after
// USE at all (confirmed directly against sqlparser's own parse_use:
// only Hive/Databricks/Snowflake dialects special-case them) — Snowflake
// is the one actually used for parsing now, specifically so `USE
// DATABASE`/`USE SCHEMA` parse. Verified this doesn't change how any
// existing CREATE TABLE SQL in this crate parses (same ColumnDef/
// TableConstraint shapes come out either way).
use sqlparser::dialect::SnowflakeDialect;
use store::{db::DBFile, valueitem::ValueItem};
use uuid::Uuid;

use crate::{
    conn::connection::Connection, error::SchemaError, rslt::resultset::ResultType, table::SqlTable,
};

pub struct Statement<F: DBFile> {
    id: uuid::Uuid,
    sql: String,
    stmts: Vec<sqlparser::ast::Statement>,
    conn: Arc<Connection<F>>,
    results: Vec<ResultType>,
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
    template: sqlparser::ast::Statement,
    // Bound values, indexed by the "?" placeholder's position (0-based,
    // in the order it appears across the statement's own AST — see
    // count_placeholders) — None until set_field is called for that
    // index. Deliberately persists across execute() calls rather than
    // being cleared: a caller can either rebind every field before each
    // execute() or leave values as-is to repeat the same execution.
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
            sqlparser::ast::Statement::Insert(_)
            | sqlparser::ast::Statement::Delete(_)
            | sqlparser::ast::Statement::Update(_)
            | sqlparser::ast::Statement::Query(_) => {}
            _ => return Err(SchemaError::BadPreparedStatement(st.to_string())),
        }
        let param_count = count_placeholders(st);
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
            sqlparser::ast::Statement::Insert(insert) => insert.clone(),
            sqlparser::ast::Statement::Update(_) => {
                return Err(SchemaError::UserError(
                    "prepared UPDATE is not executable yet — UPDATE isn't supported by this \
                     engine at all yet"
                        .into(),
                ));
            }
            sqlparser::ast::Statement::Delete(_) => {
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
        self.stmt.stmts = vec![sqlparser::ast::Statement::Insert(substituted)];
        self.stmt.results.clear();
        self.stmt.current_result = None;
        self.stmt.execute()?;
        Ok(self
            .stmt
            .results
            .first()
            .cloned()
            .expect("Insert's own execute() arm always pushes exactly one result"))
    }
}

// How many "?" placeholders `stmt` has, in the order they'll be
// substituted (see substitute_insert_placeholders) — set_field's own
// index range is 0..this. Best-effort for Update/Delete/Query (their
// WHERE/SET expressions aren't restricted the way INSERT's VALUES are,
// so count_placeholders_in_expr's fallback of 0 for an expr shape it
// doesn't specifically walk is possible) — harmless since neither is
// actually executable yet (see PreparedStatement::execute); INSERT's
// count, the one that matters for real use, is exact.
fn count_placeholders(stmt: &sqlparser::ast::Statement) -> usize {
    match stmt {
        sqlparser::ast::Statement::Insert(insert) => insert
            .source
            .as_ref()
            .map(|q| match q.body.as_ref() {
                sqlparser::ast::SetExpr::Values(values) => values
                    .rows
                    .iter()
                    .flat_map(|r| r.content.iter())
                    .map(count_placeholders_in_expr)
                    .sum(),
                _ => 0,
            })
            .unwrap_or(0),
        sqlparser::ast::Statement::Update(update) => {
            update
                .assignments
                .iter()
                .map(|a| count_placeholders_in_expr(&a.value))
                .sum::<usize>()
                + update
                    .selection
                    .as_ref()
                    .map(count_placeholders_in_expr)
                    .unwrap_or(0)
        }
        sqlparser::ast::Statement::Delete(delete) => delete
            .selection
            .as_ref()
            .map(count_placeholders_in_expr)
            .unwrap_or(0),
        sqlparser::ast::Statement::Query(query) => match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => select
                .selection
                .as_ref()
                .map(count_placeholders_in_expr)
                .unwrap_or(0),
            _ => 0,
        },
        _ => 0,
    }
}

// Recursive placeholder count for one expression — handles the
// composite Expr shapes common enough to plausibly show up in a WHERE/
// SET clause; anything else falls back to 0 rather than trying to be
// exhaustive over sqlparser's full Expr enum (see count_placeholders'
// own doc comment on why that's an acceptable, honest limitation here).
fn count_placeholders_in_expr(expr: &sqlparser::ast::Expr) -> usize {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Value(v) => usize::from(matches!(v.value, sqlparser::ast::Value::Placeholder(_))),
        Expr::BinaryOp { left, right, .. } => {
            count_placeholders_in_expr(left) + count_placeholders_in_expr(right)
        }
        Expr::UnaryOp { expr, .. } => count_placeholders_in_expr(expr),
        Expr::Nested(expr) => count_placeholders_in_expr(expr),
        Expr::IsNull(expr) => count_placeholders_in_expr(expr),
        Expr::IsNotNull(expr) => count_placeholders_in_expr(expr),
        Expr::Cast { expr, .. } => count_placeholders_in_expr(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            count_placeholders_in_expr(expr)
                + count_placeholders_in_expr(low)
                + count_placeholders_in_expr(high)
        }
        Expr::InList { expr, list, .. } => {
            count_placeholders_in_expr(expr)
                + list.iter().map(count_placeholders_in_expr).sum::<usize>()
        }
        _ => 0,
    }
}

// Clones `insert`'s VALUES rows, replacing each "?" placeholder — in
// the same left-to-right, row-major order count_placeholders counted
// them in — with a literal Expr built from the correspondingly-bound
// param. Errors if a placeholder's slot was never bound (set_field
// never called for that index); can't error the other way (more bound
// params than placeholders) since params.len() is fixed at
// PreparedStatement::new time to exactly the placeholder count.
fn substitute_insert_placeholders(
    mut insert: sqlparser::ast::Insert,
    params: &[Option<ValueItem>],
) -> Result<sqlparser::ast::Insert, SchemaError> {
    let mut next = 0usize;
    if let Some(query) = &mut insert.source
        && let sqlparser::ast::SetExpr::Values(values) = query.body.as_mut()
    {
        for row in &mut values.rows {
            for expr in &mut row.content {
                substitute_placeholder_expr(expr, params, &mut next)?;
            }
        }
    }
    Ok(insert)
}

fn substitute_placeholder_expr(
    expr: &mut sqlparser::ast::Expr,
    params: &[Option<ValueItem>],
    next: &mut usize,
) -> Result<(), SchemaError> {
    if let sqlparser::ast::Expr::Value(v) = expr
        && matches!(v.value, sqlparser::ast::Value::Placeholder(_))
    {
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
// literally-typed INSERT already goes through.
fn value_item_to_expr(v: &ValueItem) -> Result<sqlparser::ast::Expr, SchemaError> {
    use sqlparser::ast::Value;
    let value = match v {
        ValueItem::Null => Value::Null,
        ValueItem::Integer(i) => Value::Number(i.to_string(), false),
        ValueItem::Double(d) => Value::Number(d.to_string(), false),
        ValueItem::Datetime(d) => Value::Number(d.to_string(), false),
        ValueItem::Str((s, _)) => Value::SingleQuotedString(s.clone()),
        ValueItem::Blob(_) => {
            return Err(SchemaError::UserError(
                "binding a Blob value into a prepared statement is not supported yet".into(),
            ));
        }
    };
    Ok(sqlparser::ast::Expr::Value(value.into()))
}

impl<F> Statement<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(sql: &str, conn: Arc<Connection<F>>) -> Result<Self, SchemaError> {
        let stmts = sqlparser::parser::Parser::parse_sql(&SnowflakeDialect, sql)?;
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
    fn semantic_validate(stmts: &[sqlparser::ast::Statement]) -> Result<(), SchemaError> {
        for stmt in stmts {
            match stmt {
                sqlparser::ast::Statement::CreateTable(c) => validate_create_table(c)?,
                sqlparser::ast::Statement::CreateDatabase { db_name, .. } => {
                    validate_identifier("database name", &db_name.to_string())?;
                }
                sqlparser::ast::Statement::CreateSchema { schema_name, .. } => {
                    validate_identifier("schema name", &schema_name_string(schema_name)?)?;
                }
                sqlparser::ast::Statement::Insert(insert) => validate_insert(insert)?,
                sqlparser::ast::Statement::Query(query) => {
                    parse_select_star(query)?;
                }
                sqlparser::ast::Statement::AlterTable(alter) => {
                    parse_alter_table(alter)?;
                }
                sqlparser::ast::Statement::CopyIntoSnowflake { .. } => {
                    parse_copy_into(stmt)?;
                }
                sqlparser::ast::Statement::StartTransaction { statements, .. }
                    if !statements.is_empty() =>
                {
                    return Err(SchemaError::UserError(
                        "BEGIN ... END blocks are not supported".into(),
                    ));
                }
                sqlparser::ast::Statement::Rollback {
                    savepoint: Some(_), ..
                } => {
                    return Err(SchemaError::UserError(
                        "ROLLBACK TO SAVEPOINT is not supported".into(),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn execute(&mut self) -> Result<(), SchemaError> {
        for stmt in &self.stmts {
            match stmt {
                sqlparser::ast::Statement::CreateTable(c) => {
                    let schema = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?;
                    let table_name = c.name.to_string();
                    schema.create_table(SqlTable::from_sql(&schema, c.clone())?)?;
                    self.results.push(ResultType::ResultString(format!(
                        "Table '{table_name}' created"
                    )));
                }
                sqlparser::ast::Statement::CreateDatabase {
                    db_name,
                    if_not_exists,
                    ..
                } => {
                    let name = db_name.to_string().to_lowercase();
                    let message = match self.conn.create_database(&name) {
                        Ok(()) => format!("Database '{name}' created"),
                        // IF NOT EXISTS on a name that's already open must
                        // still land the connection on it, same end state
                        // as the "didn't exist yet" path above — not a
                        // silent no-op that leaves the old database
                        // selected.
                        Err(SchemaError::DatabaseInUseError(_)) if *if_not_exists => {
                            self.conn.use_database(&name)?;
                            format!("Database '{name}' already exists")
                        }
                        Err(e) => return Err(e),
                    };
                    self.results.push(ResultType::ResultString(message));
                }
                sqlparser::ast::Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                    ..
                } => {
                    let name = schema_name_string(schema_name)?;
                    let message = match self.conn.create_schema(&name) {
                        Ok(()) => format!("Schema '{name}' created"),
                        Err(SchemaError::SchemaInUseError(_)) if *if_not_exists => {
                            self.conn.use_schema(&name)?;
                            format!("Schema '{name}' already exists")
                        }
                        Err(e) => return Err(e),
                    };
                    self.results.push(ResultType::ResultString(message));
                }
                sqlparser::ast::Statement::Use(u) => match u {
                    sqlparser::ast::Use::Database(name) => {
                        let name = name.to_string().to_lowercase();
                        self.conn.use_database(&name)?;
                        self.results
                            .push(ResultType::ResultString(format!("Using database '{name}'")));
                    }
                    sqlparser::ast::Use::Schema(name) => {
                        let name = name.to_string().to_lowercase();
                        self.conn.use_schema(&name)?;
                        self.results
                            .push(ResultType::ResultString(format!("Using schema '{name}'")));
                    }
                    // Other USE targets (catalog/warehouse/role/...) have
                    // no equivalent concept here yet — silently ignored,
                    // no result entry, same as an unhandled statement
                    // (see the wildcard fallback below).
                    _ => {}
                },
                sqlparser::ast::Statement::Insert(insert) => {
                    let schema = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?;
                    let table_name = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => {
                            name.to_string().to_lowercase()
                        }
                        other => {
                            return Err(SchemaError::UserError(format!(
                                "unsupported INSERT target: {other:?}"
                            )));
                        }
                    };
                    let table = schema.get_table(&table_name).ok_or_else(|| {
                        SchemaError::BadTableName(format!("Table {table_name:?} does not exist"))
                    })?;
                    let rows = table.rows_from_insert(insert)?;
                    let count = self
                        .conn
                        .with_current_txn(|txn| schema.insert_rows(&table_name, rows, txn))?;
                    self.results.push(ResultType::Count(count));
                }
                sqlparser::ast::Statement::StartTransaction { statements, .. } => {
                    if !statements.is_empty() {
                        return Err(SchemaError::UserError(
                            "BEGIN ... END blocks are not supported".into(),
                        ));
                    }
                    self.conn.begin_transaction()?;
                    self.results
                        .push(ResultType::ResultString("Transaction started".into()));
                }
                sqlparser::ast::Statement::Commit { .. } => {
                    self.conn.commit_transaction()?;
                    self.results
                        .push(ResultType::ResultString("Transaction committed".into()));
                }
                sqlparser::ast::Statement::Rollback { savepoint, .. } => {
                    if savepoint.is_some() {
                        return Err(SchemaError::UserError(
                            "ROLLBACK TO SAVEPOINT is not supported".into(),
                        ));
                    }
                    self.conn.rollback_transaction()?;
                    self.results
                        .push(ResultType::ResultString("Transaction rolled back".into()));
                }
                sqlparser::ast::Statement::Query(query) => {
                    let table_name = parse_select_star(query)?;
                    let schema = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?;
                    let result_set = self
                        .conn
                        .with_current_txn(|txn| schema.select_all(&table_name, txn))?;
                    self.results.push(ResultType::Result(result_set));
                }
                sqlparser::ast::Statement::AlterTable(alter) => {
                    let (table_name, op) = parse_alter_table(alter)?;
                    let schema = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?;
                    match op {
                        AlterColumnOp::Add(field) => schema.add_column(&table_name, field)?,
                        AlterColumnOp::Drop(name) => schema.drop_column(&table_name, &name)?,
                        AlterColumnOp::Rename(old, new) => {
                            schema.rename_column(&table_name, &old, &new)?
                        }
                        AlterColumnOp::AddForeignKey(fk) => {
                            schema.add_foreign_key(&table_name, fk)?
                        }
                        AlterColumnOp::DropForeignKey(name) => {
                            schema.drop_foreign_key(&table_name, &name)?
                        }
                    }
                    self.results.push(ResultType::ResultString(format!(
                        "Table {table_name:?} altered"
                    )));
                }
                sqlparser::ast::Statement::CopyIntoSnowflake { .. } => {
                    let (table_name, path) = parse_copy_into(stmt)?;
                    let schema = self
                        .conn
                        .current_schema()
                        .ok_or(SchemaError::NoSchemaSelected)?;
                    let (loaded, failed) = schema.copy_csv_into(&table_name, &path)?;
                    self.results.push(ResultType::ResultString(format!(
                        "{loaded} row(s) loaded, {failed} row(s) failed"
                    )));
                }
                sqlparser::ast::Statement::AlterCollation(_) => {
                    todo!()
                }
                _ => {}
            }
        }
        Ok(())
    }

    // Returns the "current" result, initializing the cursor to the first
    // one on first call — idempotent otherwise (repeated calls return the
    // same result until get_nextresult advances it). None if there are no
    // results at all, or the cursor has been advanced past the last one.
    pub fn get_results(&mut self) -> Result<Option<ResultType>, SchemaError> {
        let i = *self.current_result.get_or_insert(0);
        Ok(self.results.get(i).cloned())
    }

    // Advances the cursor to the next result and returns it, or None if
    // there isn't one — the cursor is left unchanged in that case, so a
    // following get_results() still returns the last valid result rather
    // than nothing.
    pub fn get_nextresult(&mut self) -> Result<Option<ResultType>, SchemaError> {
        let next = self.current_result.map_or(0, |i| i + 1);
        match self.results.get(next) {
            Some(r) => {
                self.current_result = Some(next);
                Ok(Some(r.clone()))
            }
            None => Ok(None),
        }
    }
}

// CREATE SCHEMA's name can also be an AUTHORIZATION clause (Postgres-
// style: `CREATE SCHEMA AUTHORIZATION role` or `CREATE SCHEMA name
// AUTHORIZATION role`) — there's no role/authorization concept here, so
// only the plain-name form is supported.
fn schema_name_string(name: &sqlparser::ast::SchemaName) -> Result<String, SchemaError> {
    match name {
        sqlparser::ast::SchemaName::Simple(obj) => Ok(obj.to_string().to_lowercase()),
        sqlparser::ast::SchemaName::UnnamedAuthorization(_)
        | sqlparser::ast::SchemaName::NamedAuthorization(_, _) => Err(SchemaError::UserError(
            "CREATE SCHEMA ... AUTHORIZATION is not supported".into(),
        )),
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

fn validate_create_table(c: &sqlparser::ast::CreateTable) -> Result<(), SchemaError> {
    validate_table_name(&c.name.to_string())?;

    let mut seen = std::collections::HashSet::with_capacity(c.columns.len());
    for col in &c.columns {
        let name = col.name.value.to_lowercase();
        validate_identifier("column name", &name)?;
        if !seen.insert(name.clone()) {
            return Err(SchemaError::UserError(format!(
                "duplicate column name: {name}"
            )));
        }
    }

    for constraint in &c.constraints {
        let fields: Vec<String> = match constraint {
            sqlparser::ast::TableConstraint::Unique(u) => u
                .columns
                .iter()
                .map(|c| c.column.to_string().to_lowercase())
                .collect(),
            sqlparser::ast::TableConstraint::PrimaryKey(p) => p
                .columns
                .iter()
                .map(|c| c.column.to_string().to_lowercase())
                .collect(),
            // Reuses the same conversion from_sql itself calls later —
            // rejects composite keys/ON DELETE/ON UPDATE/etc. here too,
            // not just once execute() actually gets there, and its
            // `column` is what needs checking against `seen` below,
            // same as Unique/PrimaryKey's own local columns.
            sqlparser::ast::TableConstraint::ForeignKey(fk) => {
                vec![crate::table::foreign_key_from_constraint(fk, None)?.column]
            }
            _ => vec![],
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

fn validate_insert(insert: &sqlparser::ast::Insert) -> Result<(), SchemaError> {
    // Nothing downstream reads the alias yet (no SELECT/JOIN support to
    // attach it to) — just checked for being a well-formed identifier.
    if let Some(alias) = &insert.table_alias {
        validate_identifier("table alias", &alias.alias.value)?;
    }

    let mut seen = std::collections::HashSet::with_capacity(insert.columns.len());
    for col in &insert.columns {
        let name = col.to_string().to_lowercase();
        validate_identifier("column name", &name)?;
        if !seen.insert(name.clone()) {
            return Err(SchemaError::UserError(format!(
                "duplicate column name in INSERT column list: {name}"
            )));
        }
    }

    // Row width vs. an *explicit* column list is checkable without a
    // schema lookup; against the implicit "every field, in declared
    // order" form (empty column list) it isn't — that's deferred to
    // rows_from_insert, which has the actual table to check against.
    if !insert.columns.is_empty()
        && let Some(query) = &insert.source
        && let sqlparser::ast::SetExpr::Values(values) = query.body.as_ref()
    {
        for row in &values.rows {
            if row.content.len() != insert.columns.len() {
                return Err(SchemaError::UserError(format!(
                    "INSERT column list has {} column(s) but a VALUES row has {}",
                    insert.columns.len(),
                    row.content.len()
                )));
            }
        }
    }
    Ok(())
}

// Deliberately strict launchpad for real relational algebra support:
// accepts exactly "SELECT * FROM <table>" and rejects (with a specific
// message, not silent ignoring) anything else — WITH/CTEs, ORDER BY,
// LIMIT, locking clauses, set operations, explicit column lists, WHERE,
// DISTINCT, HAVING, GROUP BY, joins/multiple FROM tables, and
// subqueries/table-functions/aliases in FROM.
fn parse_select_star(query: &sqlparser::ast::Query) -> Result<String, SchemaError> {
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
    if query.limit_clause.is_some() {
        return unsupported("LIMIT");
    }
    if !query.locks.is_empty() {
        return unsupported("locking clauses");
    }
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(select) => select,
        _ => return unsupported("set operations (UNION/INTERSECT/EXCEPT) or non-SELECT queries"),
    };
    match select.projection.as_slice() {
        [sqlparser::ast::SelectItem::Wildcard(_)] => {}
        _ => return unsupported("explicit column lists (only SELECT *)"),
    }
    if select.selection.is_some() {
        return unsupported("WHERE");
    }
    if select.distinct.is_some() {
        return unsupported("DISTINCT");
    }
    if select.having.is_some() {
        return unsupported("HAVING");
    }
    if !matches!(&select.group_by, sqlparser::ast::GroupByExpr::Expressions(v, _) if v.is_empty()) {
        return unsupported("GROUP BY");
    }
    match select.from.as_slice() {
        [sqlparser::ast::TableWithJoins { relation, joins }] if joins.is_empty() => {
            match relation {
                sqlparser::ast::TableFactor::Table {
                    name,
                    alias: None,
                    args: None,
                    ..
                } => Ok(name.to_string().to_lowercase()),
                _ => unsupported("subqueries/table functions/aliases in FROM"),
            }
        }
        [] => unsupported("SELECT without FROM"),
        _ => unsupported("JOINs / multiple FROM tables"),
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

// Deliberately strict, same spirit as parse_select_star: accepts
// exactly one ADD COLUMN / DROP COLUMN / RENAME COLUMN / ADD [CONSTRAINT
// <name>] FOREIGN KEY / DROP CONSTRAINT operation per ALTER TABLE
// statement and rejects everything else sqlparser's AlterTableOperation
// can represent (other constraint kinds, projections, partitions,
// RENAME TABLE, IF EXISTS/IF NOT EXISTS, CASCADE/RESTRICT, dropping
// more than one column at once, MySQL's FIRST/AFTER column position,
// ...) with a specific message rather than silently doing something
// other than what the SQL asked for.
fn parse_alter_table(
    alter: &sqlparser::ast::AlterTable,
) -> Result<(String, AlterColumnOp), SchemaError> {
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "ALTER TABLE only supports a single ADD COLUMN / DROP COLUMN / RENAME COLUMN / \
             ADD FOREIGN KEY / DROP CONSTRAINT right now — {what} is not supported yet"
        )))
    };
    if alter.if_exists {
        return unsupported("IF EXISTS");
    }
    if alter.only {
        return unsupported("ONLY");
    }
    if alter.location.is_some() {
        return unsupported("SET LOCATION");
    }
    if alter.on_cluster.is_some() {
        return unsupported("ON CLUSTER");
    }
    if alter.table_type.is_some() {
        return unsupported("ICEBERG/DYNAMIC/EXTERNAL table types");
    }
    let table_name = alter.name.to_string().to_lowercase();
    let op = match alter.operations.as_slice() {
        [
            sqlparser::ast::AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            },
        ] => {
            if *if_not_exists {
                return unsupported("ADD COLUMN IF NOT EXISTS");
            }
            if column_position.is_some() {
                return unsupported("FIRST/AFTER column position");
            }
            AlterColumnOp::Add(crate::table::Field::try_from(column_def)?)
        }
        [
            sqlparser::ast::AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                drop_behavior,
                ..
            },
        ] => {
            if *if_exists {
                return unsupported("DROP COLUMN IF EXISTS");
            }
            if drop_behavior.is_some() {
                return unsupported("CASCADE/RESTRICT");
            }
            match column_names.as_slice() {
                [name] => AlterColumnOp::Drop(name.value.to_lowercase()),
                _ => return unsupported("dropping multiple columns in one statement"),
            }
        }
        [
            sqlparser::ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            },
        ] => AlterColumnOp::Rename(
            old_column_name.value.to_lowercase(),
            new_column_name.value.to_lowercase(),
        ),
        [
            sqlparser::ast::AlterTableOperation::AddConstraint {
                constraint: sqlparser::ast::TableConstraint::ForeignKey(fk),
                not_valid,
            },
        ] => {
            if *not_valid {
                return unsupported("ADD CONSTRAINT ... NOT VALID");
            }
            AlterColumnOp::AddForeignKey(crate::table::foreign_key_from_constraint(fk, None)?)
        }
        [
            sqlparser::ast::AlterTableOperation::DropConstraint {
                if_exists,
                name,
                drop_behavior,
            },
        ] => {
            if *if_exists {
                return unsupported("DROP CONSTRAINT IF EXISTS");
            }
            if drop_behavior.is_some() {
                return unsupported("CASCADE/RESTRICT");
            }
            AlterColumnOp::DropForeignKey(name.value.to_lowercase())
        }
        [] => return unsupported("ALTER TABLE with no operations"),
        [_] => return unsupported("this ALTER TABLE operation"),
        _ => return unsupported("multiple operations in one ALTER TABLE statement"),
    };
    Ok((table_name, op))
}

// Deliberately strict, same spirit as parse_select_star/parse_alter_table:
// accepts exactly "COPY INTO <table> FROM @<path>" — a literal local
// filesystem path, `@` stripped, not a real Snowflake stage (no
// credentials/URL/storage-integration resolution) — and rejects every
// other CopyIntoSnowflake option (COPY INTO <location> unload direction,
// target column list, source alias, FILES/PATTERN, FILE_FORMAT/COPY
// options, VALIDATION_MODE, PARTITION BY, loading from a query instead
// of a stage) with a specific message.
fn parse_copy_into(stmt: &sqlparser::ast::Statement) -> Result<(String, String), SchemaError> {
    let sqlparser::ast::Statement::CopyIntoSnowflake {
        kind,
        into,
        into_columns,
        from_obj,
        from_obj_alias,
        stage_params,
        from_transformations,
        from_query,
        files,
        pattern,
        file_format,
        copy_options,
        validation_mode,
        partition,
    } = stmt
    else {
        unreachable!("caller already matched Statement::CopyIntoSnowflake");
    };
    let unsupported = |what: &str| {
        Err(SchemaError::UserError(format!(
            "COPY INTO only supports \"COPY INTO <table> FROM @<path>\" right now — {what} is \
             not supported yet"
        )))
    };
    if !matches!(kind, sqlparser::ast::CopyIntoSnowflakeKind::Table) {
        return unsupported("COPY INTO <location> (unloading)");
    }
    if into_columns.is_some() {
        return unsupported("an explicit target column list");
    }
    if from_obj_alias.is_some() {
        return unsupported("a source alias");
    }
    if stage_params.url.is_some()
        || !stage_params.encryption.options.is_empty()
        || stage_params.endpoint.is_some()
        || stage_params.storage_integration.is_some()
        || !stage_params.credentials.options.is_empty()
    {
        return unsupported("stage credentials/URL/endpoint/storage integration");
    }
    if from_transformations.is_some() {
        return unsupported("column transformations");
    }
    if from_query.is_some() {
        return unsupported("loading from a query instead of a stage");
    }
    if files.is_some() {
        return unsupported("an explicit FILES list");
    }
    if pattern.is_some() {
        return unsupported("PATTERN");
    }
    if !file_format.options.is_empty() {
        return unsupported("FILE_FORMAT options — CSV is always assumed");
    }
    if !copy_options.options.is_empty() {
        return unsupported("COPY options");
    }
    if validation_mode.is_some() {
        return unsupported("VALIDATION_MODE");
    }
    if partition.is_some() {
        return unsupported("PARTITION BY");
    }
    let table_name = into.to_string().to_lowercase();
    let path = from_obj
        .as_ref()
        .ok_or_else(|| SchemaError::UserError("COPY INTO needs a FROM @<path>".into()))?
        .to_string();
    let path = path.strip_prefix('@').unwrap_or(&path).to_string();
    Ok((table_name, path))
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

    use std::ops::ControlFlow;

    use sqlparser::ast::{Visit, VisitMut, Visitor};

    use super::*;

    #[derive(Default)]
    struct V {}

    impl Visitor for V {
        type Break = ();

        fn post_visit_expr(
            &mut self,
            _expr: &sqlparser::ast::Expr,
        ) -> std::ops::ControlFlow<Self::Break> {
            println!("post visit expr: {:?}", _expr);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            println!("post visit query: {:?}", _query);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_relation(
            &mut self,
            _relation: &sqlparser::ast::ObjectName,
        ) -> ControlFlow<Self::Break> {
            println!("post visit rela: {:?}", _relation);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_select(
            &mut self,
            _select: &sqlparser::ast::Select,
        ) -> ControlFlow<Self::Break> {
            println!("post visit select: {:?}", _select);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_statement(
            &mut self,
            _statement: &sqlparser::ast::Statement,
        ) -> ControlFlow<Self::Break> {
            println!("post visit stmt: {:?}", _statement);
            println!("");
            ControlFlow::Continue(())
        }
        fn post_visit_table_factor(
            &mut self,
            _table_factor: &sqlparser::ast::TableFactor,
        ) -> ControlFlow<Self::Break> {
            println!("post visit table_factor: {:?}", _table_factor);
            println!("");
            ControlFlow::Continue(())
        }

        fn post_visit_value(
            &mut self,
            _value: &sqlparser::ast::ValueWithSpan,
        ) -> ControlFlow<Self::Break> {
            println!("post visit value: {:?}", _value);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, _expr: &sqlparser::ast::Expr) -> ControlFlow<Self::Break> {
            println!("pre visit expr: {:?}", _expr);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            println!("pre visit query: {:?}", _query);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_relation(
            &mut self,
            _relation: &sqlparser::ast::ObjectName,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit rela: {:?}", _relation);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_select(
            &mut self,
            _select: &sqlparser::ast::Select,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit select: {:?}", _select);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_statement(
            &mut self,
            _statement: &sqlparser::ast::Statement,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit statement: {:?}", _statement);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            _table_factor: &sqlparser::ast::TableFactor,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit t_factor: {:?}", _table_factor);
            println!("");
            ControlFlow::Continue(())
        }

        fn pre_visit_value(
            &mut self,
            _value: &sqlparser::ast::ValueWithSpan,
        ) -> ControlFlow<Self::Break> {
            println!("pre visit valye: {:?}", _value);
            println!("");
            ControlFlow::Continue(())
        }
    }

    fn exec(sql: &str) {
        let stmt = sqlparser::parser::Parser::parse_sql(&SnowflakeDialect, sql).unwrap();
        let mut v = V::default();
        let _ = stmt[0].visit(&mut v);
        //        for s in stmt {
        //            println!("{:?}", s);
        //       }
    }

    #[test]
    fn test_cd() {
        exec("create database if not exists test ");
    }

    #[test]
    fn test_ct() {
        exec("create table if not exists test (a int)");
    }

    #[test]
    fn test_sel() {
        exec("select 1+3 ,TT.a from t1  as TT");
    }
}
