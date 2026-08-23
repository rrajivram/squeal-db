use std::{fmt::Display, sync::Arc};

// GenericDialect doesn't recognize the DATABASE/SCHEMA keywords after
// USE at all (confirmed directly against sqlparser's own parse_use:
// only Hive/Databricks/Snowflake dialects special-case them) — Snowflake
// is the one actually used for parsing now, specifically so `USE
// DATABASE`/`USE SCHEMA` parse. Verified this doesn't change how any
// existing CREATE TABLE SQL in this crate parses (same ColumnDef/
// TableConstraint shapes come out either way).
use sqlparser::dialect::SnowflakeDialect;
use store::db::DBFile;
use uuid::Uuid;

use crate::{
    conn::connection::Connection, error::SchemaError, rslt::resultset::ResultType,
    table::SqlTable,
};

pub struct Statement<F: DBFile> {
    id: uuid::Uuid,
    sql: String,
    stmts: Vec<sqlparser::ast::Statement>,
    conn: Arc<Connection<F>>,
    results: Vec<ResultType>,
    current_result: Option<usize>,
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
                    self.results
                        .push(ResultType::ResultString(format!("Table '{table_name}' created")));
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
                    self.results
                        .push(ResultType::ResultString(format!("Table {table_name:?} altered")));
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
        return Err(SchemaError::BadTableName("table name cannot be empty".into()));
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
    if !matches!(&select.group_by, sqlparser::ast::GroupByExpr::Expressions(v, _) if v.is_empty())
    {
        return unsupported("GROUP BY");
    }
    match select.from.as_slice() {
        [sqlparser::ast::TableWithJoins { relation, joins }] if joins.is_empty() => match relation
        {
            sqlparser::ast::TableFactor::Table {
                name,
                alias: None,
                args: None,
                ..
            } => Ok(name.to_string().to_lowercase()),
            _ => unsupported("subqueries/table functions/aliases in FROM"),
        },
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
        [sqlparser::ast::AlterTableOperation::AddColumn {
            if_not_exists,
            column_def,
            column_position,
            ..
        }] => {
            if *if_not_exists {
                return unsupported("ADD COLUMN IF NOT EXISTS");
            }
            if column_position.is_some() {
                return unsupported("FIRST/AFTER column position");
            }
            AlterColumnOp::Add(crate::table::Field::try_from(column_def)?)
        }
        [sqlparser::ast::AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            drop_behavior,
            ..
        }] => {
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
        [sqlparser::ast::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        }] => AlterColumnOp::Rename(
            old_column_name.value.to_lowercase(),
            new_column_name.value.to_lowercase(),
        ),
        [sqlparser::ast::AlterTableOperation::AddConstraint {
            constraint: sqlparser::ast::TableConstraint::ForeignKey(fk),
            not_valid,
        }] => {
            if *not_valid {
                return unsupported("ADD CONSTRAINT ... NOT VALID");
            }
            AlterColumnOp::AddForeignKey(crate::table::foreign_key_from_constraint(fk, None)?)
        }
        [sqlparser::ast::AlterTableOperation::DropConstraint {
            if_exists,
            name,
            drop_behavior,
        }] => {
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

    use super::*;

    fn exec(sql: &str) {
        let stmt = sqlparser::parser::Parser::parse_sql(&SnowflakeDialect, sql).unwrap();
        for s in stmt {
            println!("{:?}", s);
        }
    }

    #[test]
    fn test_cd() {
        exec("create database if not exists test ");
    }

    #[test]
    fn test_ct() {
        exec("create table if not exists test (a int)");
    }
}
