use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use sqlparser::{
    ast::Statement,
    dialect::{Dialect, GenericDialect},
    parser::Parser,
};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    generator::Generator,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    constant::{MAX_TABLE_NAME_LEN, SYSTEM_SCHEMA},
    error::SchemaError,
    table::SqlTable,
};

#[derive(Clone)]
pub struct Schema<F: DBFile> {
    name: String,
    db: Arc<Db<F>>,
    tables: Arc<RwLock<HashMap<String, SqlTable>>>,
    sys_tables: Arc<RwLock<HashMap<String, TableIdType>>>,
    generator: Arc<Generator>,
    dialect: Arc<dyn Dialect>,
}

impl<F> Schema<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub fn create(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::create(&name)?;
        let mut s = Self {
            name,
            db: db.clone(),
            generator: db.get_generator(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_tables: Arc::new(RwLock::new(HashMap::new())),
            dialect: Arc::new(GenericDialect),
        };
        s.setup_schema()?;
        Ok(Arc::new(s))
    }

    fn setup_schema(&mut self) -> Result<(), SchemaError> {
        let id = self.db.create_table(SYSTEM_SCHEMA.into())?;
        self.sys_tables.write().insert(SYSTEM_SCHEMA.into(), id);
        Ok(())
    }

    fn close(self: Arc<Self>) -> Result<(F, F, F), SchemaError> {
        let tid = *self.sys_tables.read().get(SYSTEM_SCHEMA).unwrap();
        let tx = self.db.begin()?;
        for (n, t) in self.tables.read().iter() {
            let ik = IndexKey::new_from(&[ValueItem::Str((n.clone(), MAX_TABLE_NAME_LEN as u32))])?;
            self.db.update(
                tid,
                Tuple::new_with(DBIdType::Rec(ik), &to_allocvec(t)?, Some(tx.id()), None),
                &tx,
            )?;
        }
        self.db.commit(tx)?;
        let schema =
            Arc::try_unwrap(self).map_err(|_| SchemaError::UnknownError("Unknown error".into()))?;
        let (f, u, r) = schema.db.close()?;
        Ok((f, u, r))
    }

    fn load_schema(&mut self) -> Result<(), SchemaError> {
        let tid = self
            .db
            .table_id_by_name(SYSTEM_SCHEMA)?
            .ok_or(SchemaError::UnknownError(
                "Unable to load system tables!".into(),
            ))?;
        let mut cursor = self.db.table_scan(tid)?;
        while let Some(tuple) = cursor.next()? {
            let table = from_bytes::<SqlTable>(tuple.data())?;
            self.tables.write().insert(table.name.clone(), table);
        }
        Ok(())
    }

    pub fn open(name: String) -> Result<Arc<Self>, SchemaError> {
        let db = Db::open(&name)?;
        let mut s = Self {
            name: name.clone(),
            db: db.clone(),
            generator: db.get_generator(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            sys_tables: Arc::new(RwLock::new(HashMap::new())),
            dialect: Arc::new(GenericDialect),
        };
        s.load_schema()?;
        Ok(Arc::new(s))
    }

    pub fn execute(self: &Arc<Self>, sql: String) -> Result<(), SchemaError> {
        let dialect = GenericDialect;
        let statements = Parser::parse_sql(&dialect, sql.as_str())?;
        statements.iter().try_for_each(|s| self.exec_statement(s))?;

        Ok(())
    }

    fn exec_statement(self: &Arc<Self>, stmt: &Statement) -> Result<(), SchemaError> {
        match stmt {
            Statement::CreateTable(c) => {
                self.create_table(SqlTable::from_sql(self, c.clone())?)?;
            }
            Statement::AlterCollation(_) => {
                todo!()
            }
            _ => {}
        }
        Ok(())
    }

    fn create_table(self: &Arc<Self>, table: SqlTable) -> Result<(), SchemaError> {
        let mut table = table;

        // Resolve every index's target name up front and fail before
        // creating anything if one's already taken, so the loop below only
        // runs once none of them can collide — closes the one failure mode
        // that was actually reachable here (see drop_table's own doc
        // comment for why create_table_with_index_entry_size isn't undone
        // by self.db.rollback(txn): it's DDL, not a row-level, undo-logged
        // operation the way insert/update/remove are).
        let mut index_names = Vec::with_capacity(table.indices.len());
        for (count, i) in table.indices.iter().enumerate() {
            let iname = i
                .name
                .clone()
                .unwrap_or_else(|| format!("{}{}", table.name, count));
            if self.db.table_id_by_name(&iname)?.is_some() {
                return Err(SchemaError::BadTableName(format!(
                    "Index name {iname} is already in use"
                )));
            }
            index_names.push(iname);
        }

        let tid = *self.sys_tables.read().get(SYSTEM_SCHEMA).unwrap();
        let txn = self.db.begin()?;
        let ik = IndexKey::new_from(&[ValueItem::Str((
            table.name.clone(),
            MAX_TABLE_NAME_LEN as u32,
        ))])?;
        self.db.insert(
            tid,
            Tuple::new_with(
                DBIdType::Rec(ik),
                &to_allocvec(&table)?,
                Some(txn.id()),
                None,
            ),
            &txn,
        )?;
        // Tracks names as they're actually created (not just planned), so a
        // later failure in this same loop — a transient I/O error or lock
        // contention on some create_table_with_index_entry_size call, now
        // that a name collision can't happen here anymore — can be cleaned
        // up via drop_table instead of leaking. Safe to call unconditionally
        // here: nothing else can have discovered these tables yet (the
        // SqlTable row isn't in self.tables, and table_exists/get_table
        // can't see it either, until the whole create_table call succeeds).
        let mut created_names: Vec<String> = Vec::with_capacity(table.indices.len());
        let res: Result<(), SchemaError> = table
            .indices
            .iter_mut()
            .zip(index_names)
            .try_for_each(|(i, iname)| {
                let size = i.size();
                let iid = self
                    .db
                    .create_table_with_index_entry_size(iname.clone(), size as u64)?;
                i.db_table_id = iid;
                created_names.push(iname);
                Ok(())
            });
        if res.is_err() {
            for name in &created_names {
                self.db.drop_table(name)?;
            }
            self.db.rollback(txn)?;
            return res;
        }
        self.db.commit(txn)?;
        self.tables.write().insert(table.name.clone(), table);
        Ok(())
    }

    pub(crate) fn table_exists(self: &Arc<Self>, name: &str) -> bool {
        let name = name.to_lowercase();
        self.tables.read().contains_key(&name)
    }

    pub(crate) fn get_table(self: &Arc<Self>, name: &str) -> Option<SqlTable> {
        self.tables.read().get(&name.to_lowercase()).cloned()
    }

    #[allow(unused_variables)]
    fn dummy_stmt(stmt: Statement) {
        match stmt {
            Statement::Analyze(analyze) => todo!(),
            Statement::Set(set) => todo!(),
            Statement::Truncate(truncate) => todo!(),
            Statement::Msck(msck) => todo!(),
            Statement::Query(query) => todo!(),
            Statement::Insert(insert) => todo!(),
            Statement::Install { extension_name } => todo!(),
            Statement::Load { extension_name } => todo!(),
            Statement::Directory {
                overwrite,
                local,
                path,
                file_format,
                source,
            } => todo!(),
            Statement::Case(case_statement) => todo!(),
            Statement::If(if_statement) => todo!(),
            Statement::While(while_statement) => todo!(),
            Statement::Raise(raise_statement) => todo!(),
            Statement::Call(function) => todo!(),
            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                values,
            } => todo!(),
            Statement::CopyIntoSnowflake {
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
            } => todo!(),
            Statement::Open(open_statement) => todo!(),
            Statement::Close { cursor } => todo!(),
            Statement::Update(update) => todo!(),
            Statement::Delete(delete) => todo!(),
            Statement::CreateView(create_view) => todo!(),
            Statement::CreateTable(create_table) => todo!(),
            Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => todo!(),
            Statement::CreateIndex(create_index) => todo!(),
            Statement::CreateRole(create_role) => todo!(),
            Statement::CreateSecret {
                or_replace,
                temporary,
                if_not_exists,
                name,
                storage_specifier,
                secret_type,
                options,
            } => todo!(),
            Statement::CreateServer(create_server_statement) => todo!(),
            Statement::CreatePolicy(create_policy) => todo!(),
            Statement::CreateConnector(create_connector) => todo!(),
            Statement::CreateOperator(create_operator) => todo!(),
            Statement::CreateOperatorFamily(create_operator_family) => todo!(),
            Statement::CreateOperatorClass(create_operator_class) => todo!(),
            Statement::AlterTable(alter_table) => todo!(),
            Statement::AlterSchema(alter_schema) => todo!(),
            Statement::AlterIndex { name, operation } => todo!(),
            Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => todo!(),
            Statement::AlterFunction(alter_function) => todo!(),
            Statement::AlterType(alter_type) => todo!(),
            Statement::AlterCollation(alter_collation) => todo!(),
            Statement::AlterOperator(alter_operator) => todo!(),
            Statement::AlterOperatorFamily(alter_operator_family) => todo!(),
            Statement::AlterOperatorClass(alter_operator_class) => todo!(),
            Statement::AlterRole { name, operation } => todo!(),
            Statement::AlterPolicy(alter_policy) => todo!(),
            Statement::AlterConnector {
                name,
                properties,
                url,
                owner,
            } => todo!(),
            Statement::AlterSession {
                set,
                session_params,
            } => todo!(),
            Statement::AttachDatabase {
                schema_name,
                database_file_name,
                database,
            } => todo!(),
            Statement::AttachDuckDBDatabase {
                if_not_exists,
                database,
                database_path,
                database_alias,
                attach_options,
            } => todo!(),
            Statement::DetachDuckDBDatabase {
                if_exists,
                database,
                database_alias,
            } => todo!(),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => todo!(),
            Statement::DropFunction(drop_function) => todo!(),
            Statement::DropDomain(drop_domain) => todo!(),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                drop_behavior,
            } => todo!(),
            Statement::DropSecret {
                if_exists,
                temporary,
                name,
                storage_specifier,
            } => todo!(),
            Statement::DropPolicy(drop_policy) => todo!(),
            Statement::DropConnector { if_exists, name } => todo!(),
            Statement::Declare { stmts } => todo!(),
            Statement::CreateExtension(create_extension) => todo!(),
            Statement::CreateCollation(create_collation) => todo!(),
            Statement::DropExtension(drop_extension) => todo!(),
            Statement::DropOperator(drop_operator) => todo!(),
            Statement::DropOperatorFamily(drop_operator_family) => todo!(),
            Statement::DropOperatorClass(drop_operator_class) => todo!(),
            Statement::Fetch {
                name,
                direction,
                position,
                into,
            } => todo!(),
            Statement::Flush {
                object_type,
                location,
                channel,
                read_lock,
                export,
                tables,
            } => todo!(),
            Statement::Discard { object_type } => todo!(),
            Statement::ShowFunctions { filter } => todo!(),
            Statement::ShowVariable { variable } => todo!(),
            Statement::ShowStatus {
                filter,
                global,
                session,
            } => todo!(),
            Statement::ShowVariables {
                filter,
                global,
                session,
            } => todo!(),
            Statement::ShowCreate { obj_type, obj_name } => todo!(),
            Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => todo!(),
            Statement::ShowCatalogs {
                terse,
                history,
                show_options,
            } => todo!(),
            Statement::ShowDatabases {
                terse,
                history,
                show_options,
            } => todo!(),
            Statement::ShowProcessList { full } => todo!(),
            Statement::ShowSchemas {
                terse,
                history,
                show_options,
            } => todo!(),
            Statement::ShowCharset(show_charset) => todo!(),
            Statement::ShowObjects(show_objects) => todo!(),
            Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            } => todo!(),
            Statement::ShowViews {
                terse,
                materialized,
                show_options,
            } => todo!(),
            Statement::ShowCollation { filter } => todo!(),
            Statement::Use(_) => todo!(),
            Statement::StartTransaction {
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception,
                has_end_keyword,
            } => todo!(),
            Statement::Comment {
                object_type,
                object_name,
                comment,
                if_exists,
            } => todo!(),
            Statement::Commit {
                chain,
                end,
                modifier,
            } => todo!(),
            Statement::Rollback { chain, savepoint } => todo!(),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            } => todo!(),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
                or_replace,
                transient,
                clone,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                external_volume,
                catalog,
                replace_invalid_characters,
                default_ddl_collation,
                storage_serialization_policy,
                comment,
                default_charset,
                default_collation,
                catalog_sync,
                catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter,
                with_tags,
                with_contacts,
            } => todo!(),
            Statement::CreateFunction(create_function) => todo!(),
            Statement::CreateTrigger(create_trigger) => todo!(),
            Statement::DropTrigger(drop_trigger) => todo!(),
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                language,
                body,
            } => todo!(),
            Statement::CreateMacro {
                or_replace,
                temporary,
                name,
                args,
                definition,
            } => todo!(),
            Statement::CreateStage {
                or_replace,
                temporary,
                if_not_exists,
                name,
                stage_params,
                directory_table_params,
                file_format,
                copy_options,
                comment,
            } => todo!(),
            Statement::Assert { condition, message } => todo!(),
            Statement::Grant(grant) => todo!(),
            Statement::Deny(deny_statement) => todo!(),
            Statement::Revoke(revoke) => todo!(),
            Statement::Deallocate { name, prepare } => todo!(),
            Statement::Execute {
                name,
                parameters,
                has_parentheses,
                immediate,
                into,
                using,
                output,
                default,
            } => todo!(),
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => todo!(),
            Statement::Kill { modifier, id } => todo!(),
            Statement::ExplainTable {
                describe_alias,
                hive_format,
                has_table_keyword,
                table_name,
            } => todo!(),
            Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                query_plan,
                estimate,
                statement,
                format,
                options,
            } => todo!(),
            Statement::Savepoint { name } => todo!(),
            Statement::ReleaseSavepoint { name } => todo!(),
            Statement::Merge(merge) => todo!(),
            Statement::Cache {
                table_flag,
                table_name,
                has_as,
                options,
                query,
            } => todo!(),
            Statement::UNCache {
                table_name,
                if_exists,
            } => todo!(),
            Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
            } => todo!(),
            Statement::CreateDomain(create_domain) => todo!(),
            Statement::CreateType {
                name,
                representation,
            } => todo!(),
            Statement::Pragma { name, value, is_eq } => todo!(),
            Statement::Lock(lock) => todo!(),
            Statement::LockTables { tables } => todo!(),
            Statement::UnlockTables => todo!(),
            Statement::Unload {
                query,
                query_text,
                to,
                auth,
                with,
                options,
            } => todo!(),
            Statement::OptimizeTable {
                name,
                has_table_keyword,
                on_cluster,
                partition,
                include_final,
                deduplicate,
                predicate,
                zorder,
            } => todo!(),
            Statement::LISTEN { channel } => todo!(),
            Statement::UNLISTEN { channel } => todo!(),
            Statement::NOTIFY { channel, payload } => todo!(),
            Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            } => todo!(),
            Statement::RenameTable(rename_tables) => todo!(),
            Statement::List(file_staging_command) => todo!(),
            Statement::Remove(file_staging_command) => todo!(),
            Statement::RaisError {
                message,
                severity,
                state,
                arguments,
                options,
            } => todo!(),
            Statement::Throw(throw_statement) => todo!(),
            Statement::Print(print_statement) => todo!(),
            Statement::WaitFor(wait_for_statement) => todo!(),
            Statement::Return(return_statement) => todo!(),
            Statement::ExportData(export_data) => todo!(),
            Statement::CreateUser(create_user) => todo!(),
            Statement::AlterUser(alter_user) => todo!(),
            Statement::Vacuum(vacuum_statement) => todo!(),
            Statement::Reset(reset_statement) => todo!(),
        }
    }
    //    pub fn create_table(self: Arc<Self>, table: Table) -> Result<(), SchemaError> {}
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use store::memfile::MemFile;

    use super::*;
    use crate::datatype::DataType;

    fn schema() -> Arc<Schema<MemFile>> {
        Schema::create("test_schema".to_string()).unwrap()
    }

    // Runs `sql` against a fresh in-memory schema and returns the table
    // named `table_name` afterward — the create_table/from_sql helper
    // methods used before this was wired up returned the built SqlTable
    // directly; now that execute() persists (writes to the system table,
    // commits, and only then updates the in-memory map) and returns just
    // `()`, tests have to go back through the schema to see what actually
    // landed.
    fn create_and_fetch(sql: &str, table_name: &str) -> SqlTable {
        let s = schema();
        s.execute(sql.to_string()).unwrap();
        s.get_table(table_name)
            .unwrap_or_else(|| panic!("table {table_name:?} missing after successful execute()"))
    }

    fn field<'a>(t: &'a SqlTable, name: &str) -> &'a crate::table::Field {
        t.fields
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no field named {name:?} in {t:#?}"))
    }

    fn temp_schema_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("squeal_sql_test_{tag}_{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn test_create_table_extracts_name_and_field_count() {
        let t = create_and_fetch("create table users (id integer, name varchar(50))", "users");
        assert_eq!(t.name, "users");
        assert_eq!(t.fields.len(), 2);
    }

    #[test]
    fn test_create_table_maps_integer_type() {
        let t = create_and_fetch("create table t (id integer)", "t");
        assert_eq!(field(&t, "id").datatype, DataType::Integer);
    }

    #[test]
    fn test_create_table_maps_varchar_with_declared_length() {
        let t = create_and_fetch("create table t (name varchar(50))", "t");
        assert_eq!(field(&t, "name").datatype, DataType::Str(50));
    }

    #[test]
    fn test_create_table_maps_text_type() {
        let t = create_and_fetch("create table t (bio text)", "t");
        assert_eq!(field(&t, "bio").datatype, DataType::Str(32));
    }

    #[test]
    fn test_create_table_maps_double_type() {
        let t = create_and_fetch("create table t (price double)", "t");
        assert_eq!(field(&t, "price").datatype, DataType::Double);
    }

    #[test]
    fn test_create_table_unrecognized_type_becomes_unsupported() {
        // Documents current behavior rather than asserting it's desirable:
        // sqlparser::ast::DataType::Boolean has no ValueItem counterpart yet,
        // so it silently falls through to Unsupported instead of erroring.
        let t = create_and_fetch("create table t (active boolean)", "t");
        assert_eq!(field(&t, "active").datatype, DataType::Unsupported);
    }

    #[test]
    fn test_create_table_extracts_table_level_primary_key() {
        let t = create_and_fetch(
            "create table t (id integer not null, name varchar(50), primary key(id))",
            "t",
        );
        assert_eq!(t.indices.len(), 1);
        let idx = &t.indices[0];
        assert!(idx.is_primary);
        assert!(idx.is_unique);
        assert_eq!(idx.fields.len(), 1);
        assert_eq!(idx.fields[0].name, "id");
    }

    #[test]
    fn test_create_table_rejects_nullable_primary_key_column() {
        // Columns are nullable by default (see the not-null tests below),
        // and TableBuilder::build rejects a nullable primary/unique key —
        // that validation was previously unreachable since nullable used to
        // be hardcoded false everywhere.
        let s = schema();
        let err = s
            .execute("create table t (id integer, primary key(id))".to_string())
            .unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
        assert!(!s.table_exists("t"), "a rejected create must not persist");
    }

    #[test]
    fn test_create_table_extracts_table_level_unique_constraint() {
        let t = create_and_fetch(
            "create table t (id integer, email varchar(100) not null, unique(email))",
            "t",
        );
        assert_eq!(t.indices.len(), 1);
        let idx = &t.indices[0];
        assert!(!idx.is_primary);
        assert!(idx.is_unique);
        assert_eq!(idx.fields[0].name, "email");
    }

    #[test]
    fn test_create_table_extracts_inline_column_level_primary_key() {
        let t = create_and_fetch("create table t (id integer not null primary key)", "t");
        assert_eq!(t.indices.len(), 1);
        let idx = &t.indices[0];
        assert!(idx.is_primary);
        assert!(idx.is_unique);
        assert_eq!(idx.fields.len(), 1);
        assert_eq!(idx.fields[0].name, "id");
    }

    #[test]
    fn test_create_table_extracts_inline_column_level_unique() {
        let t = create_and_fetch("create table t (email varchar(100) not null unique)", "t");
        assert_eq!(t.indices.len(), 1);
        let idx = &t.indices[0];
        assert!(!idx.is_primary);
        assert!(idx.is_unique);
        assert_eq!(idx.fields[0].name, "email");
    }

    #[test]
    fn test_create_table_rejects_field_datatype_over_4mb() {
        // Field::new's size cap must actually be reached from SQL parsing
        // now (previously bypassed: From<&ColumnDef> for Field built Field
        // via a struct literal instead of routing through Field::new).
        let s = schema();
        let err = s
            .execute("create table t (bio varchar(5000000))".to_string())
            .unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    }

    #[test]
    fn test_execute_silently_ignores_non_create_table_statements() {
        // Documents current dispatch behavior: exec_statement only handles
        // Statement::CreateTable (and panics via todo!() on AlterCollation);
        // anything else, including an ordinary SELECT, falls through its
        // wildcard arm as a silent no-op rather than an error.
        let s = schema();
        s.execute("select 1".to_string()).unwrap();
        assert!(!s.table_exists("t"));
    }

    #[test]
    fn test_execute_rejects_malformed_sql() {
        let s = schema();
        let err = s.execute("create table (((".to_string()).unwrap_err();
        assert!(matches!(err, SchemaError::ParseError(_)), "got {err:?}");
    }

    #[test]
    fn test_create_table_rejects_field_name_over_128_chars() {
        let s = schema();
        let long_name = "a".repeat(129);
        let sql = format!("create table t ({long_name} integer)");
        let err = s.execute(sql).unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    }

    #[test]
    fn test_create_table_rejects_table_name_over_128_chars() {
        let s = schema();
        let long_name = "a".repeat(129);
        let sql = format!("create table {long_name} (id integer)");
        let err = s.execute(sql).unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    }

    #[test]
    fn test_create_table_rejects_a_table_that_already_exists() {
        let s = schema();
        s.execute("create table t (id integer)".to_string())
            .unwrap();
        let err = s
            .execute("create table t (id integer)".to_string())
            .unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
    }

    #[test]
    fn test_create_table_name_is_case_insensitive() {
        let s = schema();
        s.execute("create table Users (id integer)".to_string())
            .unwrap();
        assert!(s.table_exists("users"));
        assert!(s.table_exists("USERS"));
        assert!(s.get_table("USERS").is_some());
    }

    #[test]
    fn test_create_table_column_is_nullable_by_default() {
        let t = create_and_fetch("create table t (id integer)", "t");
        assert!(field(&t, "id").nullable);
    }

    #[test]
    fn test_create_table_not_null_column_is_not_nullable() {
        let t = create_and_fetch("create table t (id integer not null)", "t");
        assert!(!field(&t, "id").nullable);
    }

    #[test]
    fn test_create_table_preserves_declared_field_order() {
        let t = create_and_fetch("create table t (c integer, a integer, b integer)", "t");
        let names: Vec<_> = t.fields.iter().map(|f| f.name.clone()).collect();
        assert_eq!(
            names,
            vec!["c".to_string(), "a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn test_create_table_rejects_duplicate_field_name() {
        let s = schema();
        let err = s
            .execute("create table t (id integer, id varchar(10))".to_string())
            .unwrap_err();
        assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
    }

    // The actual ask: does a created table really persist through a close
    // + reopen, not just live in the in-memory `tables` map for the
    // lifetime of the current Schema? MemFile can't answer this — its
    // `open()` always hands back a fresh, empty buffer regardless of the
    // name given, so "reopening" it never proves anything was written to
    // durable storage. A real file-backed Schema is the only way to
    // exercise this honestly.
    #[test]
    fn test_created_table_survives_close_and_reopen_on_file_backend() {
        let path = temp_schema_path("create_reopen");
        Db::<File>::delete(&path).unwrap_or_default();

        let s = Schema::<File>::create(path.clone()).unwrap();
        s.execute(
            "create table users (id integer not null, name varchar(50), primary key(id))"
                .to_string(),
        )
        .unwrap();
        assert!(s.table_exists("users"));
        let before = s.get_table("users").unwrap();

        s.close().unwrap();

        let s2 = Schema::<File>::open(path.clone()).unwrap();
        assert!(
            s2.table_exists("users"),
            "table created before close() must still exist after reopen"
        );
        let after = s2.get_table("users").unwrap();

        assert_eq!(before.name, after.name);
        assert_eq!(after.fields.len(), 2);
        assert_eq!(field(&after, "id").datatype, DataType::Integer);
        assert_eq!(field(&after, "name").datatype, DataType::Str(50));
        assert_eq!(after.indices.len(), 1);
        assert!(after.indices[0].is_primary);
        assert_eq!(after.indices[0].fields[0].name, "id");

        Db::<File>::delete(&path).unwrap_or_default();
    }

    #[test]
    fn test_reopened_schema_rejects_recreating_an_existing_table() {
        // A more targeted version of the round-trip test above: confirms
        // load_schema() actually repopulates `tables` on open (not just
        // that get_table happens to still return something), by relying on
        // the duplicate-table-name check to fail for a genuinely fresh
        // Schema instance.
        let path = temp_schema_path("reopen_dup_check");
        Db::<File>::delete(&path).unwrap_or_default();

        let s = Schema::<File>::create(path.clone()).unwrap();
        s.execute("create table t (id integer)".to_string())
            .unwrap();
        s.close().unwrap();

        let s2 = Schema::<File>::open(path.clone()).unwrap();
        let err = s2
            .execute("create table t (id integer)".to_string())
            .unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");

        Db::<File>::delete(&path).unwrap_or_default();
    }

    #[test]
    fn test_create_table_with_no_indices_creates_no_backing_store_tables() {
        // Regression guard for the common case (no PRIMARY KEY / UNIQUE at
        // all): the indices.iter_mut().try_for_each(...) loop in
        // create_table must be a true no-op on an empty Vec, not error or
        // otherwise misbehave.
        let t = create_and_fetch("create table t (id integer, name varchar(50))", "t");
        assert!(t.indices.is_empty());
    }

    #[test]
    fn test_create_table_index_gets_a_real_backing_store_table() {
        let t = create_and_fetch(
            "create table t (id integer not null, primary key(id))",
            "t",
        );
        assert_eq!(t.indices.len(), 1);
        let idx = &t.indices[0];
        assert_ne!(
            idx.db_table_id,
            TableIdType::none(),
            "a created index must be assigned a real table id, not the none() sentinel"
        );
    }

    #[test]
    fn test_unnamed_index_gets_auto_generated_store_table_name() {
        let s = schema();
        s.execute("create table t (id integer not null, primary key(id))".to_string())
            .unwrap();
        let t = s.get_table("t").unwrap();
        let idx = &t.indices[0];
        // create_table's fallback is format!("{}{}", table.name, count) for
        // the first (count == 0) unnamed index.
        let found = s.db.table_id_by_name("t0").unwrap();
        assert_eq!(
            found,
            Some(idx.db_table_id),
            "the store-level table named \"t0\" must be exactly this index's own backing table"
        );
    }

    #[test]
    fn test_multiple_unnamed_indices_get_distinct_incrementing_names() {
        let s = schema();
        s.execute(
            "create table t (id integer not null, email varchar(50) not null, \
             primary key(id), unique(email))"
                .to_string(),
        )
        .unwrap();
        let t = s.get_table("t").unwrap();
        assert_eq!(t.indices.len(), 2);
        assert_ne!(t.indices[0].db_table_id, t.indices[1].db_table_id);

        let t0 = s.db.table_id_by_name("t0").unwrap();
        let t1 = s.db.table_id_by_name("t1").unwrap();
        assert!(t0.is_some() && t1.is_some(), "both t0 and t1 must exist");
        // Order in `indices` mirrors the order they were folded in (table-
        // level constraints first, then inline column options) — primary
        // key here comes from the table-level PRIMARY KEY(id) clause, so
        // it's index 0 / "t0"; UNIQUE(email) is index 1 / "t1".
        assert_eq!(t0, Some(t.indices[0].db_table_id));
        assert_eq!(t1, Some(t.indices[1].db_table_id));
    }

    #[test]
    fn test_explicitly_named_constraint_uses_that_name_for_the_backing_table() {
        let s = schema();
        s.execute(
            "create table t (id integer not null, constraint my_pk primary key(id))"
                .to_string(),
        )
        .unwrap();
        let t = s.get_table("t").unwrap();
        assert_eq!(t.indices[0].name, Some("my_pk".to_string()));
        let found = s.db.table_id_by_name("my_pk").unwrap();
        assert_eq!(found, Some(t.indices[0].db_table_id));
        // The auto-generated fallback name must NOT have been used once an
        // explicit name was given.
        assert_eq!(s.db.table_id_by_name("t0").unwrap(), None);
    }

    #[test]
    fn test_index_backing_table_size_reflects_field_datatypes() {
        // SqlIndex::size() sums each indexed field's DataType::size(), used
        // as the store-level index_entry_size — sanity-check it's at least
        // in the right ballpark for a Str(100) key (must be materially
        // larger than a bare Integer key's budget), rather than asserting
        // an exact byte count tied to ValueItem's own wire format.
        let s = schema();
        s.execute(
            "create table small (id integer not null, primary key(id))".to_string(),
        )
        .unwrap();
        s.execute(
            "create table big (email varchar(100) not null, primary key(email))".to_string(),
        )
        .unwrap();
        let small = s.get_table("small").unwrap();
        let big = s.get_table("big").unwrap();
        assert!(
            big.indices[0].size() > small.indices[0].size(),
            "a varchar(100) key's index budget ({}) should be larger than a bare \
             integer key's ({})",
            big.indices[0].size(),
            small.indices[0].size()
        );
    }

    #[test]
    fn test_created_indices_survive_close_and_reopen_on_file_backend() {
        let path = temp_schema_path("indices_reopen");
        Db::<File>::delete(&path).unwrap_or_default();

        let s = Schema::<File>::create(path.clone()).unwrap();
        s.execute(
            "create table users (id integer not null, email varchar(50) not null, \
             primary key(id), unique(email))"
                .to_string(),
        )
        .unwrap();
        let before = s.get_table("users").unwrap();
        assert_eq!(before.indices.len(), 2);
        s.close().unwrap();

        let s2 = Schema::<File>::open(path.clone()).unwrap();
        let after = s2.get_table("users").unwrap();
        assert_eq!(after.indices.len(), 2);
        // The SqlTable row itself (name/fields/indices metadata, including
        // each index's db_table_id) round-trips via the system schema
        // table; separately, the index's own *backing* store table must
        // still be independently findable by name after reopen too — two
        // different persistence paths (the metadata row vs. store's own
        // table registry) that both need to survive.
        assert_eq!(before.indices[0].db_table_id, after.indices[0].db_table_id);
        assert_eq!(before.indices[1].db_table_id, after.indices[1].db_table_id);
        assert!(s2.db.table_id_by_name("users0").unwrap().is_some());
        assert!(s2.db.table_id_by_name("users1").unwrap().is_some());

        Db::<File>::delete(&path).unwrap_or_default();
    }

    #[test]
    fn test_colliding_index_name_rejects_create_table_and_leaks_nothing() {
        // Regression test for a real bug found and fixed: create_table used
        // to interleave "create this index's backing store table" with the
        // rest of the loop, so a later index's name collision would leave
        // any *earlier* index in the same CREATE TABLE as an orphaned store
        // table — at the time, store had no drop_table at all, and
        // create_table_with_index_entry_size isn't a row-level, undo-logged
        // operation the way insert/update/remove are, so self.db.
        // rollback(txn) had no way to undo it. Fixed two ways, layered:
        // first by validating every index's target name up front (this
        // test) so a *collision* can never start the creation loop in the
        // first place; second, now that store::Db::drop_table exists, by
        // actually cleaning up any index the loop did create before some
        // *other* failure (see the test below, which exercises that path —
        // a collision can no longer reach it, per this test).
        //
        // t1 claims the name "shared_name" for its primary key's backing
        // table. t2 declares two indices — "idx_a" then "shared_name" (the
        // one that collides with t1's) — in declaration order. Both names
        // are checked before either is created, so "idx_a" must never be
        // created at all, not created-then-orphaned.
        let s = schema();
        s.execute(
            "create table t1 (id integer not null, constraint shared_name primary key(id))"
                .to_string(),
        )
        .unwrap();

        let err = s
            .execute(
                "create table t2 (id integer not null, val varchar(20) not null, \
                 constraint idx_a unique(val), constraint shared_name primary key(id))"
                    .to_string(),
            )
            .unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
        assert!(!s.table_exists("t2"), "t2 must not be persisted");
        assert!(
            s.db.table_id_by_name("idx_a").unwrap().is_none(),
            "idx_a must never be created at all, not created-then-orphaned"
        );
    }

    #[test]
    fn test_failed_second_index_creation_drops_the_first_instead_of_leaking_it() {
        // Exercises drop_table's actual wiring into create_table's cleanup
        // path — the validate-first name-collision check above closes that
        // one failure mode before the creation loop ever starts, so this
        // needs a *different* way for create_table_with_index_entry_size to
        // fail after an earlier index in the same statement already
        // succeeded: an index name that's individually invalid (too long),
        // which validate-first's "does this exact name already exist"
        // check can't catch, since nothing else has it yet.
        let s = schema();
        let too_long_name = "a".repeat(200);
        let err = s
            .execute(format!(
                "create table t (id integer not null, val varchar(20) not null, \
                 constraint ok_idx unique(id), constraint {too_long_name} unique(val))"
            ))
            .unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
        assert!(!s.table_exists("t"), "t must not be persisted");
        assert!(
            s.db.table_id_by_name("ok_idx").unwrap().is_none(),
            "ok_idx succeeded before the second index failed — it must have \
             been dropped by the cleanup path, not left as an orphaned store table"
        );
    }

    #[test]
    fn test_create_table_rejects_index_name_colliding_with_an_unrelated_store_table() {
        // Same validate-first check, from the angle of an index name
        // colliding with something that isn't even a squeal-sql table's
        // index — any name already registered in the underlying store::Db
        // must be rejected the same way.
        let s = schema();
        s.db.create_table("taken".to_string()).unwrap();

        let err = s
            .execute(
                "create table t (id integer not null, constraint taken primary key(id))"
                    .to_string(),
            )
            .unwrap_err();
        assert!(matches!(err, SchemaError::BadTableName(_)), "got {err:?}");
        assert!(!s.table_exists("t"));
    }
}
