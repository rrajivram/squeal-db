use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use sqlparser::{dialect::GenericDialect, parser::Parser};
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
    dialect: Arc<GenericDialect>,
}

#[cfg(test)]
mod tests;

impl<F> Schema<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub fn create_database(name: String) -> Result<Arc<Self>, SchemaError> {
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

    fn close_database(self: Arc<Self>) -> Result<(F, F, F), SchemaError> {
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

    fn exec_statement(
        self: &Arc<Self>,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<(), SchemaError> {
        match stmt {
            sqlparser::ast::Statement::CreateTable(c) => {
                self.create_table(SqlTable::from_sql(self, c.clone())?)?;
            }
            sqlparser::ast::Statement::AlterCollation(_) => {
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
        let res: Result<(), SchemaError> =
            table
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
    fn dummy_stmt(stmt: sqlparser::ast::Statement) {
        match stmt {
            sqlparser::ast::Statement::Analyze(analyze) => todo!(),
            sqlparser::ast::Statement::Set(set) => todo!(),
            sqlparser::ast::Statement::Truncate(truncate) => todo!(),
            sqlparser::ast::Statement::Msck(msck) => todo!(),
            sqlparser::ast::Statement::Query(query) => todo!(),
            sqlparser::ast::Statement::Insert(insert) => todo!(),
            sqlparser::ast::Statement::Install { extension_name } => todo!(),
            sqlparser::ast::Statement::Load { extension_name } => todo!(),
            sqlparser::ast::Statement::Directory {
                overwrite,
                local,
                path,
                file_format,
                source,
            } => todo!(),
            sqlparser::ast::Statement::Case(case_statement) => todo!(),
            sqlparser::ast::Statement::If(if_statement) => todo!(),
            sqlparser::ast::Statement::While(while_statement) => todo!(),
            sqlparser::ast::Statement::Raise(raise_statement) => todo!(),
            sqlparser::ast::Statement::Call(function) => todo!(),
            sqlparser::ast::Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                values,
            } => todo!(),
            sqlparser::ast::Statement::CopyIntoSnowflake {
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
            sqlparser::ast::Statement::Open(open_statement) => todo!(),
            sqlparser::ast::Statement::Close { cursor } => todo!(),
            sqlparser::ast::Statement::Update(update) => todo!(),
            sqlparser::ast::Statement::Delete(delete) => todo!(),
            sqlparser::ast::Statement::CreateView(create_view) => todo!(),
            sqlparser::ast::Statement::CreateTable(create_table) => todo!(),
            sqlparser::ast::Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => todo!(),
            sqlparser::ast::Statement::CreateIndex(create_index) => todo!(),
            sqlparser::ast::Statement::CreateRole(create_role) => todo!(),
            sqlparser::ast::Statement::CreateSecret {
                or_replace,
                temporary,
                if_not_exists,
                name,
                storage_specifier,
                secret_type,
                options,
            } => todo!(),
            sqlparser::ast::Statement::CreateServer(create_server_statement) => todo!(),
            sqlparser::ast::Statement::CreatePolicy(create_policy) => todo!(),
            sqlparser::ast::Statement::CreateConnector(create_connector) => todo!(),
            sqlparser::ast::Statement::CreateOperator(create_operator) => todo!(),
            sqlparser::ast::Statement::CreateOperatorFamily(create_operator_family) => todo!(),
            sqlparser::ast::Statement::CreateOperatorClass(create_operator_class) => todo!(),
            sqlparser::ast::Statement::AlterTable(alter_table) => todo!(),
            sqlparser::ast::Statement::AlterSchema(alter_schema) => todo!(),
            sqlparser::ast::Statement::AlterIndex { name, operation } => todo!(),
            sqlparser::ast::Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => todo!(),
            sqlparser::ast::Statement::AlterFunction(alter_function) => todo!(),
            sqlparser::ast::Statement::AlterType(alter_type) => todo!(),
            sqlparser::ast::Statement::AlterCollation(alter_collation) => todo!(),
            sqlparser::ast::Statement::AlterOperator(alter_operator) => todo!(),
            sqlparser::ast::Statement::AlterOperatorFamily(alter_operator_family) => todo!(),
            sqlparser::ast::Statement::AlterOperatorClass(alter_operator_class) => todo!(),
            sqlparser::ast::Statement::AlterRole { name, operation } => todo!(),
            sqlparser::ast::Statement::AlterPolicy(alter_policy) => todo!(),
            sqlparser::ast::Statement::AlterConnector {
                name,
                properties,
                url,
                owner,
            } => todo!(),
            sqlparser::ast::Statement::AlterSession {
                set,
                session_params,
            } => todo!(),
            sqlparser::ast::Statement::AttachDatabase {
                schema_name,
                database_file_name,
                database,
            } => todo!(),
            sqlparser::ast::Statement::AttachDuckDBDatabase {
                if_not_exists,
                database,
                database_path,
                database_alias,
                attach_options,
            } => todo!(),
            sqlparser::ast::Statement::DetachDuckDBDatabase {
                if_exists,
                database,
                database_alias,
            } => todo!(),
            sqlparser::ast::Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => todo!(),
            sqlparser::ast::Statement::DropFunction(drop_function) => todo!(),
            sqlparser::ast::Statement::DropDomain(drop_domain) => todo!(),
            sqlparser::ast::Statement::DropProcedure {
                if_exists,
                proc_desc,
                drop_behavior,
            } => todo!(),
            sqlparser::ast::Statement::DropSecret {
                if_exists,
                temporary,
                name,
                storage_specifier,
            } => todo!(),
            sqlparser::ast::Statement::DropPolicy(drop_policy) => todo!(),
            sqlparser::ast::Statement::DropConnector { if_exists, name } => todo!(),
            sqlparser::ast::Statement::Declare { stmts } => todo!(),
            sqlparser::ast::Statement::CreateExtension(create_extension) => todo!(),
            sqlparser::ast::Statement::CreateCollation(create_collation) => todo!(),
            sqlparser::ast::Statement::DropExtension(drop_extension) => todo!(),
            sqlparser::ast::Statement::DropOperator(drop_operator) => todo!(),
            sqlparser::ast::Statement::DropOperatorFamily(drop_operator_family) => todo!(),
            sqlparser::ast::Statement::DropOperatorClass(drop_operator_class) => todo!(),
            sqlparser::ast::Statement::Fetch {
                name,
                direction,
                position,
                into,
            } => todo!(),
            sqlparser::ast::Statement::Flush {
                object_type,
                location,
                channel,
                read_lock,
                export,
                tables,
            } => todo!(),
            sqlparser::ast::Statement::Discard { object_type } => todo!(),
            sqlparser::ast::Statement::ShowFunctions { filter } => todo!(),
            sqlparser::ast::Statement::ShowVariable { variable } => todo!(),
            sqlparser::ast::Statement::ShowStatus {
                filter,
                global,
                session,
            } => todo!(),
            sqlparser::ast::Statement::ShowVariables {
                filter,
                global,
                session,
            } => todo!(),
            sqlparser::ast::Statement::ShowCreate { obj_type, obj_name } => todo!(),
            sqlparser::ast::Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowCatalogs {
                terse,
                history,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowDatabases {
                terse,
                history,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowProcessList { full } => todo!(),
            sqlparser::ast::Statement::ShowSchemas {
                terse,
                history,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowCharset(show_charset) => todo!(),
            sqlparser::ast::Statement::ShowObjects(show_objects) => todo!(),
            sqlparser::ast::Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowViews {
                terse,
                materialized,
                show_options,
            } => todo!(),
            sqlparser::ast::Statement::ShowCollation { filter } => todo!(),
            sqlparser::ast::Statement::Use(_) => todo!(),
            sqlparser::ast::Statement::StartTransaction {
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception,
                has_end_keyword,
            } => todo!(),
            sqlparser::ast::Statement::Comment {
                object_type,
                object_name,
                comment,
                if_exists,
            } => todo!(),
            sqlparser::ast::Statement::Commit {
                chain,
                end,
                modifier,
            } => todo!(),
            sqlparser::ast::Statement::Rollback { chain, savepoint } => todo!(),
            sqlparser::ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            } => todo!(),
            sqlparser::ast::Statement::CreateDatabase {
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
            sqlparser::ast::Statement::CreateFunction(create_function) => todo!(),
            sqlparser::ast::Statement::CreateTrigger(create_trigger) => todo!(),
            sqlparser::ast::Statement::DropTrigger(drop_trigger) => todo!(),
            sqlparser::ast::Statement::CreateProcedure {
                or_alter,
                name,
                params,
                language,
                body,
            } => todo!(),
            sqlparser::ast::Statement::CreateMacro {
                or_replace,
                temporary,
                name,
                args,
                definition,
            } => todo!(),
            sqlparser::ast::Statement::CreateStage {
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
            sqlparser::ast::Statement::Assert { condition, message } => todo!(),
            sqlparser::ast::Statement::Grant(grant) => todo!(),
            sqlparser::ast::Statement::Deny(deny_statement) => todo!(),
            sqlparser::ast::Statement::Revoke(revoke) => todo!(),
            sqlparser::ast::Statement::Deallocate { name, prepare } => todo!(),
            sqlparser::ast::Statement::Execute {
                name,
                parameters,
                has_parentheses,
                immediate,
                into,
                using,
                output,
                default,
            } => todo!(),
            sqlparser::ast::Statement::Prepare {
                name,
                data_types,
                statement,
            } => todo!(),
            sqlparser::ast::Statement::Kill { modifier, id } => todo!(),
            sqlparser::ast::Statement::ExplainTable {
                describe_alias,
                hive_format,
                has_table_keyword,
                table_name,
            } => todo!(),
            sqlparser::ast::Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                query_plan,
                estimate,
                statement,
                format,
                options,
            } => todo!(),
            sqlparser::ast::Statement::Savepoint { name } => todo!(),
            sqlparser::ast::Statement::ReleaseSavepoint { name } => todo!(),
            sqlparser::ast::Statement::Merge(merge) => todo!(),
            sqlparser::ast::Statement::Cache {
                table_flag,
                table_name,
                has_as,
                options,
                query,
            } => todo!(),
            sqlparser::ast::Statement::UNCache {
                table_name,
                if_exists,
            } => todo!(),
            sqlparser::ast::Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
            } => todo!(),
            sqlparser::ast::Statement::CreateDomain(create_domain) => todo!(),
            sqlparser::ast::Statement::CreateType {
                name,
                representation,
            } => todo!(),
            sqlparser::ast::Statement::Pragma { name, value, is_eq } => todo!(),
            sqlparser::ast::Statement::Lock(lock) => todo!(),
            sqlparser::ast::Statement::LockTables { tables } => todo!(),
            sqlparser::ast::Statement::UnlockTables => todo!(),
            sqlparser::ast::Statement::Unload {
                query,
                query_text,
                to,
                auth,
                with,
                options,
            } => todo!(),
            sqlparser::ast::Statement::OptimizeTable {
                name,
                has_table_keyword,
                on_cluster,
                partition,
                include_final,
                deduplicate,
                predicate,
                zorder,
            } => todo!(),
            sqlparser::ast::Statement::LISTEN { channel } => todo!(),
            sqlparser::ast::Statement::UNLISTEN { channel } => todo!(),
            sqlparser::ast::Statement::NOTIFY { channel, payload } => todo!(),
            sqlparser::ast::Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            } => todo!(),
            sqlparser::ast::Statement::RenameTable(rename_tables) => todo!(),
            sqlparser::ast::Statement::List(file_staging_command) => todo!(),
            sqlparser::ast::Statement::Remove(file_staging_command) => todo!(),
            sqlparser::ast::Statement::RaisError {
                message,
                severity,
                state,
                arguments,
                options,
            } => todo!(),
            sqlparser::ast::Statement::Throw(throw_statement) => todo!(),
            sqlparser::ast::Statement::Print(print_statement) => todo!(),
            sqlparser::ast::Statement::WaitFor(wait_for_statement) => todo!(),
            sqlparser::ast::Statement::Return(return_statement) => todo!(),
            sqlparser::ast::Statement::ExportData(export_data) => todo!(),
            sqlparser::ast::Statement::CreateUser(create_user) => todo!(),
            sqlparser::ast::Statement::AlterUser(alter_user) => todo!(),
            sqlparser::ast::Statement::Vacuum(vacuum_statement) => todo!(),
            sqlparser::ast::Statement::Reset(reset_statement) => todo!(),
        }
    }
    //    pub fn create_table(self: Arc<Self>, table: Table) -> Result<(), SchemaError> {}
}
