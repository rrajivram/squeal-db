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

#[cfg(test)]
mod tests;

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
