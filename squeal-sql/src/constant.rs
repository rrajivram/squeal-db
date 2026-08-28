// Database-level: the one store table listing every schema in the
// database (row per schema name).
pub(crate) const SYSTEM_SCHEMAS_TABLE: &str = "sql_system.schemas";
// Schema-level: suffix for each schema's own store table listing its
// tables/indices — the actual store table name is
// `format!("{schema_name}.{SYSTEM_TABLES_SUFFIX}")`.
pub(crate) const SYSTEM_TABLES_SUFFIX: &str = "sql_system.tables";
// Auto-created by Database::create, and best-effort auto-loaded by
// Database::open.
pub(crate) const DEFAULT_SCHEMA_NAME: &str = "default";

pub(crate) const MAX_TABLE_NAME_LEN: usize = 128;
pub(crate) const DEFAULT_VAR_SIZE: usize = 32;

// Default per-query memory budget (see plan::memory::QueryMemory) for a
// LogicalPlan that doesn't request an explicit one — deliberately
// separate from PageBuffer's own max_entries (a shared, whole-database
// page cache), this bounds what a single query's own blocking
// operators (once they exist — hash join, sort, GROUP BY, ...) may
// buffer in-process. 64 MiB is an arbitrary, generous starting point;
// nothing today exercises it (no operator reserves against it yet), so
// there's no real workload to size it against.
pub(crate) const DEFAULT_QUERY_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
