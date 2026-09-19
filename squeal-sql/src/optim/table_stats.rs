use std::{
    collections::HashMap,
    f64,
    sync::Arc,
    thread::{self, JoinHandle},
};

use bloomfilter::Bloom;
use crossbeam::channel::{Receiver, RecvError, Sender, TrySendError, bounded};
use parking_lot::RwLock;
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use store::{
    cursor::Cursor,
    db::DBFile,
    error::StoreError,
    table::TableIdType,
    tuple::{DBIdType, Tuple},
    valueitem::{IndexKey, ValueItem},
};

use crate::{error::SchemaError, schema_ops::schema::Schema, table::SqlTable};

pub(crate) struct SchemaStats<F: DBFile + 'static> {
    schema: Arc<Schema<F>>,
    tables: Arc<RwLock<HashMap<TableIdType, TableStatStored>>>,
    sampling_rate: f64,
    handle: JoinHandle<Result<(), SchemaError>>,
    tx: Sender<StatMsg>,
}

#[derive(Debug)]
pub(crate) struct TableStatStored {
    id: TableIdType,
    name: String,
    row_count: usize,
    col_stats: RwLock<HashMap<usize, ColumnStatStored>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnStatStored {
    id: u32,
    name: String,
    bloom: Bloom<ValueItem>,
    unique: usize,
    nulls: usize,
    min: Option<ValueItem>,
    max: Option<ValueItem>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableStat {
    pub(crate) id: TableIdType,
    pub(crate) name: String,
    pub(crate) row_count: usize,
    // Keyed by field index (matching SqlTable::fields()'s own order) —
    // pub(crate), not private, so a caller outside this module (e.g.
    // Schema, for the `!show table stats` CLI command) can render it
    // without a bespoke accessor per field.
    pub(crate) col_stats: HashMap<usize, ColumnStat>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnStat {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) unique: usize,
    pub(crate) null: usize,
    pub(crate) min: ValueItem,
    pub(crate) max: ValueItem,
}

enum StatMsg {
    Shutdown,
    InsertLogStat((TableIdType, IndexKey)),
}

impl<F: DBFile + 'static> SchemaStats<F> {
    pub(crate) fn new(schema: Arc<Schema<F>>, sampling_rate: f64) -> Result<Self, SchemaError> {
        let mut tables = HashMap::new();
        for t in schema.list_tables() {
            let table = schema.get_table(&t).unwrap();
            let tstat = Self::table_data(&table)?;
            // we won't collect for single primary or unique keys
            tables.insert(table.db_table_id, tstat);
        }
        Self::spawn(schema, tables, sampling_rate)
    }

    // Backward-compatible reload (Schema::load): `stats_table_id` is a
    // store table with one row per TableIdType, written by `persist`
    // below — see that method's own comment for the wire format. A
    // schema opened for the first time since this feature existed has no
    // rows here yet (or the table itself was just created for it — see
    // Schema::load), which this treats identically to `new`: every
    // table in the schema's own catalog with no persisted row starts
    // fresh via `table_data`, same as a brand-new schema.
    pub(crate) fn load(
        schema: Arc<Schema<F>>,
        sampling_rate: f64,
        stats_table_id: TableIdType,
    ) -> Result<Self, SchemaError> {
        let mut tables: HashMap<TableIdType, TableStatStored> = HashMap::new();
        let mut cursor = schema.db.table_scan(stats_table_id)?;
        while let Some(tuple) = cursor.next()? {
            let persisted: PersistedTableStat = from_bytes(tuple.data())?;
            let stat = TableStatStored::from_persisted(persisted)?;
            tables.insert(stat.id, stat);
        }
        for t in schema.list_tables() {
            let table = schema.get_table(&t).unwrap();
            if !tables.contains_key(&table.db_table_id) {
                tables.insert(table.db_table_id, Self::table_data(&table)?);
            }
        }
        Self::spawn(schema, tables, sampling_rate)
    }

    fn spawn(
        schema: Arc<Schema<F>>,
        tables: HashMap<TableIdType, TableStatStored>,
        sampling_rate: f64,
    ) -> Result<Self, SchemaError> {
        let sampling_rate = sampling_rate.clamp(0., 0.99);
        let (tx, rx) = bounded(1);
        let tables = Arc::new(RwLock::new(tables));
        let t_clone = tables.clone();
        let handle = thread::spawn(move || stat_collector(rx, t_clone, sampling_rate));

        Ok(Self {
            tables,
            sampling_rate,
            schema,
            tx,
            handle,
        })
    }

    // Writes every table's current stats as one row each into
    // `stats_table_id`, keyed by DBIdType::Int(table_id) — the
    // persistence counterpart to `load` above. Called at Schema close
    // (see Schema::persist_and_shutdown_stats), alongside flush_metadata,
    // before `shutdown` stops the collector thread. An upsert (try
    // update, insert on KeyNotFound) since a table's row may already
    // exist from an earlier persist (a prior close/reopen cycle) or may
    // never have been written before.
    pub(crate) fn persist(&self, stats_table_id: TableIdType) -> Result<(), SchemaError> {
        let txn = self.schema.db.begin()?;
        for stat in self.tables.read().values() {
            let persisted = stat.to_persisted();
            let key = DBIdType::Int(persisted.id.as_u64());
            let bytes = to_allocvec(&persisted)?;
            let tuple = Tuple::new_with(key, &bytes, Some(txn.id()), None);
            match self.schema.db.update(stats_table_id, tuple.clone(), &txn) {
                Ok(()) => {}
                Err(StoreError::KeyNotFound(_)) => {
                    self.schema.db.insert(stats_table_id, tuple, &txn)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        self.schema.db.commit(txn)?;
        Ok(())
    }

    pub(crate) fn add_table(&self, table: Arc<SqlTable>) -> Result<(), SchemaError> {
        let tstat = Self::table_data(&table)?;
        self.tables.write().insert(table.db_table_id, tstat);
        Ok(())
    }

    fn table_data(table: &Arc<SqlTable>) -> Result<TableStatStored, SchemaError> {
        // Field *names* a lone PRIMARY KEY/UNIQUE index already covers —
        // no bloom/min/max tracking needed, since that column's values
        // are already known-unique by definition. Was comparing index
        // *positions* (within table.indices) against field *positions*
        // (within table.fields()) — two unrelated numberings that just
        // happened to compile — so this never actually excluded the
        // right (or in general, any correct) column; never noticed
        // before because nothing fed real rows through this path.
        let unique_fields: std::collections::HashSet<&str> = table
            .indices
            .iter()
            .filter(|ind| (ind.is_primary || ind.is_unique) && ind.fields.len() == 1)
            .map(|ind| ind.fields[0].name.as_str())
            .collect();
        let mut col_stats = HashMap::new();
        for (i, f) in table.fields().iter().enumerate() {
            if !unique_fields.contains(f.name.as_str()) {
                let bloom = Bloom::new_for_fp_rate(4096, 0.2)
                    .map_err(|s| SchemaError::UnknownError(s.to_string()))?;
                let cstat = ColumnStatStored {
                    id: f.id,
                    bloom,
                    unique: 0,
                    nulls: 0,
                    min: None,
                    max: None,
                    name: f.name.clone(),
                };
                col_stats.insert(i, cstat);
            }
        }
        let tstat = TableStatStored {
            id: table.db_table_id,
            name: table.name.clone(),
            row_count: 0,
            col_stats: RwLock::new(col_stats),
        };
        Ok(tstat)
    }

    // Best-effort, sampled: a full channel (capacity 1 — see `spawn`)
    // silently drops this update rather than blocking the caller (an
    // ordinary row insert) on the collector thread keeping up. Fine for
    // incremental, approximate stats fed by live traffic; wrong for
    // ANALYZE, which needs every row counted exactly once — see
    // `record_row_sync` for that path instead.
    pub(crate) fn log_stat(&self, table: TableIdType, record: IndexKey) {
        let r = self.tx.try_send(StatMsg::InsertLogStat((table, record)));
        match r {
            Err(TrySendError::Full(_)) => {}
            Err(_) => {}
            _ => {}
        }
    }

    // ANALYZE's own path (Schema::analyze_table): applies `record`
    // directly, synchronously, on the caller's own thread — never
    // dropped, unlike `log_stat`. A tight table-scan loop calling
    // `log_stat` once per row would have the collector thread's
    // capacity-1 channel silently discard nearly everything (the loop
    // can enqueue far faster than one background thread can drain), which
    // is fine for sampled live traffic but defeats the entire point of an
    // exhaustive re-analysis.
    pub(crate) fn record_row_sync(&self, table: TableIdType, record: IndexKey) {
        update_table_stats(&self.tables, table, record);
    }

    pub(crate) fn shutdown(self) {
        let _ = self.tx.send(StatMsg::Shutdown);
        let _ = self.handle.join();
    }

    pub(crate) fn drop_table_stats(&self, table: TableIdType) {
        self.tables.write().remove(&table);
    }

    pub(crate) fn get_table_stats(&self, table: TableIdType) -> Option<TableStat> {
        let multiplier = 1.min((1.0 / self.sampling_rate) as usize);
        self.tables
            .read()
            .get(&table)
            .map(|t| t.with_multiplier(multiplier))
    }
}

fn stat_collector(
    rx: Receiver<StatMsg>,
    stat: Arc<RwLock<HashMap<TableIdType, TableStatStored>>>,
    rate: f64,
) -> Result<(), SchemaError> {
    loop {
        let msg = rx.recv()?;
        match msg {
            StatMsg::Shutdown => break,
            StatMsg::InsertLogStat((table, data)) => {
                let val = data.hash() as f64 / u64::MAX as f64;
                if val < rate {
                    update_table_stats(&stat, table, data);
                }
            }
        }
    }
    Ok(())
}

fn update_table_stats(
    stat: &Arc<RwLock<HashMap<TableIdType, TableStatStored>>>,
    table: TableIdType,
    data: IndexKey,
) {
    let mut table_guard = stat.write();
    // A table with no entry yet (e.g. a row logged for it before
    // Schema::create_table's own add_table call registered it, or any
    // other ordering this collector can't see) is silently skipped
    // rather than panicking this background thread — SchemaStats is
    // already a best-effort, approximate mechanism (see log_stat's own
    // doc comment); losing one row's contribution to stats that don't
    // exist yet is far cheaper than losing the whole collector.
    let Some(table) = table_guard.get_mut(&table) else {
        return;
    };
    table.row_count += 1;
    for (i, f) in table.col_stats.write().iter_mut() {
        let v = &data[*i];
        // Null is excluded from min/max/unique entirely, counted only
        // here — matching SQL's own MIN/MAX/COUNT(DISTINCT), which
        // likewise ignore NULL rather than letting it participate.
        // Load-bearing for min/max specifically: ValueItem::Ord ranks
        // Null lowest of every variant (see its own type_rank), so
        // without this a plain `v < min` would let one null row make
        // min "Null" forever — the opposite of a real bound.
        if matches!(v, ValueItem::Null) {
            f.nulls += 1;
            continue;
        }
        // Both branches used to unconditionally overwrite (the `if`
        // guard's condition and its consequence had gotten mismatched),
        // so min/max ended up tracking "the last value seen" instead of
        // a running bound — never noticed before because nothing called
        // log_stat/record_row_sync with live data until now.
        match f.min.as_ref() {
            Some(min) if v < min => f.min = Some(v.clone()),
            Some(_) => {}
            None => f.min = Some(v.clone()),
        }
        match f.max.as_ref() {
            Some(max) if max < v => f.max = Some(v.clone()),
            Some(_) => {}
            None => f.max = Some(v.clone()),
        }
        if f.unique < 4096 {
            if !f.bloom.check(v) {
                f.bloom.set(v);
                f.unique += 1;
            }
        } else {
            f.unique += 1;
        }
    }
}

impl From<RecvError> for SchemaError {
    fn from(value: RecvError) -> Self {
        SchemaError::UnknownError(value.to_string())
    }
}

impl ColumnStatStored {
    fn with_mutiplier(&self, mul: usize) -> ColumnStat {
        let min = if let Some(min) = self.min.as_ref() {
            min.clone()
        } else {
            ValueItem::Null
        };
        let max = if let Some(max) = self.max.as_ref() {
            max.clone()
        } else {
            ValueItem::Null
        };
        ColumnStat {
            id: self.id,
            name: self.name.clone(),
            unique: self.unique * mul,
            max,
            min,
            null: self.nulls * mul,
        }
    }
}

impl TableStatStored {
    fn with_multiplier(&self, mul: usize) -> TableStat {
        TableStat {
            id: self.id,
            name: self.name.clone(),
            row_count: self.row_count * mul,
            col_stats: self
                .col_stats
                .read()
                .iter()
                .map(|(i, c)| (*i, c.with_mutiplier(mul)))
                .collect(),
        }
    }
}

// Wire format for SchemaStats::persist/load — one row per table, keyed by
// DBIdType::Int(table_id). Mirrors TableStatStored/ColumnStatStored field
// for field, except the bloom filter: `Bloom<ValueItem>` isn't a plain
// postcard-serializable type, but bloomfilter itself already provides a
// byte-exact to_bytes()/from_bytes() round trip (independent of the
// crate's own "serde" feature, which this workspace doesn't enable), so
// uniqueness estimation resumes exactly where it left off across a
// close/reopen instead of restarting empty.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedColumnStat {
    id: u32,
    name: String,
    bloom_bytes: Vec<u8>,
    unique: usize,
    nulls: usize,
    min: Option<ValueItem>,
    max: Option<ValueItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedTableStat {
    id: TableIdType,
    name: String,
    row_count: usize,
    col_stats: HashMap<usize, PersistedColumnStat>,
}

impl ColumnStatStored {
    fn to_persisted(&self) -> PersistedColumnStat {
        PersistedColumnStat {
            id: self.id,
            name: self.name.clone(),
            bloom_bytes: self.bloom.to_bytes(),
            unique: self.unique,
            nulls: self.nulls,
            min: self.min.clone(),
            max: self.max.clone(),
        }
    }

    fn from_persisted(p: PersistedColumnStat) -> Result<Self, SchemaError> {
        Ok(Self {
            id: p.id,
            name: p.name,
            bloom: Bloom::from_bytes(p.bloom_bytes)
                .map_err(|e| SchemaError::UnknownError(e.to_string()))?,
            unique: p.unique,
            nulls: p.nulls,
            min: p.min,
            max: p.max,
        })
    }
}

impl TableStatStored {
    fn to_persisted(&self) -> PersistedTableStat {
        PersistedTableStat {
            id: self.id,
            name: self.name.clone(),
            row_count: self.row_count,
            col_stats: self
                .col_stats
                .read()
                .iter()
                .map(|(i, c)| (*i, c.to_persisted()))
                .collect(),
        }
    }

    fn from_persisted(p: PersistedTableStat) -> Result<Self, SchemaError> {
        let mut col_stats = HashMap::new();
        for (i, c) in p.col_stats {
            col_stats.insert(i, ColumnStatStored::from_persisted(c)?);
        }
        Ok(Self {
            id: p.id,
            name: p.name,
            row_count: p.row_count,
            col_stats: RwLock::new(col_stats),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_column(seed: &[u64]) -> ColumnStatStored {
        let mut bloom = Bloom::new_for_fp_rate(4096, 0.2).unwrap();
        let mut unique = 0;
        for v in seed {
            let item = ValueItem::Integer(*v as i64);
            if !bloom.check(&item) {
                bloom.set(&item);
                unique += 1;
            }
        }
        ColumnStatStored {
            id: 7,
            name: "amount".into(),
            bloom,
            unique,
            nulls: 2,
            min: Some(ValueItem::Integer(1)),
            max: Some(ValueItem::Integer(100)),
        }
    }

    // The wire format's whole reason to exist: a close/reopen cycle must
    // resume with the *exact* same bloom-filter state (not just the
    // derived counters), since the bloom filter itself is what future
    // uniqueness estimation checks against, not something recomputed
    // from `unique` alone. Bytes in, bytes out — to_persisted/
    // from_persisted must not lose or reorder anything to_bytes/
    // from_bytes themselves wouldn't.
    #[test]
    fn test_column_stat_round_trips_through_persisted_form_including_bloom_state() {
        let seeded = [1u64, 2, 3, 2, 3, 3, 100];
        let original = sample_column(&seeded);
        let persisted = original.to_persisted();
        let restored = ColumnStatStored::from_persisted(persisted).unwrap();

        assert_eq!(restored.id, original.id);
        assert_eq!(restored.name, original.name);
        assert_eq!(restored.unique, original.unique);
        assert_eq!(restored.nulls, original.nulls);
        assert_eq!(restored.min, original.min);
        assert_eq!(restored.max, original.max);
        // The restored bloom filter must agree with the original on
        // every value already seen (no false negatives reintroduced by
        // the round trip) — checked directly, not just via the `unique`
        // counter, since a filter that silently reset to empty would
        // still report a plausible-looking `unique` count.
        for v in seeded {
            assert!(
                restored.bloom.check(&ValueItem::Integer(v as i64)),
                "value {v} was seen before persisting and must still be flagged as seen"
            );
        }
    }

    #[test]
    fn test_table_stat_round_trips_through_persisted_form() {
        let mut col_stats = HashMap::new();
        col_stats.insert(0, sample_column(&[10, 20, 30]));
        col_stats.insert(2, sample_column(&[1, 1, 1]));
        let original = TableStatStored {
            id: TableIdType::from(42u64),
            name: "orders".into(),
            row_count: 12345,
            col_stats: RwLock::new(col_stats),
        };

        let persisted = original.to_persisted();
        let bytes = to_allocvec(&persisted).unwrap();
        let decoded: PersistedTableStat = from_bytes(&bytes).unwrap();
        let restored = TableStatStored::from_persisted(decoded).unwrap();

        assert_eq!(restored.id, original.id);
        assert_eq!(restored.name, original.name);
        assert_eq!(restored.row_count, original.row_count);
        assert_eq!(
            restored.col_stats.read().len(),
            original.col_stats.read().len()
        );
        for (i, orig_col) in original.col_stats.read().iter() {
            let restored_col = restored.col_stats.read();
            let restored_col = restored_col.get(i).unwrap_or_else(|| {
                panic!("column index {i} present before persisting is missing after reload")
            });
            assert_eq!(restored_col.unique, orig_col.unique);
            assert_eq!(restored_col.min, orig_col.min);
            assert_eq!(restored_col.max, orig_col.max);
        }
    }
}
