use std::{
    collections::HashMap,
    f64,
    hash::Hash,
    sync::{Arc, atomic::AtomicUsize},
    thread::{self, JoinHandle},
};

use bloomfilter::Bloom;
use crossbeam::channel::{Receiver, RecvError, Sender, TrySendError, bounded};
use crossbeam_skiplist::SkipMap;
use parking_lot::{RwLock, RwLockReadGuard};
use rand::random;
use store::{
    db::DBFile,
    table::TableIdType,
    valueitem::{IndexKey, ValueItem},
};

use crate::{error::SchemaError, schema_ops::schema::Schema};

struct SchemaStats<F: DBFile + 'static> {
    schema: Arc<Schema<F>>,
    tables: Arc<RwLock<HashMap<TableIdType, TableStatStored>>>,
    sampling_rate: f64,
    handle: JoinHandle<Result<(), SchemaError>>,
    tx: Sender<StatMsg>,
}

#[derive(Debug)]
pub(crate) struct TableStatStored {
    id: TableIdType,
    row_count: usize,
    col_stats: RwLock<HashMap<usize, ColumnStatStored>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnStatStored {
    id: u32,
    bloom: Bloom<ValueItem>,
    unique: usize,
    nulls: usize,
    min: Option<ValueItem>,
    max: Option<ValueItem>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableStat {
    pub(crate) id: TableIdType,
    pub(crate) row_count: usize,
    col_stats: HashMap<usize, ColumnStat>,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnStat {
    pub(crate) id: u32,
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
        let sampling_rate = sampling_rate.clamp(0., 0.99);
        let mut tables = HashMap::new();
        for t in schema.list_tables() {
            let table = schema.get_table(&t).unwrap();
            // we won't collect for single primary or unique keys
            let unique_fields = table
                .indices
                .iter()
                .enumerate()
                .filter_map(|(i, ind)| {
                    if (ind.is_primary || ind.is_unique) && ind.fields.len() == 1 {
                        None
                    } else {
                        Some(i)
                    }
                })
                .collect::<Vec<_>>();
            let mut col_stats = HashMap::new();
            for (i, f) in table.fields().iter().enumerate() {
                if !unique_fields.contains(&i) {
                    let bloom = Bloom::new_for_fp_rate(4096, 0.2)
                        .map_err(|s| SchemaError::UnknownError(s.to_string()))?;
                    let cstat = ColumnStatStored {
                        id: f.id,
                        bloom,
                        unique: 0,
                        nulls: 0,
                        min: None,
                        max: None,
                    };
                    col_stats.insert(i, cstat);
                }
            }
            let tstat = TableStatStored {
                id: table.db_table_id,
                row_count: 0,
                col_stats: RwLock::new(col_stats),
            };
            tables.insert(table.db_table_id, tstat);
        }
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

    pub(crate) fn log_stat(&self, table: TableIdType, record: IndexKey) {
        let r = self.tx.try_send(StatMsg::InsertLogStat((table, record)));
        match r {
            Err(TrySendError::Full(_)) => {}
            Err(_) => {}
            _ => {}
        }
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
                let val: f64 = random();
                if val >= rate {
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
    let table = table_guard.get_mut(&table).unwrap();
    table.row_count += 1;
    for (i, f) in table.col_stats.write().iter_mut() {
        let v = &data[*i];
        if let Some(min) = f.min.as_ref()
            && min < v
        {
            f.min = Some(v.clone());
        } else {
            f.min = Some(v.clone());
        }
        if let Some(max) = f.max.as_ref()
            && max < v
        {
            f.max = Some(v.clone());
        } else {
            f.max = Some(v.clone());
        }
        if matches!(v, ValueItem::Null) {
            f.nulls += 1;
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
