use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use store::{db::DBFile, table::TableIdType};

use crate::{
    conn::connection::TableRef,
    optim::table_stats::ComputedTableStat,
    plan::{
        eval::{EvalExpr, dummy_arc_field},
        funcs::{FuncArgs, FuncTrait},
        logical::TableQuery,
    },
    source::ProjectableField,
    table::{self, Field, SqlTable},
};

pub(crate) fn pick_source_for_proj<F: DBFile + 'static>(
    page_size: usize,
    table: &TableRef<F>,
    fields: &[ProjectableField],
    stats: &Option<ComputedTableStat>,
) -> Option<(Arc<SqlTable>, usize)> {
    if let TableRef::Real(_schema, table) = table
        && let Some(stats) = &stats
    {
        let mut req_fields = vec![];
        for f in fields {
            req_fields.extend_from_slice(&extract_fields(&f.expr));
        }
        let default_table = TableIdType::default();
        let has_wildcard = req_fields.iter().any(|f| f.0 == default_table);
        req_fields.retain(|f| f.0 == table.db_table_id);
        let rows_per_page = page_size / stats.row_size;
        let scan_page_count = stats.table_stat.row_count / rows_per_page + 1;
        if let Some(indices) = &stats.indices {
            for (i, (index, stat)) in table.indices.iter().zip(indices.iter()).enumerate() {
                let index_pages = page_size / stat.nodes_per_page;
                if index_pages >= scan_page_count {
                    continue;
                }
                if (index.is_primary || index.is_unique) && has_wildcard && req_fields.len() == 1 {
                    // This is just select count(*) from X
                    if index_pages < scan_page_count {
                        return Some((table.clone(), i));
                    }
                } else {
                    let mut index_enough = true;
                    for (t, field) in &req_fields {
                        if *t != default_table {
                            assert!(!field.is_ephemeral);
                            index_enough = index.fields.iter().any(|f| f.name == field.name);
                        }
                        if !index_enough {
                            break;
                        }
                    }
                    if index_enough {
                        return Some((table.clone(), i));
                    }
                }
            }
        }
    }
    None
}

pub(crate) fn analyze_query<F: DBFile + 'static>(
    tables: &[TableQuery<F>],
    proj_fields: &[ProjectableField],
    wh_expr: &Option<EvalExpr>,
) {
    let mut table_fields = HashMap::new();
    for t in tables {
        if let TableRef::Real(_schema, _table) = &t.resolved {
            for j in &t.joins {
                let fields = extract_fields(&j.on_expr);
                for f in fields {
                    table_fields
                        .entry(f.0)
                        .and_modify(|v: &mut HashSet<Arc<Field>>| _ = v.insert(f.1.clone()))
                        .or_insert(HashSet::from([f.1.clone()]));
                }
            }
        }
    }
    for f in proj_fields {
        let fields = extract_fields(&f.expr);
        for f in fields {
            table_fields
                .entry(f.0)
                .and_modify(|v| _ = v.insert(f.1.clone()))
                .or_insert(HashSet::from([f.1.clone()]));
        }
    }
    if let Some(wh) = wh_expr {
        let fields = extract_fields(wh);
        for f in fields {
            table_fields
                .entry(f.0)
                .and_modify(|v| _ = v.insert(f.1.clone()))
                .or_insert(HashSet::from([f.1.clone()]));
        }
    }
}

fn extract_fields(expr: &EvalExpr) -> Vec<(TableIdType, Arc<Field>)> {
    let mut res = vec![];
    match expr {
        EvalExpr::None | EvalExpr::Literal(_) => {}
        EvalExpr::Value(_, field, table_id_type) => {
            if let Some(table_id) = table_id_type {
                res.push((*table_id, field.clone()));
            }
        }
        EvalExpr::Unary { op: _, field } => res.extend_from_slice(&extract_fields(field)),
        EvalExpr::Binary { lhs, op: _, rhs } => {
            res.extend_from_slice(&extract_fields(lhs));
            res.extend_from_slice(&extract_fields(rhs));
        }
        EvalExpr::Function(func_obj) => {
            for arg in func_obj.args() {
                match arg {
                    FuncArgs::Field(f) => res.extend_from_slice(&extract_fields(f)),
                    FuncArgs::Wildcard => res.push((TableIdType::default(), dummy_arc_field())),
                }
            }
        }
    }

    res
}
