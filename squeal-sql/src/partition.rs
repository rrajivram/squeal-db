//! Partitions: the pieces a table's rows are stored in.
//!
//! Every table is a list of partitions, each with its own storage; a table
//! that was not declared `PARTITION BY` has exactly one, holding all of its
//! rows. A partitioned table splits its rows by the value of one column:
//! RANGE (each partition holds the values below its bound and not below
//! the previous partition's) or LIST (each partition names its values, and
//! an optional DEFAULT one takes the rest).
//!
//! Everything a partition's storage is — which trees hold its rows and its
//! index entries — is behind [`PartitionStorage`]. Today there is one kind,
//! a B+tree per partition in this database; a partition stored elsewhere
//! (a file in another format) is a new variant there, and the compiler then
//! points at every place that reads or writes a partition.
//!
//! Indexes are local: each partition has its own tree per index, holding
//! only its own rows. So a PRIMARY KEY or UNIQUE constraint can only be
//! enforced when it includes the partition column — equal keys then always
//! land in the same partition (see `SqlTable::check_partition_keys`).
//!
//! These types are part of the catalog row (see table.rs's
//! `encode_catalog_row`): their enums are encoded by variant position, so
//! variants may only ever be appended.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use sql_parser::ddl::{PartitionDef, PartitionValues};
use store::{table::TableIdType, valueitem::ValueItem};

use crate::{datatype::DataType, error::SchemaError, table::expr_to_value_item};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionKind {
    Range,
    List,
}

/// How a partitioned table splits its rows: by the column with this field
/// id (see `Field::id` — it survives a rename).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partitioning {
    pub(crate) kind: PartitionKind,
    pub(crate) field_id: u32,
}

/// Which of the partition column's values a partition holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionBound {
    /// Every row: the one partition of a table that is not partitioned.
    All,
    /// RANGE: values below this one (and not below the previous
    /// partition's bound). NULL sorts lowest, so it is in the first.
    LessThan(ValueItem),
    /// RANGE: every value not below the previous partition's bound.
    MaxValue,
    /// LIST: exactly these values (NULL only if listed).
    In(Vec<ValueItem>),
    /// LIST: every value no other partition lists.
    Default,
}

/// Where a partition's rows and index entries are.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionStorage {
    /// B+trees in this database: one for the rows, and one per index of
    /// the table, in `SqlTable::indices` order.
    Native {
        rows: TableIdType,
        indices: Vec<TableIdType>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Partition {
    /// Never reused within a table: what its trees' names are built from.
    pub(crate) id: u32,
    /// Empty for the one partition of a table that is not partitioned.
    pub(crate) name: String,
    pub(crate) bound: PartitionBound,
    pub(crate) storage: PartitionStorage,
}

impl Partition {
    /// The one partition of a table that is not partitioned, with `indices`
    /// index trees still to be created.
    pub(crate) fn whole(indices: usize) -> Self {
        Self::new(0, String::new(), PartitionBound::All, indices)
    }

    /// A partition whose trees are not created yet (see
    /// Schema::create_partition_trees).
    pub(crate) fn new(id: u32, name: String, bound: PartitionBound, indices: usize) -> Self {
        Self {
            id,
            name,
            bound,
            storage: PartitionStorage::Native {
                rows: TableIdType::none(),
                indices: vec![TableIdType::none(); indices],
            },
        }
    }

    /// The tree holding this partition's rows.
    pub(crate) fn rows(&self) -> TableIdType {
        match &self.storage {
            PartitionStorage::Native { rows, .. } => *rows,
        }
    }

    /// The tree holding this partition's entries of the table's index
    /// `index` (a position in `SqlTable::indices`).
    pub(crate) fn index(&self, index: usize) -> TableIdType {
        match &self.storage {
            PartitionStorage::Native { indices, .. } => indices[index],
        }
    }

    pub(crate) fn set_rows(&mut self, id: TableIdType) {
        match &mut self.storage {
            PartitionStorage::Native { rows, .. } => *rows = id,
        }
    }

    /// Sets index `index`'s tree, which may be one past the last (a new
    /// index).
    pub(crate) fn set_index(&mut self, index: usize, id: TableIdType) {
        match &mut self.storage {
            PartitionStorage::Native { indices, .. } => {
                if index == indices.len() {
                    indices.push(id);
                } else {
                    indices[index] = id;
                }
            }
        }
    }

    /// What EXPLAIN and error messages call it.
    pub(crate) fn describe(&self) -> String {
        let show = |v: &ValueItem| match v {
            ValueItem::Str((s, _)) => format!("'{s}'"),
            other => other.to_string(),
        };
        match &self.bound {
            PartitionBound::All => String::new(),
            PartitionBound::LessThan(v) => format!("{} (< {})", self.name, show(v)),
            PartitionBound::MaxValue => format!("{} (< MAXVALUE)", self.name),
            PartitionBound::In(values) => format!(
                "{} (in {})",
                self.name,
                values.iter().map(show).collect::<Vec<_>>().join(", ")
            ),
            PartitionBound::Default => format!("{} (default)", self.name),
        }
    }
}

impl PartitionBound {
    /// A partition's bound as written in SQL, as values of the partition
    /// column's type.
    pub(crate) fn from_sql(
        def: &PartitionDef,
        kind: PartitionKind,
        datatype: DataType,
    ) -> Result<Self, SchemaError> {
        let name = &def.name.value;
        let wrong = |wants: &str| {
            Err(SchemaError::UserError(format!(
                "partition {name:?}: a {kind:?} partition is declared with {wants}"
            )))
        };
        match (&def.values, kind) {
            (
                PartitionValues::LessThanMax(..) | PartitionValues::LessThanMaxParen(..),
                PartitionKind::Range,
            ) => Ok(PartitionBound::MaxValue),
            (PartitionValues::LessThan(_, _, _, _, expr, _), PartitionKind::Range) => {
                match expr_to_value_item(expr, datatype)? {
                    ValueItem::Null => Err(SchemaError::UserError(format!(
                        "partition {name:?}: VALUES LESS THAN cannot be NULL"
                    ))),
                    value => Ok(PartitionBound::LessThan(value)),
                }
            }
            (PartitionValues::In(_, _, _, exprs, _), PartitionKind::List) => {
                Ok(PartitionBound::In(
                    exprs
                        .items()
                        .map(|e| expr_to_value_item(e, datatype))
                        .collect::<Result<_, _>>()?,
                ))
            }
            (PartitionValues::Default(_), PartitionKind::List) => Ok(PartitionBound::Default),
            (_, PartitionKind::Range) => wrong("VALUES LESS THAN"),
            (_, PartitionKind::List) => wrong("VALUES IN or DEFAULT"),
        }
    }
}

impl Partitioning {
    /// Checks a partitioned table's whole list of partitions: names unique,
    /// RANGE bounds strictly increasing with MAXVALUE only last, LIST
    /// values each in one partition with at most one DEFAULT.
    pub(crate) fn check(&self, partitions: &[Partition]) -> Result<(), SchemaError> {
        let err = |msg: String| Err(SchemaError::UserError(msg));
        if partitions.is_empty() {
            return err("a partitioned table needs at least one partition".into());
        }
        for (i, p) in partitions.iter().enumerate() {
            if partitions[..i]
                .iter()
                .any(|o| o.name.eq_ignore_ascii_case(&p.name))
            {
                return err(format!("duplicate partition name: {}", p.name));
            }
        }
        match self.kind {
            PartitionKind::Range => {
                for pair in partitions.windows(2) {
                    let increasing = match (&pair[0].bound, &pair[1].bound) {
                        (PartitionBound::LessThan(a), PartitionBound::LessThan(b)) => {
                            a.cmp(b) == Ordering::Less
                        }
                        (PartitionBound::LessThan(_), PartitionBound::MaxValue) => true,
                        _ => false,
                    };
                    if !increasing {
                        return err(format!(
                            "partition {:?}: each RANGE partition's bound must be above the one \
                             before it, and MAXVALUE can only be the last",
                            pair[1].name
                        ));
                    }
                }
            }
            PartitionKind::List => {
                let mut seen: Vec<&ValueItem> = vec![];
                let mut default = false;
                for p in partitions {
                    match &p.bound {
                        PartitionBound::In(values) if values.is_empty() => {
                            return err(format!("partition {:?} lists no values", p.name));
                        }
                        PartitionBound::In(values) => {
                            for v in values {
                                if seen.iter().any(|s| s.cmp(&v) == Ordering::Equal) {
                                    return err(format!(
                                        "partition {:?}: value {v} is in more than one partition",
                                        p.name
                                    ));
                                }
                                seen.push(v);
                            }
                        }
                        PartitionBound::Default if default => {
                            return err(format!(
                                "partition {:?}: a table can have only one DEFAULT partition",
                                p.name
                            ));
                        }
                        PartitionBound::Default => default = true,
                        _ => {
                            return err(format!("partition {:?} is not a LIST partition", p.name));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The partition (a position in `partitions`) a row whose partition
    /// column holds `value` belongs in; None if no partition takes it.
    pub(crate) fn route(&self, partitions: &[Partition], value: &ValueItem) -> Option<usize> {
        match self.kind {
            // Bounds increase, so the first one above the value is its
            // partition. NULL is below every bound: the first partition.
            PartitionKind::Range => partitions.iter().position(|p| match &p.bound {
                PartitionBound::LessThan(bound) => value.cmp(bound) == Ordering::Less,
                PartitionBound::MaxValue => true,
                _ => false,
            }),
            PartitionKind::List => partitions
                .iter()
                .position(|p| match &p.bound {
                    PartitionBound::In(values) => {
                        values.iter().any(|v| v.cmp(value) == Ordering::Equal)
                    }
                    _ => false,
                })
                .or_else(|| {
                    partitions
                        .iter()
                        .position(|p| p.bound == PartitionBound::Default)
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(name: &str, bound: PartitionBound) -> Partition {
        Partition::new(0, name.into(), bound, 0)
    }

    fn int(n: i64) -> ValueItem {
        ValueItem::Integer(n)
    }

    fn range() -> (Partitioning, Vec<Partition>) {
        (
            Partitioning {
                kind: PartitionKind::Range,
                field_id: 0,
            },
            vec![
                part("low", PartitionBound::LessThan(int(10))),
                part("mid", PartitionBound::LessThan(int(20))),
            ],
        )
    }

    #[test]
    fn test_range_routes_a_value_to_the_first_bound_above_it() {
        let (by, mut parts) = range();
        assert_eq!(by.route(&parts, &int(-5)), Some(0));
        assert_eq!(by.route(&parts, &int(9)), Some(0));
        // A bound is exclusive: 10 is the next partition's.
        assert_eq!(by.route(&parts, &int(10)), Some(1));
        assert_eq!(by.route(&parts, &int(19)), Some(1));
        assert_eq!(by.route(&parts, &int(20)), None, "above every bound");
        assert_eq!(
            by.route(&parts, &ValueItem::Null),
            Some(0),
            "NULL sorts lowest"
        );
        parts.push(part("rest", PartitionBound::MaxValue));
        assert_eq!(by.route(&parts, &int(20)), Some(2));
        assert_eq!(by.route(&parts, &int(i64::MAX)), Some(2));
    }

    #[test]
    fn test_list_routes_a_listed_value_and_everything_else_to_default() {
        let by = Partitioning {
            kind: PartitionKind::List,
            field_id: 0,
        };
        let mut parts = vec![
            part("a", PartitionBound::In(vec![int(1), int(2)])),
            part("b", PartitionBound::In(vec![int(3), ValueItem::Null])),
        ];
        assert_eq!(by.route(&parts, &int(2)), Some(0));
        assert_eq!(by.route(&parts, &int(3)), Some(1));
        assert_eq!(
            by.route(&parts, &ValueItem::Null),
            Some(1),
            "NULL is listed"
        );
        assert_eq!(by.route(&parts, &int(4)), None);
        parts.insert(0, part("other", PartitionBound::Default));
        assert_eq!(by.route(&parts, &int(4)), Some(0));
        assert_eq!(
            by.route(&parts, &int(2)),
            Some(1),
            "a listed value beats DEFAULT"
        );
    }

    #[test]
    fn test_check_rejects_bounds_that_do_not_split_the_values() {
        let (by, parts) = range();
        assert!(by.check(&parts).is_ok());
        let bad = |parts: Vec<Partition>, by: &Partitioning| by.check(&parts).is_err();
        assert!(bad(vec![], &by));
        assert!(bad(
            vec![
                part("a", PartitionBound::LessThan(int(10))),
                part("b", PartitionBound::LessThan(int(10))),
            ],
            &by
        ));
        assert!(bad(
            vec![
                part("a", PartitionBound::MaxValue),
                part("b", PartitionBound::LessThan(int(10))),
            ],
            &by
        ));
        assert!(bad(
            vec![
                part("a", PartitionBound::LessThan(int(1))),
                part("A", PartitionBound::LessThan(int(2))),
            ],
            &by
        ));
        let list = Partitioning {
            kind: PartitionKind::List,
            field_id: 0,
        };
        assert!(bad(
            vec![
                part("a", PartitionBound::In(vec![int(1)])),
                part("b", PartitionBound::In(vec![int(1)])),
            ],
            &list
        ));
        assert!(bad(
            vec![
                part("a", PartitionBound::Default),
                part("b", PartitionBound::Default),
            ],
            &list
        ));
        assert!(bad(vec![part("a", PartitionBound::In(vec![]))], &list));
        assert!(bad(
            vec![part("a", PartitionBound::LessThan(int(1)))],
            &list
        ));
    }
}
