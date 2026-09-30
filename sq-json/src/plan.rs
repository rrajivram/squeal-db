//! Choosing how to find a query's documents: the `_id` tree, a secondary
//! index, or a scan of the whole collection.
//!
//! A key can serve a query when the filter's required conditions (the
//! top-level AND) fix its leading fields: equalities / `$in` give points,
//! then at most one comparison gives a range — the same shape as a SQL
//! seek (see squeal-sql's plan::sarg). Whatever the plan, every document
//! found is still checked against the whole filter, so an index only ever
//! narrows what is read.
//!
//! On a multikey index (some document has an array in an indexed field),
//! each document has one entry per element, and different elements may
//! satisfy different conditions — `{a: {$gt: 10, $lt: 5}}` matches `a: [1,
//! 20]` — so two conditions on such a field are never intersected: one of
//! them alone bounds the seek, as MongoDB does.

use std::cmp::Ordering;
use std::ops::Bound;

use store::cursor::KeyRange;
use store::valueitem::ValueItem;

use crate::filter::{CmpOp, Cond, Filter, cmp_value};
use crate::keys::{bracket, encode, orders_like_mongo};
use crate::value::{Value, compare, type_order, values_equal};

/// How many key ranges one seek may expand into (combinations of `$in`
/// lists); past that, later key fields aren't used.
const MAX_RANGES: usize = 256;

/// The values one key field is limited to.
#[derive(Debug, Clone)]
enum FieldSet {
    Points(Vec<Value>),
    /// Values of one type bracket (that of `bracket`), between the bounds.
    Range {
        bracket: Value,
        lower: Bound<Value>,
        upper: Bound<Value>,
    },
}

/// A way in: key ranges over one tree, and how many leading key fields
/// they fix to points (for choosing between candidates).
#[derive(Debug, Clone)]
pub(crate) struct Seek {
    pub ranges: Vec<KeyRange>,
    pub points: usize,
    pub ranged: bool,
}

/// The key ranges `filter` allows over a key made of `paths`, or None when
/// it doesn't limit the key's first field.
pub(crate) fn seek_for(filter: &Filter, paths: &[&str], multikey: bool) -> Option<Seek> {
    let required = filter.required();
    let mut prefixes: Vec<Vec<ValueItem>> = vec![vec![]];
    let mut points = 0;
    for path in paths {
        let conds: Vec<&Cond> = required
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, c)| *c)
            .collect();
        let Some(set) = field_set(&conds, multikey) else {
            break;
        };
        match set {
            FieldSet::Points(values) => {
                if prefixes.len() * values.len().max(1) > MAX_RANGES {
                    break;
                }
                prefixes = prefixes
                    .iter()
                    .flat_map(|p| {
                        values.iter().map(move |v| {
                            let mut p = p.clone();
                            p.extend(encode(v));
                            p
                        })
                    })
                    .collect();
                points += 1;
                if prefixes.is_empty() {
                    break;
                }
            }
            FieldSet::Range {
                bracket: b,
                lower,
                upper,
            } => {
                let payload = |bound: &Bound<Value>| match bound {
                    Bound::Included(v) => Bound::Included(encode(v)[1].clone()),
                    Bound::Excluded(v) => Bound::Excluded(encode(v)[1].clone()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let ranges = prefixes
                    .into_iter()
                    .map(|mut prefix| {
                        prefix.push(bracket(&b));
                        KeyRange {
                            prefix,
                            lower: payload(&lower),
                            upper: payload(&upper),
                        }
                    })
                    .collect();
                return Some(Seek {
                    ranges,
                    points,
                    ranged: true,
                });
            }
        }
    }
    if points == 0 {
        return None;
    }
    Some(Seek {
        ranges: prefixes.into_iter().map(KeyRange::prefix).collect(),
        points,
        ranged: false,
    })
}

/// The paths `filter` pins to a single value (an equality, or `$in` with
/// one value): an order over them is no order at all.
pub(crate) fn fixed_paths(filter: &Filter) -> Vec<&str> {
    let required = filter.required();
    let mut paths: Vec<&str> = required.iter().map(|(p, _)| *p).collect();
    paths.dedup();
    paths.retain(|path| {
        let conds: Vec<&Cond> = required.iter().filter(|(p, _)| p == path).map(|(_, c)| *c).collect();
        matches!(field_set(&conds, false), Some(FieldSet::Points(v)) if v.len() == 1)
    });
    paths
}

/// Whether reading in the order of a key over `key_paths` (ascending)
/// sorts by `sort` — skipping, on either side, fields fixed to one value.
pub(crate) fn key_order_sorts(key_paths: &[&str], sort: &[(String, bool)], fixed: &[&str]) -> bool {
    let mut key = key_paths.iter().peekable();
    for (path, ascending) in sort {
        if fixed.contains(&path.as_str()) {
            continue;
        }
        while key.peek().is_some_and(|k| *k != path && fixed.contains(k)) {
            key.next();
        }
        if !ascending || key.next() != Some(&path.as_str()) {
            return false;
        }
    }
    true
}

// What the conditions on one field allow, when an index can seek it.
fn field_set(conds: &[&Cond], multikey: bool) -> Option<FieldSet> {
    let mut set: Option<FieldSet> = None;
    for cond in conds {
        let Some(this) = cond_set(cond) else {
            continue;
        };
        set = Some(match set {
            None => this,
            // Each element may satisfy a different condition: keep the
            // first alone (see the module comment).
            Some(s) if multikey => s,
            Some(s) => intersect(s, this),
        });
    }
    set
}

fn cond_set(cond: &Cond) -> Option<FieldSet> {
    // Equal documents can differ in their bytes ({x: 1} and {x: 1.0}), so
    // only values keys order like MongoDB are points.
    let point = |v: &Value| orders_like_mongo(v);
    match cond {
        Cond::Eq(v) if point(v) => Some(FieldSet::Points(vec![v.clone()])),
        Cond::In(vs) if vs.iter().all(point) => {
            let mut vs = vs.clone();
            vs.sort_by(compare);
            vs.dedup_by(|a, b| values_equal(a, b));
            Some(FieldSet::Points(vs))
        }
        Cond::Cmp(CmpOp::Gte | CmpOp::Lte, Value::Null) => {
            Some(FieldSet::Points(vec![Value::Null]))
        }
        Cond::Cmp(_, Value::Null) => Some(FieldSet::Points(vec![])),
        // An anchored pattern's matches are the strings starting with its
        // prefix: from the prefix to just past it.
        Cond::Regex(p) => p.prefix().map(|prefix| FieldSet::Range {
            bracket: Value::String(String::new()),
            upper: match successor(&prefix) {
                Some(s) => Bound::Excluded(Value::String(s)),
                None => Bound::Unbounded,
            },
            lower: Bound::Included(Value::String(prefix)),
        }),
        Cond::Cmp(op, v) if orders_like_mongo(v) => {
            let (lower, upper) = match op {
                CmpOp::Gt => (Bound::Excluded(v.clone()), Bound::Unbounded),
                CmpOp::Gte => (Bound::Included(v.clone()), Bound::Unbounded),
                CmpOp::Lt => (Bound::Unbounded, Bound::Excluded(v.clone())),
                CmpOp::Lte => (Bound::Unbounded, Bound::Included(v.clone())),
            };
            Some(FieldSet::Range {
                bracket: v.clone(),
                lower,
                upper,
            })
        }
        _ => None,
    }
}

// The least string above every string starting with `prefix`: its last
// character that can be, incremented, the rest dropped (UTF-8 bytes order
// as code points do). None when there is none (no upper bound).
fn successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = (last as u32 + 1..=char::MAX as u32).find_map(char::from_u32);
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

fn intersect(a: FieldSet, b: FieldSet) -> FieldSet {
    use FieldSet::*;
    match (a, b) {
        (Points(x), Points(y)) => Points(
            x.into_iter()
                .filter(|v| y.iter().any(|w| values_equal(v, w)))
                .collect(),
        ),
        (Points(p), r @ Range { .. }) | (r @ Range { .. }, Points(p)) => {
            Points(p.into_iter().filter(|v| in_range(v, &r)).collect())
        }
        (
            Range {
                bracket: b1,
                lower: l1,
                upper: u1,
            },
            Range {
                bracket: b2,
                lower: l2,
                upper: u2,
            },
        ) => {
            if type_order(&b1) != type_order(&b2) {
                return Points(vec![]);
            }
            Range {
                bracket: b1,
                lower: tighter(l1, l2, Ordering::Greater),
                upper: tighter(u1, u2, Ordering::Less),
            }
        }
    }
}

fn in_range(v: &Value, r: &FieldSet) -> bool {
    let FieldSet::Range { bracket, lower, upper } = r else {
        return false;
    };
    if type_order(v) != type_order(bracket) {
        return false;
    }
    let above = match lower {
        Bound::Included(lo) => cmp_value(v, CmpOp::Gte, lo),
        Bound::Excluded(lo) => cmp_value(v, CmpOp::Gt, lo),
        Bound::Unbounded => true,
    };
    let below = match upper {
        Bound::Included(hi) => cmp_value(v, CmpOp::Lte, hi),
        Bound::Excluded(hi) => cmp_value(v, CmpOp::Lt, hi),
        Bound::Unbounded => true,
    };
    above && below
}

// The more restrictive of two bounds on one side (`wins`: the ordering of
// the value that restricts more); on equal values, Excluded.
fn tighter(a: Bound<Value>, b: Bound<Value>, wins: Ordering) -> Bound<Value> {
    use Bound::*;
    match (a, b) {
        (Unbounded, x) | (x, Unbounded) => x,
        (a, b) => {
            let (va, vb) = match (&a, &b) {
                (Included(x) | Excluded(x), Included(y) | Excluded(y)) => (x, y),
                _ => unreachable!(),
            };
            match compare(va, vb) {
                Ordering::Equal if matches!(a, Excluded(_)) => a,
                Ordering::Equal => b,
                o if o == wins => a,
                _ => b,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Document;

    fn seek(filter: &str, paths: &[&str], multikey: bool) -> Option<Seek> {
        let f = Filter::parse(&Document::parse(filter).unwrap()).unwrap();
        seek_for(&f, paths, multikey)
    }

    #[test]
    fn test_points_then_one_range() {
        let s = seek(r#"{"a": 1, "b": {"$in": [2, 3]}, "c": {"$gt": 5}}"#, &["a", "b", "c"], false).unwrap();
        assert_eq!((s.points, s.ranged, s.ranges.len()), (2, true, 2));
        // A key whose first field is unconstrained can't be used.
        assert!(seek(r#"{"b": 1}"#, &["a", "b"], false).is_none());
        // Conditions a key order can't answer are left to the filter.
        assert!(seek(r#"{"a": {"$ne": 1}}"#, &["a"], false).is_none());
        assert!(seek(r#"{"a": [1, 2]}"#, &["a"], false).is_none());
    }

    #[test]
    fn test_key_order_sorts_skipping_fixed_fields() {
        let sort = |s: &[(&str, bool)]| s.iter().map(|(p, a)| (p.to_string(), *a)).collect::<Vec<_>>();
        assert!(key_order_sorts(&["a", "b"], &sort(&[("a", true)]), &[]));
        assert!(key_order_sorts(&["a", "b"], &sort(&[("a", true), ("b", true)]), &[]));
        assert!(!key_order_sorts(&["a", "b"], &sort(&[("b", true)]), &[]));
        assert!(key_order_sorts(&["a", "b"], &sort(&[("b", true)]), &["a"]));
        assert!(key_order_sorts(&["a", "b"], &sort(&[("c", true), ("a", true)]), &["c"]));
        assert!(!key_order_sorts(&["a"], &sort(&[("a", false)]), &[]));
        assert!(!key_order_sorts(&["a"], &sort(&[("a", true), ("z", true)]), &[]));
        let f = Filter::parse(&Document::parse(r#"{"a": 1, "b": {"$in": [2]}, "c": {"$in": [1, 2]}, "d": {"$gt": 1}}"#).unwrap()).unwrap();
        assert_eq!(fixed_paths(&f), vec!["a", "b"]);
    }

    #[test]
    fn test_anchored_regexes_seek_a_string_range() {
        let s = seek(r#"{"a": {"$regex": "^ab+c"}}"#, &["a"], false).unwrap();
        assert!(s.ranged);
        let bound = |b: &Bound<ValueItem>| match b {
            Bound::Included(ValueItem::Str((s, _))) => format!("[{s}"),
            Bound::Excluded(ValueItem::Str((s, _))) => format!("{s})"),
            other => format!("{other:?}"),
        };
        // `b+` needs a b, so the prefix is "ab".
        assert_eq!((bound(&s.ranges[0].lower), bound(&s.ranges[0].upper)), ("[ab".into(), "ac)".into()));
        let s = seek(r#"{"a": {"$regex": "^abc?"}}"#, &["a"], false).unwrap();
        assert_eq!(bound(&s.ranges[0].lower), "[ab");
        // Unanchored, case-insensitive or alternated patterns can't seek.
        assert!(seek(r#"{"a": {"$regex": "abc"}}"#, &["a"], false).is_none());
        assert!(seek(r#"{"a": {"$regex": "^abc", "$options": "i"}}"#, &["a"], false).is_none());
        assert!(seek(r#"{"a": {"$regex": "^a|b"}}"#, &["a"], false).is_none());
        assert_eq!(successor("a\u{10FFFF}"), Some("b".into()));
        assert_eq!(successor("\u{10FFFF}"), None);
    }

    #[test]
    fn test_conditions_intersect_unless_multikey() {
        let s = seek(r#"{"a": {"$gt": 5, "$lt": 9}}"#, &["a"], false).unwrap();
        assert!(matches!(s.ranges[0].lower, Bound::Excluded(_)) && matches!(s.ranges[0].upper, Bound::Excluded(_)));
        let s = seek(r#"{"a": {"$gt": 5, "$lt": 9}}"#, &["a"], true).unwrap();
        assert!(matches!(s.ranges[0].upper, Bound::Unbounded));
        // Different types can't both hold of one value.
        let s = seek(r#"{"a": {"$gt": 5, "$lt": "z"}}"#, &["a"], false).unwrap();
        assert!(s.ranges.is_empty());
        let s = seek(r#"{"a": {"$in": [1, 5, 9]}, "$and": [{"a": {"$gt": 3}}]}"#, &["a"], false).unwrap();
        assert_eq!(s.ranges.len(), 2);
    }
}
