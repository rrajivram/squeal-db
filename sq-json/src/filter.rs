//! Query filters: `{qty: {$gt: 5}, "tags": "red", $or: [...]}`.
//!
//! Matching follows MongoDB's rules:
//! - a path reaches through arrays (see value::lookup), and a condition on a
//!   field holding an array holds if it holds for the array itself or for
//!   any element of it: `{tags: "red"}` matches `tags: ["red", "blue"]`;
//! - comparisons are type-bracketed: `{qty: {$gt: 5}}` only ever matches
//!   numbers, and `5` equals `5.0`;
//! - `null` matches a missing field as well as an explicit null;
//! - negations (`$ne`, `$nin`, `$not`, `$nor`) match what the positive form
//!   doesn't, missing fields included.

use std::cmp::Ordering;

use crate::error::{Error, Result};
use crate::value::{Document, Value, compare, lookup, type_order, values_equal};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Nor(Vec<Filter>),
    /// Every condition holds for the field at this path.
    Field(String, Vec<Cond>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cond {
    Eq(Value),
    Ne(Value),
    Cmp(CmpOp, Value),
    In(Vec<Value>),
    Nin(Vec<Value>),
    Exists(bool),
    Size(usize),
    All(Vec<Value>),
    /// Some element of an array field matches: a filter over its elements'
    /// fields, or conditions on the element values themselves.
    ElemMatch(Box<ElemMatch>),
    Not(Vec<Cond>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ElemMatch {
    Filter(Filter),
    Conds(Vec<Cond>),
}

impl Filter {
    pub(crate) fn parse(doc: &Document) -> Result<Filter> {
        let mut parts = vec![];
        for (key, value) in doc.iter() {
            parts.push(match key.as_str() {
                "$and" => Filter::And(parse_list(key, value)?),
                "$or" => Filter::Or(parse_list(key, value)?),
                "$nor" => Filter::Nor(parse_list(key, value)?),
                "$comment" => continue,
                k if k.starts_with('$') => {
                    return Err(Error::BadValue(format!("unknown top level operator: {k}")));
                }
                path => Filter::Field(path.to_string(), parse_conds(path, value)?),
            });
        }
        Ok(Filter::And(parts))
    }

    pub(crate) fn matches(&self, doc: &Document) -> bool {
        match self {
            Filter::And(fs) => fs.iter().all(|f| f.matches(doc)),
            Filter::Or(fs) => fs.iter().any(|f| f.matches(doc)),
            Filter::Nor(fs) => !fs.iter().any(|f| f.matches(doc)),
            Filter::Field(path, conds) => {
                let values = lookup(doc, path);
                conds.iter().all(|c| c.holds(&values))
            }
        }
    }

    /// The conditions every matching document must meet on one field each —
    /// the top-level AND of the filter, flattened — for an index to use.
    pub(crate) fn required(&self) -> Vec<(&str, &Cond)> {
        let mut out = vec![];
        self.collect_required(&mut out);
        out
    }

    fn collect_required<'a>(&'a self, out: &mut Vec<(&'a str, &'a Cond)>) {
        match self {
            Filter::And(fs) => fs.iter().for_each(|f| f.collect_required(out)),
            Filter::Field(path, conds) => out.extend(conds.iter().map(|c| (path.as_str(), c))),
            Filter::Or(_) | Filter::Nor(_) => {}
        }
    }
}

fn parse_list(op: &str, value: &Value) -> Result<Vec<Filter>> {
    let Value::Array(items) = value else {
        return Err(Error::BadValue(format!("{op} must be an array")));
    };
    if items.is_empty() {
        return Err(Error::BadValue(format!("{op} must be a nonempty array")));
    }
    items
        .iter()
        .map(|item| match item {
            Value::Document(d) => Filter::parse(d),
            _ => Err(Error::BadValue(format!("{op} entries need to be full objects"))),
        })
        .collect()
}

// The conditions `{path: value}` sets: an operator document's operators, or
// equality with anything else.
fn parse_conds(path: &str, value: &Value) -> Result<Vec<Cond>> {
    match value {
        Value::Document(d) if is_operator_doc(d) => parse_operators(path, d),
        other => Ok(vec![Cond::Eq(other.clone())]),
    }
}

fn is_operator_doc(d: &Document) -> bool {
    d.iter().next().is_some_and(|(k, _)| k.starts_with('$'))
}

fn parse_operators(path: &str, d: &Document) -> Result<Vec<Cond>> {
    let array = |op: &str, v: &Value| match v {
        Value::Array(items) => Ok(items.clone()),
        _ => Err(Error::BadValue(format!("{op} needs an array"))),
    };
    d.iter()
        .map(|(op, v)| {
            Ok(match op.as_str() {
                "$eq" => Cond::Eq(v.clone()),
                "$ne" => Cond::Ne(v.clone()),
                "$gt" => Cond::Cmp(CmpOp::Gt, v.clone()),
                "$gte" => Cond::Cmp(CmpOp::Gte, v.clone()),
                "$lt" => Cond::Cmp(CmpOp::Lt, v.clone()),
                "$lte" => Cond::Cmp(CmpOp::Lte, v.clone()),
                "$in" => Cond::In(array(op, v)?),
                "$nin" => Cond::Nin(array(op, v)?),
                "$all" => Cond::All(array(op, v)?),
                "$exists" => Cond::Exists(truthy(v)),
                "$size" => match v {
                    Value::Int(n) if *n >= 0 => Cond::Size(*n as usize),
                    Value::Double(n) if *n >= 0.0 && n.fract() == 0.0 => Cond::Size(*n as usize),
                    _ => return Err(Error::BadValue("$size needs a non-negative whole number".into())),
                },
                "$elemMatch" => match v {
                    Value::Document(inner) if is_operator_doc(inner) => {
                        Cond::ElemMatch(Box::new(ElemMatch::Conds(parse_operators(path, inner)?)))
                    }
                    Value::Document(inner) => {
                        Cond::ElemMatch(Box::new(ElemMatch::Filter(Filter::parse(inner)?)))
                    }
                    _ => return Err(Error::BadValue("$elemMatch needs an Object".into())),
                },
                "$not" => match v {
                    Value::Document(inner) if is_operator_doc(inner) => {
                        Cond::Not(parse_operators(path, inner)?)
                    }
                    _ => return Err(Error::BadValue("$not needs an operator object".into())),
                },
                other => {
                    return Err(Error::BadValue(format!("unknown operator {other} on {path}")));
                }
            })
        })
        .collect()
}

fn truthy(v: &Value) -> bool {
    !matches!(v, Value::Null | Value::Bool(false))
        && !matches!(v, Value::Int(0))
        && !matches!(v, Value::Double(d) if *d == 0.0)
}

impl Cond {
    /// Whether this condition holds for a field whose path reached `values`
    /// (none: the field is missing).
    fn holds(&self, values: &[&Value]) -> bool {
        match self {
            Cond::Eq(v) => eq_holds(values, v),
            Cond::Ne(v) => !eq_holds(values, v),
            Cond::Cmp(op, v) => cmp_holds(values, *op, v),
            Cond::In(vs) => vs.iter().any(|v| eq_holds(values, v)),
            Cond::Nin(vs) => !vs.iter().any(|v| eq_holds(values, v)),
            Cond::Exists(want) => !values.is_empty() == *want,
            Cond::Size(n) => values
                .iter()
                .any(|v| matches!(v, Value::Array(items) if items.len() == *n)),
            Cond::All(vs) => !vs.is_empty() && vs.iter().all(|v| eq_holds(values, v)),
            Cond::ElemMatch(em) => values.iter().any(|v| match v {
                Value::Array(items) => items.iter().any(|e| em.matches(e)),
                _ => false,
            }),
            Cond::Not(conds) => !conds.iter().all(|c| c.holds(values)),
        }
    }
}

impl ElemMatch {
    fn matches(&self, element: &Value) -> bool {
        match self {
            ElemMatch::Filter(f) => matches!(element, Value::Document(d) if f.matches(d)),
            // The element itself is the value — not reached through a path,
            // so an array element that is itself an array isn't opened up.
            ElemMatch::Conds(conds) => conds.iter().all(|c| c.holds_for_value(element)),
        }
    }
}

impl Cond {
    fn holds_for_value(&self, v: &Value) -> bool {
        match self {
            Cond::Eq(target) => values_equal(v, target),
            Cond::Ne(target) => !values_equal(v, target),
            Cond::Cmp(op, target) => cmp_value(v, *op, target),
            Cond::In(ts) => ts.iter().any(|t| values_equal(v, t)),
            Cond::Nin(ts) => !ts.iter().any(|t| values_equal(v, t)),
            other => other.holds(&[v]),
        }
    }
}

// `field == v`: the field's value, or (for an array) any element of it.
// null also matches a missing field.
fn eq_holds(values: &[&Value], v: &Value) -> bool {
    if matches!(v, Value::Null) && values.is_empty() {
        return true;
    }
    values.iter().any(|x| {
        values_equal(x, v)
            || matches!(x, Value::Array(items) if items.iter().any(|e| values_equal(e, v)))
    })
}

fn cmp_holds(values: &[&Value], op: CmpOp, v: &Value) -> bool {
    // $gte / $lte null are equality with null (missing included).
    if matches!(v, Value::Null) {
        return matches!(op, CmpOp::Gte | CmpOp::Lte) && eq_holds(values, v);
    }
    values.iter().any(|x| {
        cmp_value(x, op, v)
            || matches!(x, Value::Array(items) if items.iter().any(|e| cmp_value(e, op, v)))
    })
}

// One value against the operand, only within the operand's type bracket.
pub(crate) fn cmp_value(x: &Value, op: CmpOp, v: &Value) -> bool {
    if type_order(x) != type_order(v) {
        return false;
    }
    let o = compare(x, v);
    match op {
        CmpOp::Gt => o == Ordering::Greater,
        CmpOp::Gte => o != Ordering::Less,
        CmpOp::Lt => o == Ordering::Less,
        CmpOp::Lte => o != Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(filter: &str, doc: &str) -> bool {
        Filter::parse(&Document::parse(filter).unwrap())
            .unwrap()
            .matches(&Document::parse(doc).unwrap())
    }

    #[test]
    fn test_equality_including_arrays_and_nested_paths() {
        assert!(matches(r#"{"a": 5}"#, r#"{"a": 5.0}"#));
        assert!(!matches(r#"{"a": 5}"#, r#"{"a": "5"}"#));
        assert!(matches(r#"{"tags": "red"}"#, r#"{"tags": ["blue", "red"]}"#));
        assert!(matches(r#"{"tags": ["blue", "red"]}"#, r#"{"tags": ["blue", "red"]}"#));
        assert!(!matches(r#"{"tags": ["red", "blue"]}"#, r#"{"tags": ["blue", "red"]}"#));
        assert!(matches(r#"{"a.b": 2}"#, r#"{"a": [{"b": 1}, {"b": 2}]}"#));
        assert!(matches(r#"{"a.1": 20}"#, r#"{"a": [10, 20]}"#));
        assert!(matches(r#"{"a": {"x": 1, "y": 2}}"#, r#"{"a": {"x": 1, "y": 2}}"#));
        // Embedded documents compare with field order.
        assert!(!matches(r#"{"a": {"y": 2, "x": 1}}"#, r#"{"a": {"x": 1, "y": 2}}"#));
    }

    #[test]
    fn test_null_matches_missing_fields() {
        assert!(matches(r#"{"a": null}"#, r#"{"b": 1}"#));
        assert!(matches(r#"{"a": null}"#, r#"{"a": null}"#));
        assert!(!matches(r#"{"a": null}"#, r#"{"a": 0}"#));
        assert!(matches(r#"{"a": {"$ne": null}}"#, r#"{"a": 0}"#));
        assert!(!matches(r#"{"a": {"$ne": null}}"#, r#"{"b": 0}"#));
        assert!(matches(r#"{"a": {"$gte": null}}"#, r#"{}"#));
        assert!(!matches(r#"{"a": {"$gt": null}}"#, r#"{"a": null}"#));
    }

    #[test]
    fn test_comparisons_are_type_bracketed() {
        assert!(matches(r#"{"a": {"$gt": 5}}"#, r#"{"a": 6}"#));
        assert!(matches(r#"{"a": {"$gt": 5}}"#, r#"{"a": 5.5}"#));
        assert!(!matches(r#"{"a": {"$gt": 5}}"#, r#"{"a": "6"}"#));
        assert!(!matches(r#"{"a": {"$lt": 5}}"#, r#"{"a": null}"#));
        assert!(matches(r#"{"a": {"$gte": 5, "$lt": 7}}"#, r#"{"a": 5}"#));
        assert!(!matches(r#"{"a": {"$gte": 5, "$lt": 7}}"#, r#"{"a": 7}"#));
        // Different elements may satisfy different conditions.
        assert!(matches(r#"{"a": {"$gt": 10, "$lt": 5}}"#, r#"{"a": [1, 20]}"#));
        assert!(matches(r#"{"s": {"$gte": "b"}}"#, r#"{"s": "banana"}"#));
        assert!(matches(r#"{"d": {"$lt": {"$date": "2024-01-01"}}}"#, r#"{"d": {"$date": "2023-06-01"}}"#));
    }

    #[test]
    fn test_in_nin_exists_size_all() {
        assert!(matches(r#"{"a": {"$in": [1, 2]}}"#, r#"{"a": 2}"#));
        assert!(matches(r#"{"a": {"$in": [null]}}"#, r#"{}"#));
        assert!(matches(r#"{"a": {"$nin": [1, 2]}}"#, r#"{"a": 3}"#));
        assert!(matches(r#"{"a": {"$nin": [1, 2]}}"#, r#"{}"#));
        assert!(!matches(r#"{"a": {"$nin": [1, 2]}}"#, r#"{"a": [2, 3]}"#));
        assert!(matches(r#"{"a": {"$exists": true}}"#, r#"{"a": null}"#));
        assert!(matches(r#"{"a": {"$exists": false}}"#, r#"{"b": 1}"#));
        assert!(matches(r#"{"a": {"$size": 2}}"#, r#"{"a": [1, [2, 3]]}"#));
        assert!(matches(r#"{"a": {"$all": [3, 1]}}"#, r#"{"a": [1, 2, 3]}"#));
        assert!(!matches(r#"{"a": {"$all": [3, 4]}}"#, r#"{"a": [1, 2, 3]}"#));
    }

    #[test]
    fn test_logical_operators_and_elem_match() {
        assert!(matches(r#"{"$or": [{"a": 1}, {"b": 2}]}"#, r#"{"b": 2}"#));
        assert!(!matches(r#"{"$nor": [{"a": 1}, {"b": 2}]}"#, r#"{"b": 2}"#));
        assert!(matches(r#"{"$and": [{"a": {"$gt": 1}}, {"a": {"$lt": 3}}]}"#, r#"{"a": 2}"#));
        assert!(matches(r#"{"a": {"$not": {"$gt": 5}}}"#, r#"{"a": 3}"#));
        assert!(matches(r#"{"a": {"$not": {"$gt": 5}}}"#, r#"{}"#));
        // One element must satisfy every condition at once.
        assert!(!matches(r#"{"a": {"$elemMatch": {"$gt": 10, "$lt": 5}}}"#, r#"{"a": [1, 20]}"#));
        assert!(matches(r#"{"a": {"$elemMatch": {"$gt": 10, "$lt": 25}}}"#, r#"{"a": [1, 20]}"#));
        let doc = r#"{"items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 5}]}"#;
        assert!(matches(r#"{"items": {"$elemMatch": {"sku": "y", "qty": {"$gt": 2}}}}"#, doc));
        assert!(!matches(r#"{"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 2}}}}"#, doc));
    }

    #[test]
    fn test_malformed_filters_are_errors() {
        for f in [
            r#"{"$foo": 1}"#,
            r#"{"a": {"$bogus": 1}}"#,
            r#"{"$or": []}"#,
            r#"{"$or": 1}"#,
            r#"{"a": {"$in": 1}}"#,
            r#"{"a": {"$size": -1}}"#,
            r#"{"a": {"$not": 5}}"#,
        ] {
            assert!(Filter::parse(&Document::parse(f).unwrap()).is_err(), "{f}");
        }
    }
}
