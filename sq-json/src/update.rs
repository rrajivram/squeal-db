//! Updates: operator documents (`{$set: {...}, $inc: {...}}`) or a whole
//! replacement document, applied to one document at a time.

use std::cmp::Ordering;

use crate::error::{Error, Result};
use crate::filter::{Cond, Filter};
use crate::value::{Document, Value, compare, values_equal};

#[derive(Debug, Clone)]
pub(crate) enum Update {
    Ops(Vec<(Op, String, Value)>),
    Replace(Document),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Op {
    Set,
    Unset,
    Inc,
    Mul,
    Min,
    Max,
    Rename,
    Push,
    AddToSet,
    Pull,
    Pop,
    SetOnInsert,
    CurrentDate,
}

impl Update {
    pub(crate) fn parse(doc: &Document) -> Result<Update> {
        let operators = doc.iter().filter(|(k, _)| k.starts_with('$')).count();
        if operators == 0 {
            check_field_names(doc)?;
            return Ok(Update::Replace(doc.clone()));
        }
        if operators != doc.len() {
            return Err(Error::BadValue(
                "an update mixes operators and plain fields: use operators only, or a whole \
                 replacement document"
                    .into(),
            ));
        }
        let mut ops = vec![];
        for (name, args) in doc.iter() {
            let op = match name.as_str() {
                "$set" => Op::Set,
                "$unset" => Op::Unset,
                "$inc" => Op::Inc,
                "$mul" => Op::Mul,
                "$min" => Op::Min,
                "$max" => Op::Max,
                "$rename" => Op::Rename,
                "$push" => Op::Push,
                "$addToSet" => Op::AddToSet,
                "$pull" => Op::Pull,
                "$pop" => Op::Pop,
                "$setOnInsert" => Op::SetOnInsert,
                "$currentDate" => Op::CurrentDate,
                other => return Err(Error::BadValue(format!("unknown update operator: {other}"))),
            };
            let Value::Document(fields) = args else {
                return Err(Error::BadValue(format!("{name} needs an object of fields")));
            };
            for (path, value) in fields.iter() {
                if path.is_empty() || path.split('.').any(|p| p.is_empty()) {
                    return Err(Error::BadValue(format!("{name}: invalid path {path:?}")));
                }
                ops.push((op, path.clone(), value.clone()));
            }
        }
        // Two operators touching the same field, or one inside the other,
        // would make the outcome depend on their order.
        let paths: Vec<&str> = ops
            .iter()
            .flat_map(|(op, path, value)| {
                let mut p = vec![path.as_str()];
                if let (Op::Rename, Value::String(to)) = (op, value) {
                    p.push(to.as_str());
                }
                p
            })
            .collect();
        for (i, a) in paths.iter().enumerate() {
            for b in &paths[i + 1..] {
                if overlaps(a, b) {
                    return Err(Error::BadValue(format!(
                        "Updating the path '{a}' would create a conflict at '{b}'"
                    )));
                }
            }
        }
        Ok(Update::Ops(ops))
    }

    /// Applies the update to `doc` (an existing document, or for an upsert
    /// the one being inserted — `inserting` makes $setOnInsert apply).
    /// Returns whether it changed anything. `_id` can't change.
    pub(crate) fn apply(&self, doc: &mut Document, inserting: bool) -> Result<bool> {
        let before = doc.clone();
        match self {
            Update::Replace(new) => {
                let mut replacement = new.clone();
                match (before.get("_id"), replacement.get("_id")) {
                    (Some(old), Some(new)) if !values_equal(old, new) => {
                        return Err(Error::ImmutableId);
                    }
                    (Some(old), _) => replacement.insert_first("_id", old.clone()),
                    _ => {}
                }
                *doc = replacement;
            }
            Update::Ops(ops) => {
                for (op, path, arg) in ops {
                    apply_op(doc, *op, path, arg, inserting)?;
                }
                if let Some(old) = before.get("_id")
                    && !doc.get("_id").is_some_and(|new| values_equal(old, new))
                {
                    return Err(Error::ImmutableId);
                }
            }
        }
        Ok(*doc != before)
    }

    /// The document an upsert starts from when nothing matched: the
    /// filter's equality conditions on plain fields (`{sku: "x", "a.b":
    /// 1}` gives `{sku: "x", a: {b: 1}}`), then the update applied to it;
    /// for a replacement, the replacement with the filter's `_id`.
    pub(crate) fn upsert_document(&self, filter: &Filter) -> Result<Document> {
        let mut doc = Document::new();
        for (path, cond) in filter.required() {
            if let Cond::Eq(v) = cond
                && !matches!(v, Value::Document(d) if d.iter().any(|(k, _)| k.starts_with('$')))
            {
                set_path(&mut doc, path, v.clone())?;
            }
        }
        match self {
            Update::Replace(r) => {
                let id = doc.get("_id").cloned();
                doc = r.clone();
                if let Some(id) = id {
                    if doc.get("_id").is_some_and(|x| !values_equal(x, &id)) {
                        return Err(Error::ImmutableId);
                    }
                    doc.insert_first("_id", id);
                }
            }
            Update::Ops(_) => {
                self.apply(&mut doc, true)?;
            }
        }
        Ok(doc)
    }
}

fn overlaps(a: &str, b: &str) -> bool {
    a == b
        || (a.starts_with(b) && a.as_bytes()[b.len()] == b'.')
        || (b.starts_with(a) && b.as_bytes()[a.len()] == b'.')
}

/// Field names a stored document may use: none starting with '$'.
pub(crate) fn check_field_names(doc: &Document) -> Result<()> {
    for (k, v) in doc.iter() {
        if k.starts_with('$') {
            return Err(Error::BadValue(format!("field names can't start with '$': {k:?}")));
        }
        check_nested(v)?;
    }
    Ok(())
}

fn check_nested(v: &Value) -> Result<()> {
    match v {
        Value::Document(d) => check_field_names(d),
        Value::Array(items) => items.iter().try_for_each(check_nested),
        _ => Ok(()),
    }
}

fn apply_op(doc: &mut Document, op: Op, path: &str, arg: &Value, inserting: bool) -> Result<()> {
    match op {
        Op::Set => set_path(doc, path, arg.clone()),
        Op::SetOnInsert if inserting => set_path(doc, path, arg.clone()),
        Op::SetOnInsert => Ok(()),
        Op::Unset => {
            unset_path(doc, path);
            Ok(())
        }
        Op::CurrentDate => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            set_path(doc, path, Value::Date(now))
        }
        Op::Inc | Op::Mul => {
            if !is_number(arg) {
                return Err(Error::BadValue(format!("cannot {op:?} with a non-numeric argument")));
            }
            let current = get_path(doc, path).cloned();
            let result = match current {
                None => match op {
                    Op::Inc => arg.clone(),
                    _ => zero_like(arg),
                },
                Some(v) if is_number(&v) => arithmetic(&v, arg, op)?,
                Some(v) => {
                    return Err(Error::BadValue(format!(
                        "cannot apply {op:?} to a non-numeric value at {path}: {v}"
                    )));
                }
            };
            set_path(doc, path, result)
        }
        Op::Min | Op::Max => {
            let replace = match get_path(doc, path) {
                None => true,
                Some(v) => {
                    let o = compare(arg, v);
                    (op == Op::Min && o == Ordering::Less) || (op == Op::Max && o == Ordering::Greater)
                }
            };
            if replace {
                set_path(doc, path, arg.clone())?;
            }
            Ok(())
        }
        Op::Rename => {
            let Value::String(to) = arg else {
                return Err(Error::BadValue("$rename's target must be a string".into()));
            };
            if let Some(v) = unset_path(doc, path) {
                set_path(doc, to, v)?;
            }
            Ok(())
        }
        Op::Push | Op::AddToSet => {
            let items = each(arg);
            let array = array_at(doc, path)?;
            for item in items {
                if op == Op::Push || !array.iter().any(|e| values_equal(e, &item)) {
                    array.push(item);
                }
            }
            Ok(())
        }
        Op::Pull => {
            if get_path(doc, path).is_none() {
                return Ok(());
            }
            let array = array_at(doc, path)?;
            let remove: Box<dyn Fn(&Value) -> bool> = match arg {
                Value::Document(d) if d.iter().next().is_some_and(|(k, _)| k.starts_with('$')) => {
                    let probe = Document::parse(&format!(
                        "{{\"v\": {{\"$elemMatch\": {}}}}}",
                        arg.to_json()
                    ))?;
                    let f = Filter::parse(&probe)?;
                    Box::new(move |e| {
                        let mut wrapper = Document::new();
                        wrapper.insert("v", Value::Array(vec![e.clone()]));
                        f.matches(&wrapper)
                    })
                }
                Value::Document(d) => {
                    let f = Filter::parse(d)?;
                    Box::new(move |e| matches!(e, Value::Document(ed) if f.matches(ed)))
                }
                v => {
                    let v = v.clone();
                    Box::new(move |e| values_equal(e, &v))
                }
            };
            array.retain(|e| !remove(e));
            Ok(())
        }
        Op::Pop => {
            if get_path(doc, path).is_none() {
                return Ok(());
            }
            let first = match arg {
                Value::Int(-1) => true,
                Value::Int(1) => false,
                Value::Double(d) if *d == -1.0 => true,
                Value::Double(d) if *d == 1.0 => false,
                _ => return Err(Error::BadValue("$pop takes 1 or -1".into())),
            };
            let array = array_at(doc, path)?;
            if !array.is_empty() {
                if first {
                    array.remove(0);
                } else {
                    array.pop();
                }
            }
            Ok(())
        }
    }
}

// $push / $addToSet's items: `{$each: [...]}`, or the one value.
fn each(arg: &Value) -> Vec<Value> {
    match arg {
        Value::Document(d) if d.len() == 1 => match d.get("$each") {
            Some(Value::Array(items)) => items.clone(),
            _ => vec![arg.clone()],
        },
        _ => vec![arg.clone()],
    }
}

// The array at `path`, created empty when missing; an error if something
// else is there.
fn array_at<'a>(doc: &'a mut Document, path: &str) -> Result<&'a mut Vec<Value>> {
    if get_path(doc, path).is_none() {
        set_path(doc, path, Value::Array(vec![]))?;
    }
    match get_path_mut(doc, path) {
        Some(Value::Array(items)) => Ok(items),
        Some(other) => Err(Error::BadValue(format!(
            "the field at {path} must be an array, not {other}"
        ))),
        None => unreachable!("set just above"),
    }
}

fn is_number(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::Double(_))
}

fn zero_like(v: &Value) -> Value {
    match v {
        Value::Int(_) => Value::Int(0),
        _ => Value::Double(0.0),
    }
}

fn arithmetic(a: &Value, b: &Value, op: Op) -> Result<Value> {
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            let r = if op == Op::Inc { x.checked_add(*y) } else { x.checked_mul(*y) };
            Value::Int(r.ok_or_else(|| Error::BadValue("integer overflow".into()))?)
        }
        (x, y) => {
            let f = |v: &Value| match v {
                Value::Int(i) => *i as f64,
                Value::Double(d) => *d,
                _ => unreachable!("checked numeric"),
            };
            Value::Double(if op == Op::Inc { f(x) + f(y) } else { f(x) * f(y) })
        }
    })
}

// ---- paths, without array expansion (an update names one place) ----

pub(crate) fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut v = doc.get(parts.next()?)?;
    for part in parts {
        v = step(v, part)?;
    }
    Some(v)
}

fn step<'a>(v: &'a Value, part: &str) -> Option<&'a Value> {
    match v {
        Value::Document(d) => d.get(part),
        Value::Array(items) => items.get(part.parse::<usize>().ok()?),
        _ => None,
    }
}

fn get_path_mut<'a>(doc: &'a mut Document, path: &str) -> Option<&'a mut Value> {
    let mut parts = path.split('.');
    let mut v = doc.get_mut(parts.next()?)?;
    for part in parts {
        v = match v {
            Value::Document(d) => d.get_mut(part)?,
            Value::Array(items) => items.get_mut(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(v)
}

/// Sets `path` to `value`, creating missing documents on the way; a
/// numeric part indexes an array (padding it with nulls if short).
pub(crate) fn set_path(doc: &mut Document, path: &str, value: Value) -> Result<()> {
    let parts: Vec<&str> = path.split('.').collect();
    set_in_doc(doc, &parts, value, path)
}

fn set_in_doc(doc: &mut Document, parts: &[&str], value: Value, path: &str) -> Result<()> {
    let [first, rest @ ..] = parts else {
        unreachable!("paths have a part");
    };
    if rest.is_empty() {
        doc.insert(*first, value);
        return Ok(());
    }
    if doc.get(first).is_none() {
        doc.insert(*first, Value::Document(Document::new()));
    }
    set_in_value(doc.get_mut(first).expect("just set"), rest, value, path)
}

fn set_in_value(v: &mut Value, parts: &[&str], value: Value, path: &str) -> Result<()> {
    match v {
        Value::Document(d) => set_in_doc(d, parts, value, path),
        Value::Array(items) => {
            let i: usize = parts[0].parse().map_err(|_| {
                Error::BadValue(format!("cannot use the part {:?} of {path} to traverse an array", parts[0]))
            })?;
            if items.len() <= i {
                items.resize(i + 1, Value::Null);
            }
            if parts.len() == 1 {
                items[i] = value;
                Ok(())
            } else {
                if matches!(items[i], Value::Null) {
                    items[i] = Value::Document(Document::new());
                }
                set_in_value(&mut items[i], &parts[1..], value, path)
            }
        }
        other => Err(Error::BadValue(format!(
            "cannot create field {:?} of {path} in element {other}",
            parts[0]
        ))),
    }
}

/// Removes `path`, returning what was there. An array element is set to
/// null rather than removed (as MongoDB's $unset does).
pub(crate) fn unset_path(doc: &mut Document, path: &str) -> Option<Value> {
    let (parent, last) = match path.rsplit_once('.') {
        Some((p, l)) => (Some(p), l),
        None => (None, path),
    };
    match parent {
        None => doc.remove(last),
        Some(p) => match get_path_mut(doc, p)? {
            Value::Document(d) => d.remove(last),
            Value::Array(items) => {
                let item = items.get_mut(last.parse::<usize>().ok()?)?;
                Some(std::mem::replace(item, Value::Null))
            }
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(doc: &str, update: &str) -> Result<(Document, bool)> {
        let mut d = Document::parse(doc).unwrap();
        let changed = Update::parse(&Document::parse(update).unwrap())?.apply(&mut d, false)?;
        Ok((d, changed))
    }

    fn after(doc: &str, update: &str) -> String {
        apply(doc, update).unwrap().0.to_json()
    }

    #[test]
    fn test_set_unset_and_nested_paths() {
        assert_eq!(after(r#"{"_id":1,"a":1}"#, r#"{"$set":{"a":2,"b.c":3}}"#), r#"{"_id":1,"a":2,"b":{"c":3}}"#);
        assert_eq!(after(r#"{"a":[1,2]}"#, r#"{"$set":{"a.3":9}}"#), r#"{"a":[1,2,null,9]}"#);
        assert_eq!(after(r#"{"a":{"b":1,"c":2}}"#, r#"{"$unset":{"a.b":""}}"#), r#"{"a":{"c":2}}"#);
        assert_eq!(after(r#"{"a":[1,2]}"#, r#"{"$unset":{"a.0":""}}"#), r#"{"a":[null,2]}"#);
        assert!(apply(r#"{"a":1}"#, r#"{"$set":{"a.b":2}}"#).is_err());
        let (_, changed) = apply(r#"{"a":1}"#, r#"{"$set":{"a":1}}"#).unwrap();
        assert!(!changed);
    }

    #[test]
    fn test_arithmetic_min_max_rename() {
        assert_eq!(after(r#"{"n":1}"#, r#"{"$inc":{"n":2,"m":5}}"#), r#"{"n":3,"m":5}"#);
        assert_eq!(after(r#"{"n":1}"#, r#"{"$inc":{"n":0.5}}"#), r#"{"n":1.5}"#);
        assert_eq!(after(r#"{"n":3}"#, r#"{"$mul":{"n":2,"z":4}}"#), r#"{"n":6,"z":0}"#);
        assert!(apply(r#"{"n":"x"}"#, r#"{"$inc":{"n":1}}"#).is_err());
        assert!(apply(r#"{"n":9223372036854775807}"#, r#"{"$inc":{"n":1}}"#).is_err());
        assert_eq!(after(r#"{"n":5}"#, r#"{"$min":{"n":3}}"#), r#"{"n":3}"#);
        assert_eq!(after(r#"{"n":5}"#, r#"{"$max":{"n":3}}"#), r#"{"n":5}"#);
        assert_eq!(after(r#"{"a":1,"b":2}"#, r#"{"$rename":{"a":"c"}}"#), r#"{"b":2,"c":1}"#);
    }

    #[test]
    fn test_array_operators() {
        assert_eq!(after(r#"{"t":[1]}"#, r#"{"$push":{"t":2}}"#), r#"{"t":[1,2]}"#);
        assert_eq!(after(r#"{}"#, r#"{"$push":{"t":{"$each":[1,2]}}}"#), r#"{"t":[1,2]}"#);
        assert_eq!(after(r#"{"t":[1,2]}"#, r#"{"$addToSet":{"t":{"$each":[2,3]}}}"#), r#"{"t":[1,2,3]}"#);
        assert_eq!(after(r#"{"t":[1,2,1,3]}"#, r#"{"$pull":{"t":1}}"#), r#"{"t":[2,3]}"#);
        assert_eq!(after(r#"{"t":[1,5,9]}"#, r#"{"$pull":{"t":{"$gt":4}}}"#), r#"{"t":[1]}"#);
        assert_eq!(
            after(r#"{"r":[{"s":8,"i":"a"},{"s":7,"i":"b"}]}"#, r#"{"$pull":{"r":{"s":8}}}"#),
            r#"{"r":[{"s":7,"i":"b"}]}"#
        );
        assert_eq!(after(r#"{"t":[1,2,3]}"#, r#"{"$pop":{"t":-1}}"#), r#"{"t":[2,3]}"#);
        assert!(apply(r#"{"t":1}"#, r#"{"$push":{"t":2}}"#).is_err());
    }

    #[test]
    fn test_replacement_and_id_immutability() {
        assert_eq!(after(r#"{"_id":1,"a":1}"#, r#"{"b":2}"#), r#"{"_id":1,"b":2}"#);
        assert!(matches!(apply(r#"{"_id":1}"#, r#"{"_id":2,"b":2}"#), Err(Error::ImmutableId)));
        assert!(matches!(apply(r#"{"_id":1}"#, r#"{"$set":{"_id":2}}"#), Err(Error::ImmutableId)));
        assert_eq!(after(r#"{"_id":1}"#, r#"{"$set":{"_id":1.0}}"#), r#"{"_id":1.0}"#);
    }

    #[test]
    fn test_malformed_updates() {
        for u in [
            r#"{"$set":{"a":1},"b":2}"#,
            r#"{"$bogus":{"a":1}}"#,
            r#"{"$set":{"a":1},"$inc":{"a":1}}"#,
            r#"{"$set":{"a":1},"$unset":{"a.b":""}}"#,
            r#"{"$set":5}"#,
            r#"{"$x":1}"#,
        ] {
            assert!(Update::parse(&Document::parse(u).unwrap()).is_err(), "{u}");
        }
        assert!(Update::parse(&Document::parse(r#"{"a":{"$b":1}}"#).unwrap()).is_err());
    }

    #[test]
    fn test_upsert_documents_start_from_the_filters_equalities() {
        let filter = Filter::parse(&Document::parse(r#"{"sku":"x","a.b":1,"q":{"$gt":5}}"#).unwrap()).unwrap();
        let u = Update::parse(&Document::parse(r#"{"$inc":{"n":1},"$setOnInsert":{"new":true}}"#).unwrap()).unwrap();
        assert_eq!(u.upsert_document(&filter).unwrap().to_json(), r#"{"sku":"x","a":{"b":1},"n":1,"new":true}"#);
        // $setOnInsert only applies when inserting.
        assert_eq!(after(r#"{"a":1}"#, r#"{"$setOnInsert":{"b":2}}"#), r#"{"a":1}"#);
    }
}
