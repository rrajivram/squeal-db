//! What a find does with the documents a filter matched: sort them, and
//! keep only the fields a projection asks for.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::value::{Document, Value, compare, lookup};

/// `{price: -1, name: 1}`: fields in order, each ascending (1) or
/// descending (-1).
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Sort(pub Vec<(String, bool)>);

impl Sort {
    pub(crate) fn parse(doc: &Document) -> Result<Sort> {
        doc.iter()
            .map(|(path, dir)| match dir {
                Value::Int(1) => Ok((path.clone(), true)),
                Value::Int(-1) => Ok((path.clone(), false)),
                Value::Double(d) if *d == 1.0 => Ok((path.clone(), true)),
                Value::Double(d) if *d == -1.0 => Ok((path.clone(), false)),
                _ => Err(Error::BadValue(format!("sort direction for {path} must be 1 or -1"))),
            })
            .collect::<Result<_>>()
            .map(Sort)
    }

    pub(crate) fn compare(&self, a: &Document, b: &Document) -> Ordering {
        for (path, ascending) in &self.0 {
            let o = compare(&sort_key(a, path, *ascending), &sort_key(b, path, *ascending));
            let o = if *ascending { o } else { o.reverse() };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    }
}

// What a document sorts by on one field: its value, or for an array its
// smallest element ascending / largest descending (MongoDB's rule); a
// missing field sorts as null.
fn sort_key(doc: &Document, path: &str, ascending: bool) -> Value {
    let mut candidates: Vec<&Value> = vec![];
    for v in lookup(doc, path) {
        match v {
            Value::Array(items) if !items.is_empty() => candidates.extend(items),
            other => candidates.push(other),
        }
    }
    let pick = if ascending {
        candidates.into_iter().min_by(|x, y| compare(x, y))
    } else {
        candidates.into_iter().max_by(|x, y| compare(x, y))
    };
    pick.cloned().unwrap_or(Value::Null)
}

/// `{name: 1, "address.city": 1}` (only these, plus `_id` unless
/// `_id: 0`) or `{secret: 0}` (all but these).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Projection {
    include: bool,
    fields: Tree,
    id: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Tree(BTreeMap<String, Node>);

#[derive(Debug, Clone, PartialEq)]
enum Node {
    Whole,
    Part(Tree),
}

impl Projection {
    pub(crate) fn parse(doc: &Document) -> Result<Option<Projection>> {
        let mut include = None;
        let mut id = true;
        // `{_id: 1}` alone is a projection (just the _id), not no
        // projection: tell it apart from `{}`.
        let mut id_given = false;
        let mut fields = Tree::default();
        for (path, v) in doc.iter() {
            let on = match v {
                Value::Bool(b) => *b,
                Value::Int(i) => *i != 0,
                Value::Double(d) => *d != 0.0,
                _ => {
                    return Err(Error::BadValue(format!(
                        "projection for {path} must be 1/0 or true/false"
                    )));
                }
            };
            if path == "_id" {
                id = on;
                id_given = true;
                continue;
            }
            match include {
                Some(i) if i != on => {
                    return Err(Error::BadValue(
                        "a projection can't mix including and excluding fields (other than _id)"
                            .into(),
                    ));
                }
                _ => include = Some(on),
            }
            fields.add(path);
        }
        Ok(match include {
            None if id && !id_given => None,
            // `{_id: 1}`: include nothing but _id.
            None if id => Some(Projection { include: true, fields, id }),
            None => Some(Projection { include: false, fields, id }),
            Some(include) => Some(Projection { include, fields, id }),
        })
    }

    pub(crate) fn apply(&self, doc: &Document) -> Document {
        let mut out = if self.include {
            include(doc, &self.fields)
        } else {
            exclude(doc, &self.fields)
        };
        match (self.id, doc.get("_id")) {
            (true, Some(id)) if self.include => out.insert_first("_id", id.clone()),
            (false, _) => {
                out.remove("_id");
            }
            _ => {}
        }
        out
    }
}

impl Tree {
    fn add(&mut self, path: &str) {
        let (head, rest) = match path.split_once('.') {
            Some((h, r)) => (h, Some(r)),
            None => (path, None),
        };
        match rest {
            None => {
                self.0.insert(head.to_string(), Node::Whole);
            }
            Some(rest) => {
                let node = self.0.entry(head.to_string()).or_insert(Node::Part(Tree::default()));
                if let Node::Part(t) = node {
                    t.add(rest);
                }
            }
        }
    }
}

// Only the fields in `tree`, in the document's own order; through arrays,
// the named fields of each element that is a document.
fn include(doc: &Document, tree: &Tree) -> Document {
    let mut out = Document::new();
    for (k, v) in doc.iter() {
        match tree.0.get(k) {
            Some(Node::Whole) => out.insert(k.clone(), v.clone()),
            Some(Node::Part(sub)) => {
                if let Some(kept) = include_value(v, sub) {
                    out.insert(k.clone(), kept);
                }
            }
            None => {}
        }
    }
    out
}

fn include_value(v: &Value, sub: &Tree) -> Option<Value> {
    match v {
        Value::Document(d) => Some(Value::Document(include(d, sub))),
        Value::Array(items) => Some(Value::Array(
            items.iter().filter_map(|i| include_value(i, sub)).collect(),
        )),
        _ => None,
    }
}

fn exclude(doc: &Document, tree: &Tree) -> Document {
    let mut out = Document::new();
    for (k, v) in doc.iter() {
        match tree.0.get(k) {
            Some(Node::Whole) => {}
            Some(Node::Part(sub)) => out.insert(k.clone(), exclude_value(v, sub)),
            None => out.insert(k.clone(), v.clone()),
        }
    }
    out
}

fn exclude_value(v: &Value, sub: &Tree) -> Value {
    match v {
        Value::Document(d) => Value::Document(exclude(d, sub)),
        Value::Array(items) => Value::Array(items.iter().map(|i| exclude_value(i, sub)).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(p: &str, doc: &str) -> String {
        Projection::parse(&Document::parse(p).unwrap())
            .unwrap()
            .unwrap()
            .apply(&Document::parse(doc).unwrap())
            .to_json()
    }

    #[test]
    fn test_inclusion_exclusion_and_nested_paths() {
        let doc = r#"{"_id":1,"name":"x","a":{"b":1,"c":2},"items":[{"s":1,"t":2},{"s":3},5],"secret":9}"#;
        assert_eq!(project(r#"{"name":1}"#, doc), r#"{"_id":1,"name":"x"}"#);
        assert_eq!(project(r#"{"name":1,"_id":0}"#, doc), r#"{"name":"x"}"#);
        assert_eq!(project(r#"{"a.c":1,"items.s":1}"#, doc), r#"{"_id":1,"a":{"c":2},"items":[{"s":1},{"s":3}]}"#);
        assert_eq!(
            project(r#"{"secret":0,"a.b":0}"#, doc),
            r#"{"_id":1,"name":"x","a":{"c":2},"items":[{"s":1,"t":2},{"s":3},5]}"#
        );
        assert_eq!(project(r#"{"_id":0}"#, r#"{"_id":1,"a":2}"#), r#"{"a":2}"#);
        // `{_id: 1}` alone keeps just the _id, as MongoDB does.
        assert_eq!(project(r#"{"_id":1}"#, r#"{"_id":1,"a":2}"#), r#"{"_id":1}"#);
        assert!(Projection::parse(&Document::parse(r#"{"a":1,"b":0}"#).unwrap()).is_err());
        assert!(Projection::parse(&Document::parse(r#"{"a":1,"b":{"$slice":1}}"#).unwrap()).is_err());
        assert!(Projection::parse(&Document::parse(r#"{}"#).unwrap()).unwrap().is_none());
    }

    #[test]
    fn test_sort_by_several_fields_with_arrays_and_missing_values() {
        let mut docs: Vec<Document> = [
            r#"{"i":1,"a":3,"b":"x"}"#,
            r#"{"i":2,"a":1,"b":"z"}"#,
            r#"{"i":3,"a":1,"b":"y"}"#,
            r#"{"i":4}"#,
            r#"{"i":5,"a":[0,10]}"#,
        ]
        .iter()
        .map(|d| Document::parse(d).unwrap())
        .collect();
        let ids = |docs: &[Document]| docs.iter().map(|d| d.get("i").unwrap().to_json()).collect::<Vec<_>>().join(",");
        let sort = Sort::parse(&Document::parse(r#"{"a":1,"b":-1}"#).unwrap()).unwrap();
        docs.sort_by(|x, y| sort.compare(x, y));
        // missing (null) first; the array sorts by its smallest element, 0.
        assert_eq!(ids(&docs), "4,5,2,3,1");
        let sort = Sort::parse(&Document::parse(r#"{"a":-1}"#).unwrap()).unwrap();
        docs.sort_by(|x, y| sort.compare(x, y));
        // descending: the array by its largest, 10.
        assert_eq!(ids(&docs), "5,1,2,3,4");
        assert!(Sort::parse(&Document::parse(r#"{"a":2}"#).unwrap()).is_err());
    }
}
