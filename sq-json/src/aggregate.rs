//! Aggregation pipelines: stages that each turn a list of documents into
//! another — `$match $project $addFields/$set $unset $group $sort $skip
//! $limit $unwind $count $lookup $replaceRoot` — and the expressions they
//! compute with.
//!
//! Collection::aggregate hands the leading `$match`/`$sort`/`$skip`/
//! `$limit` stages to find, so they use indexes; the rest run here, over
//! documents held in memory.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::filter::Filter;
use crate::query::{Projection, Sort};
use crate::update::{get_path, set_path};
use crate::value::{Document, Value, compare, lookup, values_equal};

pub(crate) enum Stage {
    /// The filter, and the document it came from (for find).
    Match(Filter, Document),
    Project(Project),
    AddFields(Vec<(String, Expr)>),
    Unset(Projection),
    Group(Expr, Vec<(String, Acc, Expr)>),
    Sort(Sort, Document),
    Skip(usize),
    Limit(usize),
    Unwind {
        path: String,
        preserve: bool,
        index: Option<String>,
    },
    Count(String),
    Lookup {
        from: String,
        local: String,
        foreign: String,
        as_: String,
    },
    ReplaceRoot(Expr),
}

pub(crate) struct Project {
    /// Including: only these fields (None: only `_id`, if kept), then the
    /// computed ones. Excluding: all but these (None: everything).
    fields: Option<Projection>,
    including: bool,
    keep_id: bool,
    computed: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Acc {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Push,
    AddToSet,
    Count,
}

#[derive(Debug, Clone)]
pub(crate) enum Expr {
    Literal(Value),
    /// `$$ROOT` / `$$CURRENT` (then optionally a path within it).
    Root,
    Path(Vec<String>),
    Object(Vec<(String, Expr)>),
    Array(Vec<Expr>),
    Op(Op, Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
    Abs,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Cmp,
    And,
    Or,
    Not,
    Cond,
    IfNull,
    Concat,
    ToUpper,
    ToLower,
    Size,
    ArrayElemAt,
    In,
}

// ---- parsing ----

pub(crate) fn parse_pipeline(stages: &[Document]) -> Result<Vec<Stage>> {
    stages.iter().map(parse_stage).collect()
}

fn parse_stage(stage: &Document) -> Result<Stage> {
    let mut fields = stage.iter();
    let (Some((name, arg)), None) = (fields.next(), fields.next()) else {
        return Err(Error::BadValue("a pipeline stage must have exactly one field".into()));
    };
    let doc = |what: &str| match arg {
        Value::Document(d) => Ok(d),
        _ => Err(Error::BadValue(format!("{name} needs {what}"))),
    };
    let count = || match arg {
        Value::Int(n) if *n >= 0 => Ok(*n as usize),
        Value::Double(n) if *n >= 0.0 && n.fract() == 0.0 => Ok(*n as usize),
        _ => Err(Error::BadValue(format!("{name} needs a non-negative whole number"))),
    };
    Ok(match name.as_str() {
        "$match" => {
            let d = doc("a filter document")?;
            Stage::Match(Filter::parse(d)?, d.clone())
        }
        "$project" => Stage::Project(parse_project(doc("a document")?)?),
        "$addFields" | "$set" => Stage::AddFields(
            doc("a document")?
                .iter()
                .map(|(k, v)| Ok((check_field(k)?, parse_expr(v)?)))
                .collect::<Result<_>>()?,
        ),
        "$unset" => {
            let names = match arg {
                Value::String(s) => vec![s.clone()],
                Value::Array(items) => items
                    .iter()
                    .map(|i| match i {
                        Value::String(s) => Ok(s.clone()),
                        _ => Err(Error::BadValue("$unset needs field names".into())),
                    })
                    .collect::<Result<_>>()?,
                _ => return Err(Error::BadValue("$unset needs a field name or a list of them".into())),
            };
            let mut spec = Document::new();
            for n in names {
                spec.insert(n, Value::Int(0));
            }
            match Projection::parse(&spec)? {
                Some(p) => Stage::Unset(p),
                None => return Err(Error::BadValue("$unset needs a field name".into())),
            }
        }
        "$group" => {
            let d = doc("a document")?;
            let id = d
                .get("_id")
                .ok_or_else(|| Error::BadValue("$group needs an _id".into()))?;
            let mut accs = vec![];
            for (field, spec) in d.iter().filter(|(k, _)| *k != "_id") {
                let field = check_field(field)?;
                let Value::Document(spec) = spec else {
                    return Err(Error::BadValue(format!("$group field {field} needs an accumulator")));
                };
                let mut ops = spec.iter();
                let (Some((op, e)), None) = (ops.next(), ops.next()) else {
                    return Err(Error::BadValue(format!("$group field {field} needs one accumulator")));
                };
                let acc = match op.as_str() {
                    "$sum" => Acc::Sum,
                    "$avg" => Acc::Avg,
                    "$min" => Acc::Min,
                    "$max" => Acc::Max,
                    "$first" => Acc::First,
                    "$last" => Acc::Last,
                    "$push" => Acc::Push,
                    "$addToSet" => Acc::AddToSet,
                    "$count" => Acc::Count,
                    other => return Err(Error::BadValue(format!("unknown accumulator {other}"))),
                };
                accs.push((field, acc, parse_expr(e)?));
            }
            Stage::Group(parse_expr(id)?, accs)
        }
        "$sort" => {
            let d = doc("a sort document")?;
            if d.is_empty() {
                return Err(Error::BadValue("$sort needs at least one field".into()));
            }
            Stage::Sort(Sort::parse(d)?, d.clone())
        }
        "$skip" => Stage::Skip(count()?),
        "$limit" => match count()? {
            0 => return Err(Error::BadValue("$limit must be positive".into())),
            n => Stage::Limit(n),
        },
        "$unwind" => {
            let (path, preserve, index) = match arg {
                Value::String(p) => (p.clone(), false, None),
                Value::Document(d) => {
                    let path = match d.get("path") {
                        Some(Value::String(p)) => p.clone(),
                        _ => return Err(Error::BadValue("$unwind needs a path".into())),
                    };
                    let preserve = matches!(d.get("preserveNullAndEmptyArrays"), Some(Value::Bool(true)));
                    let index = match d.get("includeArrayIndex") {
                        Some(Value::String(i)) => Some(check_field(i)?),
                        None => None,
                        _ => return Err(Error::BadValue("includeArrayIndex needs a field name".into())),
                    };
                    (path, preserve, index)
                }
                _ => return Err(Error::BadValue("$unwind needs a path".into())),
            };
            let Some(path) = path.strip_prefix('$') else {
                return Err(Error::BadValue("$unwind's path must start with '$'".into()));
            };
            Stage::Unwind {
                path: check_field(path)?,
                preserve,
                index,
            }
        }
        "$count" => match arg {
            Value::String(s) if !s.is_empty() && !s.starts_with('$') && !s.contains('.') => {
                Stage::Count(s.clone())
            }
            _ => return Err(Error::BadValue("$count needs a plain field name".into())),
        },
        "$lookup" => {
            let d = doc("a document")?;
            let field = |k: &str| match d.get(k) {
                Some(Value::String(s)) => Ok(s.clone()),
                _ => Err(Error::BadValue(format!("$lookup needs {k}"))),
            };
            Stage::Lookup {
                from: field("from")?,
                local: field("localField")?,
                foreign: field("foreignField")?,
                as_: check_field(&field("as")?)?,
            }
        }
        "$replaceRoot" | "$replaceWith" => {
            let new_root = match (name.as_str(), arg) {
                ("$replaceRoot", Value::Document(d)) => d
                    .get("newRoot")
                    .ok_or_else(|| Error::BadValue("$replaceRoot needs newRoot".into()))?,
                ("$replaceRoot", _) => return Err(Error::BadValue("$replaceRoot needs a document".into())),
                _ => arg,
            };
            Stage::ReplaceRoot(parse_expr(new_root)?)
        }
        other => return Err(Error::BadValue(format!("unknown pipeline stage {other}"))),
    })
}

fn check_field(name: &str) -> Result<String> {
    if name.is_empty() || name.starts_with('$') || name.split('.').any(str::is_empty) {
        return Err(Error::BadValue(format!("bad field name '{name}'")));
    }
    Ok(name.to_string())
}

fn parse_project(spec: &Document) -> Result<Project> {
    let mut plain = Document::new();
    let mut computed = vec![];
    let mut keep_id = true;
    let (mut including, mut excluding) = (false, false);
    for (field, v) in spec.iter() {
        let flag = match v {
            Value::Bool(b) => Some(*b),
            Value::Int(i) => Some(*i != 0),
            Value::Double(d) => Some(*d != 0.0),
            _ => None,
        };
        match flag {
            Some(on) if field == "_id" => keep_id = on,
            Some(on) => {
                plain.insert(check_field(field)?, Value::Bool(on));
                if on { including = true } else { excluding = true }
            }
            None => {
                computed.push((check_field(field)?, parse_expr(v)?));
                if field == "_id" {
                    keep_id = false;
                } else {
                    including = true;
                }
            }
        }
    }
    if including && excluding {
        return Err(Error::BadValue("a $project can't mix including and excluding fields (other than _id)".into()));
    }
    if !keep_id && !computed.iter().any(|(f, _)| f == "_id") {
        plain.insert("_id", Value::Int(0));
    }
    let fields = if including && plain.iter().all(|(f, _)| f == "_id") {
        None
    } else {
        Projection::parse(&plain)?
    };
    Ok(Project { fields, including, keep_id, computed })
}

/// Parses an aggregation expression.
pub(crate) fn parse_expr(v: &Value) -> Result<Expr> {
    Ok(match v {
        Value::String(s) if s.starts_with("$$") => {
            let (var, rest) = match s[2..].split_once('.') {
                Some((var, rest)) => (var, Some(rest)),
                None => (&s[2..], None),
            };
            if var != "ROOT" && var != "CURRENT" {
                return Err(Error::BadValue(format!("unknown variable $${var}")));
            }
            match rest {
                Some(path) => Expr::Path(path.split('.').map(str::to_string).collect()),
                None => Expr::Root,
            }
        }
        Value::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            if path.is_empty() || path.split('.').any(str::is_empty) {
                return Err(Error::BadValue(format!("bad field path '{s}'")));
            }
            Expr::Path(path.split('.').map(str::to_string).collect())
        }
        Value::Array(items) => Expr::Array(items.iter().map(parse_expr).collect::<Result<_>>()?),
        Value::Document(d) if d.iter().any(|(k, _)| k.starts_with('$')) => {
            let mut fields = d.iter();
            let (Some((name, arg)), None) = (fields.next(), fields.next()) else {
                return Err(Error::BadValue("an expression object must have exactly one operator".into()));
            };
            parse_op(name, arg)?
        }
        Value::Document(d) => Expr::Object(
            d.iter()
                .map(|(k, v)| Ok((check_field(k)?, parse_expr(v)?)))
                .collect::<Result<_>>()?,
        ),
        other => Expr::Literal(other.clone()),
    })
}

fn parse_op(name: &str, arg: &Value) -> Result<Expr> {
    if name == "$literal" {
        return Ok(Expr::Literal(arg.clone()));
    }
    let op = match name {
        "$add" => Op::Add,
        "$subtract" => Op::Subtract,
        "$multiply" => Op::Multiply,
        "$divide" => Op::Divide,
        "$mod" => Op::Mod,
        "$abs" => Op::Abs,
        "$eq" => Op::Eq,
        "$ne" => Op::Ne,
        "$gt" => Op::Gt,
        "$gte" => Op::Gte,
        "$lt" => Op::Lt,
        "$lte" => Op::Lte,
        "$cmp" => Op::Cmp,
        "$and" => Op::And,
        "$or" => Op::Or,
        "$not" => Op::Not,
        "$cond" => Op::Cond,
        "$ifNull" => Op::IfNull,
        "$concat" => Op::Concat,
        "$toUpper" => Op::ToUpper,
        "$toLower" => Op::ToLower,
        "$size" => Op::Size,
        "$arrayElemAt" => Op::ArrayElemAt,
        "$in" => Op::In,
        other => return Err(Error::BadValue(format!("unknown expression operator {other}"))),
    };
    let args: Vec<Expr> = match (op, arg) {
        (Op::Cond, Value::Document(d)) => ["if", "then", "else"]
            .iter()
            .map(|k| {
                d.get(k)
                    .ok_or_else(|| Error::BadValue(format!("$cond needs '{k}'")))
                    .and_then(parse_expr)
            })
            .collect::<Result<_>>()?,
        (_, Value::Array(items)) => items.iter().map(parse_expr).collect::<Result<_>>()?,
        (_, other) => vec![parse_expr(other)?],
    };
    let arity = match op {
        Op::Abs | Op::Not | Op::ToUpper | Op::ToLower | Op::Size => Some(1),
        Op::Subtract | Op::Divide | Op::Mod | Op::Eq | Op::Ne | Op::Gt | Op::Gte | Op::Lt | Op::Lte
        | Op::Cmp | Op::ArrayElemAt | Op::In => Some(2),
        Op::Cond => Some(3),
        Op::IfNull if args.len() < 2 => return Err(Error::BadValue("$ifNull needs at least 2 arguments".into())),
        _ => None,
    };
    if let Some(n) = arity
        && args.len() != n
    {
        return Err(Error::BadValue(format!("{name} needs {n} argument(s), got {}", args.len())));
    }
    Ok(Expr::Op(op, args))
}

// ---- evaluation ----

/// The value of `e` for `doc`; None when it is missing (a path to nothing).
pub(crate) fn eval(e: &Expr, doc: &Document) -> Result<Option<Value>> {
    Ok(match e {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Root => Some(Value::Document(doc.clone())),
        Expr::Path(parts) => doc.get(&parts[0]).and_then(|v| path_value(v, &parts[1..])),
        Expr::Object(fields) => {
            let mut out = Document::new();
            for (k, e) in fields {
                if let Some(v) = eval(e, doc)? {
                    out.insert(k.clone(), v);
                }
            }
            Some(Value::Document(out))
        }
        Expr::Array(items) => Some(Value::Array(
            items
                .iter()
                .map(|e| Ok(eval(e, doc)?.unwrap_or(Value::Null)))
                .collect::<Result<_>>()?,
        )),
        Expr::Op(op, args) => eval_op(*op, args, doc)?,
    })
}

// A path in an expression: through an array, the values it reaches in each
// element, as an array.
fn path_value(v: &Value, parts: &[String]) -> Option<Value> {
    let Some(part) = parts.first() else {
        return Some(v.clone());
    };
    match v {
        Value::Document(d) => d.get(part).and_then(|x| path_value(x, &parts[1..])),
        Value::Array(items) => Some(Value::Array(
            items
                .iter()
                .filter(|i| matches!(i, Value::Document(_) | Value::Array(_)))
                .filter_map(|i| path_value(i, parts))
                .collect(),
        )),
        _ => None,
    }
}

fn truthy(v: &Option<Value>) -> bool {
    match v {
        None | Some(Value::Null) | Some(Value::Bool(false)) | Some(Value::Int(0)) => false,
        Some(Value::Double(d)) => *d != 0.0,
        _ => true,
    }
}

fn nullish(v: &Option<Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

fn eval_op(op: Op, args: &[Expr], doc: &Document) -> Result<Option<Value>> {
    let arg = |i: usize| eval(&args[i], doc);
    let all = || args.iter().map(|a| eval(a, doc)).collect::<Result<Vec<_>>>();
    let cmp = |a: &Option<Value>, b: &Option<Value>| {
        compare(a.as_ref().unwrap_or(&Value::Null), b.as_ref().unwrap_or(&Value::Null))
    };
    use std::cmp::Ordering::*;
    Ok(Some(match op {
        Op::Add | Op::Multiply => {
            let values = all()?;
            if values.iter().any(nullish) {
                return Ok(Some(Value::Null));
            }
            let mut acc = Value::Int(if op == Op::Add { 0 } else { 1 });
            for v in values.into_iter().flatten() {
                acc = arithmetic(op, &acc, &v)?;
            }
            acc
        }
        Op::Subtract | Op::Divide | Op::Mod => {
            let (a, b) = (arg(0)?, arg(1)?);
            if nullish(&a) || nullish(&b) {
                return Ok(Some(Value::Null));
            }
            arithmetic(op, &a.unwrap(), &b.unwrap())?
        }
        Op::Abs => match arg(0)? {
            None | Some(Value::Null) => Value::Null,
            Some(Value::Int(i)) => i.checked_abs().map(Value::Int).unwrap_or(Value::Double((i as f64).abs())),
            Some(Value::Double(d)) => Value::Double(d.abs()),
            Some(other) => return Err(Error::BadValue(format!("$abs needs a number, not {}", other.to_json()))),
        },
        Op::Eq => Value::Bool(cmp(&arg(0)?, &arg(1)?) == Equal),
        Op::Ne => Value::Bool(cmp(&arg(0)?, &arg(1)?) != Equal),
        Op::Gt => Value::Bool(cmp(&arg(0)?, &arg(1)?) == Greater),
        Op::Gte => Value::Bool(cmp(&arg(0)?, &arg(1)?) != Less),
        Op::Lt => Value::Bool(cmp(&arg(0)?, &arg(1)?) == Less),
        Op::Lte => Value::Bool(cmp(&arg(0)?, &arg(1)?) != Greater),
        Op::Cmp => Value::Int(match cmp(&arg(0)?, &arg(1)?) {
            Less => -1,
            Equal => 0,
            Greater => 1,
        }),
        Op::And => {
            for a in args {
                if !truthy(&eval(a, doc)?) {
                    return Ok(Some(Value::Bool(false)));
                }
            }
            Value::Bool(true)
        }
        Op::Or => {
            for a in args {
                if truthy(&eval(a, doc)?) {
                    return Ok(Some(Value::Bool(true)));
                }
            }
            Value::Bool(false)
        }
        Op::Not => Value::Bool(!truthy(&arg(0)?)),
        Op::Cond => return if truthy(&arg(0)?) { arg(1) } else { arg(2) },
        Op::IfNull => {
            for a in &args[..args.len() - 1] {
                let v = eval(a, doc)?;
                if !nullish(&v) {
                    return Ok(v);
                }
            }
            return eval(&args[args.len() - 1], doc);
        }
        Op::Concat => {
            let mut out = String::new();
            for v in all()? {
                match v {
                    None | Some(Value::Null) => return Ok(Some(Value::Null)),
                    Some(Value::String(s)) => out.push_str(&s),
                    Some(other) => {
                        return Err(Error::BadValue(format!("$concat needs strings, not {}", other.to_json())));
                    }
                }
            }
            Value::String(out)
        }
        Op::ToUpper | Op::ToLower => match arg(0)? {
            None | Some(Value::Null) => Value::String(String::new()),
            Some(Value::String(s)) if op == Op::ToUpper => Value::String(s.to_uppercase()),
            Some(Value::String(s)) => Value::String(s.to_lowercase()),
            Some(other) => {
                return Err(Error::BadValue(format!("$toUpper/$toLower need a string, not {}", other.to_json())));
            }
        },
        Op::Size => match arg(0)? {
            Some(Value::Array(items)) => Value::Int(items.len() as i64),
            _ => return Err(Error::BadValue("$size needs an array".into())),
        },
        Op::ArrayElemAt => match (arg(0)?, arg(1)?) {
            (Some(Value::Array(items)), Some(Value::Int(i))) => {
                let i = if i < 0 { items.len() as i64 + i } else { i };
                return Ok(usize::try_from(i).ok().and_then(|i| items.get(i).cloned()));
            }
            (a, _) if nullish(&a) => Value::Null,
            _ => return Err(Error::BadValue("$arrayElemAt needs an array and an integer".into())),
        },
        Op::In => match arg(1)? {
            Some(Value::Array(items)) => {
                let x = arg(0)?.unwrap_or(Value::Null);
                Value::Bool(items.iter().any(|i| values_equal(i, &x)))
            }
            _ => return Err(Error::BadValue("$in needs an array as its second argument".into())),
        },
    }))
}

fn arithmetic(op: Op, a: &Value, b: &Value) -> Result<Value> {
    use Value::*;
    let float = |v: &Value| match v {
        Int(i) => Some(*i as f64),
        Double(d) => Some(*d),
        _ => None,
    };
    let bad = || {
        Error::BadValue(format!("can't apply {op:?} to {} and {}", a.to_json(), b.to_json()))
    };
    Ok(match (op, a, b) {
        (Op::Add, Date(d), n) | (Op::Add, n, Date(d)) => match n {
            Int(i) => Date(d + i),
            Double(x) => Date(d + x.round() as i64),
            _ => return Err(bad()),
        },
        (Op::Subtract, Date(x), Date(y)) => Int(x - y),
        (Op::Subtract, Date(d), Int(i)) => Date(d - i),
        (Op::Subtract, Date(d), Double(x)) => Date(d - x.round() as i64),
        (Op::Add, Int(x), Int(y)) => x.checked_add(*y).map(Int).unwrap_or(Double(*x as f64 + *y as f64)),
        (Op::Subtract, Int(x), Int(y)) => x.checked_sub(*y).map(Int).unwrap_or(Double(*x as f64 - *y as f64)),
        (Op::Multiply, Int(x), Int(y)) => x.checked_mul(*y).map(Int).unwrap_or(Double(*x as f64 * *y as f64)),
        (Op::Mod, Int(_), Int(0)) | (Op::Divide, _, Int(0)) => {
            return Err(Error::BadValue("can't divide by zero".into()));
        }
        (Op::Mod, Int(x), Int(y)) => Int(x.wrapping_rem(*y)),
        (Op::Divide, _, Double(y)) if *y == 0.0 => return Err(Error::BadValue("can't divide by zero".into())),
        _ => {
            let (x, y) = (float(a).ok_or_else(bad)?, float(b).ok_or_else(bad)?);
            Double(match op {
                Op::Add => x + y,
                Op::Subtract => x - y,
                Op::Multiply => x * y,
                Op::Divide => x / y,
                Op::Mod => x % y,
                _ => unreachable!("arithmetic operators only"),
            })
        }
    })
}

// ---- running ----

/// Runs `stages` over `docs`. `lookup(from, filter)` finds documents in
/// another collection of the same database (for `$lookup`).
pub(crate) fn run(
    stages: &[Stage],
    mut docs: Vec<Document>,
    lookup_in: &mut dyn FnMut(&str, Document) -> Result<Vec<Document>>,
) -> Result<Vec<Document>> {
    for stage in stages {
        docs = match stage {
            Stage::Match(filter, _) => docs.into_iter().filter(|d| filter.matches(d)).collect(),
            Stage::Project(p) => docs.iter().map(|d| project(p, d)).collect::<Result<_>>()?,
            Stage::AddFields(fields) => docs
                .into_iter()
                .map(|mut d| {
                    let values = fields
                        .iter()
                        .map(|(_, e)| eval(e, &d))
                        .collect::<Result<Vec<_>>>()?;
                    for ((path, _), v) in fields.iter().zip(values) {
                        if let Some(v) = v {
                            set_path(&mut d, path, v)?;
                        }
                    }
                    Ok(d)
                })
                .collect::<Result<_>>()?,
            Stage::Unset(p) => docs.iter().map(|d| p.apply(d)).collect(),
            Stage::Group(id, accs) => group(id, accs, &docs)?,
            Stage::Sort(sort, _) => {
                docs.sort_by(|a, b| sort.compare(a, b));
                docs
            }
            Stage::Skip(n) => docs.into_iter().skip(*n).collect(),
            Stage::Limit(n) => docs.into_iter().take(*n).collect(),
            Stage::Unwind { path, preserve, index } => unwind(docs, path, *preserve, index.as_deref())?,
            Stage::Count(field) => {
                if docs.is_empty() {
                    vec![]
                } else {
                    let mut d = Document::new();
                    d.insert(field.clone(), Value::Int(docs.len() as i64));
                    vec![d]
                }
            }
            Stage::Lookup { from, local, foreign, as_ } => {
                let mut out = Vec::with_capacity(docs.len());
                for mut d in docs {
                    let mut keys = vec![];
                    for v in lookup(&d, local) {
                        match v {
                            Value::Array(items) => keys.extend(items.iter().cloned()),
                            other => keys.push(other.clone()),
                        }
                    }
                    if keys.is_empty() {
                        keys.push(Value::Null);
                    }
                    let mut cond = Document::new();
                    cond.insert("$in", Value::Array(keys));
                    let mut filter = Document::new();
                    filter.insert(foreign.clone(), Value::Document(cond));
                    let found = lookup_in(from, filter)?;
                    set_path(&mut d, as_, Value::Array(found.into_iter().map(Value::Document).collect()))?;
                    out.push(d);
                }
                out
            }
            Stage::ReplaceRoot(e) => docs
                .iter()
                .map(|d| match eval(e, d)? {
                    Some(Value::Document(new)) => Ok(new),
                    other => Err(Error::BadValue(format!(
                        "the new root must be a document, not {}",
                        other.map(|v| v.to_json()).unwrap_or("missing".into())
                    ))),
                })
                .collect::<Result<_>>()?,
        };
    }
    Ok(docs)
}

fn project(p: &Project, doc: &Document) -> Result<Document> {
    let mut out = match &p.fields {
        Some(fields) => fields.apply(doc),
        None if !p.including => doc.clone(),
        None => {
            let mut out = Document::new();
            if p.keep_id
                && let Some(id) = doc.get("_id")
            {
                out.insert("_id", id.clone());
            }
            out
        }
    };
    for (path, e) in &p.computed {
        if let Some(v) = eval(e, doc)? {
            set_path(&mut out, path, v)?;
            if path == "_id" {
                let id = out.remove("_id").expect("just set");
                out.insert_first("_id", id);
            }
        }
    }
    Ok(out)
}

fn unwind(docs: Vec<Document>, path: &str, preserve: bool, index: Option<&str>) -> Result<Vec<Document>> {
    let mut out = vec![];
    for doc in docs {
        match get_path(&doc, path).cloned() {
            Some(Value::Array(items)) if !items.is_empty() => {
                for (i, item) in items.into_iter().enumerate() {
                    let mut d = doc.clone();
                    set_path(&mut d, path, item)?;
                    if let Some(f) = index {
                        set_path(&mut d, f, Value::Int(i as i64))?;
                    }
                    out.push(d);
                }
            }
            // A value that isn't an array unwinds to itself.
            Some(v) if !matches!(v, Value::Null | Value::Array(_)) => {
                let mut d = doc;
                if let Some(f) = index {
                    set_path(&mut d, f, Value::Null)?;
                }
                out.push(d);
            }
            _ if preserve => {
                let mut d = doc;
                if let Some(f) = index {
                    set_path(&mut d, f, Value::Null)?;
                }
                out.push(d);
            }
            _ => {}
        }
    }
    Ok(out)
}

// ---- $group ----

enum State {
    Sum(Value),
    Avg(f64, usize),
    Pick(Option<Value>),
    List(Vec<Value>),
    Count(i64),
}

fn group(id: &Expr, accs: &[(String, Acc, Expr)], docs: &[Document]) -> Result<Vec<Document>> {
    // Groups in the order first seen, found by their key's canonical bytes
    // (so 1 and 1.0 are one group, as MongoDB compares them equal).
    let mut groups: Vec<(Value, Vec<State>)> = vec![];
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    for doc in docs {
        let key = eval(id, doc)?.unwrap_or(Value::Null);
        let bytes = postcard::to_allocvec(&canonical(&key))?;
        let g = *index.entry(bytes).or_insert_with(|| {
            groups.push((key, accs.iter().map(|(_, acc, _)| start(*acc)).collect()));
            groups.len() - 1
        });
        for ((_, acc, e), state) in accs.iter().zip(groups[g].1.iter_mut()) {
            let v = eval(e, doc)?;
            accumulate(*acc, state, v)?;
        }
    }
    Ok(groups
        .into_iter()
        .map(|(key, states)| {
            let mut out = Document::new();
            out.insert("_id", key);
            for ((field, _, _), state) in accs.iter().zip(states) {
                out.insert(field.clone(), finish(state));
            }
            out
        })
        .collect())
}

// Numbers equal under MongoDB's comparison made identical.
fn canonical(v: &Value) -> Value {
    match v {
        Value::Double(d) if d.fract() == 0.0 && d.abs() < 9.0e15 => Value::Int(*d as i64),
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        Value::Document(d) => {
            let mut out = Document::new();
            for (k, v) in d.iter() {
                out.insert(k.clone(), canonical(v));
            }
            Value::Document(out)
        }
        other => other.clone(),
    }
}

fn start(acc: Acc) -> State {
    match acc {
        Acc::Sum => State::Sum(Value::Int(0)),
        Acc::Avg => State::Avg(0.0, 0),
        Acc::Min | Acc::Max | Acc::First | Acc::Last => State::Pick(None),
        Acc::Push | Acc::AddToSet => State::List(vec![]),
        Acc::Count => State::Count(0),
    }
}

fn accumulate(acc: Acc, state: &mut State, v: Option<Value>) -> Result<()> {
    let number = |v: &Option<Value>| matches!(v, Some(Value::Int(_) | Value::Double(_)));
    match (acc, state) {
        (Acc::Sum, State::Sum(total)) => {
            if number(&v) {
                *total = arithmetic(Op::Add, total, &v.unwrap())?;
            }
        }
        (Acc::Avg, State::Avg(total, n)) => match v {
            Some(Value::Int(i)) => {
                *total += i as f64;
                *n += 1;
            }
            Some(Value::Double(d)) => {
                *total += d;
                *n += 1;
            }
            _ => {}
        },
        (Acc::Min | Acc::Max, State::Pick(best)) => {
            if !nullish(&v) {
                let v = v.unwrap();
                let wanted = if acc == Acc::Min { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
                if best.as_ref().is_none_or(|b| compare(&v, b) == wanted) {
                    *best = Some(v);
                }
            }
        }
        (Acc::First, State::Pick(first)) => {
            if first.is_none() {
                *first = Some(v.unwrap_or(Value::Null));
            }
        }
        (Acc::Last, State::Pick(last)) => *last = Some(v.unwrap_or(Value::Null)),
        (Acc::Push, State::List(items)) => items.extend(v),
        (Acc::AddToSet, State::List(items)) => {
            if let Some(v) = v
                && !items.iter().any(|i| values_equal(i, &v))
            {
                items.push(v);
            }
        }
        (Acc::Count, State::Count(n)) => *n += 1,
        _ => unreachable!("each accumulator keeps its own state"),
    }
    Ok(())
}

fn finish(state: State) -> Value {
    match state {
        State::Sum(v) => v,
        State::Avg(_, 0) => Value::Null,
        State::Avg(total, n) => Value::Double(total / n as f64),
        State::Pick(v) => v.unwrap_or(Value::Null),
        State::List(items) => Value::Array(items),
        State::Count(n) => Value::Int(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(pipeline: &str, docs: &[&str]) -> Result<String> {
        let Value::Array(stages) = crate::value::from_json(pipeline)? else { panic!("a list") };
        let stages: Vec<Document> = stages
            .into_iter()
            .map(|s| match s {
                Value::Document(d) => d,
                _ => panic!("stages are documents"),
            })
            .collect();
        let stages = parse_pipeline(&stages)?;
        let docs = docs.iter().map(|d| Document::parse(d).unwrap()).collect();
        let out = run(&stages, docs, &mut |_, _| Ok(vec![]))?;
        Ok(out.iter().map(Document::to_json).collect::<Vec<_>>().join(","))
    }

    const SALES: [&str; 5] = [
        r#"{"_id":1,"item":"pen","qty":2,"price":1.5,"tags":["a","b"]}"#,
        r#"{"_id":2,"item":"ink","qty":1,"price":10}"#,
        r#"{"_id":3,"item":"pen","qty":5,"price":1.5,"tags":[]}"#,
        r#"{"_id":4,"item":"pad","qty":3,"price":4,"tags":["b"]}"#,
        r#"{"_id":5,"item":"pen","price":2}"#,
    ];

    #[test]
    fn test_group_with_accumulators() {
        let out = agg(
            r#"[{"$group": {"_id": "$item", "n": {"$count": {}}, "qty": {"$sum": "$qty"}, "avg": {"$avg": "$qty"},
                "hi": {"$max": "$price"}, "first": {"$first": "$_id"}, "ids": {"$push": "$_id"},
                "prices": {"$addToSet": "$price"}}},
                {"$sort": {"_id": 1}}]"#,
            &SALES,
        )
        .unwrap();
        assert_eq!(
            out,
            [
                r#"{"_id":"ink","n":1,"qty":1,"avg":1.0,"hi":10,"first":2,"ids":[2],"prices":[10]}"#,
                r#"{"_id":"pad","n":1,"qty":3,"avg":3.0,"hi":4,"first":4,"ids":[4],"prices":[4]}"#,
                r#"{"_id":"pen","n":3,"qty":7,"avg":3.5,"hi":2,"first":1,"ids":[1,3,5],"prices":[1.5,2]}"#,
            ]
            .join(",")
        );
        // Group keys equal as numbers are one group; a null key groups all.
        let out = agg(r#"[{"$group": {"_id": null, "total": {"$sum": {"$multiply": ["$qty", "$price"]}}}}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"_id":null,"total":32.5}"#);
        let out = agg(r#"[{"$group": {"_id": "$k", "n": {"$sum": 1}}}]"#, &[r#"{"k":1}"#, r#"{"k":1.0}"#]).unwrap();
        assert_eq!(out, r#"{"_id":1,"n":2}"#);
    }

    #[test]
    fn test_project_add_fields_unwind_count() {
        let out = agg(
            r#"[{"$match": {"qty": {"$gte": 2}}},
                {"$project": {"item": 1, "total": {"$multiply": ["$qty", "$price"]}, "big": {"$gt": ["$qty", 2]}}}]"#,
            &SALES,
        )
        .unwrap();
        assert_eq!(
            out,
            r#"{"_id":1,"item":"pen","total":3.0,"big":false},{"_id":3,"item":"pen","total":7.5,"big":true},{"_id":4,"item":"pad","total":12,"big":true}"#
        );
        let out = agg(r#"[{"$project": {"_id": 0, "tags": 0, "price": 0}}, {"$limit": 2}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"item":"pen","qty":2},{"item":"ink","qty":1}"#);
        let out = agg(r#"[{"$project": {"_id": "$item", "q": {"$ifNull": ["$qty", 0]}}}, {"$skip": 3}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"_id":"pad","q":3},{"_id":"pen","q":0}"#);
        let out = agg(r#"[{"$unwind": "$tags"}, {"$project": {"tags": 1}}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"_id":1,"tags":"a"},{"_id":1,"tags":"b"},{"_id":4,"tags":"b"}"#);
        let out = agg(
            r#"[{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "i"}},
                {"$project": {"tags": 1, "i": 1}}, {"$skip": 2}]"#,
            &SALES,
        )
        .unwrap();
        assert_eq!(out, r#"{"_id":2,"i":null},{"_id":3,"tags":[],"i":null},{"_id":4,"tags":"b","i":0},{"_id":5,"i":null}"#);
        let out = agg(r#"[{"$set": {"label": {"$concat": ["$item", "-", {"$toUpper": "$item"}]}}}, {"$unset": ["tags", "qty", "price"]}, {"$limit": 1}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"_id":1,"item":"pen","label":"pen-PEN"}"#);
        assert_eq!(agg(r#"[{"$match": {"item": "pen"}}, {"$count": "pens"}]"#, &SALES).unwrap(), r#"{"pens":3}"#);
        assert_eq!(agg(r#"[{"$match": {"item": "x"}}, {"$count": "n"}]"#, &SALES).unwrap(), "");
        let out = agg(r#"[{"$replaceRoot": {"newRoot": {"x": "$item", "c": {"$cond": {"if": "$tags", "then": "yes", "else": "no"}}}}}, {"$limit": 2}]"#, &SALES).unwrap();
        assert_eq!(out, r#"{"x":"pen","c":"yes"},{"x":"ink","c":"no"}"#);
    }

    #[test]
    fn test_expressions() {
        let e = |expr: &str, doc: &str| {
            let v = eval(&parse_expr(&crate::value::from_json(expr).unwrap()).unwrap(), &Document::parse(doc).unwrap());
            v.map(|v| v.map(|v| v.to_json()).unwrap_or("missing".into()))
        };
        assert_eq!(e(r#""$a.b""#, r#"{"a":[{"b":1},{"c":2},{"b":3}]}"#).unwrap(), "[1,3]");
        assert_eq!(e(r#""$nope""#, "{}").unwrap(), "missing");
        assert_eq!(e(r#"{"$add": [1, 2.5, "$x"]}"#, r#"{"x":1}"#).unwrap(), "4.5");
        assert_eq!(e(r#"{"$add": [1, "$nope"]}"#, "{}").unwrap(), "null");
        assert_eq!(e(r#"{"$subtract": ["$d", {"$date": 0}]}"#, r#"{"d":{"$date":5000}}"#).unwrap(), "5000");
        assert_eq!(e(r#"{"$divide": [7, 2]}"#, "{}").unwrap(), "3.5");
        assert_eq!(e(r#"{"$mod": [7, 3]}"#, "{}").unwrap(), "1");
        assert!(e(r#"{"$divide": [1, 0]}"#, "{}").is_err());
        assert_eq!(e(r#"{"$cmp": ["a", 1]}"#, "{}").unwrap(), "1");
        assert_eq!(e(r#"{"$and": [1, "$x", {"$not": [false]}]}"#, r#"{"x":[]}"#).unwrap(), "true");
        assert_eq!(e(r#"{"$size": "$a"}"#, r#"{"a":[1,2]}"#).unwrap(), "2");
        assert_eq!(e(r#"{"$arrayElemAt": ["$a", -1]}"#, r#"{"a":[1,2]}"#).unwrap(), "2");
        assert_eq!(e(r#"{"$in": ["$x", [1, 2]]}"#, r#"{"x":2.0}"#).unwrap(), "true");
        assert_eq!(e(r#"{"$literal": "$x"}"#, "{}").unwrap(), r#""$x""#);
        assert_eq!(e(r#"{"k": "$x", "m": "$nope"}"#, r#"{"x":1}"#).unwrap(), r#"{"k":1}"#);
        assert_eq!(e(r#""$$ROOT.x""#, r#"{"x":1}"#).unwrap(), "1");
        for bad in [r#"{"$nope": 1}"#, r#"{"$size": [1, 2]}"#, r#"{"$add": 1, "$x": 2}"#, r#""$""#, r#""$$var""#] {
            assert!(parse_expr(&crate::value::from_json(bad).unwrap()).is_err(), "{bad}");
        }
        for bad in [r#"[{"$nope": {}}]"#, r#"[{"$limit": 0}]"#, r#"[{"$group": {}}]"#, r#"[{"$project": {"a": 1, "b": 0}}]"#, r#"[{"$unwind": "tags"}]"#] {
            assert!(agg(bad, &[]).is_err(), "{bad}");
        }
    }
}
