//! Documents and the values in them: JSON's types plus MongoDB's ObjectId
//! and Date, with field order kept as written.
//!
//! Two encodings: `serde` derive (postcard) for storing a document in the
//! store, and MongoDB's extended JSON for the API (see `from_json` and
//! `Value::to_json`) — `{"$oid": "..."}`, `{"$date": "..."}` and friends —
//! so ObjectIds and dates survive a trip through JSON text.

use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    ObjectId(ObjectId),
    /// Milliseconds since the Unix epoch, UTC.
    Date(i64),
    Array(Vec<Value>),
    Document(Document),
}

/// A document: fields in the order they were written, as MongoDB keeps
/// them. Documents are small, so fields are looked up by a linear scan.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Document(Vec<(String, Value)>);

impl Document {
    pub fn new() -> Self {
        Self(vec![])
    }

    /// Parses a JSON object (extended JSON — see the module comment).
    pub fn parse(json: &str) -> Result<Document, Error> {
        match from_json(json)? {
            Value::Document(d) => Ok(d),
            other => Err(Error::BadValue(format!(
                "expected a JSON object, got {}",
                other.to_json()
            ))),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.0.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Sets `key`, in place if present, else appended.
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        let key = key.into();
        match self.get_mut(&key) {
            Some(v) => *v = value,
            None => self.0.push((key, value)),
        }
    }

    /// Sets `key` as the first field (where MongoDB keeps `_id`).
    pub fn insert_first(&mut self, key: impl Into<String>, value: Value) {
        let key = key.into();
        self.remove(&key);
        self.0.insert(0, (key, value));
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let i = self.0.iter().position(|(k, _)| k == key)?;
        Some(self.0.remove(i).1)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_json(&self) -> String {
        Value::Document(self.clone()).to_json()
    }

    pub(crate) fn fields_mut(&mut self) -> &mut Vec<(String, Value)> {
        &mut self.0
    }
}

impl fmt::Display for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json())
    }
}

/// MongoDB's 12-byte identifier: 4 bytes of seconds since the epoch, 5 of
/// per-process randomness, 3 of a counter — unique and roughly increasing
/// in creation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectId(pub [u8; 12]);

static OID_COUNTER: AtomicU32 = AtomicU32::new(0);

impl ObjectId {
    pub fn new() -> Self {
        let secs = store::clock::SystemTime::now()
            .duration_since(store::clock::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        let random = process_random();
        let count = OID_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let mut b = [0u8; 12];
        b[..4].copy_from_slice(&secs.to_be_bytes());
        b[4..9].copy_from_slice(&random[..5]);
        b[9..].copy_from_slice(&count.to_be_bytes()[1..]);
        ObjectId(b)
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn parse(hex: &str) -> Result<Self, Error> {
        let bad = || Error::BadValue(format!("invalid ObjectId {hex:?}: need 24 hex digits"));
        if hex.len() != 24 {
            return Err(bad());
        }
        let mut b = [0u8; 12];
        for (i, byte) in b.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| bad())?;
        }
        Ok(ObjectId(b))
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

// Per-process random bytes, from the standard library's randomly seeded
// hasher (no extra dependency needed for 5 bytes of entropy), mixed with
// the process id and the time — wasm32 has no process ids (asking panics)
// and no OS randomness behind that hasher, so there the time is what
// tells two page loads apart.
fn process_random() -> [u8; 8] {
    use std::hash::{BuildHasher, Hasher};
    static RANDOM: std::sync::OnceLock<[u8; 8]> = std::sync::OnceLock::new();
    *RANDOM.get_or_init(|| {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        #[cfg(not(target_arch = "wasm32"))]
        h.write_u64(std::process::id() as u64);
        let nanos = store::clock::SystemTime::now()
            .duration_since(store::clock::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        h.write_u128(nanos);
        h.finish().to_be_bytes()
    })
}

// ---- comparison: MongoDB's BSON order ----

/// A value's type bracket in MongoDB's comparison order: values of
/// different brackets compare by bracket alone (null < numbers < strings <
/// objects < arrays < ObjectId < booleans < dates), and a comparison
/// operator only ever matches values of its operand's bracket.
pub(crate) fn type_order(v: &Value) -> u8 {
    match v {
        Value::Null => 2,
        Value::Int(_) | Value::Double(_) => 3,
        Value::String(_) => 4,
        Value::Document(_) => 5,
        Value::Array(_) => 6,
        Value::ObjectId(_) => 8,
        Value::Bool(_) => 9,
        Value::Date(_) => 10,
    }
}

/// MongoDB's total order over values: by type bracket, then within it —
/// numbers by exact value (5 == 5.0; NaN below every other number), strings
/// bytewise, documents field by field, arrays element by element.
pub fn compare(a: &Value, b: &Value) -> Ordering {
    let (ta, tb) = (type_order(a), type_order(b));
    if ta != tb {
        return ta.cmp(&tb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::String(x), Value::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::ObjectId(x), Value::ObjectId(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                match compare(p, q) {
                    Ordering::Equal => {}
                    o => return o,
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Document(x), Value::Document(y)) => {
            for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
                match compare(vx, vy)
                    .then_with(|| kx.as_bytes().cmp(ky.as_bytes()))
                {
                    Ordering::Equal => {}
                    o => return o,
                }
            }
            x.len().cmp(&y.len())
        }
        (x, y) => compare_numbers(x, y),
    }
}

fn compare_numbers(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Double(x), Value::Double(y)) => cmp_f64(*x, *y),
        (Value::Int(i), Value::Double(d)) => cmp_int_double(*i, *d),
        (Value::Double(d), Value::Int(i)) => cmp_int_double(*i, *d).reverse(),
        _ => unreachable!("compare_numbers is only called on numbers"),
    }
}

// NaN below every number, equal to itself (MongoDB's sort order).
fn cmp_f64(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => x.partial_cmp(&y).expect("neither is NaN"),
    }
}

// Exact: no rounding of the integer to f64 (which loses precision past 2^53).
fn cmp_int_double(i: i64, d: f64) -> Ordering {
    if d.is_nan() {
        return Ordering::Greater;
    }
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if d >= TWO_63 {
        return Ordering::Less;
    }
    if d < -TWO_63 {
        return Ordering::Greater;
    }
    let whole = d.trunc();
    match i.cmp(&(whole as i64)) {
        Ordering::Equal => {
            let frac = d - whole;
            if frac > 0.0 {
                Ordering::Less
            } else if frac < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        o => o,
    }
}

pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    compare(a, b) == Ordering::Equal
}

// ---- dotted paths ----

/// Every value `path` ("a.b.c") reaches in `doc`, the way MongoDB resolves
/// one: a numeric component indexes an array; any other component reaching
/// an array applies to each element that is a document. Nothing when the
/// path is missing. (A terminal array is returned whole; matching looks
/// inside it — see filter.rs.)
pub fn lookup<'a>(doc: &'a Document, path: &str) -> Vec<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = vec![];
    walk_doc(doc, &parts, &mut out);
    out
}

fn walk_doc<'a>(doc: &'a Document, parts: &[&str], out: &mut Vec<&'a Value>) {
    if let Some(v) = doc.get(parts[0]) {
        walk(v, &parts[1..], out);
    }
}

fn walk<'a>(v: &'a Value, parts: &[&str], out: &mut Vec<&'a Value>) {
    let Some(part) = parts.first() else {
        out.push(v);
        return;
    };
    match v {
        Value::Document(d) => walk_doc(d, parts, out),
        Value::Array(items) => {
            if let Ok(i) = part.parse::<usize>()
                && let Some(item) = items.get(i)
            {
                walk(item, &parts[1..], out);
            }
            for item in items {
                if let Value::Document(d) = item {
                    walk_doc(d, parts, out);
                }
            }
        }
        _ => {}
    }
}

// ---- extended JSON ----

/// Parses JSON text into a Value, reading MongoDB's extended JSON forms:
/// `{"$oid": "<24 hex>"}`, `{"$date": <ms> | "<ISO-8601>" | {"$numberLong":
/// "<ms>"}}`, `{"$numberLong": "<int>"}`, `{"$numberInt": "<int>"}`,
/// `{"$numberDouble": "<float>|NaN|Infinity|-Infinity"}`.
pub fn from_json(json: &str) -> Result<Value, Error> {
    let raw: Raw = serde_json::from_str(json).map_err(|e| Error::Json(e.to_string()))?;
    raw.into_value()
}

// JSON as parsed, fields in order: serde_json's own Value would sort them.
enum Raw {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    Array(Vec<Raw>),
    Object(Vec<(String, Raw)>),
}

impl Raw {
    fn into_value(self) -> Result<Value, Error> {
        Ok(match self {
            Raw::Null => Value::Null,
            Raw::Bool(b) => Value::Bool(b),
            Raw::Int(i) => Value::Int(i),
            Raw::Double(d) => Value::Double(d),
            Raw::String(s) => Value::String(s),
            Raw::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(Raw::into_value)
                    .collect::<Result<_, _>>()?,
            ),
            Raw::Object(fields) => {
                if let [(key, value)] = fields.as_slice()
                    && key.starts_with('$')
                    && let Some(v) = extended(key, value)?
                {
                    return Ok(v);
                }
                let mut doc = Document::new();
                for (k, v) in fields {
                    doc.fields_mut().push((k, v.into_value()?));
                }
                Value::Document(doc)
            }
        })
    }
}

// One extended-JSON wrapper, or None when `key` isn't one (so the object is
// an ordinary document — or a query operator, for the query to judge).
fn extended(key: &str, value: &Raw) -> Result<Option<Value>, Error> {
    let bad = || Error::BadValue(format!("invalid {key} value"));
    let text = |v: &Raw| match v {
        Raw::String(s) => Some(s.clone()),
        _ => None,
    };
    Ok(Some(match key {
        "$oid" => Value::ObjectId(ObjectId::parse(&text(value).ok_or_else(bad)?)?),
        "$date" => Value::Date(match value {
            Raw::Int(ms) => *ms,
            Raw::Double(ms) => *ms as i64,
            Raw::String(s) => parse_iso_date(s)?,
            Raw::Object(f) => match f.as_slice() {
                [(k, Raw::String(s))] if k == "$numberLong" => s.parse().map_err(|_| bad())?,
                _ => return Err(bad()),
            },
            _ => return Err(bad()),
        }),
        "$numberLong" | "$numberInt" => {
            Value::Int(text(value).ok_or_else(bad)?.parse().map_err(|_| bad())?)
        }
        "$numberDouble" => {
            let s = text(value).ok_or_else(bad)?;
            Value::Double(match s.as_str() {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                other => other.parse().map_err(|_| bad())?,
            })
        }
        _ => return Ok(None),
    }))
}

impl<'de> Deserialize<'de> for Raw {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct RawVisitor;
        impl<'de> Visitor<'de> for RawVisitor {
            type Value = Raw;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_unit<E>(self) -> Result<Raw, E> {
                Ok(Raw::Null)
            }
            fn visit_bool<E>(self, b: bool) -> Result<Raw, E> {
                Ok(Raw::Bool(b))
            }
            fn visit_i64<E>(self, i: i64) -> Result<Raw, E> {
                Ok(Raw::Int(i))
            }
            fn visit_u64<E>(self, u: u64) -> Result<Raw, E> {
                Ok(match i64::try_from(u) {
                    Ok(i) => Raw::Int(i),
                    Err(_) => Raw::Double(u as f64),
                })
            }
            fn visit_f64<E>(self, f: f64) -> Result<Raw, E> {
                Ok(Raw::Double(f))
            }
            fn visit_str<E>(self, s: &str) -> Result<Raw, E> {
                Ok(Raw::String(s.to_string()))
            }
            fn visit_string<E>(self, s: String) -> Result<Raw, E> {
                Ok(Raw::String(s))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Raw, A::Error> {
                let mut items = vec![];
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Raw::Array(items))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Raw, A::Error> {
                let mut fields: Vec<(String, Raw)> = vec![];
                while let Some((k, v)) = map.next_entry::<String, Raw>()? {
                    if fields.iter().any(|(existing, _)| *existing == k) {
                        return Err(de::Error::custom(format!("duplicate field {k:?}")));
                    }
                    fields.push((k, v));
                }
                Ok(Raw::Object(fields))
            }
        }
        d.deserialize_any(RawVisitor)
    }
}

impl Value {
    /// Extended JSON (relaxed): numbers as JSON numbers, ObjectId as
    /// `{"$oid": ...}`, dates as `{"$date": "<ISO-8601>"}`, non-finite
    /// doubles as `{"$numberDouble": ...}`.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        write_json(self, &mut out);
        out
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json())
    }
}

fn write_json(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Double(d) if d.is_finite() => {
            let s = serde_json::to_string(d).expect("finite doubles serialize");
            out.push_str(&s);
        }
        Value::Double(d) => {
            let s = if d.is_nan() {
                "NaN"
            } else if *d > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            };
            out.push_str(&format!("{{\"$numberDouble\":\"{s}\"}}"));
        }
        Value::String(s) => out.push_str(&serde_json::to_string(s).expect("strings serialize")),
        Value::ObjectId(o) => out.push_str(&format!("{{\"$oid\":\"{}\"}}", o.to_hex())),
        Value::Date(ms) => out.push_str(&format!("{{\"$date\":\"{}\"}}", format_iso_date(*ms))),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, out);
            }
            out.push(']');
        }
        Value::Document(d) => {
            out.push('{');
            for (i, (k, v)) in d.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("strings serialize"));
                out.push(':');
                write_json(v, out);
            }
            out.push('}');
        }
    }
}

// ---- dates: ISO-8601 <-> ms since the epoch (UTC) ----

// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
// algorithm), and back.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `YYYY-MM-DD`, optionally `THH:MM[:SS[.fff]]` and `Z` or `±HH:MM`.
pub(crate) fn parse_iso_date(s: &str) -> Result<i64, Error> {
    let bad = || Error::BadValue(format!("invalid date {s:?}: expected ISO-8601"));
    let num = |t: &str| t.parse::<i64>().map_err(|_| bad());
    let (date, rest) = s.split_at(s.find('T').unwrap_or(s.len()));
    let ymd: Vec<&str> = date.split('-').collect();
    let [y, m, d] = ymd.as_slice() else {
        return Err(bad());
    };
    let (y, m, d) = (num(y)?, num(m)?, num(d)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    let mut ms = days_from_civil(y, m, d) * 86_400_000;
    let time = rest.strip_prefix('T').unwrap_or("");
    if !time.is_empty() {
        let (clock, offset) = match time.find(['Z', '+', '-']) {
            Some(i) => time.split_at(i),
            None => (time, ""),
        };
        let parts: Vec<&str> = clock.split(':').collect();
        let (h, mi) = match parts.as_slice() {
            [h, mi, ..] => (num(h)?, num(mi)?),
            _ => return Err(bad()),
        };
        let (sec, frac) = match parts.get(2) {
            Some(s) => match s.split_once('.') {
                Some((whole, f)) => {
                    let f3: String = f.chars().chain("000".chars()).take(3).collect();
                    (num(whole)?, num(&f3)?)
                }
                None => (num(s)?, 0),
            },
            None => (0, 0),
        };
        ms += ((h * 60 + mi) * 60 + sec) * 1000 + frac;
        if let Some(sign) = offset.chars().next().filter(|c| *c != 'Z') {
            let (oh, om) = offset[1..].split_once(':').ok_or_else(bad)?;
            let off = (num(oh)? * 60 + num(om)?) * 60_000;
            ms -= if sign == '+' { off } else { -off };
        }
    }
    Ok(ms)
}

pub(crate) fn format_iso_date(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s, f) = (
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000,
    );
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{f:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(json: &str) -> Value {
        from_json(json).unwrap()
    }

    #[test]
    fn test_fields_keep_their_order_and_extended_json_round_trips() {
        let json = r#"{"z":1,"a":{"$oid":"64b7f0a1c2d3e4f5a6b7c8d9"},"m":[1.5,"x",null,true],"d":{"$date":"2024-02-29T12:34:56.789Z"},"n":{"$numberDouble":"NaN"}}"#;
        let doc = Document::parse(json).unwrap();
        assert_eq!(doc.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), ["z", "a", "m", "d", "n"]);
        assert_eq!(doc.to_json(), json);
        assert!(matches!(doc.get("a"), Some(Value::ObjectId(_))));
        assert!(matches!(doc.get("d"), Some(Value::Date(_))));
    }

    #[test]
    fn test_bad_json_and_duplicate_fields_are_errors() {
        assert!(Document::parse("{").is_err());
        assert!(Document::parse(r#"{"a":1,"a":2}"#).is_err());
        assert!(Document::parse("[1]").is_err());
        assert!(from_json(r#"{"$oid":"xyz"}"#).is_err());
    }

    #[test]
    fn test_dates_parse_and_format() {
        assert_eq!(parse_iso_date("1970-01-01").unwrap(), 0);
        assert_eq!(parse_iso_date("1970-01-02T00:00:00Z").unwrap(), 86_400_000);
        assert_eq!(parse_iso_date("2000-03-01T01:00:00+01:00").unwrap(), parse_iso_date("2000-03-01").unwrap());
        assert_eq!(format_iso_date(parse_iso_date("2024-02-29T23:59:59.5Z").unwrap()), "2024-02-29T23:59:59.500Z");
        assert_eq!(format_iso_date(-1), "1969-12-31T23:59:59.999Z");
        assert_eq!(v(r#"{"$date": 1000}"#), Value::Date(1000));
    }

    #[test]
    fn test_comparison_follows_bson_order() {
        let order = [
            "null", "-1", "2.5", "3", r#""a""#, r#""b""#, "{}", r#"{"a":1}"#, "[]", "[1]",
            r#"{"$oid":"000000000000000000000000"}"#, "false", "true", r#"{"$date":0}"#,
        ];
        for w in order.windows(2) {
            assert_eq!(compare(&v(w[0]), &v(w[1])), Ordering::Less, "{} < {}", w[0], w[1]);
        }
        assert!(values_equal(&v("5"), &v("5.0")));
        // Exact past 2^53: 2^53 + 1 is not the double 2^53.
        assert_eq!(compare(&Value::Int((1 << 53) + 1), &Value::Double((1u64 << 53) as f64)), Ordering::Greater);
        assert_eq!(compare(&Value::Double(f64::NAN), &Value::Int(i64::MIN)), Ordering::Less);
    }

    #[test]
    fn test_paths_reach_through_arrays() {
        let doc = Document::parse(r#"{"a":{"b":1},"xs":[{"b":2},{"b":3},4],"n":[[5]]}"#).unwrap();
        let got = |p: &str| lookup(&doc, p).into_iter().map(|v| v.to_json()).collect::<Vec<_>>();
        assert_eq!(got("a.b"), ["1"]);
        assert_eq!(got("xs.b"), ["2", "3"]);
        assert_eq!(got("xs.1.b"), ["3"]);
        assert_eq!(got("xs.2"), ["4"]);
        assert_eq!(got("n"), ["[[5]]"]);
        assert!(got("a.c").is_empty() && got("missing").is_empty());
    }

    #[test]
    fn test_object_ids_are_unique_and_hex_round_trips() {
        let (a, b) = (ObjectId::new(), ObjectId::new());
        assert_ne!(a, b);
        assert_eq!(ObjectId::parse(&a.to_hex()).unwrap(), a);
    }
}
