//! A mongosh-style shell: statements such as
//! `db.orders.find({qty: {$gt: 5}}).sort({qty: -1}).limit(3)` run against
//! a Client, each giving back text to print.
//!
//! Arguments are JavaScript-ish object literals — unquoted keys, single-
//! or double-quoted strings, trailing commas, `ObjectId("…")`,
//! `ISODate("…")` / `new Date("…")`, `NumberLong(…)`, `NumberInt(…)` —
//! rewritten into extended JSON and read by value::from_json.

use store::db::DBFile;

use crate::client::{Client, Session};
use crate::collection::{Collection, FindOneAndOptions, FindOptions, IndexOptions};
use crate::error::{Error, Result};
use crate::value::{Document, Value, from_json};

pub const HELP: &str = "\
use <db>                          switch database
show dbs | show collections       list databases / collections
begin | commit | abort            multi-document transaction
db.<coll>.find(filter, projection)[.sort(s)][.skip(n)][.limit(n)][.count()][.explain()]
db.<coll>.findOne(filter, projection)
db.<coll>.insertOne(doc) | insertMany([docs])
db.<coll>.updateOne(filter, update, {upsert}) | updateMany(...) | replaceOne(filter, doc, {upsert})
db.<coll>.deleteOne(filter) | deleteMany(filter)
db.<coll>.findOneAndUpdate(filter, update, {sort, projection, upsert, returnDocument: 'after'})
db.<coll>.findOneAndReplace(filter, doc, {...}) | findOneAndDelete(filter, {sort, projection})
db.<coll>.countDocuments(filter) | distinct(field, filter) | aggregate([stages])
db.<coll>.createIndex(keys, {unique, name}) | dropIndex(name) | getIndexes() | drop()
exit";

pub struct Shell<F: DBFile<Item = F> + 'static> {
    client: Client<F>,
    session: Session<F>,
    db: String,
}

/// What a statement asked for, and whether the shell should stop.
pub enum Outcome {
    Output(String),
    Exit,
}

impl<F: DBFile<Item = F> + 'static> Shell<F> {
    pub fn new(client: Client<F>) -> Self {
        let session = client.start_session();
        Shell { client, session, db: "test".into() }
    }

    pub fn prompt(&self) -> String {
        let txn = if self.session.in_transaction() { " (txn)" } else { "" };
        format!("{}{txn}> ", self.db)
    }

    /// Ends the shell, closing the database.
    pub fn close(self) -> Result<()> {
        let Shell { client, session, .. } = self;
        drop(session);
        client.close()
    }

    pub fn execute(&mut self, line: &str) -> Result<Outcome> {
        let line = line.trim().trim_end_matches(';').trim();
        let words: Vec<&str> = line.split_whitespace().collect();
        let text = |s: String| Ok(Outcome::Output(s));
        match words.as_slice() {
            [] => text(String::new()),
            ["exit" | "quit"] => Ok(Outcome::Exit),
            ["help"] => text(HELP.into()),
            ["use", name] => {
                self.db = name.to_string();
                text(format!("switched to db {name}"))
            }
            ["show", "dbs" | "databases"] => text(self.client.list_database_names().join("\n")),
            ["show", "collections" | "tables"] => {
                text(self.client.database(&self.db).list_collection_names().join("\n"))
            }
            ["begin"] => {
                self.session.start_transaction()?;
                text("transaction started".into())
            }
            ["commit"] => {
                self.session.commit_transaction()?;
                text("committed".into())
            }
            ["abort" | "rollback"] => {
                self.session.abort_transaction()?;
                text("aborted".into())
            }
            _ if line.starts_with("db.") => self.call(&line[3..]).map(Outcome::Output),
            _ => Err(Error::BadValue(format!("unknown command: {line} (try help)"))),
        }
    }

    fn call(&self, statement: &str) -> Result<String> {
        let open = statement
            .find('(')
            .ok_or_else(|| Error::BadValue("expected db.<collection>.<method>(...)".into()))?;
        let (coll, method) = statement[..open]
            .rsplit_once('.')
            .ok_or_else(|| Error::BadValue("expected db.<collection>.<method>(...)".into()))?;
        let mut p = Parser { s: statement, i: open };
        let args = p.args()?;
        let mut chain = vec![];
        loop {
            p.skip_ws();
            if p.i == p.s.len() {
                break;
            }
            p.expect('.')?;
            let name = p.ident();
            chain.push((name, p.args()?));
        }
        let c = self.client.database(&self.db).collection(coll).with_session(&self.session);
        run_method(&c, method, args, chain)
    }
}

fn run_method<F: DBFile<Item = F> + 'static>(
    c: &Collection<F>,
    method: &str,
    args: Vec<Value>,
    chain: Vec<(String, Vec<Value>)>,
) -> Result<String> {
    let mut args = args.into_iter();
    if method != "find" && !chain.is_empty() {
        return Err(Error::BadValue(format!("{method}(...) can't be followed by .{}", chain[0].0)));
    }
    let lines = |docs: Vec<Document>| docs.iter().map(Document::to_json).collect::<Vec<_>>().join("\n");
    let obj = |fields: Vec<(&str, Value)>| {
        let mut d = Document::new();
        for (k, v) in fields {
            d.insert(k, v);
        }
        d.to_json()
    };
    let count = |n: usize| Value::Int(n as i64);
    Ok(match method {
        "find" => {
            let filter = doc_arg(&mut args, method, "the filter")?.unwrap_or_default();
            let mut options = FindOptions { projection: doc_arg(&mut args, method, "the projection")?, ..Default::default() };
            let mut finish = None;
            for (name, a) in chain {
                match (name.as_str(), a.as_slice()) {
                    ("sort", [Value::Document(s)]) => options.sort = Some(s.clone()),
                    ("limit", [n]) => options.limit = Some(whole(n)?).filter(|n| *n > 0),
                    ("skip", [n]) => options.skip = whole(n)?,
                    ("count" | "explain" | "toArray" | "pretty", []) => finish = Some(name),
                    _ => return Err(Error::BadValue(format!("unknown or malformed cursor method .{name}(...)"))),
                }
            }
            match finish.as_deref() {
                Some("count") => count(c.find(filter, options)?.len()).to_json(),
                Some("explain") => c.explain_find(filter, &options)?.to_json(),
                _ => lines(c.find(filter, options)?),
            }
        }
        "findOne" => {
            let filter = doc_arg(&mut args, method, "the filter")?.unwrap_or_default();
            let options = FindOptions { projection: doc_arg(&mut args, method, "the projection")?, limit: Some(1), ..Default::default() };
            c.find(filter, options)?.pop().map(|d| d.to_json()).unwrap_or("null".into())
        }
        "insertOne" => {
            let doc = doc_arg(&mut args, method, "the document")?.ok_or_else(|| Error::BadValue("insertOne needs a document".into()))?;
            obj(vec![("acknowledged", Value::Bool(true)), ("insertedId", c.insert_one(doc)?)])
        }
        "insertMany" => {
            let Some(Value::Array(items)) = args.next() else {
                return Err(Error::BadValue("insertMany needs a list of documents".into()));
            };
            let docs = items.into_iter().map(document).collect::<Result<_>>()?;
            obj(vec![("acknowledged", Value::Bool(true)), ("insertedIds", Value::Array(c.insert_many(docs)?))])
        }
        "updateOne" | "updateMany" | "replaceOne" => {
            let filter = doc_arg(&mut args, method, "the filter")?.unwrap_or_default();
            let update = doc_arg(&mut args, method, "the update")?.ok_or_else(|| Error::BadValue(format!("{method} needs an update")))?;
            let upsert = flag(&doc_arg(&mut args, method, "the options")?, "upsert");
            let r = match method {
                "updateOne" => c.update_one(filter, update, upsert)?,
                "updateMany" => c.update_many(filter, update, upsert)?,
                _ => c.replace_one(filter, update, upsert)?,
            };
            let mut fields = vec![
                ("acknowledged", Value::Bool(true)),
                ("matchedCount", count(r.matched_count)),
                ("modifiedCount", count(r.modified_count)),
            ];
            if let Some(id) = r.upserted_id {
                fields.push(("upsertedId", id));
            }
            obj(fields)
        }
        "deleteOne" | "deleteMany" => {
            let filter = doc_arg(&mut args, method, "the filter")?.unwrap_or_default();
            let n = if method == "deleteOne" { c.delete_one(filter)? } else { c.delete_many(filter)? };
            obj(vec![("acknowledged", Value::Bool(true)), ("deletedCount", count(n))])
        }
        "findOneAndUpdate" | "findOneAndReplace" | "findOneAndDelete" => {
            let filter = doc_arg(&mut args, method, "the filter")?.unwrap_or_default();
            let update = if method == "findOneAndDelete" {
                None
            } else {
                Some(doc_arg(&mut args, method, "the update")?.ok_or_else(|| Error::BadValue(format!("{method} needs an update")))?)
            };
            let opts = doc_arg(&mut args, method, "the options")?;
            let get = |k: &str| opts.as_ref().and_then(|o| o.get(k)).and_then(|v| match v {
                Value::Document(d) => Some(d.clone()),
                _ => None,
            });
            let options = FindOneAndOptions {
                sort: get("sort"),
                projection: get("projection"),
                upsert: flag(&opts, "upsert"),
                return_new: matches!(opts.as_ref().and_then(|o| o.get("returnDocument")), Some(Value::String(s)) if s == "after")
                    || flag(&opts, "returnNewDocument"),
            };
            let found = match (method, update) {
                ("findOneAndUpdate", Some(u)) => c.find_one_and_update(filter, u, options)?,
                ("findOneAndReplace", Some(u)) => c.find_one_and_replace(filter, u, options)?,
                _ => c.find_one_and_delete(filter, options)?,
            };
            found.map(|d| d.to_json()).unwrap_or("null".into())
        }
        "countDocuments" | "count" => count(c.count_documents(doc_arg(&mut args, method, "the filter")?.unwrap_or_default())?).to_json(),
        "distinct" => {
            let Some(Value::String(field)) = args.next() else {
                return Err(Error::BadValue("distinct needs a field name".into()));
            };
            Value::Array(c.distinct(&field, doc_arg(&mut args, method, "the filter")?.unwrap_or_default())?).to_json()
        }
        "aggregate" => {
            let Some(Value::Array(stages)) = args.next() else {
                return Err(Error::BadValue("aggregate needs a list of stages".into()));
            };
            lines(c.aggregate(stages.into_iter().map(document).collect::<Result<_>>()?)?)
        }
        "createIndex" => {
            let keys = doc_arg(&mut args, method, "the keys")?.ok_or_else(|| Error::BadValue("createIndex needs keys".into()))?;
            let opts = doc_arg(&mut args, method, "the options")?;
            let name = match opts.as_ref().and_then(|o| o.get("name")) {
                Some(Value::String(n)) => Some(n.clone()),
                _ => None,
            };
            let name = c.create_index(keys, IndexOptions { unique: flag(&opts, "unique"), name })?;
            Value::String(name).to_json()
        }
        "dropIndex" => {
            let Some(Value::String(name)) = args.next() else {
                return Err(Error::BadValue("dropIndex needs an index name".into()));
            };
            c.drop_index(&name)?;
            obj(vec![("ok", Value::Int(1))])
        }
        "getIndexes" => c
            .list_indexes()
            .into_iter()
            .map(|ix| {
                let mut fields = vec![("name", Value::String(ix.name)), ("key", Value::Document(ix.keys))];
                if ix.unique {
                    fields.push(("unique", Value::Bool(true)));
                }
                obj(fields)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        "drop" => {
            c.drop()?;
            "true".into()
        }
        other => return Err(Error::BadValue(format!("unknown collection method {other} (try help)"))),
    })
}

// The next argument, which must be a document if present.
fn doc_arg(args: &mut std::vec::IntoIter<Value>, method: &str, what: &str) -> Result<Option<Document>> {
    match args.next() {
        None => Ok(None),
        Some(Value::Document(d)) => Ok(Some(d)),
        Some(other) => Err(Error::BadValue(format!("{method}: {what} must be a document, not {}", other.to_json()))),
    }
}

fn document(v: Value) -> Result<Document> {
    match v {
        Value::Document(d) => Ok(d),
        other => Err(Error::BadValue(format!("expected a document, not {}", other.to_json()))),
    }
}

fn whole(v: &Value) -> Result<usize> {
    match v {
        Value::Int(n) if *n >= 0 => Ok(*n as usize),
        Value::Double(d) if *d >= 0.0 && d.fract() == 0.0 => Ok(*d as usize),
        other => Err(Error::BadValue(format!("expected a whole number, not {}", other.to_json()))),
    }
}

fn flag(opts: &Option<Document>, name: &str) -> bool {
    matches!(opts.as_ref().and_then(|o| o.get(name)), Some(Value::Bool(true)))
}

// ---- the argument language ----

struct Parser<'a> {
    s: &'a str,
    i: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<char> {
        self.s[self.i..].chars().next()
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.i += 1;
        }
    }

    fn expect(&mut self, c: char) -> Result<()> {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.i += c.len_utf8();
            Ok(())
        } else {
            Err(self.error(&format!("expected '{c}'")))
        }
    }

    fn error(&self, what: &str) -> Error {
        Error::BadValue(format!("{what} at: {}", self.s[self.i..].chars().take(30).collect::<String>()))
    }

    fn ident(&mut self) -> String {
        self.skip_ws();
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$') {
            self.i += 1;
        }
        self.s[start..self.i].to_string()
    }

    // `( value, value, ... )`
    fn args(&mut self) -> Result<Vec<Value>> {
        self.expect('(')?;
        let mut json = vec![];
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                self.i += 1;
                break;
            }
            let mut out = String::new();
            self.value(&mut out)?;
            json.push(out);
            self.skip_ws();
            match self.peek() {
                Some(',') => self.i += 1,
                Some(')') => {}
                _ => return Err(self.error("expected ',' or ')'")),
            }
        }
        json.iter().map(|j| from_json(j)).collect()
    }

    // One value, written into `out` as JSON.
    fn value(&mut self, out: &mut String) -> Result<()> {
        self.skip_ws();
        match self.peek() {
            Some('{') => self.list('{', '}', out, true),
            Some('[') => self.list('[', ']', out, false),
            Some('"' | '\'') => {
                let s = self.string()?;
                out.push_str(&serde_json::to_string(&s).expect("strings serialize"));
                Ok(())
            }
            Some(c) if c == '-' || c == '+' || c == '.' || c.is_ascii_digit() => {
                let start = self.i;
                while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || "+-.".contains(c)) {
                    self.i += 1;
                }
                let n = self.s[start..self.i].trim_start_matches('+');
                if n.parse::<f64>().is_err() {
                    return Err(self.error("bad number"));
                }
                out.push_str(n);
                Ok(())
            }
            Some(_) => {
                let word = self.ident();
                match word.as_str() {
                    "true" | "false" | "null" => out.push_str(&word),
                    "undefined" => out.push_str("null"),
                    "new" => return self.value(out),
                    "ObjectId" | "ISODate" | "Date" | "NumberLong" | "NumberInt" | "NumberDecimal" => {
                        self.expect('(')?;
                        self.skip_ws();
                        let arg = match self.peek() {
                            Some(')') => None,
                            Some('"' | '\'') => Some(self.string()?),
                            _ => {
                                let mut n = String::new();
                                self.value(&mut n)?;
                                Some(n)
                            }
                        };
                        self.expect(')')?;
                        let tagged = |tag: &str, v: String| {
                            format!("{{\"{tag}\":{}}}", serde_json::to_string(&v).expect("strings serialize"))
                        };
                        out.push_str(&match (word.as_str(), arg) {
                            ("ObjectId", None) => {
                                tagged("$oid", crate::value::ObjectId::new().to_hex())
                            }
                            ("ObjectId", Some(hex)) => tagged("$oid", hex),
                            ("ISODate" | "Date", None) => {
                                let ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as i64)
                                    .unwrap_or(0);
                                format!("{{\"$date\":{ms}}}")
                            }
                            ("ISODate" | "Date", Some(s)) if s.parse::<i64>().is_ok() => format!("{{\"$date\":{s}}}"),
                            ("ISODate" | "Date", Some(s)) => tagged("$date", s),
                            ("NumberLong", Some(n)) => tagged("$numberLong", n),
                            ("NumberInt", Some(n)) => tagged("$numberInt", n),
                            ("NumberDecimal", Some(n)) => tagged("$numberDouble", n),
                            (w, None) => return Err(self.error(&format!("{w}() needs an argument"))),
                            _ => unreachable!("matched above"),
                        });
                    }
                    "" => return Err(self.error("expected a value")),
                    other => return Err(self.error(&format!("unknown name {other}"))),
                }
                Ok(())
            }
            None => Err(self.error("expected a value")),
        }
    }

    // `{k: v, ...}` or `[v, ...]`, allowing a trailing comma.
    fn list(&mut self, open: char, close: char, out: &mut String, keyed: bool) -> Result<()> {
        self.expect(open)?;
        out.push(open);
        let mut first = true;
        loop {
            self.skip_ws();
            if self.peek() == Some(close) {
                self.i += 1;
                out.push(close);
                return Ok(());
            }
            if !first {
                out.push(',');
            }
            first = false;
            if keyed {
                self.skip_ws();
                let key = match self.peek() {
                    Some('"' | '\'') => self.string()?,
                    _ => {
                        let start = self.i;
                        while self.peek().is_some_and(|c| c.is_alphanumeric() || "_$.".contains(c)) {
                            self.i += 1;
                        }
                        if start == self.i {
                            return Err(self.error("expected a field name"));
                        }
                        self.s[start..self.i].to_string()
                    }
                };
                out.push_str(&serde_json::to_string(&key).expect("strings serialize"));
                self.expect(':')?;
                out.push(':');
            }
            self.value(out)?;
            self.skip_ws();
            match self.peek() {
                Some(',') => self.i += 1,
                Some(c) if c == close => {}
                _ => return Err(self.error(&format!("expected ',' or '{close}'"))),
            }
        }
    }

    // A quoted string, either quote, with JSON-style escapes.
    fn string(&mut self) -> Result<String> {
        let quote = self.peek().expect("at a quote");
        self.i += 1;
        let mut out = String::new();
        let mut chars = self.s[self.i..].char_indices();
        while let Some((at, c)) = chars.next() {
            match c {
                c if c == quote => {
                    self.i += at + 1;
                    return Ok(out);
                }
                '\\' => match chars.next() {
                    Some((_, 'n')) => out.push('\n'),
                    Some((_, 't')) => out.push('\t'),
                    Some((_, 'r')) => out.push('\r'),
                    Some((_, 'u')) => {
                        let hex: String = (0..4).filter_map(|_| chars.next().map(|(_, c)| c)).collect();
                        let code = u32::from_str_radix(&hex, 16).map_err(|_| self.error("bad \\u escape"))?;
                        out.push(char::from_u32(code).ok_or_else(|| self.error("bad \\u escape"))?);
                    }
                    Some((_, other)) => out.push(other),
                    None => break,
                },
                c => out.push(c),
            }
        }
        Err(self.error("unterminated string"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::db::Db;
    use store::memfile::MemFile;

    fn run(shell: &mut Shell<MemFile>, line: &str) -> String {
        match shell.execute(line) {
            Ok(Outcome::Output(s)) => s,
            Ok(Outcome::Exit) => "<exit>".into(),
            Err(e) => format!("error: {e}"),
        }
    }

    #[test]
    fn test_statements() {
        let mut sh = Shell::new(Client::start(Db::<MemFile>::create("sqjson_shell").unwrap()).unwrap());
        assert_eq!(run(&mut sh, "use shop"), "switched to db shop");
        assert_eq!(
            run(&mut sh, "db.items.insertMany([{_id: 1, name: 'pen', qty: 5, tags: ['a'],}, {_id: 2, name: \"ink\", qty: 1}])"),
            r#"{"acknowledged":true,"insertedIds":[1,2]}"#
        );
        run(&mut sh, "db.items.insertOne({_id: 3, name: 'pad', qty: 12, at: ISODate('2024-01-02T03:04:05Z'), n: NumberLong('7')})");
        assert_eq!(run(&mut sh, "db.items.find({qty: {$gt: 2}}, {name: 1}).sort({qty: -1})"), "{\"_id\":3,\"name\":\"pad\"}\n{\"_id\":1,\"name\":\"pen\"}");
        assert_eq!(run(&mut sh, "db.items.find().skip(1).limit(1)"), r#"{"_id":2,"name":"ink","qty":1}"#);
        assert_eq!(run(&mut sh, "db.items.find({qty: {$lt: 10}}).count()"), "2");
        assert_eq!(run(&mut sh, "db.items.findOne({_id: 3}, {at: 1, _id: 0})"), r#"{"at":{"$date":"2024-01-02T03:04:05.000Z"}}"#);
        assert_eq!(run(&mut sh, "db.items.findOne({_id: 99})"), "null");
        assert!(run(&mut sh, "db.items.updateOne({name: 'cap'}, {$set: {qty: 0}}, {upsert: true})").contains(r#""upsertedId""#));
        assert_eq!(run(&mut sh, "db.items.updateMany({}, {$inc: {qty: 1}})"), r#"{"acknowledged":true,"matchedCount":4,"modifiedCount":4}"#);
        assert_eq!(run(&mut sh, "db.items.createIndex({qty: 1}, {unique: true})"), r#""qty_1""#);
        assert_eq!(run(&mut sh, "db.items.find({qty: 6}).explain()"), r#"{"stage":"IXSCAN","indexName":"qty_1","keyRanges":1,"multikey":false}"#);
        assert!(run(&mut sh, "db.items.insertOne({qty: 6})").starts_with("error: E11000"));
        assert_eq!(run(&mut sh, "db.items.getIndexes()"), "{\"name\":\"_id_\",\"key\":{\"_id\":1},\"unique\":true}\n{\"name\":\"qty_1\",\"key\":{\"qty\":1},\"unique\":true}");
        assert_eq!(
            run(&mut sh, "db.items.findOneAndUpdate({qty: {$gt: 1}}, {$set: {hot: true}}, {sort: {qty: 1}, projection: {qty: 1}, returnDocument: 'after'})"),
            r#"{"_id":2,"qty":2}"#
        );
        assert_eq!(run(&mut sh, "db.items.aggregate([{$group: {_id: null, total: {$sum: '$qty'}}}])"), r#"{"_id":null,"total":22}"#);
        assert_eq!(run(&mut sh, "db.items.distinct('tags')"), r#"["a"]"#);
        assert_eq!(run(&mut sh, "show collections"), "items");
        assert_eq!(run(&mut sh, "show dbs"), "shop");

        // Transactions through begin / commit / abort.
        assert_eq!(run(&mut sh, "begin"), "transaction started");
        assert_eq!(sh.prompt(), "shop (txn)> ");
        run(&mut sh, "db.items.deleteMany({})");
        assert_eq!(run(&mut sh, "db.items.countDocuments()"), "0");
        assert_eq!(run(&mut sh, "abort"), "aborted");
        assert_eq!(run(&mut sh, "db.items.countDocuments({})"), "4");

        assert!(run(&mut sh, "db.items.find({a: })").starts_with("error: expected a value"));
        assert!(run(&mut sh, "db.items.nope()").starts_with("error: unknown collection method"));
        assert!(run(&mut sh, "frob").starts_with("error: unknown command"));
        assert_eq!(run(&mut sh, "db.items.drop()"), "true");
        assert_eq!(run(&mut sh, "exit"), "<exit>");
        sh.close().unwrap();
    }
}
