// Seeded random queries, each answered three ways — squeal on plain tables,
// squeal on the same rows partitioned, and SQLite — which must agree. No
// expected answer is written down: SQLite is the oracle for both squeal
// answers, and the plain tables for the partitioned ones.
//
// The suite runs a fixed set of seeds. For a long run:
//     SQ_ORACLE_QUERIES=20000 SQ_ORACLE_SEED=7 cargo test -p squeal-sql \
//         oracle::test_random -- --nocapture
// A disagreement names the seed, the query number and the query.
//
// The generated SQL stays inside what both engines define the same way:
// no division (squeal and SQLite differ on dividing by zero), ORDER BY
// always with NULLS FIRST/LAST (the default placement differs), no
// boolean output columns (SQLite has none), every column qualified.
use super::partition_diff::ordered;
use super::*;

// A value as both engines' answers are compared: doubles to 9 places.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum V {
    Null,
    Real(i64),
    Text(String),
}

fn real(d: f64) -> V {
    V::Real((d * 1e9).round() as i64)
}

// An integer, as the number it is: when a column holds integers and
// doubles (a UNION of the two), which of `1` and `1.0` survives DISTINCT is
// either engine's choice.
fn int(i: i64) -> V {
    real(i as f64)
}

fn from_squeal(v: &ValueItem) -> V {
    match v {
        ValueItem::Null => V::Null,
        ValueItem::Integer(i) => int(*i),
        ValueItem::Double(d) => real(*d),
        ValueItem::Str((s, _)) => V::Text(s.clone()),
        ValueItem::Boolean(b) => int(*b as i64),
        other => V::Text(format!("{other:?}")),
    }
}

type Answer = Result<Vec<Vec<V>>, String>;

fn squeal(c: &Arc<Connection<MemFile>>, sql: &str) -> Answer {
    ordered(c, sql).map(|rows| {
        rows.iter()
            .map(|r| r.iter().map(from_squeal).collect())
            .collect()
    })
}

fn sqlite(db: &rusqlite::Connection, sql: &str) -> Answer {
    let mut stmt = db.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |r| {
        (0..n)
            .map(|i| {
                Ok(match r.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => V::Null,
                    rusqlite::types::ValueRef::Integer(i) => int(i),
                    rusqlite::types::ValueRef::Real(d) => real(d),
                    rusqlite::types::ValueRef::Text(t) => {
                        V::Text(String::from_utf8_lossy(t).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(_) => V::Text("<blob>".into()),
                })
            })
            .collect()
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<_, _>>()
    .map_err(|e| e.to_string())
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
}

// The tables. ev and reg exist twice in squeal — `plain_*` and, partitioned,
// `part_*` — and as `plain_*` in SQLite; dim once everywhere.
const DDL: &[(&str, &str)] = &[
    (
        "ev",
        "(id integer not null, day integer not null, cat integer, amount integer, \
         note varchar(8), primary key(id, day))",
    ),
    (
        "reg",
        "(id integer not null, region varchar(4), amount integer)",
    ),
];
const PART: &[(&str, &str)] = &[
    (
        "ev",
        "partition by range (day) (partition p0 values less than (10), \
         partition p1 values less than (20), partition p2 values less than (30), \
         partition p3 values less than maxvalue)",
    ),
    (
        "reg",
        "partition by list (region) (partition west values in ('ca', 'wa'), \
         partition east values in ('ny'), partition other default)",
    ),
];

// Integer columns, by table, for expressions; text columns.
fn int_columns(table: &str) -> &'static [&'static str] {
    match table {
        "ev" => &["id", "day", "cat", "amount"],
        "reg" => &["id", "amount"],
        _ => &["k", "w"],
    }
}

fn text_columns(table: &str) -> &'static [&'static str] {
    match table {
        "ev" => &["note"],
        "reg" => &["region"],
        _ => &["label"],
    }
}

fn setup() -> (Arc<Connection<MemFile>>, rusqlite::Connection) {
    setup_on(conn())
}

// The tables, in the database `c` is connected to.
fn setup_on(c: Arc<Connection<MemFile>>) -> (Arc<Connection<MemFile>>, rusqlite::Connection) {
    let lite = rusqlite::Connection::open_in_memory().unwrap();
    let both = |sql: String| {
        lite.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("sqlite: {sql}: {e}"));
    };
    for (t, cols) in DDL {
        run(&c, &format!("create table plain_{t} {cols}")).unwrap();
        let part = PART.iter().find(|(p, _)| p == t).unwrap().1;
        run(&c, &format!("create table part_{t} {cols} {part}")).unwrap();
        both(format!("create table plain_{t} {cols}"));
    }
    run(
        &c,
        "create table dim (k integer not null, w integer, label varchar(8), primary key(k))",
    )
    .unwrap();
    both(
        "create table dim (k integer not null, w integer, label varchar(8), primary key(k))".into(),
    );

    let mut rng = Rng(0xDA7A_5EED_1234_5678);
    let null_or =
        |rng: &mut Rng, v: String| -> String { if rng.chance(12) { "null".into() } else { v } };
    let mut ev = vec![];
    for id in 0..70 {
        // days 0..44, many repeated; none in 30..34.
        let mut day = rng.int(0, 44);
        if (30..35).contains(&day) {
            day += 5;
        }
        let cat = rng.int(0, 3).to_string();
        let cat = null_or(&mut rng, cat);
        let amount = rng.int(-50, 200).to_string();
        let amount = null_or(&mut rng, amount);
        let note = format!("'n{}'", rng.int(0, 5));
        let note = null_or(&mut rng, note);
        ev.push(format!("({id}, {day}, {cat}, {amount}, {note})"));
    }
    let mut reg = vec![];
    for id in 0..20 {
        let region = format!("'{}'", rng.pick(&["ca", "wa", "ny", "tx", "fl"]));
        let region = null_or(&mut rng, region);
        let amount = rng.int(0, 99).to_string();
        let amount = null_or(&mut rng, amount);
        reg.push(format!("({id}, {region}, {amount})"));
    }
    let mut dim = vec![];
    for k in 0..15 {
        let w = rng.int(0, 40).to_string();
        let w = null_or(&mut rng, w);
        dim.push(format!("({}, {w}, 'd{k}')", k * 3));
    }
    for (t, rows) in [("ev", &ev), ("reg", &reg)] {
        for side in ["plain", "part"] {
            run(
                &c,
                &format!("insert into {side}_{t} values {}", rows.join(", ")),
            )
            .unwrap();
        }
        both(format!("insert into plain_{t} values {}", rows.join(", ")));
    }
    run(&c, &format!("insert into dim values {}", dim.join(", "))).unwrap();
    both(format!("insert into dim values {}", dim.join(", ")));
    for t in ["plain_ev", "part_ev", "plain_reg", "part_reg", "dim"] {
        run(&c, &format!("analyze table {t}")).unwrap();
    }
    (c, lite)
}

// A FROM item in a generated query: its alias and table kind.
struct Item {
    alias: String,
    table: &'static str,
}

struct Gen<'a> {
    rng: &'a mut Rng,
    items: Vec<Item>,
    // Whether conditions may hold subqueries (see subquery_cond).
    subqueries: bool,
}

impl Gen<'_> {
    fn table_name(table: &str) -> String {
        match table {
            "dim" => "dim".into(),
            t => format!("{{t}}_{t}"),
        }
    }

    fn int_col(&mut self) -> String {
        let i = self.rng.below(self.items.len() as u64) as usize;
        let item = &self.items[i];
        let col = *self.rng.pick(int_columns(item.table));
        format!("{}.{col}", item.alias)
    }

    fn text_col(&mut self) -> String {
        let i = self.rng.below(self.items.len() as u64) as usize;
        let item = &self.items[i];
        let col = *self.rng.pick(text_columns(item.table));
        format!("{}.{col}", item.alias)
    }

    // An integer-valued expression.
    fn int_expr(&mut self, depth: u32) -> String {
        match self.rng.below(if depth > 1 { 1 } else { 9 }) {
            0..=2 => self.int_col(),
            3 => format!("{} + {}", self.int_col(), self.rng.int(-5, 20)),
            4 => format!("{} * {}", self.int_col(), self.rng.int(-3, 3)),
            5 => format!("{} % {}", self.int_col(), self.rng.int(1, 7)),
            6 => format!("abs({})", self.int_col()),
            7 => format!("coalesce({}, {})", self.int_col(), self.rng.int(-1, 9)),
            _ => format!(
                "case when {} then {} else {} end",
                self.cond(depth + 1),
                self.int_expr(depth + 1),
                self.int_expr(depth + 1)
            ),
        }
    }

    // A text-valued expression.
    fn text_expr(&mut self) -> String {
        match self.rng.below(6) {
            0..=2 => self.text_col(),
            3 => format!(
                "{} || '{}'",
                self.text_col(),
                self.rng.pick(&["x", "", "-"])
            ),
            4 => format!("upper({})", self.text_col()),
            _ => format!("coalesce({}, 'none')", self.text_col()),
        }
    }

    fn cond(&mut self, depth: u32) -> String {
        if self.subqueries && depth <= 1 && self.rng.chance(45) {
            return self.subquery_cond();
        }
        let op = *self.rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
        match self.rng.below(if depth > 1 { 9 } else { 12 }) {
            0 | 1 => format!("{} {op} {}", self.int_col(), self.rng.int(-10, 60)),
            9 => format!(
                "{} {op} '{}'",
                self.text_col(),
                self.rng.pick(&["n2", "d5", "ny", "m"])
            ),
            10 => format!("{} {op} null", self.int_col()),
            11 => format!("{} in ({}, null)", self.int_col(), self.rng.int(0, 45)),
            2 => format!("{} {op} {}", self.int_col(), self.int_col()),
            3 => {
                let n = self.rng.int(1, 4);
                let list: Vec<String> = (0..n).map(|_| self.rng.int(0, 45).to_string()).collect();
                format!("{} in ({})", self.int_col(), list.join(", "))
            }
            4 => format!(
                "{} is {}null",
                self.int_col(),
                if self.rng.chance(50) { "not " } else { "" }
            ),
            5 => {
                let lo = self.rng.int(-5, 40);
                format!(
                    "{} between {lo} and {}",
                    self.int_col(),
                    lo + self.rng.int(0, 20)
                )
            }
            6 | 7 if depth > 1 => format!("{} = {}", self.int_col(), self.int_col()),
            8 if depth > 1 => format!("{} is null", self.text_col()),
            6 => format!(
                "{} like '{}'",
                self.text_col(),
                self.rng.pick(&["n1%", "%a", "_a", "n_", "%", "d1%", "c%"])
            ),
            7 => format!(
                "({} {} {})",
                self.cond(depth + 1),
                self.rng.pick(&["and", "or"]),
                self.cond(depth + 1)
            ),
            _ => format!("not ({})", self.cond(depth + 1)),
        }
    }

    // A condition holding a subquery over one or two inner tables (aliased
    // sq, sq2): IN / NOT IN, EXISTS / NOT EXISTS, or a scalar comparison;
    // uncorrelated, or correlated through `inner = outer` equalities.
    fn subquery_cond(&mut self) -> String {
        let table = *self.rng.pick(&["ev", "reg", "dim"]);
        let from = format!("{} sq", Self::table_name(table));
        let inner = |g: &mut Self| format!("sq.{}", g.rng.pick(int_columns(table)));
        let not = |g: &mut Self| if g.rng.chance(40) { "not " } else { "" };
        let local = |g: &mut Self| -> String {
            if g.rng.chance(50) {
                let op = *g.rng.pick(&["=", "<>", "<", ">=", ">"]);
                format!(" and {} {op} {}", inner(g), g.rng.int(-5, 50))
            } else {
                String::new()
            }
        };
        match self.rng.below(8) {
            // Uncorrelated IN.
            0 | 1 => {
                let (outer, c, l) = (self.int_col(), inner(self), local(self));
                let n = not(self);
                format!("{outer} {n}in (select {c} from {from} where 1 = 1{l})")
            }
            // Correlated EXISTS.
            2 | 3 => {
                let (c, outer, l) = (inner(self), self.int_col(), local(self));
                let n = not(self);
                if self.rng.chance(50) {
                    format!("{n}exists (select 1 from {from} where {c} = {outer}{l})")
                } else {
                    format!("{n}exists (select * from {from} where {outer} = {c}{l})")
                }
            }
            // Correlated IN.
            4 => {
                let (outer, v, c, key, l) = (
                    self.int_col(),
                    inner(self),
                    inner(self),
                    self.int_col(),
                    local(self),
                );
                let n = not(self);
                format!("{outer} {n}in (select {v} from {from} where {c} = {key}{l})")
            }
            // Uncorrelated EXISTS.
            5 => {
                let (c, n) = (inner(self), not(self));
                format!("{n}exists (select 1 from {from} where {c} > {})", self.rng.int(-10, 210))
            }
            // A scalar subquery.
            6 => {
                let (outer, c) = (self.int_col(), inner(self));
                let op = *self.rng.pick(&["=", "<", ">="]);
                let agg = *self.rng.pick(&["max", "min", "count"]);
                format!("{outer} {op} (select {agg}({c}) from {from})")
            }
            // Correlated EXISTS over a join of two inner tables.
            _ => {
                let outer = self.int_col();
                let n = not(self);
                format!(
                    "{n}exists (select 1 from dim sq join {} sq2 on sq.k = sq2.cat \
                     where sq2.day = {outer} and sq.w > {})",
                    Self::table_name("ev"),
                    self.rng.int(0, 30)
                )
            }
        }
    }

    // FROM: one table, a join of two or three, or a FROM subquery.
    fn from(&mut self) -> String {
        let tables = ["ev", "reg", "dim"];
        let first = *self.rng.pick(&tables);
        self.items.push(Item {
            alias: "a".into(),
            table: first,
        });
        let mut from = format!("{} a", Self::table_name(first));
        let joins = self.rng.below(3);
        for (n, alias) in ["b", "c"].iter().enumerate().take(joins as usize) {
            let table = *self.rng.pick(&tables);
            // Results stay small enough to compare: one cross join at most.
            let kinds: &[&str] = if from.contains("cross join") {
                &["join", "left join"]
            } else {
                &["join", "left join", "join", "left join", "cross join"]
            };
            let kind = *self.rng.pick(kinds);
            let left = self.int_col();
            self.items.push(Item {
                alias: alias.to_string(),
                table,
            });
            let right_col = *self.rng.pick(int_columns(table));
            from.push_str(&format!(" {kind} {} {alias}", Self::table_name(table)));
            if kind == "cross join" {
                continue;
            }
            let op = if self.rng.chance(80) {
                "="
            } else {
                *self.rng.pick(&["<", ">="])
            };
            from.push_str(&format!(" on {left} {op} {alias}.{right_col}"));
            if self.rng.chance(30) || n == 9 {
                from.push_str(&format!(" and {}", self.cond(2)));
            }
        }
        from
    }

    // One SELECT block: `columns` output columns, all integers.
    fn select(&mut self, columns: usize) -> String {
        self.items.clear();
        let from = self.from();
        let wh = if self.rng.chance(60) {
            format!(" where {}", self.cond(0))
        } else {
            String::new()
        };
        if self.rng.chance(35) {
            // Grouped: the group columns, then aggregates.
            let keys: Vec<String> = (0..self.rng.below(columns as u64 + 1).min(2))
                .map(|_| match self.rng.below(5) {
                    0 => self.text_col(),
                    1 => format!("{} % {}", self.int_col(), self.rng.int(2, 5)),
                    _ => self.int_col(),
                })
                .collect();
            let mut out: Vec<String> = keys.clone();
            while out.len() < columns {
                let agg = match self.rng.below(9) {
                    0 => "count(*)".to_string(),
                    1 => format!("count({})", self.int_col()),
                    2 => format!("sum({})", self.int_col()),
                    3 => format!("min({})", self.int_col()),
                    4 => format!("max({})", self.int_col()),
                    5 => format!("avg({})", self.int_col()),
                    6 => format!("max({})", self.text_col()),
                    7 => format!("sum(case when {} then 1 else 0 end)", self.cond(1)),
                    _ => format!("count(distinct {})", self.int_col()),
                };
                out.push(agg);
            }
            let group = if keys.is_empty() {
                String::new()
            } else {
                format!(" group by {}", keys.join(", "))
            };
            let having = match self.rng.below(10) {
                0..=1 => format!(
                    " having count(*) {} {}",
                    self.rng.pick(&[">", "<=", "="]),
                    self.rng.int(0, 4)
                ),
                2 => format!(
                    " having sum({}) > {}",
                    self.int_col(),
                    self.rng.int(-20, 100)
                ),
                3 => format!(" having min({}) is not null", self.int_col()),
                _ => String::new(),
            };
            return format!("select {} from {from}{wh}{group}{having}", out.join(", "));
        }
        let out: Vec<String> = (0..columns)
            .map(|_| {
                if self.rng.chance(25) {
                    self.text_expr()
                } else {
                    self.int_expr(0)
                }
            })
            .collect();
        let distinct = if self.rng.chance(15) { "distinct " } else { "" };
        format!("select {distinct}{} from {from}{wh}", out.join(", "))
    }

    // A whole query; and whether its rows' order is fully determined.
    fn query(&mut self) -> (String, bool) {
        let columns = self.rng.int(1, 3) as usize;
        let mut q = self.select(columns);
        if self.rng.chance(15) {
            let op = *self
                .rng
                .pick(&["union", "union all", "intersect", "except"]);
            q = format!("{q} {op} {}", self.select(columns));
        } else if self.rng.chance(8) {
            // As a WITH query, read once or twice.
            let names: Vec<String> = (0..columns).map(|i| format!("c{i}")).collect();
            let aliased = alias_columns(&q, &names);
            let cols = names
                .iter()
                .map(|n| format!("w.{n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Joined to itself only when it reads one table: a join's rows,
            // joined to themselves, can run to hundreds of millions.
            q = if self.rng.chance(50) || self.items.len() > 1 {
                format!("with x as ({aliased}) select {cols} from x w")
            } else {
                format!("with x as ({aliased}) select {cols} from x w join x v on w.c0 = v.c0")
            };
        } else if self.rng.chance(10) {
            // As a FROM subquery: its columns named c0, c1, ...
            let names: Vec<String> = (0..columns).map(|i| format!("c{i}")).collect();
            let aliased = alias_columns(&q, &names);
            q = format!(
                "select {} from ({aliased}) s",
                names
                    .iter()
                    .map(|n| format!("s.{n}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if self.rng.chance(30) {
            // ORDER BY every output column: the order is then the rows'.
            let order: Vec<String> = (1..=columns)
                .map(|i| {
                    format!(
                        "{i} {} nulls {}",
                        self.rng.pick(&["asc", "desc"]),
                        self.rng.pick(&["first", "last"])
                    )
                })
                .collect();
            q.push_str(&format!(" order by {}", order.join(", ")));
            if self.rng.chance(60) {
                q.push_str(&format!(" limit {}", self.rng.int(0, 12)));
                if self.rng.chance(50) {
                    q.push_str(&format!(" offset {}", self.rng.int(0, 6)));
                }
            }
            return (q, true);
        }
        (q, false)
    }
}

// `select e1, e2 from ...` with each output column given a name.
fn alias_columns(query: &str, names: &[String]) -> String {
    let (head, rest) = query.split_once(" from ").expect("a select has a from");
    let body = head.strip_prefix("select ").unwrap();
    let (distinct, body) = match body.strip_prefix("distinct ") {
        Some(b) => ("distinct ", b),
        None => ("", body),
    };
    // Split on top-level commas only.
    let mut cols = vec![];
    let (mut depth, mut start) = (0, 0);
    for (i, ch) in body.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                cols.push(body[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    cols.push(body[start..].trim());
    let named: Vec<String> = cols
        .iter()
        .zip(names)
        .map(|(c, n)| format!("{c} as {n}"))
        .collect();
    format!("select {distinct}{} from {rest}", named.join(", "))
}

#[derive(Default, Debug)]
struct Tally {
    agreed: usize,
    both_refused: usize,
    sqlite_refused: usize,
    findings: Vec<String>,
}

fn sorted(a: &Answer) -> Answer {
    a.clone().map(|mut rows| {
        rows.sort();
        rows
    })
}

fn check(
    c: &Arc<Connection<MemFile>>,
    lite: &rusqlite::Connection,
    seed: u64,
    count: usize,
    subqueries: bool,
    tally: &mut Tally,
) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    for n in 0..count {
        let mut g = Gen {
            rng: &mut rng,
            items: vec![],
            subqueries,
        };
        let (q, is_ordered) = g.query();
        if std::env::var_os("SQ_ORACLE_TRACE").is_some() {
            eprintln!("seed {seed} #{n} {q}");
        }
        let started = std::time::Instant::now();
        let plain = squeal(c, &q.replace("{t}", "plain"));
        if started.elapsed() > std::time::Duration::from_secs(1) {
            eprintln!("SLOW {:?} seed {seed} #{n} {q}", started.elapsed());
        }
        let part = squeal(c, &q.replace("{t}", "part"));
        let oracle = sqlite(lite, &q.replace("{t}", "plain"));
        let (p, pt, o) = if is_ordered {
            (plain.clone(), part.clone(), oracle.clone())
        } else {
            (sorted(&plain), sorted(&part), sorted(&oracle))
        };
        let show = |a: &Answer| match a {
            Ok(rows) => format!("{} rows {:?}", rows.len(), &rows[..rows.len().min(6)]),
            Err(e) => format!("error: {e}"),
        };
        let finding = |what: &str| {
            format!(
                "seed {seed} #{n} {what}\n  {q}\n  plain:       {}\n  partitioned: {}\n  sqlite:      {}",
                show(&p),
                show(&pt),
                show(&o)
            )
        };
        if p != pt && !(p.is_err() && pt.is_err()) {
            tally.findings.push(finding("plain and partitioned differ"));
            continue;
        }
        match (&p, &o) {
            (Ok(a), Ok(b)) if a == b => tally.agreed += 1,
            (Err(_), Err(_)) => tally.both_refused += 1,
            // A query squeal answers and SQLite refuses is one the
            // generator should not have made; counted, not a finding.
            (Ok(_), Err(_)) => tally.sqlite_refused += 1,
            (Err(_), Ok(_)) => tally
                .findings
                .push(finding("squeal refuses what SQLite answers")),
            _ => tally.findings.push(finding("squeal and SQLite differ")),
        }
    }
}

fn run_oracle(seeds: &[u64], count: usize, subqueries: bool) -> Tally {
    run_oracle_on(setup(), seeds, count, subqueries)
}

fn run_oracle_on(
    (c, lite): (Arc<Connection<MemFile>>, rusqlite::Connection),
    seeds: &[u64],
    count: usize,
    subqueries: bool,
) -> Tally {
    let mut tally = Tally::default();
    for seed in seeds {
        check(&c, &lite, *seed, count, subqueries, &mut tally);
    }
    tally
}

#[test]
fn test_random_queries_agree_with_sqlite_and_across_partitioning() {
    let count = std::env::var("SQ_ORACLE_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150);
    let seeds: Vec<u64> = match std::env::var("SQ_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => vec![1, 2, 3, 4],
    };
    let tally = run_oracle(&seeds, count, false);
    println!(
        "agreed {}, both refused {}, sqlite refused {}, findings {}",
        tally.agreed,
        tally.both_refused,
        tally.sqlite_refused,
        tally.findings.len()
    );
    assert!(
        tally.findings.is_empty(),
        "{} findings:\n{}",
        tally.findings.len(),
        tally.findings[..tally.findings.len().min(15)].join("\n")
    );
    // Not vacuous: most queries are answered, and alike.
    assert!(tally.agreed * 10 >= seeds.len() * count * 7, "{tally:?}");
}

// The same, with IN / EXISTS / scalar subqueries in conditions (see
// Gen::subquery_cond) — uncorrelated and correlated, NULLs included.
#[test]
fn test_random_subqueries_agree_with_sqlite_and_across_partitioning() {
    let count = std::env::var("SQ_ORACLE_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150);
    let seeds: Vec<u64> = match std::env::var("SQ_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => vec![5, 6, 7, 8],
    };
    let tally = run_oracle(&seeds, count, true);
    println!(
        "agreed {}, both refused {}, sqlite refused {}, findings {}",
        tally.agreed,
        tally.both_refused,
        tally.sqlite_refused,
        tally.findings.len()
    );
    assert!(
        tally.findings.is_empty(),
        "{} findings:\n{}",
        tally.findings.len(),
        tally.findings[..tally.findings.len().min(15)].join("\n")
    );
    assert!(tally.agreed * 10 >= seeds.len() * count * 7, "{tally:?}");
}

// The same queries in a database opened with almost no memory (see
// store::config::OpenConfig): a query budget and scratch cache of a few
// pages, a page cache of 64 — so sorts, hash joins and grouping spill,
// and pages are evicted, where the runs above never do either.
#[test]
fn test_random_queries_agree_with_sqlite_when_everything_spills() {
    use crate::{CreateConfig, OpenConfig};

    let count = std::env::var("SQ_ORACLE_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150);
    let config = CreateConfig::default().page_size(4096).open(
        OpenConfig::default()
            .page_cache_bytes(64 * 4096)
            .temp_cache_bytes(8 * 4096)
            .query_memory_bytes(4 * 4096),
    );
    let mgr: ConMgr<MemFile> = Arc::new(ConnectionManager::with_config(config));
    let c = mgr.create_and_connect("oracle_tiny_memory").unwrap();
    c.use_schema(DEFAULT_SCHEMA_NAME).unwrap();
    let (c, lite) = setup_on(c);
    let mut tally = Tally::default();
    for (seed, subqueries) in [(9, false), (10, false), (11, true), (12, true)] {
        check(&c, &lite, seed, count, subqueries, &mut tally);
    }
    println!("agreed {}, findings {}", tally.agreed, tally.findings.len());
    assert!(
        tally.findings.is_empty(),
        "{} findings:\n{}",
        tally.findings.len(),
        tally.findings[..tally.findings.len().min(15)].join("\n")
    );
    assert!(tally.agreed * 10 >= 4 * count * 7, "{tally:?}");
}
