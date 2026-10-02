// Differential tests: a partitioned table must answer every query exactly as
// the same table, with the same rows, does when it is not partitioned. No
// expected results are written down: the plain table is the oracle.
//
// QUERIES is what agrees today, and must keep agreeing (pruning, when it
// comes, has to pass this unchanged). KNOWN_DIFFERENCES is what does not:
// an #[ignore]d test states that they should agree, and a second test
// fails the day one of them starts to, so the list cannot go stale.
//     cargo test -p squeal-sql partition_diff -- --ignored
use super::*;

pub(super) type Outcome = Result<Vec<Vec<ValueItem>>, String>;

// Runs `sql` (one SELECT) and returns its rows, sorted, or its error.
pub(super) fn outcome(c: &Arc<Connection<MemFile>>, sql: &str) -> Outcome {
    let mut rows = ordered(c, sql)?;
    rows.sort();
    Ok(rows)
}

// Two copies of each table, with the same rows: `plain_*` not partitioned,
// `part_*` partitioned.
//   ev(id, day, cat, amount, note): RANGE on day, four partitions, one empty.
//   reg(id, region, amount): LIST on region with a DEFAULT.
//   big(id, k, v): 3000 rows, RANGE on id — big enough that a join into the
//     plain one seeks it per outer row instead of hashing it.
//   days(day, label): a small table that is not partitioned in either.
pub(super) fn twin_conn() -> Arc<Connection<MemFile>> {
    let c = conn();
    let ev = "(id integer not null, day integer not null, cat integer, amount integer, \
              note varchar(8), primary key(id, day))";
    run(&c, &format!("create table plain_ev {ev}")).unwrap();
    run(
        &c,
        &format!(
            "create table part_ev {ev} partition by range (day) ( \
             partition p0 values less than (10), partition p1 values less than (20), \
             partition p2 values less than (30), partition p3 values less than maxvalue)"
        ),
    )
    .unwrap();
    let reg = "(id integer not null, region varchar(4), amount integer)";
    run(&c, &format!("create table plain_reg {reg}")).unwrap();
    run(
        &c,
        &format!(
            "create table part_reg {reg} partition by list (region) ( \
             partition west values in ('ca', 'wa'), partition east values in ('ny'), \
             partition other default)"
        ),
    )
    .unwrap();
    run(
        &c,
        "create table days (day integer not null, label varchar(8), primary key(day))",
    )
    .unwrap();
    let big = "(id integer not null, k integer, v integer, primary key(id))";
    run(&c, &format!("create table plain_big {big}")).unwrap();
    run(
        &c,
        &format!(
            "create table part_big {big} partition by range (id) ( \
             partition b0 values less than (1000), partition b1 values less than (2000), \
             partition b2 values less than maxvalue)"
        ),
    )
    .unwrap();
    for t in ["plain", "part"] {
        for chunk in 0..6 {
            let values: Vec<String> = (chunk * 500..(chunk + 1) * 500)
                .map(|id| format!("({id}, {}, {})", id % 50, id % 3))
                .collect();
            run(
                &c,
                &format!("insert into {t}_big values {}", values.join(", ")),
            )
            .unwrap();
        }
        run(&c, &format!("create index {t}_big_k on {t}_big (k)")).unwrap();
        run(&c, &format!("analyze table {t}_big")).unwrap();
        // days 0..19 and 30..49: p2 (20..29) stays empty.
        for id in 0..40 {
            let day = if id < 20 { id } else { id + 10 };
            let cat = if id % 7 == 0 {
                "null".to_string()
            } else {
                (id % 3).to_string()
            };
            run(
                &c,
                &format!(
                    "insert into {t}_ev values ({id}, {day}, {cat}, {}, 'n{}')",
                    id * 10,
                    id % 5
                ),
            )
            .unwrap();
        }
        for (id, region) in ["'ca'", "'ny'", "'wa'", "'tx'", "null", "'ca'", "'fl'"]
            .iter()
            .enumerate()
        {
            run(
                &c,
                &format!("insert into {t}_reg values ({id}, {region}, {})", id * 5),
            )
            .unwrap();
        }
        run(&c, &format!("create index {t}_ev_cat on {t}_ev (cat)")).unwrap();
        run(&c, &format!("analyze table {t}_ev")).unwrap();
        run(&c, &format!("analyze table {t}_reg")).unwrap();
    }
    for day in [0, 5, 9, 10, 19, 25, 30, 49, 60] {
        run(&c, &format!("insert into days values ({day}, 'd{day}')")).unwrap();
    }
    run(&c, "analyze table days").unwrap();
    c
}

// `query` with every `{t}` replaced by "plain" and by "part": what each
// answers, or None when they agree.
pub(super) fn difference(c: &Arc<Connection<MemFile>>, query: &str) -> Option<(Outcome, Outcome)> {
    let plain = outcome(c, &query.replace("{t}", "plain"));
    let part = outcome(c, &query.replace("{t}", "part"));
    let same = match (&plain, &part) {
        (Ok(a), Ok(b)) => a == b,
        // Both refuse: the same answer, whatever each one's wording names.
        (Err(_), Err(_)) => true,
        _ => false,
    };
    (!same).then_some((plain, part))
}

pub(super) const QUERIES: &[&str] = &[
    // Scans and filters.
    "select * from {t}_ev",
    "select id from {t}_ev where day = 15",
    "select id from {t}_ev where day < 10",
    "select id from {t}_ev where day >= 19 and day <= 31",
    "select id from {t}_ev where day > 100",
    "select id from {t}_ev where day = 25",
    "select id from {t}_ev where id = 7 and day = 7",
    "select id from {t}_ev where id = 7",
    "select id from {t}_ev where id > 30",
    "select id from {t}_ev where cat = 1",
    "select id from {t}_ev where cat = 1 and day > 12",
    "select id from {t}_ev where cat = 1 or day = 3",
    "select id from {t}_ev where not (day < 35)",
    "select id from {t}_ev where note = 'n3' and amount > 100",
    "select id, day + amount from {t}_ev where day % 2 = 0",
    "select * from {t}_reg",
    "select id from {t}_reg where region = 'ca'",
    "select id from {t}_reg where region = 'tx'",
    "select id from {t}_reg where region > 'm'",
    "select id from {t}_reg where region <> 'ca'",
    // Aggregates, grouping, DISTINCT.
    "select count(*) from {t}_ev",
    "select count(*), sum(amount), min(day), max(day) from {t}_ev where day > 5",
    "select count(*) from {t}_ev where day = 25",
    "select cat, count(*) from {t}_ev group by cat",
    "select cat, sum(amount) from {t}_ev group by cat having count(*) > 12",
    "select day / 10, count(*) from {t}_ev group by day / 10",
    "select note, cat, max(amount) from {t}_ev group by note, cat",
    "select distinct cat from {t}_ev",
    "select distinct note from {t}_ev where day < 15",
    "select region, sum(amount) from {t}_reg group by region",
    // Joins: the partitioned table on the left.
    "select e.id, d.label from {t}_ev e join days d on e.day = d.day",
    "select e.id, d.label from {t}_ev e inner join days d on e.day = d.day where d.day > 9",
    "select e.id, d.label from {t}_ev e left join days d on e.day = d.day",
    "select e.id, d.label from {t}_ev e right join days d on e.day = d.day",
    "select e.id, d.label from {t}_ev e full join days d on e.day = d.day",
    "select e.id, d.day from {t}_ev e cross join days d where e.id < 3",
    // Joins: the partitioned table on the right (the side sought per row).
    "select d.label, e.id from days d join {t}_ev e on e.day = d.day",
    "select d.label, e.id from days d left join {t}_ev e on e.day = d.day",
    "select d.label, e.id from days d right join {t}_ev e on e.day = d.day",
    "select d.label, e.id from days d full join {t}_ev e on e.day = d.day",
    "select d.label, e.id from days d join {t}_ev e on e.day = d.day and e.id = d.day",
    "select d.label, e.id from days d join {t}_ev e on e.id = d.day and e.day = d.day",
    "select d.label, e.id from days d join {t}_ev e on e.cat = d.day",
    "select d.label, e.id from days d join {t}_ev e on e.day = d.day where e.cat = 1",
    "select d.label, e.id from days d join {t}_ev e on e.day = d.day where d.day = 19",
    // Comma joins, self joins, three tables.
    "select e.id, d.label from {t}_ev e, days d where e.day = d.day",
    "select e.id, d.label from days d, {t}_ev e where e.day = d.day and e.id = d.day",
    "select a.id, b.id from {t}_ev a join {t}_ev b on a.id = b.id and a.day = b.day where a.id < 5",
    "select a.id, b.id from {t}_ev a join {t}_ev b on a.day = b.id where a.cat = 1",
    "select a.id, b.id from {t}_ev a, {t}_ev b where a.id = b.day and b.cat = 0",
    "select e.id, r.region from {t}_ev e join {t}_reg r on e.id = r.id",
    "select e.id, r.region, d.label from {t}_ev e join {t}_reg r on e.id = r.id \
     join days d on d.day = e.day",
    "select d.label, e.id, r.region from days d join {t}_ev e on e.day = d.day \
     join {t}_reg r on r.id = e.id",
    "select r.region, count(*) from {t}_reg r join {t}_ev e on e.cat = r.id group by r.region",
    // Joins feeding grouping and ordering.
    "select d.label, count(*), sum(e.amount) from days d join {t}_ev e on e.day = d.day \
     group by d.label",
    "select e.day, count(*) from {t}_ev e join days d on e.day = d.day group by e.day",
    // Joins into the big table: sought per outer row when it is plain.
    "select d.day, b.k from days d join {t}_big b on b.id = d.day",
    "select d.day, b.k from days d left join {t}_big b on b.id = d.day",
    "select d.day, b.id from days d join {t}_big b on b.k = d.day",
    "select d.day, b.k from days d join {t}_big b on b.id = d.day where b.v = 1",
    "select d.day, b.k from days d, {t}_big b where b.id = d.day and b.v = 1",
    "select e.id, b.k from {t}_ev e join {t}_big b on b.id = e.id and b.v = e.cat",
    "select e.id, b.k from {t}_ev e join {t}_big b on b.id = e.amount",
    "select d.day, count(*) from days d join {t}_big b on b.k = d.day group by d.day",
    "select b.id from {t}_big b where b.id = 1500",
    "select b.id from {t}_big b where b.k = 7 and b.id > 2500",
    "select b.id from {t}_big b where b.id >= 990 and b.id < 1010",
    "select count(*) from {t}_big b where b.k < 3",
    "select k, count(*) from {t}_big group by k",
    // Derived tables.
    "select s.id from (select id, day from {t}_ev where day < 12) s where s.id > 5",
    "select s.cat, s.n from (select cat, count(*) as n from {t}_ev group by cat) s",
    "select d.label, s.id from days d join (select id, day from {t}_ev) s on s.day = d.day",
];

// Queries whose ORDER BY decides the order of every row (its key is
// unique): compared in the order returned, not sorted first. Partitions are
// read one after the other, so nothing but the sort gives this order.
pub(super) const ORDERED: &[&str] = &[
    "select id from {t}_ev order by day desc limit 5",
    "select id from {t}_ev order by id limit 7",
    "select id, day from {t}_ev order by day, id",
    "select id from {t}_ev where cat = 2 order by amount desc limit 3",
    "select id from {t}_ev order by id desc",
    "select cat, count(*) as n from {t}_ev group by cat order by cat",
    "select b.id from {t}_big b order by b.id limit 5",
    "select b.id from {t}_big b order by b.id desc limit 5",
    "select b.id from {t}_big b where b.id > 995 order by b.id limit 10",
    "select e.id from {t}_ev e join days d on e.day = d.day order by e.id desc limit 4",
    "select e.id from days d join {t}_ev e on e.day = d.day order by e.day limit 4",
];

// What the two answer differently today. Each is a join INTO the table
// whose ON has more than column equalities. The plain table is sought per
// outer row (a nested-loop join), which checks the whole ON condition; a
// table in several partitions is not sought (see
// QueryVisitor::nested_loop_join), so the join falls back to a hash join,
// which refuses such a condition.
pub(super) const KNOWN_DIFFERENCES: &[&str] = &[
    "select d.day, b.k from days d join {t}_big b on b.id = d.day and b.v = 1",
    "select d.day, b.k from days d join {t}_big b on b.id = d.day and b.id > 9",
    "select d.day, b.k from days d join {t}_big b on b.id = d.day and d.day < 20",
    "select d.day, b.k from days d left join {t}_big b on b.id = d.day and b.v = 1",
];

fn show(o: &Outcome) -> String {
    match o {
        Ok(rows) => format!("{} rows: {rows:?}", rows.len()),
        Err(e) => format!("error: {e}"),
    }
}

// Every difference among `queries`, described.
fn differences(c: &Arc<Connection<MemFile>>, queries: &[&str]) -> Vec<String> {
    queries
        .iter()
        .filter_map(|q| {
            difference(c, q).map(|(plain, part)| {
                format!(
                    "{q}\n  plain: {}\n  partitioned: {}",
                    show(&plain),
                    show(&part)
                )
            })
        })
        .collect()
}

#[test]
fn test_a_partitioned_table_answers_every_query_as_the_plain_one_does() {
    let c = twin_conn();
    // Not vacuous: the oracle answers each of these (two errors agreeing
    // would say nothing).
    for q in QUERIES {
        if let Err(e) = outcome(&c, &q.replace("{t}", "plain")) {
            panic!("the plain table does not answer {q}: {e}");
        }
    }
    let diffs = differences(&c, QUERIES);
    assert!(
        diffs.is_empty(),
        "{} differ:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

#[test]
fn test_a_partitioned_table_returns_ordered_rows_in_the_same_order() {
    let c = twin_conn();
    for q in ORDERED {
        let plain = ordered(&c, &q.replace("{t}", "plain"));
        let part = ordered(&c, &q.replace("{t}", "part"));
        assert!(
            plain.is_ok(),
            "the plain table does not answer {q}: {plain:?}"
        );
        assert_eq!(part, plain, "{q}");
    }
}

#[test]
#[ignore = "known failure: a join into a partitioned table cannot have non-equality ON terms"]
fn test_joins_with_extra_on_terms_into_a_partitioned_table_answer_as_the_plain_one_does() {
    let c = twin_conn();
    let diffs = differences(&c, KNOWN_DIFFERENCES);
    assert!(
        diffs.is_empty(),
        "{} differ:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

// Keeps KNOWN_DIFFERENCES honest: when one of them starts agreeing, this
// fails, and the query belongs in QUERIES from then on.
#[test]
fn test_every_known_difference_is_still_one() {
    let c = twin_conn();
    for q in KNOWN_DIFFERENCES {
        assert!(
            difference(&c, q).is_some(),
            "{q}\nnow agrees: move it from KNOWN_DIFFERENCES to QUERIES"
        );
    }
}

// Runs `sql` and returns its rows in the order returned.
pub(super) fn ordered(c: &Arc<Connection<MemFile>>, sql: &str) -> Outcome {
    let mut stmt = c.clone().create_statement(sql).map_err(|e| e.to_string())?;
    stmt.execute().map_err(|e| e.to_string())?;
    let Some(Some(ResultType::StreamingResult(mut stream))) = stmt.results.pop() else {
        return Err("no streaming result".into());
    };
    let mut rows = vec![];
    loop {
        match stream.next_result() {
            Ok(Some(row)) => rows.push(row.values().to_vec()),
            Ok(None) => return Ok(rows),
            Err(e) => return Err(e.to_string()),
        }
    }
}
