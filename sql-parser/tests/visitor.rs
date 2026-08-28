use std::ops::ControlFlow;

use sql_parser::{
    Expr, Statement, parse_one,
    visitor::{Visit, Visitor},
};

fn one(src: &str) -> Statement {
    parse_one(src).unwrap_or_else(|e| panic!("failed to parse {src:?}: {e:?}"))
}

#[derive(Default)]
struct Collector {
    relations: Vec<String>,
    expr_count: usize,
    literal_count: usize,
    statements: usize,
    queries: usize,
    select_cores: usize,
    table_factors: usize,
}

impl Visitor for Collector {
    type Break = ();

    fn post_visit_relation(&mut self, name: &sql_parser::ident::ObjectName) -> ControlFlow<()> {
        self.relations.push(name.to_dotted());
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
        self.expr_count += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_literal(&mut self, _lit: &sql_parser::literal::Literal) -> ControlFlow<()> {
        self.literal_count += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, _stmt: &Statement) -> ControlFlow<()> {
        self.statements += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &sql_parser::Query) -> ControlFlow<()> {
        self.queries += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _select: &sql_parser::query::SelectCore) -> ControlFlow<()> {
        self.select_cores += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        _tf: &sql_parser::query::TableFactor,
    ) -> ControlFlow<()> {
        self.table_factors += 1;
        ControlFlow::Continue(())
    }
}

#[test]
fn collects_relations_across_joins_and_subqueries() {
    let stmt = one(
        "SELECT * FROM a JOIN b ON a.id = b.a_id \
         WHERE a.id IN (SELECT c.id FROM c)",
    );
    let mut c = Collector::default();
    let _ = stmt.visit(&mut c);
    assert_eq!(c.relations, vec!["a", "b", "c"]);
    // one top-level statement, two queries (outer + the IN subquery), one
    // SelectCore per query, one TableFactor per FROM/JOIN entry
    assert_eq!(c.statements, 1);
    assert_eq!(c.queries, 2);
    assert_eq!(c.select_cores, 2);
    assert_eq!(c.table_factors, 3);
}

#[test]
fn collects_relations_from_insert_update_delete() {
    let mut c = Collector::default();
    let _ = one("INSERT INTO t VALUES (1, 'x')").visit(&mut c);
    let _ = one("UPDATE t SET a = 1 WHERE id = 2").visit(&mut c);
    let _ = one("DELETE FROM t WHERE id = 3").visit(&mut c);
    assert_eq!(c.relations, vec!["t", "t", "t"]);
}

#[test]
fn collects_relations_from_ddl() {
    let mut c = Collector::default();
    let _ = one("CREATE TABLE t (id INT, FOREIGN KEY (id) REFERENCES u (id))").visit(&mut c);
    let _ = one("CREATE INDEX idx ON t (id)").visit(&mut c);
    let _ = one("DROP TABLE a, b").visit(&mut c);
    let _ = one("ALTER TABLE t ADD COLUMN c INT").visit(&mut c);
    let _ = one("TRUNCATE TABLE t").visit(&mut c);
    let _ = one("COPY INTO t FROM @/tmp/x.csv").visit(&mut c);
    // CREATE TABLE's own name, its FOREIGN KEY's REFERENCES target `u`,
    // then the rest.
    assert_eq!(c.relations, vec!["t", "u", "t", "a", "b", "t", "t", "t"]);
}

#[test]
fn recurses_into_explain_and_prepare() {
    let mut c = Collector::default();
    let _ = one("EXPLAIN SELECT * FROM t").visit(&mut c);
    assert_eq!(c.relations, vec!["t"]);

    let mut c = Collector::default();
    let _ = one("PREPARE ins AS INSERT INTO t VALUES (?)").visit(&mut c);
    assert_eq!(c.relations, vec!["t"]);
    // one placeholder Expr node inside the VALUES row
    assert_eq!(c.expr_count, 1);
}

#[test]
fn counts_expressions_including_defaults_checks_and_functions() {
    let stmt = one(
        "CREATE TABLE t (\
            a INT DEFAULT 1 + 2, \
            b INT CHECK (b > 0), \
            CHECK (a < b)\
         )",
    );
    let mut c = Collector::default();
    let _ = stmt.visit(&mut c);
    // DEFAULT 1 + 2 -> Binary(Literal, Literal): 3 expr nodes
    // CHECK (b > 0) -> Binary(Column, Literal): 3 expr nodes
    // table-level CHECK (a < b) -> Binary(Column, Column): 3 expr nodes
    assert_eq!(c.expr_count, 9);
    assert_eq!(c.literal_count, 3);

    let stmt = one("SELECT count(*), sum(a) FROM t WHERE a > 0 AND b < 10");
    let mut c = Collector::default();
    let _ = stmt.visit(&mut c);
    // sum(a)'s arg, WHERE's two comparisons plus the AND -> at least these
    assert!(c.expr_count >= 4);
}

#[test]
fn short_circuits_on_break() {
    struct StopAtSecondRelation {
        seen: usize,
    }
    impl Visitor for StopAtSecondRelation {
        type Break = String;
        fn post_visit_relation(
            &mut self,
            name: &sql_parser::ident::ObjectName,
        ) -> ControlFlow<String> {
            self.seen += 1;
            if self.seen == 2 {
                ControlFlow::Break(name.to_dotted())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let stmt = one("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");
    let mut v = StopAtSecondRelation { seen: 0 };
    let result = stmt.visit(&mut v);
    assert_eq!(result, ControlFlow::Break("b".to_string()));
    // Never reached "c" — the walk stopped as soon as the hook broke.
    assert_eq!(v.seen, 2);
}

#[test]
fn pre_visit_runs_before_children_are_visited() {
    struct Order(Vec<&'static str>);
    impl Visitor for Order {
        type Break = ();
        fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
            self.0.push("pre");
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<()> {
            self.0.push("post");
            ControlFlow::Continue(())
        }
    }
    // A single binary expr in the WHERE clause: pre(binary), pre(left),
    // post(left), pre(right), post(right), post(binary). `SELECT *`
    // (not a column list) so the projection contributes no Expr nodes
    // of its own to muddy the sequence.
    let stmt = one("SELECT * FROM t WHERE 1 = 2");
    let mut o = Order(vec![]);
    let _ = stmt.visit(&mut o);
    assert_eq!(o.0, vec!["pre", "pre", "post", "pre", "post", "post"]);
}
