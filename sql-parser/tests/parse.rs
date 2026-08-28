use sql_parser::{
    Expr, Statement,
    ddl::{AlterTableOp, ColumnOption, TableConstraintKind},
    dml::InsertSource,
    expr::{BinaryOp, FunctionArg, Placeholder, UnaryOp},
    literal::{Literal, NumberValue},
    parse_one, parse_sql,
    query::{JoinOperator, SelectItem},
};

fn one(src: &str) -> Statement {
    parse_one(src).unwrap_or_else(|e| panic!("failed to parse {src:?}: {e:?}"))
}

fn select(src: &str) -> sql_parser::Query {
    match one(src) {
        Statement::Select(s) => *s,
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn where_expr(src: &str) -> Expr {
    select(src)
        .core()
        .where_clause
        .clone()
        .expect("expected WHERE")
        .expr
}

#[test]
fn select_basic() {
    let s = select("SELECT a, b FROM t");
    assert_eq!(s.core().projection.len(), 2);
    let from = s.core().from.clone().unwrap();
    match &from.tables.head.relation {
        sql_parser::query::TableFactor::Table { name, .. } => assert_eq!(name.to_dotted(), "t"),
        other => panic!("expected plain table, got {other:?}"),
    }
    assert!(s.core().where_clause.clone().is_none());
}

#[test]
fn select_star_and_qualified_star() {
    let s = select("SELECT *, t.*, a AS x FROM t");
    let items: Vec<_> = s.core().projection.items().collect();
    assert!(matches!(items[0], SelectItem::Wildcard(_)));
    assert!(matches!(items[1], SelectItem::QualifiedWildcard(..)));
    match items[2] {
        SelectItem::Expr { alias: Some(a), .. } => assert_eq!(a.name.value, "x"),
        other => panic!("expected aliased expr, got {other:?}"),
    }
}

#[test]
fn select_distinct_group_order_limit() {
    let s = select(
        "SELECT DISTINCT city, count(*) FROM users \
         WHERE age >= 21 GROUP BY city HAVING count(*) > 1 \
         ORDER BY city DESC LIMIT 10 OFFSET 5",
    );
    assert!(s.core().distinct.is_some());
    assert!(s.core().group_by.is_some());
    assert!(s.core().having.is_some());
    let order = s.order_by.unwrap();
    assert!(order.items.head.direction.unwrap().is_right()); // DESC
    assert_eq!(s.limit.unwrap().count_i64(), Some(10));
    assert_eq!(s.offset.unwrap().count_i64(), Some(5));
}

#[test]
fn select_joins() {
    let s = select(
        "SELECT * FROM a INNER JOIN b ON a.id = b.a_id \
         LEFT OUTER JOIN c ON b.id = c.b_id \
         CROSS JOIN d",
    );
    let t = &s.core().from.clone().unwrap().tables.head;
    assert_eq!(t.joins.len(), 3);
    assert!(matches!(t.joins[0].operator, JoinOperator::Inner(..)));
    assert!(matches!(t.joins[1].operator, JoinOperator::LeftOuter(..)));
    assert!(matches!(t.joins[2].operator, JoinOperator::Cross(..)));
    assert!(t.joins[2].constraint.is_none());
}

#[test]
fn join_using() {
    let s = select("SELECT * FROM a JOIN b USING (id, org_id)");
    let t = &s.core().from.clone().unwrap().tables.head;
    match t.joins[0].constraint.as_ref().unwrap() {
        sql_parser::query::JoinConstraint::Using(_, _, cols, _) => assert_eq!(cols.len(), 2),
        other => panic!("expected USING, got {other:?}"),
    }
}

#[test]
fn expr_precedence() {
    // 1 + 2 * 3 => 1 + (2 * 3)
    let e = where_expr("SELECT a FROM t WHERE x = 1 + 2 * 3");
    let Expr::Binary {
        op: BinaryOp::Eq,
        right,
        ..
    } = e
    else {
        panic!("expected =, got another shape");
    };
    let Expr::Binary {
        op: BinaryOp::Plus,
        right: mul,
        ..
    } = *right
    else {
        panic!("expected +");
    };
    assert!(matches!(
        *mul,
        Expr::Binary {
            op: BinaryOp::Multiply,
            ..
        }
    ));
}

#[test]
fn expr_and_or_not() {
    // NOT a = 1 AND b = 2 OR c = 3  =>  ((NOT (a=1)) AND (b=2)) OR (c=3)
    let e = where_expr("SELECT x FROM t WHERE NOT a = 1 AND b = 2 OR c = 3");
    let Expr::Binary {
        op: BinaryOp::Or,
        left,
        ..
    } = e
    else {
        panic!("expected OR at top");
    };
    let Expr::Binary {
        op: BinaryOp::And,
        left: not_side,
        ..
    } = *left
    else {
        panic!("expected AND under OR");
    };
    assert!(matches!(
        *not_side,
        Expr::Unary {
            op: UnaryOp::Not,
            ..
        }
    ));
}

#[test]
fn expr_between_and() {
    // BETWEEN binds its own AND: (a BETWEEN 1 AND 10) AND b
    let e = where_expr("SELECT x FROM t WHERE a BETWEEN 1 AND 10 AND b = 2");
    let Expr::Binary {
        op: BinaryOp::And,
        left,
        ..
    } = e
    else {
        panic!("expected top-level AND");
    };
    assert!(matches!(*left, Expr::Between { negated: false, .. }));
}

#[test]
fn expr_predicates() {
    assert!(matches!(
        where_expr("SELECT x FROM t WHERE a IS NOT NULL"),
        Expr::IsNull { negated: true, .. }
    ));
    match where_expr("SELECT x FROM t WHERE a NOT IN (1, 2, 3)") {
        Expr::InList { negated, list, .. } => {
            assert!(negated);
            assert_eq!(list.len(), 3);
        }
        other => panic!("expected IN list, got {other:?}"),
    }
    assert!(matches!(
        where_expr("SELECT x FROM t WHERE name LIKE 'a%'"),
        Expr::Like {
            negated: false,
            case_insensitive: false,
            ..
        }
    ));
    assert!(matches!(
        where_expr("SELECT x FROM t WHERE name NOT ILIKE '%b'"),
        Expr::Like {
            negated: true,
            case_insensitive: true,
            ..
        }
    ));
}

#[test]
fn expr_neq_spellings() {
    for src in [
        "SELECT x FROM t WHERE a <> 1",
        "SELECT x FROM t WHERE a != 1",
    ] {
        assert!(matches!(
            where_expr(src),
            Expr::Binary {
                op: BinaryOp::NotEq,
                ..
            }
        ));
    }
}

#[test]
fn expr_functions_case_cast() {
    match where_expr("SELECT x FROM t WHERE f(DISTINCT a, *, 1 + 2) = 1") {
        Expr::Binary { left, .. } => match *left {
            Expr::Function {
                name,
                distinct,
                args,
                over,
            } => {
                assert_eq!(name.value, "f");
                assert!(distinct);
                assert_eq!(args.len(), 3);
                assert!(matches!(args[1], FunctionArg::Wildcard(_)));
                assert!(over.is_none());
            }
            other => panic!("expected function, got {other:?}"),
        },
        other => panic!("expected =, got {other:?}"),
    }

    assert!(matches!(
        where_expr("SELECT x FROM t WHERE CAST(a AS INT) = 1"),
        Expr::Binary { .. }
    ));
    assert!(matches!(
        where_expr("SELECT x FROM t WHERE a::BIGINT = 1"),
        Expr::Binary { .. }
    ));

    match where_expr("SELECT x FROM t WHERE CASE WHEN a = 1 THEN 2 ELSE 3 END = 2") {
        Expr::Binary { left, .. } => match *left {
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                assert!(operand.is_none());
                assert_eq!(when_then.len(), 1);
                assert!(else_expr.is_some());
            }
            other => panic!("expected CASE, got {other:?}"),
        },
        other => panic!("expected =, got {other:?}"),
    }
}

#[test]
fn expr_literals_and_placeholders() {
    let e = where_expr("SELECT x FROM t WHERE s = 'it''s' AND f = -1.5 AND b = TRUE AND n IS NULL");
    // just make sure the whole thing parsed; check the string unescape
    fn find_string(e: &Expr) -> Option<&str> {
        match e {
            Expr::Literal(Literal::String(s)) => Some(&s.value),
            Expr::Binary { left, right, .. } => find_string(left).or_else(|| find_string(right)),
            Expr::IsNull { expr, .. } => find_string(expr),
            _ => None,
        }
    }
    assert_eq!(find_string(&e), Some("it's"));

    match where_expr("SELECT x FROM t WHERE a = ? AND b = $2 AND c = :name") {
        e @ Expr::Binary { .. } => {
            fn placeholders(e: &Expr, out: &mut Vec<Placeholder>) {
                match e {
                    Expr::Placeholder(p) => out.push(p.clone()),
                    Expr::Binary { left, right, .. } => {
                        placeholders(left, out);
                        placeholders(right, out);
                    }
                    _ => {}
                }
            }
            let mut ps = vec![];
            placeholders(&e, &mut ps);
            assert_eq!(ps.len(), 3);
            assert!(matches!(ps[0], Placeholder::Anonymous(_)));
            assert!(matches!(ps[1], Placeholder::Positional(_, 2)));
            assert!(matches!(ps[2], Placeholder::Named(_, ref n) if n == "name"));
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn insert_values_and_select() {
    let Statement::Insert(i) = one("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')") else {
        panic!("expected INSERT");
    };
    assert_eq!(i.table.to_dotted(), "t");
    let (_, cols, _) = i.columns.unwrap();
    assert_eq!(cols.len(), 2);
    let InsertSource::Values(_, rows) = i.source else {
        panic!("expected VALUES");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.head.exprs().count(), 2);

    let Statement::Insert(i) = one("INSERT INTO t SELECT a, b FROM s WHERE a > 0") else {
        panic!("expected INSERT");
    };
    assert!(matches!(i.source, InsertSource::Select(_)));
}

#[test]
fn update_and_delete() {
    let Statement::Update(u) = one("UPDATE t SET a = a + 1, b = 'x' WHERE id = 3") else {
        panic!("expected UPDATE");
    };
    assert_eq!(u.assignments.len(), 2);
    assert!(u.where_clause.is_some());

    let Statement::Delete(d) = one("DELETE FROM t WHERE id IN (1, 2)") else {
        panic!("expected DELETE");
    };
    assert_eq!(d.table.to_dotted(), "t");
    assert!(d.where_clause.is_some());
}

#[test]
fn create_table() {
    let Statement::CreateTable(c) = one("CREATE TABLE IF NOT EXISTS orders (\
            id INT PRIMARY KEY, \
            user_id BIGINT NOT NULL REFERENCES users (id), \
            amount DECIMAL(10, 2) DEFAULT 0, \
            note VARCHAR(255) NULL, \
            CONSTRAINT uq UNIQUE (user_id, note), \
            FOREIGN KEY (user_id) REFERENCES users (id)\
         )")
    else {
        panic!("expected CREATE TABLE");
    };
    assert!(c.if_not_exists.is_some());
    assert_eq!(c.name.to_dotted(), "orders");
    let cols: Vec<_> = c.columns().collect();
    assert_eq!(cols.len(), 4);
    assert!(matches!(cols[0].options[0], ColumnOption::PrimaryKey(..)));
    assert!(matches!(cols[1].options[0], ColumnOption::NotNull(..)));
    assert!(matches!(cols[1].options[1], ColumnOption::References(..)));
    assert!(matches!(cols[2].options[0], ColumnOption::Default(..)));
    let constraints: Vec<_> = c.constraints().collect();
    assert_eq!(constraints.len(), 2);
    assert!(constraints[0].name.is_some());
    assert!(matches!(
        constraints[0].kind,
        TableConstraintKind::Unique(..)
    ));
    assert!(matches!(
        constraints[1].kind,
        TableConstraintKind::ForeignKey(..)
    ));
}

#[test]
fn drop_and_alter_table() {
    let Statement::DropTable(d) = one("DROP TABLE IF EXISTS a, b.c") else {
        panic!("expected DROP TABLE");
    };
    assert!(d.if_exists.is_some());
    assert_eq!(d.names.len(), 2);
    assert_eq!(d.names.last().to_dotted(), "b.c");

    let Statement::AlterTable(a) = one("ALTER TABLE t ADD COLUMN c INT NOT NULL") else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::AddColumn(..)));

    let Statement::AlterTable(a) = one("ALTER TABLE t DROP COLUMN c") else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::DropColumn(..)));

    let Statement::AlterTable(a) = one("ALTER TABLE t RENAME TO t2") else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::RenameTo(..)));

    let Statement::AlterTable(a) = one("ALTER TABLE t RENAME COLUMN a TO b") else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::RenameColumn(..)));
}

#[test]
fn transactions() {
    assert!(matches!(one("BEGIN"), Statement::StartTransaction(_)));
    assert!(matches!(
        one("BEGIN TRANSACTION"),
        Statement::StartTransaction(_)
    ));
    assert!(matches!(
        one("START TRANSACTION"),
        Statement::StartTransaction(_)
    ));
    assert!(matches!(one("COMMIT"), Statement::Commit(_)));
    assert!(matches!(one("ROLLBACK"), Statement::Rollback(_)));
}

#[test]
fn multiple_statements() {
    let stmts =
        parse_sql("BEGIN; INSERT INTO t VALUES (1); UPDATE t SET a = 2 WHERE id = 1; COMMIT;")
            .unwrap();
    assert_eq!(stmts.len(), 4);
    assert!(matches!(stmts[0], Statement::StartTransaction(_)));
    assert!(matches!(stmts[3], Statement::Commit(_)));
}

#[test]
fn comments_and_case_insensitivity() {
    let stmts = parse_sql(
        "-- leading comment\n\
         select A /* inline */ from T where a = 1;",
    )
    .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn quoted_identifiers() {
    let s = select("SELECT \"select\", \"weird \"\"name\"\"\" FROM \"table\"");
    let items: Vec<_> = s.core().projection.items().collect();
    match items[1] {
        SelectItem::Expr {
            expr: Expr::Column(c),
            ..
        } => {
            assert_eq!(c.parts.head.value, "weird \"name\"");
            assert!(c.parts.head.quoted);
        }
        other => panic!("expected quoted column, got {other:?}"),
    }
}

#[test]
fn number_literals() {
    let Statement::Insert(i) = one("INSERT INTO t VALUES (1, 1.5, .5, -2)") else {
        panic!("expected INSERT");
    };
    let InsertSource::Values(_, rows) = i.source else {
        panic!("expected VALUES");
    };
    let exprs: Vec<_> = rows.head.exprs().collect();
    assert!(matches!(
        exprs[0],
        Expr::Literal(Literal::Number(n)) if n.value == NumberValue::Integer(1)
    ));
    assert!(matches!(
        exprs[1],
        Expr::Literal(Literal::Number(n)) if n.value == NumberValue::Float(1.5)
    ));
    assert!(matches!(
        exprs[2],
        Expr::Literal(Literal::Number(n)) if n.value == NumberValue::Float(0.5)
    ));
    assert!(matches!(
        exprs[3],
        Expr::Unary {
            op: UnaryOp::Minus,
            ..
        }
    ));
}

#[test]
fn errors_have_spans() {
    let errs = parse_sql("SELECT FROM t").unwrap_err();
    assert!(!errs.is_empty());
    assert!(errs[0].span.is_some());

    let errs = parse_sql("SELECT a FROM t WHERE").unwrap_err();
    assert!(!errs.is_empty());

    assert!(parse_sql("SELECT 'unterminated").is_err());
}

#[test]
fn table_element_debris_rejected() {
    // trailing garbage must not be silently ignored
    assert!(parse_sql("SELECT a FROM t extra_token !").is_err());
    assert!(parse_sql("CREATE TABLE t (a INT,)").is_err());
}

#[test]
fn scalar_subquery() {
    let e = where_expr("SELECT a FROM t WHERE b = (SELECT max(x) FROM u)");
    let Expr::Binary { right, .. } = e else {
        panic!("expected =");
    };
    assert!(matches!(*right, Expr::Subquery(_)));
}

#[test]
fn in_subquery() {
    match where_expr("SELECT a FROM t WHERE id NOT IN (SELECT t_id FROM u WHERE ok = TRUE)") {
        Expr::InSubquery { negated, query, .. } => {
            assert!(negated);
            assert!(query.core().where_clause.is_some());
        }
        other => panic!("expected IN subquery, got {other:?}"),
    }
    // plain lists still work
    assert!(matches!(
        where_expr("SELECT a FROM t WHERE id IN (1, 2)"),
        Expr::InList { .. }
    ));
}

#[test]
fn exists_subquery() {
    assert!(matches!(
        where_expr("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.id)"),
        Expr::Exists { .. }
    ));
    // NOT EXISTS = Unary(Not, Exists)
    match where_expr("SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM u)") {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => assert!(matches!(*expr, Expr::Exists { .. })),
        other => panic!("expected NOT EXISTS, got {other:?}"),
    }
}

#[test]
fn nested_subqueries() {
    // three levels deep
    let e = where_expr(
        "SELECT a FROM t WHERE b IN (SELECT c FROM u WHERE d = (SELECT max(e) FROM v WHERE f IN (SELECT g FROM w)))",
    );
    assert!(matches!(e, Expr::InSubquery { .. }));
}

#[test]
fn derived_table() {
    let s =
        select("SELECT x FROM (SELECT a AS x FROM t WHERE a > 0) AS sub JOIN u ON sub.x = u.id");
    let table = &s.core().from.clone().unwrap().tables.head;
    match &table.relation {
        sql_parser::query::TableFactor::Derived { query, alias, .. } => {
            assert!(query.core().where_clause.is_some());
            assert_eq!(alias.as_ref().unwrap().name.value, "sub");
        }
        other => panic!("expected derived table, got {other:?}"),
    }
    assert_eq!(table.joins.len(), 1);
}

#[test]
fn union_and_set_ops() {
    use sql_parser::query::SetOperator;
    let q = select(
        "SELECT a FROM t UNION ALL SELECT b FROM u EXCEPT SELECT c FROM v ORDER BY 1 LIMIT 3",
    );
    assert_eq!(q.compounds.len(), 2);
    match &q.compounds[0].op {
        SetOperator::Union(_, all) => assert!(all.as_ref().unwrap().is_left()),
        other => panic!("expected UNION ALL, got {other:?}"),
    }
    assert!(matches!(q.compounds[1].op, SetOperator::Except(_)));
    // ORDER BY / LIMIT apply to the whole compound, at the Query level
    assert!(q.order_by.is_some());
    assert_eq!(q.limit.unwrap().count_i64(), Some(3));
}

#[test]
fn with_cte() {
    let q = select(
        "WITH regional AS (SELECT region, sum(amount) AS total FROM orders GROUP BY region), \
              top AS (SELECT region FROM regional WHERE total > 100) \
         SELECT r.region FROM regional r JOIN top USING (region)",
    );
    let with = q.with.unwrap();
    assert!(with.recursive.is_none());
    assert_eq!(with.ctes.len(), 2);
    assert_eq!(with.ctes.head.name.value, "regional");

    let q = select(
        "WITH RECURSIVE cnt (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 10) SELECT n FROM cnt",
    );
    let with = q.with.unwrap();
    assert!(with.recursive.is_some());
    let (_, cols, _) = with.ctes.head.columns.as_ref().unwrap();
    assert_eq!(cols.len(), 1);
    // the CTE body itself is a compound query
    assert_eq!(with.ctes.head.query.compounds.len(), 1);
}

#[test]
fn cte_in_insert() {
    let Statement::Insert(i) =
        one("INSERT INTO summary WITH s AS (SELECT a FROM t) SELECT a FROM s")
    else {
        panic!("expected INSERT");
    };
    let InsertSource::Select(q) = i.source else {
        panic!("expected SELECT source");
    };
    assert!(q.with.is_some());
}

#[test]
fn check_constraints() {
    let Statement::CreateTable(c) =
        one("CREATE TABLE t (a INT CHECK (a > 0), b INT, CHECK (b > a))")
    else {
        panic!("expected CREATE TABLE");
    };
    let cols: Vec<_> = c.columns().collect();
    assert!(matches!(cols[0].options[0], ColumnOption::Check(..)));
    let cons: Vec<_> = c.constraints().collect();
    assert!(matches!(cons[0].kind, TableConstraintKind::Check(..)));
}

#[test]
fn quantified_comparisons() {
    use sql_parser::expr::Quantifier;
    match where_expr("SELECT a FROM t WHERE a > ALL (SELECT b FROM u)") {
        Expr::QuantifiedComparison { op, quantifier, .. } => {
            assert_eq!(op, BinaryOp::Gt);
            assert!(matches!(quantifier, Quantifier::All(_)));
        }
        other => panic!("expected quantified comparison, got {other:?}"),
    }
    assert!(matches!(
        where_expr("SELECT a FROM t WHERE a = ANY (SELECT b FROM u)"),
        Expr::QuantifiedComparison { .. }
    ));
    // `any` with a non-query argument is just a function call
    match where_expr("SELECT a FROM t WHERE a = any(1)") {
        Expr::Binary { right, .. } => {
            assert!(matches!(*right, Expr::Function { .. }));
        }
        other => panic!("expected function call, got {other:?}"),
    }
}

#[test]
fn window_functions() {
    use sql_parser::expr::{WindowFrameBound, WindowFrameExtent};
    let s = select(
        "SELECT rank() OVER (PARTITION BY dept ORDER BY salary DESC), \
                sum(x) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
                count(*) OVER () \
         FROM emp",
    );
    let items: Vec<_> = s.core().projection.items().collect();
    let over = |i: usize| match items[i] {
        SelectItem::Expr {
            expr: Expr::Function { over: Some(o), .. },
            ..
        } => &o.spec,
        other => panic!("expected windowed function, got {other:?}"),
    };
    let spec = over(0);
    assert!(spec.partition_by.is_some());
    assert!(spec.order_by.is_some());
    assert!(spec.frame.is_none());

    let spec = over(1);
    assert!(spec.partition_by.is_none());
    match &spec.frame.as_ref().unwrap().extent {
        WindowFrameExtent::Between(_, low, _, high) => {
            assert!(matches!(low, WindowFrameBound::UnboundedPreceding(..)));
            assert!(matches!(high, WindowFrameBound::CurrentRow(..)));
        }
        other => panic!("expected BETWEEN frame, got {other:?}"),
    }

    let spec = over(2);
    assert!(spec.partition_by.is_none() && spec.order_by.is_none() && spec.frame.is_none());
}

#[test]
fn parenthesized_set_operands() {
    use sql_parser::query::SetOperand;
    let q = select("(SELECT a FROM t ORDER BY a LIMIT 5) UNION ALL (SELECT b FROM u) ORDER BY 1");
    assert!(matches!(q.body, SetOperand::Paren(..)));
    // the inner query keeps its own ORDER BY/LIMIT
    let SetOperand::Paren(_, inner, _) = &q.body else {
        unreachable!()
    };
    assert_eq!(inner.limit.as_ref().unwrap().count_i64(), Some(5));
    assert_eq!(q.compounds.len(), 1);
    assert!(matches!(q.compounds[0].operand, SetOperand::Paren(..)));
    assert!(q.order_by.is_some());
}

#[test]
fn order_by_nulls() {
    let q = select("SELECT a FROM t ORDER BY a DESC NULLS LAST, b NULLS FIRST");
    let items: Vec<_> = q.order_by.unwrap().items.items().cloned().collect();
    let (_, dir) = items[0].nulls.as_ref().unwrap();
    assert!(dir.is_right()); // LAST
    assert!(items[0].direction.is_some());
    let (_, dir) = items[1].nulls.as_ref().unwrap();
    assert!(dir.is_left()); // FIRST
    assert!(items[1].direction.is_none());
}

#[test]
fn create_use_drop_database_and_schema() {
    let Statement::CreateDatabase(c) = one("CREATE DATABASE IF NOT EXISTS app") else {
        panic!("expected CREATE DATABASE");
    };
    assert!(c.kind.is_left());
    assert!(c.if_not_exists.is_some());
    assert_eq!(c.name.value, "app");

    let Statement::CreateDatabase(c) = one("CREATE SCHEMA analytics") else {
        panic!("expected CREATE SCHEMA");
    };
    assert!(c.kind.is_right());

    let Statement::Use(u) = one("USE app") else {
        panic!("expected USE");
    };
    assert_eq!(u.name.to_dotted(), "app");
    let Statement::Use(u) = one("USE app.analytics") else {
        panic!("expected USE");
    };
    assert_eq!(u.name.to_dotted(), "app.analytics");

    let Statement::DropDatabase(d) = one("DROP SCHEMA IF EXISTS analytics CASCADE") else {
        panic!("expected DROP SCHEMA");
    };
    assert!(d.kind.is_right());
    assert!(d.if_exists.is_some());
    assert!(d.behavior.unwrap().is_left()); // CASCADE

    let Statement::DropDatabase(d) = one("DROP DATABASE app") else {
        panic!("expected DROP DATABASE");
    };
    assert!(d.kind.is_left());
    assert!(d.behavior.is_none());
}

#[test]
fn create_drop_index_truncate() {
    let Statement::CreateIndex(c) = one(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email ASC, created_at DESC)",
    ) else {
        panic!("expected CREATE INDEX");
    };
    assert!(c.unique.is_some());
    assert!(c.if_not_exists.is_some());
    assert_eq!(c.name.value, "idx_users_email");
    assert_eq!(c.table.to_dotted(), "users");
    assert_eq!(c.columns.len(), 2);

    let Statement::DropIndex(d) = one("DROP INDEX IF EXISTS idx_users_email") else {
        panic!("expected DROP INDEX");
    };
    assert!(d.if_exists.is_some());

    let Statement::Truncate(t) = one("TRUNCATE TABLE logs") else {
        panic!("expected TRUNCATE");
    };
    assert!(t.table.is_some());
    assert!(matches!(one("TRUNCATE logs"), Statement::Truncate(_)));
}

#[test]
fn explain_statement() {
    let Statement::Explain(_, inner) = one("EXPLAIN SELECT a FROM t WHERE b = 1") else {
        panic!("expected EXPLAIN");
    };
    assert!(matches!(*inner, Statement::Select(_)));
    // EXPLAIN nests
    let Statement::Explain(_, inner) = one("EXPLAIN EXPLAIN DELETE FROM t") else {
        panic!("expected EXPLAIN");
    };
    assert!(matches!(*inner, Statement::Explain(..)));
}

#[test]
fn prepared_statement_placeholders() {
    use sql_parser::Placeholder;
    // parse once, inspect bind slots, reuse the AST across executions
    let s = one("INSERT INTO t VALUES (?, ?)");
    let ps = s.placeholders();
    assert_eq!(ps.len(), 2);
    assert!(ps.iter().all(|p| matches!(p, Placeholder::Anonymous(_))));
    assert_eq!(s.parameter_count(), 2);

    // source order across clauses and styles
    let s = one("UPDATE t SET a = ?, b = :name WHERE id = $1");
    let ps = s.placeholders();
    assert_eq!(ps.len(), 3);
    assert!(matches!(ps[0], Placeholder::Anonymous(_)));
    assert!(matches!(ps[1], Placeholder::Named(_, n) if n == "name"));
    assert!(matches!(ps[2], Placeholder::Positional(_, 1)));

    // placeholders nested in subqueries, and LIMIT ? now parses
    let s = one("SELECT a FROM t WHERE b IN (SELECT c FROM u WHERE d = ?) LIMIT ?");
    assert_eq!(s.parameter_count(), 2);

    let s = one("DELETE FROM t WHERE id = ? AND ts < ?");
    assert_eq!(s.parameter_count(), 2);
}

#[test]
fn prepare_execute_deallocate() {
    let Statement::Prepare(p) =
        one("PREPARE ins (INT, VARCHAR(10)) AS INSERT INTO t VALUES ($1, $2)")
    else {
        panic!("expected PREPARE");
    };
    assert_eq!(p.name.value, "ins");
    let (_, tys, _) = p.datatypes.as_ref().unwrap();
    assert_eq!(tys.len(), 2);
    assert!(matches!(*p.statement, Statement::Insert(_)));
    assert_eq!(p.statement.parameter_count(), 2);

    // without a type list
    let Statement::Prepare(p) = one("PREPARE q1 AS SELECT a FROM t WHERE b = ?") else {
        panic!("expected PREPARE");
    };
    assert!(p.datatypes.is_none());

    let Statement::Execute(e) = one("EXECUTE ins (1, 'x')") else {
        panic!("expected EXECUTE");
    };
    assert_eq!(e.name.value, "ins");
    let (_, args, _) = e.params.as_ref().unwrap();
    assert_eq!(args.len(), 2);
    assert!(matches!(one("EXECUTE q1"), Statement::Execute(_)));

    let Statement::Deallocate(d) = one("DEALLOCATE ins") else {
        panic!("expected DEALLOCATE");
    };
    assert!(d.prepare.is_none());
    assert!(d.name.is_right());
    let Statement::Deallocate(d) = one("DEALLOCATE PREPARE ALL") else {
        panic!("expected DEALLOCATE");
    };
    assert!(d.prepare.is_some());
    assert!(d.name.is_left());
}

#[test]
fn use_database_and_schema() {
    let Statement::Use(u) = one("USE DATABASE app") else {
        panic!("expected USE");
    };
    assert!(u.kind.unwrap().is_left());
    assert_eq!(u.name.to_dotted(), "app");

    let Statement::Use(u) = one("USE SCHEMA analytics") else {
        panic!("expected USE");
    };
    assert!(u.kind.unwrap().is_right());
    assert_eq!(u.name.to_dotted(), "analytics");

    // bare form still parses, with no kind
    let Statement::Use(u) = one("USE app.analytics") else {
        panic!("expected USE");
    };
    assert!(u.kind.is_none());
    assert_eq!(u.name.to_dotted(), "app.analytics");
}

#[test]
fn copy_into() {
    let Statement::CopyInto(c) = one("COPY INTO users FROM @/data/imports/users.csv") else {
        panic!("expected COPY INTO");
    };
    assert_eq!(c.table.to_dotted(), "users");
    assert_eq!(c.path.path, "/data/imports/users.csv");

    // a relative, extension-bearing, hyphenated path — exactly the shape
    // that would fragment into ambiguous punctuation/word tokens if `@`
    // weren't lexed as a single stage-path token (see lexer::stage_path).
    let Statement::CopyInto(c) = one("COPY INTO t FROM @./my-data_2024/f.csv") else {
        panic!("expected COPY INTO");
    };
    assert_eq!(c.path.path, "./my-data_2024/f.csv");

    assert!(parse_one("COPY INTO t FROM users").is_err());
}

#[test]
fn alter_table_add_and_drop_foreign_key() {
    let Statement::AlterTable(a) = one(
        "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)",
    ) else {
        panic!("expected ALTER TABLE");
    };
    let AlterTableOp::AddConstraint(_, c) = &a.operation else {
        panic!("expected ADD CONSTRAINT, got {:?}", a.operation);
    };
    assert_eq!(c.name.as_ref().unwrap().1.value, "fk_user");
    assert!(matches!(c.kind, TableConstraintKind::ForeignKey(..)));

    // unnamed form
    let Statement::AlterTable(a) =
        one("ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users (id)")
    else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::AddConstraint(..)));

    let Statement::AlterTable(a) = one("ALTER TABLE orders DROP CONSTRAINT fk_user") else {
        panic!("expected ALTER TABLE");
    };
    let AlterTableOp::DropConstraint(_, _, name) = &a.operation else {
        panic!("expected DROP CONSTRAINT, got {:?}", a.operation);
    };
    assert_eq!(name.value, "fk_user");

    // ADD COLUMN must still win over ADD CONSTRAINT's own leading ADD
    let Statement::AlterTable(a) = one("ALTER TABLE t ADD COLUMN c INT") else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(a.operation, AlterTableOp::AddColumn(..)));
}
