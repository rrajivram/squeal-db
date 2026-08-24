use sql_parser::{
    Expr, Statement, parse_one, parse_sql,
    ddl::{AlterTableOp, ColumnOption, TableConstraintKind, TableElement},
    dml::InsertSource,
    expr::{BinaryOp, FunctionArg, Placeholder, UnaryOp},
    literal::{Literal, NumberValue},
    query::{JoinOperator, SelectItem},
};

fn one(src: &str) -> Statement {
    parse_one(src).unwrap_or_else(|e| panic!("failed to parse {src:?}: {e:?}"))
}

fn select(src: &str) -> sql_parser::query::SelectStatement {
    match one(src) {
        Statement::Select(s) => *s,
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn where_expr(src: &str) -> Expr {
    select(src).where_clause.expect("expected WHERE").expr
}

#[test]
fn select_basic() {
    let s = select("SELECT a, b FROM t");
    assert_eq!(s.projection.len(), 2);
    let from = s.from.unwrap();
    assert_eq!(from.tables.head.relation.name.to_dotted(), "t");
    assert!(s.where_clause.is_none());
}

#[test]
fn select_star_and_qualified_star() {
    let s = select("SELECT *, t.*, a AS x FROM t");
    let items: Vec<_> = s.projection.items().collect();
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
    assert!(s.distinct.is_some());
    assert!(s.group_by.is_some());
    assert!(s.having.is_some());
    let order = s.order_by.unwrap();
    assert!(order.items.head.direction.unwrap().is_right()); // DESC
    assert_eq!(s.limit.unwrap().count.as_i64(), Some(10));
    assert_eq!(s.offset.unwrap().count.as_i64(), Some(5));
}

#[test]
fn select_joins() {
    let s = select(
        "SELECT * FROM a INNER JOIN b ON a.id = b.a_id \
         LEFT OUTER JOIN c ON b.id = c.b_id \
         CROSS JOIN d",
    );
    let t = &s.from.unwrap().tables.head;
    assert_eq!(t.joins.len(), 3);
    assert!(matches!(t.joins[0].operator, JoinOperator::Inner(..)));
    assert!(matches!(t.joins[1].operator, JoinOperator::LeftOuter(..)));
    assert!(matches!(t.joins[2].operator, JoinOperator::Cross(..)));
    assert!(t.joins[2].constraint.is_none());
}

#[test]
fn join_using() {
    let s = select("SELECT * FROM a JOIN b USING (id, org_id)");
    let t = &s.from.unwrap().tables.head;
    match t.joins[0].constraint.as_ref().unwrap() {
        sql_parser::query::JoinConstraint::Using(_, _, cols, _) => assert_eq!(cols.len(), 2),
        other => panic!("expected USING, got {other:?}"),
    }
}

#[test]
fn expr_precedence() {
    // 1 + 2 * 3 => 1 + (2 * 3)
    let e = where_expr("SELECT a FROM t WHERE x = 1 + 2 * 3");
    let Expr::Binary { op: BinaryOp::Eq, right, .. } = e else {
        panic!("expected =, got another shape");
    };
    let Expr::Binary { op: BinaryOp::Plus, right: mul, .. } = *right else {
        panic!("expected +");
    };
    assert!(matches!(*mul, Expr::Binary { op: BinaryOp::Multiply, .. }));
}

#[test]
fn expr_and_or_not() {
    // NOT a = 1 AND b = 2 OR c = 3  =>  ((NOT (a=1)) AND (b=2)) OR (c=3)
    let e = where_expr("SELECT x FROM t WHERE NOT a = 1 AND b = 2 OR c = 3");
    let Expr::Binary { op: BinaryOp::Or, left, .. } = e else {
        panic!("expected OR at top");
    };
    let Expr::Binary { op: BinaryOp::And, left: not_side, .. } = *left else {
        panic!("expected AND under OR");
    };
    assert!(matches!(*not_side, Expr::Unary { op: UnaryOp::Not, .. }));
}

#[test]
fn expr_between_and() {
    // BETWEEN binds its own AND: (a BETWEEN 1 AND 10) AND b
    let e = where_expr("SELECT x FROM t WHERE a BETWEEN 1 AND 10 AND b = 2");
    let Expr::Binary { op: BinaryOp::And, left, .. } = e else {
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
        Expr::Like { negated: false, case_insensitive: false, .. }
    ));
    assert!(matches!(
        where_expr("SELECT x FROM t WHERE name NOT ILIKE '%b'"),
        Expr::Like { negated: true, case_insensitive: true, .. }
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
            Expr::Binary { op: BinaryOp::NotEq, .. }
        ));
    }
}

#[test]
fn expr_functions_case_cast() {
    match where_expr("SELECT x FROM t WHERE f(DISTINCT a, *, 1 + 2) = 1") {
        Expr::Binary { left, .. } => match *left {
            Expr::Function { name, distinct, args } => {
                assert_eq!(name.value, "f");
                assert!(distinct);
                assert_eq!(args.len(), 3);
                assert!(matches!(args[1], FunctionArg::Wildcard(_)));
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
            Expr::Case { operand, when_then, else_expr } => {
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
    let Statement::CreateTable(c) = one(
        "CREATE TABLE IF NOT EXISTS orders (\
            id INT PRIMARY KEY, \
            user_id BIGINT NOT NULL REFERENCES users (id), \
            amount DECIMAL(10, 2) DEFAULT 0, \
            note VARCHAR(255) NULL, \
            CONSTRAINT uq UNIQUE (user_id, note), \
            FOREIGN KEY (user_id) REFERENCES users (id)\
         )",
    ) else {
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
    assert!(matches!(constraints[0].kind, TableConstraintKind::Unique(..)));
    assert!(matches!(constraints[1].kind, TableConstraintKind::ForeignKey(..)));
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
    let stmts = parse_sql(
        "BEGIN; INSERT INTO t VALUES (1); UPDATE t SET a = 2 WHERE id = 1; COMMIT;",
    )
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
    let items: Vec<_> = s.projection.items().collect();
    match items[1] {
        SelectItem::Expr { expr: Expr::Column(c), .. } => {
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
    assert!(matches!(exprs[3], Expr::Unary { op: UnaryOp::Minus, .. }));
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
