//! Prepared-statement support: enumerate the bind parameters of a parsed
//! statement.
//!
//! Parse once, call [`Statement::placeholders`] to learn the parameter slots
//! (in source order), then bind a fresh set of values each time the
//! statement is executed — the AST itself is immutable and reusable.

use crate::{
    ddl::{AlterTableOp, ColumnDef, ColumnOption, TableConstraintKind, TableElement},
    dml::InsertSource,
    expr::{Expr, FunctionArg, OverClause, Placeholder},
    query::{
        JoinConstraint, Query, SelectCore, SelectItem, SetOperand, TableFactor, TableWithJoins,
    },
    statement::Statement,
};

impl Statement {
    /// All placeholders (`?`, `$n`, `:name`) in this statement, in source
    /// order. Anonymous `?` placeholders bind by their position in this list.
    pub fn placeholders(&self) -> Vec<&Placeholder> {
        let mut out = Vec::new();
        stmt(self, &mut out);
        out
    }

    /// The number of bind parameters this statement expects.
    pub fn parameter_count(&self) -> usize {
        self.placeholders().len()
    }
}

fn stmt<'a>(s: &'a Statement, out: &mut Vec<&'a Placeholder>) {
    match s {
        Statement::Select(q) => query(q, out),
        Statement::Insert(i) => match &i.source {
            InsertSource::Values(_, rows) => {
                for row in rows.items() {
                    for e in row.exprs() {
                        expr(e, out);
                    }
                }
            }
            InsertSource::Select(q) => query(q, out),
        },
        Statement::Update(u) => {
            for a in u.assignments.items() {
                expr(&a.value, out);
            }
            if let Some(w) = &u.where_clause {
                expr(&w.expr, out);
            }
        }
        Statement::Delete(d) => {
            if let Some(w) = &d.where_clause {
                expr(&w.expr, out);
            }
        }
        Statement::CreateTable(c) => {
            for el in c.elements.items() {
                match el {
                    TableElement::Column(col) => column_def(col, out),
                    TableElement::Constraint(con) => {
                        if let TableConstraintKind::Check(_, _, e, _) = &con.kind {
                            expr(e, out);
                        }
                    }
                }
            }
        }
        Statement::CreateIndex(c) => {
            for item in c.columns.items() {
                expr(&item.expr, out);
            }
        }
        Statement::AlterTable(a) => {
            if let AlterTableOp::AddColumn(_, _, col) = &a.operation {
                column_def(col, out);
            }
        }
        Statement::Explain(_, inner) => stmt(inner, out),
        Statement::Prepare(p) => stmt(&p.statement, out),
        Statement::Execute(e) => {
            if let Some((_, args, _)) = &e.params {
                for a in args.items() {
                    expr(a, out);
                }
            }
        }
        Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::DropTable(_)
        | Statement::DropIndex(_)
        | Statement::Truncate(_)
        | Statement::CopyInto(_)
        | Statement::Use(_)
        | Statement::Deallocate(_)
        | Statement::StartTransaction(_)
        | Statement::Commit(_)
        | Statement::Rollback(_)
        | Statement::ShowTables(_)
        | Statement::ShowSchemas(_)
        | Statement::ShowTableIndex(_)
        | Statement::DescribeTable(_) => {}
    }
}

fn query<'a>(q: &'a Query, out: &mut Vec<&'a Placeholder>) {
    if let Some(with) = &q.with {
        for cte in with.ctes.items() {
            query(&cte.query, out);
        }
    }
    operand(&q.body, out);
    for c in &q.compounds {
        operand(&c.operand, out);
    }
    if let Some(o) = &q.order_by {
        for item in o.items.items() {
            expr(&item.expr, out);
        }
    }
    if let Some(l) = &q.limit {
        expr(&l.count, out);
    }
    if let Some(o) = &q.offset {
        expr(&o.count, out);
    }
}

fn operand<'a>(op: &'a SetOperand, out: &mut Vec<&'a Placeholder>) {
    match op {
        SetOperand::Select(core) => select_core(core, out),
        SetOperand::Paren(_, q, _) => query(q, out),
    }
}

fn select_core<'a>(c: &'a SelectCore, out: &mut Vec<&'a Placeholder>) {
    for item in c.projection.items() {
        if let SelectItem::Expr { expr: e, .. } = item {
            expr(e, out);
        }
    }
    if let Some(f) = &c.from {
        for t in f.tables.items() {
            table_with_joins(t, out);
        }
    }
    if let Some(w) = &c.where_clause {
        expr(&w.expr, out);
    }
    if let Some(g) = &c.group_by {
        for e in g.exprs.items() {
            expr(e, out);
        }
    }
    if let Some(h) = &c.having {
        expr(&h.expr, out);
    }
}

fn table_with_joins<'a>(t: &'a TableWithJoins, out: &mut Vec<&'a Placeholder>) {
    factor(&t.relation, out);
    for j in &t.joins {
        factor(&j.relation, out);
        if let Some(JoinConstraint::On(_, e)) = &j.constraint {
            expr(e, out);
        }
    }
}

fn factor<'a>(f: &'a TableFactor, out: &mut Vec<&'a Placeholder>) {
    match f {
        TableFactor::Derived { query: q, .. } => query(q, out),
        TableFactor::Table { .. } => {}
    }
}

fn column_def<'a>(col: &'a ColumnDef, out: &mut Vec<&'a Placeholder>) {
    for opt in &col.options {
        match opt {
            ColumnOption::Default(_, e) | ColumnOption::Check(_, _, e, _) => expr(e, out),
            _ => {}
        }
    }
}

fn over<'a>(o: &'a OverClause, out: &mut Vec<&'a Placeholder>) {
    if let Some((_, _, exprs)) = &o.spec.partition_by {
        for e in exprs.items() {
            expr(e, out);
        }
    }
    if let Some(ob) = &o.spec.order_by {
        for item in ob.items.items() {
            expr(&item.expr, out);
        }
    }
}

fn expr<'a>(e: &'a Expr, out: &mut Vec<&'a Placeholder>) {
    match e {
        Expr::Placeholder(p) => out.push(p),
        Expr::Literal(_) | Expr::Column(_) => {}
        Expr::Unary { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Nested(inner) => expr(inner, out),
        Expr::Binary { left, right, .. } => {
            expr(left, out);
            expr(right, out);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            expr(inner, out);
            for e in list {
                expr(e, out);
            }
        }
        Expr::InSubquery {
            expr: inner,
            query: q,
            ..
        } => {
            expr(inner, out);
            query(q, out);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            expr(inner, out);
            expr(low, out);
            expr(high, out);
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            expr(inner, out);
            expr(pattern, out);
        }
        Expr::Function {
            args,
            over: over_clause,
            ..
        } => {
            for a in args {
                if let FunctionArg::Expr(e) = a {
                    expr(e, out);
                }
            }
            if let Some(o) = over_clause {
                over(o, out);
            }
        }
        Expr::QuantifiedComparison { left, query: q, .. } => {
            expr(left, out);
            query(q, out);
        }
        Expr::Case {
            operand: op,
            when_then,
            else_expr,
        } => {
            if let Some(o) = op {
                expr(o, out);
            }
            for (w, t) in when_then {
                expr(w, out);
                expr(t, out);
            }
            if let Some(el) = else_expr {
                expr(el, out);
            }
        }
        Expr::Subquery(q) | Expr::Exists { query: q } => query(q, out),
    }
}
