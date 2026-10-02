//! Binding literals into placeholders: the mutable twin of params.rs's
//! walk, visiting every placeholder in the same (source) order. The parse
//! cache uses it to rebuild a statement from its shape (see cache.rs).

use crate::{
    ddl::{AlterTableOp, ColumnDef, ColumnOption, TableConstraintKind, TableElement},
    dml::InsertSource,
    expr::{Expr, FunctionArg, OverClause},
    literal::Literal,
    query::{
        JoinConstraint, Query, SelectCore, SelectItem, SetOperand, TableFactor, TableWithJoins,
    },
    statement::Statement,
};

/// Replaces each placeholder in `statements`, in source order, with the
/// next of `literals`; false unless every placeholder took one and none
/// was left over.
pub(crate) fn bind_literals(statements: &mut [Statement], literals: Vec<Literal>) -> bool {
    let mut b = Binder {
        literals: literals.into_iter(),
        short: false,
    };
    for s in statements {
        stmt(s, &mut b);
    }
    !b.short && b.literals.next().is_none()
}

struct Binder {
    literals: std::vec::IntoIter<Literal>,
    short: bool,
}

impl Binder {
    fn bind(&mut self, e: &mut Expr) {
        match self.literals.next() {
            Some(l) => *e = Expr::Literal(l),
            None => self.short = true,
        }
    }
}

fn stmt(s: &mut Statement, out: &mut Binder) {
    match s {
        Statement::Select(q) => query(q, out),
        Statement::Insert(i) => match &mut i.source {
            InsertSource::Values(_, rows) => {
                for row in rows.items_mut() {
                    for e in row.1.items_mut() {
                        expr(e, out);
                    }
                }
            }
            InsertSource::Select(q) => query(q, out),
        },
        Statement::Update(u) => {
            for a in u.assignments.items_mut() {
                expr(&mut a.value, out);
            }
            if let Some(w) = &mut u.where_clause {
                expr(&mut w.expr, out);
            }
        }
        Statement::Delete(d) => {
            if let Some(w) = &mut d.where_clause {
                expr(&mut w.expr, out);
            }
        }
        Statement::CreateTable(c) => {
            for el in c.elements.items_mut() {
                match el {
                    TableElement::Column(col) => column_def(col, out),
                    TableElement::Constraint(con) => {
                        if let TableConstraintKind::Check(_, _, e, _) = &mut con.kind {
                            expr(e, out);
                        }
                    }
                }
            }
            for def in c
                .partition_by
                .iter_mut()
                .flat_map(|p| p.partitions.items_mut())
            {
                for e in def.values.exprs_mut() {
                    expr(e, out);
                }
            }
        }
        // A literal @path only — no expression anywhere in this
        // statement's grammar for a placeholder to appear in.
        Statement::CreateTableAsCopy(_) => {}
        Statement::CreateIndex(c) => {
            for item in c.columns.items_mut() {
                expr(&mut item.expr, out);
            }
        }
        Statement::AlterTable(a) => match &mut a.operation {
            AlterTableOp::AddColumn(_, _, col) => column_def(col, out),
            AlterTableOp::AddPartition(_, def) => {
                for e in def.values.exprs_mut() {
                    expr(e, out);
                }
            }
            _ => {}
        },
        Statement::Explain(_, inner) => stmt(inner, out),
        Statement::Prepare(p) => stmt(&mut p.statement, out),
        Statement::Execute(e) => {
            if let Some((_, args, _)) = &mut e.params {
                for a in args.items_mut() {
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
        | Statement::DescribeTable(_)
        | Statement::AnalyzeTable(_)
        | Statement::AnalyzeTables(_) => {}
    }
}

fn query(q: &mut Query, out: &mut Binder) {
    if let Some(with) = &mut q.with {
        for cte in with.ctes.items_mut() {
            query(&mut cte.query, out);
        }
    }
    operand(&mut q.body, out);
    for c in &mut q.compounds {
        operand(&mut c.operand, out);
    }
    if let Some(o) = &mut q.order_by {
        for item in o.items.items_mut() {
            expr(&mut item.expr, out);
        }
    }
    if let Some(l) = &mut q.limit {
        expr(&mut l.count, out);
    }
    if let Some(o) = &mut q.offset {
        expr(&mut o.count, out);
    }
}

fn operand(op: &mut SetOperand, out: &mut Binder) {
    match op {
        SetOperand::Select(core) => select_core(core, out),
        SetOperand::Paren(_, q, _) => query(q, out),
    }
}

fn select_core(c: &mut SelectCore, out: &mut Binder) {
    for item in c.projection.items_mut() {
        if let SelectItem::Expr { expr: e, .. } = item {
            expr(e, out);
        }
    }
    if let Some(f) = &mut c.from {
        for t in f.tables.items_mut() {
            table_with_joins(t, out);
        }
    }
    if let Some(w) = &mut c.where_clause {
        expr(&mut w.expr, out);
    }
    if let Some(g) = &mut c.group_by {
        for e in g.exprs.items_mut() {
            expr(e, out);
        }
    }
    if let Some(h) = &mut c.having {
        expr(&mut h.expr, out);
    }
}

fn table_with_joins(t: &mut TableWithJoins, out: &mut Binder) {
    factor(&mut t.relation, out);
    for j in &mut t.joins {
        factor(&mut j.relation, out);
        if let Some(JoinConstraint::On(_, e)) = &mut j.constraint {
            expr(e, out);
        }
    }
}

fn factor(f: &mut TableFactor, out: &mut Binder) {
    match f {
        TableFactor::Derived { query: q, .. } => query(q, out),
        TableFactor::Table { .. } => {}
    }
}

fn column_def(col: &mut ColumnDef, out: &mut Binder) {
    for opt in &mut col.options {
        match opt {
            ColumnOption::Default(_, e) | ColumnOption::Check(_, _, e, _) => expr(e, out),
            _ => {}
        }
    }
}

fn over(o: &mut OverClause, out: &mut Binder) {
    if let Some((_, _, exprs)) = &mut o.spec.partition_by {
        for e in exprs.items_mut() {
            expr(e, out);
        }
    }
    if let Some(ob) = &mut o.spec.order_by {
        for item in ob.items.items_mut() {
            expr(&mut item.expr, out);
        }
    }
}

fn expr(e: &mut Expr, out: &mut Binder) {
    match e {
        Expr::Placeholder(_) => out.bind(e),
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
