//! A pre/post-order visitor over the handful of node kinds most callers
//! actually want to hook into — statements, queries, `SELECT` blocks, table
//! references, expressions, and literals — not a full field-by-field walk
//! of every token in the grammar (keywords, punctuation, ...).
//!
//! Modeled on sqlparser's own `Visit`/`Visitor` pair: implement [`Visitor`],
//! overriding only the `pre_visit_*`/`post_visit_*` hooks you care about
//! (everything else defaults to a no-op), then call
//! [`Visit::visit`] on a [`Statement`], [`Query`], or [`Expr`] to walk it.
//! Any hook returning `ControlFlow::Break` stops the walk immediately and
//! that value propagates out of the top-level `visit` call.
//!
//! ```
//! use std::ops::ControlFlow;
//! use sql_parser::{Statement, parse_one};
//! use sql_parser::visitor::{Visit, Visitor};
//!
//! struct CountExprs(usize);
//! impl Visitor for CountExprs {
//!     type Break = ();
//!     fn post_visit_expr(&mut self, _expr: &sql_parser::Expr) -> ControlFlow<()> {
//!         self.0 += 1;
//!         ControlFlow::Continue(())
//!     }
//! }
//!
//! let stmt = parse_one("SELECT a FROM t WHERE b = 1 AND c = 2").unwrap();
//! let mut counter = CountExprs(0);
//! stmt.visit(&mut counter);
//! assert!(counter.0 > 0);
//! ```

use std::ops::ControlFlow;

use crate::{
    ddl::{
        AlterTable, AlterTableOp, ColumnDef, ColumnOption, CopyInto, CreateIndex, CreateTable,
        DropTable, TableConstraint, TableConstraintKind, TableElement, Truncate,
    },
    dml::{Delete, Insert, InsertSource, Update},
    expr::{Expr, FunctionArg, OverClause},
    ident::ObjectName,
    literal::Literal,
    query::{
        CompoundSelect, Cte, FromClause, Join, JoinConstraint, Query, SelectCore, SelectItem,
        SetOperand, TableFactor, TableWithJoins, With,
    },
    statement::{Deallocate, Execute, Prepare, Statement},
};

/// Hook points for [`Visit::visit`]. Every method defaults to a no-op that
/// continues the walk — override only the ones you need. `pre_visit_*` runs
/// before a node's children are visited, `post_visit_*` after.
pub trait Visitor {
    type Break;

    fn pre_visit_statement(&mut self, _stmt: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_statement(&mut self, _stmt: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, _select: &SelectCore) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_select(&mut self, _select: &SelectCore) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, _tf: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_table_factor(&mut self, _tf: &TableFactor) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Fires for every `ObjectName` that names a table — a `FROM`/`JOIN`
    /// entry, an INSERT/UPDATE/DELETE target, a CREATE/ALTER/DROP
    /// TABLE/INDEX/COPY INTO name, or a FOREIGN KEY's REFERENCES target.
    /// Not called for column references (`Expr::Column`) or non-table
    /// names (database/schema/prepared-statement names).
    fn pre_visit_relation(&mut self, _name: &ObjectName) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_relation(&mut self, _name: &ObjectName) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    fn pre_visit_literal(&mut self, _lit: &Literal) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn post_visit_literal(&mut self, _lit: &Literal) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

/// Implemented by every node kind [`Visitor`] has dedicated hooks for
/// (`Statement`, `Query`, `SelectCore`, `TableFactor`, `Expr`) — the entry
/// points for a walk. Nodes without their own hooks (joins, constraints,
/// CTEs, ...) are still traversed *through*, just without a dedicated
/// callback of their own; add one to `Visitor` if you need it.
pub trait Visit {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break>;
}

macro_rules! walk {
    ($visitor:expr, $node:expr) => {
        $node.visit($visitor)?
    };
}

// ---- relation (table-name ObjectName) ----

fn visit_relation<V: Visitor>(name: &ObjectName, v: &mut V) -> ControlFlow<V::Break> {
    v.pre_visit_relation(name)?;
    v.post_visit_relation(name)
}

// ---- Statement ----

impl Visit for Statement {
    fn visit<V: Visitor>(&self, v: &mut V) -> ControlFlow<V::Break> {
        v.pre_visit_statement(self)?;
        match self {
            Statement::Select(query) => walk!(v, **query),
            Statement::Insert(insert) => visit_insert(insert, v)?,
            Statement::Update(update) => visit_update(update, v)?,
            Statement::Delete(delete) => visit_delete(delete, v)?,
            Statement::CreateTable(c) => visit_create_table(c, v)?,
            Statement::CreateIndex(c) => visit_create_index(c, v)?,
            Statement::DropTable(d) => visit_drop_table(d, v)?,
            Statement::AlterTable(a) => visit_alter_table(a, v)?,
            Statement::Truncate(t) => visit_truncate(t, v)?,
            Statement::CopyInto(c) => visit_copy_into(c, v)?,
            Statement::Prepare(p) => visit_prepare(p, v)?,
            Statement::Execute(e) => visit_execute(e, v)?,
            Statement::Explain(_, inner) => walk!(v, **inner),
            // No relation/expr/query children: naming a database/schema/
            // index/prepared-statement isn't a table reference (see
            // pre_visit_relation's own doc comment), and these carry
            // nothing else recursible.
            Statement::CreateDatabase(_)
            | Statement::DropIndex(_)
            | Statement::DropDatabase(_)
            | Statement::Use(_)
            | Statement::Deallocate(Deallocate { .. })
            | Statement::StartTransaction(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::ShowTables(_)
            | Statement::ShowSchemas(_) => {}
        }
        v.post_visit_statement(self)
    }
}

fn visit_insert<V: Visitor>(insert: &Insert, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&insert.table, v)?;
    match &insert.source {
        InsertSource::Values(_, rows) => {
            for row in rows.items() {
                for e in row.exprs() {
                    walk!(v, *e);
                }
            }
        }
        InsertSource::Select(query) => walk!(v, **query),
    }
    ControlFlow::Continue(())
}

fn visit_update<V: Visitor>(update: &Update, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&update.table, v)?;
    for a in update.assignments.items() {
        walk!(v, a.value);
    }
    if let Some(w) = &update.where_clause {
        walk!(v, w.expr);
    }
    ControlFlow::Continue(())
}

fn visit_delete<V: Visitor>(delete: &Delete, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&delete.table, v)?;
    if let Some(w) = &delete.where_clause {
        walk!(v, w.expr);
    }
    ControlFlow::Continue(())
}

fn visit_create_table<V: Visitor>(c: &CreateTable, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&c.name, v)?;
    for el in c.elements.items() {
        match el {
            TableElement::Column(col) => visit_column_def(col, v)?,
            TableElement::Constraint(con) => visit_table_constraint(con, v)?,
        }
    }
    ControlFlow::Continue(())
}

fn visit_column_def<V: Visitor>(col: &ColumnDef, v: &mut V) -> ControlFlow<V::Break> {
    for opt in &col.options {
        match opt {
            ColumnOption::Default(_, e) | ColumnOption::Check(_, _, e, _) => walk!(v, *e),
            // An inline REFERENCES names the target table just as much as
            // a table-level FOREIGN KEY does (see visit_table_constraint).
            ColumnOption::References(reference) => visit_relation(&reference.table, v)?,
            ColumnOption::NotNull(..)
            | ColumnOption::Null(_)
            | ColumnOption::PrimaryKey(..)
            | ColumnOption::Unique(_) => {}
        }
    }
    ControlFlow::Continue(())
}

fn visit_table_constraint<V: Visitor>(con: &TableConstraint, v: &mut V) -> ControlFlow<V::Break> {
    match &con.kind {
        TableConstraintKind::Check(_, _, e, _) => walk!(v, *e),
        // FOREIGN KEY ... REFERENCES other_table is a genuine reference
        // to `other_table`, not just to whatever table this constraint
        // lives on — surfaced the same way a FROM/JOIN entry is.
        TableConstraintKind::ForeignKey(_, _, _, _, _, reference) => {
            visit_relation(&reference.table, v)?
        }
        TableConstraintKind::PrimaryKey(..) | TableConstraintKind::Unique(..) => {}
    }
    ControlFlow::Continue(())
}

fn visit_create_index<V: Visitor>(c: &CreateIndex, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&c.table, v)?;
    for item in c.columns.items() {
        walk!(v, item.expr);
    }
    ControlFlow::Continue(())
}

fn visit_drop_table<V: Visitor>(d: &DropTable, v: &mut V) -> ControlFlow<V::Break> {
    for name in d.names.items() {
        visit_relation(name, v)?;
    }
    ControlFlow::Continue(())
}

fn visit_alter_table<V: Visitor>(a: &AlterTable, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&a.name, v)?;
    match &a.operation {
        AlterTableOp::AddColumn(_, _, col) => visit_column_def(col, v)?,
        AlterTableOp::AddConstraint(_, con) => visit_table_constraint(con, v)?,
        AlterTableOp::DropColumn(..)
        | AlterTableOp::RenameTo(..)
        | AlterTableOp::RenameColumn(..)
        | AlterTableOp::DropConstraint(..) => {}
    }
    ControlFlow::Continue(())
}

fn visit_truncate<V: Visitor>(t: &Truncate, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&t.name, v)
}

fn visit_copy_into<V: Visitor>(c: &CopyInto, v: &mut V) -> ControlFlow<V::Break> {
    visit_relation(&c.table, v)
}

fn visit_prepare<V: Visitor>(p: &Prepare, v: &mut V) -> ControlFlow<V::Break> {
    walk!(v, *p.statement);
    ControlFlow::Continue(())
}

fn visit_execute<V: Visitor>(e: &Execute, v: &mut V) -> ControlFlow<V::Break> {
    if let Some((_, args, _)) = &e.params {
        for a in args.items() {
            walk!(v, *a);
        }
    }
    ControlFlow::Continue(())
}

// ---- Query ----

impl Visit for Query {
    fn visit<V: Visitor>(&self, v: &mut V) -> ControlFlow<V::Break> {
        v.pre_visit_query(self)?;
        if let Some(with) = &self.with {
            visit_with(with, v)?;
        }
        visit_set_operand(&self.body, v)?;
        for c in &self.compounds {
            visit_compound(c, v)?;
        }
        if let Some(o) = &self.order_by {
            for item in o.items.items() {
                walk!(v, item.expr);
            }
        }
        if let Some(l) = &self.limit {
            walk!(v, l.count);
        }
        if let Some(o) = &self.offset {
            walk!(v, o.count);
        }
        v.post_visit_query(self)
    }
}

fn visit_with<V: Visitor>(with: &With, v: &mut V) -> ControlFlow<V::Break> {
    for cte in with.ctes.items() {
        visit_cte(cte, v)?;
    }
    ControlFlow::Continue(())
}

fn visit_cte<V: Visitor>(cte: &Cte, v: &mut V) -> ControlFlow<V::Break> {
    walk!(v, *cte.query);
    ControlFlow::Continue(())
}

fn visit_compound<V: Visitor>(c: &CompoundSelect, v: &mut V) -> ControlFlow<V::Break> {
    visit_set_operand(&c.operand, v)
}

fn visit_set_operand<V: Visitor>(op: &SetOperand, v: &mut V) -> ControlFlow<V::Break> {
    match op {
        SetOperand::Select(core) => walk!(v, **core),
        SetOperand::Paren(_, query, _) => walk!(v, **query),
    }
    ControlFlow::Continue(())
}

// ---- SelectCore ----

impl Visit for SelectCore {
    fn visit<V: Visitor>(&self, v: &mut V) -> ControlFlow<V::Break> {
        v.pre_visit_select(self)?;
        for item in self.projection.items() {
            if let SelectItem::Expr { expr, .. } = item {
                walk!(v, *expr);
            }
        }
        if let Some(f) = &self.from {
            visit_from_clause(f, v)?;
        }
        if let Some(w) = &self.where_clause {
            walk!(v, w.expr);
        }
        if let Some(g) = &self.group_by {
            for e in g.exprs.items() {
                walk!(v, *e);
            }
        }
        if let Some(h) = &self.having {
            walk!(v, h.expr);
        }
        v.post_visit_select(self)
    }
}

fn visit_from_clause<V: Visitor>(f: &FromClause, v: &mut V) -> ControlFlow<V::Break> {
    for t in f.tables.items() {
        visit_table_with_joins(t, v)?;
    }
    ControlFlow::Continue(())
}

fn visit_table_with_joins<V: Visitor>(t: &TableWithJoins, v: &mut V) -> ControlFlow<V::Break> {
    walk!(v, t.relation);
    for j in &t.joins {
        visit_join(j, v)?;
    }
    ControlFlow::Continue(())
}

fn visit_join<V: Visitor>(j: &Join, v: &mut V) -> ControlFlow<V::Break> {
    walk!(v, j.relation);
    if let Some(JoinConstraint::On(_, e)) = &j.constraint {
        walk!(v, *e);
    }
    ControlFlow::Continue(())
}

// ---- TableFactor ----

impl Visit for TableFactor {
    fn visit<V: Visitor>(&self, v: &mut V) -> ControlFlow<V::Break> {
        v.pre_visit_table_factor(self)?;
        match self {
            TableFactor::Table { name, .. } => visit_relation(name, v)?,
            TableFactor::Derived { query, .. } => walk!(v, **query),
        }
        v.post_visit_table_factor(self)
    }
}

// ---- Expr ----

impl Visit for Expr {
    fn visit<V: Visitor>(&self, v: &mut V) -> ControlFlow<V::Break> {
        v.pre_visit_expr(self)?;
        match self {
            Expr::Literal(l) => {
                v.pre_visit_literal(l)?;
                v.post_visit_literal(l)?;
            }
            Expr::Column(_) | Expr::Placeholder(_) => {}
            Expr::Unary { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Nested(expr) => walk!(v, **expr),
            Expr::Binary { left, right, .. } => {
                walk!(v, **left);
                walk!(v, **right);
            }
            Expr::InList { expr, list, .. } => {
                walk!(v, **expr);
                for e in list {
                    walk!(v, *e);
                }
            }
            Expr::InSubquery { expr, query, .. } => {
                walk!(v, **expr);
                walk!(v, **query);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                walk!(v, **expr);
                walk!(v, **low);
                walk!(v, **high);
            }
            Expr::Like { expr, pattern, .. } => {
                walk!(v, **expr);
                walk!(v, **pattern);
            }
            Expr::Function { args, over, .. } => {
                for a in args {
                    if let FunctionArg::Expr(e) = a {
                        walk!(v, *e);
                    }
                }
                if let Some(o) = over {
                    visit_over(o, v)?;
                }
            }
            Expr::QuantifiedComparison { left, query, .. } => {
                walk!(v, **left);
                walk!(v, **query);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(o) = operand {
                    walk!(v, **o);
                }
                for (w, t) in when_then {
                    walk!(v, *w);
                    walk!(v, *t);
                }
                if let Some(e) = else_expr {
                    walk!(v, **e);
                }
            }
            Expr::Subquery(query) | Expr::Exists { query } => walk!(v, **query),
        }
        v.post_visit_expr(self)
    }
}

fn visit_over<V: Visitor>(o: &OverClause, v: &mut V) -> ControlFlow<V::Break> {
    if let Some((_, _, exprs)) = &o.spec.partition_by {
        for e in exprs.items() {
            walk!(v, *e);
        }
    }
    if let Some(ob) = &o.spec.order_by {
        for item in ob.items.items() {
            walk!(v, item.expr);
        }
    }
    ControlFlow::Continue(())
}
