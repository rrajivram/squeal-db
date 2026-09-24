//! A short, hand-maintained SQL syntax cheat sheet, rendered by `!help` in
//! both squeal-cli and squeal-wasm. Lives here — not duplicated in each
//! front-end — because both already depend on squeal-sql, and a grammar
//! cheat sheet copied into two places is exactly the kind of thing that
//! silently drifts from the real grammar the first time one copy gets
//! updated and the other doesn't.
//!
//! Grounded in sql-parser's actual `Statement`/`Query`/DDL/DML types (see
//! sql-parser/src/{statement,ddl,dml,query}.rs) and squeal-sql's own
//! built-in functions (plan/funcs.rs) — not aspirational. `[brackets]`
//! mark optional syntax, `|` marks alternatives, `...` marks repetition,
//! matching the doc-comment convention sql-parser's own types already use.

/// One named group of related statements/clauses.
pub struct HelpSection {
    pub title: &'static str,
    /// (syntax, one-line description) pairs.
    pub entries: &'static [(&'static str, &'static str)],
}

pub const SQL_HELP: &[HelpSection] = &[
    HelpSection {
        title: "Queries",
        entries: &[
            ("SELECT [DISTINCT] item, ... FROM t ...", "query rows"),
            (
                "... [WHERE expr] [GROUP BY col, ...] [HAVING expr]",
                "filter and aggregate",
            ),
            (
                "... [ORDER BY col [ASC|DESC], ...] [LIMIT n] [OFFSET n]",
                "sort and paginate",
            ),
            (
                "SELECT ... UNION [ALL] | INTERSECT | EXCEPT SELECT ...",
                "combine query results",
            ),
            (
                "WITH name [(cols)] AS (query), ... SELECT ...",
                "common table expressions",
            ),
            (
                "[INNER|LEFT [OUTER]|RIGHT [OUTER]|FULL [OUTER]|CROSS] JOIN t ON expr | USING (cols)",
                "join tables",
            ),
        ],
    },
    HelpSection {
        title: "Functions",
        entries: &[
            (
                "COUNT(*|[DISTINCT] x), AVG(x), SUM(x), MIN(x), MAX(x)",
                "aggregate functions",
            ),
            ("UPPER(x), LOWER(x), CONCAT(a, b, ...)", "scalar functions"),
        ],
    },
    HelpSection {
        title: "Modify data",
        entries: &[
            ("INSERT INTO t [(cols)] VALUES (...), ...", "add rows"),
            ("INSERT INTO t SELECT ...", "add rows from a query"),
            ("UPDATE t SET col = expr, ... [WHERE expr]", "modify rows"),
            ("DELETE FROM t [WHERE expr]", "remove rows"),
            ("TRUNCATE [TABLE] t", "remove all rows from a table"),
            ("COPY INTO t FROM @path", "load rows from a local CSV file"),
        ],
    },
    HelpSection {
        title: "Tables & indexes",
        entries: &[
            (
                "CREATE TABLE [IF NOT EXISTS] t (col type [col-constraint ...], ..., [table-constraint, ...])",
                "define a table",
            ),
            (
                "  col-constraint: NOT NULL | NULL | PRIMARY KEY | UNIQUE | DEFAULT expr | REFERENCES t2[(c)] | CHECK(expr)",
                "attached to one column",
            ),
            (
                "  table-constraint: [CONSTRAINT name] PRIMARY KEY(cols) | UNIQUE(cols) | FOREIGN KEY(cols) REFERENCES t2(cols) | CHECK(expr)",
                "attached to the table",
            ),
            (
                "  types: INTEGER, BIGINT, SMALLINT, TINYINT, FLOAT/DOUBLE [PRECISION], DECIMAL(p,s), BOOLEAN, VARCHAR(n), CHAR(n), TEXT, BYTEA/BINARY, DATE, TIME, TIMESTAMP",
                "(plus common aliases: INT, INT64, REAL, STRING, ...)",
            ),
            ("DROP TABLE [IF EXISTS] t, ...", "remove a table"),
            (
                "ALTER TABLE t ADD|DROP COLUMN ... | RENAME TO name | RENAME COLUMN a TO b | ADD|DROP CONSTRAINT ...",
                "modify a table",
            ),
            (
                "CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t (col [ASC|DESC], ...)",
                "create an index",
            ),
            ("DROP INDEX [IF EXISTS] name", "remove an index"),
        ],
    },
    HelpSection {
        title: "Schemas",
        entries: &[
            ("CREATE DATABASE|SCHEMA [IF NOT EXISTS] name", "create a schema"),
            (
                "DROP DATABASE|SCHEMA [IF EXISTS] name [CASCADE|RESTRICT]",
                "remove a schema",
            ),
            ("USE [DATABASE|SCHEMA] name", "switch the current schema"),
        ],
    },
    HelpSection {
        title: "Transactions",
        entries: &[
            ("BEGIN [TRANSACTION] | START TRANSACTION", "start a transaction"),
            ("COMMIT", "commit the current transaction"),
            ("ROLLBACK", "roll back the current transaction"),
        ],
    },
    HelpSection {
        title: "Introspection",
        entries: &[
            ("SHOW TABLES", "list tables in the current schema"),
            ("SHOW SCHEMAS", "list schemas"),
            ("SHOW TABLE INDEX t", "list indexes on a table"),
            ("DESCRIBE TABLE t", "show a table's columns"),
            ("ANALYZE TABLE t | ANALYZE TABLES", "collect table statistics"),
            ("EXPLAIN <statement>", "show a statement's query plan"),
        ],
    },
    HelpSection {
        title: "Prepared statements",
        entries: &[
            (
                "PREPARE name [(types)] AS <statement>",
                "prepare a statement for reuse",
            ),
            ("EXECUTE name [(args)]", "run a prepared statement"),
            ("DEALLOCATE [PREPARE] name | ALL", "drop a prepared statement"),
        ],
    },
];

/// Renders [`SQL_HELP`] as plain text: one blank-line-separated section per
/// group, entries aligned within their own section (not globally — "CREATE
/// TABLE ..." is far longer than "COMMIT", and matching column widths
/// across sections would just leave every short section ragged for no
/// benefit).
pub fn sql_help_text() -> String {
    let mut out = String::from("Supported SQL:\n");
    for section in SQL_HELP {
        out += &format!("\n{}:\n", section.title);
        let width = section
            .entries
            .iter()
            .map(|(syntax, _)| syntax.len())
            .max()
            .unwrap_or(0);
        for (syntax, desc) in section.entries {
            out += &format!("  {syntax:<width$}  {desc}\n");
        }
    }
    out.pop(); // drop the trailing newline, matching squeal-cli/squeal-wasm's own help text
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_section_has_at_least_one_entry() {
        for section in SQL_HELP {
            assert!(
                !section.entries.is_empty(),
                "section {:?} has no entries",
                section.title
            );
        }
    }

    #[test]
    fn test_rendered_text_mentions_every_sections_title_and_syntax() {
        let text = sql_help_text();
        for section in SQL_HELP {
            assert!(text.contains(section.title), "missing section {:?}", section.title);
            for (syntax, _) in section.entries {
                assert!(text.contains(syntax), "missing entry {syntax:?}");
            }
        }
    }
}
