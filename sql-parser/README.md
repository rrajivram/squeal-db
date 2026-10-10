# sql-parser

squeal-db's SQL grammar: a lexer, an AST, and a parser, written in this repo
instead of using a general-purpose SQL parser crate, so the dialect is ours
to shape. It knows nothing about storage or execution. `squeal-sql`
consumes its AST.

## Design

The approach follows LakeSail's
[*a SQL parser in one week*](https://lakesail.com/blog/sql-parser-in-one-week/):
**the AST is the grammar.** Each AST type derives its own parser from its
shape, using [chumsky](https://github.com/zesterer/chumsky) combinators:

```rust
#[derive(Debug, Clone, PartialEq, SQLParser)]
pub struct With {
    pub with: kw::With,
    pub recursive: Option<kw::Recursive>,
    pub ctes: Seq<Cte, Comma>,
}
```

- **A struct parses as its fields in order.** An enum parses as its
  variants in order: declaration order is priority, so more specific
  variants come first.
- **Blanket impls handle composite field types.** `Option<T>` is
  "optional", `Vec<T>` is "zero or more", `Either<L, R>` is "L, else R",
  tuples are sequences, and `Seq<T, Sep>` is a separated list. So the
  derive (from the [`macros`](../macros/README.md) crate) only composes
  `Field::parser()` calls. No per-shape logic exists anywhere else.
- **Keywords and punctuation are types** (`kw::With`, `Comma`), and every
  node keeps its source span. Errors point at the exact bytes.
- **Recursion.** `Query` and `Expr` are the two recursion roots:
  subqueries, derived tables and CTE bodies refer back to `Query`, and
  expressions to `Expr`. Each is built once as a chumsky `Recursive`
  handle, carried in a shared `SqlCtx`, instead of recursing at
  construction time.
- **Hand-written exceptions.** A few nodes are written by hand where the
  derive doesn't fit, such as operator precedence in expressions and stage
  paths (`@file.csv`).

## Pipeline

1. **Lex.** `lexer::tokenize` turns text into span-carrying tokens, with
   keywords classified once, case-insensitively. Whitespace and comments
   are dropped. A hand-written lexer does the work, several times faster
   than the combinator lexer it mirrors. A test checks that both give the
   same tokens. When text doesn't lex, the combinator lexer runs to report
   why.
2. **Parse.** `parse_sql` / `parse_one` return `Vec<Statement>` / one
   `Statement`, or every error with its span.
3. **Cache.** `parse_sql_cached` keeps parses process-wide at two levels:
   - **By exact text.**
   - **By shape:** statements that differ only in their literals share one
     parse. Each new statement gets its literals bound into the shape's
     template, and spans stay exact.
   - **Safety:** a shape is checked against a real parse the first time
     it's seen.
   - **Bounds:** the cache is LRU, capped in size, and skips very long
     texts such as bulk inserts.

Also:
- **`params`.** `Statement::placeholders()` lists the `?` / `:name`
  placeholders, for prepared statements.
- **`visitor`.** A `Visit`/`Visitor` pair with pre- and post-order hooks
  for statements, queries, `SELECT` blocks, table references, expressions
  and literals, in the style of sqlparser-rs.

```rust
let stmts = sql_parser::parse_sql("select a, count(*) from t group by a")?;
```

## Testing

```bash
cargo test -p sql-parser
```

`tests/parse.rs` covers the grammar statement by statement, and
`tests/visitor.rs` covers the visitor. The parser is also exercised by
every `squeal-sql` test.
