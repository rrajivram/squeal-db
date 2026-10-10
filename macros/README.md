# macros

`#[derive(SQLParser)]`: the proc-macro behind
[`sql-parser`](../sql-parser/README.md). It builds a chumsky parser for an
AST type from the type's shape.

- **A struct** parses as the sequence of its fields:
  `A::parser().then(B::parser()).then(…)`, mapped back into the struct.
- **An enum** parses as an ordered choice of its variants, in declaration
  order.
- **Per-field behaviour** comes entirely from each field type's own
  `SQLParser` impl (`Option`, `Vec`, `Either`, tuples, keywords, and other
  AST nodes). The macro only composes them.
- **`#[sql_parser(body_only)]`** emits the parser as an inherent
  `body_parser` instead of the trait impl. It is used for the recursion
  roots (`Query`), whose trait impl returns a shared `Recursive` handle.

Derived parsers take a `SqlCtx` argument, which carries those recursion
handles. Generated code refers to `::sql_parser::…`, which works inside
`sql-parser` itself (via `extern crate self as sql_parser`) and from
dependent crates.

Built on `syn`, `quote` and `proc-macro2`.
