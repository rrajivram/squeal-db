//! CSV schema inference — "read through the first few lines of a CSV and
//! suggest datatypes." Backs `CREATE TABLE <name> AS COPY FROM @<path>`
//! (see stmt.rs's own dispatch, which also has to translate this
//! module's own [`InferredColumn`]s into a synthetic `CREATE TABLE` AST)
//! and, indirectly, squeal-wasm's/ws-napi's own `create_table_from_csv`
//! (see `Connection::create_table_from_csv`'s own doc comment for why
//! that exists as a non-SQL entry point at all — `@path` needs a real
//! filesystem, which the browser doesn't have).
//!
//! Deliberately a simple heuristic, not a statistical one: for each
//! column, classify every sampled non-empty cell as the narrowest of
//! Integer/Double/Datetime/Boolean/Str that parses (in that preference
//! order), then fold those classifications together across the sample —
//! two agreeing classifications stay that type; Integer and Double widen
//! to Double (a column with both `1` and `1.5` in it is a Double
//! column); any other disagreement (an Integer column that also has
//! `"hello"` somewhere in it) falls back to Str, the always-valid
//! catch-all every other type can be represented as. A column is
//! nullable if any sampled cell — of any inferred type — is empty.

use crate::{constant::DEFAULT_VAR_SIZE, datatype::DataType, error::SchemaError};

pub(crate) struct InferredColumn {
    pub(crate) name: String,
    pub(crate) datatype: DataType,
    pub(crate) nullable: bool,
}

// A dummy (0..0) span for every synthesized token below — same
// convention squeal-sql's own prepared-statement placeholder
// substitution already uses (stmt.rs's value_item_to_expr): nothing
// downstream reads a synthetic node's span, only real parsed ones from
// actual source text.
fn dummy_span() -> sql_parser::span::TokenSpan {
    sql_parser::span::TokenSpan { start: 0, end: 0 }
}

// This module's own DataType (the small, normalized set Field::datatype
// itself uses) back into sql_parser's own many-alias grammar type, so
// synthetic_create_table below can build a real column definition out of
// an InferredColumn. Only ever needs to produce the handful of variants
// infer_schema itself can infer (see its own Guess enum) — not a general
// converter.
fn to_parser_datatype(dt: DataType) -> sql_parser::datatype::DataType {
    use sql_parser::{datatype::DataType as PDT, keyword as kw, literal::NumberLiteral};
    match dt {
        DataType::Integer => PDT::Integer(kw::Integer::new(dummy_span())),
        DataType::Double => PDT::Double(kw::Double::new(dummy_span())),
        DataType::Datetime => PDT::Datetime(kw::Datetime::new(dummy_span())),
        DataType::Boolean => {
            PDT::Boolean(either::Either::Left(kw::Boolean::new(dummy_span())))
        }
        DataType::Str(cap) => PDT::Varchar(
            kw::Varchar::new(dummy_span()),
            Some((
                sql_parser::token::LeftParenthesis::new(dummy_span()),
                NumberLiteral {
                    span: dummy_span(),
                    raw: cap.to_string(),
                    value: sql_parser::literal::NumberValue::Integer(cap as i64),
                },
                sql_parser::token::RightParenthesis::new(dummy_span()),
            )),
        ),
        // infer_schema never produces these for a real column — Blob has
        // no CSV representation (see csv_field_to_value_item's own gap),
        // and Null/Unsupported aren't inference outcomes at all. Falls
        // back to a plain, uncapped-looking Str rather than panicking,
        // in case that ever changes.
        DataType::Blob(_) | DataType::Null | DataType::Unsupported => {
            PDT::Varchar(
                kw::Varchar::new(dummy_span()),
                Some((
                    sql_parser::token::LeftParenthesis::new(dummy_span()),
                    NumberLiteral {
                        span: dummy_span(),
                        raw: DEFAULT_VAR_SIZE.to_string(),
                        value: sql_parser::literal::NumberValue::Integer(DEFAULT_VAR_SIZE as i64),
                    },
                    sql_parser::token::RightParenthesis::new(dummy_span()),
                )),
            )
        }
    }
}

// Builds `CREATE TABLE [IF NOT EXISTS] <name> (<columns>)` from what
// infer_schema found — every inferred column becomes a plain column
// definition (NOT NULL unless infer_schema saw an empty sample cell for
// it), no PRIMARY KEY/indices/constraints of any kind (inference has no
// basis to guess at those). Fed to SqlTable::from_sql exactly like a
// statement a caller typed out by hand would be, reusing all of its
// existing validation/qualification/index-bootstrapping rather than a
// second, parallel "build a SqlTable directly" path.
pub(crate) fn synthetic_create_table(
    table_name: &str,
    columns: &[InferredColumn],
    if_not_exists: bool,
) -> sql_parser::ddl::CreateTable {
    use sql_parser::{
        ddl::{ColumnDef, ColumnOption, CreateTable, TableElement},
        ident::{Ident, ObjectName},
        keyword as kw,
        token::{Comma, LeftParenthesis, RightParenthesis},
        utils::Seq,
    };

    let ident = |name: &str| Ident {
        span: dummy_span(),
        value: name.to_string(),
        quoted: false,
    };

    let mut items = columns.iter().map(|c| {
        let options = if c.nullable {
            vec![]
        } else {
            vec![ColumnOption::NotNull(
                kw::Not::new(dummy_span()),
                kw::Null::new(dummy_span()),
            )]
        };
        TableElement::Column(ColumnDef {
            name: ident(&c.name),
            data_type: to_parser_datatype(c.datatype),
            options,
        })
    });
    // columns is never empty — infer_schema already rejects a headerless
    // CSV before this is ever called.
    let head = Box::new(items.next().expect("infer_schema always finds at least one column"));
    let tail = items
        .map(|item| (Comma::new(dummy_span()), item))
        .collect();

    CreateTable {
        create: kw::Create::new(dummy_span()),
        table: kw::Table::new(dummy_span()),
        if_not_exists: if_not_exists.then(|| {
            (
                kw::If::new(dummy_span()),
                kw::Not::new(dummy_span()),
                kw::Exists::new(dummy_span()),
            )
        }),
        name: ObjectName {
            parts: Seq {
                head: Box::new(ident(table_name)),
                tail: vec![],
            },
        },
        lparen: LeftParenthesis::new(dummy_span()),
        elements: Seq { head, tail },
        rparen: RightParenthesis::new(dummy_span()),
    }
}

// How many data rows (beyond the header) actually get sampled — matches
// this feature's own "first few lines" framing: enough to catch the
// common cases (a numeric column, a text column, a sparsely-populated
// one) without reading a potentially huge file end to end just to guess
// its shape, when the caller's real interest is COPYing it in fully
// afterward anyway (see copy_csv_str_into, called right after this).
const SAMPLE_ROWS: usize = 100;

// Cell classifications — see this module's own doc comment for the
// widening rule that folds two of these together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Guess {
    Integer,
    Double,
    Datetime,
    Boolean,
    Str,
}

fn classify_cell(cell: &str) -> Guess {
    if cell.parse::<i64>().is_ok() {
        return Guess::Integer;
    }
    if cell.parse::<f64>().is_ok() {
        return Guess::Double;
    }
    if cell.eq_ignore_ascii_case("true") || cell.eq_ignore_ascii_case("false") {
        return Guess::Boolean;
    }
    if crate::datetime::parse_datetime(cell).is_some() {
        return Guess::Datetime;
    }
    Guess::Str
}

fn widen(a: Guess, b: Guess) -> Guess {
    match (a, b) {
        (x, y) if x == y => x,
        (Guess::Integer, Guess::Double) | (Guess::Double, Guess::Integer) => Guess::Double,
        _ => Guess::Str,
    }
}

/// `content` is a whole CSV document (first row a header) — only the
/// first [`SAMPLE_ROWS`] data rows actually get examined, so a caller
/// with a large file doesn't need to pre-truncate it for this call's
/// sake.
pub(crate) fn infer_schema(content: &str) -> Result<Vec<InferredColumn>, SchemaError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|e| SchemaError::UserError(format!("could not read CSV header: {e}")))?
        .clone();
    if headers.is_empty() {
        return Err(SchemaError::UserError("CSV has no columns".into()));
    }
    let n = headers.len();
    let mut guesses: Vec<Option<Guess>> = vec![None; n];
    let mut max_len: Vec<usize> = vec![0; n];
    let mut nullable = vec![false; n];

    for record in reader.records().take(SAMPLE_ROWS) {
        let record = record
            .map_err(|e| SchemaError::UserError(format!("could not read CSV row: {e}")))?;
        if record.len() != n {
            return Err(SchemaError::UserError(format!(
                "CSV row has {} field(s), header has {n}",
                record.len()
            )));
        }
        for (i, cell) in record.iter().enumerate() {
            if cell.is_empty() {
                nullable[i] = true;
                continue;
            }
            max_len[i] = max_len[i].max(cell.len());
            let g = classify_cell(cell);
            guesses[i] = Some(match guesses[i] {
                None => g,
                Some(existing) => widen(existing, g),
            });
        }
    }

    Ok(headers
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let datatype = match guesses[i] {
                // No non-empty sample at all (every sampled cell was
                // empty) or a genuine text column either way — capacity
                // is a heuristic, not a guarantee: double the longest
                // sampled value with a reasonable floor/ceiling; a later
                // COPY reports (never silently truncates) any row whose
                // actual value overflows it.
                None | Some(Guess::Str) => {
                    let cap = (max_len[i] * 2).clamp(DEFAULT_VAR_SIZE, 1024) as u32;
                    DataType::Str(cap)
                }
                Some(Guess::Integer) => DataType::Integer,
                Some(Guess::Double) => DataType::Double,
                Some(Guess::Datetime) => DataType::Datetime,
                Some(Guess::Boolean) => DataType::Boolean,
            };
            InferredColumn {
                name: name.to_string(),
                datatype,
                nullable: nullable[i],
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names_and_types(cols: &[InferredColumn]) -> Vec<(String, DataType, bool)> {
        cols.iter()
            .map(|c| (c.name.clone(), c.datatype, c.nullable))
            .collect()
    }

    #[test]
    fn test_infers_integer_double_boolean_and_str_columns() {
        let csv = "id,price,active,name\n1,9.99,true,alice\n2,10,false,bob\n";
        let cols = infer_schema(csv).unwrap();
        let got = names_and_types(&cols);
        assert_eq!(got[0], ("id".into(), DataType::Integer, false));
        // price mixes 9.99 and 10 (an integer literal) — widens to Double.
        assert_eq!(got[1], ("price".into(), DataType::Double, false));
        assert_eq!(got[2], ("active".into(), DataType::Boolean, false));
        assert_eq!(got[3].0, "name");
        assert!(matches!(got[3].1, DataType::Str(_)));
    }

    #[test]
    fn test_a_column_with_an_empty_cell_is_nullable() {
        let csv = "id,note\n1,hi\n2,\n";
        let cols = infer_schema(csv).unwrap();
        assert!(!cols[0].nullable);
        assert!(cols[1].nullable);
    }

    #[test]
    fn test_mixed_types_in_one_column_fall_back_to_str() {
        let csv = "v\n1\nhello\n";
        let cols = infer_schema(csv).unwrap();
        assert!(matches!(cols[0].datatype, DataType::Str(_)));
    }

    #[test]
    fn test_an_all_empty_column_is_a_nullable_str() {
        let csv = "a,b\n1,\n2,\n";
        let cols = infer_schema(csv).unwrap();
        assert!(matches!(cols[1].datatype, DataType::Str(_)));
        assert!(cols[1].nullable);
    }

    #[test]
    fn test_only_the_first_sample_rows_rows_are_examined() {
        // A type-conflicting row far past the sample window must not
        // affect the inferred type.
        let mut csv = String::from("v\n");
        for _ in 0..SAMPLE_ROWS {
            csv.push_str("1\n");
        }
        csv.push_str("not-a-number\n");
        let cols = infer_schema(&csv).unwrap();
        assert_eq!(cols[0].datatype, DataType::Integer);
    }

    #[test]
    fn test_rejects_a_row_with_the_wrong_number_of_fields() {
        let csv = "a,b\n1,2,3\n";
        assert!(infer_schema(csv).is_err());
    }
}
