use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use squeal_sql::conn::connection::{Connection, ConnectionManager};
use squeal_sql::rslt::resultset::ResultType;
use squeal_sql::source::QueryStats;
use store::db::DBFile;
use store::named_memfile::NamedMemFile;

const DEFAULT_SCHEMA: &str = "default";
const HISTORY_FILE: &str = "history.txt";

enum Backend {
    File,
    Memory,
}

fn main() -> Result<()> {
    let db_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/rajiv/dev/rust/squeal_db/test_data/squeal.db".to_string());
    let mut rl = DefaultEditor::new()?;

    // Asked interactively rather than via a CLI flag — a flag has to be
    // remembered and retyped on every launch, a prompt doesn't.
    let backend = loop {
        match rl.readline("Use (f)ile or (m)emory storage? [f/m]: ") {
            Ok(line) => match line.trim().to_lowercase().as_str() {
                "f" | "file" | "" => break Backend::File,
                "m" | "mem" | "memory" => break Backend::Memory,
                other => {
                    println!("unrecognized choice {other:?} — enter 'f' or 'm'");
                }
            },
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => return Ok(()),
            Err(err) => return Err(err),
        }
    };

    let result = match backend {
        Backend::File => {
            // File-backed connections share the process-wide singleton
            // (see ConnectionManager::<File>::get_manager) rather than a
            // fresh manager, so re-running `connect` for a name already
            // open in this process reuses it instead of reopening the
            // file out from under itself.
            let mgr = ConnectionManager::get_manager();
            run_repl(rl, connect_or_create(&mgr, &db_path), &db_path)
        }
        Backend::Memory => {
            // A fresh, non-singleton manager is fine here — NamedMemFile
            // itself (unlike plain MemFile) already persists a name's
            // buffer across separate open() calls via its own
            // process-wide registry, so nothing is lost by not sharing
            // one manager instance.
            let mgr: Arc<ConnectionManager<NamedMemFile>> = Arc::new(ConnectionManager::new());
            run_repl(rl, connect_or_create(&mgr, &db_path), &db_path)
        }
    };
    print_memory_stats();
    result
}

// Dumps store::alloc's accumulated stats (see its own doc comment for
// why the tracking allocator itself lives there, not here) — meant to
// be read right after a batch of work (e.g. piping a whole import
// script into the REPL's stdin, then exiting) rather than mid-session,
// since these are process-lifetime totals/peaks, not scoped to any one
// statement.
fn print_memory_stats() {
    let stats = store::alloc::stats();
    println!();
    println!("=== allocator stats ===");
    println!("total allocated: {} bytes", stats.total_allocated);
    println!("peak usage:      {} bytes", stats.peak_usage);
    println!("current usage:   {} bytes", stats.current_usage);
    println!(
        "reallocs:        {} ({} grew, {} shrank)",
        stats.realloc_count, stats.realloc_grew, stats.realloc_shrank
    );
    let width = store::alloc::SIZE_PER_BUCKET;
    let last = store::alloc::BUCKET_COUNT - 1;
    println!("allocation size histogram (bucket width = {width} bytes):");
    for (i, count) in stats.size_histogram.iter().enumerate() {
        let label = if i == last {
            format!("{}+", i * width)
        } else {
            format!("{}-{}", i * width, (i + 1) * width - 1)
        };
        println!("  {label:>8}: {count}");
    }
}

fn connect_or_create<F>(mgr: &Arc<ConnectionManager<F>>, db_path: &str) -> Arc<Connection<F>>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // Open it if it already exists from a previous session, otherwise
    // this is a first run — create it instead.
    mgr.connect(db_path)
        .or_else(|_| mgr.create_and_connect(db_path))
        .unwrap_or_else(|e| {
            eprintln!("failed to open database {db_path:?}: {e}");
            std::process::exit(1);
        })
}

fn run_repl<F>(mut rl: DefaultEditor, conn: Arc<Connection<F>>, db_path: &str) -> Result<()>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // Database::create/open always ensures a "default" schema exists —
    // land there so CREATE TABLE etc. work immediately without an
    // explicit USE SCHEMA first. Not fatal if it's somehow missing: the
    // user can still issue CREATE SCHEMA/USE SCHEMA by hand.
    if let Err(e) = conn.use_schema(DEFAULT_SCHEMA) {
        eprintln!("warning: could not select schema {DEFAULT_SCHEMA:?}: {e}");
    }
    println!("Connected to database {db_path:?}.");

    if rl.load_history(HISTORY_FILE).is_err() {
        println!("No previous history.");
    }
    // Stats from the most recently executed StreamingResult, kept around
    // so `!print stats` can be typed as a separate line after a query
    // rather than needing to be bolted onto the query itself.
    let mut last_stats: Option<Vec<(String, QueryStats)>> = None;
    loop {
        let readline = rl.readline("sql>> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line == "exit" {
                    break;
                }
                rl.add_history_entry(line)?;
                if let Some(command) = line.strip_prefix('!') {
                    run_custom_command(command.trim(), &conn, &last_stats);
                    continue;
                }
                if let Some(stats) = run(&conn, line) {
                    last_stats = Some(stats);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(HISTORY_FILE)?;
    // Flushes every loaded schema's metadata and truncates the WAL (see
    // Connection::close/Database::close) — without this, a table
    // created (or a row inserted) in one squeal-cli run was never
    // visible to the next one against the same file. A failure here is
    // reported, not fatal: the process is exiting either way.
    if let Err(e) = conn.close() {
        eprintln!("warning: could not cleanly close the database: {e}");
    }
    Ok(())
}

// Parses+runs `sql` as one Statement (which may itself hold several
// ;-separated statements) against `conn`'s current database/schema, and
// prints every result it produced. Never propagates a SQL/store error
// up to main() — a bad statement should end the REPL turn, not the
// session. Returns the last StreamingResult's query stats seen along
// the way (a statement can produce several results; whichever one ran
// last "wins", same as what's left on screen).
fn run<F>(conn: &Arc<Connection<F>>, sql: &str) -> Option<Vec<(String, QueryStats)>>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    let mut stmt = match conn.clone().create_statement(sql) {
        Ok(s) => s,
        Err(e) => {
            println!("error: {e}");
            return None;
        }
    };
    if let Err(e) = stmt.execute() {
        println!("error: {e}");
        return None;
    }

    let mut stats = None;
    let mut next = stmt.get_results();
    loop {
        match next {
            Ok(Some(mut r)) => {
                if let Some(s) = print_result(&mut r) {
                    stats = Some(s);
                }
                next = stmt.get_nextresult();
            }
            Ok(None) => break,
            Err(e) => {
                println!("error: {e}");
                break;
            }
        }
    }
    stats
}

fn print_result(r: &mut ResultType) -> Option<Vec<(String, QueryStats)>> {
    match r {
        ResultType::ResultString(s) => {
            println!("{s}");
            None
        }
        ResultType::Count(n) => {
            println!("{n} row(s) affected");
            None
        }
        ResultType::Result(rs) => {
            let mut table = comfy_table::Table::new();
            table.set_header(rs.columns().to_vec());
            for row in rs.rows_as_strings() {
                table.add_row(row);
            }
            println!("{table}");
            println!("{}", rs.get_final_message());
            None
        }
        ResultType::StreamingResult(stream) => {
            let mut table = comfy_table::Table::new();
            table.set_header(stream.columns());
            loop {
                match stream.next_result_as_strings() {
                    Ok(Some(row)) => table.add_row(row),
                    Ok(None) => break,
                    Err(e) => {
                        println!("error: {e}");
                        break;
                    }
                };
            }
            println!("{table}");
            println!("{}", stream.get_final_message());
            // Only available once the stream is fully drained (see the
            // loop above) — Source::stats() reports totals accumulated
            // during next(), so reading it any earlier would miss
            // whatever work the remaining rows still had to do.
            stream.get_query_stats()
        }
    }
}

// Every command this REPL understands beyond plain SQL — `!`-prefixed
// ones dispatched by run_custom_command below, plus the bare `exit`
// keyword handled in the main loop — as (syntax, description) pairs, the
// single source of truth for `!help`'s listing. Kept as a flat
// list/match rather than a registry/trait — there's a small, fixed
// number of these and no shape yet that motivates more indirection.
const COMMANDS: &[(&str, &str)] = &[
    ("!help", "show this list of commands"),
    (
        "!print stats",
        "show per-operator timing/row-count stats from the last query",
    ),
    ("!reset stats", "zero the allocator stats counters"),
    (
        "!show table stats",
        "show collected table statistics for the current schema",
    ),
    ("exit", "quit the REPL (Ctrl-D also works)"),
];

// Dispatches a line that started with `!` (stripped of that prefix and
// trimmed) as a REPL-only command rather than SQL.
fn run_custom_command<F>(
    command: &str,
    conn: &Arc<Connection<F>>,
    last_stats: &Option<Vec<(String, QueryStats)>>,
) where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    match command {
        "help" => print_help(),
        "print stats" => print_query_stats(last_stats),
        "reset stats" => reset_alloc_stats(),
        "show table stats" => show_table_stats(conn),
        "" => println!("empty command — {USAGE_HINT}"),
        other => println!("unrecognized command: {other:?} — {USAGE_HINT}"),
    }
}

const USAGE_HINT: &str = "try '!help' for a list of commands";

fn print_help() {
    println!("Available commands:");
    let width = COMMANDS.iter().map(|(cmd, _)| cmd.len()).max().unwrap_or(0);
    for (cmd, desc) in COMMANDS {
        println!("  {cmd:<width$}  {desc}");
    }
    println!();
    // squeal_sql::help::SQL_HELP is the single source of truth for the SQL
    // syntax listing — shared with squeal-wasm's own !help so the two
    // front-ends can't drift apart on what SQL this engine supports.
    println!("{}", squeal_sql::help::sql_help_text());
}

// `!show table stats`: optim::table_stats::SchemaStats' current snapshot
// for the connection's current schema, rendered the same way an ordinary
// query result is (see print_result's own ResultType::Result branch) —
// reuses Connection::table_stats_report so this file doesn't need to
// know anything about how those stats are collected or stored.
fn show_table_stats<F>(conn: &Arc<Connection<F>>)
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    match conn.table_stats_report() {
        Ok(rs) => {
            let mut table = comfy_table::Table::new();
            table.set_header(rs.columns().to_vec());
            for row in rs.rows_as_strings() {
                table.add_row(row);
            }
            println!("{table}");
            println!("{}", rs.get_final_message());
        }
        Err(e) => println!("error: {e}"),
    }
}

// Zeroes store::alloc's tracking-allocator counters (see its own doc
// comment) — meant for isolating one statement's allocation footprint
// from whatever came before it in the same session (e.g. bulk INSERTs
// during test-data setup), by resetting right before running the
// statement you actually want to measure and reading the numbers
// print_memory_stats() dumps at exit.
fn reset_alloc_stats() {
    store::alloc::reset();
    println!("allocator stats reset");
}

fn print_query_stats(stats: &Option<Vec<(String, QueryStats)>>) {
    let Some(stats) = stats else {
        println!("no query stats available yet — run a query first");
        return;
    };
    for (name, query_stats) in stats {
        let indent = "  ".repeat(query_stats.level());
        println!("{indent}{name}");
        let mut entries: Vec<_> = query_stats.stats().iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in entries {
            if let Some(label) = key.strip_suffix("_ns") {
                println!("{indent}  {label}: {:.3} ms", value / 1_000_000.0);
            } else {
                println!("{indent}  {key}: {value}");
            }
        }
    }
}
