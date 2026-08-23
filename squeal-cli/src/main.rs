use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use squeal_sql::conn::connection::{Connection, ConnectionManager};
use squeal_sql::rslt::resultset::ResultType;
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
        .unwrap_or_else(|| "squeal.db".to_string());
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

    match backend {
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
                run(&conn, line);
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
// session.
fn run<F>(conn: &Arc<Connection<F>>, sql: &str)
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    let mut stmt = match conn.clone().create_statement(sql) {
        Ok(s) => s,
        Err(e) => {
            println!("error: {e}");
            return;
        }
    };
    if let Err(e) = stmt.execute() {
        println!("error: {e}");
        return;
    }

    let mut next = stmt.get_results();
    loop {
        match next {
            Ok(Some(r)) => {
                print_result(&r);
                next = stmt.get_nextresult();
            }
            Ok(None) => break,
            Err(e) => {
                println!("error: {e}");
                break;
            }
        }
    }
}

fn print_result(r: &ResultType) {
    match r {
        ResultType::ResultString(s) => println!("{s}"),
        ResultType::Count(n) => println!("{n} row(s) affected"),
        ResultType::Result(rs) => {
            let mut table = comfy_table::Table::new();
            table.set_header(rs.columns().to_vec());
            for row in rs.rows_as_strings() {
                table.add_row(row);
            }
            println!("{table}");
        }
    }
}
