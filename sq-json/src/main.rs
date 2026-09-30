//! `sq-json <file>`: a mongosh-style shell over a database file (created if
//! it doesn't exist). Type `help` for the statements.

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use sq_json::Client;
use sq_json::shell::{Outcome, Shell};

const HISTORY_FILE: &str = ".sq-json-history";

fn main() {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: sq-json <database file>");
        std::process::exit(2);
    };
    let client = if std::path::Path::new(&path).exists() {
        Client::<std::fs::File>::open(&path)
    } else {
        Client::<std::fs::File>::create(&path)
    };
    let client = client.unwrap_or_else(|e| {
        eprintln!("failed to open {path:?}: {e}");
        std::process::exit(1);
    });
    let mut shell = Shell::new(client);
    let mut rl = DefaultEditor::new().expect("a line editor");
    let _ = rl.load_history(HISTORY_FILE);
    println!("sq-json on {path:?}. Type help for commands.");
    loop {
        match rl.readline(&shell.prompt()) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = rl.add_history_entry(line.as_str());
                }
                match shell.execute(&line) {
                    Ok(Outcome::Output(s)) if s.is_empty() => {}
                    Ok(Outcome::Output(s)) => println!("{s}"),
                    Ok(Outcome::Exit) => break,
                    Err(e) => println!("error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }
    let _ = rl.save_history(HISTORY_FILE);
    if let Err(e) = shell.close() {
        eprintln!("closing: {e}");
    }
}
