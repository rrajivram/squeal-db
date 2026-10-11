//! `sq-json <file> [--setting value ...]`: a mongosh-style shell over a
//! database file (created if it doesn't exist). Type `help` for the
//! statements; `sq-json --help x` lists the settings (see store::config).

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use sq_json::{Client, CreateConfig, OpenConfig};
use sq_json::shell::{Outcome, Shell};

const HISTORY_FILE: &str = ".sq-json-history";

fn usage() -> ! {
    eprintln!("usage: sq-json <database file> [--setting value ...]\n");
    eprint!("{}", store::config::settings_help(true));
    std::process::exit(2);
}

fn main() {
    let fail = |e: &dyn std::fmt::Display| -> ! {
        eprintln!("{e}");
        std::process::exit(2);
    };
    let (positional, settings) = store::config::split_args(std::env::args().skip(1))
        .unwrap_or_else(|e| fail(&e));
    if settings.iter().any(|(name, _)| name == "help") {
        usage();
    }
    let [path] = &positional[..] else { usage() };
    let settings = settings.iter().map(|(n, v)| (n.as_str(), v.as_str()));
    // An existing file is opened — with the settings a database can be
    // opened with; a new one is created, with any of them.
    let client = if std::path::Path::new(path).exists() {
        let config = OpenConfig::from_settings(settings).unwrap_or_else(|e| fail(&e));
        Client::<std::fs::File>::open_with(path, &config)
    } else {
        let config = CreateConfig::from_settings(settings).unwrap_or_else(|e| fail(&e));
        Client::<std::fs::File>::create_with(path, &config)
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
