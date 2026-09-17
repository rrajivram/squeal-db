//! Soak runner for the crash-consistency harness (see `store::crash_harness`).
//!
//!     cargo run --release --example crash -- --seeds 100 --rounds 5 --threads 8
//!
//! Prints one line per seed and stops at the first failure with its full
//! diagnosis, so the failing seed can be re-run under a debugger or with
//! `--seed N --seeds 1`.

use store::crash_harness::{CrashConfig, run};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cfg = CrashConfig::default();
    let mut seeds = 20u64;
    let mut i = 0;
    while i < args.len() {
        let key = args[i].as_str();
        let mut next = || {
            i += 1;
            args.get(i).cloned().unwrap_or_else(|| {
                eprintln!("missing value for {key}");
                std::process::exit(2);
            })
        };
        match key {
            "--seed" => cfg.seed = next().parse().expect("--seed"),
            "--seeds" => seeds = next().parse().expect("--seeds"),
            "--threads" => cfg.threads = next().parse().expect("--threads"),
            "--tables" => cfg.tables = next().parse().expect("--tables"),
            "--keys" => cfg.keys_per_thread = next().parse().expect("--keys"),
            "--rounds" => cfg.rounds = next().parse().expect("--rounds"),
            "--max-ops" => cfg.max_ops_per_txn = next().parse().expect("--max-ops"),
            "--commit-pct" => cfg.commit_probability_pct = next().parse().expect("--commit-pct"),
            "--page-size" => cfg.page_size = next().parse().expect("--page-size"),
            "--cut-ms" => {
                let v = next();
                let (a, b) = v.split_once('-').expect("--cut-ms LO-HI");
                cfg.cut_after_ms = (a.parse().unwrap(), b.parse().unwrap());
            }
            "--no-checkpoints" => cfg.checkpoint_every_ms = None,
            "--dump-dir" => cfg.dump_dir = Some(next()),
            "--help" | "-h" => {
                println!("crash [--seed N] [--seeds N] [--threads N] [--tables N] [--keys N] [--rounds N] [--max-ops N] [--commit-pct N] [--page-size N] [--cut-ms LO-HI] [--no-checkpoints]");
                return;
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    let first = cfg.seed;
    for s in 0..seeds {
        cfg.seed = first + s;
        match run(&cfg) {
            Ok(r) => println!(
                "seed {} ok: rounds={} definite={} ambiguous={} (recovered {}) rolled_back={} checkpoints={} rows={}",
                cfg.seed, r.rounds, r.committed_definite, r.committed_ambiguous, r.ambiguous_recovered,
                r.rolled_back, r.checkpoints, r.rows_verified
            ),
            Err(e) => {
                eprintln!("FAIL\n{e}");
                std::process::exit(1);
            }
        }
    }
    println!("RESULT: PASS ({seeds} seed(s))");
}
