#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Mem,
    File,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub threads: usize,
    pub tables: usize,
    pub ops_per_thread: u64,
    pub hot_keys: u64,
    pub private_keys: u64,
    pub backend: Backend,
    pub seed: u64,
    pub report_interval_secs: u64,
    pub watchdog_timeout_secs: u64,
    pub commit_probability_pct: u8,
    pub max_ops_per_txn: u32,
    pub max_lock_retries: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            tables: 4,
            ops_per_thread: 20_000,
            hot_keys: 16,
            private_keys: 2_000,
            backend: Backend::Mem,
            seed: 0xDEAD_BEEF_CAFE_F00D,
            report_interval_secs: 2,
            watchdog_timeout_secs: 15,
            commit_probability_pct: 90,
            max_ops_per_txn: 5,
            max_lock_retries: 5,
        }
    }
}

impl Config {
    pub fn from_args() -> Self {
        let mut cfg = Self::default();
        let args: Vec<String> = std::env::args().skip(1).collect();
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
                "--threads" => cfg.threads = next().parse().expect("--threads must be a number"),
                "--tables" => cfg.tables = next().parse().expect("--tables must be a number"),
                "--ops" => cfg.ops_per_thread = next().parse().expect("--ops must be a number"),
                "--hot-keys" => cfg.hot_keys = next().parse().expect("--hot-keys must be a number"),
                "--private-keys" => {
                    cfg.private_keys = next().parse().expect("--private-keys must be a number")
                }
                "--backend" => {
                    cfg.backend = match next().as_str() {
                        "mem" => Backend::Mem,
                        "file" => Backend::File,
                        other => {
                            eprintln!("unknown backend '{other}', expected mem|file");
                            std::process::exit(2);
                        }
                    }
                }
                "--seed" => cfg.seed = next().parse().expect("--seed must be a number"),
                "--report-interval-secs" => {
                    cfg.report_interval_secs =
                        next().parse().expect("--report-interval-secs must be a number")
                }
                "--watchdog-timeout-secs" => {
                    cfg.watchdog_timeout_secs =
                        next().parse().expect("--watchdog-timeout-secs must be a number")
                }
                "--commit-probability-pct" => {
                    cfg.commit_probability_pct =
                        next().parse().expect("--commit-probability-pct must be 0-100")
                }
                "--max-ops-per-txn" => {
                    cfg.max_ops_per_txn = next().parse().expect("--max-ops-per-txn must be a number")
                }
                "--max-lock-retries" => {
                    cfg.max_lock_retries =
                        next().parse().expect("--max-lock-retries must be a number")
                }
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("unknown flag '{other}'");
                    print_help();
                    std::process::exit(2);
                }
            }
            i += 1;
        }
        cfg
    }
}

fn print_help() {
    println!(
        "Concurrency/correctness/throughput stress harness for squeal_db.\n\
         Drives the library purely through its public API (Db<MemFile>/Db<File>),\n\
         the same surface an external consumer would use.\n\n\
         USAGE: cargo run --release --example stress -- [FLAGS]\n\n\
         FLAGS:\n\
         \x20\x20--threads <N>                 worker threads (default: available_parallelism)\n\
         \x20\x20--tables <N>                  number of tables (default: 4)\n\
         \x20\x20--ops <N>                     ops per thread (default: 20000)\n\
         \x20\x20--hot-keys <N>                shared contended keys per table (default: 16)\n\
         \x20\x20--private-keys <N>            per-thread private keys per table (default: 2000)\n\
         \x20\x20--backend mem|file            storage backend (default: mem)\n\
         \x20\x20--seed <N>                    PRNG seed (default: fixed constant)\n\
         \x20\x20--report-interval-secs <N>    live progress interval (default: 2)\n\
         \x20\x20--watchdog-timeout-secs <N>   stall->deadlock threshold (default: 15)\n\
         \x20\x20--commit-probability-pct <N>  chance a txn commits vs rolls back (default: 90)\n\
         \x20\x20--max-ops-per-txn <N>         max ops batched per txn (default: 5)\n\
         \x20\x20--max-lock-retries <N>        retries on LockContentionError (default: 5)\n"
    );
}
