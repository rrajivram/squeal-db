#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Mem,
    File,
    Both,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub backend: Backend,
    pub rows: u64,
    pub small_value_size: usize,
    pub large_rows: u64,
    pub large_value_size: usize,
    pub remove_fraction_pct: u8,
    pub thread_counts: Vec<usize>,
    pub ops_per_thread: u64,
    pub seed: u64,
    pub page_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: Backend::Both,
            rows: 20_000,
            small_value_size: 64,
            large_rows: 2_000,
            large_value_size: 8_192,
            remove_fraction_pct: 20,
            thread_counts: vec![1, 2, 4, 8],
            ops_per_thread: 5_000,
            seed: 0x00C0_FFEE_1234_5678,
            page_size: 16 * 1024,
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
                "--backend" => {
                    cfg.backend = match next().as_str() {
                        "mem" => Backend::Mem,
                        "file" => Backend::File,
                        "both" => Backend::Both,
                        other => {
                            eprintln!("unknown backend '{other}', expected mem|file|both");
                            std::process::exit(2);
                        }
                    }
                }
                "--rows" => cfg.rows = next().parse().expect("--rows must be a number"),
                "--small-value-size" => {
                    cfg.small_value_size = next()
                        .parse()
                        .expect("--small-value-size must be a number")
                }
                "--large-rows" => {
                    cfg.large_rows = next().parse().expect("--large-rows must be a number")
                }
                "--large-value-size" => {
                    cfg.large_value_size = next()
                        .parse()
                        .expect("--large-value-size must be a number")
                }
                "--remove-fraction-pct" => {
                    cfg.remove_fraction_pct = next()
                        .parse()
                        .expect("--remove-fraction-pct must be 0-100")
                }
                "--thread-counts" => {
                    cfg.thread_counts = next()
                        .split(',')
                        .map(|s| s.trim().parse().expect("--thread-counts must be comma-separated numbers"))
                        .collect();
                }
                "--ops-per-thread" => {
                    cfg.ops_per_thread = next().parse().expect("--ops-per-thread must be a number")
                }
                "--seed" => cfg.seed = next().parse().expect("--seed must be a number"),
                "--page-size" => {
                    cfg.page_size = next().parse().expect("--page-size must be a number")
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
        "Performance report harness for squeal_db — drives the library purely through its\n\
         public API (Db<MemFile>/Db<File>). Makes no correctness assertions (see the\n\
         `stress` example for that); reports throughput and latency percentiles per phase.\n\n\
         USAGE: cargo run --release --example perf -- [FLAGS]\n\n\
         FLAGS:\n\
         \x20\x20--backend mem|file|both     storage backend(s) to report on (default: both)\n\
         \x20\x20--rows <N>                  rows for single-threaded phases (default: 20000)\n\
         \x20\x20--small-value-size <N>      bytes per row in the small-value phases (default: 64)\n\
         \x20\x20--large-rows <N>            rows for the large-value (overflow-page) phase (default: 2000)\n\
         \x20\x20--large-value-size <N>      bytes per row in the large-value phase (default: 8192)\n\
         \x20\x20--remove-fraction-pct <N>   percent of rows removed in the remove phase (default: 20)\n\
         \x20\x20--thread-counts <a,b,c>     thread counts for the scaling phase (default: 1,2,4,8)\n\
         \x20\x20--ops-per-thread <N>        inserts per thread in the scaling phase (default: 5000)\n\
         \x20\x20--seed <N>                  PRNG seed (default: fixed constant)\n\
         \x20\x20--page-size <N>             page size in bytes (default: 16384)\n"
    );
}
