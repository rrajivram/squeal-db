//! What the comparison binaries share: running each engine in a process of
//! its own (so its memory is its own), measuring that memory, agreeing on
//! answers across processes, and the report.
//!
//! A binary run with no `--child` is the driver: it runs itself once per
//! engine, profile and repeat as a child, reads the `PHASE` lines each
//! child prints, and prints the medians as tables.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    process::Command,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::Thread,
    time::Instant,
};

use serde::{Deserialize, Serialize};

// ---- memory ---------------------------------------------------------------

/// This process's memory: its physical footprint now and the most it has
/// been (what Activity Monitor shows — heap and other dirty memory, not
/// clean file-backed pages the OS can drop).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Mem {
    pub now: u64,
    pub peak: u64,
}

#[cfg(target_vendor = "apple")]
pub fn mem() -> Mem {
    // SAFETY: proc_pid_rusage fills the struct it is given, of the flavor
    // asked for; a zeroed one is a valid starting value.
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        let r = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            &mut info as *mut _ as *mut _,
        );
        if r != 0 {
            return Mem::default();
        }
        Mem {
            now: info.ri_phys_footprint,
            peak: info.ri_lifetime_max_phys_footprint,
        }
    }
}

#[cfg(not(target_vendor = "apple"))]
pub fn mem() -> Mem {
    // Resident set from /proc: the nearest equivalent.
    let field = |name: &str| -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with(name))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
            })
            .unwrap_or(0)
            * 1024
    };
    Mem {
        now: field("VmRSS:"),
        peak: field("VmHWM:"),
    }
}

// ---- answers ----------------------------------------------------------------

/// A phase's answers reduced to what every engine must agree on: how many
/// rows, and the sum of every value in them (numbers as themselves, text
/// and blobs by length).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Digest {
    pub rows: u64,
    pub sum: f64,
}

impl Digest {
    pub fn row(&mut self) {
        self.rows += 1;
    }
    pub fn value(&mut self, v: f64) {
        self.sum += v;
    }
    pub fn merge(&mut self, o: Digest) {
        self.rows += o.rows;
        self.sum += o.sum;
    }
    pub fn agrees(&self, o: &Digest) -> bool {
        self.rows == o.rows && (self.sum - o.sum).abs() <= 1e-6 * self.sum.abs().max(1.0)
    }
}

// ---- async ------------------------------------------------------------------

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs a future to completion on this thread. Turso's API is async but
/// does its I/O itself, so it needs no runtime, only something to poll it.
pub fn block_on<F: Future>(f: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = pin!(f);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park_timeout(std::time::Duration::from_millis(1)),
        }
    }
}

// ---- the child's side ---------------------------------------------------------

/// One phase as a child reports it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseOut {
    pub phase: String,
    pub ops: u64,
    pub nanos: u64,
    pub digest: Option<Digest>,
    pub mem: Mem,
}

/// Times `f` as one phase and prints it for the driver. Memory is read
/// after, outside the timing.
pub fn phase(name: &str, ops: u64, f: impl FnOnce() -> Option<Digest>) {
    let t = Instant::now();
    let digest = f();
    let nanos = t.elapsed().as_nanos() as u64;
    let out = PhaseOut {
        phase: name.to_string(),
        ops,
        nanos,
        digest,
        mem: mem(),
    };
    println!("PHASE {}", serde_json::to_string(&out).unwrap());
}

/// A line about the engine as it is running (its effective settings), for
/// the report.
pub fn info(text: &str) {
    println!("INFO {text}");
}

/// What a child was asked to run.
pub struct ChildArgs {
    pub engine: String,
    pub profile: String,
    pub size: u64,
    pub threads: usize,
    pub dir: std::path::PathBuf,
}

/// `--child <engine> <profile> <size> <threads> <dir>`, if that is how this
/// process was started.
pub fn child_args() -> Option<ChildArgs> {
    let args: Vec<String> = std::env::args().collect();
    let i = args.iter().position(|a| a == "--child")?;
    let a = &args[i + 1..];
    Some(ChildArgs {
        engine: a[0].clone(),
        profile: a[1].clone(),
        size: a[2].parse().unwrap(),
        threads: a[3].parse().unwrap(),
        dir: a[4].clone().into(),
    })
}

// ---- the driver's side ----------------------------------------------------------

/// What the driver runs: every engine under every profile, `runs` times.
pub struct Plan {
    pub engines: Vec<(String, String)>,
    pub profiles: Vec<(String, String)>,
    pub size: u64,
    pub threads: usize,
    pub runs: usize,
}

/// Driver options from the command line: `--engines a,b --profiles x,y
/// --size N --threads N --runs N`, each defaulting to what is given.
pub fn plan_from_args(
    engines: &[(&str, &str)],
    profiles: &[(&str, &str)],
    size: u64,
) -> Plan {
    let args: Vec<String> = std::env::args().collect();
    let opt = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1).cloned())
    };
    let pick = |all: &[(&str, &str)], name: &str| -> Vec<(String, String)> {
        let chosen = opt(name);
        all.iter()
            .filter(|(k, _)| chosen.as_ref().is_none_or(|c| c.split(',').any(|x| x == *k)))
            .map(|(k, d)| (k.to_string(), d.to_string()))
            .collect()
    };
    Plan {
        engines: pick(engines, "--engines"),
        profiles: pick(profiles, "--profiles"),
        size: opt("--size").and_then(|s| s.parse().ok()).unwrap_or(size),
        threads: opt("--threads").and_then(|s| s.parse().ok()).unwrap_or(4),
        runs: opt("--runs").and_then(|s| s.parse().ok()).unwrap_or(3),
    }
}

/// Runs every child the plan asks for and prints the report. `unit` says
/// what a phase's ops are counted in.
pub fn drive(plan: &Plan, title: &str) {
    let exe = std::env::current_exe().unwrap();
    // (profile, engine) -> each run's phases
    let mut results: BTreeMap<(usize, usize), Vec<Vec<PhaseOut>>> = BTreeMap::new();
    let mut infos: BTreeMap<(usize, usize), Vec<String>> = BTreeMap::new();
    for run in 0..plan.runs {
        for (pi, (profile, _)) in plan.profiles.iter().enumerate() {
            for (ei, (engine, _)) in plan.engines.iter().enumerate() {
                let dir = std::env::temp_dir().join(format!(
                    "squeal_bench_{}_{engine}_{profile}_{run}",
                    std::process::id()
                ));
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).unwrap();
                eprintln!("run {}/{}: {engine}, {profile}", run + 1, plan.runs);
                let out = Command::new(&exe)
                    .args([
                        "--child",
                        engine,
                        profile,
                        &plan.size.to_string(),
                        &plan.threads.to_string(),
                        dir.to_str().unwrap(),
                    ])
                    .output()
                    .expect("run a child");
                let _ = std::fs::remove_dir_all(&dir);
                let stdout = String::from_utf8_lossy(&out.stdout);
                if !out.status.success() {
                    eprintln!(
                        "{engine}/{profile} failed:\n{stdout}\n{}",
                        String::from_utf8_lossy(&out.stderr)
                    );
                    std::process::exit(1);
                }
                let phases: Vec<PhaseOut> = stdout
                    .lines()
                    .filter_map(|l| l.strip_prefix("PHASE "))
                    .map(|j| serde_json::from_str(j).unwrap())
                    .collect();
                results.entry((pi, ei)).or_default().push(phases);
                infos.insert(
                    (pi, ei),
                    stdout
                        .lines()
                        .filter_map(|l| l.strip_prefix("INFO "))
                        .map(String::from)
                        .collect(),
                );
            }
        }
    }
    report(plan, title, &results, &infos);
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn per_op(nanos: f64) -> String {
    if nanos >= 1e6 {
        format!("{:.1} ms", nanos / 1e6)
    } else if nanos >= 1e3 {
        format!("{:.2} µs", nanos / 1e3)
    } else {
        format!("{nanos:.0} ns")
    }
}

fn mib(bytes: f64) -> String {
    format!("{:.0} MiB", bytes / (1024.0 * 1024.0))
}

fn report(
    plan: &Plan,
    title: &str,
    results: &BTreeMap<(usize, usize), Vec<Vec<PhaseOut>>>,
    infos: &BTreeMap<(usize, usize), Vec<String>>,
) {
    println!("# {title}\n");
    println!(
        "size {}, {} reader threads, median of {} run(s), each engine in its own process. \
         Times are per operation; memory is the process's physical footprint.\n",
        plan.size, plan.threads, plan.runs
    );
    for (pi, (profile, about)) in plan.profiles.iter().enumerate() {
        println!("## Profile `{profile}`: {about}\n");
        for (ei, (engine, about)) in plan.engines.iter().enumerate() {
            let running = infos.get(&(pi, ei)).map(|v| v.join("; ")).unwrap_or_default();
            if running.is_empty() {
                println!("- **{engine}**: {about}");
            } else {
                println!("- **{engine}**: {about}. Running with {running}");
            }
        }
        println!();
        let phases: Vec<&PhaseOut> = results[&(pi, 0)][0].iter().collect();
        let mut header = String::from("| phase |");
        let mut rule = String::from("|---|");
        for (engine, _) in &plan.engines {
            header.push_str(&format!(" {engine} |"));
            rule.push_str("---:|");
        }
        header.push_str(" same answers |");
        rule.push_str("---|");
        println!("{header}\n{rule}");
        let mut disagreements = vec![];
        for (k, first) in phases.iter().enumerate() {
            if first.ops == 0 {
                continue;
            }
            let mut line = format!("| {} |", first.phase);
            let mut digests = vec![];
            for ei in 0..plan.engines.len() {
                let runs = &results[&(pi, ei)];
                let t = median(runs.iter().map(|r| r[k].nanos as f64).collect());
                line.push_str(&format!(" {} |", per_op(t / first.ops as f64)));
                digests.extend(runs.iter().map(|r| (ei, r[k].digest)));
            }
            let reference = digests[0].1;
            let agree = digests.iter().all(|(_, d)| match (d, &reference) {
                (Some(a), Some(b)) => a.agrees(b),
                (None, None) => true,
                _ => false,
            });
            if !agree {
                disagreements.push((first.phase.clone(), digests.clone()));
            }
            line.push_str(if reference.is_none() {
                " — |"
            } else if agree {
                " yes |"
            } else {
                " **NO** |"
            });
            println!("{line}");
        }
        println!();
        // Memory: after the named phases, and the peak.
        let marks: Vec<(usize, &str)> = phases
            .iter()
            .enumerate()
            .filter(|(_, p)| p.phase.starts_with('[') || p.ops == 0)
            .map(|(i, p)| (i, p.phase.as_str()))
            .collect();
        let mut header = String::from("| memory |");
        let mut rule = String::from("|---|");
        for (engine, _) in &plan.engines {
            header.push_str(&format!(" {engine} |"));
            rule.push_str("---:|");
        }
        println!("{header}\n{rule}");
        for (k, name) in marks.iter().copied().chain(std::iter::once((usize::MAX, "peak"))) {
            let mut line = format!("| {name} |");
            for ei in 0..plan.engines.len() {
                let runs = &results[&(pi, ei)];
                let v = median(
                    runs.iter()
                        .map(|r| {
                            if k == usize::MAX {
                                r.iter().map(|p| p.mem.peak).max().unwrap_or(0) as f64
                            } else {
                                r[k].mem.now as f64
                            }
                        })
                        .collect(),
                );
                line.push_str(&format!(" {} |", mib(v)));
            }
            println!("{line}");
        }
        println!();
        for (phase, digests) in disagreements {
            eprintln!("disagreement in {profile} / {phase}:");
            for (ei, d) in digests {
                eprintln!("  {}: {d:?}", plan.engines[ei].0);
            }
        }
    }
}
