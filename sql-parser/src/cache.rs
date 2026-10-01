//! A process-wide cache of parsed statements, keyed by the exact SQL text.
//!
//! Parsing is deterministic — the same text always gives the same
//! statements — so a repeated statement can reuse its first parse. The key
//! is the whole text, not a checksum of it: the map hashes the text only to
//! find a bucket and then compares it in full, so two texts can never share
//! an entry the way two colliding checksums would.
//!
//! Only successful parses are kept (an error is cheap to reproduce, and
//! rare), only texts up to MAX_CACHED_LEN (a bulk INSERT is rarely repeated
//! and would crowd out everything else), and at most CAPACITY of them, the
//! least recently used going first.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use crate::{ParseError, Statement, parse_sql};

const CAPACITY: usize = 512;
const MAX_CACHED_LEN: usize = 8 * 1024;

#[derive(Default)]
struct Cache {
    entries: HashMap<String, Entry>,
    // Advances on every use; an entry's `used` says when it was last used,
    // and `by_use` finds the least recently used without a scan.
    clock: u64,
    by_use: BTreeMap<u64, String>,
    hits: u64,
    misses: u64,
}

struct Entry {
    statements: Arc<[Statement]>,
    used: u64,
}

/// How the cache has done so far (process-wide).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
}

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// As [`parse_sql`], reusing the statements of an earlier parse of the
/// same text.
pub fn parse_sql_cached(src: &str) -> Result<Arc<[Statement]>, Vec<ParseError>> {
    if src.len() > MAX_CACHED_LEN {
        return parse_sql(src).map(Arc::from);
    }
    {
        let mut c = cache().lock().unwrap_or_else(|e| e.into_inner());
        c.clock += 1;
        let now = c.clock;
        if let Some(entry) = c.entries.get_mut(src) {
            let before = std::mem::replace(&mut entry.used, now);
            let statements = entry.statements.clone();
            if let Some(text) = c.by_use.remove(&before) {
                c.by_use.insert(now, text);
            }
            c.hits += 1;
            return Ok(statements);
        }
        c.misses += 1;
    }
    // Parsed outside the lock: two threads parsing the same new text both
    // do the work once, and the second insert just replaces the first.
    let statements: Arc<[Statement]> = parse_sql(src)?.into();
    let mut c = cache().lock().unwrap_or_else(|e| e.into_inner());
    if c.entries.len() >= CAPACITY
        && !c.entries.contains_key(src)
        && let Some((_, oldest)) = c.by_use.pop_first()
    {
        c.entries.remove(&oldest);
    }
    c.clock += 1;
    let used = c.clock;
    let replaced = c.entries.insert(
        src.to_string(),
        Entry {
            statements: statements.clone(),
            used,
        },
    );
    if let Some(old) = replaced {
        c.by_use.remove(&old.used);
    }
    c.by_use.insert(used, src.to_string());
    Ok(statements)
}

pub fn cache_stats() -> CacheStats {
    let c = cache().lock().unwrap_or_else(|e| e.into_inner());
    CacheStats {
        hits: c.hits,
        misses: c.misses,
        entries: c.entries.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cache is process-wide and tests run in parallel: each test uses
    // texts of its own, and checks sharing by pointer, not by counts.

    #[test]
    fn test_the_same_text_shares_one_parse() {
        let a = parse_sql_cached("select 1 as cache_test_one").unwrap();
        let b = parse_sql_cached("select 1 as cache_test_one").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let c = parse_sql_cached("select 1 as cache_test_two").unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(*a, *parse_sql("select 1 as cache_test_one").unwrap());
    }

    #[test]
    fn test_errors_and_long_texts_are_not_kept() {
        assert!(parse_sql_cached("select from cache_test_bad").is_err());
        assert!(parse_sql_cached("select from cache_test_bad").is_err());
        let long = format!("select '{}' as cache_test_long", "x".repeat(MAX_CACHED_LEN));
        let a = parse_sql_cached(&long).unwrap();
        let b = parse_sql_cached(&long).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(a, b);
    }

    #[test]
    fn test_the_cache_stays_bounded() {
        let first = parse_sql_cached("select 0 as cache_test_bound").unwrap();
        for i in 1..=CAPACITY + 10 {
            parse_sql_cached(&format!("select {i} as cache_test_bound")).unwrap();
        }
        assert!(cache_stats().entries <= CAPACITY);
        // The oldest went first.
        let again = parse_sql_cached("select 0 as cache_test_bound").unwrap();
        assert!(!Arc::ptr_eq(&first, &again));
    }
}
