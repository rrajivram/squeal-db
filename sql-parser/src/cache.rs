//! A process-wide cache of parsed statements.
//!
//! Parsing is deterministic, so a statement seen before can reuse its first
//! parse. Two levels:
//!
//! - **Exact text.** The key is the whole text, not a checksum of it: the
//!   map hashes the text only to find a bucket and then compares it in full,
//!   so two texts never share an entry the way two colliding checksums
//!   could.
//! - **Shape.** Statements that differ only in their literals
//!   (`... where id = 'A1'`, `... where id = 'B2'`) share one parse: the
//!   text with each string and number literal replaced by a placeholder of
//!   the literal's own length (`?`, or `:ppp`) so every span keeps its byte
//!   offsets, and the literals' kinds. The shape is parsed once into a template; each
//!   statement of that shape is the template with its own literals bound
//!   into the placeholders, spans included — exactly what parsing its text
//!   gives. That is checked, against a real parse, the first time a shape
//!   is seen; a shape that fails the check (a literal where a placeholder
//!   can't go, say `varchar(10)`) is remembered as not parameterizable and
//!   its statements use the exact-text cache. Statements with placeholders
//!   of their own are never shaped.
//!
//! Only successful parses are kept, only texts up to MAX_CACHED_LEN (a bulk
//! INSERT is rarely repeated and would crowd out everything else), and at
//! most CAPACITY entries per level, the least recently used going first.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use crate::bind::bind_literals;
use crate::lexer::tokenize;
use crate::literal::{Literal, NumberLiteral, NumberValue, StringLiteral};
use crate::token::{Punctuation, StringStyle, Token, TokenStruct};
use crate::{ParseError, Statement, parse_sql};

const CAPACITY: usize = 512;
const MAX_CACHED_LEN: usize = 8 * 1024;

// A bounded least-recently-used map from text to V.
struct Lru<V> {
    entries: HashMap<String, (V, u64)>,
    // Advances on every use; `by_use` finds the least recently used
    // without a scan.
    clock: u64,
    by_use: BTreeMap<u64, String>,
}

impl<V: Clone> Lru<V> {
    fn new() -> Self {
        Lru {
            entries: HashMap::new(),
            clock: 0,
            by_use: BTreeMap::new(),
        }
    }

    fn get(&mut self, key: &str) -> Option<V> {
        self.clock += 1;
        let now = self.clock;
        let (value, used) = self.entries.get_mut(key)?;
        let before = std::mem::replace(used, now);
        let value = value.clone();
        if let Some(k) = self.by_use.remove(&before) {
            self.by_use.insert(now, k);
        }
        Some(value)
    }

    fn insert(&mut self, key: String, value: V) {
        if self.entries.len() >= CAPACITY
            && !self.entries.contains_key(&key)
            && let Some((_, oldest)) = self.by_use.pop_first()
        {
            self.entries.remove(&oldest);
        }
        self.clock += 1;
        let used = self.clock;
        if let Some((_, old)) = self.entries.insert(key.clone(), (value, used)) {
            self.by_use.remove(&old);
        }
        self.by_use.insert(used, key);
    }
}

// What a shape's entry holds.
#[derive(Clone)]
enum Shape {
    Template(Arc<[Statement]>),
    NotParameterizable,
}

struct Cache {
    exact: Lru<Arc<[Statement]>>,
    shapes: Lru<Shape>,
    stats: CacheStats,
}

/// How the cache has done so far (process-wide).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Texts seen before.
    pub hits: u64,
    /// New texts of a shape seen before, bound from its template.
    pub shape_hits: u64,
    /// Statements parsed.
    pub misses: u64,
    pub entries: usize,
}

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(Cache {
            exact: Lru::new(),
            shapes: Lru::new(),
            stats: CacheStats::default(),
        })
    })
}

fn lock() -> std::sync::MutexGuard<'static, Cache> {
    cache().lock().unwrap_or_else(|e| e.into_inner())
}

/// As [`parse_sql`], reusing an earlier parse of the same text, or of a
/// text differing only in its literals.
pub fn parse_sql_cached(src: &str) -> Result<Arc<[Statement]>, Vec<ParseError>> {
    if src.len() > MAX_CACHED_LEN {
        return parse_sql(src).map(Arc::from);
    }
    {
        let mut c = lock();
        if let Some(statements) = c.exact.get(src) {
            c.stats.hits += 1;
            return Ok(statements);
        }
    }
    if let Some(statements) = from_shape(src) {
        return Ok(statements);
    }
    exact_parse(src)
}

// Parses `src` and keeps it by its text.
fn exact_parse(src: &str) -> Result<Arc<[Statement]>, Vec<ParseError>> {
    // Parsed outside the lock: two threads parsing the same new text both
    // do the work, and the second insert just replaces the first.
    let statements: Arc<[Statement]> = parse_sql(src)?.into();
    let mut c = lock();
    c.stats.misses += 1;
    c.exact.insert(src.to_string(), statements.clone());
    Ok(statements)
}

// `src` through its shape's template, or None when it has no usable shape
// (no literals, placeholders of its own, a shape that isn't
// parameterizable, or one that fails to parse).
fn from_shape(src: &str) -> Option<Arc<[Statement]>> {
    let tokens = tokenize(src).ok()?;
    let (key, normalized, literals) = shape_of(src, &tokens)?;
    let shape = lock().shapes.get(&key);
    match shape {
        Some(Shape::NotParameterizable) => None,
        Some(Shape::Template(template)) => {
            let mut statements = template.to_vec();
            if !bind_literals(&mut statements, literals) {
                return None;
            }
            if verify() {
                let parsed = parse_sql(src).ok();
                assert_eq!(
                    parsed.as_deref(),
                    Some(&statements[..]),
                    "parse cache: binding {src:?} into its shape differs from parsing it"
                );
            }
            // Kept by its text as well: a statement repeated verbatim is
            // then a lookup, not a bind.
            let statements: Arc<[Statement]> = statements.into();
            let mut c = lock();
            c.stats.shape_hits += 1;
            c.exact.insert(src.to_string(), statements.clone());
            Some(statements)
        }
        None => {
            // A new shape: parse it and this text, and keep the template
            // only if binding this text's literals into it gives exactly
            // this text's parse.
            let parsed = parse_sql(src).ok()?;
            let template = parse_sql(&normalized).ok();
            let fits = template.as_ref().is_some_and(|t| {
                let mut bound = t.clone();
                bind_literals(&mut bound, literals) && bound == parsed
            });
            let parsed: Arc<[Statement]> = parsed.into();
            let mut c = lock();
            c.stats.misses += 1;
            let shape = match template {
                Some(t) if fits => Shape::Template(t.into()),
                _ => Shape::NotParameterizable,
            };
            c.shapes.insert(key, shape);
            c.exact.insert(src.to_string(), parsed.clone());
            Some(parsed)
        }
    }
}

// The shape of `src`: its key, the text to parse as its template, and the
// literals to bind into that, in order. None when it has no literals, has
// placeholders of its own, or has a literal its parse wouldn't accept.
fn shape_of(src: &str, tokens: &[TokenStruct]) -> Option<(String, String, Vec<Literal>)> {
    let mut normalized = String::with_capacity(src.len());
    let mut kinds = String::new();
    let mut literals = vec![];
    let mut at = 0;
    for t in tokens {
        let literal = match &t.token {
            Token::Punctuation(
                Punctuation::QuestionMark | Punctuation::Dollar | Punctuation::Colon,
            ) => return None,
            Token::String {
                raw,
                kind: StringStyle::SingleQuoted(_),
            } => {
                kinds.push('s');
                Literal::String(StringLiteral {
                    span: t.span,
                    value: raw.replace("''", "'"),
                })
            }
            Token::Number { raw } => {
                kinds.push('n');
                let value = match raw.parse::<i64>() {
                    Ok(i) => NumberValue::Integer(i),
                    Err(_) => NumberValue::Float(raw.parse::<f64>().ok()?),
                };
                Literal::Number(NumberLiteral {
                    span: t.span,
                    raw: (*raw).to_string(),
                    value,
                })
            }
            _ => continue,
        };
        normalized.push_str(&src[at..t.span.start]);
        // A placeholder token exactly as long as the literal, so that the
        // spans of everything around it (`id = ...` ends where the literal
        // does) match: `?` for one byte, else a named one, `:ppp`.
        match t.span.end - t.span.start {
            1 => normalized.push('?'),
            n => {
                normalized.push(':');
                normalized.extend(std::iter::repeat_n('p', n - 1));
            }
        }
        at = t.span.end;
        literals.push(literal);
    }
    if literals.is_empty() {
        return None;
    }
    normalized.push_str(&src[at..]);
    let key = format!("{kinds}\u{0}{normalized}");
    Some((key, normalized, literals))
}

// SQ_PARSE_CACHE_VERIFY=1: check every statement bound from a shape
// against a real parse of its text (for running a test suite as a corpus).
fn verify() -> bool {
    static VERIFY: OnceLock<bool> = OnceLock::new();
    *VERIFY.get_or_init(|| std::env::var_os("SQ_PARSE_CACHE_VERIFY").is_some())
}

pub fn cache_stats() -> CacheStats {
    let c = lock();
    CacheStats {
        entries: c.exact.entries.len() + c.shapes.entries.len(),
        ..c.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cache is process-wide and tests run in parallel: each test uses
    // texts of its own, and checks sharing by pointer, not by counts.

    #[test]
    fn test_the_same_text_shares_one_parse() {
        let sql = "select cache_test_one from t";
        let a = parse_sql_cached(sql).unwrap();
        let b = parse_sql_cached(sql).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let c = parse_sql_cached("select cache_test_two from t").unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(*a, *parse_sql(sql).unwrap());
    }

    #[test]
    fn test_texts_differing_in_literals_are_bound_from_one_shape() {
        let texts = [
            "select * from shape_t where id = 'ORD0000001' and n > 5",
            "select * from shape_t where id = 'ORD0000777' and n > 9",
            "select * from shape_t where id = 'it''s here' and n > 1.5",
            "select * from shape_t where id = 'ORD1' and n > 123456",
        ];
        for t in texts {
            // Twice: the first may make the shape, the second uses it.
            for _ in 0..2 {
                assert_eq!(*parse_sql_cached(t).unwrap(), *parse_sql(t).unwrap(), "{t}");
            }
        }
        let before = cache_stats().shape_hits;
        let t = "select * from shape_t where id = 'ORD0000042' and n > 7";
        assert_eq!(*parse_sql_cached(t).unwrap(), *parse_sql(t).unwrap());
        assert!(
            cache_stats().shape_hits > before,
            "a new text of a known shape is bound, not parsed"
        );
    }

    #[test]
    fn test_literals_a_placeholder_cannot_replace_keep_their_exact_parse() {
        for t in [
            "create table shape_c (a varchar(10), b integer, primary key(a))",
            "insert into shape_c values ('x', 1), ('y', -2)",
            "select a from shape_c order by 1 limit 3",
            "select 'lit' as shape_l, 2 + 3 * 4 from shape_c",
            "select * from shape_c where a = ?",
            "select * from shape_c where a = $1 and b = :b",
        ] {
            for _ in 0..2 {
                match (parse_sql_cached(t), parse_sql(t)) {
                    (Ok(a), Ok(b)) => assert_eq!(*a, *b, "{t}"),
                    (Err(_), Err(_)) => {}
                    (a, b) => panic!("{t}: cached {a:?} vs parsed {b:?}"),
                }
            }
        }
    }

    #[test]
    fn test_errors_and_long_texts_are_not_kept() {
        assert!(parse_sql_cached("select from cache_test_bad").is_err());
        assert!(parse_sql_cached("select from cache_test_bad where x = 'y'").is_err());
        assert!(parse_sql_cached("select from cache_test_bad where x = 'y'").is_err());
        let long = format!("select '{}' as cache_test_long", "x".repeat(MAX_CACHED_LEN));
        let a = parse_sql_cached(&long).unwrap();
        let b = parse_sql_cached(&long).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(a, b);
    }

    #[test]
    fn test_the_cache_stays_bounded() {
        let first = parse_sql_cached("select cache_test_bound_0 from t").unwrap();
        for i in 1..=CAPACITY + 10 {
            parse_sql_cached(&format!("select cache_test_bound_{i} from t")).unwrap();
        }
        assert!(lock().exact.entries.len() <= CAPACITY);
        // The oldest went first.
        let again = parse_sql_cached("select cache_test_bound_0 from t").unwrap();
        assert!(!Arc::ptr_eq(&first, &again));
    }
}

#[cfg(test)]
mod bench {
    use super::*;

    // Where a shape-cache hit's time goes. By hand, in release:
    //   cargo test --release -p sql-parser --lib cache::bench -- --ignored --nocapture
    #[test]
    #[ignore]
    fn shape_hit_parts() {
        let texts: Vec<String> = (0..40_000)
            .map(|i| format!("select * from orders where order_id = 'ORD{i:07}'"))
            .collect();
        parse_sql_cached(&texts[0]).unwrap();
        parse_sql_cached(&texts[1]).unwrap();
        let time = |what: &str, f: &mut dyn FnMut(&str)| {
            let start = std::time::Instant::now();
            for t in &texts[2..] {
                f(t);
            }
            let us = start.elapsed().as_secs_f64() * 1e6 / (texts.len() - 2) as f64;
            println!("{what:<30} {us:>7.2} us");
        };
        time("tokenize", &mut |t| {
            tokenize(t).unwrap();
        });
        time("tokenize + shape_of", &mut |t| {
            let tokens = tokenize(t).unwrap();
            shape_of(t, &tokens).unwrap();
        });
        let key = {
            let tokens = tokenize(&texts[0]).unwrap();
            shape_of(&texts[0], &tokens).unwrap().0
        };
        let Some(Shape::Template(template)) = lock().shapes.get(&key) else {
            panic!("no template");
        };
        time("template clone", &mut |_| {
            let _ = template.to_vec();
        });
        time("clone + bind", &mut |t| {
            let tokens = tokenize(t).unwrap();
            let (_, _, literals) = shape_of(t, &tokens).unwrap();
            let mut s = template.to_vec();
            bind_literals(&mut s, literals);
        });
        time("parse_sql_cached (new text)", &mut |t| {
            parse_sql_cached(t).unwrap();
        });
        time("parse_sql_cached (repeat)", &mut |t| {
            parse_sql_cached(t).unwrap();
        });
    }
}
