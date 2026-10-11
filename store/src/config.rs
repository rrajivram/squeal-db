//! What a database is created and opened with.
//!
//! Two kinds of setting, kept apart by when they can be chosen:
//!
//! - [`CreateConfig`]: fixed when the database is created and written into
//!   its file — the page size and the largest index key. Every later open
//!   reads them from the file; they are not asked for again.
//! - [`OpenConfig`]: how this process runs the database, chosen each time it
//!   is opened (and when it is created, which opens it too): how much
//!   memory, how long to wait, when to checkpoint. Nothing here is
//!   written to the file or changes what is in it, so the same database
//!   can be opened with a small cache on one machine and a large one on
//!   another.
//!
//! ```
//! use store::config::{CreateConfig, OpenConfig};
//!
//! let open = OpenConfig::default().page_cache_bytes(256 << 20);
//! let create = CreateConfig::default().page_size(8 * 1024).open(open);
//! assert!(create.validate().is_ok());
//! ```

use std::time::Duration;

use crate::{
    db::{DBSizeType, SnapshotLimits},
    error::StoreError,
};

pub const DEFAULT_PAGE_SIZE: DBSizeType = 16 * 1024;
pub const MIN_PAGE_SIZE: DBSizeType = 4 * 1024;
pub const MAX_PAGE_SIZE: DBSizeType = 1024 * 1024;
pub const DEFAULT_MAX_INDEX_KEY_SIZE: DBSizeType = 512;
pub const MIN_MAX_INDEX_KEY_SIZE: DBSizeType = 64;
pub const MAX_MAX_INDEX_KEY_SIZE: DBSizeType = 8 * 1024;

pub const DEFAULT_PAGE_CACHE_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_TEMP_CACHE_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
pub const DEFAULT_CHECKPOINT_LOG_BYTES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(10);
pub const DEFAULT_MAX_RETAINED_WAL_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAX_VERSION_RECORDS: usize = 1_000_000;

/// How many index entries of a table's largest size a page must hold. A
/// full page splits at its midpoint, and each half must then take another
/// entry: with fewer than four to a page that can't be promised, and the
/// tree fails an insert ("No space in page") after two or three rows.
pub const MIN_ENTRIES_PER_PAGE: u64 = 4;

/// What an index entry takes beyond its key's own bytes, at most: the
/// key's framing, the transaction fields, the pointer to its row, and the
/// allowance layers above add when they size a table's entries (64 in
/// squeal-sql and sq-json). A database's pages must hold
/// MIN_ENTRIES_PER_PAGE entries of `max_index_key_size` plus this.
pub const INDEX_ENTRY_FRAMING: DBSizeType = 96;

/// The largest index entry (a key with its framing — a table's
/// `index_entry_size`) a database of this page size can hold
/// MIN_ENTRIES_PER_PAGE of in a page: what is left of a page after its
/// header (which reserves `max_index_key_size` for a high key) and the
/// slot directory, in four. 0 when a page has no room at all.
pub fn max_index_entry_size(page_size: DBSizeType, max_index_key_size: DBSizeType) -> DBSizeType {
    // The slotted page's own header, and a slot per entry.
    const PAGE_CONTENT_HEADER: DBSizeType = 8;
    const SLOT: DBSizeType = crate::pages::slotted::SLOT_ENTRY_BYTES as DBSizeType;
    let usable = page_size
        .saturating_sub(crate::page::page_overhead(max_index_key_size) as DBSizeType)
        .saturating_sub(crate::page::USABLE_DATA_MARGIN)
        .saturating_sub(PAGE_CONTENT_HEADER);
    (usable / MIN_ENTRIES_PER_PAGE).saturating_sub(SLOT)
}

// The page cache holds at least this many pages, whatever is asked for: a
// descent holds a page per level, a split three more, and a cache that
// can't hold them makes no progress.
const MIN_CACHE_PAGES: usize = 16;

/// How a process runs a database it opens. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenConfig {
    /// The page cache's size: how much of the database stays in memory.
    /// Rounded down to whole pages. Default 128 MiB.
    pub page_cache_bytes: u64,
    /// Memory for query scratch pages (sort runs, hash-join tables, temp
    /// tables) before they spill to `<db>.tmp`. Default 64 MiB.
    pub temp_cache_bytes: u64,
    /// What one query may hold in memory before its operators spill to
    /// scratch pages. The store keeps and reports it (`Db::config`); the
    /// layer that runs queries enforces it. Default 64 MiB.
    pub query_memory_bytes: u64,
    /// How long a page lock is waited for before `LockTimeout`. A
    /// legitimate hold is microseconds: this is a bug detector, not a knob
    /// for contention. Default 1 s.
    pub lock_timeout: Duration,
    /// A checkpoint runs once the current WAL segment is this big.
    /// Default 16 MiB.
    pub checkpoint_log_bytes: u64,
    /// A checkpoint also runs once this many pages are dirty (they stay in
    /// memory until one does). None: half the page cache.
    pub checkpoint_dirty_pages: Option<usize>,
    /// What a long-lived transaction may pin before it is aborted with
    /// `SnapshotTooOld`.
    pub snapshot_limits: SnapshotLimits,
    /// How often the maintenance thread runs when nothing wakes it.
    /// Default 10 ms.
    pub maintenance_interval: Duration,
}

impl Default for OpenConfig {
    fn default() -> Self {
        Self {
            page_cache_bytes: DEFAULT_PAGE_CACHE_BYTES,
            temp_cache_bytes: DEFAULT_TEMP_CACHE_BYTES,
            query_memory_bytes: DEFAULT_QUERY_MEMORY_BYTES,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
            checkpoint_log_bytes: DEFAULT_CHECKPOINT_LOG_BYTES,
            checkpoint_dirty_pages: None,
            snapshot_limits: SnapshotLimits {
                max_retained_wal_bytes: DEFAULT_MAX_RETAINED_WAL_BYTES,
                max_version_records: DEFAULT_MAX_VERSION_RECORDS,
            },
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        }
    }
}

impl OpenConfig {
    pub fn page_cache_bytes(mut self, bytes: u64) -> Self {
        self.page_cache_bytes = bytes;
        self
    }

    pub fn temp_cache_bytes(mut self, bytes: u64) -> Self {
        self.temp_cache_bytes = bytes;
        self
    }

    pub fn query_memory_bytes(mut self, bytes: u64) -> Self {
        self.query_memory_bytes = bytes;
        self
    }

    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    pub fn checkpoint_log_bytes(mut self, bytes: u64) -> Self {
        self.checkpoint_log_bytes = bytes;
        self
    }

    pub fn checkpoint_dirty_pages(mut self, pages: usize) -> Self {
        self.checkpoint_dirty_pages = Some(pages);
        self
    }

    pub fn snapshot_limits(mut self, limits: SnapshotLimits) -> Self {
        self.snapshot_limits = limits;
        self
    }

    pub fn maintenance_interval(mut self, interval: Duration) -> Self {
        self.maintenance_interval = interval;
        self
    }

    /// Refuses settings no database could run with.
    pub fn validate(&self) -> Result<(), StoreError> {
        let bad = |what: &str| Err(StoreError::InvalidConfig(what.to_string()));
        if self.page_cache_bytes == 0 {
            return bad("page_cache_bytes must be more than 0");
        }
        if self.query_memory_bytes == 0 {
            return bad("query_memory_bytes must be more than 0");
        }
        if self.lock_timeout.is_zero() {
            return bad("lock_timeout must be more than 0");
        }
        if self.checkpoint_log_bytes == 0 {
            return bad("checkpoint_log_bytes must be more than 0");
        }
        if self.checkpoint_dirty_pages == Some(0) {
            return bad("checkpoint_dirty_pages must be more than 0");
        }
        if self.maintenance_interval.is_zero() {
            return bad("maintenance_interval must be more than 0");
        }
        Ok(())
    }

    /// The page cache in pages of `page_size` — at least a few (see
    /// MIN_CACHE_PAGES), however little was asked for.
    pub fn cache_pages(&self, page_size: DBSizeType) -> usize {
        ((self.page_cache_bytes / page_size.max(1)) as usize).max(MIN_CACHE_PAGES)
    }

    /// The dirty-page count that triggers a checkpoint, for a cache of
    /// `cache_pages`: as set, or half the cache — never more than the
    /// cache holds, or the cache would fill with pages it may not evict.
    pub fn dirty_page_limit(&self, cache_pages: usize) -> usize {
        self.checkpoint_dirty_pages
            .unwrap_or(cache_pages / 2)
            .clamp(1, cache_pages.max(1))
    }
}

/// What a database is created with: what is fixed in its file, and how
/// the creating process runs it. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConfig {
    /// Bytes a page: 4 KiB to 1 MiB. Default 16 KiB.
    pub page_size: DBSizeType,
    /// The largest index key, in bytes: 64 to 8 KiB, and small enough that
    /// a page holds four entries of it (see max_index_entry_size).
    /// Default 512.
    pub max_index_key_size: DBSizeType,
    /// How the database runs once created.
    pub open: OpenConfig,
}

impl Default for CreateConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            max_index_key_size: DEFAULT_MAX_INDEX_KEY_SIZE,
            open: OpenConfig::default(),
        }
    }
}

impl CreateConfig {
    pub fn page_size(mut self, bytes: DBSizeType) -> Self {
        self.page_size = bytes;
        self
    }

    pub fn max_index_key_size(mut self, bytes: DBSizeType) -> Self {
        self.max_index_key_size = bytes;
        self
    }

    pub fn open(mut self, open: OpenConfig) -> Self {
        self.open = open;
        self
    }

    /// Refuses what OpenConfig::validate does. The page and key sizes are
    /// checked when the header they go into is (see Db::create_with).
    pub fn validate(&self) -> Result<(), StoreError> {
        self.open.validate()
    }
}

impl From<OpenConfig> for CreateConfig {
    fn from(open: OpenConfig) -> Self {
        Self::default().open(open)
    }
}

// ---- settings by name ----------------------------------------------------
//
// One text form for every front end (a command line's `--page-cache-bytes
// 256m`, a JSON options object's `pageCacheBytes`): a setting's name in
// any of snake_case, kebab-case or camelCase, and its value as text.

/// The settings `OpenConfig::set` takes: name, what a value looks like,
/// what it is.
pub const OPEN_SETTINGS: &[(&str, &str, &str)] = &[
    ("page_cache_bytes", "size", "page cache: how much of the database stays in memory (128m)"),
    ("temp_cache_bytes", "size", "memory for query scratch pages before they spill to disk (64m)"),
    ("query_memory_bytes", "size", "what one query may hold in memory before it spills (64m)"),
    ("lock_timeout", "duration", "how long a page lock is waited for (1s)"),
    ("checkpoint_log_bytes", "size", "WAL growth that triggers a checkpoint (16m)"),
    ("checkpoint_dirty_pages", "count", "dirty pages that trigger a checkpoint (half the page cache)"),
    ("max_retained_wal_bytes", "size", "WAL a long transaction may pin before it is aborted (256m)"),
    ("max_version_records", "count", "row versions a long transaction may pin before it is aborted (1000000)"),
    ("maintenance_interval", "duration", "how often background maintenance runs when idle (10ms)"),
];

/// The settings only `CreateConfig::set` takes — fixed in the file when
/// the database is created.
pub const CREATE_SETTINGS: &[(&str, &str, &str)] = &[
    ("page_size", "size", "bytes a page, a power of two from 4k to 1m (16k)"),
    ("max_index_key_size", "size", "largest index key, a power of two from 64 to 8k (512)"),
];

// `pageCacheBytes`, `page-cache-bytes`, `PAGE_CACHE_BYTES` -> `page_cache_bytes`.
fn normalize(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, ch) in name.trim().trim_start_matches('-').chars().enumerate() {
        if ch == '-' || ch == ' ' {
            out.push('_');
        } else if ch.is_ascii_uppercase() && name.chars().any(|c| c.is_ascii_lowercase()) {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}

fn bad(what: String) -> StoreError {
    StoreError::InvalidConfig(what)
}

/// A size in bytes: a number, optionally with k, m or g (powers of 1024;
/// `kb`, `kib` and the like read the same): `8192`, `16k`, `256m`, `1g`.
pub fn parse_bytes(text: &str) -> Result<u64, StoreError> {
    let t = text.trim().to_ascii_lowercase();
    let digits = t.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = t[digits.len()..].trim_end_matches("ib").trim_end_matches('b');
    let shift = match unit {
        "" => 0,
        "k" => 10,
        "m" => 20,
        "g" => 30,
        _ => return Err(bad(format!("{text:?} is not a size (a number, or one with k, m or g)"))),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(1 << shift))
        .ok_or_else(|| bad(format!("{text:?} is not a size (a number, or one with k, m or g)")))
}

/// A duration: a number with us, ms, s or m, or a bare number of
/// milliseconds: `250ms`, `2s`, `500`.
pub fn parse_duration(text: &str) -> Result<Duration, StoreError> {
    let t = text.trim().to_ascii_lowercase();
    let digits = t.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let micros: u64 = match &t[digits.len()..] {
        "us" => 1,
        "" | "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        _ => return Err(bad(format!("{text:?} is not a duration (a number with us, ms, s or m)"))),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(micros))
        .map(Duration::from_micros)
        .ok_or_else(|| bad(format!("{text:?} is not a duration (a number with us, ms, s or m)")))
}

fn parse_count(text: &str) -> Result<usize, StoreError> {
    text.trim()
        .replace('_', "")
        .parse()
        .map_err(|_| bad(format!("{text:?} is not a count")))
}

impl OpenConfig {
    /// Sets one setting by name (see OPEN_SETTINGS) from its value as
    /// text. A name that isn't one is refused, naming those that are.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), StoreError> {
        let named = |e: StoreError| match e {
            StoreError::InvalidConfig(m) => bad(format!("{name}: {m}")),
            e => e,
        };
        match normalize(name).as_str() {
            "page_cache_bytes" => self.page_cache_bytes = parse_bytes(value).map_err(named)?,
            "temp_cache_bytes" => self.temp_cache_bytes = parse_bytes(value).map_err(named)?,
            "query_memory_bytes" => self.query_memory_bytes = parse_bytes(value).map_err(named)?,
            "lock_timeout" => self.lock_timeout = parse_duration(value).map_err(named)?,
            "checkpoint_log_bytes" => {
                self.checkpoint_log_bytes = parse_bytes(value).map_err(named)?
            }
            "checkpoint_dirty_pages" => {
                self.checkpoint_dirty_pages = Some(parse_count(value).map_err(named)?)
            }
            "max_retained_wal_bytes" => {
                self.snapshot_limits.max_retained_wal_bytes = parse_bytes(value).map_err(named)?
            }
            "max_version_records" => {
                self.snapshot_limits.max_version_records = parse_count(value).map_err(named)?
            }
            "maintenance_interval" => {
                self.maintenance_interval = parse_duration(value).map_err(named)?
            }
            other if CREATE_SETTINGS.iter().any(|(n, _, _)| *n == other) => {
                return Err(bad(format!(
                    "{other} is fixed when a database is created; it can't be set when opening one"
                )));
            }
            _ => {
                let known: Vec<&str> = OPEN_SETTINGS.iter().map(|(n, _, _)| *n).collect();
                return Err(bad(format!(
                    "unknown setting {name:?}; the settings are {}",
                    known.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// `set` for each of `settings`, then `validate`.
    pub fn from_settings<'a>(
        settings: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, StoreError> {
        let mut config = Self::default();
        for (name, value) in settings {
            config.set(name, value)?;
        }
        config.validate()?;
        Ok(config)
    }
}

impl CreateConfig {
    /// Sets one setting by name — one of CREATE_SETTINGS, or any of
    /// OPEN_SETTINGS (for how the database runs once created).
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), StoreError> {
        match normalize(name).as_str() {
            "page_size" => {
                self.page_size =
                    parse_bytes(value).map_err(|e| bad(format!("{name}: {e}")))?
            }
            "max_index_key_size" => {
                self.max_index_key_size =
                    parse_bytes(value).map_err(|e| bad(format!("{name}: {e}")))?
            }
            _ => {
                return self.open.set(name, value).map_err(|e| match e {
                    StoreError::InvalidConfig(m) if m.starts_with("unknown setting") => {
                        let known: Vec<&str> = CREATE_SETTINGS
                            .iter()
                            .chain(OPEN_SETTINGS)
                            .map(|(n, _, _)| *n)
                            .collect();
                        bad(format!(
                            "unknown setting {name:?}; the settings are {}",
                            known.join(", ")
                        ))
                    }
                    e => e,
                });
            }
        }
        Ok(())
    }

    /// `set` for each of `settings`, then `validate`.
    pub fn from_settings<'a>(
        settings: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, StoreError> {
        let mut config = Self::default();
        for (name, value) in settings {
            config.set(name, value)?;
        }
        config.validate()?;
        Ok(config)
    }
}

/// Splits a command line's arguments into its positional ones and its
/// settings: `--name value` or `--name=value`. (What each program does
/// with a flag that is not a setting is its own business: it finds it
/// among the settings and takes it out.)
pub fn split_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(Vec<String>, Vec<(String, String)>), StoreError> {
    let mut positional = vec![];
    let mut settings = vec![];
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let Some(flag) = arg.strip_prefix("--") else {
            positional.push(arg);
            continue;
        };
        match flag.split_once('=') {
            // The one flag with no value: for the program to answer.
            None if flag == "help" => settings.push((flag.to_string(), String::new())),
            Some((name, value)) => settings.push((name.to_string(), value.to_string())),
            None => {
                let value = args
                    .next()
                    .ok_or_else(|| bad(format!("--{flag} needs a value")))?;
                settings.push((flag.to_string(), value));
            }
        }
    }
    Ok((positional, settings))
}

/// The settings as lines of help, for a command line: those a database is
/// created with (`create`), or only those it is opened with.
pub fn settings_help(create: bool) -> String {
    let mut out = String::new();
    let mut section = |title: &str, settings: &[(&str, &str, &str)]| {
        out.push_str(title);
        out.push('\n');
        for (name, kind, what) in settings {
            let flag = format!("--{} <{kind}>", name.replace('_', "-"));
            out.push_str(&format!("  {flag:<36} {what}\n"));
        }
    };
    if create {
        section("when creating a database (fixed in its file):", CREATE_SETTINGS);
    }
    section("each time a database is opened:", OPEN_SETTINGS);
    out.push_str("sizes take k, m or g (16k, 256m); durations take us, ms, s or m (250ms)\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_are_valid_and_what_they_were() {
        let c = CreateConfig::default();
        assert!(c.validate().is_ok());
        assert_eq!(c.page_size, 16 * 1024);
        assert_eq!(c.open.cache_pages(c.page_size), 8192);
        assert_eq!(c.open.dirty_page_limit(8192), 4096);
    }

    #[test]
    fn test_zero_settings_are_refused() {
        for bad in [
            OpenConfig::default().page_cache_bytes(0),
            OpenConfig::default().query_memory_bytes(0),
            OpenConfig::default().lock_timeout(Duration::ZERO),
            OpenConfig::default().checkpoint_log_bytes(0),
            OpenConfig::default().checkpoint_dirty_pages(0),
            OpenConfig::default().maintenance_interval(Duration::ZERO),
        ] {
            assert!(matches!(bad.validate(), Err(StoreError::InvalidConfig(_))), "{bad:?}");
        }
        // No scratch memory is a choice: everything spills.
        assert!(OpenConfig::default().temp_cache_bytes(0).validate().is_ok());
    }

    #[test]
    fn test_a_tiny_cache_still_holds_a_descent() {
        let c = OpenConfig::default().page_cache_bytes(1);
        assert_eq!(c.cache_pages(16 * 1024), MIN_CACHE_PAGES);
        // The dirty limit never exceeds the cache.
        assert_eq!(c.clone().checkpoint_dirty_pages(1000).dirty_page_limit(16), 16);
        assert_eq!(c.dirty_page_limit(16), 8);
    }

    #[test]
    fn test_settings_by_name_in_any_spelling() {
        let mut c = CreateConfig::default();
        for (name, value) in [
            ("page_size", "8k"),
            ("maxIndexKeySize", "256"),
            ("--page-cache-bytes", "256m"),
            ("TEMP_CACHE_BYTES", "1GiB"),
            ("queryMemoryBytes", "2mb"),
            ("lock-timeout", "250ms"),
            ("checkpoint_log_bytes", "4194304"),
            ("checkpoint_dirty_pages", "1_000"),
            ("max_retained_wal_bytes", "64m"),
            ("max_version_records", "5000"),
            ("maintenance_interval", "2s"),
        ] {
            c.set(name, value).unwrap_or_else(|e| panic!("{name}={value}: {e}"));
        }
        assert_eq!(c.page_size, 8 * 1024);
        assert_eq!(c.max_index_key_size, 256);
        assert_eq!(c.open.page_cache_bytes, 256 << 20);
        assert_eq!(c.open.temp_cache_bytes, 1 << 30);
        assert_eq!(c.open.query_memory_bytes, 2 << 20);
        assert_eq!(c.open.lock_timeout, Duration::from_millis(250));
        assert_eq!(c.open.checkpoint_log_bytes, 4 << 20);
        assert_eq!(c.open.checkpoint_dirty_pages, Some(1000));
        assert_eq!(c.open.snapshot_limits.max_retained_wal_bytes, 64 << 20);
        assert_eq!(c.open.snapshot_limits.max_version_records, 5000);
        assert_eq!(c.open.maintenance_interval, Duration::from_secs(2));
        // Every documented setting is one `set` takes.
        for (name, kind, _) in CREATE_SETTINGS.iter().chain(OPEN_SETTINGS) {
            let value = if *kind == "duration" { "1s" } else { "4096" };
            CreateConfig::default()
                .set(name, value)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn test_bad_settings_say_what_is_wrong() {
        let msg = |r: Result<(), StoreError>| match r {
            Err(StoreError::InvalidConfig(m)) => m,
            other => panic!("{other:?}"),
        };
        let mut open = OpenConfig::default();
        assert!(msg(open.set("page_size", "8k")).contains("fixed when a database is created"));
        assert!(msg(open.set("pagecache", "8k")).contains("page_cache_bytes"));
        assert!(msg(open.set("page_cache_bytes", "lots")).contains("not a size"));
        assert!(msg(open.set("lock_timeout", "5 fortnights")).contains("not a duration"));
        assert!(msg(open.set("max_version_records", "-1")).contains("not a count"));
        let mut create = CreateConfig::default();
        assert!(msg(create.set("nope", "1")).contains("page_size"));
        assert!(matches!(
            OpenConfig::from_settings([("page_cache_bytes", "0")]),
            Err(StoreError::InvalidConfig(_))
        ));
        assert_eq!(
            OpenConfig::from_settings([("query_memory_bytes", "1m")]).unwrap(),
            OpenConfig::default().query_memory_bytes(1 << 20)
        );
    }
}
