// This crate has exactly one temporal type — DataType::Datetime /
// ValueItem::Datetime(u64) — with no separate DATE or TIME. This module
// is what makes that one type usable with the date/time literal forms
// real data actually shows up in ("2020-04-13", "12:53:24", or the two
// combined), rather than only accepting a bare pre-computed number
// (which is all `expr_to_value_item`/CSV loading could accept before
// this existed).
//
// A pure date parses to Unix epoch seconds at midnight UTC that day; a
// pure time parses to seconds since midnight (not anchored to any
// date); a combined "date time" or "dateTtime" parses to the sum of
// both. A pure time value is always < 86400, and any real date on or
// after 1970-01-02 is not — that's the only thing distinguishing the
// two when a `Datetime` column is read back, which is enough for now
// without needing this crate to grow separate DATE/TIME types.

// Days since the Unix epoch (1970-01-01) for a proleptic Gregorian
// calendar date — Howard Hinnant's days_from_civil algorithm
// (http://howardhinnant.github.io/date_algorithms.html), correct for
// any year (including the leap-year 4/100/400 rule) without needing a
// calendar library. Verified against Python's datetime in this crate's
// own tests below.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (i64::from(m) + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

fn parse_date(s: &str) -> Option<u64> {
    let mut parts = s.splitn(4, '-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let days = days_from_civil(y, m, d);
    u64::try_from(days.checked_mul(86400)?).ok()
}

fn parse_time(s: &str) -> Option<u64> {
    let mut parts = s.splitn(4, ':');
    let h: u64 = parts.next()?.parse().ok()?;
    let m: u64 = parts.next()?.parse().ok()?;
    let sec: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || h >= 24 || m >= 60 || sec >= 60 {
        return None;
    }
    Some(h * 3600 + m * 60 + sec)
}

/// Parses "YYYY-MM-DD", "HH:MM:SS", "YYYY-MM-DD HH:MM:SS", or
/// "YYYY-MM-DDTHH:MM:SS" into the u64 DataType::Datetime/
/// ValueItem::Datetime itself uses — see this module's own doc comment
/// for the encoding. None for anything else (including a bare number,
/// which callers should already accept as a literal Datetime value in
/// its own right, without going through this parser at all).
pub(crate) fn parse_datetime(s: &str) -> Option<u64> {
    let (date_part, time_part) = match s.split_once(['T', ' ']) {
        Some((d, t)) => (Some(d), Some(t)),
        None if s.contains(':') => (None, Some(s)),
        None => (Some(s), None),
    };
    // Not `.map(parse_date).flatten()`: that would collapse "no date
    // part at all" and "a date part that failed to parse" into the
    // same None, silently accepting the time-only half of an input
    // whose date half was actually garbage. `?` inside the `Some` arm
    // propagates a real parse failure out of parse_datetime entirely,
    // instead of masking it.
    let date_secs = match date_part {
        Some(s) => Some(parse_date(s)?),
        None => None,
    };
    let time_secs = match time_part {
        Some(s) => Some(parse_time(s)?),
        None => None,
    };
    match (date_secs, time_secs) {
        (Some(d), Some(t)) => Some(d + t),
        (Some(d), None) => Some(d),
        (None, Some(t)) => Some(t),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Cross-checked against Python's datetime.date(...) - datetime.date(1970,1,1)):
    //   date(1970,1,1)  -> 0
    //   date(2020,4,13) -> 18365
    //   date(2024,4,21) -> 19834
    #[test]
    fn test_days_from_civil_matches_known_reference_values() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2020, 4, 13), 18365);
        assert_eq!(days_from_civil(2024, 4, 21), 19834);
    }

    #[test]
    fn test_days_from_civil_handles_leap_years() {
        // 2020 is a leap year (divisible by 4, not by 100): Feb 29 exists.
        assert_eq!(days_from_civil(2020, 2, 29), 18321);
        assert_eq!(days_from_civil(2020, 3, 1), 18321 + 1);
        // 1900 is NOT a leap year (divisible by 100, not by 400).
        // 2000 IS a leap year (divisible by 400).
        assert!(days_from_civil(2000, 3, 1) - days_from_civil(2000, 2, 1) == 29);
        assert!(days_from_civil(1900, 3, 1) - days_from_civil(1900, 2, 1) == 28);
    }

    #[test]
    fn test_parse_datetime_date_only() {
        assert_eq!(parse_datetime("2020-04-13"), Some(18365 * 86400));
        assert_eq!(parse_datetime("1970-01-01"), Some(0));
    }

    #[test]
    fn test_parse_datetime_time_only() {
        assert_eq!(parse_datetime("12:53:24"), Some(12 * 3600 + 53 * 60 + 24));
        assert_eq!(parse_datetime("00:00:00"), Some(0));
    }

    #[test]
    fn test_parse_datetime_combined_with_space_or_t() {
        let expected = Some(18365 * 86400 + 12 * 3600 + 53 * 60 + 24);
        assert_eq!(parse_datetime("2020-04-13 12:53:24"), expected);
        assert_eq!(parse_datetime("2020-04-13T12:53:24"), expected);
    }

    #[test]
    fn test_parse_datetime_rejects_malformed_input() {
        assert_eq!(parse_datetime("not-a-date"), None);
        assert_eq!(parse_datetime("2020-13-01"), None); // month 13
        assert_eq!(parse_datetime("2020-01-32"), None); // day 32
        assert_eq!(parse_datetime("25:00:00"), None); // hour 25
        assert_eq!(parse_datetime("12:60:00"), None); // minute 60
        assert_eq!(parse_datetime(""), None);
    }
}
