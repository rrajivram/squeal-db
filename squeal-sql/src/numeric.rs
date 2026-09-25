//! Integer/Double comparison — the one definition every place that compares
//! two values uses (expression evaluation in plan::eval, join matching in
//! source::joinmatch, join hashing in source::hash), so `1 = 1.0` is true in
//! a WHERE filter and a hash join alike.
//!
//! Exact, not `i as f64`: above 2^53 distinct integers round to the same
//! double (`9007199254740993_i64 as f64 == 9007199254740992.0`), so a cast
//! would call two different numbers equal. cmp_int_double below compares
//! the integer against the double's integer part and fraction instead.

use std::cmp::Ordering;

use store::valueitem::ValueItem;

// 2^63, exactly representable as an f64 — the first value past i64::MAX.
const TWO_63: f64 = 9_223_372_036_854_775_808.0;

// Exact ordering of `i` against `d`. None only when `d` is NaN.
pub(crate) fn cmp_int_double(i: i64, d: f64) -> Option<Ordering> {
    if d.is_nan() {
        return None;
    }
    if d >= TWO_63 {
        return Some(Ordering::Less);
    }
    if d < -TWO_63 {
        return Some(Ordering::Greater);
    }
    // `t` is an integer in [-2^63, 2^63), so the cast is exact.
    let t = d.trunc();
    Some(match i.cmp(&(t as i64)) {
        Ordering::Equal => {
            let frac = d - t;
            if frac > 0.0 {
                Ordering::Less
            } else if frac < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        other => other,
    })
}

// For an Integer/Double pair (either order): Some(exact ordering), with the
// inner None meaning the Double is NaN. None for any other pair — callers
// fall back to their own same-type handling.
pub(crate) fn cmp_mixed(a: &ValueItem, b: &ValueItem) -> Option<Option<Ordering>> {
    match (a, b) {
        (ValueItem::Integer(i), ValueItem::Double(d)) => Some(cmp_int_double(*i, *d)),
        (ValueItem::Double(d), ValueItem::Integer(i)) => {
            Some(cmp_int_double(*i, *d).map(Ordering::reverse))
        }
        _ => None,
    }
}

// What a join key value hashes as: a Double holding an exact integer (in
// i64 range) hashes as that Integer, so any Integer/Double pair cmp_mixed
// calls equal also hashes equal — a hash join between an integer column
// and a double column puts `1` and `1.0` in the same bucket. None means
// "hash the value as-is".
pub(crate) fn hash_normalized(v: &ValueItem) -> Option<ValueItem> {
    match v {
        ValueItem::Double(d) if d.fract() == 0.0 && *d >= -TWO_63 && *d < TWO_63 => {
            Some(ValueItem::Integer(*d as i64))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_against_double_orders_by_value() {
        assert_eq!(cmp_int_double(2, 2.5), Some(Ordering::Less));
        assert_eq!(cmp_int_double(3, 2.5), Some(Ordering::Greater));
        assert_eq!(cmp_int_double(2, 2.0), Some(Ordering::Equal));
        assert_eq!(cmp_int_double(-2, -2.5), Some(Ordering::Greater));
        assert_eq!(cmp_int_double(-3, -2.5), Some(Ordering::Less));
        assert_eq!(cmp_int_double(0, -0.0), Some(Ordering::Equal));
    }

    #[test]
    fn test_no_rounding_above_2_pow_53() {
        // 2^53 + 1 isn't representable as an f64; `as f64` rounds it to
        // 2^53, which would make these equal.
        let big = 9_007_199_254_740_993_i64;
        assert_eq!(cmp_int_double(big, 9_007_199_254_740_992.0), Some(Ordering::Greater));
        assert_eq!(cmp_int_double(big - 1, 9_007_199_254_740_992.0), Some(Ordering::Equal));
    }

    #[test]
    fn test_out_of_range_and_special_doubles() {
        assert_eq!(cmp_int_double(i64::MAX, 1e19), Some(Ordering::Less));
        assert_eq!(cmp_int_double(i64::MIN, -1e19), Some(Ordering::Greater));
        assert_eq!(cmp_int_double(i64::MIN, -9_223_372_036_854_775_808.0), Some(Ordering::Equal));
        assert_eq!(cmp_int_double(0, f64::INFINITY), Some(Ordering::Less));
        assert_eq!(cmp_int_double(0, f64::NEG_INFINITY), Some(Ordering::Greater));
        assert_eq!(cmp_int_double(0, f64::NAN), None);
    }

    #[test]
    fn test_cmp_mixed_is_symmetric_and_ignores_other_pairs() {
        let (i, d) = (ValueItem::Integer(1), ValueItem::Double(1.5));
        assert_eq!(cmp_mixed(&i, &d), Some(Some(Ordering::Less)));
        assert_eq!(cmp_mixed(&d, &i), Some(Some(Ordering::Greater)));
        assert_eq!(cmp_mixed(&i, &ValueItem::Integer(1)), None);
        assert_eq!(cmp_mixed(&d, &ValueItem::Double(1.0)), None);
    }

    #[test]
    fn test_equal_integer_and_double_hash_the_same() {
        assert_eq!(hash_normalized(&ValueItem::Double(3.0)), Some(ValueItem::Integer(3)));
        assert_eq!(hash_normalized(&ValueItem::Double(-0.0)), Some(ValueItem::Integer(0)));
        assert_eq!(hash_normalized(&ValueItem::Double(3.5)), None);
        assert_eq!(hash_normalized(&ValueItem::Double(1e19)), None);
        assert_eq!(hash_normalized(&ValueItem::Integer(3)), None);
    }
}
