use std::cmp::Ordering;

use store::valueitem::{IndexKey, ValueItem};

use crate::{error::SchemaError, source::join::JoinType};

// The matching rules every equi-join algorithm shares, so HashedSource and
// SortJoinSource can only differ in HOW they find candidate pairs, never in
// WHAT counts as a match or what an outer join emits for the rest:
//   - when two rows match (`keys_match`) and how they order (`cmp_keys`,
//     which agrees with `keys_match`: Equal exactly when they match),
//   - which side's unmatched rows the join type keeps,
//   - the NULL-padded row an unmatched row is paired with,
//   - assembling an output row (left columns, then right columns).
// Sides are the algorithm's own PHYSICAL left/right; an algorithm that
// swaps sides (HashedSource) builds its matcher from the swapped values and
// reorders the output itself.
#[derive(Debug, Clone)]
pub(crate) struct JoinMatcher {
    join_type: JoinType,
    left_fields: Vec<usize>,
    right_fields: Vec<usize>,
    left_null: IndexKey,
    right_null: IndexKey,
}

impl JoinMatcher {
    pub(crate) fn new(
        join_type: JoinType,
        left_fields: &[usize],
        right_fields: &[usize],
        left_width: usize,
        right_width: usize,
    ) -> Result<Self, SchemaError> {
        debug_assert_eq!(left_fields.len(), right_fields.len());
        Ok(Self {
            join_type,
            left_fields: left_fields.to_vec(),
            right_fields: right_fields.to_vec(),
            left_null: IndexKey::new_from_owned(vec![ValueItem::Null; left_width])?,
            right_null: IndexKey::new_from_owned(vec![ValueItem::Null; right_width])?,
        })
    }

    // Join-key equality. NULL == NULL here (ValueItem's own equality), so
    // rows with NULL keys join each other — the behavior HashedSource has
    // always had, kept identical for every algorithm. This is the one place
    // to change if NULL keys should instead never match (SQL semantics).
    pub(crate) fn keys_match(&self, left: &IndexKey, right: &IndexKey) -> bool {
        self.cmp_keys(left, right) == Ordering::Equal
    }

    // Lexicographic over the join-key fields, in ValueItem's total order —
    // the same order sort-based algorithms sort by (NULL lowest).
    pub(crate) fn cmp_keys(&self, left: &IndexKey, right: &IndexKey) -> Ordering {
        for (l, r) in self.left_fields.iter().zip(&self.right_fields) {
            match left.values()[*l].cmp(&right.values()[*r]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub(crate) fn keeps_unmatched_left(&self) -> bool {
        matches!(self.join_type, JoinType::Left | JoinType::Full)
    }

    pub(crate) fn keeps_unmatched_right(&self) -> bool {
        matches!(self.join_type, JoinType::Right | JoinType::Full)
    }

    // All-NULL rows shaped like each side's own columns.
    pub(crate) fn left_null(&self) -> &IndexKey {
        &self.left_null
    }

    pub(crate) fn right_null(&self) -> &IndexKey {
        &self.right_null
    }

    pub(crate) fn combine(&self, left: &IndexKey, right: &IndexKey) -> Result<IndexKey, SchemaError> {
        let mut values = left.values().to_vec();
        values.extend_from_slice(right.values());
        Ok(IndexKey::new_from_owned(values)?)
    }

    // An unmatched left row, paired with NULLs for the right side.
    pub(crate) fn left_only(&self, left: &IndexKey) -> Result<IndexKey, SchemaError> {
        self.combine(left, &self.right_null)
    }

    // An unmatched right row, paired with NULLs for the left side.
    pub(crate) fn right_only(&self, right: &IndexKey) -> Result<IndexKey, SchemaError> {
        self.combine(&self.left_null, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: &[i64]) -> IndexKey {
        IndexKey::new_from_owned(v.iter().map(|i| ValueItem::Integer(*i)).collect()).unwrap()
    }

    fn matcher(t: JoinType) -> JoinMatcher {
        // Join on left col 1 = right col 0; left has 2 columns, right 3.
        JoinMatcher::new(t, &[1], &[0], 2, 3).unwrap()
    }

    #[test]
    fn test_keys_match_and_cmp_agree_and_use_the_given_field_positions() {
        let m = matcher(JoinType::Inner);
        let (l, r) = (row(&[9, 5]), row(&[5, 0, 0]));
        assert!(m.keys_match(&l, &r));
        assert_eq!(m.cmp_keys(&l, &r), Ordering::Equal);
        assert_eq!(m.cmp_keys(&row(&[0, 4]), &r), Ordering::Less);
        assert_eq!(m.cmp_keys(&row(&[0, 6]), &r), Ordering::Greater);
        assert!(!m.keys_match(&row(&[5, 4]), &r));
    }

    #[test]
    fn test_composite_keys_compare_left_to_right() {
        let m = JoinMatcher::new(JoinType::Inner, &[0, 1], &[0, 1], 2, 2).unwrap();
        assert_eq!(m.cmp_keys(&row(&[1, 9]), &row(&[2, 0])), Ordering::Less);
        assert_eq!(m.cmp_keys(&row(&[2, 1]), &row(&[2, 0])), Ordering::Greater);
        assert!(m.keys_match(&row(&[2, 3]), &row(&[2, 3])));
    }

    #[test]
    fn test_null_keys_match_each_other_and_sort_first() {
        let m = JoinMatcher::new(JoinType::Inner, &[0], &[0], 1, 1).unwrap();
        let null = IndexKey::new_from_owned(vec![ValueItem::Null]).unwrap();
        assert!(m.keys_match(&null, &null));
        assert_eq!(m.cmp_keys(&null, &row(&[i64::MIN])), Ordering::Less);
    }

    #[test]
    fn test_which_join_types_keep_which_unmatched_side() {
        let keep = |t| {
            let m = matcher(t);
            (m.keeps_unmatched_left(), m.keeps_unmatched_right())
        };
        assert_eq!(keep(JoinType::Inner), (false, false));
        assert_eq!(keep(JoinType::Left), (true, false));
        assert_eq!(keep(JoinType::Right), (false, true));
        assert_eq!(keep(JoinType::Full), (true, true));
    }

    #[test]
    fn test_rows_assemble_left_then_right_and_pad_the_missing_side_with_nulls() {
        let m = matcher(JoinType::Full);
        let (l, r) = (row(&[1, 2]), row(&[2, 3, 4]));
        assert_eq!(m.combine(&l, &r).unwrap().values(), row(&[1, 2, 2, 3, 4]).values());
        let lo = m.left_only(&l).unwrap();
        assert_eq!(lo.values().len(), 5);
        assert_eq!(&lo.values()[..2], l.values());
        assert!(lo.values()[2..].iter().all(|v| *v == ValueItem::Null));
        let ro = m.right_only(&r).unwrap();
        assert!(ro.values()[..2].iter().all(|v| *v == ValueItem::Null));
        assert_eq!(&ro.values()[2..], r.values());
    }
}
