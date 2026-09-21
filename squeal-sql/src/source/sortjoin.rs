use crate::source::{column_names, planinfo::PlanNode};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    panic,
    sync::Arc,
    thread,
    time::Instant,
};

use postcard::{from_bytes, to_allocvec};
use store::{
    cursor::Cursor,
    db::{DBFile, Db},
    run::{Run, RunCursor},
    valueitem::IndexKey,
};

use crate::{
    error::SchemaError,
    plan::memory::{MemReservation, QueryMemory},
    source::{
        ComputedTableStat, ProjectableField, QueryStats, Source,
        join::JoinType,
        joinmatch::JoinMatcher,
        merge_stats,
        sort::{SortField, SortSource},
    },
};

// Sort-merge join: sorts both inputs on the join keys (concurrently, one
// thread each), then walks them in lockstep. What counts as a match, and
// what LEFT/RIGHT/FULL emit for unmatched rows, is JoinMatcher's — shared
// with HashedSource — this type only decides which pairs to look at.
//
// A run of right rows sharing one key (a "group") is buffered so every left
// row with that key can be paired with all of them. The buffer is charged to
// QueryMemory; once that budget is exhausted the whole group moves to a
// temp-pool Run (spilling to `<db>.tmp` as its own cache fills) and is
// re-read once per left row of the group.
// The right rows of the current key group: in memory while the query budget
// allows, in a Run once it doesn't.
struct GroupBuf<F: DBFile + 'static> {
    rows: Vec<IndexKey>,
    mem: Vec<MemReservation>,
    spill: Option<Run<F>>,
    len: usize,
    // Any one row of the group — what the next left row's key is compared to.
    first: Option<IndexKey>,
}

impl<F> GroupBuf<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn new() -> Self {
        Self {
            rows: vec![],
            mem: vec![],
            spill: None,
            len: 0,
            first: None,
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.mem.clear();
        self.spill = None;
        self.len = 0;
        self.first = None;
    }

    fn spilled(&self) -> bool {
        self.spill.is_some()
    }

    fn push(&mut self, row: IndexKey, db: &Arc<Db<F>>, budget: &Arc<QueryMemory>) -> Result<(), SchemaError> {
        if self.first.is_none() {
            self.first = Some(row.clone());
        }
        self.len += 1;
        if self.spill.is_none() {
            match budget.try_reserve(row.size()) {
                Ok(r) => {
                    self.mem.push(r);
                    self.rows.push(row);
                    return Ok(());
                }
                Err(SchemaError::QueryMemoryExceeded { .. }) => {
                    let mut run = db.create_run()?;
                    for r in self.rows.drain(..) {
                        run.append(&to_allocvec(&r)?)?;
                    }
                    // The rows now live in the run; give the budget back.
                    self.mem.clear();
                    self.spill = Some(run);
                }
                Err(e) => return Err(e),
            }
        }
        self.spill
            .as_mut()
            .expect("spilled above")
            .append(&to_allocvec(&row)?)?;
        Ok(())
    }
}

pub(crate) struct SortJoinSource<F: DBFile + 'static> {
    // Present until the first next(), which hands them to the sorts.
    left_source: Option<Box<dyn Source>>,
    right_source: Option<Box<dyn Source>>,
    left_sorted: Option<SortSource<F>>,
    right_sorted: Option<SortSource<F>>,
    // primed: both sorts have produced their first row (left_cur/right_cur
    // are valid). Cleared by reset(), which re-sorts lazily.
    primed: bool,
    db: Arc<Db<F>>,
    mem: Arc<QueryMemory>,
    matcher: JoinMatcher,
    left_fields: Vec<usize>,
    right_fields: Vec<usize>,
    fields: Arc<[ProjectableField]>,
    // The next unconsumed row of each sorted side (None once exhausted).
    left_cur: Option<IndexKey>,
    right_cur: Option<IndexKey>,
    // While in_group: `left_cur` is being paired with every row of `group`
    // (all right rows sharing its key); group_pos is the next one to emit.
    in_group: bool,
    group: GroupBuf<F>,
    group_pos: usize,
    // Spilled groups: the read position within the run for the current pass.
    group_cursor: Option<RunCursor<F>>,
    sort_time: u128,
    next_time: u128,
    // Key groups that outgrew the memory budget and moved to a Run.
    spilled_groups: usize,
}

impl<F> SortJoinSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    pub(crate) fn new(
        left_source: Box<dyn Source>,
        right_source: Box<dyn Source>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
        left_fields: &[usize],
        right_fields: &[usize],
        join_type: JoinType,
    ) -> Result<Self, SchemaError> {
        if matches!(join_type, JoinType::Cross) {
            return Err(SchemaError::UnknownError(
                "a sort join needs equi-join keys; CROSS JOIN has none".into(),
            ));
        }
        let fields = Arc::from(
            left_source
                .fields()
                .iter()
                .chain(right_source.fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        );
        let matcher = JoinMatcher::new(
            join_type,
            left_fields,
            right_fields,
            left_source.fields().len(),
            right_source.fields().len(),
        )?;
        Ok(Self {
            left_source: Some(left_source),
            right_source: Some(right_source),
            left_sorted: None,
            right_sorted: None,
            primed: false,
            db,
            mem,
            matcher,
            left_fields: left_fields.to_vec(),
            right_fields: right_fields.to_vec(),
            fields,
            left_cur: None,
            right_cur: None,
            in_group: false,
            group: GroupBuf::new(),
            group_pos: 0,
            group_cursor: None,
            sort_time: 0,
            next_time: 0,
            spilled_groups: 0,
        })
    }

    fn make_sort_field(fields: &[usize]) -> Vec<SortField> {
        fields
            .iter()
            .map(|f| SortField {
                asc: true,
                null_first: true,
                index: *f,
            })
            .collect()
    }

    // Wraps both inputs in sorts. Nothing is read yet; see prime().
    fn build_sorts(&mut self) -> Result<(), SchemaError> {
        let (Some(left), Some(right)) = (self.left_source.take(), self.right_source.take()) else {
            return Err(SchemaError::UnknownError(
                "sort join has no inputs to sort".into(),
            ));
        };
        self.left_sorted = Some(SortSource::new(
            left,
            &Self::make_sort_field(&self.left_fields),
            None,
            self.db.clone(),
            self.mem.clone(),
        )?);
        self.right_sorted = Some(SortSource::new(
            right,
            &Self::make_sort_field(&self.right_fields),
            None,
            self.db.clone(),
            self.mem.clone(),
        )?);
        Ok(())
    }

    // Pulls each side's first row on its own thread — a SortSource sorts
    // lazily on its first next(), so that call IS the sort — leaving both
    // sides positioned on their first row.
    fn prime(&mut self) -> Result<(), SchemaError> {
        let start = Instant::now();
        let (Some(left), Some(right)) = (self.left_sorted.as_mut(), self.right_sorted.as_mut())
        else {
            return Err(SchemaError::UnknownError("sort join is not built".into()));
        };
        let (left_first, right_first) = thread::scope(|s| {
            let l = s.spawn(|| left.next());
            let r = s.spawn(|| right.next());
            (l.join(), r.join())
        });
        let left_first = left_first.unwrap_or_else(|e| panic::resume_unwind(e));
        let right_first = right_first.unwrap_or_else(|e| panic::resume_unwind(e));
        self.left_cur = left_first?;
        self.right_cur = right_first?;
        self.primed = true;
        self.sort_time += start.elapsed().as_nanos();
        Ok(())
    }

    fn pull_left(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        self.left_sorted.as_mut().expect("sorted").next()
    }

    fn pull_right(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        self.right_sorted.as_mut().expect("sorted").next()
    }

    // `left_cur` and `right_cur` share a key: buffer every consecutive right
    // row with that key, leaving `right_cur` on the first one past it.
    fn start_group(&mut self, left: &IndexKey) -> Result<(), SchemaError> {
        self.end_group();
        let mut cur = self.right_cur.take();
        while let Some(row) = cur {
            if self.matcher.cmp_keys(left, &row) != Ordering::Equal {
                self.right_cur = Some(row);
                break;
            }
            self.group.push(row, &self.db, &self.mem)?;
            cur = self.pull_right()?;
        }
        if self.group.spilled() {
            self.spilled_groups += 1;
        }
        self.in_group = true;
        self.restart_group()
    }

    fn end_group(&mut self) {
        self.in_group = false;
        self.group.clear();
        self.group_pos = 0;
        self.group_cursor = None;
    }

    // Rewind to the first row of the group, for the next left row.
    fn restart_group(&mut self) -> Result<(), SchemaError> {
        self.group_pos = 0;
        self.group_cursor = match &self.group.spill {
            Some(run) => Some(run.cursor()?),
            None => None,
        };
        Ok(())
    }

    fn next_group_row(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if let Some(cursor) = self.group_cursor.as_mut() {
            return match cursor.next()? {
                Some(t) => Ok(Some(from_bytes(t.data())?)),
                None => Ok(None),
            };
        }
        let row = self.group.rows.get(self.group_pos).cloned();
        self.group_pos += 1;
        Ok(row)
    }

    fn step(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if self.left_sorted.is_none() {
            self.build_sorts()?;
        }
        if !self.primed {
            self.prime()?;
        }
        loop {
            if self.in_group {
                if let Some(right) = self.next_group_row()? {
                    let left = self.left_cur.as_ref().expect("in a group");
                    return Ok(Some(self.matcher.combine(left, &right)?));
                }
                // This left row has met the whole group; the next left row
                // either shares the key (and meets it too) or ends the group.
                self.left_cur = self.pull_left()?;
                let same_key = match (&self.left_cur, &self.group.first) {
                    (Some(l), Some(g)) => self.matcher.cmp_keys(l, g) == Ordering::Equal,
                    _ => false,
                };
                if same_key {
                    self.restart_group()?;
                } else {
                    self.end_group();
                }
                continue;
            }
            let order = match (&self.left_cur, &self.right_cur) {
                (None, None) => return Ok(None),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(l), Some(r)) => self.matcher.cmp_keys(l, r),
            };
            match order {
                Ordering::Less => {
                    let left = self.left_cur.take().expect("left is behind");
                    self.left_cur = self.pull_left()?;
                    if self.matcher.keeps_unmatched_left() {
                        return Ok(Some(self.matcher.left_only(&left)?));
                    }
                }
                Ordering::Greater => {
                    let right = self.right_cur.take().expect("right is behind");
                    self.right_cur = self.pull_right()?;
                    if self.matcher.keeps_unmatched_right() {
                        return Ok(Some(self.matcher.right_only(&right)?));
                    }
                }
                Ordering::Equal => {
                    let left = self.left_cur.clone().expect("keys are equal");
                    self.start_group(&left)?;
                }
            }
        }
    }
}

impl<F> Source for SortJoinSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        let side = |raw: &Option<Box<dyn Source>>, sorted: &Option<SortSource<F>>| {
            match (raw, sorted) {
                (Some(s), _) => Some(s.plan()),
                (None, Some(s)) => Some(s.plan()),
                _ => None,
            }
        };
        let names = |raw: &Option<Box<dyn Source>>, sorted: &Option<SortSource<F>>| {
            match (raw, sorted) {
                (Some(s), _) => column_names(&s.fields()),
                (None, Some(s)) => column_names(&s.fields()),
                _ => vec![],
            }
        };
        let (left, right) = (
            names(&self.left_source, &self.left_sorted),
            names(&self.right_source, &self.right_sorted),
        );
        let name = |cols: &[String], i: &usize| cols.get(*i).cloned().unwrap_or_else(|| format!("#{i}"));
        let keys = self
            .left_fields
            .iter()
            .zip(&self.right_fields)
            .map(|(l, r)| format!("left({}) = right({})", name(&left, l), name(&right, r)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut node = PlanNode::new("SortMergeJoin").detail(format!("{:?} on {keys}", self.matcher.join_type()));
        for child in [
            side(&self.left_source, &self.left_sorted),
            side(&self.right_source, &self.right_sorted),
        ]
        .into_iter()
        .flatten()
        {
            node = node.child(child);
        }
        node
    }


    fn fields(&self) -> Arc<[ProjectableField]> {
        self.fields.clone()
    }

    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        let start = Instant::now();
        let out = self.step();
        self.next_time += start.elapsed().as_nanos();
        out
    }

    // Rewinds both inputs; the next next() re-sorts and starts the merge over.
    fn reset(&mut self) -> Result<(), SchemaError> {
        if let Some(s) = self.left_sorted.as_mut() {
            s.reset()?;
        }
        if let Some(s) = self.right_sorted.as_mut() {
            s.reset()?;
        }
        // Not yet built: the raw inputs are still here.
        if let Some(s) = self.left_source.as_mut() {
            s.reset()?;
        }
        if let Some(s) = self.right_source.as_mut() {
            s.reset()?;
        }
        self.end_group();
        self.left_cur = None;
        self.right_cur = None;
        self.primed = false;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let mut res = vec![(
            "SortJoin".to_string(),
            QueryStats {
                stats: HashMap::from([
                    ("sort_ns".into(), self.sort_time as f64),
                    ("next_ns".into(), self.next_time as f64),
                    ("spilled_groups".into(), self.spilled_groups as f64),
                ]),
                level: 0,
            },
        )];
        for s in [&self.left_sorted, &self.right_sorted].into_iter().flatten() {
            res = merge_stats(res, s.query_stats());
        }
        Some(res)
    }

    fn table_stats(&self) -> Option<ComputedTableStat> {
        None
    }
}

impl<F: DBFile + 'static> Debug for SortJoinSource<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortJoin")
            .field("left", &self.left_source)
            .field("right", &self.right_source)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use store::{memfile::MemFile, valueitem::ValueItem};

    use super::*;
    use crate::source::{
        hash::HashedSource,
        test_support::VecSource,
    };

    type Row = Vec<ValueItem>;

    fn int(i: i64) -> ValueItem {
        ValueItem::Integer(i)
    }

    fn make_db() -> Arc<Db<MemFile>> {
        Db::<MemFile>::create("sort_join_test.db").unwrap()
    }

    fn source(rows: &[Row]) -> Box<dyn Source> {
        Box::new(VecSource::new(&["k", "v"], rows.to_vec()))
    }

    fn sort_join(left: &[Row], right: &[Row], t: JoinType, mem: usize) -> SortJoinSource<MemFile> {
        SortJoinSource::new(
            source(left),
            source(right),
            make_db(),
            QueryMemory::new(mem),
            &[0],
            &[0],
            t,
        )
        .unwrap()
    }

    fn run(mut s: impl Source) -> Vec<Row> {
        let mut out = vec![];
        while let Some(r) = s.next().unwrap() {
            out.push(r.values().to_vec());
        }
        out.sort();
        out
    }

    // Independent of JoinMatcher: plain nested loops over ValueItem ==.
    fn oracle(left: &[Row], right: &[Row], t: JoinType) -> Vec<Row> {
        let (keep_l, keep_r) = match t {
            JoinType::Inner => (false, false),
            JoinType::Left => (true, false),
            JoinType::Right => (false, true),
            JoinType::Full => (true, true),
            JoinType::Cross => unreachable!(),
        };
        let mut out = vec![];
        let mut right_hit = vec![false; right.len()];
        for l in left {
            let mut hit = false;
            for (i, r) in right.iter().enumerate() {
                if l[0] == r[0] {
                    hit = true;
                    right_hit[i] = true;
                    out.push([l.clone(), r.clone()].concat());
                }
            }
            if !hit && keep_l {
                out.push([l.clone(), vec![ValueItem::Null; 2]].concat());
            }
        }
        if keep_r {
            for (i, r) in right.iter().enumerate() {
                if !right_hit[i] {
                    out.push([vec![ValueItem::Null; 2], r.clone()].concat());
                }
            }
        }
        out.sort();
        out
    }

    // Deterministic pseudo-random rows: `n` rows, keys in 0..keys (with
    // duplicates) plus some NULL keys, and a unique payload per row.
    fn rows(seed: u64, n: usize, keys: i64, base: i64) -> Vec<Row> {
        let mut x = seed;
        (0..n)
            .map(|i| {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let k = ((x >> 33) as i64) % (keys + 2);
                let key = if k >= keys { ValueItem::Null } else { int(k) };
                vec![key, int(base + i as i64)]
            })
            .collect()
    }

    const TYPES: [JoinType; 4] = [JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::Full];

    #[test]
    fn test_every_join_type_matches_an_independent_nested_loop_oracle() {
        for seed in 0..8 {
            let left = rows(seed, 60, 10, 0);
            let right = rows(seed + 100, 80, 14, 1000);
            for t in TYPES {
                let got = run(sort_join(&left, &right, t, 1 << 20));
                assert_eq!(got, oracle(&left, &right, t), "seed {seed} {t:?}");
            }
        }
    }

    // The point of sharing JoinMatcher: both algorithms agree exactly.
    #[test]
    fn test_sort_join_and_hash_join_produce_identical_results() {
        for seed in 20..26 {
            let left = rows(seed, 70, 12, 0);
            let right = rows(seed + 50, 50, 9, 1000);
            for t in TYPES {
                let hash = HashedSource::new(
                    source(&left),
                    source(&right),
                    make_db(),
                    QueryMemory::new(1 << 20),
                    &[0],
                    &[0],
                    t,
                )
                .unwrap();
                assert_eq!(
                    run(sort_join(&left, &right, t, 1 << 20)),
                    run(hash),
                    "seed {seed} {t:?}"
                );
            }
        }
    }

    #[test]
    fn test_empty_sides() {
        let some = rows(1, 5, 3, 0);
        for t in TYPES {
            assert_eq!(run(sort_join(&[], &[], t, 1 << 20)), vec![] as Vec<Row>, "{t:?}");
            assert_eq!(run(sort_join(&some, &[], t, 1 << 20)), oracle(&some, &[], t), "{t:?}");
            assert_eq!(run(sort_join(&[], &some, t, 1 << 20)), oracle(&[], &some, t), "{t:?}");
        }
    }

    #[test]
    fn test_a_large_duplicate_group_on_both_sides_yields_the_full_cross_product() {
        let left: Vec<Row> = (0..30).map(|i| vec![int(7), int(i)]).collect();
        let right: Vec<Row> = (0..40).map(|i| vec![int(7), int(1000 + i)]).collect();
        let got = run(sort_join(&left, &right, JoinType::Inner, 1 << 20));
        assert_eq!(got.len(), 30 * 40);
        assert_eq!(got, oracle(&left, &right, JoinType::Inner));
    }

    #[test]
    fn test_composite_keys() {
        let mk = |a: i64, b: i64, v: i64| vec![int(a), int(b), int(v)];
        let left = vec![mk(1, 1, 0), mk(1, 2, 1), mk(2, 1, 2)];
        let right = vec![mk(1, 2, 10), mk(2, 1, 11), mk(2, 2, 12)];
        let s = |r: &[Row]| -> Box<dyn Source> { Box::new(VecSource::new(&["a", "b", "v"], r.to_vec())) };
        let join = SortJoinSource::new(
            s(&left),
            s(&right),
            make_db(),
            QueryMemory::new(1 << 20),
            &[0, 1],
            &[0, 1],
            JoinType::Inner,
        )
        .unwrap();
        assert_eq!(
            run(join),
            vec![
                [mk(1, 2, 1), mk(1, 2, 10)].concat(),
                [mk(2, 1, 2), mk(2, 1, 11)].concat(),
            ]
        );
    }

    #[test]
    fn test_reset_replays_the_same_result_before_and_after_the_first_scan() {
        let left = rows(3, 40, 6, 0);
        let right = rows(4, 40, 6, 1000);
        for t in TYPES {
            let mut s = sort_join(&left, &right, t, 1 << 20);
            s.reset().unwrap(); // reset before anything ran is harmless
            let mut first = vec![];
            while let Some(r) = s.next().unwrap() {
                first.push(r.values().to_vec());
            }
            s.reset().unwrap();
            let mut second = vec![];
            while let Some(r) = s.next().unwrap() {
                second.push(r.values().to_vec());
            }
            first.sort();
            second.sort();
            assert_eq!(first, oracle(&left, &right, t), "{t:?}");
            assert_eq!(second, first, "{t:?} after reset");
        }
    }

    fn spill_db() -> Arc<Db<MemFile>> {
        let db = make_db();
        // Room for only a few temp pages, so a spilled group really is
        // evicted to the temp file rather than living in the pool's cache.
        db.set_temp_cache_bytes(4 * 8192);
        db
    }

    fn run_with(
        db: Arc<Db<MemFile>>,
        left: &[Row],
        right: &[Row],
        t: JoinType,
        mem: usize,
    ) -> (Vec<Row>, usize) {
        let mut s = SortJoinSource::new(
            source(left),
            source(right),
            db,
            QueryMemory::new(mem),
            &[0],
            &[0],
            t,
        )
        .unwrap();
        let mut out = vec![];
        while let Some(r) = s.next().unwrap() {
            out.push(r.values().to_vec());
        }
        out.sort();
        (out, s.spilled_groups)
    }

    // A group larger than the memory budget spills to a Run and the join is
    // still exactly right, for every join type, with duplicate keys on both
    // sides, unmatched rows around the big group, and NULL keys.
    #[test]
    fn test_a_group_over_the_memory_budget_spills_and_stays_correct() {
        let mut left = rows(11, 40, 5, 0);
        let mut right = rows(12, 3000, 5, 10_000);
        // Groups of ~1,000+ rows per key at ~20 bytes each: far over the budget below.
        left.extend((0..25).map(|i| vec![int(2), int(500 + i)]));
        right.extend((0..6000).map(|i| vec![int(2), int(50_000 + i)]));
        for t in TYPES {
            let db = spill_db();
            let (got, spilled) = run_with(db.clone(), &left, &right, t, 48 * 1024);
            assert_eq!(got, oracle(&left, &right, t), "{t:?}");
            assert!(spilled > 0, "{t:?}: the big groups must have spilled");
            // ...and they really left memory: pages were written to the
            // temp file, not just held in the pool's cache.
            assert!(db.stats().temp.spills > 0, "{t:?}: nothing reached the temp file");
        }
    }

    #[test]
    fn test_a_group_within_the_budget_does_not_spill() {
        let left = rows(1, 30, 6, 0);
        let right = rows(2, 30, 6, 1000);
        let (got, spilled) = run_with(spill_db(), &left, &right, JoinType::Full, 1 << 22);
        assert_eq!(got, oracle(&left, &right, JoinType::Full));
        assert_eq!(spilled, 0);
    }

    // Spill decisions are per group: small groups stay in memory alongside a
    // big spilled one.
    #[test]
    fn test_only_the_oversized_groups_spill() {
        let big: Vec<Row> = (0..6000).map(|i| vec![int(1), int(i)]).collect();
        let small: Vec<Row> = (0..3).map(|i| vec![int(2), int(9000 + i)]).collect();
        let right = [big, small].concat();
        let left: Vec<Row> = (0..4).map(|i| vec![int(1 + i % 2), int(i)]).collect();
        let (got, spilled) = run_with(spill_db(), &left, &right, JoinType::Inner, 48 * 1024);
        assert_eq!(got, oracle(&left, &right, JoinType::Inner));
        assert_eq!(spilled, 1);
    }

    #[test]
    fn test_reset_after_a_spilling_join_replays_it_and_releases_the_budget() {
        let left: Vec<Row> = (0..10).map(|i| vec![int(1), int(i)]).collect();
        let right: Vec<Row> = (0..6000).map(|i| vec![int(1), int(i)]).collect();
        let mem = QueryMemory::new(48 * 1024);
        let mut s = SortJoinSource::new(
            source(&left),
            source(&right),
            spill_db(),
            mem.clone(),
            &[0],
            &[0],
            JoinType::Inner,
        )
        .unwrap();
        let mut count = |s: &mut SortJoinSource<MemFile>| {
            let mut n = 0;
            while s.next().unwrap().is_some() {
                n += 1;
            }
            n
        };
        assert_eq!(count(&mut s), 10 * 6000);
        s.reset().unwrap();
        assert_eq!(count(&mut s), 10 * 6000);
        assert!(s.spilled_groups >= 2);
        drop(s);
        assert_eq!(mem.used(), 0, "every reservation is released");
    }

    #[test]
    fn test_cross_join_is_rejected() {
        let r = SortJoinSource::new(
            source(&[]),
            source(&[]),
            make_db(),
            QueryMemory::new(1024),
            &[0],
            &[0],
            JoinType::Cross,
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_output_fields_are_left_then_right() {
        let s = sort_join(&[], &[], JoinType::Inner, 1024);
        assert_eq!(s.fields().len(), 4);
    }
}
