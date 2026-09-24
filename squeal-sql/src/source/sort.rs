use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
    vec::IntoIter,
};

use postcard::{from_bytes, to_allocvec};
use sql_parser::{Expr, query::OrderByClause};
use store::{
    clock::Instant,
    cursor::Cursor,
    db::{DBFile, Db},
    run::{Run, RunCursor},
    tuple::Tuple,
    valueitem::{IndexKey, ValueItem},
};

use crate::{
    error::SchemaError,
    plan::memory::{MemReservation, QueryMemory},
    source::{ProjectableField, QueryStats, Source, column_names, merge_stats, planinfo::PlanNode},
};

#[derive(Debug, Clone)]
pub(crate) struct SortField {
    pub(crate) asc: bool,
    pub(crate) null_first: bool,
    pub(crate) index: usize,
}

pub(crate) struct SortSource<F: DBFile + 'static> {
    source: Box<dyn Source>,
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    results: Option<Vec<IndexKey>>,
    db: Arc<Db<F>>,
    mem: Arc<QueryMemory>,
    progress: Option<SortProgress<F>>,
    // Cost of actually sorting: build_sort (the external-merge-sort
    // path) or the CrateHeap top-K build (the limited path) — whichever
    // one-time phase produced `progress`/`results`. Kept apart from
    // next_time (below) since sorting is a one-shot cost, not something
    // that scales with how many rows get pulled afterward.
    sort_time: u128,
    // Steady-state cost of yielding one already-sorted row (a Vec::pop
    // or a SortProgress::next), summed across every call after the
    // sort itself has finished.
    next_time: u128,
}

#[derive(Debug)]
struct CrateItem<'a> {
    key: IndexKey,
    order: &'a Vec<SortField>,
}

#[derive(Debug)]
#[allow(unused)]
struct CrateHeap<'a> {
    heap: BinaryHeap<CrateItem<'a>>,
    source: Vec<SortField>,
    limit: usize,
}

struct SortProgress<F: DBFile + 'static> {
    run: RunCursor<F>,
    iter: Option<IntoIter<IndexKey>>,
}

struct SortedRuns<F: DBFile + 'static> {
    runs: Vec<Run<F>>,
    count: usize,
    mem: Vec<MemReservation>,
    record_size: usize,
}

// What build_initial_runs produced.
enum InitialBuild<F: DBFile + 'static> {
    // No rows at all.
    Empty,
    // The whole input fit in the memory budget: already sorted, ascending.
    // Never touched a Run.
    InMemory(Vec<IndexKey>),
    // Didn't fit: today's Run-backed runs, ready for merge_runs.
    Spilled(SortedRuns<F>),
}

// What build_sort produced — BuildOutcome::InMemory carries the same
// ascending order InitialBuild::InMemory does.
enum BuildOutcome<F: DBFile + 'static> {
    Empty,
    InMemory(Vec<IndexKey>),
    Spilled(SortProgress<F>),
}

impl<F> SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    // Resolves ORDER BY column references against `source`'s OWN field
    // list — i.e. the query's SELECT-list output — not the raw
    // FROM-clause tables. `source` here is always already the fully
    // projected row (handle_select applies Projection/GroupSource before
    // ORDER BY ever runs), so a raw-table-based position and this
    // source's actual row width can disagree the moment the SELECT list
    // doesn't project every FROM-clause column in its original order —
    // confirmed via direct repro: `SELECT rank FROM t ORDER BY rank`
    // resolved `rank` to its raw position in `t` (1, since `t` also has
    // `id` at 0) and then indexed a 1-column projected row with it,
    // panicking. Resolving against source.fields() instead can't go out
    // of sync with the row actually being sorted, since it's read
    // directly off the same object.
    pub(crate) fn create_from(
        source: Box<dyn Source>,
        clause: &OrderByClause,
        limit: Option<usize>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
    ) -> Result<Self, SchemaError> {
        let fields = source.fields();
        let mut items = vec![];
        for c in clause.items.items() {
            let index = Self::resolve_order_by_index(&c.expr, &fields)?;
            let asc = c.direction.map(|a| a.is_left()).unwrap_or(true);
            let null_first = c.nulls.map(|(_, n)| n.is_left()).unwrap_or(false);
            items.push(SortField {
                asc,
                null_first,
                index,
            });
        }
        Self::new(source, &items, limit, db, mem)
    }

    pub fn new(
        source: Box<dyn Source>,
        fields: &[SortField],
        limit: Option<usize>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
    ) -> Result<Self, SchemaError> {
        Ok(Self {
            sort_fields: fields.to_vec(),
            source,
            limit,
            results: None,
            db,
            mem,
            progress: None,
            sort_time: 0,
            next_time: 0,
        })
    }

    // ORDER BY only ever supports a plain column reference (matching
    // the pre-existing limit here — a computed expression like `a+b`
    // already fell through to the same "non-value sort value" error via
    // EvalExpr::from_expr, since only EvalExpr::Value survived the match
    // below it). Resolves by matching the column's own (possibly
    // qualified, e.g. `t.rank`) last name segment against each
    // projected field's display_name — the position found is directly
    // the row position, since `fields` IS the projected row's own field
    // list, one for one.
    fn resolve_order_by_index(
        expr: &Expr,
        fields: &[ProjectableField],
    ) -> Result<usize, SchemaError> {
        let Expr::Column(c) = expr else {
            return Err(SchemaError::UnknownError(
                "Do not know how to process non-value sort value".into(),
            ));
        };
        let name = c.idents().last().map(|i| i.value.clone()).ok_or_else(|| {
            SchemaError::UnknownError("empty column reference in ORDER BY".into())
        })?;
        let mut found = None;
        for (i, f) in fields.iter().enumerate() {
            if f.display_name.eq_ignore_ascii_case(&name) {
                if found.is_some() {
                    return Err(SchemaError::AmbiguousFieldError(name));
                }
                found = Some(i);
            }
        }
        found.ok_or(SchemaError::FieldNotFound(name))
    }

    pub(crate) fn with_fields(
        source: Box<dyn Source>,
        db: Arc<Db<F>>,
        mem: Arc<QueryMemory>,
        fields: &[usize],
    ) -> Result<Self, SchemaError> {
        let sort_fields = fields
            .iter()
            .map(|f| SortField {
                asc: true,
                null_first: true,
                index: *f,
            })
            .collect::<Vec<_>>();
        Ok(Self {
            sort_fields,
            source,
            progress: None,
            db,
            mem,
            limit: None,
            results: None,
            sort_time: 0,
            next_time: 0,
        })
    }

    // What sorting the whole input produced. InMemory is the common case: the
    // input never exceeded the memory budget, so it was sorted with a plain
    // Vec — no Run, no postcard — see build_initial_runs' own doc comment.
    // Spilled is today's external-merge-sort path, unchanged, for when it
    // didn't fit.
    fn build_sort(&mut self) -> Result<BuildOutcome<F>, SchemaError> {
        let record_size = self
            .source
            .fields()
            .iter()
            .map(|f| f.field.datatype.size())
            .sum::<usize>();
        if record_size == 0 {
            return Err(SchemaError::UnknownError("Record size is 0".into()));
        }
        let built = self.build_initial_runs(record_size)?;
        let mut run = match built {
            InitialBuild::Empty => return Ok(BuildOutcome::Empty),
            InitialBuild::InMemory(sorted) => return Ok(BuildOutcome::InMemory(sorted)),
            InitialBuild::Spilled(runs) => runs,
        };
        // merge_runs is strictly 2-way (see its own pairwise pop loop), so
        // reducing `run.runs.len()` initial runs down to 1 always takes
        // ceil(log2(run.runs.len())) rounds — computed directly from the
        // actual run count build_initial_runs just returned, not
        // re-derived from `count`/`mem.len()` (mem.len() tracks pages
        // reserved for buffering, frozen once the memory budget is first
        // exhausted, which has no fixed relationship to how many runs
        // that ends up producing).
        let num_passes = (run.runs.len() as f64).log2().ceil() as usize;
        for _ in 0..num_passes {
            run = self.merge_runs(run, record_size)?;
        }
        assert!(run.runs.len() == 1);
        let mut cursor = run.runs.pop().unwrap().cursor()?;
        let iter = cursor
            .next()?
            .map(|t| Self::from_tuple(t).map(|v| v.into_iter()))
            .transpose()?;

        Ok(BuildOutcome::Spilled(SortProgress { run: cursor, iter }))
    }

    fn merge_runs(
        &mut self,
        runs: SortedRuns<F>,
        record_size: usize,
    ) -> Result<SortedRuns<F>, SchemaError> {
        let mut runs = runs;
        let mut new_runs = vec![];
        assert!(runs.runs.len() > 1);
        let len = runs.runs.len();
        for _ in (0..len - 1).step_by(2) {
            let lhs = runs.runs.pop().unwrap();
            let rhs = runs.runs.pop().unwrap();
            new_runs.push(self.merge_two(lhs, rhs, record_size)?);
        }
        if !runs.runs.is_empty() {
            new_runs.push(runs.runs.pop().unwrap())
        }

        Ok(SortedRuns {
            runs: new_runs,
            count: runs.count,
            mem: runs.mem,
            record_size: runs.record_size,
        })
    }

    // A real streaming merge of two sorted runs, not a page-at-a-time
    // zipper: `lhs`/`rhs` each keep a small pending buffer of whatever's
    // left from the page most recently pulled off their cursor, refilled
    // one page at a time only once that buffer runs dry. Comparing and
    // emitting record-by-record (rather than merging exactly one page
    // from each side per iteration) is what makes this correct when the
    // two runs don't have the same page count or aligned page
    // boundaries — pairing whole pages positionally previously let
    // whichever side ran out of pages first dump its remaining pages
    // through unmerged, out of order relative to data already written
    // from the other side.
    fn merge_two(
        &mut self,
        lhs: Run<F>,
        rhs: Run<F>,
        record_size: usize,
    ) -> Result<Run<F>, SchemaError> {
        let mut run = self.db.create_run()?;
        let mut lhs_cursor = lhs.cursor()?;
        let mut rhs_cursor = rhs.cursor()?;
        let records_per_page = run.data_size() as usize / record_size;

        let mut lhs_buf: VecDeque<IndexKey> = VecDeque::new();
        let mut rhs_buf: VecDeque<IndexKey> = VecDeque::new();
        let mut lhs_done = false;
        let mut rhs_done = false;
        let mut out: Vec<IndexKey> = Vec::with_capacity(records_per_page);

        loop {
            if lhs_buf.is_empty() && !lhs_done {
                match lhs_cursor.next()? {
                    Some(t) => lhs_buf.extend(Self::from_tuple(t)?),
                    None => lhs_done = true,
                }
            }
            if rhs_buf.is_empty() && !rhs_done {
                match rhs_cursor.next()? {
                    Some(t) => rhs_buf.extend(Self::from_tuple(t)?),
                    None => rhs_done = true,
                }
            }

            let take_left = match (lhs_buf.front(), rhs_buf.front()) {
                (Some(l), Some(r)) => {
                    let l = CrateItem {
                        key: l.clone(),
                        order: &self.sort_fields,
                    };
                    let r = CrateItem {
                        key: r.clone(),
                        order: &self.sort_fields,
                    };
                    l <= r
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let item = if take_left {
                lhs_buf.pop_front().unwrap()
            } else {
                rhs_buf.pop_front().unwrap()
            };
            out.push(item);
            if out.len() == records_per_page {
                run.set_content(&to_allocvec(&out)?)?;
                run.new_page()?;
                out.clear();
            }
        }

        if !out.is_empty() {
            run.set_content(&to_allocvec(&out)?)?;
        }

        Ok(run)
    }

    fn from_tuple(tuple: Tuple) -> Result<Vec<IndexKey>, SchemaError> {
        Ok(from_bytes(tuple.data())?)
    }

    // Reads the whole input, sorted, ready for build_sort. The common case
    // — the input never exceeds the memory budget reserved along the way —
    // returns InMemory: nothing beyond `run` (kept only to learn
    // records_per_page/data_size; never written to) ever touches a Run
    // page or postcard. Only once a reservation actually fails does this
    // fall back to closing what's accumulated so far into a real Run and
    // continuing the existing page/run-budgeted accumulation — from that
    // point on this is unchanged from before this in-memory fast path
    // existed.
    fn build_initial_runs(&mut self, record_size: usize) -> Result<InitialBuild<F>, SchemaError> {
        let mut run = self.db.create_run()?;
        let mut runs = vec![];
        let mut mems = vec![];
        let records_per_page = run.data_size() as usize / record_size;
        let mut page_count_in_run = 0;
        let mut pages_per_run = 0;
        let mut total_count = 0;
        let mut mem_filled = false;
        // Every row belonging to the CURRENT (not yet closed) run, held
        // whole until the run actually closes — a run has to be one
        // single globally sorted sequence across however many pages it
        // holds (merge_two/merge_runs read a run's pages strictly in
        // page order and assume the whole thing is monotonic), so it
        // can only be sorted correctly as one batch, right before being
        // split into pages. Sorting and flushing one
        // records_per_page-sized chunk at a time as it filled (the
        // previous shape here) only ever guaranteed each individual
        // PAGE was internally sorted — nothing kept page 2's rows >=
        // page 1's, so any run spanning more than one page silently
        // wasn't actually sorted as a whole, and every downstream GROUP
        // BY/ORDER BY correctness guarantee broke the moment a run grew
        // past one page (invisible in prior tests, which always forced
        // a one-page-per-run budget).
        let mut current_run: Vec<IndexKey> = vec![];
        let mem = self.mem.try_reserve(run.data_size() as usize)?;
        mems.push(mem);
        while let Some(r) = self.source.next()? {
            total_count += 1;
            current_run.push(r);
            if !current_run.len().is_multiple_of(records_per_page) {
                continue;
            }
            if mem_filled {
                // Counts the page's worth just accumulated —
                // incrementing before comparing (rather than after, the
                // original order here) is what makes a run actually
                // close out at `pages_per_run` pages instead of
                // `pages_per_run + 1`.
                page_count_in_run += 1;
                if page_count_in_run == pages_per_run {
                    page_count_in_run = 0;
                    Self::close_run(
                        &self.sort_fields,
                        &mut run,
                        &mut current_run,
                        records_per_page,
                    )?;
                    runs.push(run);
                    run = self.db.create_run()?;
                }
            } else {
                let new_mem = self.mem.try_reserve(run.data_size() as usize);
                if let Err(_e) = new_mem {
                    mem_filled = true;
                    // reached max buffers , create a new run
                    pages_per_run = mems.len();
                    page_count_in_run = 0;
                    Self::close_run(
                        &self.sort_fields,
                        &mut run,
                        &mut current_run,
                        records_per_page,
                    )?;
                    runs.push(run);
                    run = self.db.create_run()?;
                } else {
                    mems.push(new_mem.unwrap());
                    page_count_in_run += 1;
                }
            }
        }
        // Never had to spill: the whole input is still sitting in
        // `current_run`, and `runs` never got a single entry written to it.
        // Sort it in memory and hand it back directly — `run` (never
        // written to) and `mems` (the reservations) both just drop here.
        if !mem_filled && runs.is_empty() {
            if current_run.is_empty() {
                return Ok(InitialBuild::Empty);
            }
            return Ok(InitialBuild::InMemory(Self::sort_rows(&self.sort_fields, current_run)));
        }
        if !current_run.is_empty() {
            Self::close_run(
                &self.sort_fields,
                &mut run,
                &mut current_run,
                records_per_page,
            )?;
            runs.push(run);
        }

        Ok(InitialBuild::Spilled(SortedRuns {
            runs,
            count: total_count,
            mem: mems,
            record_size,
        }))
    }

    // Sorts every row accumulated for the run currently being built —
    // as one whole batch, not per page, see build_initial_runs' own
    // comment on why that distinction is load-bearing — and splits the
    // result across `run`'s pages, records_per_page rows each. Drains
    // `rows` so the caller's accumulator is ready to start the next
    // run.
    fn close_run(
        sort_fields: &Vec<SortField>,
        run: &mut Run<F>,
        rows: &mut Vec<IndexKey>,
        records_per_page: usize,
    ) -> Result<(), SchemaError> {
        let sorted = Self::sort_rows(sort_fields, std::mem::take(rows));
        let chunks: Vec<&[IndexKey]> = sorted.chunks(records_per_page.max(1)).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let data = to_allocvec(&chunk.to_vec())?;
            assert!(data.len() <= run.data_size() as usize);
            run.set_content(&data)?;
            if i + 1 < chunks.len() {
                run.new_page()?;
            }
        }
        Ok(())
    }

    // Sorts `rows` as one whole batch (not per chunk — see
    // build_initial_runs' own comment on why that distinction is
    // load-bearing for a Run spanning more than one page), ascending by
    // `sort_fields`. Shared by close_run (about to split the result across
    // Run pages) and the in-memory fast path (which just keeps it).
    fn sort_rows(sort_fields: &[SortField], rows: Vec<IndexKey>) -> Vec<IndexKey> {
        let mut rows = rows;
        rows.sort_unstable_by(|a, b| cmp_by_fields(a, b, sort_fields));
        rows
    }
}

impl<F: DBFile + 'static> SortProgress<F> {
    fn next(&mut self) -> Result<Option<IndexKey>, SchemaError> {
        if let Some(iter) = &mut self.iter {
            if let Some(res) = iter.next() {
                Ok(Some(res))
            } else {
                self.iter = self
                    .run
                    .next()?
                    .map(|t| SortSource::<F>::from_tuple(t).map(|v| v.into_iter()))
                    .transpose()?;
                self.next()
            }
        } else {
            Ok(None)
        }
    }
}

impl<F> Source for SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn plan(&self) -> PlanNode {
        let names = column_names(&self.source.fields());
        let keys = self
            .sort_fields
            .iter()
            .map(|f| {
                format!(
                    "{} {}{}",
                    names.get(f.index).cloned().unwrap_or_else(|| format!("#{}", f.index)),
                    if f.asc { "ASC" } else { "DESC" },
                    if f.null_first { " NULLS FIRST" } else { "" },
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        match self.limit {
            Some(n) => PlanNode::new("TopN").detail(format!("{n} by {keys}")),
            None => PlanNode::new("Sort").detail(keys),
        }
        .child(self.source.plan())
    }


    fn fields(&self) -> Arc<[super::ProjectableField]> {
        self.source.fields()
    }

    fn next(&mut self) -> Result<Option<store::valueitem::IndexKey>, SchemaError> {
        // Timed per-phase, not as one Instant wrapping the whole call:
        // this function tail-recurses into itself (twice, below) once a
        // one-time phase (top-K build / external sort) hands off to the
        // steady-state read phase. A single outer Instant spanning a
        // recursive self.next() call would double-count that inner
        // call's own already-recorded elapsed time, since the outer
        // span's elapsed() is measured only after the inner call (and
        // its own bookkeeping) has already returned.
        if let Some(results) = &mut self.results {
            let start = Instant::now();
            let out = results.pop().to_owned();
            self.next_time += start.elapsed().as_nanos();
            return Ok(out);
        }
        let mut limited = false;
        if let Some(_limit) = self.limit {
            limited = true;
        }
        if limited {
            let start = Instant::now();
            let mut heap = CrateHeap::new(self);
            while let Some(rec) = self.source.next()? {
                heap.push(CrateItem {
                    key: rec.to_owned(),
                    order: &self.sort_fields,
                });
            }
            // into_sorted_vec() returns ascending order, but next() reads
            // it back with .pop() (removes from the *end*) — reversed
            // here so the first pop() yields the smallest (ASC-first)
            // item, not the largest.
            let mut sorted = heap
                .heap
                .into_sorted_vec()
                .drain(..)
                .map(|i| i.key)
                .collect::<Vec<_>>();
            sorted.reverse();
            self.results = Some(sorted);
            self.sort_time += start.elapsed().as_nanos();
            self.next()
        } else if let Some(progress) = &mut self.progress {
            let start = Instant::now();
            let out = progress.next();
            self.next_time += start.elapsed().as_nanos();
            out
        } else {
            let start = Instant::now();
            match self.build_sort()? {
                BuildOutcome::Empty => self.results = Some(vec![]),
                // `results` is read back with .pop() (from the end), so the
                // already-ascending sort is reversed here — same convention
                // the limited/CrateHeap path above uses.
                BuildOutcome::InMemory(mut sorted) => {
                    sorted.reverse();
                    self.results = Some(sorted);
                }
                BuildOutcome::Spilled(progress) => self.progress = Some(progress),
            }
            self.sort_time += start.elapsed().as_nanos();
            self.next()
        }
    }

    // Rewinds the input and discards the sorted output, so the next call
    // to next() re-reads and re-sorts. (This used to be a no-op, which left
    // an already-drained sort returning nothing on a second scan.)
    fn reset(&mut self) -> Result<(), SchemaError> {
        self.source.reset()?;
        self.results = None;
        self.progress = None;
        Ok(())
    }

    fn query_stats(&self) -> Option<Vec<(String, QueryStats)>> {
        let this_stats = vec![(
            "SortSource".to_string(),
            QueryStats {
                stats: HashMap::from([
                    ("sort_ns".into(), self.sort_time as f64),
                    ("next_ns".into(), self.next_time as f64),
                ]),
                level: 0,
            },
        )];
        Some(merge_stats(this_stats, self.source.query_stats()))
    }
}

impl<F> Debug for SortSource<F>
where
    F: DBFile + 'static,
    F: DBFile<Item = F>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortSource")
            .field("fields", &self.sort_fields)
            .finish()
    }
}

impl<'a> CrateHeap<'a> {
    fn new<F: DBFile + 'static>(source: &SortSource<F>) -> Self {
        Self {
            source: source.sort_fields.clone(),
            heap: BinaryHeap::new(),
            limit: source.limit.unwrap(),
        }
    }

    fn push(&mut self, item: CrateItem<'a>) {
        self.heap.push(item);
        // Strictly greater, not equal: this is a max-heap holding the
        // smallest `limit` items seen so far (ASC) by always evicting the
        // current worst offender once there's one too many. Evicting the
        // moment the heap merely *reaches* `limit` (as opposed to
        // exceeding it) throws away a row that belongs in the result on
        // every single push from then on — with exactly `limit` total
        // rows, this used to end up one row short every time.
        if self.heap.len() > self.limit {
            self.heap.pop();
        }
    }
}

// Was, inline in CrateItem::cmp: push every field's Ordering into a Vec (a
// heap allocation on every single comparison — O(n log n) of them for a
// sort of n rows), then loop back over the Vec to find the first non-Equal.
// Returning as soon as a field decides (a tie still falls through to the
// next field either way) does the same job with no allocation — the
// dominant cost of a full ORDER BY (found while quantifying the Run/
// postcard cost this same sort was also paying — see plan/eval's same
// investigation). A free function, not just CrateItem::cmp's body, so
// sort_rows can sort a plain `Vec<IndexKey>` directly (sort_unstable_by)
// without wrapping every row in a CrateItem first.
fn cmp_by_fields(lhs_key: &IndexKey, rhs_key: &IndexKey, order: &[SortField]) -> Ordering {
    // Iterate the ORDER BY clauses themselves, in their own order — not
    // lhs_key.values() — and use each one's own `index` (its flat position
    // in the row, resolved back in create_from) to pull its value out of
    // the row. `ORDER BY name` on a 3-column `SELECT *` has exactly one
    // sort field but a 3-value row; the two lengths only ever coincide when
    // every projected column is also an ORDER BY key, which isn't the
    // general case.
    for field in order {
        let lhs = &lhs_key.values()[field.index];
        let rhs = &rhs_key.values()[field.index];
        let ord = match (lhs, rhs) {
            // Both NULL on this column is a tie — fall through to the next
            // sort key, not a forced Less/Greater.
            (ValueItem::Null, ValueItem::Null) => Ordering::Equal,
            // Only one side is NULL: which side it's on decides the
            // verdict, not just null_first alone — self holding NULL and
            // other holding NULL need opposite answers for the same
            // null_first setting, or cmp(a,b)/cmp(b,a) stop being exact
            // opposites (a real Ord violation: sort()/BinaryHeap both
            // assume that).
            (ValueItem::Null, _) => {
                if field.null_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, ValueItem::Null) => {
                if field.null_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            _ => {
                if field.asc {
                    lhs.cmp(rhs)
                } else {
                    rhs.cmp(lhs)
                }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

impl<'a> Ord for CrateItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_by_fields(&self.key, &other.key, self.order)
    }
}

impl<'a> PartialOrd for CrateItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for CrateItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for CrateItem<'a> {}

#[cfg(test)]
mod tests {
    use store::valueitem::{IndexKey, ValueItem};

    use super::*;
    use crate::source::test_support::{VecSource, drain};

    // Only asc/null_first/index matter to CrateItem::cmp — expr is never
    // read by it (that's SortSource::next's own concern, once it's
    // implemented), so a dummy literal is fine here.
    fn field(index: usize, asc: bool, null_first: bool) -> SortField {
        SortField {
            asc,
            null_first,
            index,
        }
    }

    fn key(values: &[ValueItem]) -> IndexKey {
        IndexKey::new_from(values).unwrap()
    }

    fn item<'a>(key: &IndexKey, order: &'a Vec<SortField>) -> CrateItem<'a> {
        CrateItem {
            key: key.clone(),
            order,
        }
    }

    #[test]
    fn test_single_column_ascending() {
        let order = vec![field(0, true, false)];
        let a = key(&[ValueItem::Integer(1)]);
        let b = key(&[ValueItem::Integer(2)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Less);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Greater);
    }

    #[test]
    fn test_single_column_descending() {
        let order = vec![field(0, false, false)];
        let a = key(&[ValueItem::Integer(1)]);
        let b = key(&[ValueItem::Integer(2)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Greater);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Less);
    }

    #[test]
    fn test_nulls_first_sorts_null_before_non_null_regardless_of_which_side_is_null() {
        let order = vec![field(0, true, true)];
        let null_key = key(&[ValueItem::Null]);
        let val_key = key(&[ValueItem::Integer(1)]);
        assert_eq!(
            item(&null_key, &order).cmp(&item(&val_key, &order)),
            Ordering::Less,
            "null should sort first"
        );
        assert_eq!(
            item(&val_key, &order).cmp(&item(&null_key, &order)),
            Ordering::Greater,
            "compared the other way round, the non-null side must consistently be Greater"
        );
    }

    #[test]
    fn test_nulls_last_sorts_null_after_non_null_regardless_of_which_side_is_null() {
        let order = vec![field(0, true, false)];
        let null_key = key(&[ValueItem::Null]);
        let val_key = key(&[ValueItem::Integer(1)]);
        assert_eq!(
            item(&null_key, &order).cmp(&item(&val_key, &order)),
            Ordering::Greater,
            "null should sort last"
        );
        assert_eq!(
            item(&val_key, &order).cmp(&item(&null_key, &order)),
            Ordering::Less
        );
    }

    #[test]
    fn test_two_nulls_on_the_same_column_are_a_tie() {
        let order = vec![field(0, true, true)];
        let a = key(&[ValueItem::Null]);
        let b = key(&[ValueItem::Null]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Equal);
    }

    #[test]
    fn test_cmp_is_antisymmetric_for_every_null_combination() {
        for null_first in [true, false] {
            let order = vec![field(0, true, null_first)];
            let null_key = key(&[ValueItem::Null]);
            let val_key = key(&[ValueItem::Integer(1)]);
            let forward = item(&null_key, &order).cmp(&item(&val_key, &order));
            let backward = item(&val_key, &order).cmp(&item(&null_key, &order));
            assert_eq!(
                backward,
                forward.reverse(),
                "cmp(a,b) and cmp(b,a) must be exact opposites (null_first={null_first})"
            );
        }
    }

    #[test]
    fn test_multi_column_secondary_breaks_tie_on_primary() {
        // ORDER BY col0 ASC, col1 DESC
        let order = vec![field(0, true, false), field(1, false, false)];
        let a = key(&[ValueItem::Integer(1), ValueItem::Integer(10)]);
        let b = key(&[ValueItem::Integer(1), ValueItem::Integer(20)]);
        // Same col0 (tie) -> col1 DESC means the larger col1 sorts first.
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Greater);
        assert_eq!(item(&b, &order).cmp(&item(&a, &order)), Ordering::Less);
    }

    #[test]
    fn test_multi_column_primary_decides_when_it_differs() {
        let order = vec![field(0, true, false), field(1, false, false)];
        let a = key(&[ValueItem::Integer(1), ValueItem::Integer(999)]);
        let b = key(&[ValueItem::Integer(2), ValueItem::Integer(1)]);
        assert_eq!(item(&a, &order).cmp(&item(&b, &order)), Ordering::Less);
    }

    #[test]
    fn test_sorting_a_vec_end_to_end_matches_expected_order() {
        let order = vec![field(0, true, true)]; // ASC, NULLS FIRST
        let keys = [
            key(&[ValueItem::Integer(3)]),
            key(&[ValueItem::Null]),
            key(&[ValueItem::Integer(1)]),
            key(&[ValueItem::Integer(2)]),
        ];
        let mut items: Vec<CrateItem> = keys.iter().map(|k| item(k, &order)).collect();
        items.sort();
        let sorted_vals: Vec<&ValueItem> = items.iter().map(|it| &it.key.values()[0]).collect();
        assert_eq!(
            sorted_vals,
            vec![
                &ValueItem::Null,
                &ValueItem::Integer(1),
                &ValueItem::Integer(2),
                &ValueItem::Integer(3)
            ]
        );
    }

    // Pinned to MemFile, matching this codebase's usual test convention
    // (a fresh in-memory Db, cheap, isolated) — nothing in these tests
    // exercises `db` itself yet (it's only used by the `else { todo!() }`
    // unlimited-sort branch, for building Runs), so any valid handle
    // will do.
    fn sort_source(
        source: Box<dyn Source>,
        sort_fields: Vec<SortField>,
        limit: usize,
    ) -> SortSource<store::memfile::MemFile> {
        // MemFile hands back a fresh, independent in-memory buffer per
        // create() call regardless of name, so a fixed literal is fine.
        let db = store::db::Db::<store::memfile::MemFile>::create("sort_test").unwrap();
        SortSource {
            source,
            sort_fields,
            limit: Some(limit),
            results: None,
            db,
            mem: QueryMemory::new(1024),
            progress: None,
            sort_time: 0,
            next_time: 0,
        }
    }

    fn str_row(rows: &[&str]) -> Vec<Vec<ValueItem>> {
        rows.iter()
            .map(|s| vec![ValueItem::Str((s.to_string(), s.len() as u32))])
            .collect()
    }

    #[test]
    fn test_sort_source_limit_equal_to_row_count_returns_every_row_in_order() {
        // Regression test: CrateHeap::push used to evict as soon as the
        // heap merely *reached* `limit` (not exceeded it), so a
        // limit-equals-row-count query — the ordinary case for a small
        // table — silently lost one row every time.
        let source: Box<dyn Source> =
            Box::new(VecSource::new(&["name"], str_row(&["raj", "kav", "gan"])));
        let mut sort = sort_source(source, vec![field(0, true, false)], 3);
        assert_eq!(drain(&mut sort), str_row(&["gan", "kav", "raj"]));
    }

    #[test]
    fn test_sort_source_limit_smaller_than_row_count_keeps_the_smallest_asc() {
        let source: Box<dyn Source> =
            Box::new(VecSource::new(&["name"], str_row(&["raj", "kav", "gan"])));
        let mut sort = sort_source(source, vec![field(0, true, false)], 2);
        assert_eq!(drain(&mut sort), str_row(&["gan", "kav"]));
    }

    #[test]
    fn test_sort_source_returns_rows_in_ascending_order_not_reversed() {
        // Regression test: into_sorted_vec() is ascending, but next()
        // used to read it back with Vec::pop() (removes from the end)
        // without reversing first, so results came out descending.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["n"],
            vec![
                vec![ValueItem::Integer(3)],
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
            ],
        ));
        let mut sort = sort_source(source, vec![field(0, true, false)], 3);
        assert_eq!(
            drain(&mut sort),
            vec![
                vec![ValueItem::Integer(1)],
                vec![ValueItem::Integer(2)],
                vec![ValueItem::Integer(3)],
            ]
        );
    }

    #[test]
    fn test_sort_field_index_can_point_into_a_wider_row() {
        // Regression test for the actual reported crash: `SELECT *
        // FROM t1 ORDER BY name` on a 3-column table has one SortField
        // but a 3-value row per record — the field's own `index` (not
        // its position among the sort fields) must be used to find its
        // value in the row.
        let source: Box<dyn Source> = Box::new(VecSource::new(
            &["id", "name"],
            vec![
                vec![ValueItem::Integer(1), ValueItem::Str(("raj".into(), 3))],
                vec![ValueItem::Integer(2), ValueItem::Str(("kav".into(), 3))],
                vec![ValueItem::Integer(5), ValueItem::Str(("gan".into(), 3))],
            ],
        ));
        // ORDER BY name (row position 1), not id.
        let mut sort = sort_source(source, vec![field(1, true, false)], 3);
        let rows = drain(&mut sort);
        let names: Vec<&ValueItem> = rows.iter().map(|r| &r[1]).collect();
        assert_eq!(
            names,
            vec![
                &ValueItem::Str(("gan".into(), 3)),
                &ValueItem::Str(("kav".into(), 3)),
                &ValueItem::Str(("raj".into(), 3)),
            ]
        );
    }

    // Regression test: build_initial_runs used to sort and flush one
    // records_per_page-sized batch at a time, in a fresh BinaryHeap per
    // page — so each individual PAGE came out internally sorted, but
    // nothing kept page 2's rows >= page 1's, meaning a run spanning
    // more than one page was NOT actually a single sorted sequence
    // (merge_two/merge_runs assume it is). Every existing test above
    // this one deliberately used a one-page-per-run memory budget
    // (`probe_data_size()`, "a budget of exactly one page forces a new
    // run on every flush"), which sidesteps the bug entirely — a run
    // with exactly one page is trivially "sorted across its own
    // pages". This test instead gives a run enough budget for several
    // pages (`data_size * 4`), on a 2-column row sorted by its
    // non-zero-index column (the actual GROUP BY key position for any
    // query that isn't `SELECT` on a single-column table), and checks
    // the full output is globally sorted — this is exactly the shape
    // that silently fragmented GROUP BY into far more groups than
    // actually existed once real query data grew past one page.
    #[test]
    fn test_a_run_spanning_multiple_pages_is_globally_sorted_not_just_page_locally() {
        let data_size = probe_data_size();
        let record_size = crate::datatype::DataType::Integer.size() * 2;
        let records_per_page = data_size / record_size;
        // Enough rows to span several multi-page runs (not just one),
        // so both build_initial_runs' own per-run sort AND
        // merge_runs/merge_two's cross-run merge get exercised.
        let total_rows = records_per_page * 4 * 3;

        let rows: Vec<Vec<ValueItem>> = (0..total_rows as i64)
            .map(|i| vec![ValueItem::Integer(i), ValueItem::Integer(i % 7)])
            .collect();
        let source: Box<dyn Source> = Box::new(VecSource::new(&["id", "cat"], rows));
        let db = store::db::Db::<store::memfile::MemFile>::create("multi_page_run_test").unwrap();
        // data_size * 4: enough reservable budget for a run to grow to
        // several pages before memory forces a new one — the one thing
        // every other unlimited-sort test above avoids.
        let mem = QueryMemory::new(data_size * 4);
        let mut sort = SortSource::with_fields(source, db, mem, &[1]).unwrap();
        let out = drain(&mut sort);
        assert_eq!(out.len(), total_rows);
        let vals: Vec<i64> = out
            .iter()
            .map(|r| match &r[1] {
                ValueItem::Integer(i) => *i,
                _ => panic!("expected Integer"),
            })
            .collect();
        assert!(
            vals.is_sorted(),
            "output must be globally sorted by column 1, not just sorted within each page"
        );
    }

    // ---- unlimited sort (no LIMIT clause): build_initial_runs, merge_runs,
    // merge_two, and the num_passes calculation that ties them together ----

    // `limit: None` is what routes SortSource::next through build_sort's
    // external-merge-sort path instead of CrateHeap's in-memory top-K —
    // the one thing sort_source()/`Some(limit)` above never exercises.
    fn unlimited_sort_source(
        rows: Vec<Vec<ValueItem>>,
        mem_limit: usize,
    ) -> SortSource<store::memfile::MemFile> {
        let source: Box<dyn Source> = Box::new(VecSource::new(&["n"], rows));
        let db = store::db::Db::<store::memfile::MemFile>::create("unlimited_sort_test").unwrap();
        SortSource {
            source,
            sort_fields: vec![field(0, true, false)],
            limit: None,
            results: None,
            db,
            mem: QueryMemory::new(mem_limit),
            progress: None,
            sort_time: 0,
            next_time: 0,
        }
    }

    // record_size as SortSource itself computes it for a single-column
    // VecSource: every VecSource field is a Field::from(name), which is
    // always a Str(DEFAULT_VAR_SIZE) column regardless of what ValueItem
    // kind actually ends up in the rows (VecSource has no schema inference
    // — see its own `fields()`), so this is the one true record_size for
    // every test below, not just an estimate.
    fn probe_record_size() -> usize {
        crate::datatype::DataType::Str(crate::constant::DEFAULT_VAR_SIZE as u32).size()
    }

    // A run's own data_size() only depends on the (fixed) database page
    // size, so a throwaway run from a fresh Db reports the same value
    // every SortSource-owned run in these tests will too.
    fn probe_data_size() -> usize {
        let db = store::db::Db::<store::memfile::MemFile>::create("probe_data_size").unwrap();
        db.create_run().unwrap().data_size() as usize
    }

    fn int_rows(n: i64) -> Vec<Vec<ValueItem>> {
        // Descending input — sorting ascending has to actually reorder
        // everything, not just pass an already-sorted source through.
        (0..n).rev().map(|i| vec![ValueItem::Integer(i)]).collect()
    }

    fn run_contents(run: &Run<store::memfile::MemFile>) -> Vec<IndexKey> {
        let mut cursor = run.cursor().unwrap();
        let mut out = vec![];
        while let Some(t) = cursor.next().unwrap() {
            out.extend(SortSource::<store::memfile::MemFile>::from_tuple(t).unwrap());
        }
        out
    }

    #[test]
    fn test_build_initial_runs_stays_in_memory_when_budget_is_generous() {
        let record_size = probe_record_size();
        let mut sort = unlimited_sort_source(int_rows(5), 10_000_000);
        let built = sort.build_initial_runs(record_size).unwrap();
        let InitialBuild::InMemory(sorted) = built else {
            panic!("a generous budget must never spill to a Run");
        };
        assert_eq!(
            sorted.iter().map(|k| k.values()[0].clone()).collect::<Vec<_>>(),
            (0..5).map(ValueItem::Integer).collect::<Vec<_>>(),
            "the in-memory result must already be sorted ascending"
        );
        // The one throwaway Run created just to learn data_size()/
        // records_per_page (never written to) is dropped before returning
        // — nothing this path does allocates a page that outlives the call.
        assert_eq!(
            sort.db.stats().temp.live_pages,
            0,
            "the in-memory path must not leave any Run page allocated"
        );
    }

    #[test]
    fn test_build_initial_runs_falls_back_to_a_run_once_the_budget_is_exceeded() {
        let record_size = probe_record_size();
        let data_size = probe_data_size();
        // One page's worth of reservation room: the initial reservation
        // succeeds, but growing past it must fail and force a spill.
        let mut sort = unlimited_sort_source(int_rows(9_999), data_size);
        let built = sort.build_initial_runs(record_size).unwrap();
        assert!(
            matches!(built, InitialBuild::Spilled(_)),
            "exceeding the budget must spill to a Run, not stay in memory"
        );
    }

    #[test]
    fn test_build_initial_runs_drops_no_rows_across_a_forced_multi_run_split() {
        // Regression test: build_initial_runs used to discard the exact
        // row that filled the in-memory heap on every flush (the row
        // that triggered `heap.len() == records_per_page` was never
        // pushed into either the flushed batch or the next heap).
        let record_size = probe_record_size();
        let data_size = probe_data_size();
        let records_per_page = data_size / record_size;
        let total_rows = records_per_page * 4;

        // A budget of exactly one page forces a new run on every flush.
        let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
        let InitialBuild::Spilled(runs) = sort.build_initial_runs(record_size).unwrap() else {
            panic!("a one-page budget over 4 pages of rows must spill");
        };

        let total_recovered: usize = runs.runs.iter().map(|r| run_contents(r).len()).sum();
        assert_eq!(
            runs.count, total_rows,
            "SortedRuns.count must reflect every row consumed from the source"
        );
        assert_eq!(
            total_recovered, total_rows,
            "every row fed in must be recoverable by reading every returned run"
        );
    }

    #[test]
    fn test_build_initial_runs_gives_every_run_the_same_page_budget() {
        // Regression test: a run past the first used to end up with one
        // MORE page than `pages_per_run` (the page-count budget derived
        // from how much fit in the first run before memory ran out),
        // since a page got added unconditionally before the "is this run
        // full yet" check ever compared against the just-written page.
        let record_size = probe_record_size();
        let data_size = probe_data_size();
        let records_per_page = data_size / record_size;
        let total_rows = records_per_page * 4;

        let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
        let InitialBuild::Spilled(runs) = sort.build_initial_runs(record_size).unwrap() else {
            panic!("a one-page budget over 4 pages of rows must spill");
        };

        // Every run except a possible smaller trailing leftover should
        // hold exactly one page's worth (the budget here is one page).
        for (i, run) in runs.runs.iter().enumerate() {
            let n = run_contents(run).len();
            assert!(
                n == records_per_page || (i == runs.runs.len() - 1 && n <= records_per_page),
                "run {i} has {n} records, expected {records_per_page} (or a smaller trailing leftover)"
            );
        }
    }

    #[test]
    fn test_num_passes_reduces_any_run_count_to_exactly_one() {
        // Exercises build_sort's num_passes calculation indirectly across
        // several run counts (odd/even/exact/leftover) — if it's wrong in
        // either direction, build_sort's own `assert!(run.runs.len() ==
        // 1)` fails: too few passes leaves more than one run, and
        // merge_runs itself asserts `runs.len() > 1` on entry, so one
        // pass too many would panic outright rather than silently no-op.
        let record_size = probe_record_size();
        let data_size = probe_data_size();
        let records_per_page = data_size / record_size;

        for extra_pages in [0usize, 1, 2, 3, 4, 7] {
            let total_rows = records_per_page * (extra_pages + 1);
            let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
            let out = drain(&mut sort);
            assert_eq!(
                out.len(),
                total_rows,
                "extra_pages={extra_pages}: row count must be preserved"
            );
        }
    }

    // A handful of end-to-end shapes covering: an exact multiple of the
    // per-run page budget (4 runs), an odd initial run count (3 runs,
    // exercising merge_runs's leftover-run carry-over), and a leftover
    // partial run on top of several full ones (5 runs + a short 6th).
    // Each checks the FULL output is complete and correctly ordered, not
    // just the right length — the specific thing that used to fail: every
    // row present, but merged chunks landing in the wrong relative order
    // whenever the two runs being merged didn't have matching page counts.
    #[test]
    fn test_unlimited_sort_end_to_end_exact_page_multiple() {
        let data_size = probe_data_size();
        let record_size = probe_record_size();
        let total_rows = (data_size / record_size) * 4;
        let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
        let out = drain(&mut sort);
        let vals: Vec<i64> = out
            .iter()
            .map(|r| match &r[0] {
                ValueItem::Integer(i) => *i,
                _ => panic!("expected Integer"),
            })
            .collect();
        assert_eq!(vals, (0..total_rows as i64).collect::<Vec<_>>());
    }

    #[test]
    fn test_unlimited_sort_end_to_end_odd_run_count() {
        let data_size = probe_data_size();
        let record_size = probe_record_size();
        let total_rows = (data_size / record_size) * 3;
        let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
        let out = drain(&mut sort);
        let vals: Vec<i64> = out
            .iter()
            .map(|r| match &r[0] {
                ValueItem::Integer(i) => *i,
                _ => panic!("expected Integer"),
            })
            .collect();
        assert_eq!(vals, (0..total_rows as i64).collect::<Vec<_>>());
    }

    #[test]
    fn test_unlimited_sort_end_to_end_uneven_leftover_partial_run() {
        let data_size = probe_data_size();
        let record_size = probe_record_size();
        let total_rows = (data_size / record_size) * 5 + 137;
        let mut sort = unlimited_sort_source(int_rows(total_rows as i64), data_size);
        let out = drain(&mut sort);
        let vals: Vec<i64> = out
            .iter()
            .map(|r| match &r[0] {
                ValueItem::Integer(i) => *i,
                _ => panic!("expected Integer"),
            })
            .collect();
        assert_eq!(vals, (0..total_rows as i64).collect::<Vec<_>>());
    }

    // The property sort-based GROUP BY actually depends on: every row for
    // a given key ends up contiguous in the final output, even when that
    // key's own rows are numerous enough to span multiple pages within
    // one initial run AND get split across several separate initial runs
    // (built independently from arrival-order batches, with no idea the
    // batches share a key) that later have to be merged back together.
    #[test]
    fn test_unlimited_sort_keeps_a_key_contiguous_when_it_spans_multiple_pages_and_runs() {
        let data_size = probe_data_size();
        let record_size = probe_record_size();
        let records_per_page = data_size / record_size;

        // Each count is deliberately not a multiple of records_per_page,
        // and well over one page, so every key's own rows straddle at
        // least one page boundary within whatever initial run(s) they
        // land in. Fed key 2, then 0, then 1 (not already sorted), so
        // producing ascending output requires genuine cross-run merging,
        // not runs that already happened to land in the right order.
        let counts: [(i64, usize); 3] = [
            (2, records_per_page * 2 + 137),
            (0, records_per_page + 50),
            (1, records_per_page * 3 + 9),
        ];

        let mut rows: Vec<Vec<ValueItem>> = vec![];
        for (key, count) in counts {
            for _ in 0..count {
                rows.push(vec![ValueItem::Integer(key)]);
            }
        }

        // A one-page memory budget forces multiple initial runs (see
        // build_initial_runs), so a single key's ~1000+ rows are
        // virtually guaranteed to be split across several of them.
        let mut sort = unlimited_sort_source(rows, data_size);
        let out = drain(&mut sort);

        let total: usize = counts.iter().map(|(_, c)| c).sum();
        assert_eq!(out.len(), total, "no rows dropped or duplicated");

        let vals: Vec<i64> = out
            .iter()
            .map(|r| match &r[0] {
                ValueItem::Integer(i) => *i,
                _ => panic!("expected Integer"),
            })
            .collect();
        assert!(
            vals.is_sorted(),
            "output must be fully ascending, not just grouped"
        );

        // Contiguity: once the sort moves past a key, it must never
        // reappear later — the exact thing a streaming sort-then-group
        // aggregation depends on.
        let mut seen_keys = vec![];
        let mut prev = None;
        for &v in &vals {
            if Some(v) != prev {
                assert!(
                    !seen_keys.contains(&v),
                    "key {v} reappeared after the sort had already moved past it — \
                     its rows got split apart instead of staying contiguous"
                );
                seen_keys.push(v);
                prev = Some(v);
            }
        }

        // Every row survived, per key.
        for (key, count) in counts {
            let actual = vals.iter().filter(|&&v| v == key).count();
            assert_eq!(
                actual, count,
                "key {key}: expected {count} rows, got {actual}"
            );
        }
    }

    // An empty input used to trip build_sort's `assert!(runs.len() == 1)`.
    #[test]
    fn test_an_unlimited_sort_of_an_empty_input_yields_nothing() {
        let mut sort = unlimited_sort_source(vec![], 1 << 20);
        assert!(sort.next().unwrap().is_none());
        assert!(sort.next().unwrap().is_none(), "and stays exhausted");
    }

    #[test]
    fn test_reset_makes_a_drained_sort_yield_its_rows_again() {
        let rows: Vec<Vec<ValueItem>> = [3, 1, 2].iter().map(|n| vec![ValueItem::Integer(*n)]).collect();
        let mut sort = unlimited_sort_source(rows, 1 << 20);
        let mut drain = |s: &mut SortSource<store::memfile::MemFile>| {
            let mut out = vec![];
            while let Some(r) = s.next().unwrap() {
                out.push(r.values().to_vec());
            }
            out
        };
        let first = drain(&mut sort);
        assert_eq!(first.len(), 3);
        sort.reset().unwrap();
        assert_eq!(drain(&mut sort), first);
    }
}

