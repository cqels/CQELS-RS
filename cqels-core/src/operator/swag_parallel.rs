//! Tier-2 batch-parallel join aggregation — layer 5e of the SWAG
//! join port.
//!
//! Ports cqels-java's `ParallelJoinAggregator` from
//! `org.cqels.operator.aggregate.swag.join`. Takes the two
//! per-side payload [`TimeIndex`]es out of a
//! [`WindowJoinState`](crate::operator::WindowJoinState) (or any
//! other source) and computes the join aggregate from scratch in
//! parallel — a fallback path that complements Layer 2's
//! incremental (Tier-1) F-IVM updates.
//!
//! ## When to use
//!
//! - **`BATCH_ON_CLOSE` semantics.** Run once per window-fire and
//!   replace the running aggregate with the recomputed value.
//! - **Verification.** Cross-check the incremental aggregate
//!   against a fresh batch computation during tests / sanity runs.
//! - **Recovery.** Rebuild the aggregate after state corruption
//!   or a checkpoint restore.
//!
//! ## Algorithm
//!
//! 1. Iterate left-side entries (`TimeIndexEntry<PL>`) in
//!    parallel.
//! 2. For each: query the right [`TimeIndex`] for the matching
//!    interval `[ts + lower, ts + upper]`, fold the right
//!    payload-count map into a single ring element via the
//!    spec's `add_right`, multiply with the left payload through
//!    `spec.multiply_left`, then scale by the left bag count
//!    (multiplicity) via repeated `spec.add`.
//! 3. Combine partial sums in parallel via rayon's `reduce`.
//!
//! ## Differences vs Java
//!
//! - Java uses `Fork/Join` + a hand-rolled `ExponentialSplitter`.
//!   The Rust port uses `rayon::iter::ParallelIterator` —
//!   rayon's work-stealing scheduler already handles adaptive
//!   chunking, so the `growthFactor` parameter from Java is
//!   absent here. A `parallel_threshold` knob still controls
//!   when to fall back to a sequential loop (skip the rayon
//!   setup cost on tiny inputs).
//! - Single-threaded `TimeIndex` (per the project's existing
//!   single-thread-per-operator model). The aggregator takes
//!   `&TimeIndex` references; the caller is responsible for
//!   ensuring no concurrent mutation happens during the parallel
//!   walk.

use rayon::prelude::*;

use crate::operator::swag_ctrie::TimeIndex;
use crate::operator::swag_join_state::{IntervalBounds, JoinAggregateSpec};

/// Default split threshold — below this many left entries the
/// aggregator falls back to a sequential reduce. Matches Java's
/// default of 1000.
pub const DEFAULT_PARALLEL_THRESHOLD: usize = 1000;

/// Tier-2 batch-parallel aggregator over the two payload
/// [`TimeIndex`]es of a pairwise interval join.
///
/// Generic over a [`JoinAggregateSpec`] — the same ring algebra
/// already used by [`crate::operator::WindowJoinState`].
pub struct ParallelJoinAggregator<S: JoinAggregateSpec> {
    spec: S,
    parallel_threshold: usize,
}

impl<S: JoinAggregateSpec> ParallelJoinAggregator<S> {
    /// Builds an aggregator with [`DEFAULT_PARALLEL_THRESHOLD`].
    pub fn new(spec: S) -> Self {
        Self::with_threshold(spec, DEFAULT_PARALLEL_THRESHOLD)
    }

    /// Builds an aggregator with an explicit parallel/sequential
    /// crossover threshold (number of left entries).
    pub fn with_threshold(spec: S, parallel_threshold: usize) -> Self {
        Self {
            spec,
            parallel_threshold: parallel_threshold.max(1),
        }
    }

    /// Spec borrowed reference.
    pub fn spec(&self) -> &S {
        &self.spec
    }

    /// Configured threshold.
    pub fn parallel_threshold(&self) -> usize {
        self.parallel_threshold
    }

    /// Computes the join aggregate from scratch given the two
    /// per-side payload time indexes and the interval bounds.
    ///
    /// Returns `spec.zero()` if either side is empty.
    pub fn aggregate(
        &self,
        left: &TimeIndex<S::PL>,
        right: &TimeIndex<S::PR>,
        bounds: IntervalBounds,
    ) -> S::V
    where
        S: Sync,
        S::PL: Sync,
        S::PR: Sync,
        S::V: Send,
    {
        if left.is_empty() || right.is_empty() {
            return self.spec.zero();
        }

        // Collect left entries up-front — we need a `Sync`
        // snapshot rayon can chunk over. The total payload size
        // is `O(distinct timestamps on the left)`, typically much
        // smaller than the bag size.
        //
        // `query_range_entries(MIN, MAX)` is the simplest way to
        // produce the same Vec<TimeIndexEntry<PL>> shape Java's
        // splitter consumes. The range is total so no events get
        // filtered out at this step.
        let left_entries = left.query_range_entries(i64::MIN, i64::MAX);

        // Fall back to a sequential reduce on small inputs to
        // skip rayon's chunking + thread-pool setup.
        if left_entries.len() < self.parallel_threshold {
            return left_entries
                .into_iter()
                .map(|entry| self.lift_entry(right, &entry, bounds))
                .fold(self.spec.zero(), |acc, partial| {
                    self.spec.add(&acc, &partial)
                });
        }

        // Parallel path. Each entry is independent: we read the
        // right index (shared, immutable) and emit a partial V;
        // `reduce` folds with `spec.add`. `par_iter()` yields
        // `&TimeIndexEntry<PL>` references, which `lift_entry`
        // accepts directly — saves the per-element clone of the
        // owned entry that `into_par_iter()` would otherwise
        // require here.
        left_entries
            .par_iter()
            .map(|entry| self.lift_entry(right, entry, bounds))
            .reduce(|| self.spec.zero(), |a, b| self.spec.add(&a, &b))
    }

    /// Per-left-entry contribution. Mirrors Java's
    /// `JoinSwagOp.lift(entry)`:
    ///
    /// 1. Query the right index for the matching interval.
    /// 2. Aggregate the right range's bag payloads into a single
    ///    ring element via `add_right` + `zero_right`.
    /// 3. Multiply the left payload by that aggregate.
    /// 4. Scale by the left bag count (multiplicity).
    fn lift_entry(
        &self,
        right: &TimeIndex<S::PR>,
        entry: &crate::operator::swag_ctrie::TimeIndexEntry<S::PL>,
        bounds: IntervalBounds,
    ) -> S::V {
        let ts = entry.timestamp;
        let right_start = ts + bounds.lower;
        let right_end = ts + bounds.upper;

        let right_range = right.query_range(right_start, right_end);
        if right_range.is_empty() {
            return self.spec.zero();
        }

        // Fold right-side payload bag into a single PR.
        let mut right_agg = self.spec.zero_right();
        for (payload, count) in &right_range.payload_counts {
            for _ in 0..*count {
                right_agg = self.spec.add_right(&right_agg, payload);
            }
        }

        // Single-pair delta then scaled by left bag count.
        let single = self.spec.multiply_left(&entry.payload, &right_agg);
        let mut acc = single.clone();
        for _ in 1..entry.count {
            acc = self.spec.add(&acc, &single);
        }
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::swag_join_state::{IntervalBounds, JoinAggregateSpec, WindowJoinState};

    /// `COUNT(*)` spec — same as in `swag_join_state` tests.
    /// Each side's payload is `1`, aggregate is `i64`.
    #[derive(Clone, Copy)]
    struct CountSpec;
    impl JoinAggregateSpec for CountSpec {
        type L = i32;
        type R = i32;
        type PL = i64;
        type PR = i64;
        type V = i64;
        fn left_payload(&self, _: &i32) -> i64 {
            1
        }
        fn right_payload(&self, _: &i32) -> i64 {
            1
        }
        fn zero(&self) -> i64 {
            0
        }
        fn add(&self, a: &i64, b: &i64) -> i64 {
            a + b
        }
        fn negate(&self, v: &i64) -> i64 {
            -v
        }
        fn multiply_left(&self, l: &i64, r: &i64) -> i64 {
            l * r
        }
        fn multiply_right(&self, l: &i64, r: &i64) -> i64 {
            l * r
        }
        fn zero_left(&self) -> i64 {
            0
        }
        fn zero_right(&self) -> i64 {
            0
        }
        fn add_left(&self, a: &i64, b: &i64) -> i64 {
            a + b
        }
        fn add_right(&self, a: &i64, b: &i64) -> i64 {
            a + b
        }
        fn name(&self) -> &str {
            "COUNT"
        }
    }

    /// Helper: build a `WindowJoinState` populated with `lefts`
    /// and `rights`, then run the parallel aggregator over its
    /// indexes and return the incrementally-maintained vs
    /// parallel-recomputed pair.
    fn run_both(bounds: IntervalBounds, lefts: &[(i64, i32)], rights: &[(i64, i32)]) -> (i64, i64) {
        let mut state = WindowJoinState::new(CountSpec, bounds);
        for (ts, v) in lefts {
            state.insert_left(*ts, *v);
        }
        for (ts, v) in rights {
            state.insert_right(*ts, *v);
        }
        let inc = state.aggregate();

        let agg = ParallelJoinAggregator::new(CountSpec);
        let par = agg.aggregate(
            state.left_time_index().expect("cross-product mode"),
            state.right_time_index().expect("cross-product mode"),
            bounds,
        );

        (inc, par)
    }

    #[test]
    fn empty_inputs_return_zero() {
        let agg = ParallelJoinAggregator::new(CountSpec);
        let left: TimeIndex<i64> = TimeIndex::new();
        let right: TimeIndex<i64> = TimeIndex::new();
        assert_eq!(agg.aggregate(&left, &right, IntervalBounds::new(0, 10)), 0);
    }

    #[test]
    fn matches_incremental_on_small_workload() {
        let (inc, par) = run_both(
            IntervalBounds::new(0, 10),
            &[(0, 1), (1, 2), (2, 3)],
            &[(5, 1), (5, 2), (8, 3)],
        );
        // Each left pairs with each right within [ts, ts+10]:
        // ts=0 paired with rights at [0,10] = 3 rights
        // ts=1 paired with rights at [1,11] = 3 rights
        // ts=2 paired with rights at [2,12] = 3 rights
        // Total: 9 pairs.
        assert_eq!(inc, par, "parallel must match incremental");
        assert_eq!(inc, 9);
    }

    #[test]
    fn matches_incremental_on_workload_above_threshold() {
        // Force the parallel path: build 100 lefts + 100 rights
        // with a threshold of 8.
        let mut lefts = Vec::new();
        let mut rights = Vec::new();
        for i in 0..100 {
            lefts.push((i as i64, i));
            rights.push(((i + 10) as i64, i));
        }
        let mut state = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 1000));
        for (ts, v) in &lefts {
            state.insert_left(*ts, *v);
        }
        for (ts, v) in &rights {
            state.insert_right(*ts, *v);
        }

        let agg = ParallelJoinAggregator::with_threshold(CountSpec, 8);
        let par = agg.aggregate(
            state.left_time_index().expect("cross-product mode"),
            state.right_time_index().expect("cross-product mode"),
            IntervalBounds::new(0, 1000),
        );
        assert_eq!(state.aggregate(), par);
    }

    #[test]
    fn threshold_one_still_yields_correct_aggregate() {
        // Threshold=1 always goes through the rayon path.
        let agg = ParallelJoinAggregator::with_threshold(CountSpec, 1);
        let mut state = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 100));
        state.insert_left(0, 1);
        state.insert_right(5, 2);
        let par = agg.aggregate(
            state.left_time_index().expect("cross-product mode"),
            state.right_time_index().expect("cross-product mode"),
            IntervalBounds::new(0, 100),
        );
        assert_eq!(par, 1);
    }

    #[test]
    fn key_indexed_state_exposes_no_time_indexes() {
        let mut state: WindowJoinState<CountSpec, i32> = WindowJoinState::with_keys(
            CountSpec,
            IntervalBounds::new(0, 100),
            |l: &i32| *l,
            |r: &i32| *r,
        );
        state.insert_left(0, 1);
        // Key-indexed mode doesn't expose flat TimeIndexes; the
        // aggregator only supports the cross-product path.
        assert!(state.left_time_index().is_none());
        assert!(state.right_time_index().is_none());
    }
}
