//! Key + window-partitioned wrapper over [`WindowJoinState`] —
//! layer 5d of the SWAG join port.
//!
//! Ports cqels-java's `WindowedJoinAggregateOperator` from
//! `org.cqels.operator.aggregate.swag.join`. Builds on Layer 2
//! (`WindowJoinState` for a single pairwise join over one
//! `(key, window)` partition) by adding the partitioning layer:
//!
//! - Each event is routed to a `(join_key, window_id)` partition.
//! - Each partition owns its own [`WindowJoinState`] instance.
//! - `process_left` / `process_right` insert into the per-partition
//!   state and emit per-event aggregate results.
//! - `fire_window(key, window)` returns the running aggregate for
//!   that partition (or `spec.zero()` when no state exists yet).
//! - `purge_window(key, window)` drops the partition's state.
//!
//! ## Window assignment
//!
//! The operator is parameterized over a [`WindowAssigner`] —
//! a small trait that maps an event timestamp to one or more
//! window identifiers. We ship one canonical implementation,
//! [`TumblingWindowAssigner`], that splits timestamps into
//! fixed-size buckets. Sliding / session assigners can be plugged
//! in by callers without touching this module.
//!
//! ## What's not here (deferred)
//!
//! - Java's Reactor `Mono`/`Flux` API surface — the Rust port uses
//!   ordinary methods plus a future-friendly streaming
//!   `process(...)` taking a merged update stream (see Layer 4's
//!   [`crate::operator::WindowedJoinAggregator`] for that pattern).
//! - `SwagConfig::ParallelMode` (`BATCH_ON_CLOSE` / `MIXED`) — Layer
//!   5e's [`crate::operator::ParallelJoinAggregator`] is the
//!   recompute path; users can invoke it inside `fire_window` when
//!   needed.
//! - Async batch-fire schedulers (Java's `subscribeOn(scheduler)`) —
//!   the Rust operator is synchronous; an async wrapper would just
//!   spawn `fire_window` onto a runtime.

use std::collections::HashMap;
use std::hash::Hash;

use crate::operator::swag_join_state::{IntervalBounds, JoinAggregateSpec, WindowJoinState};

/// Boxed `Fn(&T) -> K` extractor. Aliased to keep the struct
/// declaration readable (clippy `type_complexity`).
type BoxedExtractor<T, K> = Box<dyn Fn(&T) -> K + Send + Sync + 'static>;

// ─── WindowAssigner ────────────────────────────────────────────────

/// Maps an event timestamp to one or more window identifiers. The
/// identifier type `W` is opaque to the operator — anything
/// `Eq + Hash + Clone` works.
pub trait WindowAssigner<W>: Send + Sync + 'static
where
    W: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Returns the set of windows that the given timestamp belongs
    /// to. For tumbling assignment this is a single window; for
    /// sliding assignment, multiple overlapping windows. An empty
    /// return value means the event is dropped.
    fn assign(&self, timestamp: i64) -> Vec<W>;
}

/// Tumbling-window assigner: each timestamp lands in exactly one
/// window keyed by `(ts / size)`. Window identifiers are `i64`
/// bucket numbers.
#[derive(Clone, Copy, Debug)]
pub struct TumblingWindowAssigner {
    size: i64,
}

impl TumblingWindowAssigner {
    pub fn new(size: i64) -> Self {
        assert!(size > 0, "tumbling window size must be positive");
        Self { size }
    }

    pub fn size(&self) -> i64 {
        self.size
    }
}

impl WindowAssigner<i64> for TumblingWindowAssigner {
    fn assign(&self, timestamp: i64) -> Vec<i64> {
        vec![timestamp.div_euclid(self.size)]
    }
}

// ─── Result envelope ──────────────────────────────────────────────

/// Which side fed an emitted result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Side {
    Left,
    Right,
}

/// One emitted result from the operator's `process_left` /
/// `process_right` paths. Mirrors Java's `JoinAggregateResult`.
#[derive(Clone, Debug)]
pub struct JoinAggregateResult<K, W, V> {
    pub key: K,
    pub window: W,
    pub aggregate: V,
    pub delta: V,
    pub side: Side,
}

// ─── Operator ──────────────────────────────────────────────────────

/// State key combining a join key and window identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StateKey<K, W> {
    key: K,
    window: W,
}

/// Key + window-partitioned streaming wrapper over
/// [`WindowJoinState`]. Each `(key, window)` partition holds its
/// own pairwise interval-join state with independent F-IVM
/// aggregate.
///
/// # Type parameters
///
/// - `S` — Layer 2's [`JoinAggregateSpec`] (ring algebra).
/// - `K` — join key type. Extracted from each event via the
///   per-side key extractors registered at construction.
/// - `W` — opaque window identifier; produced by a
///   [`WindowAssigner`].
///
/// # Construction
///
/// ```ignore
/// let op = WindowedJoinAggregateOperator::new(
///     CountSpec,
///     IntervalBounds::new(0, 60_000),
///     TumblingWindowAssigner::new(300_000),
///     |l: &MyLeft| l.user_id,
///     |r: &MyRight| r.user_id,
/// );
/// ```
pub struct WindowedJoinAggregateOperator<S, K, W>
where
    S: JoinAggregateSpec + Clone,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    W: Eq + Hash + Clone + Send + Sync + 'static,
{
    spec: S,
    bounds: IntervalBounds,
    assigner: Box<dyn WindowAssigner<W>>,
    left_extractor: BoxedExtractor<S::L, K>,
    right_extractor: BoxedExtractor<S::R, K>,
    state_map: HashMap<StateKey<K, W>, WindowJoinState<S>>,
}

impl<S, K, W> WindowedJoinAggregateOperator<S, K, W>
where
    S: JoinAggregateSpec + Clone,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    W: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Builds a new operator.
    pub fn new<A>(
        spec: S,
        bounds: IntervalBounds,
        assigner: A,
        left_extractor: impl Fn(&S::L) -> K + Send + Sync + 'static,
        right_extractor: impl Fn(&S::R) -> K + Send + Sync + 'static,
    ) -> Self
    where
        A: WindowAssigner<W>,
    {
        Self {
            spec,
            bounds,
            assigner: Box::new(assigner),
            left_extractor: Box::new(left_extractor),
            right_extractor: Box::new(right_extractor),
            state_map: HashMap::new(),
        }
    }

    /// Current number of `(key, window)` partitions held in state.
    pub fn partition_count(&self) -> usize {
        self.state_map.len()
    }

    /// Process one left-side event. Returns one result per window
    /// the event was assigned to (typically 1 for tumbling
    /// assignment, ≥1 for sliding).
    pub fn process_left(
        &mut self,
        timestamp: i64,
        element: S::L,
    ) -> Vec<JoinAggregateResult<K, W, S::V>>
    where
        S::L: Clone,
    {
        let key = (self.left_extractor)(&element);
        let windows = self.assigner.assign(timestamp);
        let mut out = Vec::with_capacity(windows.len());
        for window in windows {
            let state_key = StateKey {
                key: key.clone(),
                window: window.clone(),
            };
            let state = self
                .state_map
                .entry(state_key)
                .or_insert_with(|| WindowJoinState::new(self.spec.clone(), self.bounds));
            let delta = state.insert_left(timestamp, element.clone());
            let aggregate = state.aggregate();
            out.push(JoinAggregateResult {
                key: key.clone(),
                window,
                aggregate,
                delta,
                side: Side::Left,
            });
        }
        out
    }

    /// Process one right-side event.
    pub fn process_right(
        &mut self,
        timestamp: i64,
        element: S::R,
    ) -> Vec<JoinAggregateResult<K, W, S::V>>
    where
        S::R: Clone,
    {
        let key = (self.right_extractor)(&element);
        let windows = self.assigner.assign(timestamp);
        let mut out = Vec::with_capacity(windows.len());
        for window in windows {
            let state_key = StateKey {
                key: key.clone(),
                window: window.clone(),
            };
            let state = self
                .state_map
                .entry(state_key)
                .or_insert_with(|| WindowJoinState::new(self.spec.clone(), self.bounds));
            let delta = state.insert_right(timestamp, element.clone());
            let aggregate = state.aggregate();
            out.push(JoinAggregateResult {
                key: key.clone(),
                window,
                aggregate,
                delta,
                side: Side::Right,
            });
        }
        out
    }

    /// Fire a window — return its current incremental aggregate.
    /// Returns `spec.zero()` when no state exists for the
    /// `(key, window)` pair.
    pub fn fire_window(&self, key: &K, window: &W) -> S::V {
        let state_key = StateKey {
            key: key.clone(),
            window: window.clone(),
        };
        match self.state_map.get(&state_key) {
            Some(state) => state.aggregate(),
            None => self.spec.zero(),
        }
    }

    /// Purge a window's state. After this call, subsequent
    /// `fire_window(key, window)` returns `spec.zero()`.
    pub fn purge_window(&mut self, key: &K, window: &W) {
        let state_key = StateKey {
            key: key.clone(),
            window: window.clone(),
        };
        self.state_map.remove(&state_key);
    }

    /// Drop every partition's state. Convenience for tests and
    /// stream-restart paths.
    pub fn clear(&mut self) {
        self.state_map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `COUNT(*)` spec — payload `1`, ring `(i64, +, ·)`.
    #[derive(Clone, Copy)]
    struct CountSpec;
    impl JoinAggregateSpec for CountSpec {
        type L = (i32, i64); // (key, _unused)
        type R = (i32, i64); // (key, _unused)
        type PL = i64;
        type PR = i64;
        type V = i64;
        fn left_payload(&self, _: &Self::L) -> i64 {
            1
        }
        fn right_payload(&self, _: &Self::R) -> i64 {
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

    fn make_op() -> WindowedJoinAggregateOperator<CountSpec, i32, i64> {
        WindowedJoinAggregateOperator::new(
            CountSpec,
            IntervalBounds::new(0, 100),
            TumblingWindowAssigner::new(1000),
            |l: &(i32, i64)| l.0,
            |r: &(i32, i64)| r.0,
        )
    }

    #[test]
    fn tumbling_assigner_groups_by_window_id() {
        let a = TumblingWindowAssigner::new(1000);
        assert_eq!(a.assign(0), vec![0]);
        assert_eq!(a.assign(500), vec![0]);
        assert_eq!(a.assign(999), vec![0]);
        assert_eq!(a.assign(1000), vec![1]);
        assert_eq!(a.assign(2500), vec![2]);
    }

    #[test]
    #[should_panic(expected = "tumbling window size must be positive")]
    fn tumbling_assigner_rejects_zero_size() {
        let _ = TumblingWindowAssigner::new(0);
    }

    #[test]
    fn separate_keys_drive_separate_partitions() {
        let mut op = make_op();
        op.process_left(10, (1, 100));
        op.process_left(20, (2, 200));
        op.process_right(30, (1, 999));
        // Key 1 has 1 left + 1 right inside [10, 110] window:
        // delta = 1.
        // Key 2 has only a left; no match yet.
        assert_eq!(op.partition_count(), 2);
        assert_eq!(op.fire_window(&1, &0), 1);
        assert_eq!(op.fire_window(&2, &0), 0);
    }

    #[test]
    fn separate_windows_drive_separate_partitions() {
        let mut op = make_op();
        // Window 0 = [0, 1000), Window 1 = [1000, 2000).
        op.process_left(100, (1, 0));
        op.process_right(200, (1, 0)); // same key, same window 0
        op.process_left(1100, (1, 0)); // same key, window 1
        op.process_right(1200, (1, 0)); // window 1
        assert_eq!(op.partition_count(), 2);
        // Each window saw exactly one matching pair.
        assert_eq!(op.fire_window(&1, &0), 1);
        assert_eq!(op.fire_window(&1, &1), 1);
    }

    #[test]
    fn process_returns_one_result_per_assigned_window() {
        let mut op = make_op();
        let results = op.process_left(500, (7, 0));
        // Tumbling → exactly 1 window.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, 7);
        assert_eq!(results[0].window, 0);
        assert_eq!(results[0].side, Side::Left);
        // First left, no right → delta = 0.
        assert_eq!(results[0].delta, 0);
        // Now a matching right.
        let results = op.process_right(550, (7, 0));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].delta, 1);
        assert_eq!(results[0].aggregate, 1);
        assert_eq!(results[0].side, Side::Right);
    }

    #[test]
    fn fire_window_returns_zero_for_missing_partition() {
        let op = make_op();
        assert_eq!(op.fire_window(&999, &42), 0);
    }

    #[test]
    fn purge_drops_partition_state() {
        let mut op = make_op();
        op.process_left(10, (1, 0));
        op.process_right(20, (1, 0));
        assert_eq!(op.fire_window(&1, &0), 1);
        op.purge_window(&1, &0);
        assert_eq!(op.fire_window(&1, &0), 0);
        assert_eq!(op.partition_count(), 0);
    }

    #[test]
    fn clear_drops_every_partition() {
        let mut op = make_op();
        op.process_left(10, (1, 0));
        op.process_left(20, (2, 0));
        op.process_left(30, (3, 0));
        assert_eq!(op.partition_count(), 3);
        op.clear();
        assert_eq!(op.partition_count(), 0);
        assert_eq!(op.fire_window(&1, &0), 0);
    }

    /// Sliding-window assigner stub — each event lands in every
    /// window whose `[start, start + size)` interval contains its
    /// timestamp; `start` advances by `slide`. Mirrors the
    /// canonical Flink/CQELS sliding semantics.
    #[derive(Clone, Copy)]
    struct SlidingAssigner {
        size: i64,
        slide: i64,
    }
    impl WindowAssigner<i64> for SlidingAssigner {
        fn assign(&self, timestamp: i64) -> Vec<i64> {
            // Cap the start to >= 0 for synthetic test timestamps;
            // mirrors Layer 2's `SlidingWindow` clamp.
            let first = ((timestamp - self.size) / self.slide + 1) * self.slide;
            let first = first.max(0);
            let mut out = Vec::new();
            let mut start = first;
            while start <= timestamp {
                if timestamp < start + self.size {
                    out.push(start / self.slide);
                }
                start += self.slide;
            }
            out
        }
    }

    #[test]
    fn sliding_assigner_emits_one_result_per_overlapping_window() {
        let mut op: WindowedJoinAggregateOperator<CountSpec, i32, i64> =
            WindowedJoinAggregateOperator::new(
                CountSpec,
                IntervalBounds::new(0, 100),
                SlidingAssigner {
                    size: 1000,
                    slide: 500,
                },
                |l: &(i32, i64)| l.0,
                |r: &(i32, i64)| r.0,
            );
        // ts=10000 with size=1000 slide=500 lands in windows
        // starting at 9500 and 10000 (window ids 19 and 20).
        let results = op.process_left(10000, (1, 0));
        assert_eq!(results.len(), 2);
        assert_eq!(op.partition_count(), 2);
    }
}
