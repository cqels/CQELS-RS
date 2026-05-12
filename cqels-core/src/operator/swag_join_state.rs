//! Pairwise interval join state — layer 2 of the F-IVM port.
//!
//! Ports `WindowJoinState` and its abstract `JoinAggregateSpec`
//! contract from cqels-java's
//! `org.cqels.operator.aggregate.swag.join`.
//!
//! Layer 1 ([`crate::operator::swag_join`]) gave us the indexing
//! primitives — this layer builds the **stateful pairwise join
//! operator** that uses them. It maintains left+right
//! `KeyIndexedTimeIndex` (or plain [`TimeIndex`] in cross-product
//! mode) plus a running aggregate, and computes the F-IVM delta on
//! every insert / evict.
//!
//! ## F-IVM delta rule
//!
//! For a sliding interval join over a ring `(V, +, ·, 0, -)`:
//!
//! ```text
//! Δ(L ⋈ R) = ΔL ⋈ R + L ⋈ ΔR
//!
//! INSERT_LEFT  at t : delta = leftPayload × agg(right, t+lb..t+ub)
//! INSERT_RIGHT at t : delta = agg(left,  t-ub..t-lb) × rightPayload
//! EVICT_*           : negate(insert delta)
//! ```
//!
//! Equi-join filtering is wired through caller-supplied key
//! extractor closures: when both sides supply one, only opposite-
//! side elements with a matching extracted key contribute to the
//! delta.
//!
//! ## Scope
//!
//! This layer ports the **incremental** path. Java's
//! `WindowJoinState.recomputeParallel(...)` (Tier-2 batch
//! recomputation via `ParallelJoinAggregator`) is deferred to a
//! later slice; it depends on the full planner and aggregator
//! hierarchy.

use std::hash::Hash;

use crate::operator::swag_ctrie::{RangeResult, TimeIndex};
use crate::operator::swag_join::KeyIndexedTimeIndex;

// ─── PayloadType + JoinAggregateSpec ────────────────────────────────

/// Which side of the join a payload aggregate belongs to. Surfaces
/// in the spec's per-side zero / add methods (`zero_left` /
/// `zero_right` / `add_left` / `add_right`), routed through the
/// `aggregate_left` / `aggregate_right` free functions below.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PayloadType {
    Left,
    Right,
}

/// Ring-algebra specification for an incrementally-maintained join
/// aggregate.
///
/// Mirrors Java's `JoinAggregateSpec<L, R, PL, PR, V>` interface —
/// the abstract base every concrete F-IVM aggregate (`CountSpec`,
/// `SumLeftSpec`, etc.) implements. The Rust port collapses the
/// per-side `addLeft`/`addRight`/`zeroLeft`/`zeroRight` methods into
/// a single pair parameterized by [`PayloadType`], which keeps
/// implementations leaner.
///
/// # Type parameters
///
/// - `L`, `R` — input element types.
/// - `PL`, `PR` — left/right *payload* types (ring elements).
/// - `V` — aggregate value type (ring element).
///
/// # Required invariants
///
/// - `(V, add, zero, negate)` is an additive group.
/// - `(PL, add_payload(Left, ...), zero_payload(Left))` is an
///   additive monoid; likewise for the right side.
/// - `multiply_left(leftPayload, rightAggregate)` distributes over
///   `add` on the right side; symmetrically for `multiply_right`.
pub trait JoinAggregateSpec: Send + Sync {
    type L;
    type R;
    type PL: Clone + Eq + Hash;
    type PR: Clone + Eq + Hash;
    type V: Clone;

    /// Extracts the left-side ring payload from an input element.
    fn left_payload(&self, left: &Self::L) -> Self::PL;

    /// Extracts the right-side ring payload from an input element.
    fn right_payload(&self, right: &Self::R) -> Self::PR;

    /// Aggregate-level zero (additive identity).
    fn zero(&self) -> Self::V;

    /// Aggregate-level addition.
    fn add(&self, a: &Self::V, b: &Self::V) -> Self::V;

    /// Aggregate-level additive inverse.
    fn negate(&self, v: &Self::V) -> Self::V;

    /// `leftPayload × rightRangeAgg`.
    fn multiply_left(&self, left: &Self::PL, right: &Self::PR) -> Self::V;

    /// `leftRangeAgg × rightPayload`.
    fn multiply_right(&self, left: &Self::PL, right: &Self::PR) -> Self::V;

    /// Payload-side zero. `side` selects left or right.
    fn zero_left(&self) -> Self::PL;
    fn zero_right(&self) -> Self::PR;

    /// Payload-side addition (used to aggregate a range of payloads
    /// into a single ring element).
    fn add_left(&self, a: &Self::PL, b: &Self::PL) -> Self::PL;
    fn add_right(&self, a: &Self::PR, b: &Self::PR) -> Self::PR;

    /// Human-readable name used in `Debug` output. Concrete specs
    /// supply this for diagnostics.
    fn name(&self) -> &str;
}

// ─── IntervalBounds ─────────────────────────────────────────────────

/// Closed interval `[lower, upper]` used as the temporal join
/// predicate offset (typically expressed in milliseconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IntervalBounds {
    pub lower: i64,
    pub upper: i64,
}

impl IntervalBounds {
    pub fn new(lower: i64, upper: i64) -> Self {
        assert!(lower <= upper, "lower must be <= upper");
        Self { lower, upper }
    }
}

// ─── Key extractor handle ───────────────────────────────────────────

/// Boxed key extractor. The owner holds it for the operator's
/// lifetime. We use `dyn Fn` rather than the layer-1
/// [`crate::operator::swag_join::KeyExtractor`] trait alias because
/// `WindowJoinState` needs to store the closure by value behind a
/// trait object.
type BoxedExtractor<T, K> = Box<dyn Fn(&T) -> K + Send + Sync + 'static>;

// ─── WindowJoinState ────────────────────────────────────────────────

/// Stateful pairwise interval-join operator with incremental F-IVM
/// delta computation.
///
/// Maintains time-indexed buffers for the left and right inputs,
/// plus a running aggregate value. Every `insert_*` / `evict_*` call
/// returns the delta for that operation and folds it into the
/// aggregate.
///
/// Two equivalent indexing modes:
/// - **Cross-product** (no key extractors): both sides share one
///   [`TimeIndex`] each, all temporal matches participate.
/// - **Equi-join** (key extractors supplied): both sides use a
///   [`KeyIndexedTimeIndex`]; only opposite-side elements with a
///   matching extracted key contribute to the delta.
///
/// # Example
///
/// ```
/// # use std::hash::Hash;
/// use cqels_core::operator::swag_join_state::{
///     IntervalBounds, JoinAggregateSpec, PayloadType, WindowJoinState,
/// };
///
/// /// Toy `COUNT(*)` spec: every element contributes payload `1`,
/// /// products multiply, sums add.
/// struct CountSpec;
/// impl JoinAggregateSpec for CountSpec {
///     type L = i32; type R = i32;
///     type PL = i64; type PR = i64; type V = i64;
///     fn left_payload(&self, _: &i32) -> i64 { 1 }
///     fn right_payload(&self, _: &i32) -> i64 { 1 }
///     fn zero(&self) -> i64 { 0 }
///     fn add(&self, a: &i64, b: &i64) -> i64 { a + b }
///     fn negate(&self, v: &i64) -> i64 { -v }
///     fn multiply_left(&self, l: &i64, r: &i64) -> i64 { l * r }
///     fn multiply_right(&self, l: &i64, r: &i64) -> i64 { l * r }
///     fn zero_left(&self) -> i64 { 0 }
///     fn zero_right(&self) -> i64 { 0 }
///     fn add_left(&self, a: &i64, b: &i64) -> i64 { a + b }
///     fn add_right(&self, a: &i64, b: &i64) -> i64 { a + b }
///     fn name(&self) -> &str { "COUNT" }
/// }
///
/// // 0 .. +10ms interval join; cross-product.
/// let mut state = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 10));
/// state.insert_left(0, 1);
/// state.insert_right(5, 2);
/// assert_eq!(state.aggregate(), 1, "one (left, right) match");
/// ```
pub struct WindowJoinState<S: JoinAggregateSpec, K = ()>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    spec: S,
    bounds: IntervalBounds,
    indexes: Indexes<S, K>,
    aggregate: S::V,
    name: String,
}

enum Indexes<S: JoinAggregateSpec, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    CrossProduct {
        left: TimeIndex<S::PL>,
        right: TimeIndex<S::PR>,
    },
    KeyIndexed {
        left: KeyIndexedTimeIndex<K, S::PL>,
        right: KeyIndexedTimeIndex<K, S::PR>,
        left_extractor: BoxedExtractor<S::L, K>,
        right_extractor: BoxedExtractor<S::R, K>,
    },
}

impl<S: JoinAggregateSpec> WindowJoinState<S, ()> {
    /// Builds a cross-product-mode operator (no key filtering — every
    /// temporal match participates).
    pub fn new(spec: S, bounds: IntervalBounds) -> Self {
        let aggregate = spec.zero();
        let name = spec.name().to_string();
        Self {
            spec,
            bounds,
            indexes: Indexes::CrossProduct {
                left: TimeIndex::new(),
                right: TimeIndex::new(),
            },
            aggregate,
            name,
        }
    }
}

impl<S: JoinAggregateSpec, K> WindowJoinState<S, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Builds an equi-join operator that filters opposite-side
    /// elements by the supplied key extractors.
    pub fn with_keys(
        spec: S,
        bounds: IntervalBounds,
        left_extractor: impl Fn(&S::L) -> K + Send + Sync + 'static,
        right_extractor: impl Fn(&S::R) -> K + Send + Sync + 'static,
    ) -> Self {
        let aggregate = spec.zero();
        let name = spec.name().to_string();
        Self {
            spec,
            bounds,
            indexes: Indexes::KeyIndexed {
                left: KeyIndexedTimeIndex::new(),
                right: KeyIndexedTimeIndex::new(),
                left_extractor: Box::new(left_extractor),
                right_extractor: Box::new(right_extractor),
            },
            aggregate,
            name,
        }
    }

    /// Current aggregate value.
    pub fn aggregate(&self) -> S::V
    where
        S::V: Clone,
    {
        self.aggregate.clone()
    }

    /// Spec name for diagnostics.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Interval bounds.
    pub fn bounds(&self) -> IntervalBounds {
        self.bounds
    }

    /// `true` when this state has key extractors configured.
    pub fn is_key_indexed(&self) -> bool {
        matches!(self.indexes, Indexes::KeyIndexed { .. })
    }

    /// Total left-side payload occurrences.
    pub fn left_count(&self) -> u64 {
        match &self.indexes {
            Indexes::CrossProduct { left, .. } => left.size(),
            Indexes::KeyIndexed { left, .. } => left.size(),
        }
    }

    /// Total right-side payload occurrences.
    pub fn right_count(&self) -> u64 {
        match &self.indexes {
            Indexes::CrossProduct { right, .. } => right.size(),
            Indexes::KeyIndexed { right, .. } => right.size(),
        }
    }

    /// Number of distinct left-side timestamps.
    pub fn left_bucket_count(&self) -> usize {
        match &self.indexes {
            Indexes::CrossProduct { left, .. } => left.bucket_count(),
            Indexes::KeyIndexed { left, .. } => left.bucket_count(),
        }
    }

    /// Number of distinct right-side timestamps.
    pub fn right_bucket_count(&self) -> usize {
        match &self.indexes {
            Indexes::CrossProduct { right, .. } => right.bucket_count(),
            Indexes::KeyIndexed { right, .. } => right.bucket_count(),
        }
    }

    /// `true` when both sides are empty.
    pub fn is_empty(&self) -> bool {
        match &self.indexes {
            Indexes::CrossProduct { left, right } => left.is_empty() && right.is_empty(),
            Indexes::KeyIndexed { left, right, .. } => left.is_empty() && right.is_empty(),
        }
    }

    /// Number of distinct join keys (key-indexed mode only; returns
    /// `0` in cross-product mode).
    pub fn key_count(&self) -> usize {
        match &self.indexes {
            Indexes::KeyIndexed { left, right, .. } => left.key_count().max(right.key_count()),
            Indexes::CrossProduct { .. } => 0,
        }
    }

    /// Drops all indexed payloads and resets the aggregate to zero.
    /// Maps to a window PURGE event.
    pub fn clear(&mut self) {
        match &mut self.indexes {
            Indexes::CrossProduct { left, right } => {
                left.clear();
                right.clear();
            }
            Indexes::KeyIndexed { left, right, .. } => {
                left.clear();
                right.clear();
            }
        }
        self.aggregate = self.spec.zero();
    }

    /// Inserts a left element at `timestamp` and returns the delta
    /// contributed to the aggregate.
    pub fn insert_left(&mut self, timestamp: i64, elem: S::L) -> S::V {
        let left_payload = self.spec.left_payload(&elem);
        let right_start = timestamp + self.bounds.lower;
        let right_end = timestamp + self.bounds.upper;

        let right_range = match &self.indexes {
            Indexes::CrossProduct { right, .. } => right.query_range(right_start, right_end),
            Indexes::KeyIndexed {
                right: right_idx,
                left_extractor,
                ..
            } => {
                let key = left_extractor(&elem);
                right_idx.query_range_by_key(&key, right_start, right_end)
            }
        };

        let right_agg = aggregate_right(&self.spec, &right_range);
        let delta = self.spec.multiply_left(&left_payload, &right_agg);

        // Side-effect the index AFTER the delta is computed so the
        // freshly-inserted left payload doesn't self-join.
        match &mut self.indexes {
            Indexes::CrossProduct { left, .. } => left.insert(timestamp, left_payload),
            Indexes::KeyIndexed {
                left: left_idx,
                left_extractor,
                ..
            } => {
                let key = left_extractor(&elem);
                left_idx.insert(key, timestamp, left_payload);
            }
        }

        self.aggregate = self.spec.add(&self.aggregate, &delta);
        delta
    }

    /// Inserts a right element at `timestamp` and returns the delta.
    pub fn insert_right(&mut self, timestamp: i64, elem: S::R) -> S::V {
        let right_payload = self.spec.right_payload(&elem);
        let left_start = timestamp - self.bounds.upper;
        let left_end = timestamp - self.bounds.lower;

        let left_range = match &self.indexes {
            Indexes::CrossProduct { left, .. } => left.query_range(left_start, left_end),
            Indexes::KeyIndexed {
                left: left_idx,
                right_extractor,
                ..
            } => {
                let key = right_extractor(&elem);
                left_idx.query_range_by_key(&key, left_start, left_end)
            }
        };

        let left_agg = aggregate_left(&self.spec, &left_range);
        let delta = self.spec.multiply_right(&left_agg, &right_payload);

        match &mut self.indexes {
            Indexes::CrossProduct { right, .. } => right.insert(timestamp, right_payload),
            Indexes::KeyIndexed {
                right: right_idx,
                right_extractor,
                ..
            } => {
                let key = right_extractor(&elem);
                right_idx.insert(key, timestamp, right_payload);
            }
        }

        self.aggregate = self.spec.add(&self.aggregate, &delta);
        delta
    }

    /// Evicts a previously-inserted left element and returns the
    /// (negative) delta.
    pub fn evict_left(&mut self, timestamp: i64, elem: S::L) -> S::V {
        let left_payload = self.spec.left_payload(&elem);
        let right_start = timestamp + self.bounds.lower;
        let right_end = timestamp + self.bounds.upper;

        // Evict first so the leaving payload isn't double-counted.
        match &mut self.indexes {
            Indexes::CrossProduct { left, .. } => {
                left.evict(timestamp, &left_payload);
            }
            Indexes::KeyIndexed {
                left: left_idx,
                left_extractor,
                ..
            } => {
                let key = left_extractor(&elem);
                left_idx.evict(&key, timestamp, &left_payload);
            }
        }

        let right_range = match &self.indexes {
            Indexes::CrossProduct { right, .. } => right.query_range(right_start, right_end),
            Indexes::KeyIndexed {
                right: right_idx,
                left_extractor,
                ..
            } => {
                let key = left_extractor(&elem);
                right_idx.query_range_by_key(&key, right_start, right_end)
            }
        };

        let right_agg = aggregate_right(&self.spec, &right_range);
        let positive = self.spec.multiply_left(&left_payload, &right_agg);
        let delta = self.spec.negate(&positive);
        self.aggregate = self.spec.add(&self.aggregate, &delta);
        delta
    }

    /// Evicts a previously-inserted right element and returns the
    /// (negative) delta.
    pub fn evict_right(&mut self, timestamp: i64, elem: S::R) -> S::V {
        let right_payload = self.spec.right_payload(&elem);
        let left_start = timestamp - self.bounds.upper;
        let left_end = timestamp - self.bounds.lower;

        match &mut self.indexes {
            Indexes::CrossProduct { right, .. } => {
                right.evict(timestamp, &right_payload);
            }
            Indexes::KeyIndexed {
                right: right_idx,
                right_extractor,
                ..
            } => {
                let key = right_extractor(&elem);
                right_idx.evict(&key, timestamp, &right_payload);
            }
        }

        let left_range = match &self.indexes {
            Indexes::CrossProduct { left, .. } => left.query_range(left_start, left_end),
            Indexes::KeyIndexed {
                left: left_idx,
                right_extractor,
                ..
            } => {
                let key = right_extractor(&elem);
                left_idx.query_range_by_key(&key, left_start, left_end)
            }
        };

        let left_agg = aggregate_left(&self.spec, &left_range);
        let positive = self.spec.multiply_right(&left_agg, &right_payload);
        let delta = self.spec.negate(&positive);
        self.aggregate = self.spec.add(&self.aggregate, &delta);
        delta
    }
}

/// Sum a range query's left-side payloads, respecting bag
/// multiplicities. Returns the spec's `zero_left()` on empty input.
fn aggregate_left<S: JoinAggregateSpec>(spec: &S, range: &RangeResult<S::PL>) -> S::PL {
    if range.is_empty() {
        return spec.zero_left();
    }
    let mut acc = spec.zero_left();
    for (payload, count) in &range.payload_counts {
        for _ in 0..*count {
            acc = spec.add_left(&acc, payload);
        }
    }
    acc
}

/// Sum a range query's right-side payloads, respecting bag
/// multiplicities. Returns the spec's `zero_right()` on empty input.
fn aggregate_right<S: JoinAggregateSpec>(spec: &S, range: &RangeResult<S::PR>) -> S::PR {
    if range.is_empty() {
        return spec.zero_right();
    }
    let mut acc = spec.zero_right();
    for (payload, count) in &range.payload_counts {
        for _ in 0..*count {
            acc = spec.add_right(&acc, payload);
        }
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `COUNT(*)`-style spec: every element contributes payload `1`,
    /// the ring is `(i64, +, ·, 0, neg)`.
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

    // ─── Cross-product mode ────────────────────────────────────────

    #[test]
    fn insert_then_match_increments_count_by_one() {
        let mut s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 10));
        let d1 = s.insert_left(0, 1);
        // No right yet → delta should be 0.
        assert_eq!(d1, 0);
        let d2 = s.insert_right(5, 2);
        assert_eq!(d2, 1, "one (left, right) match in [0,10] window");
        assert_eq!(s.aggregate(), 1);
        assert_eq!(s.left_count(), 1);
        assert_eq!(s.right_count(), 1);
    }

    #[test]
    fn match_must_satisfy_interval_bounds() {
        // [0, 5] only — right at t=10 is outside left=0's window.
        let mut s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 5));
        s.insert_left(0, 1);
        let d = s.insert_right(10, 2);
        assert_eq!(d, 0, "right outside left's [0,5] window must not match");
        assert_eq!(s.aggregate(), 0);
    }

    #[test]
    fn evict_undoes_prior_insert_delta() {
        let mut s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 10));
        s.insert_left(0, 1);
        s.insert_right(5, 2);
        assert_eq!(s.aggregate(), 1);
        let d = s.evict_left(0, 1);
        assert_eq!(d, -1, "eviction delta should be negative-1 here");
        assert_eq!(s.aggregate(), 0);
        assert_eq!(s.left_count(), 0);
    }

    #[test]
    fn cross_product_counts_every_temporal_match() {
        // Two lefts, three rights, all within window.
        let mut s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 100));
        s.insert_left(0, 1);
        s.insert_left(10, 2);
        s.insert_right(20, 3);
        s.insert_right(30, 4);
        s.insert_right(40, 5);
        // Each left pairs with each right in its window: 2 × 3 = 6.
        assert_eq!(s.aggregate(), 6);
    }

    #[test]
    fn clear_resets_aggregate_and_indexes() {
        let mut s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 10));
        s.insert_left(0, 1);
        s.insert_right(5, 2);
        s.clear();
        assert_eq!(s.aggregate(), 0);
        assert!(s.is_empty());
        // Post-clear inserts work fresh.
        s.insert_left(0, 1);
        s.insert_right(5, 2);
        assert_eq!(s.aggregate(), 1);
    }

    // ─── Key-indexed mode ──────────────────────────────────────────

    /// Element type carrying a key for equi-join.
    #[derive(Clone, Debug)]
    struct Keyed {
        key: i64,
    }

    /// `COUNT(*)` spec over `Keyed` elements.
    struct KeyedCountSpec;
    impl JoinAggregateSpec for KeyedCountSpec {
        type L = Keyed;
        type R = Keyed;
        type PL = i64;
        type PR = i64;
        type V = i64;
        fn left_payload(&self, _: &Keyed) -> i64 {
            1
        }
        fn right_payload(&self, _: &Keyed) -> i64 {
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
            "KEYED_COUNT"
        }
    }

    #[test]
    fn equi_join_only_matches_same_key() {
        let mut s: WindowJoinState<KeyedCountSpec, i64> = WindowJoinState::with_keys(
            KeyedCountSpec,
            IntervalBounds::new(0, 100),
            |k: &Keyed| k.key,
            |k: &Keyed| k.key,
        );

        assert!(s.is_key_indexed());

        s.insert_left(0, Keyed { key: 1 });
        s.insert_left(10, Keyed { key: 2 });
        s.insert_right(20, Keyed { key: 1 });
        s.insert_right(30, Keyed { key: 2 });
        s.insert_right(40, Keyed { key: 3 }); // no match

        // Exactly 2 matches: (k=1, k=1) + (k=2, k=2).
        assert_eq!(s.aggregate(), 2);
        assert_eq!(s.left_count(), 2);
        assert_eq!(s.right_count(), 3);
        assert_eq!(s.key_count(), 3);
    }

    #[test]
    fn equi_join_evict_drops_only_matching_pair() {
        let mut s: WindowJoinState<KeyedCountSpec, i64> = WindowJoinState::with_keys(
            KeyedCountSpec,
            IntervalBounds::new(0, 100),
            |k: &Keyed| k.key,
            |k: &Keyed| k.key,
        );

        s.insert_left(0, Keyed { key: 1 });
        s.insert_right(20, Keyed { key: 1 });
        s.insert_right(30, Keyed { key: 1 });
        // Two matches at key=1.
        assert_eq!(s.aggregate(), 2);

        let d = s.evict_left(0, Keyed { key: 1 });
        assert_eq!(d, -2, "evicting the lone left at key=1 cancels both pairs");
        assert_eq!(s.aggregate(), 0);
    }

    // ─── Misc ──────────────────────────────────────────────────────

    #[test]
    fn cross_product_key_count_is_zero() {
        let s = WindowJoinState::new(CountSpec, IntervalBounds::new(0, 10));
        assert!(!s.is_key_indexed());
        assert_eq!(s.key_count(), 0);
    }

    #[test]
    #[should_panic(expected = "lower must be <= upper")]
    fn invalid_interval_panics() {
        let _ = IntervalBounds::new(10, 5);
    }
}
