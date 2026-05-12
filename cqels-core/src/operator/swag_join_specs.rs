//! Concrete `JoinAggregateSpec` implementations + a streaming
//! windowed-aggregate runner — layer 4 (pairwise half) of the F-IVM
//! port.
//!
//! Layer 2 ([`crate::operator::swag_join_state`]) gave us the
//! abstract [`JoinAggregateSpec`] contract and the stateful
//! [`WindowJoinState`]. This module wires those into the four
//! canonical binary-join aggregations from cqels-java's
//! `org.cqels.operator.aggregate.swag.join`:
//!
//! - [`CountSpec`] — `COUNT(*)` over joined tuples.
//! - [`SumLeftSpec`] — `SUM(left.value)`.
//! - [`SumRightSpec`] — `SUM(right.value)`.
//! - [`SumProductSpec`] — `SUM(left.value × right.value)`.
//!
//! Plus a streaming wrapper:
//!
//! - [`WindowedJoinAggregator`] — drives a [`WindowJoinState`] from
//!   a merged `Stream<JoinUpdate<L, R>>` and emits one
//!   [`JoinAggregateEvent`] per processed update.
//!
//! Scope (deferred to a follow-up — Layer 5)
//! - `MultiWayJoinState` (the recursive F-IVM executor across an
//!   N-way join graph).
//! - `MultiWayAggregateSpec` + `TriangleJoinAggregateSpec` (the
//!   multi-way spec hierarchy).
//! - `ParallelJoinAggregator` (Tier-2 batch recomputation).

use std::marker::PhantomData;
use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

use crate::operator::swag_join_state::{IntervalBounds, JoinAggregateSpec, WindowJoinState};

// ─── CountSpec ──────────────────────────────────────────────────────

/// `COUNT(*)` over the join. Every element contributes payload `1`;
/// the running aggregate counts matched tuples. Mirrors Java's
/// `CountSpec<L, R>`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CountSpec<L, R>(PhantomData<fn() -> (L, R)>);

impl<L, R> CountSpec<L, R> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<L, R> JoinAggregateSpec for CountSpec<L, R>
where
    L: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    type L = L;
    type R = R;
    type PL = i64;
    type PR = i64;
    type V = i64;

    fn left_payload(&self, _: &Self::L) -> Self::PL {
        1
    }
    fn right_payload(&self, _: &Self::R) -> Self::PR {
        1
    }
    fn zero(&self) -> Self::V {
        0
    }
    fn add(&self, a: &Self::V, b: &Self::V) -> Self::V {
        a + b
    }
    fn negate(&self, v: &Self::V) -> Self::V {
        -v
    }
    fn multiply_left(&self, l: &Self::PL, r: &Self::PR) -> Self::V {
        l * r
    }
    fn multiply_right(&self, l: &Self::PL, r: &Self::PR) -> Self::V {
        l * r
    }
    fn zero_left(&self) -> Self::PL {
        0
    }
    fn zero_right(&self) -> Self::PR {
        0
    }
    fn add_left(&self, a: &Self::PL, b: &Self::PL) -> Self::PL {
        a + b
    }
    fn add_right(&self, a: &Self::PR, b: &Self::PR) -> Self::PR {
        a + b
    }
    fn name(&self) -> &str {
        "COUNT"
    }
}

// ─── SumLeftSpec ────────────────────────────────────────────────────

/// `SUM(left.value)` over joined tuples. The left payload is the
/// extracted numeric value; the right payload is `1` (a count).
/// Mirrors Java's `SumLeftSpec<L, R>`.
///
/// `Eq + Hash` payload types are required by `TimeIndex`'s
/// bag-semantics buckets. We encode `f64` via the [`F64Bits`]
/// newtype so two equal-bit-pattern values are equal *and* hash
/// the same.
pub struct SumLeftSpec<L, R, F>
where
    F: Fn(&L) -> f64 + Send + Sync + 'static,
{
    extractor: F,
    _phantom: PhantomData<fn() -> (L, R)>,
}

impl<L, R, F> SumLeftSpec<L, R, F>
where
    F: Fn(&L) -> f64 + Send + Sync + 'static,
{
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<L, R, F> JoinAggregateSpec for SumLeftSpec<L, R, F>
where
    L: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&L) -> f64 + Send + Sync + 'static,
{
    type L = L;
    type R = R;
    type PL = F64Bits;
    type PR = i64;
    type V = F64Bits;

    fn left_payload(&self, l: &L) -> F64Bits {
        F64Bits::from_f64((self.extractor)(l))
    }
    fn right_payload(&self, _: &R) -> i64 {
        1
    }
    fn zero(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn add(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn negate(&self, v: &F64Bits) -> F64Bits {
        F64Bits::from_f64(-v.to_f64())
    }
    fn multiply_left(&self, l: &F64Bits, r: &i64) -> F64Bits {
        F64Bits::from_f64(l.to_f64() * (*r as f64))
    }
    fn multiply_right(&self, l: &F64Bits, r: &i64) -> F64Bits {
        F64Bits::from_f64(l.to_f64() * (*r as f64))
    }
    fn zero_left(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn zero_right(&self) -> i64 {
        0
    }
    fn add_left(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn add_right(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }
    fn name(&self) -> &str {
        "SUM_LEFT"
    }
}

// ─── SumRightSpec ───────────────────────────────────────────────────

/// `SUM(right.value)` over joined tuples. Mirror image of
/// [`SumLeftSpec`].
pub struct SumRightSpec<L, R, F>
where
    F: Fn(&R) -> f64 + Send + Sync + 'static,
{
    extractor: F,
    _phantom: PhantomData<fn() -> (L, R)>,
}

impl<L, R, F> SumRightSpec<L, R, F>
where
    F: Fn(&R) -> f64 + Send + Sync + 'static,
{
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<L, R, F> JoinAggregateSpec for SumRightSpec<L, R, F>
where
    L: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&R) -> f64 + Send + Sync + 'static,
{
    type L = L;
    type R = R;
    type PL = i64;
    type PR = F64Bits;
    type V = F64Bits;

    fn left_payload(&self, _: &L) -> i64 {
        1
    }
    fn right_payload(&self, r: &R) -> F64Bits {
        F64Bits::from_f64((self.extractor)(r))
    }
    fn zero(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn add(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn negate(&self, v: &F64Bits) -> F64Bits {
        F64Bits::from_f64(-v.to_f64())
    }
    fn multiply_left(&self, l: &i64, r: &F64Bits) -> F64Bits {
        F64Bits::from_f64((*l as f64) * r.to_f64())
    }
    fn multiply_right(&self, l: &i64, r: &F64Bits) -> F64Bits {
        F64Bits::from_f64((*l as f64) * r.to_f64())
    }
    fn zero_left(&self) -> i64 {
        0
    }
    fn zero_right(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn add_left(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }
    fn add_right(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn name(&self) -> &str {
        "SUM_RIGHT"
    }
}

// ─── SumProductSpec ─────────────────────────────────────────────────

/// `SUM(left.value × right.value)` over joined tuples. Both payloads
/// are numeric values; the aggregate accumulates products. Mirrors
/// Java's `SumProductSpec<L, R>`.
pub struct SumProductSpec<L, R, FL, FR>
where
    FL: Fn(&L) -> f64 + Send + Sync + 'static,
    FR: Fn(&R) -> f64 + Send + Sync + 'static,
{
    left_extractor: FL,
    right_extractor: FR,
    _phantom: PhantomData<fn() -> (L, R)>,
}

impl<L, R, FL, FR> SumProductSpec<L, R, FL, FR>
where
    FL: Fn(&L) -> f64 + Send + Sync + 'static,
    FR: Fn(&R) -> f64 + Send + Sync + 'static,
{
    pub fn new(left_extractor: FL, right_extractor: FR) -> Self {
        Self {
            left_extractor,
            right_extractor,
            _phantom: PhantomData,
        }
    }
}

impl<L, R, FL, FR> JoinAggregateSpec for SumProductSpec<L, R, FL, FR>
where
    L: Send + Sync + 'static,
    R: Send + Sync + 'static,
    FL: Fn(&L) -> f64 + Send + Sync + 'static,
    FR: Fn(&R) -> f64 + Send + Sync + 'static,
{
    type L = L;
    type R = R;
    type PL = F64Bits;
    type PR = F64Bits;
    type V = F64Bits;

    fn left_payload(&self, l: &L) -> F64Bits {
        F64Bits::from_f64((self.left_extractor)(l))
    }
    fn right_payload(&self, r: &R) -> F64Bits {
        F64Bits::from_f64((self.right_extractor)(r))
    }
    fn zero(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn add(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn negate(&self, v: &F64Bits) -> F64Bits {
        F64Bits::from_f64(-v.to_f64())
    }
    fn multiply_left(&self, l: &F64Bits, r: &F64Bits) -> F64Bits {
        F64Bits::from_f64(l.to_f64() * r.to_f64())
    }
    fn multiply_right(&self, l: &F64Bits, r: &F64Bits) -> F64Bits {
        F64Bits::from_f64(l.to_f64() * r.to_f64())
    }
    fn zero_left(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn zero_right(&self) -> F64Bits {
        F64Bits::ZERO
    }
    fn add_left(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn add_right(&self, a: &F64Bits, b: &F64Bits) -> F64Bits {
        F64Bits::from_f64(a.to_f64() + b.to_f64())
    }
    fn name(&self) -> &str {
        "SUM_PRODUCT"
    }
}

// ─── F64Bits ────────────────────────────────────────────────────────

/// Bit-pattern wrapper around `f64` so it can serve as a `TimeIndex`
/// payload (`Eq + Hash` required for bag-semantics buckets).
///
/// Two `F64Bits` are equal iff their underlying `f64` values have
/// the same IEEE-754 bit pattern — which means `+0.0 != -0.0` and
/// `NaN != NaN`. For the F-IVM ring algebra this is the safe
/// choice: we never want a `NaN`-tainted payload to dedupe with a
/// "real" zero, and we never want `+0.0` and `-0.0` to silently
/// collapse during eviction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct F64Bits(u64);

impl F64Bits {
    pub const ZERO: F64Bits = F64Bits(0);

    pub fn from_f64(v: f64) -> Self {
        F64Bits(v.to_bits())
    }

    pub fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl From<f64> for F64Bits {
    fn from(v: f64) -> Self {
        Self::from_f64(v)
    }
}

impl From<F64Bits> for f64 {
    fn from(b: F64Bits) -> Self {
        b.to_f64()
    }
}

// ─── Streaming runner ───────────────────────────────────────────────

/// Update on a binary join's left or right input.
#[derive(Clone, Debug)]
pub enum JoinUpdate<L, R> {
    /// Insert an element on the left side.
    InsertLeft { timestamp: i64, element: L },
    /// Insert an element on the right side.
    InsertRight { timestamp: i64, element: R },
    /// Evict a previously-inserted left element.
    EvictLeft { timestamp: i64, element: L },
    /// Evict a previously-inserted right element.
    EvictRight { timestamp: i64, element: R },
}

/// Which input side produced an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Side {
    Left,
    Right,
}

/// One emitted aggregate event from the streaming runner. Carries
/// the side that triggered it, the delta, and the running aggregate
/// after applying the delta.
#[derive(Clone, Debug)]
pub struct JoinAggregateEvent<V> {
    pub side: Side,
    pub timestamp: i64,
    pub delta: V,
    pub aggregate: V,
}

/// Boxed `Send` stream of `JoinUpdate`s — input to
/// [`WindowedJoinAggregator::process`]. Aliased here to keep the
/// public signature readable.
pub type BoxedUpdateStream<L, R> = Pin<Box<dyn Stream<Item = JoinUpdate<L, R>> + Send>>;

/// Boxed `Send` stream of `JoinAggregateEvent`s — output of
/// [`WindowedJoinAggregator::process`].
pub type BoxedEventStream<V> = Pin<Box<dyn Stream<Item = JoinAggregateEvent<V>> + Send>>;

/// Streaming runner over a [`WindowJoinState`].
///
/// `process` consumes a `Stream<JoinUpdate<L, R>>` (a merge of the
/// left and right input streams plus optional retractions) and
/// emits one [`JoinAggregateEvent`] per processed update. The
/// running aggregate is maintained incrementally by the underlying
/// state's F-IVM delta machinery.
///
/// Use [`from_keyed`](Self::from_keyed) when the join is an
/// equi-join with caller-supplied key extractors.
pub struct WindowedJoinAggregator<S, K = ()>
where
    S: JoinAggregateSpec,
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    state: WindowJoinState<S, K>,
}

impl<S: JoinAggregateSpec> WindowedJoinAggregator<S, ()> {
    /// Builds a cross-product (no key filtering) aggregator.
    pub fn cross_product(spec: S, bounds: IntervalBounds) -> Self {
        Self {
            state: WindowJoinState::new(spec, bounds),
        }
    }
}

impl<S, K> WindowedJoinAggregator<S, K>
where
    S: JoinAggregateSpec,
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
{
    /// Builds an equi-join aggregator that filters opposite-side
    /// elements by the supplied key extractors.
    pub fn from_keyed(
        spec: S,
        bounds: IntervalBounds,
        left_extractor: impl Fn(&S::L) -> K + Send + Sync + 'static,
        right_extractor: impl Fn(&S::R) -> K + Send + Sync + 'static,
    ) -> Self {
        Self {
            state: WindowJoinState::with_keys(spec, bounds, left_extractor, right_extractor),
        }
    }

    /// Borrowed access to the inner join state for diagnostics.
    pub fn state(&self) -> &WindowJoinState<S, K> {
        &self.state
    }

    /// Current running aggregate.
    pub fn aggregate(&self) -> S::V {
        self.state.aggregate()
    }

    /// Drives the join state from a merged update stream and emits
    /// one event per processed update. Consumes `self` because the
    /// returned stream owns the state.
    pub fn process(
        mut self,
        updates: BoxedUpdateStream<S::L, S::R>,
    ) -> BoxedEventStream<S::V>
    where
        S: Send + 'static,
        S::L: Send + 'static,
        S::R: Send + 'static,
        S::PL: Send + Sync + 'static,
        S::PR: Send + Sync + 'static,
        S::V: Send + Clone + 'static,
    {
        Box::pin(updates.map(move |update| match update {
            JoinUpdate::InsertLeft { timestamp, element } => {
                let delta = self.state.insert_left(timestamp, element);
                JoinAggregateEvent {
                    side: Side::Left,
                    timestamp,
                    delta,
                    aggregate: self.state.aggregate(),
                }
            }
            JoinUpdate::InsertRight { timestamp, element } => {
                let delta = self.state.insert_right(timestamp, element);
                JoinAggregateEvent {
                    side: Side::Right,
                    timestamp,
                    delta,
                    aggregate: self.state.aggregate(),
                }
            }
            JoinUpdate::EvictLeft { timestamp, element } => {
                let delta = self.state.evict_left(timestamp, element);
                JoinAggregateEvent {
                    side: Side::Left,
                    timestamp,
                    delta,
                    aggregate: self.state.aggregate(),
                }
            }
            JoinUpdate::EvictRight { timestamp, element } => {
                let delta = self.state.evict_right(timestamp, element);
                JoinAggregateEvent {
                    side: Side::Right,
                    timestamp,
                    delta,
                    aggregate: self.state.aggregate(),
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    // ─── CountSpec ─────────────────────────────────────────────────

    #[test]
    fn count_spec_counts_pairs_in_pairwise_join() {
        let mut s = WindowJoinState::new(CountSpec::<i32, i32>::new(), IntervalBounds::new(0, 10));
        s.insert_left(0, 1);
        s.insert_right(5, 2);
        s.insert_right(8, 3);
        assert_eq!(s.aggregate(), 2);
    }

    // ─── SumLeftSpec ───────────────────────────────────────────────

    #[derive(Clone, Copy, Debug)]
    struct Sale {
        amount: f64,
    }

    #[test]
    fn sum_left_spec_sums_left_values_per_right_match() {
        let spec: SumLeftSpec<Sale, (), _> = SumLeftSpec::new(|s: &Sale| s.amount);
        let mut state = WindowJoinState::new(spec, IntervalBounds::new(0, 10));
        state.insert_left(0, Sale { amount: 10.0 });
        state.insert_left(2, Sale { amount: 5.0 });
        // Two right hits — each pairs with both lefts. Sum:
        // (10 + 5) × 2 = 30.
        state.insert_right(5, ());
        state.insert_right(7, ());
        assert!((state.aggregate().to_f64() - 30.0).abs() < 1e-9);
    }

    // ─── SumRightSpec ──────────────────────────────────────────────

    #[derive(Clone, Copy, Debug)]
    struct Shipment {
        qty: f64,
    }

    #[test]
    fn sum_right_spec_mirrors_sum_left() {
        let spec: SumRightSpec<(), Shipment, _> = SumRightSpec::new(|s: &Shipment| s.qty);
        let mut state = WindowJoinState::new(spec, IntervalBounds::new(0, 10));
        state.insert_left(0, ());
        state.insert_left(2, ());
        // Each right (3 + 5) sums up to 8; counted twice for two
        // lefts → 16.
        state.insert_right(5, Shipment { qty: 3.0 });
        state.insert_right(7, Shipment { qty: 5.0 });
        assert!((state.aggregate().to_f64() - 16.0).abs() < 1e-9);
    }

    // ─── SumProductSpec ───────────────────────────────────────────

    #[test]
    fn sum_product_spec_emits_inner_product() {
        // SUM(left × right). Two pairs: (10 × 3), (10 × 5),
        // (5 × 3), (5 × 5) = 30 + 50 + 15 + 25 = 120.
        let spec: SumProductSpec<Sale, Shipment, _, _> =
            SumProductSpec::new(|s: &Sale| s.amount, |s: &Shipment| s.qty);
        let mut state = WindowJoinState::new(spec, IntervalBounds::new(0, 10));
        state.insert_left(0, Sale { amount: 10.0 });
        state.insert_left(2, Sale { amount: 5.0 });
        state.insert_right(5, Shipment { qty: 3.0 });
        state.insert_right(7, Shipment { qty: 5.0 });
        assert!((state.aggregate().to_f64() - 120.0).abs() < 1e-9);
    }

    // ─── F64Bits ───────────────────────────────────────────────────

    #[test]
    fn f64_bits_round_trip_and_signed_zero_distinct() {
        let a = F64Bits::from_f64(1.5);
        assert!((a.to_f64() - 1.5).abs() < 1e-12);

        // Sanity: +0.0 and -0.0 have distinct bit patterns and are
        // therefore NOT equal as F64Bits (intentional — see docs).
        assert_ne!(F64Bits::from_f64(0.0), F64Bits::from_f64(-0.0));
    }

    // ─── WindowedJoinAggregator ──────────────────────────────────

    #[tokio::test]
    async fn windowed_aggregator_emits_one_event_per_update() {
        let agg: WindowedJoinAggregator<CountSpec<i32, i32>, ()> =
            WindowedJoinAggregator::cross_product(
                CountSpec::<i32, i32>::new(),
                IntervalBounds::new(0, 10),
            );

        let updates = vec![
            JoinUpdate::<i32, i32>::InsertLeft {
                timestamp: 0,
                element: 1,
            },
            JoinUpdate::<i32, i32>::InsertRight {
                timestamp: 5,
                element: 2,
            },
            JoinUpdate::<i32, i32>::InsertRight {
                timestamp: 8,
                element: 3,
            },
        ];
        let s: Pin<Box<dyn Stream<Item = JoinUpdate<i32, i32>> + Send>> =
            Box::pin(stream::iter(updates));
        let events: Vec<_> = agg.process(s).collect().await;
        assert_eq!(events.len(), 3);

        // Aggregate progression: 0, 1, 2.
        assert_eq!(events[0].aggregate, 0);
        assert_eq!(events[0].side, Side::Left);
        assert_eq!(events[1].aggregate, 1);
        assert_eq!(events[1].side, Side::Right);
        assert_eq!(events[2].aggregate, 2);
        assert_eq!(events[2].side, Side::Right);
    }

    #[tokio::test]
    async fn windowed_aggregator_evictions_decrement_aggregate() {
        let agg: WindowedJoinAggregator<CountSpec<i32, i32>, ()> =
            WindowedJoinAggregator::cross_product(
                CountSpec::<i32, i32>::new(),
                IntervalBounds::new(0, 10),
            );
        let updates = vec![
            JoinUpdate::<i32, i32>::InsertLeft {
                timestamp: 0,
                element: 1,
            },
            JoinUpdate::<i32, i32>::InsertRight {
                timestamp: 5,
                element: 2,
            },
            JoinUpdate::<i32, i32>::EvictLeft {
                timestamp: 0,
                element: 1,
            },
        ];
        let s: Pin<Box<dyn Stream<Item = JoinUpdate<i32, i32>> + Send>> =
            Box::pin(stream::iter(updates));
        let events: Vec<_> = agg.process(s).collect().await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].aggregate, 0, "evict left zeros the count");
        assert_eq!(events[2].delta, -1);
    }

    #[tokio::test]
    async fn windowed_aggregator_keyed_equi_join() {
        // Each `i32` is its own join key.
        let agg: WindowedJoinAggregator<CountSpec<i32, i32>, i32> =
            WindowedJoinAggregator::from_keyed(
                CountSpec::<i32, i32>::new(),
                IntervalBounds::new(0, 100),
                |l: &i32| *l,
                |r: &i32| *r,
            );
        let updates = vec![
            JoinUpdate::<i32, i32>::InsertLeft {
                timestamp: 0,
                element: 1,
            },
            JoinUpdate::<i32, i32>::InsertLeft {
                timestamp: 1,
                element: 2,
            },
            JoinUpdate::<i32, i32>::InsertRight {
                timestamp: 10,
                element: 1,
            }, // matches left 1
            JoinUpdate::<i32, i32>::InsertRight {
                timestamp: 11,
                element: 3,
            }, // no match
        ];
        let s: Pin<Box<dyn Stream<Item = JoinUpdate<i32, i32>> + Send>> =
            Box::pin(stream::iter(updates));
        let events: Vec<_> = agg.process(s).collect().await;
        assert_eq!(events.last().unwrap().aggregate, 1);
    }
}
