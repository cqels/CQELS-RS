//! Triangle (3-way) join aggregation spec — layer 5b of the SWAG
//! join port.
//!
//! Ports cqels-java's `TriangleJoinAggregateSpec<A, B, C, PA, PB, PC, V>`
//! interface. Compared to the generic
//! [`MultiWayAggregateSpec`](crate::operator::swag_multiway::MultiWayAggregateSpec)
//! (layer 5a), the triangle variant carries **type-safe
//! per-relation payload types** — useful when each of the three
//! sides has a distinct payload semantics (e.g. user weights,
//! post weights, like weights).
//!
//! ## Relationship to layer 5a
//!
//! `MultiWayJoinState<E, K, S: MultiWayAggregateSpec>` (layer 5a)
//! already drives triangle topologies — it's how the existing
//! `triangle_multiway_count_matches_three_way_join` test works.
//! That path requires a single shared payload type `S::P` across
//! all three relations.
//!
//! `TriangleJoinAggregateSpec` is the **type-safe surface** when
//! you genuinely have three different payload types and don't
//! want to fold them into a sum type. The executor side that
//! consumes this trait natively is intentionally deferred — the
//! generic `MultiWayJoinState` path covers everything triangle
//! that doesn't need heterogeneous typing, and a dedicated
//! `TriangleJoinState` would just duplicate that algorithm.
//! This module ships the spec + a built-in `TriangleCount` so
//! the surface mirrors cqels-java; bridging adapters to
//! `MultiWayJoinState` can land later without a breaking change.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

/// `PhantomData` over three element types — keeps the spec
/// type-parameters live without runtime cost. Aliased to keep
/// the struct declarations readable (clippy `type_complexity`).
type ThreeTypeMarker<A, B, C> = PhantomData<fn() -> (A, B, C)>;

// ─── Trait ─────────────────────────────────────────────────────────

/// Triangle-shaped ring-algebra spec — three relations A, B, C with
/// distinct payload types `PA`, `PB`, `PC` aggregating into `V`.
/// Mirrors Java's `TriangleJoinAggregateSpec`.
pub trait TriangleJoinAggregateSpec: Send + Sync + 'static {
    type A;
    type B;
    type C;
    type PA: Clone + Eq + Hash + Send + Sync + 'static;
    type PB: Clone + Eq + Hash + Send + Sync + 'static;
    type PC: Clone + Eq + Hash + Send + Sync + 'static;
    type V: Clone + Send + Sync + 'static;

    fn payload_a(&self, a: &Self::A) -> Self::PA;
    fn payload_b(&self, b: &Self::B) -> Self::PB;
    fn payload_c(&self, c: &Self::C) -> Self::PC;

    fn zero(&self) -> Self::V;
    fn add(&self, v1: &Self::V, v2: &Self::V) -> Self::V;
    fn negate(&self, v: &Self::V) -> Self::V;

    fn multiply_triple(&self, a: &Self::PA, b: &Self::PB, c: &Self::PC) -> Self::V;

    fn zero_a(&self) -> Self::PA;
    fn zero_b(&self) -> Self::PB;
    fn zero_c(&self) -> Self::PC;

    fn add_a(&self, a1: &Self::PA, a2: &Self::PA) -> Self::PA;
    fn add_b(&self, b1: &Self::PB, b2: &Self::PB) -> Self::PB;
    fn add_c(&self, c1: &Self::PC, c2: &Self::PC) -> Self::PC;

    /// Bag-aware range aggregation for relation A. Default sums
    /// each payload `count` times. Mirrors Java's
    /// `aggregateRangeA` default.
    fn aggregate_range_a(&self, payload_counts: &HashMap<Self::PA, u64>) -> Self::PA {
        if payload_counts.is_empty() {
            return self.zero_a();
        }
        let mut sum = self.zero_a();
        for (p, c) in payload_counts {
            for _ in 0..*c {
                sum = self.add_a(&sum, p);
            }
        }
        sum
    }

    /// Bag-aware range aggregation for relation B.
    fn aggregate_range_b(&self, payload_counts: &HashMap<Self::PB, u64>) -> Self::PB {
        if payload_counts.is_empty() {
            return self.zero_b();
        }
        let mut sum = self.zero_b();
        for (p, c) in payload_counts {
            for _ in 0..*c {
                sum = self.add_b(&sum, p);
            }
        }
        sum
    }

    /// Bag-aware range aggregation for relation C.
    fn aggregate_range_c(&self, payload_counts: &HashMap<Self::PC, u64>) -> Self::PC {
        if payload_counts.is_empty() {
            return self.zero_c();
        }
        let mut sum = self.zero_c();
        for (p, c) in payload_counts {
            for _ in 0..*c {
                sum = self.add_c(&sum, p);
            }
        }
        sum
    }

    fn name(&self) -> &str;
}

// ─── Built-in: TriangleCount ───────────────────────────────────────

/// Built-in triangle `COUNT(*)` spec.
///
/// Every triangle contributes `1`. Each relation's payload is
/// `1i64`; the aggregate is the product across all three. Mirrors
/// Java's `TriangleJoinAggregateSpec.count()`.
///
/// Type-parameterized over the three element types `(A, B, C)` so
/// the spec carries the typing through (vs the type-erased
/// `MultiWayCount` which would treat them all identically).
pub struct TriangleCount<A, B, C>(ThreeTypeMarker<A, B, C>);

impl<A, B, C> Default for TriangleCount<A, B, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A, B, C> TriangleCount<A, B, C> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<A, B, C> TriangleJoinAggregateSpec for TriangleCount<A, B, C>
where
    A: Send + Sync + 'static,
    B: Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    type A = A;
    type B = B;
    type C = C;
    type PA = i64;
    type PB = i64;
    type PC = i64;
    type V = i64;

    fn payload_a(&self, _: &A) -> i64 {
        1
    }
    fn payload_b(&self, _: &B) -> i64 {
        1
    }
    fn payload_c(&self, _: &C) -> i64 {
        1
    }

    fn zero(&self) -> i64 {
        0
    }
    fn add(&self, v1: &i64, v2: &i64) -> i64 {
        v1 + v2
    }
    fn negate(&self, v: &i64) -> i64 {
        -v
    }

    fn multiply_triple(&self, a: &i64, b: &i64, c: &i64) -> i64 {
        a * b * c
    }

    fn zero_a(&self) -> i64 {
        0
    }
    fn zero_b(&self) -> i64 {
        0
    }
    fn zero_c(&self) -> i64 {
        0
    }
    fn add_a(&self, a1: &i64, a2: &i64) -> i64 {
        a1 + a2
    }
    fn add_b(&self, b1: &i64, b2: &i64) -> i64 {
        b1 + b2
    }
    fn add_c(&self, c1: &i64, c2: &i64) -> i64 {
        c1 + c2
    }

    fn name(&self) -> &str {
        "TRIANGLE_COUNT"
    }
}

// ─── Built-in: TriangleSumProduct ──────────────────────────────────

/// Built-in triangle `SUM(extract_a × extract_b × extract_c)` —
/// for each triangle tuple, multiply the three extracted numerics
/// and sum across all matches. Mirrors Java's
/// `TriangleJoinAggregateSpec.sumProduct(...)` family. Uses
/// [`F64Bits`](crate::operator::swag_join_specs::F64Bits) for
/// hash-stable f64 payloads.
pub struct TriangleSumProduct<A, B, C, FA, FB, FC>
where
    FA: Fn(&A) -> f64 + Send + Sync + 'static,
    FB: Fn(&B) -> f64 + Send + Sync + 'static,
    FC: Fn(&C) -> f64 + Send + Sync + 'static,
{
    extract_a: FA,
    extract_b: FB,
    extract_c: FC,
    _phantom: ThreeTypeMarker<A, B, C>,
}

impl<A, B, C, FA, FB, FC> TriangleSumProduct<A, B, C, FA, FB, FC>
where
    FA: Fn(&A) -> f64 + Send + Sync + 'static,
    FB: Fn(&B) -> f64 + Send + Sync + 'static,
    FC: Fn(&C) -> f64 + Send + Sync + 'static,
{
    pub fn new(extract_a: FA, extract_b: FB, extract_c: FC) -> Self {
        Self {
            extract_a,
            extract_b,
            extract_c,
            _phantom: PhantomData,
        }
    }
}

impl<A, B, C, FA, FB, FC> TriangleJoinAggregateSpec for TriangleSumProduct<A, B, C, FA, FB, FC>
where
    A: Send + Sync + 'static,
    B: Send + Sync + 'static,
    C: Send + Sync + 'static,
    FA: Fn(&A) -> f64 + Send + Sync + 'static,
    FB: Fn(&B) -> f64 + Send + Sync + 'static,
    FC: Fn(&C) -> f64 + Send + Sync + 'static,
{
    type A = A;
    type B = B;
    type C = C;
    type PA = crate::operator::swag_join_specs::F64Bits;
    type PB = crate::operator::swag_join_specs::F64Bits;
    type PC = crate::operator::swag_join_specs::F64Bits;
    type V = crate::operator::swag_join_specs::F64Bits;

    fn payload_a(&self, a: &A) -> Self::PA {
        Self::PA::from_f64((self.extract_a)(a))
    }
    fn payload_b(&self, b: &B) -> Self::PB {
        Self::PB::from_f64((self.extract_b)(b))
    }
    fn payload_c(&self, c: &C) -> Self::PC {
        Self::PC::from_f64((self.extract_c)(c))
    }

    fn zero(&self) -> Self::V {
        Self::V::ZERO
    }
    fn add(&self, v1: &Self::V, v2: &Self::V) -> Self::V {
        Self::V::from_f64(v1.to_f64() + v2.to_f64())
    }
    fn negate(&self, v: &Self::V) -> Self::V {
        Self::V::from_f64(-v.to_f64())
    }

    fn multiply_triple(&self, a: &Self::PA, b: &Self::PB, c: &Self::PC) -> Self::V {
        Self::V::from_f64(a.to_f64() * b.to_f64() * c.to_f64())
    }

    fn zero_a(&self) -> Self::PA {
        Self::PA::ZERO
    }
    fn zero_b(&self) -> Self::PB {
        Self::PB::ZERO
    }
    fn zero_c(&self) -> Self::PC {
        Self::PC::ZERO
    }
    fn add_a(&self, a1: &Self::PA, a2: &Self::PA) -> Self::PA {
        Self::PA::from_f64(a1.to_f64() + a2.to_f64())
    }
    fn add_b(&self, b1: &Self::PB, b2: &Self::PB) -> Self::PB {
        Self::PB::from_f64(b1.to_f64() + b2.to_f64())
    }
    fn add_c(&self, c1: &Self::PC, c2: &Self::PC) -> Self::PC {
        Self::PC::from_f64(c1.to_f64() + c2.to_f64())
    }

    fn name(&self) -> &str {
        "TRIANGLE_SUM_PRODUCT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::swag_join_specs::F64Bits;

    // ─── TriangleCount ─────────────────────────────────────────────

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct User(i32);
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct Post(i32);
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct Like(i32);

    #[test]
    fn triangle_count_unit_payloads() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        assert_eq!(spec.payload_a(&User(7)), 1);
        assert_eq!(spec.payload_b(&Post(8)), 1);
        assert_eq!(spec.payload_c(&Like(9)), 1);
    }

    #[test]
    fn triangle_count_multiplies_three_payloads() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        assert_eq!(spec.multiply_triple(&3, &4, &5), 60);
    }

    #[test]
    fn triangle_count_ring_axioms() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        assert_eq!(spec.zero(), 0);
        assert_eq!(spec.add(&3, &7), 10);
        assert_eq!(spec.add(&spec.zero(), &7), 7, "zero is left identity");
        assert_eq!(spec.add(&7, &spec.zero()), 7, "zero is right identity");
        assert_eq!(spec.add(&7, &spec.negate(&7)), 0, "additive inverse");
    }

    #[test]
    fn triangle_count_range_aggregation_respects_bag_counts() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        let mut counts: HashMap<i64, u64> = HashMap::new();
        counts.insert(1, 3); // payload 1, multiplicity 3
        counts.insert(5, 2); // payload 5, multiplicity 2
                             // Sum = 3*1 + 2*5 = 13.
        assert_eq!(spec.aggregate_range_a(&counts), 13);
        assert_eq!(spec.aggregate_range_b(&counts), 13);
        assert_eq!(spec.aggregate_range_c(&counts), 13);
    }

    #[test]
    fn triangle_count_empty_range_returns_zero() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        let empty: HashMap<i64, u64> = HashMap::new();
        assert_eq!(spec.aggregate_range_a(&empty), spec.zero_a());
    }

    #[test]
    fn triangle_count_name() {
        let spec: TriangleCount<User, Post, Like> = TriangleCount::new();
        assert_eq!(spec.name(), "TRIANGLE_COUNT");
    }

    // ─── TriangleSumProduct ────────────────────────────────────────

    #[derive(Clone, Copy)]
    struct Weighted {
        value: f64,
    }

    #[test]
    fn triangle_sum_product_extracts_and_multiplies() {
        let spec: TriangleSumProduct<Weighted, Weighted, Weighted, _, _, _> =
            TriangleSumProduct::new(
                |w: &Weighted| w.value,
                |w: &Weighted| w.value,
                |w: &Weighted| w.value,
            );
        let a = spec.payload_a(&Weighted { value: 2.0 });
        let b = spec.payload_b(&Weighted { value: 3.0 });
        let c = spec.payload_c(&Weighted { value: 5.0 });
        let product = spec.multiply_triple(&a, &b, &c);
        // 2 × 3 × 5 = 30.
        assert!((product.to_f64() - 30.0).abs() < 1e-9);
    }

    #[test]
    fn triangle_sum_product_zero_handling() {
        let spec: TriangleSumProduct<f64, f64, f64, _, _, _> =
            TriangleSumProduct::new(|v: &f64| *v, |v: &f64| *v, |v: &f64| *v);
        // Anything × 0 = 0.
        let z = spec.zero_a();
        let v = F64Bits::from_f64(7.5);
        assert!(spec.multiply_triple(&z, &v, &v).to_f64().abs() < 1e-12);
    }

    #[test]
    fn triangle_sum_product_bag_aggregation_weighted_sum() {
        let spec: TriangleSumProduct<f64, f64, f64, _, _, _> =
            TriangleSumProduct::new(|v: &f64| *v, |v: &f64| *v, |v: &f64| *v);
        let mut counts: HashMap<F64Bits, u64> = HashMap::new();
        counts.insert(F64Bits::from_f64(1.5), 2); // 1.5 with multiplicity 2
        counts.insert(F64Bits::from_f64(3.0), 1); // 3.0 with multiplicity 1
                                                  // Sum = 1.5 + 1.5 + 3.0 = 6.0.
        let agg = spec.aggregate_range_a(&counts);
        assert!((agg.to_f64() - 6.0).abs() < 1e-9);
    }

    #[test]
    fn triangle_sum_product_name() {
        let spec: TriangleSumProduct<f64, f64, f64, _, _, _> =
            TriangleSumProduct::new(|v: &f64| *v, |v: &f64| *v, |v: &f64| *v);
        assert_eq!(spec.name(), "TRIANGLE_SUM_PRODUCT");
    }
}
