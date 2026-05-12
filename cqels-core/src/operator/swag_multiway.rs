//! Multi-way F-IVM join executor — layer 5a of the SWAG join port.
//!
//! Ports cqels-java's `MultiWayAggregateSpec` + `MultiWayJoinState`
//! from `org.cqels.operator.aggregate.swag.join`. Builds on Layer 1
//! (`KeyIndexedTimeIndex`/`CTrieIndex` etc.), Layer 3
//! (`JoinGraph`/`FactorizedViewPlan`), and Layer 4's pairwise
//! infrastructure.
//!
//! ## What's here
//!
//! - [`MultiWayAggregateSpec`] — generalized ring-algebra contract
//!   for N-way join aggregation. The single-payload model (one type
//!   `P` shared across relations) is the Rust analogue of Java's
//!   `Object`-typed payloads with cleaner type safety; for genuinely
//!   heterogeneous payloads the caller uses a sum type.
//! - [`MultiWayCount`] / [`MultiWaySumProduct`] — built-in specs
//!   mirroring `MultiWayAggregateSpec.count()` /
//!   `MultiWayAggregateSpec.sumProduct()`.
//! - [`MultiWayJoinState`] — recursive F-IVM executor over a
//!   [`JoinGraph`] with per-relation extractors + per-edge
//!   [`CTrieIndex`] indexes. `insert(name, ts, elem)` returns the
//!   F-IVM positive delta and folds it into the running aggregate;
//!   `delete(name, ts, elem)` returns the negated delta.
//!
//! ## What's deferred to Layer 5b+
//!
//! - `TriangleJoinAggregateSpec` and other concrete multi-way specs
//!   beyond `Count` / `SumProduct`.
//! - Java's prefix/suffix `RangeCache` optimization. It's an
//!   evaluation cache, not correctness — kept out so the executor
//!   stays reviewable. Can be retrofitted without changing the
//!   public surface.
//! - `delete_range` (Java itself notes this has a limitation —
//!   needs full-tuple TimeIndex enhancement first).
//! - `ParallelJoinAggregator` Tier-2 batch recompute.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use crate::operator::swag_ctrie::CTrieIndex;
use crate::operator::swag_join::JoinRecord;
use crate::operator::swag_join_graph::{FactorizedViewPlan, JoinGraph};
use crate::operator::swag_multiway_cache::{RangeCache, RangeCacheKey};

// ─── MultiWayAggregateSpec ──────────────────────────────────────────

/// Ring-algebra specification for N-way join aggregation.
///
/// Generalizes [`crate::operator::swag_join_state::JoinAggregateSpec`]
/// (which is binary-only) to arbitrary join topologies. The Java
/// interface uses raw `Object` payloads for every relation; this
/// Rust trait collapses to a single payload type `P` shared across
/// relations. Heterogeneous payloads are expressible by choosing a
/// sum type for `P`.
///
/// # Required invariants
///
/// - `(V, add, zero, negate)` is an Abelian group.
/// - `(P, add_payload, zero_payload(name))` is a commutative monoid
///   per relation.
/// - `multiply` distributes over `add` in the obvious way (one
///   payload per relation, multiplied; sum of products).
pub trait MultiWayAggregateSpec: Send + Sync + 'static {
    /// Shared payload type across relations.
    type P: Clone + Eq + Hash + Send + Sync + 'static;
    /// Aggregate ring element.
    type V: Clone + Send + Sync + 'static;

    /// Additive identity of the aggregate ring.
    fn zero(&self) -> Self::V;
    /// Ring addition.
    fn add(&self, a: &Self::V, b: &Self::V) -> Self::V;
    /// Additive inverse.
    fn negate(&self, v: &Self::V) -> Self::V;
    /// Multiply per-relation payloads into a single aggregate.
    /// `payloads` is keyed by relation name.
    fn multiply(&self, payloads: &BTreeMap<String, Self::P>) -> Self::V;
    /// Per-relation zero payload (additive identity for the
    /// relation's payload monoid).
    fn zero_payload(&self, relation_name: &str) -> Self::P;
    /// Per-relation payload addition.
    fn add_payload(&self, relation_name: &str, a: &Self::P, b: &Self::P) -> Self::P;
    /// Human-readable spec name (used in `Debug`).
    fn name(&self) -> &str;
}

// ─── Built-in specs ─────────────────────────────────────────────────

/// Built-in `COUNT(*)` over an N-way join. Each relation contributes
/// payload `1`; the aggregate is the product of all per-relation
/// counts. Mirrors Java's `MultiWayAggregateSpec.count()`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MultiWayCount;

impl MultiWayAggregateSpec for MultiWayCount {
    type P = i64;
    type V = i64;
    fn zero(&self) -> i64 {
        0
    }
    fn add(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }
    fn negate(&self, v: &i64) -> i64 {
        -v
    }
    fn multiply(&self, payloads: &BTreeMap<String, i64>) -> i64 {
        let mut product: i64 = 1;
        for v in payloads.values() {
            product *= v;
        }
        product
    }
    fn zero_payload(&self, _: &str) -> i64 {
        0
    }
    fn add_payload(&self, _: &str, a: &i64, b: &i64) -> i64 {
        a + b
    }
    fn name(&self) -> &str {
        "MULTIWAY_COUNT"
    }
}

/// Built-in `SUM(p₁ × p₂ × ... × pₙ)` — sum of products across
/// joined tuples. Mirrors Java's
/// `MultiWayAggregateSpec.sumProduct()`. Uses
/// [`F64Bits`](crate::operator::swag_join_specs::F64Bits) for the
/// payload so it can serve as a `TimeIndex` bag key.
#[derive(Clone, Copy, Debug, Default)]
pub struct MultiWaySumProduct;

impl MultiWayAggregateSpec for MultiWaySumProduct {
    type P = crate::operator::swag_join_specs::F64Bits;
    type V = crate::operator::swag_join_specs::F64Bits;

    fn zero(&self) -> Self::V {
        Self::V::ZERO
    }
    fn add(&self, a: &Self::V, b: &Self::V) -> Self::V {
        Self::V::from_f64(a.to_f64() + b.to_f64())
    }
    fn negate(&self, v: &Self::V) -> Self::V {
        Self::V::from_f64(-v.to_f64())
    }
    fn multiply(&self, payloads: &BTreeMap<String, Self::P>) -> Self::V {
        let mut product = 1.0_f64;
        for v in payloads.values() {
            product *= v.to_f64();
        }
        Self::V::from_f64(product)
    }
    fn zero_payload(&self, _: &str) -> Self::P {
        Self::P::ZERO
    }
    fn add_payload(&self, _: &str, a: &Self::P, b: &Self::P) -> Self::P {
        Self::P::from_f64(a.to_f64() + b.to_f64())
    }
    fn name(&self) -> &str {
        "MULTIWAY_SUM_PRODUCT"
    }
}

// ─── Extractors ─────────────────────────────────────────────────────

/// Per-relation extractors registered with [`MultiWayJoinState`]:
/// one key extractor (for indexing + matching) and one payload
/// extractor (for the aggregate ring).
pub struct RelationExtractors<E, K, P> {
    key: Box<dyn Fn(&E) -> K + Send + Sync>,
    payload: Box<dyn Fn(&E) -> P + Send + Sync>,
}

impl<E, K, P> RelationExtractors<E, K, P> {
    pub fn new(
        key: impl Fn(&E) -> K + Send + Sync + 'static,
        payload: impl Fn(&E) -> P + Send + Sync + 'static,
    ) -> Self {
        Self {
            key: Box::new(key),
            payload: Box::new(payload),
        }
    }
}

/// `relation -> neighbor -> per-edge CTrie`. Aliased to keep the
/// struct definition readable (clippy `type_complexity`).
type EdgeIndices<K, E, P> = HashMap<String, HashMap<String, CTrieIndex<K, JoinRecord<E, P>>>>;

// ─── MultiWayJoinState ──────────────────────────────────────────────

/// Recursive F-IVM executor for an N-way join over a
/// [`JoinGraph`]. Holds one [`CTrieIndex`] per outgoing edge (so
/// each `(relation, neighbor)` pair has its own key→time index)
/// and a running aggregate that's updated incrementally on every
/// insert/delete.
///
/// # Type parameters
///
/// - `E` — element type. Same for every relation; users can use a
///   sum type when relations carry heterogeneous shapes.
/// - `K` — join-key type. Same across the graph.
/// - `S` — the [`MultiWayAggregateSpec`]; provides payload + ring.
///
/// # Construction
///
/// 1. Build a connected [`JoinGraph`] (Layer 3).
/// 2. For each relation: register a key extractor + payload
///    extractor via [`MultiWayJoinState::with_extractors`].
///
/// The graph must be connected and every relation must have a
/// registered extractor.
pub struct MultiWayJoinState<E, K, S>
where
    E: Clone + Eq + Hash + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: MultiWayAggregateSpec,
{
    graph: JoinGraph,
    spec: Arc<S>,
    extractors: HashMap<String, RelationExtractors<E, K, S::P>>,
    /// Per-edge indices: `relation -> neighbor -> CTrieIndex<K, JoinRecord<E, P>>`.
    indices: EdgeIndices<K, E, <S as MultiWayAggregateSpec>::P>,
    /// Memoizes per-edge range query results during recursive
    /// delta computation. Invalidated on every `insert`/`delete`
    /// of the participating relation. See
    /// [`crate::operator::swag_multiway_cache`] for semantics.
    range_cache: RangeCache<K, E, S::P>,
    aggregate: S::V,
    variable_order: crate::operator::swag_join::VariableOrder,
    view_plan: FactorizedViewPlan,
    total_inserts: u64,
    total_matches: u64,
}

/// Capacity for the recursive-join [`RangeCache`]. Picked to match
/// Java's `MultiWayJoinState` default of 1024 entries.
const RANGE_CACHE_CAPACITY: usize = 1024;

impl<E, K, S> MultiWayJoinState<E, K, S>
where
    E: Clone + Eq + Hash + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: MultiWayAggregateSpec,
{
    /// Build an executor for the given graph and spec, registering
    /// per-relation extractors. The graph must be connected and
    /// every relation must have a registered extractor.
    ///
    /// # Panics
    ///
    /// - Graph not connected.
    /// - Any relation lacks an extractor (or any extractor is for
    ///   an unknown relation).
    /// - Any relation has degree 0 (isolated).
    pub fn with_extractors(
        graph: JoinGraph,
        spec: S,
        mut extractors: HashMap<String, RelationExtractors<E, K, S::P>>,
    ) -> Self {
        assert!(graph.is_connected(), "join graph must be connected");

        // Validate extractor coverage.
        for name in graph.relation_names() {
            assert!(
                extractors.contains_key(name),
                "missing extractor for relation '{name}'"
            );
            assert!(
                graph.degree(name) > 0,
                "isolated relation '{name}' (degree == 0)"
            );
        }
        let extra: Vec<String> = extractors
            .keys()
            .filter(|k| graph.relation(k).is_none())
            .cloned()
            .collect();
        assert!(
            extra.is_empty(),
            "extractor registered for unknown relation(s): {extra:?}"
        );

        let variable_order = graph.build_variable_order();
        let view_plan = FactorizedViewPlan::from_graph(&graph, variable_order.clone());

        // Build one CTrieIndex per outgoing edge.
        let mut indices: EdgeIndices<K, E, S::P> = HashMap::new();
        for name in graph.relation_names() {
            let mut edge_map: HashMap<String, CTrieIndex<K, JoinRecord<E, S::P>>> = HashMap::new();
            for neighbor in graph.neighbors(name) {
                edge_map.insert(neighbor.to_string(), CTrieIndex::new(1));
            }
            indices.insert(name.to_string(), edge_map);
        }

        // Move extractors into the state.
        let mut owned: HashMap<String, RelationExtractors<E, K, S::P>> = HashMap::new();
        for name in graph.relation_names() {
            let ext = extractors.remove(name).expect("validated above");
            owned.insert(name.to_string(), ext);
        }

        let aggregate = spec.zero();
        Self {
            graph,
            spec: Arc::new(spec),
            extractors: owned,
            indices,
            range_cache: RangeCache::new(RANGE_CACHE_CAPACITY),
            aggregate,
            variable_order,
            view_plan,
            total_inserts: 0,
            total_matches: 0,
        }
    }

    /// Read-only access to the underlying graph.
    pub fn graph(&self) -> &JoinGraph {
        &self.graph
    }

    /// Derived variable order for the join.
    pub fn variable_order(&self) -> &crate::operator::swag_join::VariableOrder {
        &self.variable_order
    }

    /// Derived factorized view plan.
    pub fn view_plan(&self) -> &FactorizedViewPlan {
        &self.view_plan
    }

    /// Current running aggregate.
    pub fn aggregate(&self) -> S::V {
        self.aggregate.clone()
    }

    /// Number of inserts processed since construction (or last
    /// [`clear`](Self::clear)).
    pub fn total_inserts(&self) -> u64 {
        self.total_inserts
    }

    /// Number of matching join tuples discovered during
    /// `insert`/`delete` so far.
    pub fn total_matches(&self) -> u64 {
        self.total_matches
    }

    /// Reset all indices + aggregate + counters.
    pub fn clear(&mut self) {
        for edge_map in self.indices.values_mut() {
            for idx in edge_map.values_mut() {
                idx.clear();
            }
        }
        self.range_cache.clear();
        self.aggregate = self.spec.zero();
        self.total_inserts = 0;
        self.total_matches = 0;
    }

    /// Number of currently-cached range queries. Exposed for tests
    /// and diagnostics — the actual cache content is private.
    pub fn range_cache_len(&self) -> usize {
        self.range_cache.len()
    }

    /// Insert `element` into `relation_name` at `timestamp`. Returns
    /// the F-IVM delta (also folded into the running aggregate).
    pub fn insert(&mut self, relation_name: &str, timestamp: i64, element: E) -> S::V {
        assert!(
            self.indices.contains_key(relation_name),
            "unknown relation: {relation_name}"
        );
        self.total_inserts += 1;
        let payload = (self.extractors[relation_name].payload)(&element);
        let delta = self.compute_delta(relation_name, timestamp, &element, &payload);
        self.aggregate = self.spec.add(&self.aggregate, &delta);
        self.index_element(relation_name, timestamp, element, payload);
        // Mutation invalidates cached ranges that include this
        // relation on either side.
        self.range_cache.invalidate_relation(relation_name);
        delta
    }

    /// Delete `element` from `relation_name` at `timestamp` (i.e.
    /// a specific previously-inserted occurrence). Returns the
    /// negative F-IVM delta.
    pub fn delete(&mut self, relation_name: &str, timestamp: i64, element: E) -> S::V {
        assert!(
            self.indices.contains_key(relation_name),
            "unknown relation: {relation_name}"
        );
        let payload = (self.extractors[relation_name].payload)(&element);
        // Remove from index BEFORE computing delta, otherwise the
        // element joins with itself.
        let removed = self.unindex_element(relation_name, timestamp, &element, &payload);
        if !removed {
            return self.spec.zero();
        }
        // Invalidate cached ranges that touch the mutated relation
        // before the delta walk, so the recursive query sees
        // post-eviction state.
        self.range_cache.invalidate_relation(relation_name);
        let positive = self.compute_delta(relation_name, timestamp, &element, &payload);
        let delta = self.spec.negate(&positive);
        self.aggregate = self.spec.add(&self.aggregate, &delta);
        delta
    }

    // ─── delta computation ───────────────────────────────────────

    fn compute_delta(
        &mut self,
        relation_name: &str,
        timestamp: i64,
        element: &E,
        payload: &S::P,
    ) -> S::V {
        let neighbors: Vec<String> = self
            .graph
            .neighbors(relation_name)
            .into_iter()
            .map(String::from)
            .collect();
        if neighbors.is_empty() {
            return self.spec.zero();
        }
        // Use the fast binary path ONLY for genuine 2-relation
        // graphs. Java's port uses this shortcut whenever the
        // inserted relation has degree 1, which is incorrect for
        // multi-way graphs (e.g. star topologies where satellite
        // relations have degree 1 but the join is N-way) — there
        // it would emit a 2-way delta instead of the full N-way
        // contribution. The Rust port gates on the global relation
        // count to keep F-IVM semantics correct.
        if self.graph.relation_names().len() == 2 && neighbors.len() == 1 {
            return self.binary_delta(relation_name, timestamp, element, payload, &neighbors[0]);
        }
        self.multi_way_delta(relation_name, timestamp, element.clone(), payload.clone())
    }

    fn binary_delta(
        &mut self,
        relation_name: &str,
        timestamp: i64,
        element: &E,
        payload: &S::P,
        neighbor_name: &str,
    ) -> S::V {
        let Some(edge) = self.graph.edge(relation_name, neighbor_name) else {
            return self.spec.zero();
        };
        let Some((start, end)) = edge.window_bounds(timestamp, relation_name) else {
            return self.spec.zero();
        };

        let query_key = (self.extractors[relation_name].key)(element);
        let neighbor_idx = self
            .indices
            .get(neighbor_name)
            .and_then(|m| m.get(relation_name));
        let Some(idx) = neighbor_idx else {
            return self.spec.zero();
        };

        let entries = idx.query_range_entries(&[Some(query_key)], start, end);
        if entries.is_empty() {
            return self.spec.zero();
        }

        let mut neighbor_payload_sum = self.spec.zero_payload(neighbor_name);
        let mut match_count: u64 = 0;
        for entry in &entries {
            for _ in 0..entry.count {
                neighbor_payload_sum = self.spec.add_payload(
                    neighbor_name,
                    &neighbor_payload_sum,
                    &entry.payload.payload,
                );
                match_count += 1;
            }
        }
        self.total_matches += match_count;

        let mut payloads = BTreeMap::new();
        payloads.insert(relation_name.to_string(), payload.clone());
        payloads.insert(neighbor_name.to_string(), neighbor_payload_sum);
        self.spec.multiply(&payloads)
    }

    fn multi_way_delta(
        &mut self,
        relation_name: &str,
        timestamp: i64,
        element: E,
        payload: S::P,
    ) -> S::V {
        // Bootstrap from the inserted element.
        let mut payloads: BTreeMap<String, S::P> = BTreeMap::new();
        payloads.insert(relation_name.to_string(), payload);
        let mut timestamps: HashMap<String, i64> = HashMap::new();
        timestamps.insert(relation_name.to_string(), timestamp);
        let mut elements: HashMap<String, E> = HashMap::new();
        elements.insert(relation_name.to_string(), element);

        let relation_order = self.build_relation_order(relation_name);
        self.recursive_join(&relation_order, payloads, timestamps, elements)
    }

    fn recursive_join(
        &mut self,
        relation_order: &[String],
        payloads: BTreeMap<String, S::P>,
        timestamps: HashMap<String, i64>,
        elements: HashMap<String, E>,
    ) -> S::V {
        if payloads.len() == relation_order.len() {
            self.total_matches += 1;
            return self.spec.multiply(&payloads);
        }
        let Some(next) = self.select_next_relation(relation_order, &payloads) else {
            return self.spec.zero();
        };
        let Some(driver) = self.select_driver_relation(&next, relation_order, &payloads) else {
            return self.spec.zero();
        };
        let Some(edge) = self.graph.edge(&driver, &next) else {
            return self.spec.zero();
        };
        let driver_ts = *timestamps.get(&driver).expect("driver bound");
        let Some((start, end)) = edge.window_bounds(driver_ts, &driver) else {
            return self.spec.zero();
        };
        let Some(driver_element) = elements.get(&driver) else {
            return self.spec.zero();
        };
        let query_key = (self.extractors[&driver].key)(driver_element);

        let Some(next_idx) = self.indices.get(&next).and_then(|m| m.get(&driver)) else {
            return self.spec.zero();
        };
        // Memoize the range query through the per-edge cache so
        // repeated `(neighbor, driver, key, start, end)` lookups
        // during recursive enumeration don't reprobe the CTrie.
        // Mirrors Java's MultiWayJoinState.queryRangeEntries
        // cache wrapper.
        let cache_key =
            RangeCacheKey::new(next.clone(), driver.clone(), query_key.clone(), start, end);
        let entries = match self.range_cache.get(&cache_key) {
            Some(cached) => cached.clone(),
            None => {
                let fresh = next_idx.query_range_entries(&[Some(query_key.clone())], start, end);
                self.range_cache.put(cache_key, fresh.clone());
                fresh
            }
        };
        if entries.is_empty() {
            return self.spec.zero();
        }

        let mut total = self.spec.zero();
        for entry in entries {
            let neighbor_ts = entry.timestamp;
            let count = entry.count;
            let record = entry.payload;
            if !self.is_compatible(&next, &record.element, neighbor_ts, &timestamps, &elements) {
                continue;
            }
            let mut ext_payloads = payloads.clone();
            ext_payloads.insert(next.clone(), record.payload.clone());
            let mut ext_timestamps = timestamps.clone();
            ext_timestamps.insert(next.clone(), neighbor_ts);
            let mut ext_elements = elements.clone();
            ext_elements.insert(next.clone(), record.element);
            let delta =
                self.recursive_join(relation_order, ext_payloads, ext_timestamps, ext_elements);
            for _ in 0..count {
                total = self.spec.add(&total, &delta);
            }
        }
        total
    }

    fn build_relation_order(&self, root: &str) -> Vec<String> {
        let mut order: Vec<String> = vec![root.to_string()];
        let mut seen: std::collections::HashSet<String> = [root.to_string()].into_iter().collect();
        for layer in self.view_plan.prefix_layers() {
            for r in &layer.relations {
                if seen.insert(r.clone()) {
                    order.push(r.clone());
                }
            }
        }
        for r in self.graph.relation_names() {
            if seen.insert(r.to_string()) {
                order.push(r.to_string());
            }
        }
        order
    }

    fn select_next_relation(
        &self,
        relation_order: &[String],
        payloads: &BTreeMap<String, S::P>,
    ) -> Option<String> {
        for r in relation_order {
            if payloads.contains_key(r) {
                continue;
            }
            // Must have an edge to at least one already-bound relation.
            for bound in payloads.keys() {
                if self.graph.edge(bound, r).is_some() {
                    return Some(r.clone());
                }
            }
        }
        None
    }

    fn select_driver_relation(
        &self,
        candidate: &str,
        relation_order: &[String],
        payloads: &BTreeMap<String, S::P>,
    ) -> Option<String> {
        for r in relation_order {
            if payloads.contains_key(r) && self.graph.edge(r, candidate).is_some() {
                return Some(r.clone());
            }
        }
        None
    }

    fn is_compatible(
        &self,
        candidate: &str,
        candidate_element: &E,
        candidate_ts: i64,
        bound_timestamps: &HashMap<String, i64>,
        bound_elements: &HashMap<String, E>,
    ) -> bool {
        for (bound, bound_ts) in bound_timestamps {
            let Some(edge) = self.graph.edge(bound, candidate) else {
                continue;
            };
            let Some((start, end)) = edge.window_bounds(*bound_ts, bound) else {
                return false;
            };
            if candidate_ts < start || candidate_ts > end {
                return false;
            }
            let Some(bound_element) = bound_elements.get(bound) else {
                return false;
            };
            let bound_key = (self.extractors[bound].key)(bound_element);
            let candidate_key = (self.extractors[candidate].key)(candidate_element);
            if bound_key != candidate_key {
                return false;
            }
        }
        true
    }

    fn index_element(&mut self, relation_name: &str, timestamp: i64, element: E, payload: S::P) {
        let record = JoinRecord::new(element.clone(), payload);
        let key = (self.extractors[relation_name].key)(&element);
        let Some(edge_map) = self.indices.get_mut(relation_name) else {
            return;
        };
        for idx in edge_map.values_mut() {
            idx.insert(std::slice::from_ref(&key), timestamp, record.clone());
        }
    }

    fn unindex_element(
        &mut self,
        relation_name: &str,
        timestamp: i64,
        element: &E,
        payload: &S::P,
    ) -> bool {
        let key = (self.extractors[relation_name].key)(element);
        let record = JoinRecord::new(element.clone(), payload.clone());
        let Some(edge_map) = self.indices.get_mut(relation_name) else {
            return false;
        };
        let mut any = false;
        for idx in edge_map.values_mut() {
            if idx.delete(std::slice::from_ref(&key), timestamp, &record) {
                any = true;
            }
        }
        any
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pair element type used by most tests.
    type Pair = (i32, i32);
    /// Pair-with-label used by triangle/star tests.
    type Labeled = (i32, &'static str);

    /// Convenience: build a count-extractor map for `Pair`-typed
    /// relations all keyed on the first component. Avoids the
    /// "different closures have different types" issue by going
    /// through `Box<dyn Fn>` (which is what `RelationExtractors`
    /// stores anyway).
    fn pair_count_extractors(
        names: &[&str],
    ) -> HashMap<String, RelationExtractors<Pair, i32, i64>> {
        let mut out = HashMap::new();
        for name in names {
            out.insert(
                (*name).to_string(),
                RelationExtractors::new(|e: &Pair| e.0, |_: &Pair| 1i64),
            );
        }
        out
    }

    /// Same shape but for `Labeled` elements.
    fn labeled_count_extractors(
        names: &[&str],
    ) -> HashMap<String, RelationExtractors<Labeled, i32, i64>> {
        let mut out = HashMap::new();
        for name in names {
            out.insert(
                (*name).to_string(),
                RelationExtractors::new(|e: &Labeled| e.0, |_: &Labeled| 1i64),
            );
        }
        out
    }

    // ─── Binary count ─────────────────────────────────────────────

    #[test]
    fn binary_multiway_count_equals_pairwise() {
        // Two relations A(x), B(x) with edge over key x.
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);

        let extractors = pair_count_extractors(&["A", "B"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);

        state.insert("A", 0, (1, 100));
        state.insert("A", 1, (1, 101));
        state.insert("B", 10, (1, 200));
        // Two A's (both key=1) match one B (key=1) → count = 2.
        assert_eq!(state.aggregate(), 2);
        assert_eq!(state.total_matches(), 2);
    }

    #[test]
    fn binary_multiway_evict_undoes_inserts() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);
        let extractors = pair_count_extractors(&["A", "B"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);

        state.insert("A", 0, (1, 100));
        state.insert("B", 10, (1, 200));
        assert_eq!(state.aggregate(), 1);

        let delta = state.delete("A", 0, (1, 100));
        assert_eq!(delta, -1);
        assert_eq!(state.aggregate(), 0);
    }

    #[test]
    fn binary_multiway_key_filters_matches() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);
        let extractors = pair_count_extractors(&["A", "B"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("A", 0, (1, 100));
        state.insert("A", 1, (2, 101));
        state.insert("B", 10, (1, 200));
        state.insert("B", 11, (2, 201));
        state.insert("B", 12, (3, 202)); // no match
                                         // Each A pairs only with its same-key B → 2 matches.
        assert_eq!(state.aggregate(), 2);
    }

    // ─── Triangle (3-way cyclic) ──────────────────────────────────

    #[test]
    fn triangle_multiway_count_matches_three_way_join() {
        // R1(x, y), R2(y, z), R3(x, z). Single join key per relation
        // (we pick one component) → still demonstrates 3-way recursive
        // join machinery.
        let mut g = JoinGraph::new();
        g.add_relation("R1", &["x", "y"]);
        g.add_relation("R2", &["y", "z"]);
        g.add_relation("R3", &["x", "z"]);
        // For this test we use the same key (y for R1↔R2, z for
        // R2↔R3, x for R1↔R3) by making each relation's key
        // extractor return the *first* attribute that participates
        // in the join with its partner. We approximate that here by
        // picking a single key per relation that's shared with all
        // its neighbors (forces the test data to use a consistent
        // value).
        g.add_edge("R1", "R2", 0, 1000);
        g.add_edge("R2", "R3", 0, 1000);
        g.add_edge("R1", "R3", 0, 1000);

        // Element = (key, label).
        let extractors = labeled_count_extractors(&["R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        // Insert three matching elements with the same key.
        state.insert("R1", 0, (42, "a"));
        state.insert("R2", 10, (42, "b"));
        state.insert("R3", 20, (42, "c"));
        assert_eq!(state.aggregate(), 1, "single triangle match expected");

        // A duplicate match: insert another R3 with the same key.
        state.insert("R3", 30, (42, "d"));
        // One more triangle (R1, R2, R3-new) → 2 total.
        assert_eq!(state.aggregate(), 2);
    }

    #[test]
    fn triangle_no_match_when_one_relation_empty() {
        let mut g = JoinGraph::new();
        g.add_relation("R1", &["x"]);
        g.add_relation("R2", &["x"]);
        g.add_relation("R3", &["x"]);
        g.add_edge("R1", "R2", 0, 1000);
        g.add_edge("R2", "R3", 0, 1000);
        g.add_edge("R1", "R3", 0, 1000);
        let extractors = labeled_count_extractors(&["R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("R1", 0, (1, "a"));
        state.insert("R2", 10, (1, "b"));
        // R3 never inserted → no triangle.
        assert_eq!(state.aggregate(), 0);
    }

    // ─── Star topology ────────────────────────────────────────────

    #[test]
    fn star_multiway_count_over_three_satellites() {
        // R0 connected to R1, R2, R3 (all share key x with R0).
        let mut g = JoinGraph::new();
        g.add_relation("R0", &["x"]);
        g.add_relation("R1", &["x"]);
        g.add_relation("R2", &["x"]);
        g.add_relation("R3", &["x"]);
        g.add_edge("R0", "R1", 0, 1000);
        g.add_edge("R0", "R2", 0, 1000);
        g.add_edge("R0", "R3", 0, 1000);
        let extractors = labeled_count_extractors(&["R0", "R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        // Insert one R0 + one of each satellite at the same key.
        state.insert("R0", 0, (1, "r0"));
        state.insert("R1", 10, (1, "r1"));
        state.insert("R2", 20, (1, "r2"));
        state.insert("R3", 30, (1, "r3"));
        assert_eq!(state.aggregate(), 1, "single star tuple expected");
    }

    // ─── MultiWaySumProduct ───────────────────────────────────────

    #[test]
    fn multiway_sum_product_over_binary_join() {
        use crate::operator::swag_join_specs::F64Bits;

        // Element type carries (key, payload) where payload uses
        // F64Bits so the element is Eq + Hash.
        type FElem = (i32, F64Bits);

        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);
        let mut extractors: HashMap<String, RelationExtractors<FElem, i32, F64Bits>> =
            HashMap::new();
        extractors.insert(
            "A".into(),
            RelationExtractors::new(|e: &FElem| e.0, |e: &FElem| e.1),
        );
        extractors.insert(
            "B".into(),
            RelationExtractors::new(|e: &FElem| e.0, |e: &FElem| e.1),
        );
        let mut state = MultiWayJoinState::with_extractors(g, MultiWaySumProduct, extractors);

        state.insert("A", 0, (1, F64Bits::from_f64(10.0)));
        state.insert("A", 1, (1, F64Bits::from_f64(5.0)));
        state.insert("B", 10, (1, F64Bits::from_f64(3.0)));
        state.insert("B", 11, (1, F64Bits::from_f64(4.0)));

        // Pairs joined within [0, 100]:
        //   (10, 3) → 30
        //   (10, 4) → 40
        //   (5, 3)  → 15
        //   (5, 4)  → 20
        // Total = 105.
        assert!((state.aggregate().to_f64() - 105.0).abs() < 1e-9);
    }

    // ─── Validation panics ────────────────────────────────────────

    #[test]
    #[should_panic(expected = "join graph must be connected")]
    fn rejects_disconnected_graph() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["y"]);
        // No edge → not connected.
        let extractors = pair_count_extractors(&["A", "B"]);
        let _ = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
    }

    #[test]
    #[should_panic(expected = "missing extractor")]
    fn rejects_missing_extractor() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);
        let mut extractors: HashMap<String, RelationExtractors<(i32,), i32, i64>> = HashMap::new();
        extractors.insert(
            "A".into(),
            RelationExtractors::new(|e: &(i32,)| e.0, |_: &(i32,)| 1i64),
        );
        // B missing.
        let _ = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
    }

    // ─── Clear ────────────────────────────────────────────────────

    #[test]
    fn clear_resets_state() {
        let mut g = JoinGraph::new();
        g.add_relation("A", &["x"]);
        g.add_relation("B", &["x"]);
        g.add_edge("A", "B", 0, 100);
        let extractors = pair_count_extractors(&["A", "B"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("A", 0, (1, 100));
        state.insert("B", 10, (1, 200));
        assert_eq!(state.aggregate(), 1);
        state.clear();
        assert_eq!(state.aggregate(), 0);
        assert_eq!(state.total_inserts(), 0);
        assert_eq!(state.total_matches(), 0);
        assert_eq!(state.range_cache_len(), 0, "clear empties the range cache");
        // Fresh inserts still work.
        state.insert("A", 0, (1, 100));
        state.insert("B", 10, (1, 200));
        assert_eq!(state.aggregate(), 1);
    }

    // ─── RangeCache integration ───────────────────────────────────
    //
    // The cache is scratch space WITHIN a single `compute_delta`
    // call: we populate it on the recursive range-query path, then
    // invalidate the mutated relation's entries before the next
    // insert/delete starts its own walk. So `range_cache_len()`
    // reads zero between operations from the outside — that's
    // intentional, mirroring Java's eager invalidation. The tests
    // below verify aggregate correctness under the cache, plus the
    // observable `range_cache_len() == 0` invariant after each
    // insert.

    #[test]
    fn range_cache_does_not_corrupt_recursive_join_aggregate() {
        // Triangle topology so the executor takes the
        // `recursive_join` path (binary fast path bypasses the
        // cache). Single matching tuple at key=1.
        let mut g = JoinGraph::new();
        g.add_relation("R1", &["x"]);
        g.add_relation("R2", &["x"]);
        g.add_relation("R3", &["x"]);
        g.add_edge("R1", "R2", 0, 1000);
        g.add_edge("R2", "R3", 0, 1000);
        g.add_edge("R1", "R3", 0, 1000);
        let extractors = labeled_count_extractors(&["R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("R1", 0, (1, "a"));
        state.insert("R2", 10, (1, "b"));
        state.insert("R3", 20, (1, "c"));
        assert_eq!(state.aggregate(), 1);
        // External observer always sees an empty cache between
        // operations — invalidation happens eagerly.
        assert_eq!(state.range_cache_len(), 0);
    }

    #[test]
    fn range_cache_invalidated_between_inserts() {
        let mut g = JoinGraph::new();
        g.add_relation("R1", &["x"]);
        g.add_relation("R2", &["x"]);
        g.add_relation("R3", &["x"]);
        g.add_edge("R1", "R2", 0, 1000);
        g.add_edge("R2", "R3", 0, 1000);
        g.add_edge("R1", "R3", 0, 1000);
        let extractors = labeled_count_extractors(&["R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("R1", 0, (1, "a"));
        state.insert("R2", 10, (1, "b"));
        state.insert("R3", 20, (1, "c"));
        assert_eq!(state.range_cache_len(), 0);

        // Adding a second R3 with the same key forms a second
        // triangle. If invalidation were broken the cached R3
        // queries from the first insert would mask the new R3
        // record and the aggregate would stay at 1.
        state.insert("R3", 30, (1, "d"));
        assert_eq!(state.aggregate(), 2);
        assert_eq!(state.range_cache_len(), 0);
    }

    #[test]
    fn range_cache_correctness_under_eviction() {
        // Star topology with 4 satellites — each satellite insert
        // routes through recursive_join, exercising the cache on
        // both populate + invalidate paths. Cross-check the
        // aggregate matches the F-IVM-correct answer.
        let mut g = JoinGraph::new();
        g.add_relation("R0", &["x"]);
        g.add_relation("R1", &["x"]);
        g.add_relation("R2", &["x"]);
        g.add_relation("R3", &["x"]);
        g.add_edge("R0", "R1", 0, 1000);
        g.add_edge("R0", "R2", 0, 1000);
        g.add_edge("R0", "R3", 0, 1000);
        let extractors = labeled_count_extractors(&["R0", "R1", "R2", "R3"]);
        let mut state = MultiWayJoinState::with_extractors(g, MultiWayCount, extractors);
        state.insert("R0", 0, (1, "r0"));
        state.insert("R1", 10, (1, "r1"));
        state.insert("R2", 20, (1, "r2"));
        // No full star tuple yet: only the central R0 + 2 satellites.
        // Layer-5a's F-IVM-correct multi_way_delta means each
        // satellite insert with all four relations present yields a
        // full delta, but until R3 arrives the running aggregate
        // stays at 0.
        assert_eq!(state.aggregate(), 0);
        state.insert("R3", 30, (1, "r3"));
        // Single matching star → 1.
        assert_eq!(state.aggregate(), 1);
        // Aggregate stays correct as we add more matching satellites.
        state.insert("R1", 40, (1, "r1b"));
        assert_eq!(state.aggregate(), 2);
        state.insert("R2", 50, (1, "r2b"));
        assert_eq!(state.aggregate(), 4);
    }
}
