//! SWAG join primitives — layer 1 of the F-IVM multi-way join port.
//!
//! Ports the small data-structure primitives from cqels-java's
//! `org.cqels.operator.aggregate.swag.join` package that the
//! downstream planner / multi-way join state will sit on top of:
//!
//! - [`KeyIndexedTimeIndex`] — equi-join index that nests one
//!   [`TimeIndex`] per join key for O(1) key lookup followed by a
//!   time-range query. Mirrors Java's `KeyIndexedTimeIndex<P>`.
//! - [`JoinRecord`] — wraps `(element, payload)`. Mirrors Java's
//!   `JoinRecord`.
//! - [`KeyExtractor`] — trait alias for `Fn(&T) -> K`, plus helper
//!   constructors `identity()` / `constant()`. Mirrors Java's
//!   `KeyExtractor<T>` functional interface.
//! - [`VariableOrder`] — variable ordering for multi-way join
//!   evaluation. Mirrors Java's `VariableOrder` value type.
//!
//! The downstream consumers (`WindowJoinState`, `MultiWayJoinState`,
//! `JoinGraph`, `FactorizedViewPlan`, the aggregate-spec hierarchy,
//! `WindowedJoinAggregateOperator`) account for ~5K extra Java lines
//! and are deferred to follow-up PRs.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use crate::operator::swag_ctrie::{RangeResult, TimeIndex};

// ─── JoinRecord ─────────────────────────────────────────────────────

/// Pairs a stream element with its extracted aggregation payload.
/// Mirrors Java's `JoinRecord`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinRecord<E, P> {
    pub element: E,
    pub payload: P,
}

impl<E, P> JoinRecord<E, P> {
    pub fn new(element: E, payload: P) -> Self {
        Self { element, payload }
    }
}

// ─── KeyExtractor ───────────────────────────────────────────────────

/// Trait alias for a deterministic join-key extractor.
///
/// Rust closures already satisfy this — the trait is provided as a
/// type alias for readability in long signatures. Use [`identity`] /
/// [`constant`] for the common no-extraction cases.
///
/// # Examples
///
/// ```
/// use cqels_core::operator::swag_join::{identity, constant};
///
/// let id = identity::<i32, i32>();
/// assert_eq!(id(&42), 42);
///
/// let one = constant::<&str, u8>(1);
/// assert_eq!(one(&"any"), 1);
/// assert_eq!(one(&"thing"), 1);
/// ```
pub trait KeyExtractor<T, K>: Fn(&T) -> K + Send + Sync + 'static {}
impl<T, K, F> KeyExtractor<T, K> for F where F: Fn(&T) -> K + Send + Sync + 'static {}

/// Builds a `KeyExtractor` that uses the element itself as the
/// join key. Mirrors Java's `KeyExtractor.identity()`.
pub fn identity<T, K>() -> Arc<dyn KeyExtractor<T, K, Output = K>>
where
    T: Clone + 'static,
    K: From<T> + Send + Sync + 'static,
{
    Arc::new(|t: &T| K::from(t.clone()))
}

/// Builds a `KeyExtractor` that always returns `value`. Effectively
/// disables key filtering (cross-product). Mirrors Java's
/// `KeyExtractor.constant()`.
pub fn constant<T, K>(value: K) -> Arc<dyn KeyExtractor<T, K, Output = K>>
where
    T: 'static,
    K: Clone + Send + Sync + 'static,
{
    Arc::new(move |_t: &T| value.clone())
}

// ─── VariableOrder ──────────────────────────────────────────────────

/// Method used to derive the variable order.
///
/// Mirrors Java's `VariableOrder.Method` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum VariableOrderMethod {
    /// GYO reduction to recover a join tree for an acyclic query.
    GyoJoinTree,
    /// Minimum-fill heuristic for cyclic queries.
    MinFill,
}

/// Variable ordering for multi-way join evaluation.
///
/// The `prefix_order` lists variables in the sequence they should be
/// bound during evaluation; the `elimination_order` is the reverse
/// (useful for width heuristics). `acyclic` indicates whether the
/// query is acyclic (and therefore admits a join tree); `join_tree`
/// is an adjacency map among variables. Mirrors Java's
/// `VariableOrder` value type.
#[derive(Clone, Debug)]
pub struct VariableOrder {
    prefix_order: Vec<String>,
    elimination_order: Vec<String>,
    acyclic: bool,
    join_tree: HashMap<String, HashSet<String>>,
    method: VariableOrderMethod,
}

impl VariableOrder {
    /// Builds a `VariableOrder`. The `elimination_order` should
    /// typically be `prefix_order` reversed but the caller is free to
    /// supply any ordering compatible with their variable-elimination
    /// algorithm.
    pub fn new(
        prefix_order: Vec<String>,
        elimination_order: Vec<String>,
        acyclic: bool,
        join_tree: HashMap<String, HashSet<String>>,
        method: VariableOrderMethod,
    ) -> Self {
        Self {
            prefix_order,
            elimination_order,
            acyclic,
            join_tree,
            method,
        }
    }

    pub fn prefix_order(&self) -> &[String] {
        &self.prefix_order
    }

    pub fn elimination_order(&self) -> &[String] {
        &self.elimination_order
    }

    pub fn is_acyclic(&self) -> bool {
        self.acyclic
    }

    pub fn join_tree(&self) -> &HashMap<String, HashSet<String>> {
        &self.join_tree
    }

    pub fn method(&self) -> VariableOrderMethod {
        self.method
    }
}

// ─── KeyIndexedTimeIndex ────────────────────────────────────────────

/// Key-indexed time-ordered index for equi-join with temporal
/// predicates. Maintains one [`TimeIndex<P>`] per join key, so
/// equi-join lookups are O(1) key + O(k log n) time, vs. the
/// O(K · k log n) cross-product over all keys via
/// [`Self::query_range`] (see method docs).
///
/// Mirrors Java's `KeyIndexedTimeIndex<P>`.
///
/// # Type parameters
///
/// - `K` — join key type. Java uses raw `Object`; the Rust port is
///   generic per usage site.
/// - `P` — payload type (bag-semantics aggregation; needs
///   `Eq + Hash + Clone`).
///
/// # Complexity
///
/// - `insert`: `O(log n)` (`n` = timestamps for this key).
/// - `evict`: `O(log n)`.
/// - `query_range_by_key`: `O(1)` key lookup + `O(k log n)` time
///   range, `k` = buckets in range.
/// - `query_range` (all keys): `O(K · k log n)`, `K` = distinct keys.
///
/// # Examples
///
/// ```
/// use cqels_core::operator::swag_join::KeyIndexedTimeIndex;
///
/// let mut idx: KeyIndexedTimeIndex<i64, &'static str> = KeyIndexedTimeIndex::new();
/// idx.insert(42, 1000, "order-A");
/// idx.insert(42, 1500, "order-B");
/// idx.insert(99, 1200, "order-C");
///
/// let r = idx.query_range_by_key(&42, 0, 2000);
/// assert_eq!(r.total_count, 2);
/// ```
#[derive(Debug)]
pub struct KeyIndexedTimeIndex<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    key_index: HashMap<K, TimeIndex<P>>,
    total_count: u64,
}

impl<K, P> KeyIndexedTimeIndex<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    /// Builds an empty index.
    pub fn new() -> Self {
        Self {
            key_index: HashMap::new(),
            total_count: 0,
        }
    }

    /// Inserts `payload` at `timestamp` under `join_key`.
    pub fn insert(&mut self, join_key: K, timestamp: i64, payload: P) {
        self.key_index
            .entry(join_key)
            .or_default()
            .insert(timestamp, payload);
        self.total_count += 1;
    }

    /// Removes one occurrence of `payload` at `timestamp` under
    /// `join_key`. Returns `true` if a payload was present.
    pub fn evict(&mut self, join_key: &K, timestamp: i64, payload: &P) -> bool {
        let Some(ti) = self.key_index.get_mut(join_key) else {
            return false;
        };
        let removed = ti.evict(timestamp, payload);
        if removed {
            self.total_count -= 1;
            if ti.is_empty() {
                self.key_index.remove(join_key);
            }
        }
        removed
    }

    /// Aggregates payloads in `[start, end]` for the given join key.
    /// Empty result for unknown keys.
    pub fn query_range_by_key(&self, join_key: &K, start: i64, end: i64) -> RangeResult<P> {
        let Some(ti) = self.key_index.get(join_key) else {
            return RangeResult::empty();
        };
        ti.query_range(start, end)
    }

    /// Aggregates payloads in `[start, end]` across **every** join
    /// key. Use for full-scan stats / cross-product semantics; for
    /// equi-join filtering, prefer [`Self::query_range_by_key`].
    pub fn query_range(&self, start: i64, end: i64) -> RangeResult<P> {
        if start > end {
            return RangeResult::empty();
        }
        let mut total = 0u64;
        let mut agg: HashMap<P, u64> = HashMap::new();
        for ti in self.key_index.values() {
            let r = ti.query_range(start, end);
            total += r.total_count;
            for (p, c) in r.payload_counts {
                *agg.entry(p).or_insert(0) += c;
            }
        }
        RangeResult::new(total, agg)
    }

    /// Total payload occurrences across every join key (including
    /// bag multiplicities).
    pub fn size(&self) -> u64 {
        self.total_count
    }

    /// Number of distinct join keys currently indexed.
    pub fn key_count(&self) -> usize {
        self.key_index.len()
    }

    /// Total bucket count across every per-key `TimeIndex`.
    pub fn bucket_count(&self) -> usize {
        self.key_index.values().map(|ti| ti.bucket_count()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// All currently-indexed join keys.
    pub fn join_keys(&self) -> impl Iterator<Item = &K> {
        self.key_index.keys()
    }

    /// Reference to the per-key time index, or `None` if the key is
    /// not present. Exposed for batch / parallel aggregation flows
    /// that iterate per-key without going through the public API.
    pub fn time_index_for_key(&self, join_key: &K) -> Option<&TimeIndex<P>> {
        self.key_index.get(join_key)
    }

    /// Clears every per-key index.
    pub fn clear(&mut self) {
        self.key_index.clear();
        self.total_count = 0;
    }

    /// Snapshot of the index's shape — key count, total elements,
    /// bucket distribution. Maps to Java's `IndexStats`.
    pub fn stats(&self) -> IndexStats {
        if self.key_index.is_empty() {
            return IndexStats::empty();
        }
        let mut min_buckets = usize::MAX;
        let mut max_buckets = usize::MIN;
        let mut min_elements = u64::MAX;
        let mut max_elements = u64::MIN;
        for ti in self.key_index.values() {
            let buckets = ti.bucket_count();
            let size = ti.size();
            min_buckets = min_buckets.min(buckets);
            max_buckets = max_buckets.max(buckets);
            min_elements = min_elements.min(size);
            max_elements = max_elements.max(size);
        }
        IndexStats {
            key_count: self.key_index.len(),
            total_elements: self.total_count,
            total_buckets: self.bucket_count(),
            min_buckets_per_key: min_buckets,
            max_buckets_per_key: max_buckets,
            min_elements_per_key: min_elements,
            max_elements_per_key: max_elements,
        }
    }
}

impl<K, P> Default for KeyIndexedTimeIndex<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics snapshot for [`KeyIndexedTimeIndex`]. Mirrors Java's
/// nested `IndexStats`.
#[derive(Clone, Debug)]
pub struct IndexStats {
    pub key_count: usize,
    pub total_elements: u64,
    pub total_buckets: usize,
    pub min_buckets_per_key: usize,
    pub max_buckets_per_key: usize,
    pub min_elements_per_key: u64,
    pub max_elements_per_key: u64,
}

impl IndexStats {
    fn empty() -> Self {
        Self {
            key_count: 0,
            total_elements: 0,
            total_buckets: 0,
            min_buckets_per_key: 0,
            max_buckets_per_key: 0,
            min_elements_per_key: 0,
            max_elements_per_key: 0,
        }
    }

    pub fn avg_buckets_per_key(&self) -> f64 {
        if self.key_count == 0 {
            0.0
        } else {
            self.total_buckets as f64 / self.key_count as f64
        }
    }

    pub fn avg_elements_per_key(&self) -> f64 {
        if self.key_count == 0 {
            0.0
        } else {
            self.total_elements as f64 / self.key_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── JoinRecord ────────────────────────────────────────────────

    #[test]
    fn join_record_equality_by_field_value() {
        let r1: JoinRecord<i32, &'static str> = JoinRecord::new(1, "a");
        let r2: JoinRecord<i32, &'static str> = JoinRecord::new(1, "a");
        let r3: JoinRecord<i32, &'static str> = JoinRecord::new(2, "a");
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    // ─── KeyExtractor ─────────────────────────────────────────────

    #[test]
    fn identity_extractor_returns_element() {
        let ext = identity::<i32, i32>();
        assert_eq!(ext(&42), 42);
        assert_eq!(ext(&-7), -7);
    }

    #[test]
    fn constant_extractor_returns_value() {
        let ext = constant::<&str, u8>(1);
        assert_eq!(ext(&"alpha"), 1);
        assert_eq!(ext(&"beta"), 1);
    }

    #[test]
    fn arbitrary_closure_satisfies_key_extractor_trait_alias() {
        // Sanity: a regular closure satisfies KeyExtractor automatically.
        let extract = |s: &String| s.len();
        // Use it like a normal Fn.
        assert_eq!(extract(&"hi".to_string()), 2);
    }

    // ─── VariableOrder ─────────────────────────────────────────────

    #[test]
    fn variable_order_round_trips_fields() {
        let prefix = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let elim = vec!["c".to_string(), "b".to_string(), "a".to_string()];
        let mut tree: HashMap<String, HashSet<String>> = HashMap::new();
        tree.entry("a".to_string())
            .or_default()
            .insert("b".to_string());
        let vo = VariableOrder::new(
            prefix.clone(),
            elim.clone(),
            true,
            tree,
            VariableOrderMethod::GyoJoinTree,
        );
        assert_eq!(vo.prefix_order(), prefix.as_slice());
        assert_eq!(vo.elimination_order(), elim.as_slice());
        assert!(vo.is_acyclic());
        assert_eq!(vo.method(), VariableOrderMethod::GyoJoinTree);
        assert!(vo.join_tree().contains_key("a"));
    }

    // ─── KeyIndexedTimeIndex ───────────────────────────────────────

    #[test]
    fn key_indexed_insert_and_query_by_key_filters_correctly() {
        let mut idx: KeyIndexedTimeIndex<i64, &'static str> = KeyIndexedTimeIndex::new();
        idx.insert(42, 1000, "a");
        idx.insert(42, 1500, "b");
        idx.insert(99, 1200, "c");

        let r42 = idx.query_range_by_key(&42, 0, 2000);
        assert_eq!(r42.total_count, 2);
        assert_eq!(r42.payload_counts.get("a"), Some(&1));
        assert_eq!(r42.payload_counts.get("b"), Some(&1));
        assert!(!r42.payload_counts.contains_key("c"));

        let r99 = idx.query_range_by_key(&99, 0, 2000);
        assert_eq!(r99.total_count, 1);
        assert_eq!(r99.payload_counts.get("c"), Some(&1));
    }

    #[test]
    fn key_indexed_query_range_aggregates_across_all_keys() {
        let mut idx: KeyIndexedTimeIndex<i64, &'static str> = KeyIndexedTimeIndex::new();
        idx.insert(1, 1000, "x");
        idx.insert(2, 1100, "y");
        idx.insert(3, 1200, "z");

        let r = idx.query_range(0, 2000);
        assert_eq!(r.total_count, 3);
        assert_eq!(r.payload_counts.len(), 3);
    }

    #[test]
    fn key_indexed_query_for_unknown_key_returns_empty() {
        let mut idx: KeyIndexedTimeIndex<i64, &'static str> = KeyIndexedTimeIndex::new();
        idx.insert(1, 1000, "x");

        let r = idx.query_range_by_key(&999, 0, 2000);
        assert_eq!(r.total_count, 0);
        assert!(r.payload_counts.is_empty());
    }

    #[test]
    fn key_indexed_evict_drops_empty_keys() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        idx.insert(1, 1000, 42);
        assert_eq!(idx.key_count(), 1);
        assert!(idx.evict(&1, 1000, &42));
        assert_eq!(idx.key_count(), 0, "empty key should be removed");
        assert!(idx.is_empty());
        // Second evict for a now-missing key is a no-op.
        assert!(!idx.evict(&1, 1000, &42));
    }

    #[test]
    fn key_indexed_evict_keeps_key_with_remaining_buckets() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        idx.insert(1, 1000, 42);
        idx.insert(1, 2000, 43);
        assert!(idx.evict(&1, 1000, &42));
        assert_eq!(idx.key_count(), 1);
        assert_eq!(idx.size(), 1);
    }

    #[test]
    fn key_indexed_time_range_filters_within_key() {
        let mut idx: KeyIndexedTimeIndex<i64, &'static str> = KeyIndexedTimeIndex::new();
        idx.insert(1, 100, "early");
        idx.insert(1, 1500, "mid");
        idx.insert(1, 3000, "late");

        let r = idx.query_range_by_key(&1, 500, 2000);
        assert_eq!(r.total_count, 1);
        assert_eq!(r.payload_counts.get("mid"), Some(&1));
    }

    #[test]
    fn key_indexed_join_keys_iterator_covers_all_keys() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        idx.insert(1, 1000, 1);
        idx.insert(2, 1100, 2);
        idx.insert(3, 1200, 3);
        let mut keys: Vec<i64> = idx.join_keys().copied().collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn key_indexed_stats_reflect_distribution() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        // Key 1: 3 buckets, 4 elements (one bucket has multiplicity 2).
        idx.insert(1, 100, 10);
        idx.insert(1, 100, 10);
        idx.insert(1, 200, 11);
        idx.insert(1, 300, 12);
        // Key 2: 1 bucket, 1 element.
        idx.insert(2, 500, 20);

        let s = idx.stats();
        assert_eq!(s.key_count, 2);
        assert_eq!(s.total_elements, 5);
        assert_eq!(s.total_buckets, 4);
        assert_eq!(s.min_buckets_per_key, 1);
        assert_eq!(s.max_buckets_per_key, 3);
        assert_eq!(s.min_elements_per_key, 1);
        assert_eq!(s.max_elements_per_key, 4);
        // Avg = (1 + 3) / 2 = 2.0 buckets/key.
        assert!((s.avg_buckets_per_key() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn key_indexed_stats_empty_is_all_zero() {
        let idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        let s = idx.stats();
        assert_eq!(s.key_count, 0);
        assert_eq!(s.total_elements, 0);
        assert_eq!(s.total_buckets, 0);
        assert_eq!(s.avg_buckets_per_key(), 0.0);
        assert_eq!(s.avg_elements_per_key(), 0.0);
    }

    #[test]
    fn key_indexed_clear_resets_everything() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        idx.insert(1, 100, 10);
        idx.insert(2, 200, 20);
        idx.clear();
        assert!(idx.is_empty());
        assert_eq!(idx.key_count(), 0);
    }

    #[test]
    fn key_indexed_time_index_for_key_exposes_underlying_index() {
        let mut idx: KeyIndexedTimeIndex<i64, i32> = KeyIndexedTimeIndex::new();
        idx.insert(1, 100, 42);
        let ti = idx
            .time_index_for_key(&1)
            .expect("key 1 should have a time index");
        assert_eq!(ti.size(), 1);
        assert!(idx.time_index_for_key(&999).is_none());
    }
}
