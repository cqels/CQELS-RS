//! SWAG CTrie index for factorized multi-way joins.
//!
//! Provides [`TimeIndex`] — a time-bucketed range index with bag
//! semantics — and [`CTrieIndex`] — a fixed-depth trie that nests
//! one `TimeIndex` per leaf, supporting wildcard partial-key queries.
//! Together they form the indexing primitive behind cqels-java's
//! `org.cqels.operator.aggregate.swag.join` factorized join planner.
//!
//! Scope: the index data structures only. The downstream multi-way
//! join planner, F-IVM delta rules, and parallel aggregator
//! (`MultiWayJoinState`, `FactorizedViewPlan`, `WindowedJoinAggregateOperator`
//! on the Java side) are deferred — they depend on these primitives
//! but introduce 5K+ additional lines of orthogonal logic.
//!
//! Differences vs Java
//! - **Single-threaded.** Java uses `ConcurrentSkipListMap` for
//!   thread-safe access. The Rust port uses [`BTreeMap`] — every
//!   existing operator in this crate is driven from one task and
//!   pays no synchronization cost. If we later need an MVCC variant
//!   we can wrap this in `Arc<RwLock<...>>` or port the
//!   `SnapshotMap` family separately.
//! - **Typed keys.** Java uses raw `Object` so a single trie can mix
//!   integer and string keys. The Rust port is generic over the key
//!   type `K`, which keeps the API safer at the cost of one trie
//!   instance per key-type mix.
//!
//! # Examples
//!
//! ```
//! use cqels_core::operator::swag_ctrie::CTrieIndex;
//!
//! // Index likes by (user, post). Query: all likes for user 1 in
//! // the time window [0, 2000].
//! let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
//! idx.insert(&[1, 100], 1000, "like-A");
//! idx.insert(&[1, 200], 1500, "like-B");
//! idx.insert(&[2, 100], 1800, "like-C");
//!
//! let r = idx.query_range(&[Some(1), None], 0, 2000);
//! assert_eq!(r.total_count, 2);
//! ```

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

// ─── TimeIndexEntry ─────────────────────────────────────────────────

/// A single `(timestamp, payload, multiplicity)` triple emitted by
/// `query_range_entries` style methods. Mirrors Java's
/// `TimeIndexEntry<P>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimeIndexEntry<P> {
    pub timestamp: i64,
    pub payload: P,
    pub count: u64,
}

impl<P> TimeIndexEntry<P> {
    pub fn new(timestamp: i64, payload: P, count: u64) -> Self {
        Self {
            timestamp,
            payload,
            count,
        }
    }
}

// ─── RangeResult ────────────────────────────────────────────────────

/// Result of an aggregating range query: total count across all
/// matched timestamps, plus per-payload multiplicities. Mirrors
/// Java's `TimeIndex.RangeResult<P>`.
#[derive(Clone, Debug)]
pub struct RangeResult<P> {
    pub total_count: u64,
    pub payload_counts: HashMap<P, u64>,
}

impl<P> RangeResult<P> {
    pub fn new(total_count: u64, payload_counts: HashMap<P, u64>) -> Self {
        Self {
            total_count,
            payload_counts,
        }
    }

    pub fn empty() -> Self {
        Self {
            total_count: 0,
            payload_counts: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

// ─── Bucket ─────────────────────────────────────────────────────────

/// Payloads at a single timestamp, with multiplicity (bag semantics).
#[derive(Clone, Debug)]
struct Bucket<P>
where
    P: Eq + Hash,
{
    counts: HashMap<P, u64>,
    total_count: u64,
}

impl<P> Bucket<P>
where
    P: Eq + Hash + Clone,
{
    fn new() -> Self {
        Self {
            counts: HashMap::new(),
            total_count: 0,
        }
    }

    fn add(&mut self, payload: P) {
        *self.counts.entry(payload).or_insert(0) += 1;
        self.total_count += 1;
    }

    /// Returns `true` if a payload occurrence was removed.
    fn remove(&mut self, payload: &P) -> bool {
        let Some(c) = self.counts.get_mut(payload) else {
            return false;
        };
        if *c == 0 {
            return false;
        }
        *c -= 1;
        if *c == 0 {
            self.counts.remove(payload);
        }
        self.total_count -= 1;
        true
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

// ─── TimeIndex ──────────────────────────────────────────────────────

/// Time-bucketed index over payloads with bag semantics.
///
/// Maintains a sorted map `BTreeMap<timestamp, Bucket<P>>`. Range
/// queries return aggregated multiplicities; `delete_range` evicts
/// whole buckets in one call (the canonical sliding-window
/// retraction primitive on the Java side).
#[derive(Clone, Debug)]
pub struct TimeIndex<P>
where
    P: Eq + Hash + Clone,
{
    index: BTreeMap<i64, Bucket<P>>,
}

impl<P> TimeIndex<P>
where
    P: Eq + Hash + Clone,
{
    /// Builds an empty index.
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }

    /// Inserts a single occurrence of `payload` at `timestamp`.
    pub fn insert(&mut self, timestamp: i64, payload: P) {
        self.index
            .entry(timestamp)
            .or_insert_with(Bucket::new)
            .add(payload);
    }

    /// Removes one occurrence of `payload` at `timestamp`. Returns
    /// `true` if a payload was present and removed.
    pub fn evict(&mut self, timestamp: i64, payload: &P) -> bool {
        let Some(bucket) = self.index.get_mut(&timestamp) else {
            return false;
        };
        let removed = bucket.remove(payload);
        if bucket.is_empty() {
            self.index.remove(&timestamp);
        }
        removed
    }

    /// Aggregates all payloads with timestamps in `[start, end]`
    /// (inclusive both sides). Returns a `RangeResult` with total
    /// count + per-payload counts.
    pub fn query_range(&self, start: i64, end: i64) -> RangeResult<P> {
        if start > end {
            return RangeResult::empty();
        }
        let mut total = 0u64;
        let mut agg: HashMap<P, u64> = HashMap::new();
        for (_ts, bucket) in self.index.range(start..=end) {
            total += bucket.total_count;
            for (p, c) in &bucket.counts {
                *agg.entry(p.clone()).or_insert(0) += c;
            }
        }
        RangeResult::new(total, agg)
    }

    /// Returns one `TimeIndexEntry` per `(timestamp, payload)` pair
    /// in the inclusive range, preserving the per-bucket multiplicity.
    /// Ordered by timestamp ascending.
    pub fn query_range_entries(&self, start: i64, end: i64) -> Vec<TimeIndexEntry<P>> {
        if start > end {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (ts, bucket) in self.index.range(start..=end) {
            for (p, c) in &bucket.counts {
                out.push(TimeIndexEntry::new(*ts, p.clone(), *c));
            }
        }
        out
    }

    /// Deletes every payload with a timestamp in `[start, end]` and
    /// returns the deleted aggregate. Matches Java's
    /// `TimeIndex.deleteRange` semantics (whole-bucket deletion).
    pub fn delete_range(&mut self, start: i64, end: i64) -> RangeResult<P> {
        if start > end {
            return RangeResult::empty();
        }
        // Collect timestamps first (BTreeMap doesn't support range-
        // drain in stable Rust as of 1.83). Then drain each bucket
        // in a second pass.
        let keys: Vec<i64> = self.index.range(start..=end).map(|(ts, _)| *ts).collect();
        let mut total = 0u64;
        let mut agg: HashMap<P, u64> = HashMap::new();
        for ts in keys {
            let bucket = self.index.remove(&ts).expect("just collected");
            total += bucket.total_count;
            for (p, c) in bucket.counts {
                *agg.entry(p).or_insert(0) += c;
            }
        }
        RangeResult::new(total, agg)
    }

    /// Total payload occurrences across every bucket.
    pub fn size(&self) -> u64 {
        self.index.values().map(|b| b.total_count).sum()
    }

    /// Number of distinct timestamps (buckets) in the index.
    pub fn bucket_count(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Clears every bucket. Drops payloads.
    pub fn clear(&mut self) {
        self.index.clear();
    }
}

impl<P> Default for TimeIndex<P>
where
    P: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

// ─── CTrieIndex ─────────────────────────────────────────────────────

/// Fixed-depth trie indexing payloads under composite keys, with a
/// `TimeIndex<P>` at each leaf. Mirrors Java's `CTrieIndex<P>` from
/// the SWAG join package.
///
/// Each trie node holds a `HashMap<K, TrieNode<...>>`; the leaves at
/// depth `key_count` carry a `TimeIndex<P>`. Wildcard queries pass
/// `None` for any key position to scan all children at that depth.
///
/// # Complexity
///
/// - `insert`: `O(k + log m)` (`k` = `key_count`, `m` = distinct
///   timestamps at the matching leaf).
/// - Exact-key `query_range`: `O(k + r log m)` (`r` = buckets in
///   `[start, end]`).
/// - `w`-wildcard `query_range`: `O(k + b^w · r log m)` (`b` =
///   branching factor at wildcard positions).
#[derive(Debug)]
pub struct CTrieIndex<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    root: TrieNode<K, P>,
    key_count: usize,
    total_count: u64,
}

#[derive(Debug)]
struct TrieNode<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    children: HashMap<K, TrieNode<K, P>>,
    time_index: Option<TimeIndex<P>>,
}

impl<K, P> TrieNode<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            time_index: None,
        }
    }
}

impl<K, P> CTrieIndex<K, P>
where
    K: Eq + Hash + Clone,
    P: Eq + Hash + Clone,
{
    /// Creates a new index with the given composite-key depth.
    ///
    /// # Panics
    ///
    /// Panics if `key_count == 0`.
    pub fn new(key_count: usize) -> Self {
        assert!(key_count > 0, "key_count must be positive");
        Self {
            root: TrieNode::new(),
            key_count,
            total_count: 0,
        }
    }

    /// Inserts `payload` under composite key `keys` at the given
    /// `timestamp`.
    ///
    /// # Panics
    ///
    /// Panics if `keys.len() != key_count`.
    pub fn insert(&mut self, keys: &[K], timestamp: i64, payload: P) {
        self.validate_keys(keys);
        let mut node = &mut self.root;
        for k in keys {
            node = node.children.entry(k.clone()).or_insert_with(TrieNode::new);
        }
        let time_index = node.time_index.get_or_insert_with(TimeIndex::new);
        time_index.insert(timestamp, payload);
        self.total_count += 1;
    }

    /// Aggregates all payloads in `[start, end]` whose composite key
    /// matches `partial_keys`. A `None` at position `i` matches any
    /// key value at depth `i`.
    pub fn query_range(&self, partial_keys: &[Option<K>], start: i64, end: i64) -> RangeResult<P> {
        self.validate_partial_keys(partial_keys);
        Self::query_range_recursive(&self.root, partial_keys, 0, start, end, self.key_count)
    }

    /// Returns per-`(timestamp, payload)` entries for the same shape
    /// of query as [`query_range`](Self::query_range).
    pub fn query_range_entries(
        &self,
        partial_keys: &[Option<K>],
        start: i64,
        end: i64,
    ) -> Vec<TimeIndexEntry<P>> {
        self.validate_partial_keys(partial_keys);
        let mut out = Vec::new();
        Self::query_range_entries_recursive(
            &self.root,
            partial_keys,
            0,
            start,
            end,
            self.key_count,
            &mut out,
        );
        out
    }

    fn query_range_recursive(
        node: &TrieNode<K, P>,
        partial_keys: &[Option<K>],
        depth: usize,
        start: i64,
        end: i64,
        key_count: usize,
    ) -> RangeResult<P> {
        if depth == key_count {
            return match &node.time_index {
                Some(ti) => ti.query_range(start, end),
                None => RangeResult::empty(),
            };
        }
        match &partial_keys[depth] {
            None => {
                // Wildcard: aggregate across all children at this
                // depth.
                let mut total = 0u64;
                let mut agg: HashMap<P, u64> = HashMap::new();
                for child in node.children.values() {
                    let sub = Self::query_range_recursive(
                        child,
                        partial_keys,
                        depth + 1,
                        start,
                        end,
                        key_count,
                    );
                    total += sub.total_count;
                    for (p, c) in sub.payload_counts {
                        *agg.entry(p).or_insert(0) += c;
                    }
                }
                RangeResult::new(total, agg)
            }
            Some(k) => match node.children.get(k) {
                Some(child) => Self::query_range_recursive(
                    child,
                    partial_keys,
                    depth + 1,
                    start,
                    end,
                    key_count,
                ),
                None => RangeResult::empty(),
            },
        }
    }

    fn query_range_entries_recursive(
        node: &TrieNode<K, P>,
        partial_keys: &[Option<K>],
        depth: usize,
        start: i64,
        end: i64,
        key_count: usize,
        out: &mut Vec<TimeIndexEntry<P>>,
    ) {
        if depth == key_count {
            if let Some(ti) = &node.time_index {
                out.extend(ti.query_range_entries(start, end));
            }
            return;
        }
        match &partial_keys[depth] {
            None => {
                for child in node.children.values() {
                    Self::query_range_entries_recursive(
                        child,
                        partial_keys,
                        depth + 1,
                        start,
                        end,
                        key_count,
                        out,
                    );
                }
            }
            Some(k) => {
                if let Some(child) = node.children.get(k) {
                    Self::query_range_entries_recursive(
                        child,
                        partial_keys,
                        depth + 1,
                        start,
                        end,
                        key_count,
                        out,
                    );
                }
            }
        }
    }

    /// Deletes a single `(keys, timestamp, payload)` occurrence.
    /// Returns `true` if it was present.
    ///
    /// # Panics
    ///
    /// Panics if `keys.len() != key_count`.
    pub fn delete(&mut self, keys: &[K], timestamp: i64, payload: &P) -> bool {
        self.validate_keys(keys);
        // Walk down to the leaf; bail if any segment is missing.
        let mut node = &mut self.root;
        for k in keys {
            let Some(child) = node.children.get_mut(k) else {
                return false;
            };
            node = child;
        }
        let removed = match &mut node.time_index {
            Some(ti) => ti.evict(timestamp, payload),
            None => false,
        };
        if removed {
            self.total_count -= 1;
        }
        removed
    }

    /// Deletes every payload in `[start, end]` that matches
    /// `partial_keys`. Returns the number of payloads removed.
    pub fn delete_range(&mut self, partial_keys: &[Option<K>], start: i64, end: i64) -> u64 {
        self.validate_partial_keys(partial_keys);
        let key_count = self.key_count;
        let deleted =
            Self::delete_range_recursive(&mut self.root, partial_keys, 0, start, end, key_count);
        self.total_count = self.total_count.saturating_sub(deleted);
        deleted
    }

    fn delete_range_recursive(
        node: &mut TrieNode<K, P>,
        partial_keys: &[Option<K>],
        depth: usize,
        start: i64,
        end: i64,
        key_count: usize,
    ) -> u64 {
        if depth == key_count {
            return match &mut node.time_index {
                Some(ti) => ti.delete_range(start, end).total_count,
                None => 0,
            };
        }
        match &partial_keys[depth] {
            None => {
                let mut total = 0u64;
                for child in node.children.values_mut() {
                    total += Self::delete_range_recursive(
                        child,
                        partial_keys,
                        depth + 1,
                        start,
                        end,
                        key_count,
                    );
                }
                total
            }
            Some(k) => match node.children.get_mut(k) {
                Some(child) => Self::delete_range_recursive(
                    child,
                    partial_keys,
                    depth + 1,
                    start,
                    end,
                    key_count,
                ),
                None => 0,
            },
        }
    }

    /// Total payload occurrences across the whole trie.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Composite-key depth of this trie.
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Clears every child + every leaf time-index.
    pub fn clear(&mut self) {
        self.root.children.clear();
        self.root.time_index = None;
        self.total_count = 0;
    }

    fn validate_keys(&self, keys: &[K]) {
        assert_eq!(
            keys.len(),
            self.key_count,
            "expected {} keys, got {}",
            self.key_count,
            keys.len(),
        );
    }

    fn validate_partial_keys(&self, partial_keys: &[Option<K>]) {
        assert_eq!(
            partial_keys.len(),
            self.key_count,
            "expected {} partial keys, got {}",
            self.key_count,
            partial_keys.len(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── TimeIndex tests ────────────────────────────────────────────

    #[test]
    fn time_index_insert_and_range_aggregates() {
        let mut idx: TimeIndex<&'static str> = TimeIndex::new();
        idx.insert(100, "a");
        idx.insert(100, "a");
        idx.insert(100, "b");
        idx.insert(200, "a");
        idx.insert(500, "c");

        let r = idx.query_range(0, 250);
        assert_eq!(r.total_count, 4);
        assert_eq!(r.payload_counts.get("a"), Some(&3));
        assert_eq!(r.payload_counts.get("b"), Some(&1));
        assert!(!r.payload_counts.contains_key("c"));
    }

    #[test]
    fn time_index_evict_decrements_multiplicity() {
        let mut idx: TimeIndex<i32> = TimeIndex::new();
        idx.insert(100, 1);
        idx.insert(100, 1);
        assert_eq!(idx.size(), 2);
        assert!(idx.evict(100, &1));
        assert_eq!(idx.size(), 1);
        assert!(idx.evict(100, &1));
        assert_eq!(idx.size(), 0);
        // Empty bucket should be dropped.
        assert!(!idx.evict(100, &1));
        assert!(idx.is_empty());
    }

    #[test]
    fn time_index_delete_range_returns_aggregate() {
        let mut idx: TimeIndex<&'static str> = TimeIndex::new();
        idx.insert(100, "a");
        idx.insert(200, "b");
        idx.insert(300, "c");
        let deleted = idx.delete_range(150, 250);
        assert_eq!(deleted.total_count, 1);
        assert_eq!(deleted.payload_counts.get("b"), Some(&1));
        // Surviving buckets at 100 and 300 untouched.
        assert_eq!(idx.size(), 2);
        assert_eq!(idx.bucket_count(), 2);
    }

    #[test]
    fn time_index_query_range_entries_preserves_multiplicity() {
        let mut idx: TimeIndex<&'static str> = TimeIndex::new();
        idx.insert(100, "a");
        idx.insert(100, "a");
        idx.insert(200, "b");
        let entries = idx.query_range_entries(0, 250);
        // Two distinct (ts, payload) entries: (100, "a", 2), (200, "b", 1).
        assert_eq!(entries.len(), 2);
        let entry_a = entries.iter().find(|e| e.payload == "a").unwrap();
        assert_eq!(entry_a.count, 2);
        let entry_b = entries.iter().find(|e| e.payload == "b").unwrap();
        assert_eq!(entry_b.count, 1);
    }

    #[test]
    fn time_index_inverted_range_returns_empty() {
        let mut idx: TimeIndex<i32> = TimeIndex::new();
        idx.insert(100, 1);
        let r = idx.query_range(500, 100);
        assert!(r.is_empty());
        let entries = idx.query_range_entries(500, 100);
        assert!(entries.is_empty());
        let deleted = idx.delete_range(500, 100);
        assert!(deleted.is_empty());
    }

    // ─── CTrieIndex tests ───────────────────────────────────────────

    #[test]
    fn ctrie_insert_and_full_key_query() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[1, 100], 1500, "y");
        idx.insert(&[2, 100], 1800, "z");
        assert_eq!(idx.total_count(), 3);

        // Exact key (1, 100) in [0, 2000] should return only x + y.
        let r = idx.query_range(&[Some(1), Some(100)], 0, 2000);
        assert_eq!(r.total_count, 2);
        assert_eq!(r.payload_counts.get("x"), Some(&1));
        assert_eq!(r.payload_counts.get("y"), Some(&1));
        assert!(!r.payload_counts.contains_key("z"));
    }

    #[test]
    fn ctrie_wildcard_first_key_aggregates_children() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[2, 100], 1100, "y");
        idx.insert(&[3, 100], 1200, "z");
        idx.insert(&[3, 200], 1300, "w");

        // [None, Some(100)] = any user, post 100.
        let r = idx.query_range(&[None, Some(100)], 0, 2000);
        assert_eq!(r.total_count, 3);
        assert_eq!(r.payload_counts.get("x"), Some(&1));
        assert_eq!(r.payload_counts.get("y"), Some(&1));
        assert_eq!(r.payload_counts.get("z"), Some(&1));
        assert!(!r.payload_counts.contains_key("w"));
    }

    #[test]
    fn ctrie_wildcard_second_key_aggregates_children() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[1, 200], 1100, "y");
        idx.insert(&[2, 100], 1200, "z");

        let r = idx.query_range(&[Some(1), None], 0, 2000);
        assert_eq!(r.total_count, 2);
        assert_eq!(r.payload_counts.get("x"), Some(&1));
        assert_eq!(r.payload_counts.get("y"), Some(&1));
    }

    #[test]
    fn ctrie_double_wildcard_is_full_scan() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[2, 200], 1100, "y");
        idx.insert(&[3, 300], 1200, "z");

        let r = idx.query_range(&[None, None], 0, 2000);
        assert_eq!(r.total_count, 3);
    }

    #[test]
    fn ctrie_time_range_filters_results() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(1);
        idx.insert(&[1], 100, "a");
        idx.insert(&[1], 500, "b");
        idx.insert(&[1], 1500, "c");

        let r = idx.query_range(&[Some(1)], 200, 1000);
        assert_eq!(r.total_count, 1);
        assert_eq!(r.payload_counts.get("b"), Some(&1));
    }

    #[test]
    fn ctrie_delete_decrements_total_count() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[1, 100], 1500, "y");
        assert_eq!(idx.total_count(), 2);
        assert!(idx.delete(&[1, 100], 1000, &"x"));
        assert_eq!(idx.total_count(), 1);
        assert!(!idx.delete(&[1, 100], 1000, &"x"));
        assert_eq!(idx.total_count(), 1);
        // Non-existent key path.
        assert!(!idx.delete(&[9, 9], 1000, &"x"));
    }

    #[test]
    fn ctrie_delete_range_with_wildcard() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.insert(&[1, 200], 1100, "y");
        idx.insert(&[2, 100], 1200, "z");
        idx.insert(&[2, 200], 1300, "w");

        let removed = idx.delete_range(&[None, None], 1100, 1200);
        assert_eq!(removed, 2);
        assert_eq!(idx.total_count(), 2);

        // Confirm remaining payloads.
        let r = idx.query_range(&[None, None], 0, 2000);
        assert_eq!(r.total_count, 2);
        assert!(r.payload_counts.contains_key("x"));
        assert!(r.payload_counts.contains_key("w"));
    }

    #[test]
    fn ctrie_clear_resets_everything() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(2);
        idx.insert(&[1, 100], 1000, "x");
        idx.clear();
        assert_eq!(idx.total_count(), 0);
        let r = idx.query_range(&[None, None], 0, 2000);
        assert!(r.is_empty());
    }

    #[test]
    fn ctrie_query_range_entries_preserves_multiplicity_across_wildcard() {
        let mut idx: CTrieIndex<i64, &'static str> = CTrieIndex::new(1);
        idx.insert(&[1], 1000, "x");
        idx.insert(&[1], 1000, "x"); // duplicate at same timestamp
        idx.insert(&[2], 1000, "x");

        let entries = idx.query_range_entries(&[None], 0, 2000);
        // Two distinct trie leaves under key=1 and key=2, each with
        // their own bucket at ts=1000. Entries are
        // (ts=1000, payload="x", count={2,1}). Total combined count
        // across entries = 3.
        let total: u64 = entries.iter().map(|e| e.count).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn ctrie_string_keys_work_too() {
        let mut idx: CTrieIndex<String, i32> = CTrieIndex::new(2);
        idx.insert(&["alice".into(), "post-1".into()], 1000, 42);
        idx.insert(&["bob".into(), "post-1".into()], 1100, 43);
        let r = idx.query_range(&[None, Some("post-1".into())], 0, 2000);
        assert_eq!(r.total_count, 2);
    }

    #[test]
    #[should_panic(expected = "key_count must be positive")]
    fn ctrie_rejects_zero_key_count() {
        let _: CTrieIndex<i64, i32> = CTrieIndex::new(0);
    }

    #[test]
    #[should_panic(expected = "expected 2 keys, got 1")]
    fn ctrie_validates_insert_arity() {
        let mut idx: CTrieIndex<i64, i32> = CTrieIndex::new(2);
        idx.insert(&[1], 1000, 42);
    }

    #[test]
    #[should_panic(expected = "expected 2 partial keys, got 3")]
    fn ctrie_validates_query_arity() {
        let idx: CTrieIndex<i64, i32> = CTrieIndex::new(2);
        let _ = idx.query_range(&[Some(1), None, Some(3)], 0, 1000);
    }
}
