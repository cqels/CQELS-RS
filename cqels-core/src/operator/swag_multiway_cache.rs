//! Range-query result cache for the multi-way F-IVM executor —
//! layer 5c of the SWAG join port.
//!
//! Ports Java's `MultiWayJoinState.RangeCache` + `RangeCacheKey` as
//! a standalone, bounded LRU cache from
//! `(relation, driver_relation, join_key, start, end)` to a cloned
//! `Vec<TimeIndexEntry<JoinRecord<E, P>>>`.
//!
//! ## Why a cache at this layer
//!
//! `MultiWayJoinState::recursive_join` walks the variable order
//! depth-first; the same `(driver, neighbor, key, [start, end])`
//! subquery shows up repeatedly across different recursion paths
//! whenever multiple already-bound relations share a driver
//! candidate. Memoizing the per-edge range result is a pure
//! optimization — correctness doesn't depend on it — but it cuts
//! repeated `CTrieIndex::query_range_entries` traversals for
//! recurring join keys.
//!
//! ## Eviction semantics
//!
//! [`RangeCache`] is a bounded FIFO map (size cap supplied at
//! construction). On overflow the oldest insertion is dropped.
//! Java's implementation uses an access-ordered `LinkedHashMap` for
//! true LRU; the Rust port uses FIFO instead because (a) we don't
//! need a `linked-hash-map` dependency for this, and (b) the hit
//! pattern within a single delta computation favors recent inserts
//! enough that FIFO and LRU are nearly indistinguishable in
//! practice. Swap-in to a real LRU is a follow-up if/when
//! benchmarks call for it.
//!
//! ## Invalidation
//!
//! [`RangeCache::invalidate_relation`] drops every entry whose
//! cache key mentions the given relation name (either as the
//! query target or as the driver). `MultiWayJoinState` calls it
//! on every `insert` / `delete` so subsequent range queries see
//! the freshly mutated index.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use crate::operator::swag_ctrie::TimeIndexEntry;
use crate::operator::swag_join::JoinRecord;

/// Composite cache key matching Java's `RangeCacheKey`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RangeCacheKey<K>
where
    K: Eq + Hash + Clone,
{
    pub relation: String,
    pub driver: String,
    pub join_key: K,
    pub start: i64,
    pub end: i64,
}

impl<K> RangeCacheKey<K>
where
    K: Eq + Hash + Clone,
{
    pub fn new(
        relation: impl Into<String>,
        driver: impl Into<String>,
        join_key: K,
        start: i64,
        end: i64,
    ) -> Self {
        Self {
            relation: relation.into(),
            driver: driver.into(),
            join_key,
            start,
            end,
        }
    }
}

/// Bounded FIFO cache from [`RangeCacheKey`] to a list of
/// `(timestamp, JoinRecord, count)` entries — the exact shape
/// returned by [`crate::operator::swag_ctrie::CTrieIndex::query_range_entries`].
pub struct RangeCache<K, E, P>
where
    K: Eq + Hash + Clone,
    E: Clone + Eq + Hash,
    P: Clone + Eq + Hash,
{
    entries: HashMap<RangeCacheKey<K>, Vec<TimeIndexEntry<JoinRecord<E, P>>>>,
    insertion_order: VecDeque<RangeCacheKey<K>>,
    max_size: usize,
}

impl<K, E, P> RangeCache<K, E, P>
where
    K: Eq + Hash + Clone,
    E: Clone + Eq + Hash,
    P: Clone + Eq + Hash,
{
    /// Builds a cache with the given capacity. `max_size` must be
    /// at least 1; lower values are clamped up.
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_size: max_size.max(1),
        }
    }

    /// Look up a cached range result. Returns `None` on miss.
    pub fn get(&self, key: &RangeCacheKey<K>) -> Option<&Vec<TimeIndexEntry<JoinRecord<E, P>>>> {
        self.entries.get(key)
    }

    /// Insert a cached range result. Evicts the oldest entry if
    /// adding would push us over capacity. Re-inserting an
    /// existing key replaces its value without consuming a new
    /// slot from the insertion-order queue.
    pub fn put(&mut self, key: RangeCacheKey<K>, value: Vec<TimeIndexEntry<JoinRecord<E, P>>>) {
        use std::collections::hash_map::Entry;
        // The replacement path (existing key) does not touch the
        // insertion-order queue.
        if let Entry::Occupied(mut occ) = self.entries.entry(key.clone()) {
            occ.insert(value);
            return;
        }
        // Vacant path: evict if at capacity, then insert.
        if self.entries.len() >= self.max_size {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, value);
    }

    /// Drop every cache entry whose key mentions `relation` on
    /// either side (as the target or as the driver). Called when
    /// the underlying index for `relation` is mutated so subsequent
    /// queries see fresh data.
    pub fn invalidate_relation(&mut self, relation: &str) {
        if self.entries.is_empty() {
            return;
        }
        let to_remove: Vec<RangeCacheKey<K>> = self
            .entries
            .keys()
            .filter(|k| k.relation == relation || k.driver == relation)
            .cloned()
            .collect();
        for k in &to_remove {
            self.entries.remove(k);
        }
        self.insertion_order
            .retain(|k| k.relation != relation && k.driver != relation);
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop every entry.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Entries = Vec<TimeIndexEntry<JoinRecord<i32, i64>>>;

    fn make_entries(payloads: &[(i64, i32, i64, u64)]) -> Entries {
        payloads
            .iter()
            .map(|(ts, elem, payload, count)| {
                TimeIndexEntry::new(*ts, JoinRecord::new(*elem, *payload), *count)
            })
            .collect()
    }

    #[test]
    fn put_and_get_round_trip() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        let key = RangeCacheKey::new("A", "B", 42, 0, 100);
        let entries = make_entries(&[(50, 1, 10, 2)]);
        c.put(key.clone(), entries.clone());
        let got = c.get(&key).expect("hit");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].count, 2);
    }

    #[test]
    fn miss_on_unknown_key() {
        let c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        let key = RangeCacheKey::new("A", "B", 42, 0, 100);
        assert!(c.get(&key).is_none());
    }

    #[test]
    fn distinct_keys_distinguish_by_join_key_and_bounds() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        let k1 = RangeCacheKey::new("A", "B", 1, 0, 100);
        let k2 = RangeCacheKey::new("A", "B", 2, 0, 100);
        let k3 = RangeCacheKey::new("A", "B", 1, 0, 200);
        c.put(k1.clone(), make_entries(&[(0, 1, 1, 1)]));
        c.put(k2.clone(), make_entries(&[(0, 2, 1, 1)]));
        c.put(k3.clone(), make_entries(&[(0, 3, 1, 1)]));
        assert!(c.get(&k1).is_some());
        assert!(c.get(&k2).is_some());
        assert!(c.get(&k3).is_some());
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn capacity_evicts_oldest_on_overflow() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(2);
        let k1 = RangeCacheKey::new("A", "B", 1, 0, 10);
        let k2 = RangeCacheKey::new("A", "B", 2, 0, 10);
        let k3 = RangeCacheKey::new("A", "B", 3, 0, 10);
        c.put(k1.clone(), make_entries(&[(0, 1, 1, 1)]));
        c.put(k2.clone(), make_entries(&[(0, 2, 1, 1)]));
        c.put(k3.clone(), make_entries(&[(0, 3, 1, 1)]));
        // k1 should have been evicted (FIFO).
        assert!(c.get(&k1).is_none());
        assert!(c.get(&k2).is_some());
        assert!(c.get(&k3).is_some());
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn invalidate_relation_drops_matching_target_or_driver() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        let k_target = RangeCacheKey::new("X", "B", 1, 0, 10);
        let k_driver = RangeCacheKey::new("A", "X", 2, 0, 10);
        let k_unrelated = RangeCacheKey::new("A", "B", 3, 0, 10);
        c.put(k_target.clone(), make_entries(&[(0, 1, 1, 1)]));
        c.put(k_driver.clone(), make_entries(&[(0, 2, 1, 1)]));
        c.put(k_unrelated.clone(), make_entries(&[(0, 3, 1, 1)]));
        assert_eq!(c.len(), 3);

        c.invalidate_relation("X");

        assert!(c.get(&k_target).is_none());
        assert!(c.get(&k_driver).is_none());
        assert!(c.get(&k_unrelated).is_some());
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn invalidate_unknown_relation_is_noop() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        let key = RangeCacheKey::new("A", "B", 1, 0, 10);
        c.put(key.clone(), make_entries(&[(0, 1, 1, 1)]));
        c.invalidate_relation("Z");
        assert!(c.get(&key).is_some());
    }

    #[test]
    fn clear_drops_all_entries() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(8);
        c.put(
            RangeCacheKey::new("A", "B", 1, 0, 10),
            make_entries(&[(0, 1, 1, 1)]),
        );
        c.put(
            RangeCacheKey::new("A", "B", 2, 0, 10),
            make_entries(&[(0, 2, 1, 1)]),
        );
        assert_eq!(c.len(), 2);
        c.clear();
        assert!(c.is_empty());
    }

    #[test]
    fn put_replaces_existing_value_without_growing_order_queue() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(2);
        let k = RangeCacheKey::new("A", "B", 1, 0, 10);
        c.put(k.clone(), make_entries(&[(0, 1, 1, 1)]));
        // Re-put with the same key: should not be considered a new
        // insertion for capacity purposes.
        c.put(k.clone(), make_entries(&[(0, 1, 2, 2)]));
        c.put(
            RangeCacheKey::new("A", "B", 2, 0, 10),
            make_entries(&[(0, 2, 1, 1)]),
        );
        // After the replacement + one new insert, capacity = 2 is
        // fully used but the first key should still be present.
        assert_eq!(c.len(), 2);
        assert!(c.get(&k).is_some());
        let got = c.get(&k).unwrap();
        assert_eq!(got[0].count, 2, "got latest stored value");
    }

    #[test]
    fn zero_capacity_clamps_to_one() {
        let mut c: RangeCache<i32, i32, i64> = RangeCache::new(0);
        let k = RangeCacheKey::new("A", "B", 1, 0, 10);
        c.put(k.clone(), make_entries(&[(0, 1, 1, 1)]));
        assert!(c.get(&k).is_some());
        assert_eq!(c.len(), 1);
    }
}
