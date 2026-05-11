//! Parallel windowed aggregate operator.
//!
//! Mirrors Java's `org.cqels.operator.parallel.ParallelWindowedAggregateOperator`.
//!
//! Algorithm:
//! 1. Hash-partition the input slice by `(window_key, group_key)` so every
//!    bucket holds a disjoint subset of (window, group) pairs.
//! 2. Run [`crate::operator::aggregate::WindowedAggregateOperator::process_batch`]
//!    on each bucket in parallel via rayon.
//! 3. Concatenate the per-bucket results.
//!
//! Because the partitioning is hash-deterministic on the same key the
//! aggregator groups by, no cross-bucket merge step is needed and the
//! result set is identical to the sequential version regardless of
//! parallelism.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use rayon::prelude::*;

use super::aggregate::{AggregateFunction, AggregateResult, GroupKey, WindowedAggregateOperator};
use super::parallel::ParallelExecutionConfig;
use crate::stream::Timestamped;

/// Parallel batch aggregator.
///
/// Generic over:
/// - `T`: element type (must be [`Timestamped`] and `Send + Sync`).
/// - `ACC`: aggregate accumulator (`Send + Sync + Clone`).
/// - `R`: aggregate result (`Send`).
/// - `AF`: the [`AggregateFunction`] (`Send + Sync`).
/// - `GF`: group-key extractor (`Send + Sync`).
pub struct ParallelWindowedAggregateOperator<T, ACC, R, AF, GF>
where
    AF: AggregateFunction<T, ACC, R>,
    GF: Fn(&T) -> Option<GroupKey> + Send + Sync,
{
    aggregate_fn: Arc<AF>,
    group_by_fn: Arc<GF>,
    window_size_ms: i64,
    parallelism: usize,
    _phantom: PhantomData<(T, ACC, R)>,
}

impl<T, ACC, R, AF, GF> ParallelWindowedAggregateOperator<T, ACC, R, AF, GF>
where
    T: Timestamped + Clone + Send + Sync + 'static,
    ACC: Clone + Send + Sync + 'static,
    R: Clone + Send + 'static,
    AF: AggregateFunction<T, ACC, R> + Send + Sync + 'static,
    GF: Fn(&T) -> Option<GroupKey> + Send + Sync + 'static,
{
    /// Creates a new operator with the default [`ParallelExecutionConfig`].
    pub fn new(aggregate_fn: AF, group_by_fn: GF, window_size_ms: i64) -> Self {
        Self::with_config(
            aggregate_fn,
            group_by_fn,
            window_size_ms,
            &ParallelExecutionConfig::default(),
        )
    }

    /// Creates a new operator with explicit parallelism configuration.
    pub fn with_config(
        aggregate_fn: AF,
        group_by_fn: GF,
        window_size_ms: i64,
        config: &ParallelExecutionConfig,
    ) -> Self {
        Self {
            aggregate_fn: Arc::new(aggregate_fn),
            group_by_fn: Arc::new(group_by_fn),
            window_size_ms,
            parallelism: config.parallelism.max(1),
            _phantom: PhantomData,
        }
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    /// Runs the aggregate over a slice of input elements. Returns one
    /// [`AggregateResult`] per `(window_key, group_key)` partition.
    pub fn process_batch(&self, elements: &[T]) -> Vec<AggregateResult<R>> {
        if elements.is_empty() {
            return Vec::new();
        }
        if self.parallelism <= 1 {
            // Skip the partitioning overhead.
            return self.process_sequential(elements);
        }

        // Hash-partition into `parallelism` buckets keyed by
        // (window_key, group_key).
        let mut buckets: Vec<Vec<T>> = (0..self.parallelism).map(|_| Vec::new()).collect();
        for elem in elements {
            let window_key = elem.timestamp() / self.window_size_ms;
            let group_key = (self.group_by_fn)(elem);
            let bucket = self.bucket_for(window_key, &group_key);
            buckets[bucket].push(elem.clone());
        }

        let aggregate_fn = Arc::clone(&self.aggregate_fn);
        let group_by_fn = Arc::clone(&self.group_by_fn);
        let window_size_ms = self.window_size_ms;

        // Run each bucket through the sequential aggregator in parallel.
        buckets
            .into_par_iter()
            .filter(|b| !b.is_empty())
            .flat_map_iter(move |bucket| {
                let af = Arc::clone(&aggregate_fn);
                let gf = Arc::clone(&group_by_fn);
                let op = WindowedAggregateOperator::new(
                    AggregateFnRef::new(af),
                    move |t: &T| gf(t),
                    window_size_ms,
                );
                op.process_batch(&bucket).into_iter()
            })
            .collect()
    }

    fn process_sequential(&self, elements: &[T]) -> Vec<AggregateResult<R>> {
        let af = Arc::clone(&self.aggregate_fn);
        let gf = Arc::clone(&self.group_by_fn);
        let op = WindowedAggregateOperator::new(
            AggregateFnRef::new(af),
            move |t: &T| gf(t),
            self.window_size_ms,
        );
        op.process_batch(elements)
    }

    fn bucket_for(&self, window_key: i64, group_key: &Option<GroupKey>) -> usize {
        let mut hasher = DefaultHasher::new();
        window_key.hash(&mut hasher);
        group_key.hash(&mut hasher);
        (hasher.finish() as usize) % self.parallelism
    }
}

/// Adapter so `Arc<AF>` can satisfy [`AggregateFunction`] when a
/// `WindowedAggregateOperator` requires it by value.
struct AggregateFnRef<T, ACC, R, AF> {
    inner: Arc<AF>,
    _phantom: PhantomData<(T, ACC, R)>,
}

impl<T, ACC, R, AF> AggregateFnRef<T, ACC, R, AF> {
    fn new(inner: Arc<AF>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

// SAFETY: only forwards calls into the Send + Sync inner aggregator.
unsafe impl<T, ACC, R, AF: Send> Send for AggregateFnRef<T, ACC, R, AF> {}
unsafe impl<T, ACC, R, AF: Sync> Sync for AggregateFnRef<T, ACC, R, AF> {}

impl<T, ACC, R, AF> AggregateFunction<T, ACC, R> for AggregateFnRef<T, ACC, R, AF>
where
    AF: AggregateFunction<T, ACC, R> + Send + Sync,
{
    fn create_accumulator(&self) -> ACC {
        self.inner.create_accumulator()
    }

    fn add(&self, element: &T, acc: ACC) -> ACC {
        self.inner.add(element, acc)
    }

    fn merge(&self, a: ACC, b: ACC) -> ACC {
        self.inner.merge(a, b)
    }

    fn get_result(&self, acc: &ACC) -> R {
        self.inner.get_result(acc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::aggregate::{CountAggregate, SumAggregate};
    use crate::stream::TimestampedValue;

    fn make_events(n: usize) -> Vec<TimestampedValue<u32>> {
        (0..n as u32)
            .map(|i| TimestampedValue::new(i, (i as i64) * 100))
            .collect()
    }

    #[test]
    fn empty_input_returns_empty_result() {
        let op = ParallelWindowedAggregateOperator::new(
            CountAggregate,
            |_: &TimestampedValue<u32>| None,
            1_000,
        );
        assert!(op.process_batch(&[]).is_empty());
    }

    #[test]
    fn single_partition_count_matches_sequential() {
        let config = ParallelExecutionConfig::builder().parallelism(1).build();
        let op = ParallelWindowedAggregateOperator::with_config(
            CountAggregate,
            |_: &TimestampedValue<u32>| None,
            1_000,
            &config,
        );
        let events = make_events(50);
        let results = op.process_batch(&events);
        // 50 events over 50 * 100ms = 5000ms timeline, 1000ms windows → 5 windows
        assert_eq!(results.len(), 5);
        let total: i64 = results.iter().map(|r| r.value).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn parallel_count_matches_sequential_baseline() {
        let events = make_events(200);
        let sequential = {
            let op = ParallelWindowedAggregateOperator::with_config(
                CountAggregate,
                |_: &TimestampedValue<u32>| None,
                1_000,
                &ParallelExecutionConfig::builder().parallelism(1).build(),
            );
            sort_results(op.process_batch(&events))
        };
        for &p in &[2usize, 4, 8] {
            let op = ParallelWindowedAggregateOperator::with_config(
                CountAggregate,
                |_: &TimestampedValue<u32>| None,
                1_000,
                &ParallelExecutionConfig::builder().parallelism(p).build(),
            );
            let parallel = sort_results(op.process_batch(&events));
            assert_eq!(parallel.len(), sequential.len(), "parallelism={p}");
            for (s, p_r) in sequential.iter().zip(parallel.iter()) {
                assert_eq!(s.window_start, p_r.window_start, "parallelism={p}");
                assert_eq!(s.value, p_r.value, "parallelism={p}");
            }
        }
    }

    #[test]
    fn parallel_grouped_sum_matches_sequential_baseline() {
        let events: Vec<TimestampedValue<u32>> = (0..200)
            .map(|i| TimestampedValue::new(i, (i as i64) * 50))
            .collect();
        let group_fn =
            |e: &TimestampedValue<u32>| Some(GroupKey::single(format!("g{}", e.value % 4)));
        let sum_extractor = |e: &TimestampedValue<u32>| e.value as f64;

        let sequential = {
            let op = ParallelWindowedAggregateOperator::with_config(
                SumAggregate::new(sum_extractor),
                group_fn,
                1_000,
                &ParallelExecutionConfig::builder().parallelism(1).build(),
            );
            sort_grouped_results(op.process_batch(&events))
        };
        for &p in &[2usize, 4, 8] {
            let op = ParallelWindowedAggregateOperator::with_config(
                SumAggregate::new(sum_extractor),
                group_fn,
                1_000,
                &ParallelExecutionConfig::builder().parallelism(p).build(),
            );
            let parallel = sort_grouped_results(op.process_batch(&events));
            assert_eq!(parallel.len(), sequential.len(), "parallelism={p}");
            for (s, p_r) in sequential.iter().zip(parallel.iter()) {
                assert_eq!(s.window_start, p_r.window_start, "parallelism={p}");
                assert_eq!(s.group_key, p_r.group_key, "parallelism={p}");
                assert!(
                    (s.value - p_r.value).abs() < 1e-9,
                    "parallelism={p}: {} vs {}",
                    s.value,
                    p_r.value,
                );
            }
        }
    }

    #[test]
    fn parallelism_setting_visible() {
        let op = ParallelWindowedAggregateOperator::with_config(
            CountAggregate,
            |_: &TimestampedValue<u32>| None,
            1_000,
            &ParallelExecutionConfig::builder().parallelism(8).build(),
        );
        assert_eq!(op.parallelism(), 8);
    }

    fn sort_results(mut r: Vec<AggregateResult<i64>>) -> Vec<AggregateResult<i64>> {
        r.sort_by_key(|res| res.window_start);
        r
    }

    fn sort_grouped_results(mut r: Vec<AggregateResult<f64>>) -> Vec<AggregateResult<f64>> {
        r.sort_by(|a, b| {
            a.window_start
                .cmp(&b.window_start)
                .then_with(|| format!("{:?}", a.group_key).cmp(&format!("{:?}", b.group_key)))
        });
        r
    }
}
