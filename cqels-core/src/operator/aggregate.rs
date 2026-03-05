use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

/// Core aggregate function trait for incremental stream aggregation.
///
/// An `AggregateFunction` defines four operations that together enable
/// incremental, mergeable aggregation over stream windows:
///
/// 1. **create** -- Initialize an empty accumulator.
/// 2. **add** -- Fold a new element into the accumulator.
/// 3. **get_result** -- Extract the final result from an accumulator.
/// 4. **merge** -- Combine two independent accumulators (enables parallel
///    and out-of-order aggregation).
///
/// # Type Parameters
///
/// * `T` -- Input element type.
/// * `ACC` -- Accumulator type that holds intermediate state.
/// * `R` -- Result type extracted from the accumulator.
///
/// Maps to Java's `AggregateFunction<T, ACC, R>` in CQELS 2.0.
pub trait AggregateFunction<T, ACC, R>: Send + Sync {
    /// Creates a new, empty accumulator representing the identity element
    /// of this aggregation.
    fn create_accumulator(&self) -> ACC;

    /// Folds `element` into `accumulator`, returning the updated accumulator.
    fn add(&self, element: &T, accumulator: ACC) -> ACC;

    /// Extracts the final result from the given accumulator.
    fn get_result(&self, accumulator: &ACC) -> R;

    /// Merges two independent accumulators into one. This must be
    /// associative and commutative for correct parallel aggregation.
    fn merge(&self, a: ACC, b: ACC) -> ACC;
}

/// Extension trait for aggregate functions that support efficient element
/// retraction (removal).
///
/// In sliding-window aggregation, elements expire from the window as it
/// advances. A retractable aggregate can undo the effect of an element in
/// O(1) time rather than recomputing from scratch, which is critical for
/// high-throughput stream processing.
///
/// Maps to Java's `RetractableAggregateFunction<T, ACC, R>` in CQELS 2.0.
pub trait RetractableAggregateFunction<T, ACC, R>: AggregateFunction<T, ACC, R> {
    /// Removes the contribution of `element` from the `accumulator`,
    /// returning the updated accumulator.
    fn retract(&self, element: &T, accumulator: ACC) -> ACC;

    /// Returns `true` if retraction runs in O(1) time. Defaults to `true`.
    fn supports_efficient_retraction(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Concrete aggregate functions
// ---------------------------------------------------------------------------

/// Counts the number of elements in a window.
///
/// Accumulator is `i64`; result is `i64`. Supports retraction.
pub struct CountAggregate;

impl<T> AggregateFunction<T, i64, i64> for CountAggregate {
    fn create_accumulator(&self) -> i64 {
        0
    }
    fn add(&self, _element: &T, accumulator: i64) -> i64 {
        accumulator + 1
    }
    fn get_result(&self, accumulator: &i64) -> i64 {
        *accumulator
    }
    fn merge(&self, a: i64, b: i64) -> i64 {
        a + b
    }
}

impl<T> RetractableAggregateFunction<T, i64, i64> for CountAggregate {
    fn retract(&self, _element: &T, accumulator: i64) -> i64 {
        accumulator - 1
    }
}

/// Sums numeric values extracted from stream elements via an extractor
/// function. Supports retraction.
pub struct SumAggregate<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> SumAggregate<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> AggregateFunction<T, f64, f64>
    for SumAggregate<T, F>
{
    fn create_accumulator(&self) -> f64 {
        0.0
    }
    fn add(&self, element: &T, accumulator: f64) -> f64 {
        accumulator + (self.extractor)(element)
    }
    fn get_result(&self, accumulator: &f64) -> f64 {
        *accumulator
    }
    fn merge(&self, a: f64, b: f64) -> f64 {
        a + b
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> RetractableAggregateFunction<T, f64, f64>
    for SumAggregate<T, F>
{
    fn retract(&self, element: &T, accumulator: f64) -> f64 {
        accumulator - (self.extractor)(element)
    }
}

/// Accumulator for incremental average computation.
///
/// Tracks a running sum and count so the average can be computed as
/// `sum / count` and updated in O(1) per element.
#[derive(Clone, Debug, Default)]
pub struct AvgAccumulator {
    /// Running sum of all added values.
    pub sum: f64,
    /// Number of elements added (minus retracted).
    pub count: i64,
}

/// Computes the arithmetic average of numeric values extracted via an
/// extractor function. Uses [`AvgAccumulator`] for O(1) incremental
/// updates and supports retraction.
pub struct AvgAggregate<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> AvgAggregate<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> AggregateFunction<T, AvgAccumulator, f64>
    for AvgAggregate<T, F>
{
    fn create_accumulator(&self) -> AvgAccumulator {
        AvgAccumulator::default()
    }
    fn add(&self, element: &T, mut acc: AvgAccumulator) -> AvgAccumulator {
        acc.sum += (self.extractor)(element);
        acc.count += 1;
        acc
    }
    fn get_result(&self, acc: &AvgAccumulator) -> f64 {
        if acc.count == 0 {
            0.0
        } else {
            acc.sum / acc.count as f64
        }
    }
    fn merge(&self, a: AvgAccumulator, b: AvgAccumulator) -> AvgAccumulator {
        AvgAccumulator {
            sum: a.sum + b.sum,
            count: a.count + b.count,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync>
    RetractableAggregateFunction<T, AvgAccumulator, f64> for AvgAggregate<T, F>
{
    fn retract(&self, element: &T, mut acc: AvgAccumulator) -> AvgAccumulator {
        acc.sum -= (self.extractor)(element);
        acc.count -= 1;
        acc
    }
}

/// Tracks the minimum numeric value extracted from stream elements.
///
/// The accumulator is a single `f64` initialized to [`f64::MAX`].
/// Note: `MinAggregate` does **not** support efficient retraction because
/// removing the current minimum requires rescanning remaining elements.
pub struct MinAggregate<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> MinAggregate<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> AggregateFunction<T, f64, f64>
    for MinAggregate<T, F>
{
    fn create_accumulator(&self) -> f64 {
        f64::MAX
    }
    fn add(&self, element: &T, accumulator: f64) -> f64 {
        accumulator.min((self.extractor)(element))
    }
    fn get_result(&self, accumulator: &f64) -> f64 {
        *accumulator
    }
    fn merge(&self, a: f64, b: f64) -> f64 {
        a.min(b)
    }
}

/// Tracks the maximum numeric value extracted from stream elements.
///
/// The accumulator is a single `f64` initialized to [`f64::MIN`].
/// Like [`MinAggregate`], this does **not** support efficient retraction.
pub struct MaxAggregate<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64> MaxAggregate<T, F> {
    pub fn new(extractor: F) -> Self {
        Self {
            extractor,
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync, F: Fn(&T) -> f64 + Send + Sync> AggregateFunction<T, f64, f64>
    for MaxAggregate<T, F>
{
    fn create_accumulator(&self) -> f64 {
        f64::MIN
    }
    fn add(&self, element: &T, accumulator: f64) -> f64 {
        accumulator.max((self.extractor)(element))
    }
    fn get_result(&self, accumulator: &f64) -> f64 {
        *accumulator
    }
    fn merge(&self, a: f64, b: f64) -> f64 {
        a.max(b)
    }
}

// ---------------------------------------------------------------------------
// Aggregate result
// ---------------------------------------------------------------------------

/// The result of applying an aggregate function over a single window
/// (and optionally a group key).
///
/// Contains the computed value together with the window boundaries and
/// the group key (if a GROUP BY was specified). The `timestamp` field
/// is set to the window end time, making the result itself a timestamped
/// value suitable for downstream processing.
#[derive(Clone, Debug)]
pub struct AggregateResult<R> {
    pub window_start: i64,
    pub window_end: i64,
    pub group_key: Option<GroupKey>,
    pub value: R,
    pub timestamp: i64,
}

impl<R: fmt::Display> fmt::Display for AggregateResult<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AggregateResult[window=[{}, {}), value={}",
            self.window_start, self.window_end, self.value
        )?;
        if let Some(key) = &self.group_key {
            write!(f, ", group={key}")?;
        }
        write!(f, "]")
    }
}

/// A composite group key for GROUP BY operations.
///
/// Holds one or more string values that together identify a group. For
/// example, grouping by `(city, country)` produces a `GroupKey` with two
/// values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupKey {
    /// Ordered list of grouping values.
    pub values: Vec<String>,
}

impl GroupKey {
    pub fn new(values: Vec<String>) -> Self {
        Self { values }
    }

    pub fn single(value: impl Into<String>) -> Self {
        Self {
            values: vec![value.into()],
        }
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.values.join(", "))
    }
}

// ---------------------------------------------------------------------------
// Windowed aggregate operator
// ---------------------------------------------------------------------------

/// Windowed aggregate operator that partitions elements by time window and
/// an optional group key, then applies an [`AggregateFunction`] to each
/// partition.
///
/// This is the main entry point for performing grouped, windowed aggregation
/// in CQELS queries (e.g., `SELECT COUNT(*) ... GROUP BY ?sensor`).
///
/// # Type Parameters
///
/// * `T` -- Input element type (must be [`Timestamped`](crate::stream::Timestamped)).
/// * `ACC` -- Accumulator type used by the aggregate function.
/// * `R` -- Result type produced by the aggregate function.
/// * `AF` -- The concrete [`AggregateFunction`] implementation.
/// * `GF` -- A closure that extracts an optional [`GroupKey`] from each element.
///
/// Maps to Java's `WindowedAggregateOperator` in CQELS 2.0.
pub struct WindowedAggregateOperator<T, ACC, R, AF, GF>
where
    AF: AggregateFunction<T, ACC, R>,
    GF: Fn(&T) -> Option<GroupKey> + Send + Sync,
{
    aggregate_fn: AF,
    group_by_fn: GF,
    window_size_ms: i64,
    _phantom: PhantomData<(T, ACC, R)>,
}

impl<T, ACC, R, AF, GF> WindowedAggregateOperator<T, ACC, R, AF, GF>
where
    AF: AggregateFunction<T, ACC, R>,
    GF: Fn(&T) -> Option<GroupKey> + Send + Sync,
{
    pub fn new(aggregate_fn: AF, group_by_fn: GF, window_size_ms: i64) -> Self {
        Self {
            aggregate_fn,
            group_by_fn,
            window_size_ms,
            _phantom: PhantomData,
        }
    }
}

impl<T, ACC, R, AF, GF> WindowedAggregateOperator<T, ACC, R, AF, GF>
where
    T: crate::stream::Timestamped,
    ACC: Clone,
    R: Clone,
    AF: AggregateFunction<T, ACC, R>,
    GF: Fn(&T) -> Option<GroupKey> + Send + Sync,
{
    /// Processes a batch of elements and produces aggregate results.
    pub fn process_batch(&self, elements: &[T]) -> Vec<AggregateResult<R>> {
        // Group by window and optional group key
        let mut window_groups: HashMap<(i64, Option<GroupKey>), ACC> = HashMap::new();

        for elem in elements {
            let ts = elem.timestamp();
            let window_key = ts / self.window_size_ms;
            let group_key = (self.group_by_fn)(elem);
            let key = (window_key, group_key);

            let acc = window_groups
                .entry(key)
                .or_insert_with(|| self.aggregate_fn.create_accumulator());
            *acc = self.aggregate_fn.add(elem, acc.clone());
        }

        window_groups
            .into_iter()
            .map(|((window_key, group_key), acc)| {
                let window_start = window_key * self.window_size_ms;
                let window_end = window_start + self.window_size_ms;
                AggregateResult {
                    window_start,
                    window_end,
                    group_key,
                    value: self.aggregate_fn.get_result(&acc),
                    timestamp: window_end,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper type alias to pin CountAggregate to a concrete T
    type CountStr = dyn AggregateFunction<&'static str, i64, i64>;
    type CountI32 = dyn AggregateFunction<i32, i64, i64>;

    #[test]
    fn test_count_aggregate() {
        let agg = CountAggregate;
        let agg_ref: &CountStr = &agg;
        let mut acc = agg_ref.create_accumulator();
        acc = agg_ref.add(&"a", acc);
        acc = agg_ref.add(&"b", acc);
        acc = agg_ref.add(&"c", acc);
        assert_eq!(agg_ref.get_result(&acc), 3);
    }

    #[test]
    fn test_count_retractable() {
        let agg = CountAggregate;
        let agg_ref: &CountI32 = &agg;
        let mut acc = agg_ref.create_accumulator();
        acc = agg_ref.add(&1, acc);
        acc = agg_ref.add(&2, acc);
        acc = RetractableAggregateFunction::<i32, i64, i64>::retract(&agg, &1, acc);
        assert_eq!(agg_ref.get_result(&acc), 1);
    }

    #[test]
    fn test_sum_aggregate() {
        let agg = SumAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&1.0, acc);
        acc = agg.add(&2.5, acc);
        acc = agg.add(&3.5, acc);
        assert!((agg.get_result(&acc) - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sum_retractable() {
        let agg = SumAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&10.0, acc);
        acc = agg.add(&5.0, acc);
        acc = agg.retract(&10.0, acc);
        assert!((agg.get_result(&acc) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_aggregate() {
        let agg = AvgAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&10.0, acc);
        acc = agg.add(&20.0, acc);
        acc = agg.add(&30.0, acc);
        assert!((agg.get_result(&acc) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_retractable() {
        let agg = AvgAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&10.0, acc);
        acc = agg.add(&20.0, acc);
        acc = agg.add(&30.0, acc);
        acc = agg.retract(&30.0, acc);
        assert!((agg.get_result(&acc) - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_empty() {
        let agg = AvgAggregate::new(|x: &f64| *x);
        let acc = agg.create_accumulator();
        assert!((agg.get_result(&acc) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_min_aggregate() {
        let agg = MinAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&5.0, acc);
        acc = agg.add(&2.0, acc);
        acc = agg.add(&8.0, acc);
        assert!((agg.get_result(&acc) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_aggregate() {
        let agg = MaxAggregate::new(|x: &f64| *x);
        let mut acc = agg.create_accumulator();
        acc = agg.add(&5.0, acc);
        acc = agg.add(&2.0, acc);
        acc = agg.add(&8.0, acc);
        assert!((agg.get_result(&acc) - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_aggregates() {
        let agg = CountAggregate;
        let agg_ref: &CountI32 = &agg;
        let mut acc1 = agg_ref.create_accumulator();
        acc1 = agg_ref.add(&1, acc1);
        acc1 = agg_ref.add(&2, acc1);

        let mut acc2 = agg_ref.create_accumulator();
        acc2 = agg_ref.add(&3, acc2);

        let merged = agg_ref.merge(acc1, acc2);
        assert_eq!(agg_ref.get_result(&merged), 3);
    }

    #[test]
    fn test_group_key() {
        let gk = GroupKey::single("city");
        assert_eq!(gk.to_string(), "(city)");

        let gk2 = GroupKey::new(vec!["city".into(), "country".into()]);
        assert_eq!(gk2.to_string(), "(city, country)");
    }

    #[test]
    fn test_windowed_aggregate_operator() {
        use crate::stream::TimestampedValue;

        let agg = WindowedAggregateOperator::new(
            CountAggregate,
            |_: &TimestampedValue<f64>| None,
            5000, // 5-second windows
        );

        let elements = vec![
            TimestampedValue::new(1.0, 0),
            TimestampedValue::new(2.0, 1000),
            TimestampedValue::new(3.0, 2000),
            TimestampedValue::new(4.0, 5000),
            TimestampedValue::new(5.0, 6000),
        ];

        let results = agg.process_batch(&elements);
        assert_eq!(results.len(), 2); // two windows

        let mut window_counts: HashMap<i64, i64> = HashMap::new();
        for r in &results {
            window_counts.insert(r.window_start, r.value);
        }
        assert_eq!(window_counts[&0], 3); // elements at 0, 1000, 2000
        assert_eq!(window_counts[&5000], 2); // elements at 5000, 6000
    }
}
