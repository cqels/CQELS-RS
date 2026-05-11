//! Criterion benchmarks for the parallel operators.
//!
//! Validates the speedup claims made by `ParallelHashJoinOperator` and
//! `ParallelWindowedAggregateOperator` over their sequential baselines.
//! Each operator is run at parallelism levels {1, 2, 4, 8} so the
//! report makes the scaling behaviour visible.
//!
//! Run with:
//! ```bash
//! cargo bench -p cqels-benchmarks --bench parallel_operators
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;
use tokio::runtime::Runtime;

use cqels_core::operator::aggregate::{CountAggregate, GroupKey, WindowedAggregateOperator};
use cqels_core::operator::parallel::{ParallelExecutionConfig, ParallelExecutionConfigBuilder};
use cqels_core::operator::parallel_aggregate::ParallelWindowedAggregateOperator;
use cqels_core::operator::parallel_hash_join::ParallelHashJoinOperator;
use cqels_core::stream::Timestamped;

// ─── ParallelHashJoinOperator ────────────────────────────────────────

/// One side of the synthetic hash-join input.
type JoinTuple = (usize, String);

/// Build inputs for the hash join: `left_size` left tuples with key
/// `i % key_space`, `right_size` right tuples with key
/// `(2*i) % key_space`. One-to-many joins emerge naturally.
fn join_inputs(
    left_size: usize,
    right_size: usize,
    key_space: usize,
) -> (Vec<JoinTuple>, Vec<JoinTuple>) {
    let left: Vec<JoinTuple> = (0..left_size)
        .map(|i| (i % key_space, format!("L{i}")))
        .collect();
    let right: Vec<JoinTuple> = (0..right_size)
        .map(|i| ((2 * i) % key_space, format!("R{i}")))
        .collect();
    (left, right)
}

fn bench_parallel_hash_join_scaling(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let left_size = 5_000;
    let right_size = 5_000;
    let key_space = 500;
    let (left_data, right_data) = join_inputs(left_size, right_size, key_space);

    let mut group = c.benchmark_group("parallel_hash_join_scaling");
    group.throughput(Throughput::Elements((left_size + right_size) as u64));

    for &parallelism in &[1usize, 2, 4, 8] {
        let left_data = left_data.clone();
        let right_data = right_data.clone();
        group.bench_with_input(
            BenchmarkId::from_parameter(parallelism),
            &parallelism,
            |b, &parallelism| {
                let cfg: ParallelExecutionConfig = ParallelExecutionConfigBuilder::default()
                    .parallelism(parallelism)
                    .build();
                b.iter(|| {
                    let left_data = left_data.clone();
                    let right_data = right_data.clone();
                    let op = ParallelHashJoinOperator::<
                        JoinTuple,
                        JoinTuple,
                        usize,
                        (String, String),
                    >::with_config(
                        |l| l.0, |r| r.0, |l, r| (l.1.clone(), r.1.clone()), &cfg
                    );
                    runtime.block_on(async {
                        let left = Box::pin(futures::stream::iter(left_data));
                        let right = Box::pin(futures::stream::iter(right_data));
                        let out: Vec<_> = op.join(left, right).await.collect().await;
                        black_box(out.len());
                    });
                });
            },
        );
    }
    group.finish();
}

// ─── ParallelWindowedAggregateOperator ───────────────────────────────

/// Minimal `Timestamped` element used by the aggregate benches. The
/// `group_id` provides the GROUP BY key; the `ts` field drives window
/// assignment.
#[derive(Clone, Debug)]
struct TimedEvent {
    ts: i64,
    group_id: usize,
}

impl Timestamped for TimedEvent {
    fn timestamp(&self) -> i64 {
        self.ts
    }
}

fn aggregate_inputs(n: usize, groups: usize, window_ms: i64) -> Vec<TimedEvent> {
    // Spread events across 4 windows so the operator has cross-window
    // partitions to manage in addition to the group partitions.
    let span_ms = window_ms * 4;
    (0..n)
        .map(|i| TimedEvent {
            ts: (i as i64 * span_ms) / (n as i64),
            group_id: i % groups,
        })
        .collect()
}

fn bench_parallel_aggregate_scaling(c: &mut Criterion) {
    let n = 50_000;
    let groups = 32;
    let window_ms: i64 = 1_000;
    let events = aggregate_inputs(n, groups, window_ms);

    let mut group = c.benchmark_group("parallel_aggregate_scaling");
    group.throughput(Throughput::Elements(n as u64));

    for &parallelism in &[1usize, 2, 4, 8] {
        let events = events.clone();
        group.bench_with_input(
            BenchmarkId::from_parameter(parallelism),
            &parallelism,
            |b, &parallelism| {
                let cfg = ParallelExecutionConfigBuilder::default()
                    .parallelism(parallelism)
                    .build();
                b.iter(|| {
                    let op = ParallelWindowedAggregateOperator::with_config(
                        CountAggregate,
                        |e: &TimedEvent| Some(GroupKey::single(format!("g{}", e.group_id))),
                        window_ms,
                        &cfg,
                    );
                    let results = op.process_batch(&events);
                    black_box(results.len());
                });
            },
        );
    }
    group.finish();
}

/// Sanity baseline: plain `WindowedAggregateOperator` (no parallelism)
/// over the same workload. Lets the reader compare the parallel
/// operator's `parallelism=1` overhead against the un-partitioned
/// sequential version.
fn bench_sequential_aggregate_baseline(c: &mut Criterion) {
    let n = 50_000;
    let groups = 32;
    let window_ms: i64 = 1_000;
    let events = aggregate_inputs(n, groups, window_ms);

    c.bench_function("sequential_aggregate_baseline", |b| {
        b.iter(|| {
            let op = WindowedAggregateOperator::new(
                CountAggregate,
                |e: &TimedEvent| Some(GroupKey::single(format!("g{}", e.group_id))),
                window_ms,
            );
            let results = op.process_batch(&events);
            black_box(results.len());
        });
    });
}

criterion_group!(
    benches,
    bench_parallel_hash_join_scaling,
    bench_parallel_aggregate_scaling,
    bench_sequential_aggregate_baseline,
);
criterion_main!(benches);
