//! Parallel broadcast hash-join operator.
//!
//! Mirrors Java's `org.cqels.operator.parallel.ParallelHashJoinOperator`.
//!
//! Algorithm:
//! 1. **Build phase** (sequential): collect the left stream into a
//!    `HashMap<K, Vec<L>>` keyed by the left join key.
//! 2. **Probe phase** (parallel-friendly): map every right element through
//!    the hash table and emit a join result for each matching left element.
//!
//! Best fit: small left side that fits in memory + large right side. The
//! probe phase is naturally parallelizable because each right element's
//! lookup is independent.
//!
//! This Rust port returns a `Stream<Item = Out>`. The probe is run via
//! [`futures::stream::StreamExt::buffer_unordered`] with the configured
//! parallelism, so when consumed by a multi-threaded executor (e.g.
//! `tokio` with multiple worker threads) the probes overlap. With the
//! default single-threaded `current_thread` runtime the probes still run
//! concurrently but on one thread.

use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};

use super::parallel::ParallelExecutionConfig;

type LeftKeyFn<L, K> = Arc<dyn Fn(&L) -> K + Send + Sync>;
type RightKeyFn<R, K> = Arc<dyn Fn(&R) -> K + Send + Sync>;
type JoinFn<L, R, Out> = Arc<dyn Fn(&L, &R) -> Out + Send + Sync>;

/// Parallel broadcast hash-join operator.
///
/// Generic over:
/// - `L`: left element type (built into the hash table).
/// - `R`: right element type (probes the hash table).
/// - `K`: join key type (`Eq + Hash + Clone`).
/// - `Out`: join result type.
pub struct ParallelHashJoinOperator<L, R, K, Out>
where
    L: Clone + Send + Sync + 'static,
    R: Send + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    Out: Send + 'static,
{
    left_key: LeftKeyFn<L, K>,
    right_key: RightKeyFn<R, K>,
    join_fn: JoinFn<L, R, Out>,
    parallelism: usize,
}

impl<L, R, K, Out> ParallelHashJoinOperator<L, R, K, Out>
where
    L: Clone + Send + Sync + 'static,
    R: Send + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    Out: Send + 'static,
{
    /// Creates a new operator with the given key extractors and join
    /// function. Uses the default [`ParallelExecutionConfig`] for
    /// parallelism.
    pub fn new(
        left_key: impl Fn(&L) -> K + Send + Sync + 'static,
        right_key: impl Fn(&R) -> K + Send + Sync + 'static,
        join_fn: impl Fn(&L, &R) -> Out + Send + Sync + 'static,
    ) -> Self {
        Self::with_config(
            left_key,
            right_key,
            join_fn,
            &ParallelExecutionConfig::default(),
        )
    }

    /// Creates a new operator with explicit parallelism configuration.
    pub fn with_config(
        left_key: impl Fn(&L) -> K + Send + Sync + 'static,
        right_key: impl Fn(&R) -> K + Send + Sync + 'static,
        join_fn: impl Fn(&L, &R) -> Out + Send + Sync + 'static,
        config: &ParallelExecutionConfig,
    ) -> Self {
        Self {
            left_key: Arc::new(left_key),
            right_key: Arc::new(right_key),
            join_fn: Arc::new(join_fn),
            parallelism: config.parallelism.max(1),
        }
    }

    /// Returns the parallelism level used during the probe phase.
    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    /// Performs the broadcast hash join.
    ///
    /// `left` is fully drained into the hash table before probing begins.
    /// `right` is then streamed through the table; for each matching left
    /// element, a join result is emitted.
    pub async fn join(
        &self,
        left: Pin<Box<dyn Stream<Item = L> + Send>>,
        right: Pin<Box<dyn Stream<Item = R> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Out> + Send>> {
        let table = build_hash_table(left, self.left_key.clone()).await;
        let table = Arc::new(table);
        let right_key = self.right_key.clone();
        let join_fn = self.join_fn.clone();

        let parallelism = self.parallelism;
        let stream = right
            .map(move |r| {
                let table = table.clone();
                let right_key = right_key.clone();
                let join_fn = join_fn.clone();
                async move { probe(&table, &r, &right_key, &join_fn) }
            })
            .buffer_unordered(parallelism)
            .flat_map(futures::stream::iter);
        Box::pin(stream)
    }
}

async fn build_hash_table<L, K>(
    mut left: Pin<Box<dyn Stream<Item = L> + Send>>,
    key_fn: LeftKeyFn<L, K>,
) -> HashMap<K, Vec<L>>
where
    L: Clone + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    let mut table: HashMap<K, Vec<L>> = HashMap::new();
    while let Some(elem) = left.next().await {
        let key = key_fn(&elem);
        table.entry(key).or_default().push(elem);
    }
    table
}

fn probe<L, R, K, Out>(
    table: &HashMap<K, Vec<L>>,
    right: &R,
    right_key: &RightKeyFn<R, K>,
    join_fn: &JoinFn<L, R, Out>,
) -> Vec<Out>
where
    L: Clone,
    K: Eq + Hash + Clone,
{
    let key = right_key(right);
    match table.get(&key) {
        Some(matching) => matching.iter().map(|l| join_fn(l, right)).collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;

    #[tokio::test]
    async fn joins_matching_keys() {
        let op = ParallelHashJoinOperator::<(i32, &str), (i32, &str), i32, (String, String)>::new(
            |l| l.0,
            |r| r.0,
            |l, r| (l.1.to_string(), r.1.to_string()),
        );
        let left = Box::pin(futures::stream::iter(vec![
            (1, "alice"),
            (2, "bob"),
            (3, "carol"),
        ]));
        let right = Box::pin(futures::stream::iter(vec![(1, "x"), (2, "y"), (4, "z")]));
        let mut out: Vec<(String, String)> = op.join(left, right).await.collect().await;
        out.sort();
        assert_eq!(
            out,
            vec![("alice".into(), "x".into()), ("bob".into(), "y".into()),]
        );
    }

    #[tokio::test]
    async fn empty_left_yields_no_results() {
        let op = ParallelHashJoinOperator::<i32, i32, i32, (i32, i32)>::new(
            |&l| l,
            |&r| r,
            |&l, &r| (l, r),
        );
        let left = Box::pin(futures::stream::iter(Vec::<i32>::new()));
        let right = Box::pin(futures::stream::iter(vec![1, 2, 3]));
        let out: Vec<_> = op.join(left, right).await.collect().await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn empty_right_yields_no_results() {
        let op = ParallelHashJoinOperator::<i32, i32, i32, (i32, i32)>::new(
            |&l| l,
            |&r| r,
            |&l, &r| (l, r),
        );
        let left = Box::pin(futures::stream::iter(vec![1, 2, 3]));
        let right = Box::pin(futures::stream::iter(Vec::<i32>::new()));
        let out: Vec<_> = op.join(left, right).await.collect().await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn one_to_many_relationship_emits_all_pairs() {
        // Left has multiple entries with the same key.
        let op = ParallelHashJoinOperator::<(i32, &str), (i32, &str), i32, (String, String)>::new(
            |l| l.0,
            |r| r.0,
            |l, r| (l.1.to_string(), r.1.to_string()),
        );
        let left = Box::pin(futures::stream::iter(vec![
            (1, "alice"),
            (1, "andy"),
            (2, "bob"),
        ]));
        let right = Box::pin(futures::stream::iter(vec![(1, "ride")]));
        let mut out: Vec<_> = op.join(left, right).await.collect().await;
        out.sort();
        assert_eq!(
            out,
            vec![
                ("alice".into(), "ride".into()),
                ("andy".into(), "ride".into()),
            ]
        );
    }

    #[tokio::test]
    async fn parallelism_setting_does_not_change_results() {
        // Same data, different parallelism — same result set.
        for &p in &[1usize, 2, 4, 8] {
            let config = ParallelExecutionConfig::builder().parallelism(p).build();
            let op = ParallelHashJoinOperator::<i32, i32, i32, (i32, i32)>::with_config(
                |&l| l,
                |&r| r,
                |&l, &r| (l, r),
                &config,
            );
            let left = Box::pin(futures::stream::iter(0..100));
            let right = Box::pin(futures::stream::iter(50..150));
            let mut out: Vec<_> = op.join(left, right).await.collect().await;
            out.sort();
            let expected: Vec<_> = (50..100).map(|n| (n, n)).collect();
            assert_eq!(out, expected, "parallelism={p}");
        }
    }

    #[tokio::test]
    async fn against_nested_loop_baseline() {
        // Match against a brute-force nested-loop implementation.
        let left_data: Vec<(i32, i32)> = (0..50).map(|i| (i % 10, i)).collect();
        let right_data: Vec<(i32, i32)> = (0..30).map(|i| (i % 10, 100 + i)).collect();

        let op = ParallelHashJoinOperator::<(i32, i32), (i32, i32), i32, (i32, i32)>::new(
            |l| l.0,
            |r| r.0,
            |l, r| (l.1, r.1),
        );
        let left = Box::pin(futures::stream::iter(left_data.clone()));
        let right = Box::pin(futures::stream::iter(right_data.clone()));
        let mut indexed: Vec<_> = op.join(left, right).await.collect().await;
        indexed.sort();

        let mut baseline: Vec<(i32, i32)> = Vec::new();
        for l in &left_data {
            for r in &right_data {
                if l.0 == r.0 {
                    baseline.push((l.1, r.1));
                }
            }
        }
        baseline.sort();

        assert_eq!(indexed, baseline);
    }
}
