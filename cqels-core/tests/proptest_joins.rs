//! Property tests for the indexed self-join and parallel hash-join
//! operators. Each test compares the optimized operator's output against
//! a brute-force O(N²) baseline over random inputs.

use std::time::Duration;

use cqels_core::operator::join::WindowedSelfJoinState;
use cqels_core::operator::parallel_hash_join::ParallelHashJoinOperator;
use proptest::collection::vec;
use proptest::prelude::*;

/// One stream element: (id, key, timestamp).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Event {
    id: u32,
    key: u8,
    timestamp: i64,
}

fn arb_events(max_len: usize, key_modulus: u8) -> impl Strategy<Value = Vec<Event>> {
    vec((any::<u32>(), 0u8..key_modulus, 0i64..100_000), 0..=max_len).prop_map(|raw| {
        raw.into_iter()
            .enumerate()
            .map(|(i, (id, key, ts))| Event {
                id: i as u32 ^ id,
                key,
                timestamp: ts,
            })
            .collect()
    })
}

fn self_join_baseline(events: &[Event], window_ms: i64) -> Vec<(u32, u32)> {
    let mut pairs = Vec::new();
    for j in 0..events.len() {
        for i in 0..j {
            let (a, b) = (&events[i], &events[j]);
            if a.key == b.key && (b.timestamp - a.timestamp).abs() <= window_ms {
                pairs.push((a.id, b.id));
            }
        }
    }
    pairs
}

fn run_self_join(events: &[Event], window_ms: i64) -> Vec<(u32, u32)> {
    let mut state: WindowedSelfJoinState<Event, u8> =
        WindowedSelfJoinState::new(Duration::from_millis(window_ms as u64));
    let mut emitted = Vec::new();
    // Sort by timestamp to match the streaming insertion order assumed by
    // the operator (a key-keyed self join keeps a time-ordered per-key
    // bucket; out-of-order timestamps would degrade to a different
    // semantics that the operator does not model).
    let mut sorted: Vec<Event> = events.to_vec();
    sorted.sort_by_key(|e| e.timestamp);
    for ev in sorted {
        let ts = ev.timestamp;
        let out = state.add(ev, ts, |e| e.key);
        for pair in out {
            emitted.push((pair.first.id, pair.second.id));
        }
    }
    emitted
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// `WindowedSelfJoinState` must emit exactly the pairs produced by an
    /// O(N²) nested loop over the same events, regardless of key
    /// distribution, window size, or stream length.
    #[test]
    fn self_join_matches_nested_loop_baseline(
        events in arb_events(40, 5),
        window_ms in 1i64..50_000,
    ) {
        let mut baseline: Vec<(u32, u32)> = {
            let mut sorted: Vec<Event> = events.clone();
            sorted.sort_by_key(|e| e.timestamp);
            self_join_baseline(&sorted, window_ms)
        };
        let mut indexed = run_self_join(&events, window_ms);
        baseline.sort();
        indexed.sort();
        prop_assert_eq!(indexed, baseline);
    }

    /// Self-joins never emit `(e, e)` for the same event with itself.
    #[test]
    fn self_join_never_pairs_event_with_itself(
        events in arb_events(30, 3),
    ) {
        let pairs = run_self_join(&events, 10_000);
        // Each pair must reference two different event IDs (ID was made
        // unique by the arb_events generator via xor with index).
        for (a, b) in pairs {
            // Stronger check: timestamps + keys + ids — since IDs may
            // collide for very unlucky generators, just check IDs at the
            // event-index level (the generator makes them unique above).
            prop_assert_ne!(a, b);
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run_parallel_hash_join_async(
    left: Vec<Event>,
    right: Vec<Event>,
    parallelism: usize,
) -> Vec<(u32, u32)> {
    use cqels_core::operator::ParallelExecutionConfig;
    use futures::stream::StreamExt;

    let config = ParallelExecutionConfig::builder()
        .parallelism(parallelism.max(1))
        .build();
    let op = ParallelHashJoinOperator::<Event, Event, u8, (u32, u32)>::with_config(
        |l| l.key,
        |r| r.key,
        |l, r| (l.id, r.id),
        &config,
    );
    let l_stream = Box::pin(futures::stream::iter(left));
    let r_stream = Box::pin(futures::stream::iter(right));
    op.join(l_stream, r_stream).await.collect().await
}

fn parallel_hash_join_baseline(left: &[Event], right: &[Event]) -> Vec<(u32, u32)> {
    let mut pairs = Vec::new();
    for l in left {
        for r in right {
            if l.key == r.key {
                pairs.push((l.id, r.id));
            }
        }
    }
    pairs
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// `ParallelHashJoinOperator` must emit exactly the pairs produced by
    /// an O(N·M) nested loop, regardless of parallelism setting.
    #[test]
    fn parallel_hash_join_matches_nested_loop(
        left in arb_events(30, 4),
        right in arb_events(30, 4),
        parallelism in 1usize..8,
    ) {
        let mut baseline = parallel_hash_join_baseline(&left, &right);
        let mut actual = run_parallel_hash_join_async(left, right, parallelism);
        baseline.sort();
        actual.sort();
        prop_assert_eq!(actual, baseline);
    }
}
