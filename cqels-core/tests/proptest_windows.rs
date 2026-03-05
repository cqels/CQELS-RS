//! Property-based tests for window operators.
//!
//! Verifies invariants:
//! - Tumbling windows partition the stream (no element is lost)
//! - Count windows produce exact batch sizes
//! - Sliding windows always span their configured range
//! - Window boundaries are consistent

use std::time::Duration;

use futures::StreamExt;
use proptest::prelude::*;

use cqels_core::stream::Timestamped;
use cqels_core::window::{SlidingWindow, TumblingCountWindow, TumblingWindow, Window};

/// Simple timestamped element for testing.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TestElement {
    value: i64,
    timestamp: i64,
}

impl Timestamped for TestElement {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

fn make_elements(timestamps: Vec<i64>) -> Vec<TestElement> {
    timestamps
        .into_iter()
        .enumerate()
        .map(|(i, ts)| TestElement {
            value: i as i64,
            timestamp: ts,
        })
        .collect()
}

/// Strategy for generating sorted timestamp sequences.
fn sorted_timestamps(count: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0i64..100_000, count..=count).prop_map(|mut v| {
        v.sort();
        v
    })
}

// ---------------------------------------------------------------------------
// Tumbling Window Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn tumbling_window_preserves_all_elements(
        timestamps in sorted_timestamps(50),
        window_ms in 1000i64..20000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps.clone());
        let total = elements.len();

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingWindow::new(Duration::from_millis(window_ms as u64));
            let batches: Vec<_> = window.apply(stream).collect().await;

            let total_in_batches: usize = batches.iter().map(|b| b.size()).sum();
            prop_assert_eq!(
                total_in_batches, total,
                "tumbling window should preserve all {} elements, got {}",
                total, total_in_batches
            );
            Ok(())
        })?;
    }

    #[test]
    fn tumbling_window_batches_are_non_empty(
        timestamps in sorted_timestamps(20),
        window_ms in 1000i64..20000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingWindow::new(Duration::from_millis(window_ms as u64));
            let batches: Vec<_> = window.apply(stream).collect().await;

            for batch in &batches {
                prop_assert!(!batch.is_empty(), "no batch should be empty");
            }
            Ok(())
        })?;
    }

    #[test]
    fn tumbling_window_span_equals_window_size(
        timestamps in sorted_timestamps(30),
        window_ms in 1000i64..20000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingWindow::new(Duration::from_millis(window_ms as u64));
            let batches: Vec<_> = window.apply(stream).collect().await;

            for batch in &batches {
                let span = batch.window_end - batch.window_start;
                prop_assert_eq!(
                    span, window_ms,
                    "window span {} should equal window size {}",
                    span, window_ms
                );
            }
            Ok(())
        })?;
    }

    #[test]
    fn tumbling_window_elements_within_bounds(
        timestamps in sorted_timestamps(30),
        window_ms in 1000i64..20000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingWindow::new(Duration::from_millis(window_ms as u64));
            let batches: Vec<_> = window.apply(stream).collect().await;

            for batch in &batches {
                for elem in &batch.elements {
                    prop_assert!(
                        elem.timestamp >= batch.window_start && elem.timestamp < batch.window_end,
                        "element ts={} should be in [{}, {})",
                        elem.timestamp, batch.window_start, batch.window_end
                    );
                }
            }
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Count Window Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn count_window_batch_sizes_valid(
        n in 10usize..100,
        batch_size in 1usize..20,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let timestamps: Vec<i64> = (0..n as i64).collect();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingCountWindow::new(batch_size);
            let batches: Vec<_> = window.apply(stream).collect().await;

            // At least n/batch_size full batches; may also include partial final batch
            let min_batches = n / batch_size;
            let max_batches = if n % batch_size == 0 { min_batches } else { min_batches + 1 };
            prop_assert!(
                batches.len() >= min_batches && batches.len() <= max_batches,
                "expected {}-{} batches for {} elements with batch_size {}, got {}",
                min_batches, max_batches, n, batch_size, batches.len()
            );

            // All full batches should have exactly batch_size; last may be partial
            for (i, batch) in batches.iter().enumerate() {
                if i < min_batches {
                    prop_assert_eq!(
                        batch.size(), batch_size,
                        "full batch {} should have exactly {} elements",
                        i, batch_size
                    );
                } else {
                    // Partial final batch
                    prop_assert!(batch.size() <= batch_size);
                    prop_assert!(batch.size() > 0);
                }
            }
            Ok(())
        })?;
    }

    #[test]
    fn count_window_preserves_all_elements(
        n in 10usize..100,
        batch_size in 1usize..20,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let timestamps: Vec<i64> = (0..n as i64).collect();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = TumblingCountWindow::new(batch_size);
            let batches: Vec<_> = window.apply(stream).collect().await;

            let total_in_batches: usize = batches.iter().map(|b| b.size()).sum();
            // Should contain all elements (full batches + possible partial)
            prop_assert_eq!(
                total_in_batches, n,
                "count window should emit all {} elements, got {}",
                n, total_in_batches
            );
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Sliding Window Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn sliding_window_span_equals_range(
        timestamps in sorted_timestamps(30),
        range_ms in 5000i64..20000,
        slide_ms in 1000i64..5000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps);

        rt.block_on(async {
            let stream = Box::pin(futures::stream::iter(elements));
            let window = SlidingWindow::new(
                Duration::from_millis(range_ms as u64),
                Duration::from_millis(slide_ms as u64),
            );
            let batches: Vec<_> = window.apply(stream).collect().await;

            for batch in &batches {
                let span = batch.window_end - batch.window_start;
                prop_assert_eq!(
                    span, range_ms,
                    "sliding window span {} should equal range {}",
                    span, range_ms
                );
            }
            Ok(())
        })?;
    }

    #[test]
    fn sliding_window_produces_overlap(
        timestamps in sorted_timestamps(50),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elements = make_elements(timestamps.clone());
        let total = elements.len();

        rt.block_on(async {
            // range > slide guarantees overlap
            let stream = Box::pin(futures::stream::iter(elements));
            let window = SlidingWindow::new(
                Duration::from_secs(15),
                Duration::from_secs(5),
            );
            let batches: Vec<_> = window.apply(stream).collect().await;

            if batches.len() > 1 {
                let total_in_windows: usize = batches.iter().map(|b| b.size()).sum();
                prop_assert!(
                    total_in_windows >= total,
                    "overlapping windows should have >= total elements: {} >= {}",
                    total_in_windows, total
                );
            }
            Ok(())
        })?;
    }
}
