//! Example: Window-based stream processing with aggregation
//!
//! Demonstrates:
//! - Tumbling time windows and count windows
//! - Sliding windows with overlap
//! - Session windows for activity-based grouping
//! - Windowed aggregation (count, sum, avg, min, max)
//!
//! Run with: `cargo run --example window_aggregation`

use std::time::Duration;

use cqels_core::operator::aggregate::{
    AggregateFunction, AvgAggregate, CountAggregate, GroupKey, MaxAggregate, MinAggregate,
    SumAggregate, WindowedAggregateOperator,
};
use cqels_core::stream::Timestamped;
use cqels_core::window::{
    SessionWindow, SlidingWindow, TumblingCountWindow, TumblingWindow, Window, WindowedBatch,
};
use futures::StreamExt;

/// A simple timestamped reading for demonstration.
#[derive(Debug, Clone)]
struct Reading {
    sensor: String,
    value: f64,
    timestamp: i64,
}

impl Timestamped for Reading {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

fn reading(sensor: &str, value: f64, ts: i64) -> Reading {
    Reading {
        sensor: sensor.to_string(),
        value,
        timestamp: ts,
    }
}

fn print_batches(batches: &[WindowedBatch<Reading>]) {
    for (i, batch) in batches.iter().enumerate() {
        let values: Vec<f64> = batch.elements.iter().map(|r| r.value).collect();
        println!(
            "    Window {}: [{}-{}ms] {} elements, values: {:?}",
            i + 1,
            batch.window_start,
            batch.window_end,
            batch.size(),
            values
        );
    }
}

#[tokio::main]
async fn main() {
    println!("=== Window-Based Aggregation Example ===\n");

    // Sensor readings over 30 seconds
    let readings = vec![
        // 0-10s window
        reading("s1", 22.0, 1000),
        reading("s2", 25.0, 2000),
        reading("s1", 23.5, 5000),
        reading("s2", 26.0, 7000),
        reading("s1", 24.0, 9000),
        // 10-20s window
        reading("s2", 28.0, 11000),
        reading("s1", 30.0, 13000),
        reading("s1", 31.5, 15000),
        reading("s2", 27.0, 18000),
        // 20-30s window
        reading("s1", 29.0, 21000),
        reading("s2", 24.0, 25000),
        reading("s1", 26.0, 28000),
    ];

    // --- 1. Tumbling Time Window ---
    println!("--- 1. Tumbling Time Window (10s) ---");
    let window = TumblingWindow::new(Duration::from_secs(10));
    let stream = Box::pin(futures::stream::iter(readings.clone()));
    let batches: Vec<_> = window.apply(stream).collect().await;
    println!(
        "  Input: {} readings -> {} batches",
        readings.len(),
        batches.len()
    );
    print_batches(&batches);
    println!();

    // --- 2. Tumbling Count Window ---
    println!("--- 2. Tumbling Count Window (4 elements per batch) ---");
    let count_window = TumblingCountWindow::new(4);
    let stream = Box::pin(futures::stream::iter(readings.clone()));
    let batches: Vec<_> = count_window.apply(stream).collect().await;
    println!("  {} batches of 4:", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        let names: Vec<&str> = batch.elements.iter().map(|r| r.sensor.as_str()).collect();
        println!(
            "    Batch {}: {} elements from sensors {:?}",
            i + 1,
            batch.size(),
            names
        );
    }
    println!();

    // --- 3. Sliding Window ---
    println!("--- 3. Sliding Window (15s range, 10s slide) ---");
    let sliding = SlidingWindow::new(Duration::from_secs(15), Duration::from_secs(10));
    let stream = Box::pin(futures::stream::iter(readings.clone()));
    let batches: Vec<_> = sliding.apply(stream).collect().await;
    println!("  {} overlapping batches:", batches.len());
    print_batches(&batches);
    println!();

    // --- 4. Session Window ---
    println!("--- 4. Session Window (gap = 6s) ---");
    let session_readings = vec![
        // Session 1: rapid activity
        reading("s1", 10.0, 1000),
        reading("s1", 11.0, 3000),
        reading("s1", 12.0, 5000),
        // gap > 6s
        // Session 2: resumed activity
        reading("s1", 20.0, 12000),
        reading("s1", 21.0, 14000),
        // gap > 6s
        // Session 3: single reading
        reading("s1", 30.0, 25000),
    ];

    let session = SessionWindow::new(Duration::from_secs(6));
    let stream = Box::pin(futures::stream::iter(session_readings));
    let batches: Vec<_> = session.apply(stream).collect().await;
    println!("  {} sessions detected:", batches.len());
    print_batches(&batches);
    println!();

    // --- 5. Windowed Aggregation with grouping ---
    println!("--- 5. Windowed Aggregation (count per sensor per 10s window) ---");

    let count_op = WindowedAggregateOperator::new(
        CountAggregate,
        |r: &Reading| Some(GroupKey::single(r.sensor.clone())),
        10_000,
    );

    // Collect tumbling window batches again for aggregation
    let window = TumblingWindow::new(Duration::from_secs(10));
    let stream = Box::pin(futures::stream::iter(readings.clone()));
    let batches: Vec<_> = window.apply(stream).collect().await;

    for (i, batch) in batches.iter().enumerate() {
        let results = count_op.process_batch(&batch.elements);
        for res in &results {
            let group = res
                .group_key
                .as_ref()
                .map_or("all".to_string(), |k| k.to_string());
            println!(
                "    Window {}: sensor={}, count={}",
                i + 1,
                group,
                res.value
            );
        }
    }
    println!();

    // --- 6. Multiple aggregates on same data ---
    println!("--- 6. Multiple Aggregates (sum, avg, min, max per window) ---");

    let sum_agg = SumAggregate::new(|r: &Reading| r.value);
    let avg_agg = AvgAggregate::new(|r: &Reading| r.value);
    let min_agg = MinAggregate::new(|r: &Reading| r.value);
    let max_agg = MaxAggregate::new(|r: &Reading| r.value);

    for (i, batch) in batches.iter().enumerate() {
        let mut sum_acc = sum_agg.create_accumulator();
        let mut avg_acc = avg_agg.create_accumulator();
        let mut min_acc = min_agg.create_accumulator();
        let mut max_acc = max_agg.create_accumulator();

        for elem in &batch.elements {
            sum_acc = sum_agg.add(elem, sum_acc);
            avg_acc = avg_agg.add(elem, avg_acc);
            min_acc = min_agg.add(elem, min_acc);
            max_acc = max_agg.add(elem, max_acc);
        }

        println!(
            "    Window {} [{}-{}ms]: sum={:.1}, avg={:.1}, min={:.1}, max={:.1}",
            i + 1,
            batch.window_start,
            batch.window_end,
            sum_agg.get_result(&sum_acc),
            avg_agg.get_result(&avg_acc),
            min_agg.get_result(&min_acc),
            max_agg.get_result(&max_acc),
        );
    }

    println!("\n=== Done ===");
}
