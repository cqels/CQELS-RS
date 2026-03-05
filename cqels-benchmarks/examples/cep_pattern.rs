//! Example: Complex Event Processing (CEP) with pattern matching
//!
//! Demonstrates:
//! - Building event patterns with the fluent Pattern API
//! - Strict, relaxed, and context-sensitive matching
//! - Time-windowed pattern detection
//! - Negation patterns
//! - Inspecting match results
//!
//! Run with: `cargo run --example cep_pattern`

use std::time::Duration;

use cqels_core::stream::Timestamped;
use cqels_engine::{NfaPatternProcessor, Pattern, PatternMatch};
use futures::StreamExt;

/// A simple timestamped event for demonstration.
#[derive(Debug, Clone)]
struct SensorEvent {
    name: String,
    value: f64,
    timestamp: i64,
}

impl Timestamped for SensorEvent {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

fn event(name: &str, value: f64, ts: i64) -> SensorEvent {
    SensorEvent {
        name: name.to_string(),
        value,
        timestamp: ts,
    }
}

fn print_matches(label: &str, matches: &[PatternMatch<SensorEvent>]) {
    println!("  {label}: {} match(es)", matches.len());
    for (i, m) in matches.iter().enumerate() {
        let events: Vec<_> = m
            .events()
            .iter()
            .map(|e| format!("{}={}", e.name, e.value))
            .collect();
        println!(
            "    Match {}: [{}] (duration: {}ms)",
            i + 1,
            events.join(", "),
            m.duration()
        );
    }
}

#[tokio::main]
async fn main() {
    println!("=== CEP Pattern Matching Example ===\n");

    // --- Scenario 1: Detect rising temperature sequence ---
    println!("--- Scenario 1: Rising Temperature (strict contiguity) ---");
    println!("  Pattern: temp > 30 THEN temp > 50 THEN temp > 70\n");

    let pattern = Pattern::begin("warm")
        .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 30.0)
        .next("hot")
        .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 50.0)
        .next("critical")
        .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 70.0);

    let events = vec![
        event("temp", 25.0, 1000),
        event("temp", 35.0, 2000), // warm
        event("temp", 55.0, 3000), // hot (strict: must be immediately after warm)
        event("temp", 75.0, 4000), // critical
        event("temp", 20.0, 5000),
    ];

    let processor = NfaPatternProcessor::new(pattern);
    let stream = Box::pin(futures::stream::iter(events));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Rising temp", &matches);
    println!();

    // --- Scenario 2: Relaxed contiguity (skip non-matching events) ---
    println!("--- Scenario 2: Relaxed Contiguity (skip irrelevant events) ---");
    println!("  Pattern: temp > 30 FOLLOWED_BY pressure > 1020\n");

    let pattern = Pattern::begin("high_temp")
        .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 30.0)
        .followed_by("high_pressure")
        .where_cond(|e: &SensorEvent| e.name == "pressure" && e.value > 1020.0);

    let events = vec![
        event("temp", 35.0, 1000),       // matches high_temp
        event("humidity", 80.0, 2000),   // skipped (relaxed contiguity)
        event("humidity", 85.0, 3000),   // skipped
        event("pressure", 1025.0, 4000), // matches high_pressure
    ];

    let processor = NfaPatternProcessor::new(pattern);
    let stream = Box::pin(futures::stream::iter(events));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Relaxed", &matches);
    println!();

    // --- Scenario 3: Context-sensitive condition ---
    println!("--- Scenario 3: Context-Sensitive (monotonically increasing) ---");
    println!("  Pattern: start value, FOLLOWED_BY higher value (checked against previous)\n");

    let pattern = Pattern::begin("first")
        .where_cond(|e: &SensorEvent| e.name == "temp")
        .followed_by("second")
        .where_context(|e: &SensorEvent, prev: &[SensorEvent]| {
            e.name == "temp" && prev.last().is_some_and(|p| e.value > p.value)
        });

    let events = vec![
        event("temp", 20.0, 1000), // first
        event("temp", 15.0, 2000), // not increasing, skipped
        event("temp", 25.0, 3000), // > 20, matches second
    ];

    let processor = NfaPatternProcessor::new(pattern);
    let stream = Box::pin(futures::stream::iter(events));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Increasing", &matches);
    println!();

    // --- Scenario 4: Time-windowed pattern ---
    println!("--- Scenario 4: Time Window Constraint ---");
    println!("  Pattern: alert FOLLOWED_BY recovery, within 5 seconds\n");

    // Case A: recovery arrives within the window
    let pattern_a = Pattern::begin("alert")
        .where_cond(|e: &SensorEvent| e.name == "alert")
        .followed_by("recovery")
        .where_cond(|e: &SensorEvent| e.name == "recovery")
        .within(Duration::from_secs(5));

    let events_in = vec![
        event("alert", 1.0, 1000),
        event("noise", 0.0, 2000),
        event("recovery", 1.0, 4000), // within 5s of alert
    ];

    let processor = NfaPatternProcessor::new(pattern_a);
    let stream = Box::pin(futures::stream::iter(events_in));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Within window", &matches);

    // Case B: recovery arrives outside the window
    let pattern_b = Pattern::begin("alert")
        .where_cond(|e: &SensorEvent| e.name == "alert")
        .followed_by("recovery")
        .where_cond(|e: &SensorEvent| e.name == "recovery")
        .within(Duration::from_secs(5));

    let events_out = vec![
        event("alert", 1.0, 1000),
        event("noise", 0.0, 3000),
        event("recovery", 1.0, 7000), // 6s after alert -> outside 5s window
    ];

    let processor = NfaPatternProcessor::new(pattern_b);
    let stream = Box::pin(futures::stream::iter(events_out));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Outside window", &matches);
    println!();

    // --- Scenario 5: Negation (NOT NEXT) ---
    println!("--- Scenario 5: Negation ---");
    println!("  Pattern: start THEN NOT error THEN success\n");

    // Case A: no error between start and success
    let pattern_a = Pattern::begin("start")
        .where_cond(|e: &SensorEvent| e.name == "start")
        .not_next("error")
        .where_cond(|e: &SensorEvent| e.name == "error")
        .next("success")
        .where_cond(|e: &SensorEvent| e.name == "success");

    let events_ok = vec![
        event("start", 1.0, 1000),
        event("success", 1.0, 2000), // no error in between
    ];

    let processor = NfaPatternProcessor::new(pattern_a);
    let stream = Box::pin(futures::stream::iter(events_ok));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("No error occurred", &matches);

    // Case B: error between start and success
    let pattern_b = Pattern::begin("start")
        .where_cond(|e: &SensorEvent| e.name == "start")
        .not_next("error")
        .where_cond(|e: &SensorEvent| e.name == "error")
        .next("success")
        .where_cond(|e: &SensorEvent| e.name == "success");

    let events_err = vec![
        event("start", 1.0, 1000),
        event("error", 1.0, 2000), // error immediately after start
        event("success", 1.0, 3000),
    ];

    let processor = NfaPatternProcessor::new(pattern_b);
    let stream = Box::pin(futures::stream::iter(events_err));
    let matches: Vec<_> = processor.process(stream).collect().await;
    print_matches("Error occurred", &matches);

    println!("\n=== Done ===");
}
