# Stream Processing

> **Prerequisites**: [Data Model](data-model.md), [Architecture](architecture.md)
>
> **Next steps**: [Query Languages](query-languages.md), [Advanced](advanced.md)

This is the central guide to CQELS stream processing: elements, windows,
operators, and aggregation.

## Stream Elements

### StreamElement

The top-level element entering the pipeline:

```rust
use cqels_core::stream::{StreamElement, RdfStreamElement, StreamRecord};

// RDF element (triple + timestamp)
let rdf = StreamElement::Rdf(RdfStreamElement::new(stmt, 1000));
assert!(rdf.is_rdf());
assert_eq!(rdf.timestamp(), 1000);

// Record element (string payload + timestamp)
let rec = StreamElement::Record(StreamRecord::new("event-data", 2000));
```

### RdfStreamElement

The most common element type -- an RDF `Statement` with a timestamp:

```rust
use cqels_core::stream::RdfStreamElement;

let elem = RdfStreamElement::new(stmt, 5000);  // explicit timestamp
let elem = RdfStreamElement::now(stmt);          // current system time
```

### StreamEvent\<T\>

A typed event that can be either a data record or a watermark:

```rust
use cqels_core::stream::StreamEvent;

let record = StreamEvent::record("sensor-reading", 1000);
let watermark = StreamEvent::watermark(5000);

assert!(record.is_record());
assert!(watermark.is_watermark());
assert_eq!(record.value(), Some(&"sensor-reading"));
```

Watermarks signal that no events with timestamps before the watermark will
arrive, enabling windows to close.

## The Timestamped Trait

All types that flow through windows and CEP must implement `Timestamped`:

```rust
use cqels_core::stream::Timestamped;

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
```

`RdfStreamElement`, `StreamElement`, and `StreamEvent<T>` already implement
`Timestamped`.

## Window Types

Windows partition an unbounded stream into finite batches. All window types
implement the `Window<T>` trait:

```rust
pub trait Window<T: Timestamped + Clone + Send + 'static>: Send + Sync {
    fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = WindowedBatch<T>> + Send>>;
    fn window_type(&self) -> WindowType;
}
```

### TumblingWindow

Non-overlapping, fixed-duration windows:

```
Time:  |----10s----|----10s----|----10s----|
Events: a b c d     e f g       h i
Batch:  [a,b,c,d]  [e,f,g]     [h,i]
```

```rust
use std::time::Duration;
use cqels_core::window::TumblingWindow;

let window = TumblingWindow::new(Duration::from_secs(10));
```

### SlidingWindow

Overlapping windows with a fixed size and slide interval:

```
Time:   |-----15s range-----|
        |----10s slide--|-----15s range-----|
Events:  a b c d e f g   h i j k
```

```rust
use cqels_core::window::SlidingWindow;

let window = SlidingWindow::new(
    Duration::from_secs(15), // range
    Duration::from_secs(10), // slide
);
```

Elements near window boundaries appear in multiple batches.

### SessionWindow

Windows driven by activity gaps. A new session starts after a period of
inactivity exceeding the gap threshold:

```rust
use cqels_core::window::SessionWindow;

let window = SessionWindow::new(Duration::from_secs(6));
// Events 3s apart -> same session
// Gap > 6s -> new session
```

### TumblingCountWindow

Fixed-count batches regardless of time:

```rust
use cqels_core::window::TumblingCountWindow;

let window = TumblingCountWindow::new(4); // 4 elements per batch
```

### Factory Functions

Shorthand constructors:

```rust
use cqels_core::window::{tumbling, sliding, session, tumbling_count};

let w = tumbling(Duration::from_secs(10));
let w = sliding(Duration::from_secs(15), Duration::from_secs(10));
let w = session(Duration::from_secs(5));
let w = tumbling_count(100);
```

### WindowedBatch

The output of a window contains elements plus boundaries:

```rust
let batch: WindowedBatch<RdfStreamElement> = /* from window.apply() */;
batch.elements;      // Vec<T> - the elements in this window
batch.window_start;  // i64 - start timestamp (ms)
batch.window_end;    // i64 - end timestamp (ms)
batch.window_type;   // WindowType enum
batch.size();        // number of elements
batch.is_empty();    // true if no elements
```

## Aggregation

### AggregateFunction Trait

The core abstraction for computing aggregates over stream data:

```rust
pub trait AggregateFunction<T, ACC, R>: Send + Sync {
    fn create_accumulator(&self) -> ACC;
    fn add(&self, element: &T, accumulator: ACC) -> ACC;
    fn get_result(&self, accumulator: &ACC) -> R;
    fn merge(&self, a: ACC, b: ACC) -> ACC;
}
```

The functional style (accumulators are values, not mutable state) enables
parallelization and efficient merging.

### Built-in Aggregates

| Type | Accumulator | Result | Notes |
|------|-------------|--------|-------|
| `CountAggregate` | `usize` | `usize` | Counts elements |
| `SumAggregate<T, F>` | `f64` | `f64` | Sum via extractor function |
| `AvgAggregate<T, F>` | `(f64, usize)` | `f64` | Mean via extractor |
| `MinAggregate<T, F>` | `f64` | `f64` | Minimum via extractor |
| `MaxAggregate<T, F>` | `f64` | `f64` | Maximum via extractor |

### Using Aggregates

```rust
use cqels_core::operator::aggregate::{
    AggregateFunction, CountAggregate, SumAggregate, AvgAggregate,
};

#[derive(Clone)]
struct Reading { value: f64 }

let readings = vec![
    Reading { value: 22.0 },
    Reading { value: 25.0 },
    Reading { value: 23.5 },
];

// Count
let count = CountAggregate;
let mut acc = count.create_accumulator();
for r in &readings {
    acc = count.add(r, acc);
}
assert_eq!(count.get_result(&acc), 3);

// Average
let avg = AvgAggregate::new(|r: &Reading| r.value);
let mut acc = avg.create_accumulator();
for r in &readings {
    acc = avg.add(r, acc);
}
println!("Average: {:.1}", avg.get_result(&acc)); // 23.5
```

### WindowedAggregateOperator

Combines windowing with grouped aggregation:

```rust
use cqels_core::operator::aggregate::{
    WindowedAggregateOperator, CountAggregate, GroupKey,
};

let op = WindowedAggregateOperator::new(
    CountAggregate,
    |r: &Reading| Some(GroupKey::single(r.sensor.clone())), // group by sensor
    10_000, // window size hint
);

let results = op.process_batch(&batch.elements);
for res in &results {
    let group = res.group_key.as_ref().map_or("all".into(), |k| k.to_string());
    println!("{}: count={}", group, res.value);
}
```

### RetractableAggregateFunction

For efficient sliding window support, implement `retract`:

```rust
pub trait RetractableAggregateFunction<T, ACC, R>: AggregateFunction<T, ACC, R> {
    fn retract(&self, element: &T, accumulator: ACC) -> ACC;
    fn supports_efficient_retraction(&self) -> bool;
}
```

`CountAggregate`, `SumAggregate`, and `AvgAggregate` support retraction.
`MinAggregate` and `MaxAggregate` do not (they require a full recomputation).

## SWAG (Sliding-Window Aggregation)

The `SwagOp` trait provides the Two-Stacks Lite algorithm for O(1) amortized
sliding window aggregation:

```rust
pub trait SwagOp<In, Partial: Clone, Out>: Send + Sync {
    fn identity(&self) -> Partial;
    fn lift(&self, value: &In) -> Partial;
    fn combine(&self, a: &Partial, b: &Partial) -> Partial;
    fn lower(&self, aggregate: &Partial) -> Out;
    fn invert(&self, value: &Partial) -> Option<Partial>;
    fn is_invertible(&self) -> bool;
}
```

Built-in SWAG ops: `SwagCountOp`, `SwagSumOp`, `SwagAvgOp`, `SwagMinOp`,
`SwagMaxOp`.

## Other Operators

### FilterOperator

Removes bindings that don't satisfy a predicate:

```rust
use cqels_core::operator::filter::FilterOperator;
use cqels_model::{BindingSet, Value};

let filter = FilterOperator::new(|bs: &BindingSet| {
    bs.get("temp")
        .and_then(|v| v.as_float())
        .map_or(false, |t| t > 30.0)
});

assert!(filter.evaluate(&hot_binding));   // temp=42.0 -> true
assert!(!filter.evaluate(&cold_binding)); // temp=18.0 -> false
```

### BindOperator

Computes a new variable from existing bindings:

```rust
use cqels_core::operator::bind::BindOperator;

let bind = BindOperator::new("temp_f", |bs: &BindingSet| {
    bs.get("temp_c")
        .and_then(|v| v.as_float())
        .map(|c| Value::from(c * 9.0 / 5.0 + 32.0))
});
```

### WindowedJoinState

Time-windowed join between two streams:

```rust
use cqels_core::operator::join::WindowedJoinState;

let mut join = WindowedJoinState::new(Duration::from_secs(10));
// join.add_left(element, timestamp, &join_fn) -> matches
// join.add_right(element, timestamp, &join_fn) -> matches
```

### TopKOperator

Maintains a ranked subset of elements:

```rust
use cqels_core::operator::ranking::{TopKOperator, SortDirection, SortKey};

let mut top3 = TopKOperator::new(3, |r: &Reading| r.value, SortDirection::Descending);
top3.insert(reading1);
top3.insert(reading2);
let ranked = top3.result(); // Vec<RankedElement<Reading>>
```

### RSP-QL Operators

Stream construction operators from the RSP-QL model:

| Operator | Semantics |
|----------|-----------|
| `IStreamOperator` | Emits newly inserted elements |
| `DStreamOperator` | Emits deleted elements |
| `RStreamOperator` | Emits full window snapshots |

## Parallel Execution

Configure parallel processing for operators:

```rust
use cqels_core::operator::parallel::ParallelExecutionConfig;

let config = ParallelExecutionConfig::default();
// config.parallelism -- number of threads (default: available CPUs)
// config.auto_parallelize -- enable automatic parallelization
// config.min_stream_size -- minimum batch size before parallelizing
```
