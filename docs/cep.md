# Complex Event Processing

> **Prerequisites**: [Stream Processing](stream-processing.md)
>
> **Next steps**: [Reasoning](reasoning.md), [Advanced](advanced.md)

The CEP module (`cqels-engine::cep`) detects patterns across event streams
using an NFA (Non-deterministic Finite Automaton) engine. It supports strict
and relaxed contiguity, quantifiers, negation, context conditions, and time
constraints.

## Overview

CEP answers questions like:
- "Did temperature rise above 30, then 50, then 70 in consecutive readings?"
- "Did an alert occur without a recovery within 5 seconds?"
- "Did sensor values increase monotonically across 3+ events?"

The workflow is: **define a pattern** -> **compile it** -> **process a stream**
-> **collect matches**.

## Custom Event Types

Your events must implement `Timestamped + Clone + Send + 'static`:

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

## The Pattern Builder API

Patterns are built with a fluent API starting from `Pattern::begin()`:

```rust
use cqels_engine::Pattern;

let pattern = Pattern::begin("start")          // first state
    .where_cond(|e: &SensorEvent| e.value > 30.0)  // condition
    .next("middle")                            // strict contiguity
    .where_cond(|e: &SensorEvent| e.value > 50.0)
    .followed_by("end")                        // relaxed contiguity
    .where_cond(|e: &SensorEvent| e.value > 70.0)
    .within(Duration::from_secs(10));          // time constraint
```

### State Transitions

| Method | Contiguity | Behavior |
|--------|------------|----------|
| `.next(name)` | Strict | Next event must match immediately |
| `.followed_by(name)` | Relaxed | Skips non-matching events |
| `.not_next(name)` | Strict negation | Next event must NOT match |

### Conditions

| Method | Description |
|--------|-------------|
| `.where_cond(\|e\| bool)` | Simple condition on current event |
| `.where_context(\|e, prev\| bool)` | Condition with access to previously matched events |

### Quantifiers

| Method | Meaning |
|--------|---------|
| `.times(n)` | Exactly n occurrences |
| `.one_or_more()` | 1 or more occurrences |
| `.optional()` | 0 or 1 occurrences |

### Time Constraint

`.within(Duration)` constrains the total match duration from first to last
event.

## Processing Patterns

Compile a pattern into an NFA processor and feed it a stream:

```rust
use cqels_engine::NfaPatternProcessor;
use futures::StreamExt;

let processor = NfaPatternProcessor::new(pattern);
let stream = Box::pin(futures::stream::iter(events));
let matches: Vec<_> = processor.process(stream).collect().await;
```

## PatternMatch

Each match contains the matched events and metadata:

```rust
let m: &PatternMatch<SensorEvent> = &matches[0];

m.events();           // &[SensorEvent] - all matched events
m.get_event("start"); // Option<&SensorEvent> - by state name
m.first();            // Option<&SensorEvent>
m.last();             // Option<&SensorEvent>
m.start_timestamp();  // i64
m.end_timestamp();    // i64
m.duration();         // i64 (ms)
m.count();            // number of matched events
```

## Contiguity Modes

### Strict Contiguity (`.next()`)

Events must be immediately adjacent -- no intervening events allowed:

```rust
let pattern = Pattern::begin("warm")
    .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 30.0)
    .next("hot")
    .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 50.0)
    .next("critical")
    .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 70.0);
```

Given events `[25, 35, 55, 75, 20]`, this matches `[35, 55, 75]` because
they are consecutive.

### Relaxed Contiguity (`.followed_by()`)

Non-matching events are skipped:

```rust
let pattern = Pattern::begin("high_temp")
    .where_cond(|e: &SensorEvent| e.name == "temp" && e.value > 30.0)
    .followed_by("high_pressure")
    .where_cond(|e: &SensorEvent| e.name == "pressure" && e.value > 1020.0);
```

Given `[temp=35, humidity=80, humidity=85, pressure=1025]`, this matches
`[temp=35, pressure=1025]` -- the humidity events are skipped.

## Context Conditions

Access previously matched events for conditions like monotonic increase:

```rust
let pattern = Pattern::begin("first")
    .where_cond(|e: &SensorEvent| e.name == "temp")
    .followed_by("second")
    .where_context(|e: &SensorEvent, prev: &[SensorEvent]| {
        e.name == "temp" && prev.last().is_some_and(|p| e.value > p.value)
    });
```

Given `[temp=20, temp=15, temp=25]`:
- `temp=15` is skipped (15 < 20, not increasing)
- `temp=25` matches (25 > 20)

## Time Windows

Constrain the total duration of a match:

```rust
let pattern = Pattern::begin("alert")
    .where_cond(|e: &SensorEvent| e.name == "alert")
    .followed_by("recovery")
    .where_cond(|e: &SensorEvent| e.name == "recovery")
    .within(Duration::from_secs(5));
```

- `[alert@1s, recovery@4s]` -- matches (3s duration < 5s window)
- `[alert@1s, recovery@7s]` -- no match (6s duration > 5s window)

## Negation

Detect the *absence* of events:

```rust
let pattern = Pattern::begin("start")
    .where_cond(|e: &SensorEvent| e.name == "start")
    .not_next("error")
    .where_cond(|e: &SensorEvent| e.name == "error")
    .next("success")
    .where_cond(|e: &SensorEvent| e.name == "success");
```

- `[start, success]` -- matches (no error in between)
- `[start, error, success]` -- no match (error occurred immediately after start)

## Complete Example

Putting it all together with multiple scenarios:

```rust
use std::time::Duration;
use cqels_core::stream::Timestamped;
use cqels_engine::{NfaPatternProcessor, Pattern};
use futures::StreamExt;

#[derive(Debug, Clone)]
struct Event {
    kind: String,
    value: f64,
    timestamp: i64,
}

impl Timestamped for Event {
    fn timestamp(&self) -> i64 { self.timestamp }
}

#[tokio::main]
async fn main() {
    // Detect: value spike (>80) followed by crash (<10) within 30 seconds
    let pattern = Pattern::begin("spike")
        .where_cond(|e: &Event| e.value > 80.0)
        .followed_by("crash")
        .where_cond(|e: &Event| e.value < 10.0)
        .within(Duration::from_secs(30));

    let events = vec![
        Event { kind: "reading".into(), value: 50.0, timestamp: 1000 },
        Event { kind: "reading".into(), value: 95.0, timestamp: 5000 },  // spike
        Event { kind: "reading".into(), value: 60.0, timestamp: 10000 }, // ignored
        Event { kind: "reading".into(), value: 5.0,  timestamp: 20000 }, // crash
    ];

    let processor = NfaPatternProcessor::new(pattern);
    let stream = Box::pin(futures::stream::iter(events));
    let matches: Vec<_> = processor.process(stream).collect().await;

    for m in &matches {
        println!(
            "Spike-crash detected: {} events over {}ms",
            m.count(),
            m.duration(),
        );
    }
}
```
