# Advanced Guide

> **Prerequisites**: [Stream Processing](stream-processing.md), [Query Languages](query-languages.md),
> [CEP](cep.md), [Reasoning](reasoning.md)
>
> **Next steps**: [API Reference](api-reference.md)

This guide covers extending the engine with custom operators, using the
runtime, performance tuning, and integration patterns.

## Custom Aggregate Functions

Implement `AggregateFunction<T, ACC, R>` for domain-specific aggregation:

```rust
use cqels_core::operator::aggregate::AggregateFunction;

/// Computes variance of f64 values extracted from elements.
struct VarianceAggregate<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: std::marker::PhantomData<T>,
}

/// Accumulator: (sum, sum_of_squares, count)
type VarianceAcc = (f64, f64, usize);

impl<T, F: Fn(&T) -> f64 + Send + Sync> AggregateFunction<T, VarianceAcc, f64>
    for VarianceAggregate<T, F>
{
    fn create_accumulator(&self) -> VarianceAcc { (0.0, 0.0, 0) }

    fn add(&self, element: &T, (sum, sq, n): VarianceAcc) -> VarianceAcc {
        let v = (self.extractor)(element);
        (sum + v, sq + v * v, n + 1)
    }

    fn get_result(&self, (sum, sq, n): &VarianceAcc) -> f64 {
        if *n == 0 { return 0.0; }
        let mean = sum / *n as f64;
        sq / *n as f64 - mean * mean
    }

    fn merge(&self, a: VarianceAcc, b: VarianceAcc) -> VarianceAcc {
        (a.0 + b.0, a.1 + b.1, a.2 + b.2)
    }
}
```

For sliding windows, also implement `RetractableAggregateFunction`:

```rust
use cqels_core::operator::aggregate::RetractableAggregateFunction;

impl<T, F: Fn(&T) -> f64 + Send + Sync> RetractableAggregateFunction<T, VarianceAcc, f64>
    for VarianceAggregate<T, F>
{
    fn retract(&self, element: &T, (sum, sq, n): VarianceAcc) -> VarianceAcc {
        let v = (self.extractor)(element);
        (sum - v, sq - v * v, n.saturating_sub(1))
    }

    fn supports_efficient_retraction(&self) -> bool { true }
}
```

## Custom SWAG Operations

For O(1) amortized sliding window aggregation, implement `SwagOp`:

```rust
use cqels_core::operator::swag::SwagOp;

struct SwagVarianceOp<T, F: Fn(&T) -> f64> {
    extractor: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F: Fn(&T) -> f64 + Send + Sync> SwagOp<T, VarianceAcc, f64>
    for SwagVarianceOp<T, F>
{
    fn identity(&self) -> VarianceAcc { (0.0, 0.0, 0) }

    fn lift(&self, value: &T) -> VarianceAcc {
        let v = (self.extractor)(value);
        (v, v * v, 1)
    }

    fn combine(&self, a: &VarianceAcc, b: &VarianceAcc) -> VarianceAcc {
        (a.0 + b.0, a.1 + b.1, a.2 + b.2)
    }

    fn lower(&self, (sum, sq, n): &VarianceAcc) -> f64 {
        if *n == 0 { return 0.0; }
        let mean = sum / *n as f64;
        sq / *n as f64 - mean * mean
    }

    fn invert(&self, (sum, sq, n): &VarianceAcc) -> Option<VarianceAcc> {
        Some((-sum, -sq, *n)) // invertible
    }

    fn is_invertible(&self) -> bool { true }
    fn name(&self) -> &str { "variance" }
}
```

## Implementing ContinuousQuery

Create custom query implementations:

```rust
use async_trait::async_trait;
use cqels_core::query::{ContinuousQuery, QueryInputs, QueryType};
use cqels_core::stream::StreamElement;
use std::pin::Pin;
use futures::Stream;

struct MyQuery {
    id: String,
    query_string: String,
}

#[async_trait]
impl ContinuousQuery for MyQuery {
    type Result = StreamElement;

    fn query_id(&self) -> &str { &self.id }
    fn query_string(&self) -> &str { &self.query_string }
    fn query_type(&self) -> QueryType { QueryType::Custom }

    fn execute(
        &self,
        mut inputs: QueryInputs,
    ) -> Pin<Box<dyn Stream<Item = StreamElement> + Send>> {
        let stream = inputs.take_stream("sensors").unwrap();
        // Apply windowing, filtering, aggregation...
        stream
    }
}
```

## The StreamEngine Runtime

### ReactiveStreamEngine

The engine manages stream registration, query execution, and lifecycle:

```rust
use cqels_engine::{ReactiveStreamEngine, StreamEngine};
use cqels_core::stream::StreamElement;

#[tokio::main]
async fn main() {
    let engine = ReactiveStreamEngine::new();

    // Register a stream
    let stream = Box::pin(futures::stream::iter(elements));
    engine.register_stream("sensors", stream).await.unwrap();

    // Register a query
    let result_stream = engine.register_query(Box::new(my_query)).await.unwrap();

    // Start the engine
    engine.start().await.unwrap();

    // Process results
    use futures::StreamExt;
    let mut results = result_stream;
    while let Some(result) = results.next().await {
        println!("Result: {:?}", result);
    }

    engine.stop().await.unwrap();
}
```

### Broadcast Capacity

Configure the internal broadcast channel capacity:

```rust
let engine = ReactiveStreamEngine::with_capacity(1024);
```

A larger capacity reduces backpressure at the cost of memory. Default is
suitable for most workloads.

### Multi-Subscriber Streams

Get a broadcast receiver for any registered stream:

```rust
if let Some(receiver) = engine.get_stream_receiver("sensors").await {
    // receiver: broadcast::Receiver<StreamElement>
    // Multiple subscribers can receive the same events
}
```

## Performance Tuning

### Parallel Execution

Configure parallel processing for operators:

```rust
use cqels_core::operator::parallel::{
    ParallelExecutionConfig, AggregationBackend, SwagConfig,
};

let config = ParallelExecutionConfig {
    parallelism: 4,          // number of threads
    prefetch: 256,           // prefetch buffer size
    auto_parallelize: true,  // auto-detect when to parallelize
    min_stream_size: 1000,   // minimum batch size for parallelization
    aggregation_backend: AggregationBackend::Swag(SwagConfig::default()),
};
```

### Aggregation Backend

| Backend | Best For | Trade-off |
|---------|----------|-----------|
| `Legacy` | Small windows, simple aggregates | Lower overhead |
| `Swag` | Large sliding windows | O(1) amortized per element |

### Window Size Impact

- Larger windows consume more memory (all elements buffered)
- Sliding windows with small slide ratios create many overlapping batches
- Session windows are memory-efficient when gaps are frequent
- Count windows have bounded memory usage (exactly N elements)

### RETE Network Tuning

- `default_window` controls working memory size -- shorter windows use less
  memory but may miss cross-window joins
- `enable_recursive_inference(false)` prevents inference chains that can
  grow unbounded
- Priority-based conflict resolution is faster than MEA for most workloads

## Combining CEP with Reasoning

### Pattern: RETE -> CEP

Use inferred triples as input to CEP patterns:

```rust
let mut network = ReteNetwork::compile(config);

// Convert inferred triples to CEP-compatible events
let mut cep_events = Vec::new();
for element in &stream {
    let inferred = network.process_element(element);
    for inf in inferred {
        cep_events.push(inf.to_rdf_stream_element());
    }
}

// Feed inferred events to CEP
let processor = NfaPatternProcessor::new(alert_pattern);
let stream = Box::pin(futures::stream::iter(cep_events));
let matches: Vec<_> = processor.process(stream).collect().await;
```

### Pattern: CEP -> RETE

Feed CEP match results back as new stream elements for reasoning:

```rust
for m in &matches {
    // Create a new RDF triple from the match
    let stmt = Statement::new(
        Term::Iri(IriTerm::new("http://alert/1")),
        IriTerm::new("http://example.org/matchedPattern"),
        Term::Literal(LiteralTerm::new(format!("{} events", m.count()))),
    );
    let elem = RdfStreamElement::new(stmt, m.end_timestamp());
    network.process_element(&elem);
}
```

## RSP-QL Operators

The RSP-QL model defines three stream construction operators:

### IStreamOperator

Emits elements that are newly inserted into a window:

```rust
use cqels_core::operator::rspql::IStreamOperator;
```

### DStreamOperator

Emits elements that are being deleted (evicted) from a window:

```rust
use cqels_core::operator::rspql::DStreamOperator;
```

### RStreamOperator

Emits the full window snapshot on each update:

```rust
use cqels_core::operator::rspql::RStreamOperator;
```

These operators work with `WindowUpdate` and `WindowSnapshot` types to
provide different views of window state changes.

## Error Handling Patterns

### Comprehensive Error Matching

```rust
use cqels_model::{CqelsError, ParseErrorKind};

match result {
    Err(CqelsError::Parse(detail)) => {
        println!("Parse error ({:?}): {}", detail.kind, detail.message);
        if let (Some(line), Some(col)) = (detail.line, detail.column) {
            println!("  at line {line}, column {col}");
        }
    }
    Err(CqelsError::BindingNotFound { variable }) => {
        println!("Missing variable: {variable}");
    }
    Err(CqelsError::InvalidTerm { detail }) => {
        println!("Invalid term: {detail}");
    }
    Err(e) => println!("Other error: {e}"),
    Ok(value) => { /* success */ }
}
```

### Using CqelsResult

```rust
use cqels_model::CqelsResult;

fn process_sensor(data: &str) -> CqelsResult<Value> {
    let def = CqelsQlParser::parse(data)?;  // ParseError -> CqelsError::Parse
    let binding = bs.get_required("temp")?;  // -> CqelsError::BindingNotFound
    Ok(binding.clone())
}
```
