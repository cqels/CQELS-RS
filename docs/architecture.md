# Architecture

> **Prerequisites**: [Getting Started](getting-started.md)
>
> **Next steps**: [Data Model](data-model.md), [Stream Processing](stream-processing.md)

This document describes how the five crates compose and how data flows
through the processing pipeline.

## Crate Dependency Graph

```
cqels-model              Foundational RDF types
    |
    v
cqels-core               Stream processing core
    |
    +------+------+
    |      |      |
    v      v      v
cqels-   cqels-  cqels-
engine   reason  bench
         -ing    -marks
```

All crates depend on `cqels-model`. `cqels-core` is the central processing
hub. The leaf crates (`engine`, `reasoning`, `benchmarks`) are independent
of each other.

## Crate Summaries

### cqels-model

The foundation layer containing zero processing logic:

- **`Term`** -- RDF term enum (IRI, blank node, literal)
- **`Statement`** -- RDF triple/quad
- **`Value`** -- Dynamically typed runtime value
- **`BindingSet`** -- Variable-to-value mappings with timestamp
- **`CqelsError`** -- Unified error type with structured parse errors
- Bidirectional `From`/`TryFrom` conversions with `oxrdf`

### cqels-core

The processing pipeline heart, organized into five modules:

| Module | Purpose |
|--------|---------|
| `stream` | Stream element types, `Timestamped` trait |
| `window` | 4 window implementations + `Window<T>` trait |
| `operator` | Aggregate, filter, join, bind, ranking, SWAG, RSP-QL, parallel config |
| `parser` | CqelsQL + CypherQL parsers producing AST definitions |
| `query` | `ContinuousQuery` trait, `QueryInputs` |

### cqels-engine

Runtime orchestration with two modules:

- **`engine`** -- `StreamEngine` trait + `ReactiveStreamEngine` backed by
  tokio broadcast channels for multi-subscriber streams
- **`cep`** -- NFA pattern matching: `Pattern` builder, `NfaPatternProcessor`,
  `PatternMatch`, contiguity modes, quantifiers, negation

### cqels-reasoning

RETE forward-chaining inference:

- **Alpha network** -- Single-pattern filtering against incoming triples
- **Beta network** -- Multi-pattern join on shared variables
- **Working memory** -- Indexed fact storage with subject/predicate/object lookups
- **Conflict resolution** -- Priority-based or fire-all strategies
- **Productions** -- Activated rules producing `InferredRdfStreamElement` with provenance

### cqels-benchmarks

Criterion benchmarks, synthetic data generators (`generate_sensor_readings`,
`generate_social_events`, `generate_rdf_stream_batch`), and four runnable examples.

## Data Flow Pipeline

```
1. Ingest          Raw events arrive as StreamElement / RdfStreamElement
       |
       v
2. Window          Window<T> partitions the stream into WindowedBatch<T>
       |
       v
3. Evaluate        Operators (join, filter, aggregate) process each batch
       |
       v
4. Reason          (optional) RETE network infers new triples
       |
       v
5. Emit            Results pushed downstream as new stream elements
```

### Step 1: Ingest

Events enter the system as `StreamElement` values. For RDF data, this is
`StreamElement::Rdf(RdfStreamElement)` carrying a `Statement` and a
timestamp in milliseconds.

### Step 2: Window

The `Window<T>` trait accepts a `Pin<Box<dyn Stream<Item = T> + Send>>`
and produces a stream of `WindowedBatch<T>`. Each batch contains the
elements that belong to one window instance, plus the window boundaries.

### Step 3: Evaluate

Operators transform and combine batches:

- **FilterOperator** -- Removes bindings that don't match a predicate
- **BindOperator** -- Computes a new variable from existing bindings
- **WindowedJoinState** -- Joins two streams within a time window
- **AggregateFunction** -- Computes COUNT, SUM, AVG, MIN, MAX over batches
- **TopKOperator** -- Maintains a ranked subset

### Step 4: Reason (optional)

The `ReteNetwork` accepts `RdfStreamElement` values and produces
`InferredRdfStreamElement` values. Inferred triples can be fed back
into the pipeline as new stream elements.

### Step 5: Emit

Results flow downstream as `BindingSet` values, stream elements, or
aggregate results, depending on the query type.

## Key Design Decisions

### Flat Enums Over Class Hierarchies

Where Java CQELS uses deep inheritance (e.g., `StreamElement` subclasses),
the Rust port uses flat enums with explicit variants. This avoids virtual
dispatch overhead and makes pattern matching exhaustive.

### Builder Patterns

Complex types use the builder pattern: `Rule::builder()`,
`ReasoningConfig::builder()`, `RuleSet::builder()`. This keeps
constructors simple while supporting many optional parameters.

### `Pin<Box<dyn Stream>>` for Composition

Async stream pipelines compose via `Pin<Box<dyn Stream<Item = T> + Send>>`.
This provides type-erased, heap-allocated streams that can be chained
through windows, operators, and processors.

### `Timestamped` Trait

All streamable items implement `Timestamped` (a single method:
`fn timestamp(&self) -> i64`). This allows generic window and CEP
implementations that work with any timestamped type.

### Functional Aggregation

Aggregate functions use a functional style:
`fn add(&self, element: &T, accumulator: ACC) -> ACC`. Accumulators
are values, not mutable state, enabling easy parallelization and merging.

## Threading Model

- **tokio** for the async runtime -- all stream processing is `async`
- **Broadcast channels** for fan-out -- `ReactiveStreamEngine` uses
  `tokio::sync::broadcast` to multicast stream elements to multiple
  subscribers
- **DashMap** for concurrent state -- the engine's stream registry uses
  `DashMap` for lock-free concurrent reads
- **rayon** for data parallelism -- optional parallel aggregation and
  join operators via the `ParallelExecutionConfig`

All public traits require `Send + Sync` bounds, ensuring safe use across
tokio tasks.
