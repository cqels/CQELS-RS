# Getting Started

> **Prerequisites**: Rust 1.75+, Cargo, basic familiarity with async Rust (tokio).
>
> **Next steps**: [Data Model](data-model.md), [Architecture](architecture.md), [Stream Processing](stream-processing.md)

This guide walks you from zero to a working CQELS program. No prior RDF or
stream processing knowledge is required.

## Installation

Add the crates you need to your `Cargo.toml`:

```toml
[dependencies]
cqels-model = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-core = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-engine = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-reasoning = { git = "https://github.com/HiveIntel/cqels-rs" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

Most programs need `cqels-model` and `cqels-core`. Add `cqels-engine` for CEP
pattern matching, or `cqels-reasoning` for rule-based inference.

## Core Concepts

**RDF streams** are sequences of RDF triples, each carrying a timestamp.
A triple is a (subject, predicate, object) fact -- for example,
`<sensor/1> <temperature> "42.0"`. Triples arrive continuously, forming
an unbounded stream.

**Windows** bound the infinite stream into finite chunks. A 10-second
tumbling window groups all triples that arrived within each 10-second
interval. Windows can be time-based (tumbling, sliding), count-based,
or session-based (gap-driven).

**Continuous queries** run forever, emitting results as new data arrives.
CQELS supports two query languages: CqelsQL (SPARQL-like) and CypherQL
(Cypher-like), both with streaming window extensions.

## Your First Program: Sensor Monitoring

Create a new Rust project and add the dependencies above. Then write the
following in `src/main.rs`:

```rust
use cqels_model::{Term, Statement};
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_core::stream::RdfStreamElement;

fn main() {
    // 1. Build an RDF triple: <sensor/1> <temperature> "42.0"^^xsd:double
    let subject = Term::Iri(IriTerm::new("http://sensor/1"));
    let predicate = IriTerm::new("http://example.org/temperature");
    let object = Term::Literal(
        LiteralTerm::new("42.0")
            .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
    );
    let stmt = Statement::new(subject, predicate, object);

    // 2. Wrap it as a stream element with a timestamp (milliseconds)
    let element = RdfStreamElement::new(stmt, 1000);

    println!("Statement: {}", element.statement);
    println!("Timestamp: {}ms", element.timestamp);
}
```

### Adding a Window

Now apply a tumbling window to group elements into 10-second batches:

```rust
use std::time::Duration;
use cqels_core::stream::{RdfStreamElement, Timestamped};
use cqels_core::window::{TumblingWindow, Window};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    // Generate 10 elements spread over 25 seconds
    let elements: Vec<RdfStreamElement> = (0..10)
        .map(|i| {
            let stmt = cqels_model::Statement::new(
                cqels_model::Term::Iri(
                    cqels_model::term::IriTerm::new(format!("http://sensor/{}", i % 3)),
                ),
                cqels_model::term::IriTerm::new("http://example.org/value"),
                cqels_model::Term::Literal(
                    cqels_model::term::LiteralTerm::new(format!("{:.1}", i as f64 * 2.5)),
                ),
            );
            RdfStreamElement::new(stmt, i * 3000) // 3 seconds apart
        })
        .collect();

    // Apply a 10-second tumbling window
    let window = TumblingWindow::new(Duration::from_secs(10));
    let stream = Box::pin(futures::stream::iter(elements));
    let batches: Vec<_> = window.apply(stream).collect().await;

    for (i, batch) in batches.iter().enumerate() {
        println!(
            "Window {}: {} elements [{}-{}ms]",
            i + 1,
            batch.size(),
            batch.window_start,
            batch.window_end,
        );
    }
}
```

## Parsing a CqelsQL Query

Use `CqelsQlParser` to parse a CqelsQL query into an AST:

```rust
use cqels_core::parser::CqelsQlParser;

fn main() {
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor ?temp
        FROM STREAM sensors [RANGE 10s]
        WHERE {
            ?sensor ex:temperature ?temp .
        }
        ORDER BY ?temp DESC
        LIMIT 5
    "#;

    let def = CqelsQlParser::parse(query_str).expect("parse failed");
    println!("Streams: {:?}", def.streams.iter().map(|s| &s.name).collect::<Vec<_>>());
    println!("Select: {} elements", def.select_elements.len());
    println!("Limit: {:?}", def.limit);
}
```

The returned `CqelsQueryDefinition` contains the full AST: streams, window
specs, triple patterns, filters, aggregates, ORDER BY, and LIMIT.

## Running the Examples

The `cqels-benchmarks` crate includes four runnable examples:

```bash
# Sensor stream processing with RETE reasoning
cargo run --example sensor_stream -p cqels-benchmarks

# Window types and aggregation functions
cargo run --example window_aggregation -p cqels-benchmarks

# Complex Event Processing with 5 pattern scenarios
cargo run --example cep_pattern -p cqels-benchmarks

# CypherQL query parsing
cargo run --example cypher_query -p cqels-benchmarks
```

## Next Steps

- [Data Model](data-model.md) -- Deep dive into Term, Statement, Value, BindingSet
- [Architecture](architecture.md) -- Understand how the 5 crates compose
- [Stream Processing](stream-processing.md) -- Windows, operators, aggregation
- [Query Languages](query-languages.md) -- CqelsQL and CypherQL syntax reference
- [CEP](cep.md) -- Complex Event Processing patterns
- [Reasoning](reasoning.md) -- RETE forward-chaining inference
