# CQELS-RS

**Continuous Query Evaluation over Linked Streams -- in Rust.**

A high-performance streaming query engine for RDF data, featuring dual query
languages, windowed processing, complex event detection, and rule-based
reasoning. Rust port of the [CQELS 2.0](https://cqels.github.io/) engine.

## Features

- **Dual query languages** -- CqelsQL (SPARQL-based) and CypherQL (Cypher-based) with streaming extensions
- **Four window types** -- Tumbling, sliding, session, and count-based windows
- **Stream operators** -- Aggregate, filter, join, bind, ranking, and SWAG (Sliding-Window AGgregation)
- **Complex Event Processing** -- NFA-based pattern matching with strict/relaxed contiguity, quantifiers, negation, and time constraints
- **RETE reasoning** -- Forward-chaining incremental inference over RDF streams with provenance tracking
- **Async-first** -- Built on tokio with `Stream`-based dataflow composition
- **RDF ecosystem** -- Bidirectional interop with [oxrdf](https://docs.rs/oxrdf)

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
cqels-model = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-core = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-engine = { git = "https://github.com/HiveIntel/cqels-rs" }
cqels-reasoning = { git = "https://github.com/HiveIntel/cqels-rs" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

Parse a CqelsQL query and process sensor data:

```rust
use cqels_core::parser::CqelsQlParser;
use cqels_model::{Term, Statement};
use cqels_model::term::{IriTerm, LiteralTerm};

// Parse a continuous query
let query = CqelsQlParser::parse(r#"
    PREFIX ex: <http://example.org/>
    SELECT ?sensor ?temp
    FROM STREAM sensors [RANGE 10s]
    WHERE { ?sensor ex:temperature ?temp . }
    ORDER BY ?temp DESC
    LIMIT 5
"#).unwrap();

// Build an RDF triple
let stmt = Statement::new(
    Term::Iri(IriTerm::new("http://sensor/1")),
    IriTerm::new("http://example.org/temperature"),
    Term::Literal(LiteralTerm::new("42.0")
        .with_datatype("http://www.w3.org/2001/XMLSchema#double")),
);
```

## Architecture

```
cqels-model          Foundational RDF types (Term, Statement, Value, BindingSet)
    |
cqels-core           Stream processing core (windows, operators, parsers)
    |
    +-- cqels-engine      Runtime engine + CEP pattern matching
    +-- cqels-reasoning   RETE forward-chaining inference
    +-- cqels-benchmarks  Criterion benchmarks + examples
```

## Examples

| Example | Description | Command |
|---------|-------------|---------|
| `sensor_stream` | CqelsQL parsing + RETE reasoning over IoT data | `cargo run --example sensor_stream -p cqels-benchmarks` |
| `window_aggregation` | All window types + COUNT/SUM/AVG/MIN/MAX | `cargo run --example window_aggregation -p cqels-benchmarks` |
| `cep_pattern` | CEP with 5 pattern scenarios (strict, relaxed, negation, etc.) | `cargo run --example cep_pattern -p cqels-benchmarks` |
| `cypher_query` | CypherQL query parsing with relationships and properties | `cargo run --example cypher_query -p cqels-benchmarks` |

## Documentation

| Guide | Level | Description |
|-------|-------|-------------|
| [Getting Started](docs/getting-started.md) | Beginner | Installation, first program, core concepts |
| [Data Model](docs/data-model.md) | Beginner | Terms, Statements, Values, Bindings, Errors |
| [Architecture](docs/architecture.md) | Intermediate | Crate structure, data flow, design decisions |
| [Stream Processing](docs/stream-processing.md) | Intermediate | Windows, operators, aggregation |
| [Query Languages](docs/query-languages.md) | Intermediate | CqelsQL and CypherQL syntax and usage |
| [Complex Event Processing](docs/cep.md) | Intermediate | NFA pattern matching and detection |
| [Reasoning](docs/reasoning.md) | Advanced | RETE network, rules, inference |
| [Advanced Guide](docs/advanced.md) | Advanced | Custom operators, performance tuning, engine runtime |
| [Testing](docs/testing.md) | Reference | Regression suites, `xtask` workflows, CI policy |
| [API Reference](docs/api-reference.md) | Reference | All public types and traits |

Full API docs: `cargo doc --workspace --no-deps --open`

## Testing Workflow

The supported contributor workflow uses `xtask`:

```bash
cargo xtask test pr
cargo xtask test impact --base origin/main
cargo xtask test full
```

See [docs/testing.md](docs/testing.md) for the full layered regression policy,
issue-regression rules, and CI process.

## Requirements

- Rust 1.83+
- Edition 2021

## License

Apache-2.0
