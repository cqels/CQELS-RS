# CQELS-RS Examples

These self-contained Rust programs consume released CQELS-RS crates. They
demonstrate the same progression as the Java distribution: query parsing,
windowing, joins, CEP, reasoning, and MCP deployment guidance. They do not
vendor or mirror the engine implementation.

## Prerequisites

- Rust 1.85+
- Cargo

All dependencies resolve from the published crate release named in
`Cargo.toml`; no engine source checkout is required.

## Build and run

```bash
cargo run --manifest-path examples/Cargo.toml --bin query-language
cargo run --manifest-path examples/Cargo.toml --bin windowing
cargo run --manifest-path examples/Cargo.toml --bin cep
cargo run --manifest-path examples/Cargo.toml --bin reasoning
```

## Scenarios

| Binary | Demonstrates |
|--------|--------------|
| `query-language` | CQELS-QL parsing, prefixes, stream windows, ordering, and limits |
| `windowing` | Count and time windows over timestamped RDF elements |
| `cep` | Declarative `SEQ` syntax and CEP query registration |
| `reasoning` | RDFS subclass/type entailment with the RETE profile |

The examples are intentionally small and API-focused. Full engine source,
benchmarks, profiling studies, and parity fixtures remain private.
