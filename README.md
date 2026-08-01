# CQELS-RS

**CQELS-RS** is the Rust distribution of the CQELS continuous query engine.

CQELS is a continuous query engine for RDF and graph streams, with CQELS-QL
and CypherQL queries, windowed processing, CEP, reasoning, geospatial
functions, SHACL validation, persistent storage, and an MCP server.

> **Latest release:** [`2.0.0-alpha.16`](https://github.com/cqels/CQELS-RS/releases/tag/v2.0.0-alpha.16) · **License:** MIT · **Requires:** Rust 1.85+
>
> **New here?** Start with [GETTING_STARTED.md](GETTING_STARTED.md) · **Runnable examples:** [examples/](examples/)

---

## What it does

- **CQELS-QL** -- SPARQL-style continuous queries with time, count, session,
  sliding, directional, aggregate, and stream-static windows.
- **CypherQL** -- continuous property-graph pattern matching over RDF streams.
- **Complex Event Processing** -- declarative `FILTER(SEQ(...))` patterns with
  quantifiers, negation, contiguity, and time constraints.
- **Reasoning and validation** -- incremental RETE inference, RDFS/OWL
  profiles, SHACL validation, repair candidates, and ASP integration.
- **Operators and joins** -- filtering, binding, aggregation, ranking, MINUS,
  indexed self-joins, multi-way joins, and parallel execution paths.
- **Geospatial and storage** -- GeoSPARQL functions, R-tree indexing, and
  pluggable journal/checkpoint backends including sled and LMDB.
- **MCP integration** -- tools, prompts, resources, stream queries, reasoning,
  persistence, stdio, and opt-in Streamable HTTP.

See [CQELS-QL_SPEC.md](CQELS-QL_SPEC.md) for the public language reference.

## Quick start

Add the crates for the release you are targeting:

```toml
[dependencies]
cqels-model = "2.0.0-alpha.16"
cqels-core = "2.0.0-alpha.16"
cqels-engine = "2.0.0-alpha.16"
cqels-reasoning = "2.0.0-alpha.16"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

Parse a streaming query:

```rust
use cqels_core::parser::CqelsQlParser;

let query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?sensor ?temperature
    FROM STREAM sensors [RANGE 10s]
    WHERE { ?sensor ex:temperature ?temperature . }
"#;

let definition = CqelsQlParser::parse(query).expect("query should parse");
assert_eq!(definition.streams.len(), 1);
```

This repository is the public artifact proxy. It intentionally does not contain
the engine workspace or implementation source. Development, source review,
design records, agent configuration, issues, and pull requests are maintained
in the private [HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs)
repository.

## Distribution

Use the published Rust crates for library applications. Keep all CQELS crates
on the same version and select the version named by the release you target.

The related packages include:

- `cqels-model` -- RDF terms, statements, values, and bindings
- `cqels-core` -- query parsing, windows, operators, and execution primitives
- `cqels-engine` -- runtime, stream lifecycle, and CEP
- `cqels-reasoning` -- RETE inference profiles
- `cqels-mcp` -- MCP server binary and protocol surface

For the standalone MCP server, download the platform archive from the
[GitHub Releases](https://github.com/cqels/CQELS-RS/releases) page. Each
archive includes a SHA-256 checksum.

See [GETTING_STARTED.md](GETTING_STARTED.md) for installation and release
selection guidance.

## Public contents

- [CQELS-QL specification](CQELS-QL_SPEC.md)
- [Runnable Rust examples](examples/)
- [MCP server distribution guide](mcp-server/README.md)
- [Release verification](SUPPLY_CHAIN.md)

## Public boundary

This repository contains the public distribution surface only: specifications,
examples, release metadata, checksums, and launcher guidance. The Rust engine
workspace, source review, design records, agent configuration, issues, pull
requests, and release build workflow remain in the private
[HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs) repository.

## License

MIT
