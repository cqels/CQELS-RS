# CQELS 2.0 — Standalone CQELS Engine in Rust

**CQELS-RS** is the Rust implementation of the CQELS Execution Framework for
continuous querying and reasoning over RDF and graph streams. It shares CQELS-QL
and a versioned MCP compatibility target with [CQELS4J](https://github.com/cqels/CQELS4J).

> **Current release:** [2.0.0-alpha.21](https://github.com/cqels/CQELS-RS/releases/tag/v2.0.0-alpha.21) · **License:** MIT
>
> [Getting started](GETTING_STARTED.md) · [Fleet demos](examples/README.md) · [Compatibility and limitations](COMPATIBILITY.md)

## What it does

- **Continuous queries:** register CQELS-QL queries, push timestamped RDF observations,
  and drain matching results through MCP.
- **Complex Event Processing:** detect ordered events with `FILTER(SEQ(...))`.
- **Reasoning:** infer RDF facts using the advertised RDFS/OWL profiles.
- **Agent memory:** tools for facts, episodes, procedures, decisions, and access policy.
- **MCP:** stdio and opt-in Streamable HTTP; 25 tools, 9 resources, 1 resource template,
  and 10 prompts in this release. [Full descriptors](mcp-server/contract.json).

The fleet speed aggregate and stream/static depot lookup produce the measured
Java results in this release. This does not establish universal query or language
parity; read [COMPATIBILITY.md](COMPATIBILITY.md) for the tested boundary.

## Quick start

Install Python 3.9+ and Git. The prebuilt server does not require Rust, Cargo, Java,
Maven, or a registry account. On a supported platform:

```bash
git clone https://github.com/cqels/CQELS-RS.git
cd CQELS-RS
python3 mcp-server/install.py
python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp --scenario low-battery
```

The installer selects your platform and verifies the archive against the pinned
SHA-256 and the release checksum. On Windows use `python` and
`.cqels/bin/cqels-mcp.exe`. [Manual installation and platforms](GETTING_STARTED.md).

Expected result:

```text
low-battery: PASS [{"obs": "https://example.org/fleet/obs/2", "soc": 18.5}, {"obs": "https://example.org/fleet/obs/4", "soc": 12.0}]
```

The demo sends the same battery readings as Java's `HelloCqels`: 64, 18.5, 41, 12,
and 27.5 percent. Only observations 2 and 4 cross the threshold.

### Use it as a library

The alpha.21 CQELS packages are **not available on crates.io**. The `examples/src/`
Rust files are API illustrations, not an installable library quick start. Use the
released MCP executable for the runnable examples below. A public Cargo dependency
route will be documented only after its packages and examples can be verified.

## Demonstration scenarios

| Scenario | Java counterpart | Rust alpha.21 result |
| --- | --- | --- |
| Low battery | HelloCqels | Two alerts, numeric payloads normalized |
| Speed aggregation | WindowedAggregation | Three running aggregate rows, matching Java |
| Stream/static depot lookup | StreamStaticJoin | One depot lookup row, matching Java |
| Speed drop then spike | ComplexEventPattern | One match; reversed order emits none |
| EV subclass inference | RdfsReasoning | Inferred Vehicle type |

[Run all scenarios](examples/README.md). Successful runs require the expected
rows, including the aggregate and lookup results; registration alone cannot pass.

## Use CQELS as an MCP server

Run `.cqels/bin/cqels-mcp` for newline-delimited JSON-RPC over stdio. Configure
an MCP client with the executable's absolute path. The [MCP guide](mcp-server/README.md)
includes client configuration, HTTP, persistent storage, and discovery.

## Interoperability with COVESA CDSP

The fleet examples reuse CQELS4J's SOSA vocabulary, COVESA VSS signal IRIs, and
pseudonymous EV identifiers. These examples demonstrate data vocabulary alignment;
they do not establish complete CDSP, S2DM, storage-backend, or Java API parity.
The [Java CDSP mapping](https://github.com/cqels/CQELS4J/blob/master/CDSP_MAPPING.md)
is reference material, not a Rust capability guarantee.

## Query language and standards

[CQELS-QL](CQELS-QL_SPEC.md) adds streaming sources and windows to SPARQL-style
patterns. The examples use [SOSA/SSN](https://www.w3.org/TR/vocab-ssn/) and RDF
terms. CypherQL is a reduced streaming graph dialect, not the full openCypher
language. Consult the released server's syntax resources and validate queries
before registration; registration alone does not prove execution correctness.

## Release status

The [compatibility page](COMPATIBILITY.md) identifies the tested artifact and
bounded guarantees. [RELEASE.json](RELEASE.json) pins versions and archive digests.
[Release verification](SUPPLY_CHAIN.md) explains the checksums;
[RELEASING.md](RELEASING.md) describes how claims are checked when versions change.
This repository contains distribution documentation, examples, and release assets;
it does not contain the engine implementation workspace.

## License

[MIT](LICENSE)
