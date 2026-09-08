# CQELS-RS and CQELS4J compatibility

Tested release: **2.0.0-alpha.20** on both implementations. Rust build revision:
`a76244c51456ecc20252d718ca8f26b39a05c756`. Archive and Java reference digests are
pinned in [RELEASE.json](RELEASE.json). These observations concern the downloadable
artifacts, not subsequent source changes or an unreleased Delta/reasoning port.

## Interface boundary

The released Rust and Java servers expose matching discovery descriptors: **25
tools, 9 resources, 1 resource template, 10 prompts**. The negotiated stdio protocol
is `2024-11-05`. [contract.json](mcp-server/contract.json) records the complete
schemas and descriptions; the executable probe compares all descriptors.

This is an MCP interface comparison, not Java ABI, Maven, classpath-plugin, native
Rust API, or full query-semantic compatibility. The descriptor descriptions are
server-advertised claims; the measured behavior below takes precedence where the
release does not deliver those claims.

## Measured fleet behavior

All scenarios use the same input and query on both executables. JSON object key
order and row order are ignored; duplicate rows are retained. Only declared
numeric columns are normalized from Rust strings to numbers. RDFS checks the
specific subclass consequence independently of unrelated axiomatic triples.

| Probe | Rust | Java | Consequence |
| --- | --- | --- | --- |
| low-battery | obs/2 = 18.5, obs/4 = 12 | Same values as JSON numbers | Consumers must accept numeric strings from Rust |
| aggregation | No rows | Three running rows: (avg, peak, n) = (60,60,1), (70,80,2), (60,80,2) | Grouped three-pattern fleet speed aggregate is not usable on this Rust artifact |
| static-join | No rows | EV-7Q2 at depot/north | This mixed stream/background lookup is not usable on this Rust artifact |
| cep | One match, timestamps 1000–2000 | Same | Drop then spike works |
| cep-reversed | No matches | No matches | Reversed order is a negative control |
| rdfs | EV-7Q2 inferred as Vehicle | Same consequence | Schema-driven subclass inference works with RDFS_FULL |

Empty results are observed across a 1.5-second drain interval after all pushes.
The probes assert both the working behavior and the known differences. If a later
release fixes a limitation, the old expectation fails so the documentation must
change. Inputs, queries, and expected rows are in [examples/fleet/](examples/fleet/).
These bounded probes do not prove that every aggregate or static query fails, nor
that every other query form succeeds. There is currently no verified Rust
workaround for the two listed query routes.

## Query shapes and external dependencies

- `[NOW]` low-battery filtering uses typed numeric N-Quads and an explicit STREAM
  block. A plain JSON fact object defaults to a string literal.
- CEP registration must set `cep: true`. Single-triple CEP steps in these probes
  receive single-triple observations. Multi-triple graph-event matching is a
  different query shape and is not established by this result.
- RDFS inference stores the ontology in `cqels://memory/schema` and data in
  `cqels://memory/longterm`, then calls `reason(profile: "RDFS_FULL")`.
- `query` reads the stored RDF snapshot, not in-flight stream events.
- Solve/standing ASP routes can require an external Clingo executable. The fleet
  demos require neither Clingo nor SHACL repair. Their results are not evidence
  for all ASP/SHACL programs, Java extension functions, or storage plugins.
- CypherQL is a reduced graph query dialect. Full openCypher compatibility is not
  claimed; Java-specific unsupported syntax is not automatically a Rust limit.
- Persistent RDF memory and durable stream storage are separate settings; see
  the [MCP guide](mcp-server/README.md). Observe the durable acknowledgement rather
  than assuming that accepted data has been persisted.

## Reproduce

Install the pinned Rust binary and run:

```bash
python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp --report rust.json
```

For the reference comparison, download the Java shaded jar from the
[Java alpha.20 release](https://github.com/cqels/CQELS4J/releases/tag/v2.0.0-alpha.20),
verify its SHA-256 against `RELEASE.json`, install JDK 17+, and run:

```bash
python3 examples/mcp_fleet.py --java-jar cqels-mcp-2.0.0-alpha.20-shaded.jar --report java.json
```

The driver launches Java with the required module-opening argument. It verifies
server versions and the full discovery contract before checking the scenarios.
The Java reference launcher has a startup race: sending `initialize` before its
startup-completion log can lose the response with a "Failed to enqueue message"
error. The driver waits for that log before negotiation, then checks engine
readiness on both servers. Keep the reference jar's default startup logging.
Reports distinguish passing scenarios from reproduced known differences.
