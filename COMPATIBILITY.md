# CQELS-RS and CQELS4J compatibility

Rust release: **2.0.0-alpha.21**. Java reference:
[Java 2.0.0-alpha.20](https://github.com/cqels/CQELS4J/releases/tag/v2.0.0-alpha.20).
Rust source revision: `2d1b43a7c8f7e9cef143680f7747969ad4e5d038`.
Archive and reference digests are pinned in [RELEASE.json](RELEASE.json).
These observations concern the verified executable artifacts, not all unreleased
language or reasoning work.

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
| aggregation | (avg, peak, n) = (60,60,1), (70,80,2), (60,80,2) | Same numeric values | This grouped fleet aggregate works |
| static-join | EV-7Q2 at depot/north | Same row | This mixed stream/background lookup works |
| cep | One match, timestamps 1000–2000 | Same | Drop then spike works |
| cep-reversed | No matches | No matches | Reversed order is a negative control |
| rdfs | EV-7Q2 inferred as Vehicle | Same consequence | Schema-driven subclass inference works with RDFS_FULL |

Every push must acknowledge exactly one accepted observation, and the static
seed is read back from its named graph before registration. Reports retain these
controls alongside the query rows. Successful aggregate and lookup probes require
nonempty, exact expected results. Duplicate rows are retained. The independent
release suite also verifies result order, live memory edits, query-ID reuse,
replay, and solver-backed operations. A bounded passing example does not establish
full language, modifier, numerical, or storage-backend parity.

Inputs, queries, and expected rows are in [examples/fleet/](examples/fleet/).
Rust's implementation version differs from the Java reference. The release
checks assert both actual versions and compare the remaining contract strictly.

The release descriptors describe CEP `events` as an array, but both tested
artifacts actually return a human-readable string. The probe records this
encoding and validates the event subjects in addition to the match timestamps.

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

The driver verifies the jar against the pinned Java SHA-256 and uses
`java_reference.version` for Java version checks. It launches Java with the
required module-opening argument. It verifies
server versions and the full discovery contract before checking the scenarios.
The Java reference launcher has a startup race: sending `initialize` before its
startup-completion log can lose the response with a "Failed to enqueue message"
error. The driver waits for that log before negotiation, then checks engine
readiness on both servers. Keep the reference jar's default startup logging.
Reports retain raw rows and normalized comparison rows separately, including
controls for failed scenarios. They distinguish successful results from failed assertions and reference
transport failures.

The Java reference can also intermittently fail to enqueue a response after
startup, during a tool call. Waiting for the launcher does not eliminate this
separate transport failure. The driver reports the timeout and fails the run;
it does not retry the operation into a passing result.
