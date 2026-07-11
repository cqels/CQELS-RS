# CQELS Java alpha.10 vs Rust Port Comparative Analysis

Date: 2026-07-12

Java reference: `github.com/cqels/claude` tag `v2.0.0-alpha.10`, peeled commit
`d246ade186bfb42fe5da43351bad973343d19411`.

Rust reference: this worktree on `codex/mcp-alpha10-stack`.

## Verdict

Rust has broad overlap with the advertised MCP surface, but it is not yet at full
behavioral parity with Java `2.0.0-alpha.10`.

The Rust port currently has the main user-visible alpha.9/alpha.10 handles:
`create_stream`, `push_stream_events`, `validate_stream_query`,
`register_stream_query`, `watch_invariant`, `register_rules`,
`CQELS_MCP_REASONING`, live MCP resources, and the `CQELS_MCP_RDF_STORE_PATH`
environment key. That is surface coverage for many agent workflows, not a proof
that the behavior is identical.

The remaining gap is semantic parity. Rust now has the default MCP server wired
to fail closed for the main governed tool/resource surfaces, and it now
preserves Java-style graph observations on the CQELS-QL/MCP stream-ingest path.
The latest local fixture sweep also closes the previously tracked Rust-side
core-query fixture failures in this corpus: Rust now passes all 12 parity
fixtures against the checked-in oracles. That is still not full Java alpha.10
parity: persistence, broader notifications, plugin SPI/discovery, broader
cross-language fixture contracts, and exact input-contract details such as
Java's Turtle-only SHACL shape registration and registration-time ASP parse are
not fully implemented or proved yet.

## Evidence Summary

Java alpha.10:

- Release notes describe continuous reasoning, persistent RDF quad storage, and
  deploy-time plugin SPI in `CHANGELOG.md`.
- `GraphStreamElement` carries a list of RDF statements as one timestamped
  stream element; Java's CQELS-QL compiled path expands the graph only during
  matching so `[TRIPLES n]` counts observations.
- `StreamIngestToolHandler` enforces governance, event-time bounds, stream caps,
  message/statement/character caps, reserved-graph rejection, parse-before-push,
  strict `facts[*].objectType` values (`uri` or `literal`, default
  `literal`), and single-vs-multi-statement push semantics.
- `QueryToolHandler`, `ContinuousReasoningToolHandler`, and
  `KnowledgeGraphResourceProvider` wire governance into query, stream-query,
  continuous reasoning, result drains, and metadata resources.
- `CqelsMcpServer` uses RDF4J `NativeStore` when
  `CQELS_MCP_RDF_STORE_PATH` is set, wires resource notifications, discovers
  embedding providers via `ServiceLoader`, and loads governed plugin tools.

Rust current state:

- `cqels-core/src/stream.rs` now has `StreamElement::Graph` and
  `GraphStreamElement`; `is_rdf()` remains a single-statement guard, while
  `rdf_statements()` expands graph observations.
- `cqels-core/src/compiler/compiled.rs` expands graph observations during
  CQELS-QL matching and the self-join fast path while count windows still count
  the graph as one stream element.
- `cqels-engine::DataStream` exposes `push_graph`, and
  `cqels-storage-spi` round-trips graph elements through the JSON stream codec.
- `cqels-mcp/src/stream_query.rs` parses RDF-message observations and now
  pushes one statement as `RdfStreamElement`, multi-statement observations as
  `GraphStreamElement`, and inferred triples as appended single RDF elements.
- `push_stream_events` now rejects empty event arrays, fractional or
  out-of-range event times, over-limit streams, per-message statement floods,
  over-limit observation counts, per-event character floods, total-call
  character floods, and Java-invalid `facts[*].objectType` values before any
  push.
- `watch_invariant` and `register_rules` now use Java-style returned query-id
  prefixes (`wi-` / `rl-`), enforce Java alpha.10 live-registration caps,
  advertise/enforce the Java buffer-size caps, and reject over-limit SHACL shape
  or ASP rule payloads before registration. `register_rules` also enforces the
  Java alpha.10 `argNames`, `emit`, and `maxFacts` bounds.
- `cqels-mcp/src/tools.rs` has an `AccessPolicyRegistry`, and the default
  server now wires it into memory recall, context assembly, `query`, governed
  stream tools, continuous observer registration, result polling, and live
  resources.
- `cqels-mcp/src/resource.rs` now withholds governed metadata/result resources
  under active policy, preserves result buffers instead of draining them, and
  surfaces Java alpha.10 `engine/status` liveness fields (`version`,
  `transport`, `persistence`, `rdfStore.persistent`, `streamReasoning`) even
  when live query metadata is withheld.
- `cqels-mcp/src/bin/cqels_mcp_server.rs` treats `CQELS_MCP_RDF_STORE_PATH` as
  a sled-backed MCP memory alias under `<path>/cqels-mcp-memory`, not as a
  persistent RDF quad store.
- Rust extension points are native registries and traits; there is no
  Java-style deploy-time plugin SPI or provider discovery layer.
- Focused Rust MCP verification on this branch (2026-07-12):
  - `cargo test -p cqels-mcp watch_invariant --lib`: 2/2 passed.
  - `cargo test -p cqels-mcp register_rules --lib`: 3/3 passed.
  - `cargo test -p cqels-mcp`: 217 unit tests plus stdio integration passed.
- Fresh fixture sweep on this branch (2026-07-12):
  - `cargo xtask parity --rust-only`: Rust passed 12/12 fixtures.
  - Java alpha.10 runner (`mvn -q -B -DskipTests
    -Dcqels.dependency.version=2.0.0-alpha.10 package`, then
    `java -jar target/cqels-parity-runner-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar --all ../fixtures`):
    Java alpha.10 passed 5/12 fixtures against the current checked-in oracles. The
    remaining Java failures/errors are documented semantic/oracle differences
    or Java parser hard-errors for STREAM-scoped OPTIONAL/UNION.

## Latest Executable Sweep

| Fixture | Rust current | Java alpha.10 |
|---|---|---|
| `simple-select-now` | ok | ok |
| `range-window-low-volume` | ok | ok |
| `triples-window-batch` | ok | ok |
| `order-by-limit` | ok | ok |
| `stream-static-lookup-join` | ok | ok |
| `cep-sequence-two-events` | ok | FAIL |
| `count-aggregate` | ok | FAIL |
| `self-join-rides` | ok | FAIL |
| `triples-filter-numeric` | ok | FAIL |
| `triples-multi-pattern-join` | ok | FAIL |
| `optional-extra-property` | ok | ERR |
| `union-two-properties` | ok | ERR |

The Java `ERR` rows are alpha.10 parser/runtime hard errors for OPTIONAL/UNION
inside a `STREAM` block. The Java `FAIL` rows are binding mismatches against
the current hand-spec/Rust-oriented oracles, not necessarily regressions in
Java: several fixture metadata files intentionally document RSTREAM cumulative
emission, SEQ standard-path behavior, aggregate emission shape, or typed-filter
differences. The current 5/12 Java alpha.10 count is against checked-in
hand-spec/Rust-oriented oracles, not Java-golden captures; the
`triples-filter-numeric` row is one reason the stale earlier 6/12 note no
longer applies, because the fixture now uses typed integer data and Java
alpha.10 emitted no matching rows in this run.

## Capability Matrix

| Area | Java alpha.10 behavior | Rust current behavior | Parity |
|---|---|---|---|
| MCP tool/resource names | Alpha.10 advertises stream ingest, stream query, continuous reasoning, memory, reasoning, procedure, episodic, decision, governance, resources, prompts, stdio/HTTP. | Rust exposes the main names plus several Rust extras; this analysis did not produce a formal one-by-one inventory diff. | Broad surface overlap; inventory diff still needed before final parity sign-off. |
| `create_stream` | Idempotent, bounded by `MAX_STREAMS`, denied under active governance. | Idempotent, denied under active governance in the default server, and bounded by the Java alpha.10 stream cap. | Close for the MCP tool. |
| `push_stream_events` | Parse/validate all events before push, cap events/messages/statements/chars/streams, reject fractional/out-of-range event times, reject `cqels://` graph contexts, deny under governance, default missing/null `facts[*].objectType` to `literal`, accept only `uri` or `literal`, and push one statement as RDF and multi-statement observations as graph elements. | Parses before push, denies under active governance in the default server, rejects empty event arrays, enforces Java-style event-time range/whole-number validation, stream cap, per-message cap, total-observation cap, total-statement cap, N-Quads/per-event character cap, total-call character cap, and Java-style `objectType` default/enum strictness; reserved contexts are rejected, empty RDF messages are skipped, nonblank `nquads` takes precedence over `facts`, and single-vs-multi observations use the Java RDF/graph split. | Close for tracked alpha.10 hardening; broader schema inventory still needed. |
| Atomic multi-statement observations | `GraphStreamElement` keeps one observation in one window slot; CQELS-QL expands statements for matching. Java documents this as supported on the CQELS-QL compiled stream/windowed path, not on every legacy/Cypher/CEP path. | Rust now has the same CQELS-QL/MCP support boundary: graph observations count as one element for windows, expand for CQELS-QL matching and the self-join fast path, round-trip through the stream codec, and are exposed via `DataStream::push_graph`. | Close for CQELS-QL/MCP graph-observation semantics; cross-language fixture evidence still needed. |
| `[TRIPLES n]` observation semantics | Counts stream elements, so graph observations count as one and never split. | Rust count windows now count a graph observation as one element, and MCP multi-statement observations are delivered as graph elements. | Close for the tested CQELS-QL path. |
| `register_stream_query` | Governed fail-closed; buffers results, supports Java language/CEP surface according to Java handler. Existing listeners drop results while governance is active. | Buffers and polls results, denies registration under active governance in the default server, and listener callbacks drop results under active policy; still only accepts `cqelsql`, while `cep:true` fails loud. | Partial. |
| `poll_stream_results` / result resource | Java drains via `recall_memory(queryId)` and resource template, denied/withheld under governance. | `poll_stream_results` denies under active governance; `cqels://queries/{queryId}/results` returns a denial payload without draining. | Close for governed withholding; remaining parity depends on broader Java result-resource semantics. |
| `watch_invariant` | Continuous SHACL per observation, governed fail-closed, forced `wi-` query-id prefix, registration cap of 16, 500k shape-size cap, buffer cap, optional notifications, drops results under governance. Java accepts SHACL Turtle and parses/compiles before registration. | Present, denied under active governance in the default server, drops observer results while governance is active, forces `wi-` ids, enforces 16 live watch registrations, rejects over-500k shape payloads, clamps buffers, and queues stdio `notifications/resources/updated` signals for notify-enabled query-results resources. Rust still accepts JSON/N-Quads shape statements rather than Java's Turtle-only shape contract. | Close for governance, prefix, caps, buffering, and stdio query-result notifications; partial for exact shape input contract. |
| `register_rules` | Continuous ASP accumulate/solve, governed fail-closed, forced `rl-` query-id prefix, registration cap of 8, 100k rule-size cap, `argNames` cap/validation, `emit` validation, `maxFacts`/buffer caps, registration-time ASP parse, delta-memory diagnostic, optional notifications, result drop under governance. | Present with `maxFacts`, `emit`, buffers, solver injection, default-server governance denial, result drop under active policy, forced `rl-` ids, 8 live rule registrations, 100k rule payload rejection, `argNames` cap/blank/duplicate validation, Java-style `maxFacts` and buffer caps, and stdio query-result notifications. Rust does not yet parse the ASP program at registration time or emit Java's delta-memory-cap diagnostic. | Close for the main input bounds and runtime observer behavior; partial for Java's registration-time ASP parse and diagnostic details. |
| `CQELS_MCP_REASONING` | Opt-in `rdfs` or `rdfs-full` stream reasoning; original observations flow first, graph observations stay atomic, inferred single triples are appended, and a RETE fact cap hardens memory. | Opt-in parser and RDFS/RDFS-full flow are present; original observations flow first, graph observations stay atomic, and inferred triples are appended as single RDF observations. `apply_stream_reasoning` still does not wire a Java-equivalent RETE fact cap. | Partial. |
| MCP resources | Metadata resources are withheld under governance, except liveness fields in `engine/status`; `engine/status` reports persistence and RDF-store persistence facts. | Governed resource registry withholds metadata/result resources, keeps Java-style `engine/status` liveness fields readable, reports `registeredQueryCount`, `persistence`, `rdfStore.persistent`, and `streamReasoning`, preserves result buffers when governed, and drains queued query-result update notifications for stdio transports. `rdfStore.persistent` correctly remains `false` because Rust does not yet implement Java's RDF4J NativeStore equivalent. | Close for status shape, governed liveness, and stdio query-result notification shape; broader persistence and non-stdio/non-query notification semantics remain partial. |
| `CQELS_MCP_RDF_STORE_PATH` | Switches the RDF repository to RDF4J `NativeStore`; stored facts and saved procedures survive restart. | Accepted as an alias root for sled MCP memory; stream events and RDF quads are not persisted as Java NativeStore equivalents. | Not equivalent. |
| Plugin SPI | New `cqels-plugin-spi`; `ServiceLoader` discovers plugins and embedding providers; plugin tools are namespaced, atomic, and governed. | Native `ToolRegistry`, `MemoryStore`, solver injection, and resource registries exist, but no deploy-time plugin discovery or governed plugin registrar. | Intentional non-parity today; blocker if the goal is full Java alpha.10 parity. |
| Notifications | Java uses `McpNotifier` for query/resource result notifications when requested. | Rust now queues one canonical `notifications/resources/updated` signal per notify-enabled result row and the stdio loop emits those signals after the triggering request. HTTP remains request/response only, and non-query resources such as materialized inference notifications are not broadly wired. | Partial, but no longer accepted-only for query-result stdio notifications. |
| Ingest event-time validation | Whole-number epoch millis or ISO instant, bounded to `[0, 7258118400000]`, overflow-safe. | Rust now rejects fractional, negative, and far-future event times, validates numeric strings and UTC instants through the same upper bound, and uses checked timestamp arithmetic. | Close for the MCP ingest path. |
| Fixture parity | Java/Rust fixture harness exists; the CI Java-runner gate enforces a named known-passing subset and runs the full corpus informationally. A fresh local Java alpha.10 run passed 5/12 against the current oracles. | Rust now passes the full checked-in 12-fixture corpus locally. The corpus covers NOW, TRIPLES windows, FILTER, STREAM-scoped OPTIONAL/UNION, ORDER BY/LIMIT, static `FROM <iri>` lookup, aggregate, CEP SEQ documentation, and known emission-semantics differences. | Strong evidence for covered fixtures only, not universal equality. |
| Broader core-query parity | Java alpha.10 includes fixes or behavior for static lookup joins and Java-side ORDER BY/LIMIT handling. Some fixtures also document intentional or unresolved semantic differences, such as raw RSTREAM re-emission versus Rust's single-emission output. Java currently hard-errors for STREAM-scoped OPTIONAL/UNION in the local Java runner. | Rust now accepts the fixture-covered STREAM-scoped OPTIONAL/UNION forms, `ORDER BY DESC(?var)`, numeric FILTERs over typed integer fixture events, and SPARQL `FROM <iri>` static graph evaluation. Known remaining comparison limits are `FILTER(SEQ())` standard-path semantics, Java/Rust RSTREAM-vs-single-emission differences, and broader untested grammar/runtime surfaces. | Improved; full Java alpha.10 parity still needs more cross-language fixtures beyond this corpus. |

## Meaning of Fixture-by-Fixture, Not Universal Equality

The parity harness proves the fixtures it runs. It does not prove that every
CQELS feature, every MCP tool, and every security edge case behaves identically.

That distinction matters here for two reasons.

First, the current known fixture gate is subset-based. Rust now passes every
checked-in fixture locally, but the corpus is still finite. Workloads such as
`cep-sequence-two-events`, `count-aggregate`, `self-join-rides`, and
`triples-multi-pattern-join` continue to document Java/Rust semantic or
comparison-mode differences, especially around SEQ enforcement and raw
RSTREAM-style re-emission. Those are valuable regression artifacts, but they
are not a universal equivalence proof.

Second, the alpha.9/alpha.10 deltas include surfaces that the query fixtures do
not exercise broadly enough: governed denials and result withholding,
graph-element atomicity through MCP ingest, persistent RDF quad storage, plugin
discovery, notifications, and broader MCP input-contract strictness.

So the accurate statement is: Rust has validated parity for the specific
fixtures and unit tests it carries, and it has much of the alpha.10 MCP surface.
It cannot yet be declared universally identical to Java alpha.10.

## Blockers to Claiming Full Alpha.10 Parity

1. Add cross-language fixtures for alpha.9/alpha.10 MCP semantics: graph
   observations, event-time rejection, governed denials, resource withholding,
   result-drop behavior, and persistence restart behavior. Ensure future
   extension/plugin tools are wrapped in the same fail-closed posture.
2. Reconcile the remaining broader core-query comparison gaps that are not
   closed by the 12-fixture Rust sweep: standard-path SEQ execution,
   Java/Rust RSTREAM-vs-single-emission comparison mode, deterministic
   aggregate ordering across repeated runs, and broader parser/runtime
   coverage outside the current fixture corpus.
3. Decide and implement the persistence story for `CQELS_MCP_RDF_STORE_PATH`:
   true persistent RDF quad store parity, or an explicit documented non-parity
   accepted by the project.
4. Extend notification parity beyond the newly wired stdio query-result
   signals: decide whether HTTP/SSE-style push and non-query resource
   notifications are required for the Rust release target.
5. Choose the plugin parity target. Full Java alpha.10 parity requires a
   deploy-time Rust plugin SPI/discovery layer with namespacing, collision
   checks, atomic registration, and governance wrapping; otherwise this remains
   a deliberate non-parity item.
6. Finish the exact MCP input-contract inventory beyond the now-closed
   `push_stream_events` `facts[*].objectType` contract and the newly aligned
   continuous-reasoning caps: notably Java's Turtle-only `watch_invariant`
   shapes contract, `register_rules` registration-time ASP parse, and
   Java's delta-memory-cap diagnostic behavior.

## Recommended Next Implementation Order

1. Alpha.9/alpha.10 MCP cross-language fixtures. The Rust query corpus is now
   green locally; the next evidence gap is MCP graph/governance/result behavior
   rather than more Rust-only fixture cleanup.
2. Exact MCP input-contract inventory. The tracked ingest hardening,
   `facts[*].objectType` strictness, and continuous-reasoning caps are closed,
   but Java schema compatibility still needs a one-by-one pass.
3. Persistence semantics. This needs a design choice before code.
4. Notifications and plugin SPI. These are important for full release parity
   but less likely to corrupt query results than the first three.
