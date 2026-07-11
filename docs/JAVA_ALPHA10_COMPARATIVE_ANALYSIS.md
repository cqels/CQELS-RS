# CQELS Java alpha.10 vs Rust Port Comparative Analysis

Date: 2026-07-11

Java reference: `cqels/claude` tag `v2.0.0-alpha.10`, peeled commit
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
parity: persistence, notification, plugin, broader cross-language fixture
contracts, and some exact input-contract details are not fully implemented or
proved yet.

## Evidence Summary

Java alpha.10:

- Release notes describe continuous reasoning, persistent RDF quad storage, and
  deploy-time plugin SPI in `CHANGELOG.md`.
- `GraphStreamElement` carries a list of RDF statements as one timestamped
  stream element; Java's CQELS-QL compiled path expands the graph only during
  matching so `[TRIPLES n]` counts observations.
- `StreamIngestToolHandler` enforces governance, event-time bounds, stream caps,
  message/statement/character caps, reserved-graph rejection, parse-before-push,
  and single-vs-multi-statement push semantics.
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
  over-limit observation counts, per-event character floods, and total-call
  character floods before any push.
- `cqels-mcp/src/tools.rs` has an `AccessPolicyRegistry`, and the default
  server now wires it into memory recall, context assembly, `query`, governed
  stream tools, continuous observer registration, result polling, and live
  resources.
- `cqels-mcp/src/resource.rs` now withholds governed metadata/result resources
  under active policy and preserves result buffers instead of draining them.
- `cqels-mcp/src/bin/cqels_mcp_server.rs` treats `CQELS_MCP_RDF_STORE_PATH` as
  a sled-backed MCP memory alias under `<path>/cqels-mcp-memory`, not as a
  persistent RDF quad store.
- Rust extension points are native registries and traits; there is no
  Java-style deploy-time plugin SPI or provider discovery layer.
- Fresh fixture sweep on this branch:
  - `cargo xtask parity --rust-only`: Rust passed 12/12 fixtures.
  - `cargo xtask parity --java-only`: Java passed 6/12 fixtures against the
    current checked-in oracles; the remaining Java failures/errors are either
    documented semantic/oracle differences or Java parser hard-errors for
    STREAM-scoped OPTIONAL/UNION.

## Capability Matrix

| Area | Java alpha.10 behavior | Rust current behavior | Parity |
|---|---|---|---|
| MCP tool/resource names | Alpha.10 advertises stream ingest, stream query, continuous reasoning, memory, reasoning, procedure, episodic, decision, governance, resources, prompts, stdio/HTTP. | Rust exposes the main names plus several Rust extras; this analysis did not produce a formal one-by-one inventory diff. | Broad surface overlap; inventory diff still needed before final parity sign-off. |
| `create_stream` | Idempotent, bounded by `MAX_STREAMS`, denied under active governance. | Idempotent, denied under active governance in the default server, and bounded by the Java alpha.10 stream cap. | Close for the MCP tool. |
| `push_stream_events` | Parse/validate all events before push, cap events/messages/statements/chars/streams, reject fractional/out-of-range event times, reject `cqels://` graph contexts, deny under governance, push one statement as RDF and multi-statement observations as graph elements. | Parses before push, denies under active governance in the default server, rejects empty event arrays, enforces Java-style event-time range/whole-number validation, stream cap, per-message cap, total-observation cap, total-statement cap, N-Quads/per-event character cap, and total-call character cap; reserved contexts are rejected, empty RDF messages are skipped, nonblank `nquads` takes precedence over `facts`, and single-vs-multi observations use the Java RDF/graph split. Rust still accepts some extra `objectType` aliases such as `iri`/blank that Java rejects. | Close for tracked alpha.10 hardening; exact schema strictness inventory still needed. |
| Atomic multi-statement observations | `GraphStreamElement` keeps one observation in one window slot; CQELS-QL expands statements for matching. Java documents this as supported on the CQELS-QL compiled stream/windowed path, not on every legacy/Cypher/CEP path. | Rust now has the same CQELS-QL/MCP support boundary: graph observations count as one element for windows, expand for CQELS-QL matching and the self-join fast path, round-trip through the stream codec, and are exposed via `DataStream::push_graph`. | Close for CQELS-QL/MCP graph-observation semantics; cross-language fixture evidence still needed. |
| `[TRIPLES n]` observation semantics | Counts stream elements, so graph observations count as one and never split. | Rust count windows now count a graph observation as one element, and MCP multi-statement observations are delivered as graph elements. | Close for the tested CQELS-QL path. |
| `register_stream_query` | Governed fail-closed; buffers results, supports Java language/CEP surface according to Java handler. Existing listeners drop results while governance is active. | Buffers and polls results, denies registration under active governance in the default server, and listener callbacks drop results under active policy; still only accepts `cqelsql`, while `cep:true` fails loud. | Partial. |
| `poll_stream_results` / result resource | Java drains via `recall_memory(queryId)` and resource template, denied/withheld under governance. | `poll_stream_results` denies under active governance; `cqels://queries/{queryId}/results` returns a denial payload without draining. | Close for governed withholding; remaining parity depends on broader Java result-resource semantics. |
| `watch_invariant` | Continuous SHACL per observation, governed fail-closed, registration caps, shape size cap, optional notifications, drops results under governance. | Present, denied under active governance in the default server, and drops observer results while governance is active; no Java-equivalent registration/shape caps; notifications accepted but not pushed. | Partial. |
| `register_rules` | Continuous ASP accumulate/solve, governed fail-closed, rules/args/facts/buffer caps, optional notifications, result drop under governance. | Present with `maxFacts`, `emit`, buffers, solver injection, default-server governance denial, and result drop under active policy; notifications accepted but not pushed; caps are not fully aligned. | Partial. |
| `CQELS_MCP_REASONING` | Opt-in `rdfs` or `rdfs-full` stream reasoning; original observations flow first, graph observations stay atomic, inferred single triples are appended, and a RETE fact cap hardens memory. | Opt-in parser and RDFS/RDFS-full flow are present; original observations flow first, graph observations stay atomic, and inferred triples are appended as single RDF observations. `apply_stream_reasoning` still does not wire a Java-equivalent RETE fact cap. | Partial. |
| MCP resources | Metadata resources are withheld under governance, except liveness fields in `engine/status`; `engine/status` reports persistence and RDF-store persistence facts. | Governed resource registry withholds metadata/result resources and keeps `engine/status` liveness/features readable; status still lacks Java's persistence/rdfStore shape. | Partial. |
| `CQELS_MCP_RDF_STORE_PATH` | Switches the RDF repository to RDF4J `NativeStore`; stored facts and saved procedures survive restart. | Accepted as an alias root for sled MCP memory; stream events and RDF quads are not persisted as Java NativeStore equivalents. | Not equivalent. |
| Plugin SPI | New `cqels-plugin-spi`; `ServiceLoader` discovers plugins and embedding providers; plugin tools are namespaced, atomic, and governed. | Native `ToolRegistry`, `MemoryStore`, solver injection, and resource registries exist, but no deploy-time plugin discovery or governed plugin registrar. | Intentional non-parity today; blocker if the goal is full Java alpha.10 parity. |
| Notifications | Java uses `McpNotifier` for query/resource result notifications when requested. | Rust accepts `notify` and has notification helper payloads, but tool schemas state unsolicited push notifications are not wired. | Partial. |
| Ingest event-time validation | Whole-number epoch millis or ISO instant, bounded to `[0, 7258118400000]`, overflow-safe. | Rust now rejects fractional, negative, and far-future event times, validates numeric strings and UTC instants through the same upper bound, and uses checked timestamp arithmetic. | Close for the MCP ingest path. |
| Fixture parity | Java/Rust fixture harness exists; the CI Java-runner gate enforces a named known-passing subset and runs the full corpus informationally. A fresh local Java run passed 6/12 against the current oracles. | Rust now passes the full checked-in 12-fixture corpus locally. The corpus covers NOW, TRIPLES windows, FILTER, STREAM-scoped OPTIONAL/UNION, ORDER BY/LIMIT, static `FROM <iri>` lookup, aggregate, CEP SEQ documentation, and known emission-semantics differences. | Strong evidence for covered fixtures only, not universal equality. |
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
discovery, notifications, and exact MCP input-contract strictness.

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
4. Choose the notification parity target. If Rust claims Java `notify:true`
   parity, wire unsolicited result/resource notifications; otherwise keep it
   documented as accepted-but-not-implemented compatibility.
5. Choose the plugin parity target. Full Java alpha.10 parity requires a
   deploy-time Rust plugin SPI/discovery layer with namespacing, collision
   checks, atomic registration, and governance wrapping; otherwise this remains
   a deliberate non-parity item.
6. Finish the exact MCP input-contract inventory, including whether Rust should
   reject Java-invalid convenience aliases such as `objectType: "iri"` on
   `push_stream_events`.

## Recommended Next Implementation Order

1. Alpha.9/alpha.10 MCP cross-language fixtures. The Rust query corpus is now
   green locally; the next evidence gap is MCP graph/governance/result behavior
   rather than more Rust-only fixture cleanup.
2. Exact MCP input-contract inventory. The tracked ingest hardening is closed,
   but strict Java schema compatibility still needs a one-by-one pass.
3. Persistence semantics. This needs a design choice before code.
4. Notifications and plugin SPI. These are important for full release parity
   but less likely to corrupt query results than the first three.
