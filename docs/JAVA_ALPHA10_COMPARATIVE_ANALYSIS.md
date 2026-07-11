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

The remaining gap is semantic parity. Java alpha.10 adds specific fail-closed,
atomic-observation, persistence, notification, plugin, and ingest-hardening
behaviors that Rust does not fully implement yet.

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

- `cqels-core/src/stream.rs` has `StreamElement::Rdf` and
  `StreamElement::Record`, but no graph element variant.
- `cqels-core/src/compiler/compiled.rs` only expands `StreamElement::Rdf` in
  the CQELS-QL matcher and self-join fast path.
- `cqels-mcp/src/stream_query.rs` parses RDF-message observations, but pushes
  every statement into the engine as an individual `RdfStreamElement`.
- `cqels-mcp/src/tools.rs` has an `AccessPolicyRegistry`, but the default server
  only wires it into `recall_memory` and `assemble_context`.
- `cqels-mcp/src/resource.rs` drains query-result resources and returns
  metadata without an access-policy check.
- `cqels-mcp/src/bin/cqels_mcp_server.rs` treats `CQELS_MCP_RDF_STORE_PATH` as
  a sled-backed MCP memory alias under `<path>/cqels-mcp-memory`, not as a
  persistent RDF quad store.
- Rust extension points are native registries and traits; there is no
  Java-style deploy-time plugin SPI or provider discovery layer.

## Capability Matrix

| Area | Java alpha.10 behavior | Rust current behavior | Parity |
|---|---|---|---|
| MCP tool/resource names | Alpha.10 advertises stream ingest, stream query, continuous reasoning, memory, reasoning, procedure, episodic, decision, governance, resources, prompts, stdio/HTTP. | Rust exposes the main names plus several Rust extras; this analysis did not produce a formal one-by-one inventory diff. | Broad surface overlap; inventory diff still needed before final parity sign-off. |
| `create_stream` | Idempotent, bounded by `MAX_STREAMS`, denied under active governance. | Idempotent, no stream cap, no governance gate. | Partial. |
| `push_stream_events` | Parse/validate all events before push, cap events/messages/statements/chars/streams, reject fractional/out-of-range event times, reject `cqels://` graph contexts, deny under governance, push one statement as RDF and multi-statement observations as graph elements. | Parses before push, caps event count, total statements, N-Quads body size, rejects reserved contexts, but accepts rounded floats/out-of-range times, lacks per-message/total-observation/total-character/stream caps, lacks governance, and pushes multi-statement observations as separate RDF elements. | Partial; not alpha.10 equivalent. |
| Atomic multi-statement observations | `GraphStreamElement` keeps one observation in one window slot; CQELS-QL expands statements for matching. | No graph element; engine windows see N statement elements for an N-triple observation. MCP observers see the batch whole, but CQELS-QL count windows do not. | Blocker. |
| `[TRIPLES n]` observation semantics | Counts stream elements, so graph observations count as one and never split. | Counts per-statement elements for multi-statement observations pushed by MCP. | Blocker. |
| `register_stream_query` | Governed fail-closed; buffers results, supports Java language/CEP surface according to Java handler. Existing listeners drop results while governance is active. | Buffers and polls results, but only accepts `cqelsql`; `cep:true` fails loud; no governance gate; listeners keep buffering under active policy. | Partial. |
| `poll_stream_results` / result resource | Java drains via `recall_memory(queryId)` and resource template, denied/withheld under governance. | `poll_stream_results` and `cqels://queries/{queryId}/results` drain buffers without governance. | Partial. |
| `watch_invariant` | Continuous SHACL per observation, governed fail-closed, registration caps, shape size cap, optional notifications, drops results under governance. | Present; validates the MCP observation batch whole; no governance gate; no Java-equivalent registration/shape caps; notifications accepted but not pushed. | Partial. |
| `register_rules` | Continuous ASP accumulate/solve, governed fail-closed, rules/args/facts/buffer caps, optional notifications, result drop under governance. | Present with `maxFacts`, `emit`, buffers, and solver injection; no governance gate; notifications accepted but not pushed; caps are not fully aligned. | Partial. |
| `CQELS_MCP_REASONING` | Opt-in `rdfs` or `rdfs-full` stream reasoning; inferred triples flow to standing queries; RETE fact cap hardens memory. | Opt-in parser and RDFS/RDFS-full flow are present; inferred statements are batched for MCP observers. The MCP `register_rules` path has `maxFacts`, but `apply_stream_reasoning` does not wire a Java-equivalent fact cap for the opt-in stream RETE path. | Partial. |
| MCP resources | Metadata resources are withheld under governance, except liveness fields in `engine/status`; `engine/status` reports persistence and RDF-store persistence facts. | Resources expose query ids, streams, counts, capabilities, and result buffers without governance; status lacks Java's persistence/rdfStore shape. | Partial. |
| `CQELS_MCP_RDF_STORE_PATH` | Switches the RDF repository to RDF4J `NativeStore`; stored facts and saved procedures survive restart. | Accepted as an alias root for sled MCP memory; stream events and RDF quads are not persisted as Java NativeStore equivalents. | Not equivalent. |
| Plugin SPI | New `cqels-plugin-spi`; `ServiceLoader` discovers plugins and embedding providers; plugin tools are namespaced, atomic, and governed. | Native `ToolRegistry`, `MemoryStore`, solver injection, and resource registries exist, but no deploy-time plugin discovery or governed plugin registrar. | Intentional non-parity today; blocker if the goal is full Java alpha.10 parity. |
| Notifications | Java uses `McpNotifier` for query/resource result notifications when requested. | Rust accepts `notify` and has notification helper payloads, but tool schemas state unsolicited push notifications are not wired. | Partial. |
| Ingest event-time validation | Whole-number epoch millis or ISO instant, bounded to `[0, 7258118400000]`, overflow-safe. | Accepts i64, clamps u64 to i64 max, rounds finite f64, accepts negative/out-of-range i64. | Blocker for alpha.10 hardening. |
| Fixture parity | Java/Rust fixture harness exists; the CI Java-runner gate enforces a named known-passing subset and runs the full corpus informationally. | Rust has fixture-by-fixture validation for selected workloads, plus gap-tracking fixtures for parser, static graph, CEP, aggregate ordering, and emission-semantics differences. | Good evidence for covered fixtures only, not universal equality. |
| Broader core-query parity | Java alpha.10 includes fixes or behavior for some fixtures outside the alpha.10 MCP delta, including static lookup joins and Java-side ORDER BY/LIMIT handling. | Current fixture metadata tracks Rust gaps: `FILTER(SEQ())` parsed but not enforced through the standard CQELS-QL path, `OPTIONAL` and `UNION` inside `STREAM` rejected by the parser, `ORDER BY DESC(?var)` rejected, `FROM <iri>` static graph parsed but not applied at evaluation, and aggregate row order can be nondeterministic. | Full Java alpha.10 parity must include these, not just MCP alpha.10. |

## Meaning of Fixture-by-Fixture, Not Universal Equality

The parity harness proves the fixtures it runs. It does not prove that every
CQELS feature, every MCP tool, and every security edge case behaves identically.

That distinction matters here for two reasons.

First, the current known fixture gate is subset-based. The corpus includes
workloads such as `optional-extra-property`, `union-two-properties`,
`cep-sequence-two-events`, `stream-static-lookup-join`, `count-aggregate`, and
`order-by-limit` that document either Rust limitations, Java/Rust semantic
differences, or comparison-mode limitations. Those are valuable regression
artifacts, but they are not a universal equivalence proof.

Second, the alpha.9/alpha.10 deltas include surfaces that the query fixtures do
not exercise: governance fail-closed behavior, graph-element atomicity through
MCP ingest, persistent RDF quad storage, plugin discovery, notifications, and
ingest DoS/event-time hardening.

So the accurate statement is: Rust has validated parity for the specific
fixtures and unit tests it carries, and it has much of the alpha.10 MCP surface.
It cannot yet be declared universally identical to Java alpha.10.

## Blockers to Claiming Full Alpha.10 Parity

1. Add a Rust `GraphStreamElement` equivalent and update CQELS-QL execution so
   graph observations count as one window element but expand to statements for
   CQELS matching.
2. Change MCP stream ingest to push single-statement observations as RDF
   elements and multi-statement observations as graph elements.
3. Wire `AccessPolicyRegistry` into `query`, `register_stream_query`,
   stream-query result drains, stream tools, continuous reasoning tools, and
   metadata resources with Java's fail-closed posture.
4. Harden `push_stream_events` to match Java alpha.10 bounds and event-time
   validation.
5. Reconcile the broader core-query fixture gaps that still separate the Rust
   port from Java alpha.10 behavior or from Java/Rust deterministic comparison:
   standard-path SEQ execution, STREAM-scoped OPTIONAL/UNION parsing, ORDER BY
   DESC parsing, SPARQL `FROM <iri>` static graph evaluation, and deterministic
   aggregate row ordering.
6. Decide and implement the persistence story for `CQELS_MCP_RDF_STORE_PATH`:
   true persistent RDF quad store parity, or an explicit documented non-parity
   accepted by the project.
7. Choose the notification parity target. If Rust claims Java `notify:true`
   parity, wire unsolicited result/resource notifications; otherwise keep it
   documented as accepted-but-not-implemented compatibility.
8. Choose the plugin parity target. Full Java alpha.10 parity requires a
   deploy-time Rust plugin SPI/discovery layer with namespacing, collision
   checks, atomic registration, and governance wrapping; otherwise this remains
   a deliberate non-parity item.
9. Expand cross-language fixtures to cover alpha.9/alpha.10 MCP semantics,
   including graph observations, governed denials, event-time rejection, result
   withholding, and persistence restart behavior.

## Recommended Next Implementation Order

1. Governance fail-closed wiring. This is security-sensitive and localized to
   MCP tools/resources.
2. Graph observation semantics. This is the core stream correctness gap for
   RDF Messages and `[TRIPLES n]`.
3. Ingest hardening. This closes alpha.10's network-facing DoS and overflow
   posture.
4. Core-query fixture reconciliation. This keeps the alpha.10 claim from being
   MCP-only.
5. Persistence semantics. This needs a design choice before code.
6. Notifications and plugin SPI. These are important for full release parity
   but less likely to corrupt query results than the first three.
