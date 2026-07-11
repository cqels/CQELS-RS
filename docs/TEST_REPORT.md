# CQELS-RS Test Report — Java Parity Verification

**Repository:** `HiveIntel/cqels-rs`
**Captured at:** alpha.10 MCP parity stack (2026-07-11)
**Toolchain:** Rust 1.83+ (MSRV), Cargo 1.95.0

This document is a point-in-time snapshot of Rust parity coverage against
`cqels-java`. It does not supersede the remaining alpha.10 gap list in
[`JAVA_ALPHA10_COMPARATIVE_ANALYSIS.md`](./JAVA_ALPHA10_COMPARATIVE_ANALYSIS.md).
For the contributor-facing day-to-day test workflow, see
[`testing.md`](./testing.md).

**Latest delta (2026-07-11):** the default `cqels-mcp` server now wires
Java alpha.10's stream-ingest and continuous-reasoning MCP surface:
`create_stream`, `push_stream_events`, `validate_stream_query`,
`watch_invariant`, `register_rules`, RDF-message parsing, opt-in
`CQELS_MCP_REASONING`, initialize instructions, live namespace/status/docs
resources, and Java-compatible `CQELS_MCP_RDF_STORE_PATH` aliasing.
The stream-ingest path also enforces Java alpha.10's event-time,
DoS-hardening caps, and `facts[*].objectType` contract (`uri` or `literal`,
default `literal`). The Rust parity runner now passes **12/12** checked-in
Java/Rust fixtures locally after closing the `FROM <iri>` static-graph
evaluation gap and correcting the numeric FILTER fixture to use typed integer
literals. `cargo test -p cqels-mcp` now passes **209** unit tests plus the
stdio integration test.

---

## 1. Executive summary

| Metric | Value |
|---|---|
| **Total tests** | **1,884 passing** (default features) + 43 optional-feature tests = **~1,927** |
| **Failures** | **0** |
| **Ignored** | 4 (live-server or opt-in doc tests gated by env vars / explicit examples) |
| **Test groups** | 51 across 15 workspace crates |
| **Latest full-workspace run** | **0 failures** |
| **Local CI gates** | fmt ✅ · clippy ✅ (workspace + features) · test ✅ · doc ✅ |
| **Parity-plan `[!]` deferrals remaining** | **0** |

---

## 2. CI gate matrix

| Gate | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | ✅ clean |
| Clippy (workspace) | `cargo clippy --workspace --all-targets -- -D warnings` | ✅ clean |
| Clippy (kuksa feature) | `cargo clippy -p cqels-cdsp --features kuksa --all-targets -- -D warnings` | ✅ clean |
| Clippy (thrift feature) | `cargo clippy -p cqels-storage-iotdb --features thrift --all-targets -- -D warnings` | ✅ clean |
| Tests | `cargo test --workspace` | ✅ 1884 passed, 4 ignored |
| Tests (kuksa feature) | `cargo test -p cqels-cdsp --features kuksa` | ✅ 27 passed, 1 ignored (`KUKSA_HOST`) |
| Tests (thrift feature) | `cargo test -p cqels-storage-iotdb --features thrift` | ✅ 16 passed, 2 ignored (`IOTDB_HOST`) |
| Docs | `RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps` | ✅ clean |

---

## 3. Per-crate test counts (default features, lib unit tests only)

| Crate | Tests | Role | Java module(s) ported |
|---|---:|---|---|
| `cqels-core` | **748** | Stream operators, parser, compiler, query pipeline | `cqels-core` |
| `cqels-reasoning` | **160** | RETE network, RDFS / OWL profiles, rule sets | `cqels-reasoning` |
| `cqels-engine` | **140** | `ReactiveStreamEngine`, CEP runtime, persistence wiring | `cqels-engine` |
| `cqels-model` | **107** | RDF model, BindingSet, statements, terms | `cqels-model` |
| `cqels-geo` | **67** | GeoSPARQL spatial reasoning | `cqels-geo` |
| `cqels-mcp` | **209** | MCP tool/prompt/resource surface + JSON-RPC stdio / HTTP transports | `cqels-mcp` |
| `cqels-shacl` | **60** | SHACL validation engine, repair candidates | `cqels-shacl` |
| `cqels-asp` | **50** | ASP solver trait, Clingo subprocess solver | `cqels-asp` |
| `cqels-cdsp` | **15** | COVESA VSS signal envelope + mapper + source | `cqels-cdsp` |
| `cqels-storage-iotdb` | **14** | IoTDB-backed persistent storage adapter | `cqels-storage-iotdb` |
| `cqels-benchmarks` | **10** | Workload generators + benchmark harness | (Rust-only) |
| `cqels-storage-sled` | **8** | Sled embedded KV backend (pure-Rust alt.) | (Rust-only fill-in) |
| `cqels-storage-lmdb` | **8** | LMDB embedded backend via `heed` | (Rust-only fill-in) |
| `cqels-storage-rocksdb` | **8** | RocksDB backend (real C++ library) | `cqels-storage-rocksdb` |
| `cqels-storage-spi` | **15** | Trait-only crate (impls live in backend crates) | `cqels-storage-spi` |
| **TOTAL (lib unit)** | **1,619** | | |
| Integration + doc tests across the workspace | **265** | | |
| **GRAND TOTAL** | **1,884** | | |

---

## 4. Java-parity feature coverage

Each row maps a Java upstream feature to the Rust implementation and
the tests that prove the port.

### Phase 1 — Core correctness

| Java feature | Rust impl | Tests proving parity |
|---|---|---|
| `MinusOperator` (SPARQL 1.1 anti-join) | `cqels-core::operator::minus` | `cqels-core::operator::minus::tests::*` (4 tests, AST → operator → execution) |
| Implicit stream binding | `cqels-core::parser::cqelsql` auto-bind | `parser::cqelsql::tests::implicit_binding_*` (5 tests) |
| RSP-QL named window parsing (`FROM NAMED WINDOW :W ON STREAM s [spec]` + `WINDOW :W { … }`) | `parser::cqelsql.pest` + `parser::ast` | `parser::cqelsql::tests::parses_from_named_window_declaration`, `parses_window_pattern_group`, `named_window_does_not_shadow_static_named_graph`, `multiple_named_windows_register_independently`, `named_window_with_triples_spec_parses`, error-path tests (7 tests) |
| Named-window compile-time lowering | `compiler::named_window` | `compiler::named_window::tests::*` (8 lowering tests + 3 end-to-end compile tests) |
| Named-window aliased stream views | `CqelsStreamDefinition::aliased` + `ContinuousQuery::input_stream_aliases` + `ReactiveStreamEngine::build_query_inputs_with_aliases` | `engine::tests::build_query_inputs_with_aliases_*` (2 tests) + `runtime::tests::test_named_window_aliased_view_delivers_events` (end-to-end) |
| Per-stream windowing in `execute` | `compiler::compiled::apply_window_spec` + per-source merge | `compiler::compiled::tests::apply_window_spec_now_*`, `apply_window_spec_triples_*`, `multi_stream_query_applies_per_stream_windowing` (3 tests) |
| Source-tagged batch matching (no cross-stream leakage) | `SourceTaggedBatch` + `patterns_by_source` filter | `compiler::compiled::tests::source_tagged_batches_keep_pattern_matches_per_source` (XOR assertion) |
| CEP `FILTER(SEQ(?e1; ?e2; …))` (Java PR #36) | `cqels-engine::cep_compiler` + `cqels-core::operator` | `parser::cqelsql::tests::seq_*` (5 tests) + `cqels-engine::cep_compiler::tests::*` (CEP NFA, Kleene, negation, alias) |

### Phase 2 — Windowing maturity

| Java feature | Rust impl | Tests |
|---|---|---|
| Tumbling / sliding RANGE windows | `cqels-core::window::TumblingWindow`, `SlidingWindow` | `window::tests::*` (boundary, slide, range coverage) |
| TRIPLES / TRIPLES_SLIDE count windows | `TumblingCountWindow`, `SlidingCountWindow` | `window::tests::count_window_*` |
| Triggerable session/sliding windows | `cqels-core::windowing::triggerable_*` | doc tests + module unit tests |
| Windowed indexed self-join (Java PR #25) | `cqels-core::operator::join::WindowedSelfJoinState` + `compiler::self_join` | `self_join::tests::*` + `operator::join::tests::*` + compiler hint detection + the `try_self_join_fast_path` end-to-end test |

### Phase 3 — Performance, persistence, integration

| Java feature | Rust impl | Tests |
|---|---|---|
| Parallel hash-join (`ParallelHashJoinOperator`) | `cqels-core::operator::parallel_hash_join` | `parallel_hash_join::tests::*` (rayon work-stealing, key distribution) |
| SWAG (F-IVM) — pairwise + N-way | 5 layers + 6 sub-slices in `operator::swag_*` | `swag_*::tests::*` across 7 modules (single biggest test surface in cqels-core) |
| Sled embedded KV backend | `cqels-storage-sled` | 8 tests: append/read, range read, truncate, checkpoint write/latest/delete, provider factory, next-offset recovery |
| LMDB embedded backend | `cqels-storage-lmdb` (uses `heed`) | 8 tests, same shape |
| RocksDB backend (lifts `links = "rocksdb"` oxigraph clash) | `cqels-storage-rocksdb` | 8 tests, same shape |
| IoTDB time-series adapter | `cqels-storage-iotdb` — narrow `IotDbExecutor` trait + in-memory ref impl | 14 tests (executor-level + backend-level) |
| IoTDB **real Thrift client** | `cqels-storage-iotdb::ThriftIotDbExecutor` (feature `thrift`) | 2 unit tests + 1 `IOTDB_HOST`-gated live round-trip |
| MCP tool + prompt + resource surface (Java's `cqels-mcp`) | `cqels-mcp` (`query`, `parse_query`, `analyze_query`, `validate_stream_query`, `reasoning_profiles`, `shacl_capabilities`, `reason`, `store/recall/forget_memory`, `create_stream`, `push_stream_events`, `register_stream_query`/`forget_stream_query`, `watch_invariant`, `register_rules`, `save/list/run_procedure`, `record_event`/`recall_episodes`, `explain_decision`/`recall_decisions`, `register_reasoning`, `set_access_policy`, `assemble_context`, `validate`, `solve`, initialize instructions, 8 CQELS prompt templates; resources `cqels://kg/stats`, `cqels://kg/namespaces`, `cqels://engine/status`, `cqels://streams`, `cqels://queries`, `cqels://reasoning/capabilities`, `cqels://docs/cqelsql`, `cqels://docs/cep`, `cqels://queries/{queryId}/results`; stdio + HTTP JSON-RPC transports; `CQELS_MCP_RDF_STORE_PATH` accepted as an alias root for MCP memory records/procedure definitions, with sled data kept under `<path>/cqels-mcp-memory` and not used for pushed stream events) | 209 `cqels-mcp` unit tests plus stdio integration, covering stateless/memory/reasoning tools, stream ingest/query/observer tools, SHACL `validate`, ASP `solve`, resources, prompts, stdio, and HTTP transport |
| MCP stream ingest/query/observer wiring | `cqels-mcp::stream_query::StreamQueryHub` | `stream_query::tests::*` (custom `queryId` / buffer coverage, RDF-message ingest, graph observation atomicity, Java alpha.10 event-time, DoS caps, strict fact `objectType` default/enum handling, `watch_invariant`, `register_rules`, `CQELS_MCP_REASONING`, bounded ASP delta dedup, reasoned observer batching, plus unregister race regression) |
| MCP `validate` (SHACL bridge) | `cqels-mcp::validate` (any `Arc<dyn AspSolver>`) | 8 tests with recording `MockSolver` |
| MCP `solve` (ASP bridge) | `cqels-mcp::solve` | 8 tests |
| COVESA CDSP / VSS signal ingestion | `cqels-cdsp` | 15 default + 12 with `kuksa` feature = 27 (live test gated by `KUKSA_HOST`) |
| **KUKSA.val gRPC live client** | `cqels-cdsp::kuksa_source::KuksaVssSignalSource` (feature `kuksa`) | 11 unit tests + 1 dead-endpoint + 1 `KUKSA_HOST`-gated live |
| Reasoning profiles (RDFS, OWL-Lite/QL/EL/RL) | `cqels-reasoning::profile` + `cqels-reasoning::rule` | 160 tests (working memory, RETE network, conflict resolution, rule sets) |
| SHACL validation + ASP-encoded repair | `cqels-shacl` | 60 tests (shape parser, validation engine, repair candidates, severity, async solver mock) |
| GeoSPARQL (WKT + R*-tree spatial) | `cqels-geo` | 67 tests |

### Phase 3d — Property-based correctness

These run as part of `cqels-core` integration tests:

| Property | Cases |
|---|---|
| `WindowedSelfJoinState` matches O(N²) nested-loop baseline over random streams | 50–100 |
| `ParallelHashJoinOperator` matches O(N·M) baseline | 50–100 |
| (See `cqels-core/tests/proptest_joins.rs`) | |

---

## 5. Optional-feature test detail

| Feature | Crate | Default tests | Feature-on tests | Ignored (env-gated) |
|---|---|---:|---:|---|
| `kuksa` | `cqels-cdsp` | 15 | **27** | `kuksa_live_subscription_delivers_signals` (`KUKSA_HOST`) |
| `thrift` | `cqels-storage-iotdb` | 14 | **16** | `thrift_live_round_trip` (`IOTDB_HOST`); 1 doctest also `no_run` |

Both real-transport features compile cleanly with `-D warnings` and
pass their full test suites against in-memory mocks; live tests are
opt-in to keep CI deterministic.

---

## 6. Out-of-scope items (intentional non-parity)

These appear in the Java codebase but are deliberately not ported, per
[`JAVA_PARITY_PLAN.md`](./JAVA_PARITY_PLAN.md):

- Java's reflection-heavy `BinaryCodecRegistry` generic dispatch — the
  byte-based Rust SPI is sufficient for the backends we ship.
- Java's `LogicalToRDFConverter` — the Rust port uses a typed
  `Statement` model throughout instead.
- Java alpha.10's JVM deploy-time extension mechanisms
  (`cqels-plugin-spi`, `ServiceLoader` plugin discovery, and
  `ServiceLoader` embedding-provider discovery). Rust extension points are
  native traits/registries rather than jar/classpath loading.

---

## 7. Feature flag matrix

| Build invocation | Build OK | Tests passing |
|---|:---:|:---:|
| `cargo build --workspace` | ✅ | 1,884 passed, 4 ignored |
| `cargo build -p cqels-cdsp --features kuksa` | ✅ | 27 / 27 |
| `cargo build -p cqels-storage-iotdb --features thrift` | ✅ | 16 / 16 |

---

## 8. Determinism evidence

A previously-flaky test
(`stream_query::unregister_removes_query_from_list_and_clears_buffer`)
was stabilized in commit `fcdef33` by pre-registering the stream the
query subscribes to, eliminating a race between the engine's
auto-cleanup-on-empty-stream path and the explicit `unregister` call.
Five consecutive isolated runs after the fix and the latest full-workspace
run on the alpha.10 MCP parity stack produced:

```
Latest full workspace: passed=1884 failed=0 ignored=4
```

No failures were observed in the latest full-workspace run.

---

## 9. Parity verdict

**The Rust port has broad checked parity coverage for the roadmap items in
[`JAVA_PARITY_PLAN.md`](./JAVA_PARITY_PLAN.md), but full Java alpha.10
release parity remains subject to the blockers in
[`JAVA_ALPHA10_COMPARATIVE_ANALYSIS.md`](./JAVA_ALPHA10_COMPARATIVE_ANALYSIS.md).**
Current checked roadmap coverage:

- **Phase 1** (core correctness): ✅ complete
- **Phase 2** (windowing maturity): ✅ complete
- **Phase 3a** (parallel hash-join + SWAG): ✅ complete — all 5
  layers + 6 sub-slices
- **Phase 3b** (persistent storage): ✅ complete — Sled, LMDB,
  RocksDB (`links` clash lifted), IoTDB
- **Phase 3c** (external integration): ✅ complete — MCP tool
  prompt/resource surface with stdio/HTTP transports, `cqels-cdsp` with
  real KUKSA gRPC client
- **Phase 3d** (property-based testing): ✅ complete

Counting the `[!]` (blocked / needs decision) entries in
`JAVA_PARITY_PLAN.md` returns **zero** outside the legend
definition itself. Each previously-blocked item — RocksDB symbol
clash workaround, IoTDB no-driver decision, named-window execution
semantics, multi-stream pattern leakage — has been resolved across
PRs #44–#57.

---

## 10. How to regenerate this report

```bash
# Full default-feature suite:
cargo test --workspace 2>&1 | grep -E "^test result: ok\." \
    | awk 'BEGIN{p=0;i=0;g=0} {p+=$4; i+=$8; g++} \
           END{print "groups:",g," tests:",p," ignored:",i}'

# Optional features:
cargo test -p cqels-cdsp --features kuksa
cargo test -p cqels-storage-iotdb --features thrift

# CI gates:
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p cqels-cdsp --features kuksa --all-targets -- -D warnings
cargo clippy -p cqels-storage-iotdb --features thrift --all-targets -- -D warnings
RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace --no-deps
```
