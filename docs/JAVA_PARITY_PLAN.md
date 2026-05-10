# Java Parity Implementation Plan

**Reference:** `github.com/cqels/claude` (Java) @ `ecce069` (2026-05).
**Goal:** Close the algorithmic / language-feature gaps between cqels-rs and cqels-java identified in the audit. Track progress per phase.

## Methodology

Per the project's behavioral guidelines:

- **Goal-Driven:** Each task lists a *verify* check. Move on only when the check passes.
- **Surgical:** Each PR touches one feature; no drive-by refactors of unrelated code.
- **Simplicity:** Port the algorithm, not the framework. Skip Java-isms (reflection, complex builder ceremonies) when a leaner Rust equivalent suffices.
- **Pre-commit MANDATORY** (from project CLAUDE.md):
  1. `~/.cargo/bin/cargo fmt --all`
  2. `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
  3. `~/.cargo/bin/cargo test --workspace`
  4. `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`

## Status legend

- [ ] not started
- [~] in progress
- [x] done
- [!] blocked / needs decision

## Phase 1 — Core correctness gaps

### 1.1 MinusOperator (SPARQL 1.1 anti-join)

- [x] Port `MinusOperator` from `cqels-java/cqels-core/src/main/java/org/cqels/operator/minus/`.
- **Semantics:** For `Ω1 MINUS Ω2`, keep μ1 iff there is no μ2 compatible with μ1 and `dom(μ1) ∩ dom(μ2) ≠ ∅`.
- **Verify:** 14 parity tests in [cqels-core/src/operator/minus.rs](../cqels-core/src/operator/minus.rs) mirroring Java's `MinusOperatorTest`; pre-commit suite green.

### 1.2 Named windows + implicit stream binding (Java PR #31)

**Status note:** Java has parser + AST for `FROM NAMED WINDOW` but execution is unimplemented — `CqelsQueryCompiler` throws `UnsupportedOperationException`, tracked in Java issue #39 (commit `3afb720`). Porting Java's incomplete parser is busywork until Java itself executes named-window queries. **Deferring Phase 1.2b until upstream completes Phase 3.**

- [x] **1.2a Implicit stream binding** (Java Phase 1, complete and working): bare triple patterns in `WHERE` auto-bind to a single `FROM STREAM` when no `STREAM`/`WINDOW` blocks are present.
  - **Verify:** [cqels-core/src/parser/cqelsql.rs](../cqels-core/src/parser/cqelsql.rs) — `apply_implicit_stream_binding`; 6 new parser parity tests covering single-stream binding, multi-triple binding, explicit-block bypass, multi-stream skip, static-graph skip, named-graph skip.
- [!] **1.2b Named window parsing** (`FROM NAMED WINDOW :name ON stream [spec]` + `WINDOW :name { ... }`): defer until Java upstream implements execution.

### 1.3 Declarative CEP via FILTER(SEQ()) (Java PR #36)

- [x] Add `SEQ(...)` built-in recognized inside `FILTER` (grammar + AST).
- [x] Compile `FILTER(SEQ(...))` AST nodes to existing `cqels-engine` NFA pipeline.
- **Verify:** 7 parser parity tests + 9 `CepPatternCompiler` tests covering ordering, quantifiers, negation, alias, and window propagation; end-to-end tests run the compiled `Pattern` through `NfaPatternProcessor` over a stream of `RdfStreamElement`s.
- [x] **Single-event + cross-event FILTER predicates** wired into the compiler via `cqels-core::expression::ExpressionEvaluator`. `classify_filters` walks WHERE-level `Filter` groups, parses each expression, classifies by the set of event variables it references, and attaches single-event filters as `where_cond` on that event's stage or cross-event filters as `where_context` on the latest referenced stage. `bind_event_variables` constructs a `BindingSet` from the matched stream element using the event's triple patterns. 4 new e2e tests (single-event keep/drop + cross-event keep/drop) and 1 unit test for variable collection.

## Phase 2 — Windowing maturity

### 2.1 Triggers and evictors

- [x] Trigger types: `EventTimeTrigger`, `ProcessingTimeTrigger`, `CountTrigger`, `PurgingTrigger`, `DeltaTrigger`, `ProcessingTimeoutTrigger`, `ContinuousEventTimeTrigger`, `ContinuousProcessingTimeTrigger` (8/8 ported).
- [x] Evictor types: `CountEvictor`, `TimeEvictor`, `DeltaEvictor` (3/3 ported).
- [x] Bridge integration via `TriggerableWindowProcessor` in `cqels-core::windowing::processor` — buffers elements and consults a trigger/evictor per-window. Existing `window::Window<T>` operators continue to work unchanged.
- **Verify:** 33 parity tests across `cqels-core/src/windowing.rs`, `windowing/processor.rs`, `windowing/delta.rs`, `windowing/timeout.rs`, `windowing/continuous.rs`.

### 2.2 Windowed self-join with indexed hash join (Java PR #25)

- [x] Indexed hash-join state for self-join over a single window (`WindowedSelfJoinState<T, K>` in `cqels-core::operator::join`).
- [x] Compile-time detection of self-join AST patterns (`detect_self_joins` in `cqels-core::compiler::self_join`). Reports source, shared variables, and pattern indices as `SelfJoinHint`s.
- [!] Auto-substitution inside `CqelsQueryCompiler::compile` — deferred; the detection function is exposed for downstream rewriters.
- **Verify:** 6 operator tests + 7 detection tests.

## Phase 3 — Performance + persistence

### 3a Parallel hash-join (Java PR `ParallelHashJoinOperator`)

- [x] `ParallelHashJoinOperator<L, R, K, Out>` in `cqels-core::operator::parallel_hash_join`. Sequential build phase materializes the left side into `HashMap<K, Vec<L>>`; concurrent probe phase via `futures::stream::buffer_unordered`.
- **Verify:** 6 tests — basic join, empty inputs, one-to-many, parallelism invariance over {1, 2, 4, 8}, equivalence vs O(N·M) baseline over 50×30 elements.
- [!] **Deferred:** parallel windowed-aggregate operator and SWAG CTrie index.

### 3b Persistent storage backend

- [x] `cqels-storage-sled` — first production-grade backend implementing all SPI traits (`PersistentBackend`, `EventJournal`, `CheckpointStore`, `StorageBackendProvider`) against the pure-Rust `sled` embedded KV store.
- **Verify:** 8 tests — append/read round-trip, read-from offset filter, truncate-before, checkpoint write/latest, latest-by-id ordering, delete-older-than, provider creation, next-offset recovery across reopen.
- [!] **Why sled, not RocksDB?** The Rust `rocksdb` crate and oxigraph's transitively-included `oxrocksdb-sys` both declare `links = "rocksdb"`, so Cargo refuses to compile both in the same workspace. Sled has equivalent semantics for our use case, no native link conflict, and faster compile times. A future RocksDB backend can slot in once `oxrocksdb-sys` is gated behind an opt-in feature.
- [!] **Deferred:** dedicated LMDB and IoTDB backends.

### 3c External integration modules (not started)

- [ ] **`cqels-mcp` Rust port:** MCP server exposing engine to LLM agents (8 tools, 4 resources).
- [ ] **`cqels-cdsp` Rust port:** COVESA vehicle-data integration.

## Out of scope (intentional)

- Porting Java's reflection-heavy `BinaryCodecRegistry` generic dispatch — the byte-based Rust SPI is fine for the backends we'll add.
- Re-introducing the separate `cqels-reasoning-rete` crate — folded `cqels-reasoning` is simpler.
- Java's mislabeled `cqels-s2` (which is Lucene semantic search, not S2 cells).

## Done log

- **1.1 MinusOperator** — `cqels-core/src/operator/minus.rs`, 14 parity tests; SPARQL 1.1 §8.3 compatibility, disjoint-domain anti-join semantics. Skipped Java's `Builder` and `Duration` timeout (not needed in Rust idiom).
- **1.2a Implicit stream binding** — `apply_implicit_stream_binding` in `cqels-core/src/parser/cqelsql.rs`. Bare triple patterns auto-bind to the single FROM STREAM when no explicit STREAM block / multiple streams / static or named graphs are present. 6 parser parity tests; updated existing `test_parse_rdf_type_shorthand`. Also exposed `streams()` / `static_graphs()` / `named_graphs()` accessors on the builder.
- **1.3 Declarative CEP via FILTER(SEQ())** — `SeqConstraint`/`SeqArg` AST in `cqels-core::parser::ast`, pest grammar rules (`seq_call`, `seq_arg`, `seq_quantifier`, `seq_not_kw`), parser logic in `parse_seq_call`/`parse_seq_arg`, and `CepPatternCompiler` in `cqels-engine::cep_compiler` that maps SEQ to `Pattern<RdfStreamElement>`. 7 parser tests + 9 compiler tests including 2 end-to-end through `NfaPatternProcessor`. Added `Pattern::previous()` accessor for chain introspection. Not yet ported: single-event/cross-event FILTER predicate evaluation (needs expression evaluator wiring).
- **2.1 Triggers + evictors** — new `cqels-core::windowing` module with `WindowBounds` (`TimeWindow`, `GlobalWindow`), `TriggerResult`, `TriggerContext`, `Trigger` trait, `Evictor` trait, and concrete `EventTimeTrigger`/`ProcessingTimeTrigger`/`CountTrigger`/`PurgingTrigger`/`CountEvictor`/`TimeEvictor`. 17 parity tests. Triggers hold inline state instead of Java's framework-managed partitioned state. Continuous/delta variants and integration with existing `window::Window<T>` operators are tracked as follow-ups.
- **2.2 Windowed self-join with indexed hash** — `WindowedSelfJoinState<T, K>` in `cqels-core::operator::join` with `SelfJoinPair<T>` result type. Hash index keyed by caller-supplied join key, time-ordered per-key buckets, watermark-driven eviction. 6 parity tests including an O(N²) baseline equivalence check. Compile-time detection (rewriting self-join AST patterns to use this operator) is deferred — the operator is exposed for direct construction.
- **1.3 follow-up: SEQ FILTER predicates wired** — `classify_filters` + `bind_event_variables` + `term_to_value` in `cqels-engine::cep_compiler` route single-event filters into per-state `where_cond` and cross-event filters into `where_context` at the latest referenced state. Uses the existing `cqels-core::expression::ExpressionEvaluator`. 5 new tests bring the cep_compiler suite to 14.
- **2.2 follow-up: compile-time self-join detection** — new `cqels-core::compiler::self_join` module with `SelfJoinHint` and `detect_self_joins(query_def)`. Inspects WHERE-level Stream pattern groups, reports pairs sharing source + at least one variable. 7 tests.
- **2.1 follow-up: TriggerableWindowProcessor** — new `cqels-core::windowing::processor` bridges trigger/evictor framework to a stateful per-window stream processor. 5 tests.
- **2.1 follow-up: Delta + Timeout triggers/evictors** — `DeltaFunction`, `DeltaTrigger`, `DeltaEvictor` (`windowing::delta`); `ProcessingTimeoutTrigger` (`windowing::timeout`). 11 tests.
- **2.1 follow-up: Continuous trigger variants** — `ContinuousEventTimeTrigger`, `ContinuousProcessingTimeTrigger` (`windowing::continuous`). 7 tests. Trigger family now 8/8 ported.
- **3a Parallel hash-join** — `ParallelHashJoinOperator<L, R, K, Out>` in `cqels-core::operator::parallel_hash_join`. Sequential build → concurrent probe via `buffer_unordered`. 6 tests including a baseline equivalence over 50×30 elements.
- **3b cqels-storage-sled** — first production-grade backend. Implements `PersistentBackend`/`EventJournal`/`CheckpointStore`/`StorageBackendProvider`. Sled trees keyed by 8-byte big-endian offsets/IDs, JSON-encoded payloads, recoverable next-offset counter. 8 tests including reopen-and-resume.
