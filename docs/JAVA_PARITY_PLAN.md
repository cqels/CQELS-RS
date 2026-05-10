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
- **Not yet ported (follow-up):** Java's per-event FILTER predicates (single-event `where_cond`) and cross-event `where_context` guards. These require wiring an expression evaluator (which exists in `cqels-core::expression`) into the compiler.

## Phase 2 — Windowing maturity

### 2.1 Triggers and evictors

- [x] Trigger types: `EventTimeTrigger`, `ProcessingTimeTrigger`, `CountTrigger`, `PurgingTrigger`.
- [!] Deferred: `DeltaTrigger`, `DeltaEvictor` (need `DeltaFunction` machinery); `ContinuousEventTimeTrigger`, `ContinuousProcessingTimeTrigger`, `ProcessingTimeoutTrigger` (need timer service plumbing).
- [x] Evictor types: `CountEvictor`, `TimeEvictor`.
- [!] Wire into existing `window::Window<T>` operators — follow-up; new traits live in a separate `windowing` module to avoid disturbing existing operators.
- **Verify:** 17 parity tests in [cqels-core/src/windowing.rs](../cqels-core/src/windowing.rs) covering watermark thresholds, processing-time fires, count thresholds with reset, purging wrapper, and time/count evictors.

### 2.2 Windowed self-join with indexed hash join (Java PR #25)

- [ ] Indexed hash-join state for self-join over a single window.
- [ ] Self-join key extraction at compile time.
- **Verify:** port Java's windowed-self-join tests; benchmark shows it beats nested-loop fallback on N>10k.

## Phase 3 — Pick ONE direction (decision pending)

- [ ] **3a Performance:** parallel hash-join + parallel windowed-aggregate operators; SWAG CTrie index.
- [ ] **3b cqels-mcp Rust port:** MCP server exposing engine to LLM agents (8 tools, 4 resources).
- [ ] **3c RocksDB storage backend:** first production persistence backend behind the SPI.
- [ ] **3d cqels-cdsp Rust port:** COVESA vehicle-data integration.

Decision deferred until Phase 1 completes — benchmarks and user demand will drive choice.

## Out of scope (intentional)

- Porting Java's reflection-heavy `BinaryCodecRegistry` generic dispatch — the byte-based Rust SPI is fine for the backends we'll add.
- Re-introducing the separate `cqels-reasoning-rete` crate — folded `cqels-reasoning` is simpler.
- Java's mislabeled `cqels-s2` (which is Lucene semantic search, not S2 cells).

## Done log

- **1.1 MinusOperator** — `cqels-core/src/operator/minus.rs`, 14 parity tests; SPARQL 1.1 §8.3 compatibility, disjoint-domain anti-join semantics. Skipped Java's `Builder` and `Duration` timeout (not needed in Rust idiom).
- **1.2a Implicit stream binding** — `apply_implicit_stream_binding` in `cqels-core/src/parser/cqelsql.rs`. Bare triple patterns auto-bind to the single FROM STREAM when no explicit STREAM block / multiple streams / static or named graphs are present. 6 parser parity tests; updated existing `test_parse_rdf_type_shorthand`. Also exposed `streams()` / `static_graphs()` / `named_graphs()` accessors on the builder.
- **1.3 Declarative CEP via FILTER(SEQ())** — `SeqConstraint`/`SeqArg` AST in `cqels-core::parser::ast`, pest grammar rules (`seq_call`, `seq_arg`, `seq_quantifier`, `seq_not_kw`), parser logic in `parse_seq_call`/`parse_seq_arg`, and `CepPatternCompiler` in `cqels-engine::cep_compiler` that maps SEQ to `Pattern<RdfStreamElement>`. 7 parser tests + 9 compiler tests including 2 end-to-end through `NfaPatternProcessor`. Added `Pattern::previous()` accessor for chain introspection. Not yet ported: single-event/cross-event FILTER predicate evaluation (needs expression evaluator wiring).
- **2.1 Triggers + evictors** — new `cqels-core::windowing` module with `WindowBounds` (`TimeWindow`, `GlobalWindow`), `TriggerResult`, `TriggerContext`, `Trigger` trait, `Evictor` trait, and concrete `EventTimeTrigger`/`ProcessingTimeTrigger`/`CountTrigger`/`PurgingTrigger`/`CountEvictor`/`TimeEvictor`. 17 parity tests. Triggers hold inline state instead of Java's framework-managed partitioned state. Continuous/delta variants and integration with existing `window::Window<T>` operators are tracked as follow-ups.
