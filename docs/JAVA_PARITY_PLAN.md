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

- [ ] **1.2a Implicit stream binding** (Java Phase 1, complete and working): bare triple patterns in `WHERE` auto-bind to a single `FROM STREAM` when no `STREAM`/`WINDOW` blocks are present.
  - **Verify:** parser tests show implicit binding produces same AST as explicit `STREAM { ... }`; existing tests stay green.
- [!] **1.2b Named window parsing** (`FROM NAMED WINDOW :name ON stream [spec]` + `WINDOW :name { ... }`): defer until Java upstream implements execution.

### 1.3 Declarative CEP via FILTER(SEQ()) (Java PR #36)

- [ ] Add `SEQ(...)` built-in recognized inside `FILTER`.
- [ ] Compile `FILTER(SEQ(...))` AST nodes to existing `cqels-engine` NFA pipeline.
- **Verify:** port Java's `DeclarativeCepTest`; identical event matches.

## Phase 2 — Windowing maturity

### 2.1 Triggers and evictors

- [ ] Trigger types: `EventTimeTrigger`, `ProcessingTimeTrigger`, `ContinuousEventTimeTrigger`, `ContinuousProcessingTimeTrigger`, `CountTrigger`, `DeltaTrigger`, `PurgingTrigger`, `ProcessingTimeoutTrigger`.
- [ ] Evictor types: `CountEvictor`, `TimeEvictor`, `DeltaEvictor`.
- [ ] Wire into existing `WindowType` so legacy windows keep working.
- **Verify:** parity tests over event-time vs processing-time scenarios; deterministic ordering tests.

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
