# CQELS-RS Project Instructions

## Environment

- Cargo path: `~/.cargo/bin/cargo` (not in default PATH)
- MSRV: Rust 1.85 (see `rust-version` in workspace Cargo.toml)
- Workspace members: cqels-model, cqels-core, cqels-engine, cqels-reasoning, cqels-geo, cqels-benchmarks, xtask
- Repo: HiveIntel/cqels-rs on GitHub

## Build & Test Commands

- Build: `~/.cargo/bin/cargo build --workspace`
- Test: `~/.cargo/bin/cargo test --workspace`
- Format: `~/.cargo/bin/cargo fmt --all`
- Clippy: `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
- Docs: `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`

## Workflow Rules

### Before every commit — MANDATORY
Run ALL of these in order. Fix any failures before committing. Do NOT skip any step.
1. `~/.cargo/bin/cargo fmt --all`
2. `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
3. `~/.cargo/bin/cargo test --workspace`
4. `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`

### After pushing — check CI without blocking
- Use `gh run list --repo HiveIntel/cqels-rs --limit 1` to get the run ID
- Use `gh run view <id> --repo HiveIntel/cqels-rs` to check status (do NOT use `gh run watch`)
- NEVER use `gh run watch` — it blocks for minutes and wastes context window
- If CI still running, tell the user and move on. Check again only when asked.

### Autonomy
- Fix lint/format/test errors without asking — just fix and re-run
- When the user says "commit" or "push", do the full pre-commit checks first automatically
- When the user reports a CI failure, diagnose and fix it end-to-end, then push
- Minimize round-trips: batch all fixes into one commit when possible

## CI Pipeline

CI runs 10 jobs split into PR-required and nightly/scheduled tiers.
Config: `.github/workflows/ci.yml`

### PR-required (push/PR events)
- Format, Clippy, Documentation
- PR Impact Suite (`cargo xtask test impact --base origin/main`)
- Fast Workspace Tests (`cargo nextest run --workspace --lib --bins --tests`)

### Nightly/scheduled (cron + workflow_dispatch)
- Full Regression Sweep (`cargo xtask test full`)
- Coverage (`cargo xtask coverage`, artifacts uploaded)
- MSRV (1.85) check
- Benchmarks Compile + Benchmarks Observe (artifacts uploaded)

### xtask
- `cargo xtask test pr` — full local PR check (fmt + clippy + doc + tests + impact)
- `cargo xtask test impact --base origin/main` — impact-based regression suite
- `cargo xtask test full` — all workspace tests
- `cargo xtask coverage` — code coverage with cargo-llvm-cov
- `cargo xtask bench-observe` — benchmark observation run
- `cargo xtask parity` — Rust + Java parity sweep across all fixtures, prints a side-by-side pass/fail table (`--rust-only` / `--java-only` to limit; Java side skips cleanly if `mvn` or cqels/claude aren't installed locally)
- `cargo xtask parity diff <fixture-dir>` — runs both engines on the fixture's query+events and compares their captured bindings directly, bypassing `expected.jsonl`. Lets you answer "do the engines agree on this query?" without writing a hand-spec oracle.
- `cargo xtask parity capture --engine <rust|java> <fixture-dir>` — runs the chosen engine and overwrites the fixture's `expected.jsonl` with its captured output; flips `ground_truth` in `metadata.toml` to `<engine>-captured`. Useful for pinning a snapshot you trust.

### MSRV Notes
- Benchmarks excluded from MSRV check (criterion deps have high MSRV)
- Cargo.lock committed and MSRV uses `--locked` to pin dependency versions
- MSRV is 1.85, raised from 1.83 because `cqels-storage-rocksdb`'s
  `librocksdb-sys` (via `rocksdb` 0.24) requires rustc 1.85

## Code Conventions

- Rust edition 2021
- `#[non_exhaustive]` on public enums exposed across crate boundaries
- Prefer `try_build()` returning Result over panicking `build()` for builders
- Tests in `#[cfg(test)] mod tests` adjacent to implementation
- Avoid `3.14` or similar float literals that trigger `clippy::approx_constant`
- Cargo.lock is committed to git (not gitignored)

## Behavioral Guidelines

Source: https://github.com/forrestchang/andrej-karpathy-skills (Karpathy-inspired). These bias toward caution over speed; for trivial tasks, use judgment. They override default behavior where they conflict.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.
