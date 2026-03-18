# CQELS-RS Project Instructions

## Environment

- Cargo path: `~/.cargo/bin/cargo` (not in default PATH)
- MSRV: Rust 1.83 (see `rust-version` in workspace Cargo.toml)
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
- MSRV (1.83) check
- Benchmarks Compile + Benchmarks Observe (artifacts uploaded)

### xtask
- `cargo xtask test pr` — full local PR check (fmt + clippy + doc + tests + impact)
- `cargo xtask test impact --base origin/main` — impact-based regression suite
- `cargo xtask test full` — all workspace tests
- `cargo xtask coverage` — code coverage with cargo-llvm-cov
- `cargo xtask bench-observe` — benchmark observation run

### MSRV Notes
- Benchmarks excluded from MSRV check (criterion deps have high MSRV)
- Cargo.lock committed and MSRV uses `--locked` to pin dependency versions
- No dependency pins needed at MSRV 1.83

## Code Conventions

- Rust edition 2021
- `#[non_exhaustive]` on public enums exposed across crate boundaries
- Prefer `try_build()` returning Result over panicking `build()` for builders
- Tests in `#[cfg(test)] mod tests` adjacent to implementation
- Avoid `3.14` or similar float literals that trigger `clippy::approx_constant`
- Cargo.lock is committed to git (not gitignored)
