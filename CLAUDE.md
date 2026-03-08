# CQELS-RS Project Instructions

## Environment

- Cargo path: `~/.cargo/bin/cargo` (not in default PATH)
- MSRV: Rust 1.75 (see `rust-version` in workspace Cargo.toml)
- Workspace members: cqels-model, cqels-core, cqels-engine, cqels-reasoning, cqels-benchmarks

## Build & Test Commands

- Build: `~/.cargo/bin/cargo build --workspace`
- Test: `~/.cargo/bin/cargo test --workspace`
- Format: `~/.cargo/bin/cargo fmt --all`
- Clippy: `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
- Docs: `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`
- MSRV check: `~/.cargo/bin/cargo check --workspace --exclude cqels-benchmarks --locked`

## Before Every Commit

**Always** run these checks before committing (or use `/pre-commit`):
1. `~/.cargo/bin/cargo fmt --all`
2. `~/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings`
3. `~/.cargo/bin/cargo test --workspace`
4. `RUSTDOCFLAGS="-Dwarnings" ~/.cargo/bin/cargo doc --workspace --no-deps`

Only commit if all 4 pass. Fix any issues first.

## CI Pipeline

CI runs 7 jobs: Check, Format, Clippy, Test, Documentation, MSRV (1.75), Benchmarks compile.
Config: `.github/workflows/ci.yml`

### MSRV Notes
- Benchmarks are excluded from MSRV check (criterion has aggressive MSRV deps)
- Cargo.lock is committed and MSRV uses `--locked` to pin dependency versions
- When adding/updating deps, verify MSRV compatibility: pin transitive deps if needed
  (e.g., `~/.cargo/bin/cargo update <pkg>@<ver> --precise <compatible-ver>`)

## Code Conventions

- Rust edition 2021
- Use `#[non_exhaustive]` on public enums exposed across crate boundaries
- Prefer `try_build()` returning Result over panicking `build()` for builders
- Tests go in `#[cfg(test)] mod tests` adjacent to implementation
- Avoid `3.14` or similar float literals that trigger `clippy::approx_constant`

## Dependency Management

- Cargo.lock is committed to git (not gitignored)
- After adding/updating deps, always check MSRV compatibility
- Known pins for MSRV 1.75: rayon 1.10.0, rayon-core 1.12.1, half 2.4.1, pest* 2.7.15
