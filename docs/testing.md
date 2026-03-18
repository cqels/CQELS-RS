# Testing

CQELS-RS uses a layered regression strategy so contributors can get fast
feedback locally while CI still catches broader cross-crate regressions.

## Supported entrypoints

The repo-standard workflow is driven by `xtask`:

```bash
cargo xtask test pr
cargo xtask test impact --base origin/main
cargo xtask test full
cargo xtask coverage
cargo xtask bench-observe
```

## Layered suites

### Crate-local unit and property tests

- `cqels-model`: model unit tests + `proptest_model`
- `cqels-core`: parser/window/operator/compiler tests + proptests
- `cqels-engine`: engine/runtime/CEP lifecycle tests
- `cqels-reasoning`: RETE and stream adapter tests
- `cqels-geo`: GeoSPARQL function/index/reasoning tests

### Cross-crate regression binaries

The end-to-end coverage in `cqels-benchmarks/tests` is split into named suites:

- `query_language_regressions`
- `runtime_lifecycle_regressions`
- `reasoning_regressions`
- `geo_regressions`
- `window_aggregation_regressions`

These binaries are what `cargo xtask test impact` targets when a subsystem is
changed.

## Contributor workflow

Before opening a PR:

```bash
cargo xtask test impact --base origin/main
```

If the change touches parser/compiler/runtime/reasoning/geo behavior, also run
the matching regression binary directly. If the change is broad or the impact is
unclear, run:

```bash
cargo xtask test full
```

Before closing a bug issue:

- add an issue-numbered regression test in the relevant suite
- run the smallest matching suite locally
- make sure the issue repro is captured by automation

## PR expectations

Every new feature or fix should add tests at:

- the lowest layer that proves the behavior
- the highest layer where the behavior is externally visible

Minimum expectations:

- parser/compiler changes: unit parser test + end-to-end query regression
- runtime/engine changes: lifecycle test + runtime regression if user-visible
- reasoning/geo semantics changes: focused semantic unit tests + runtime/query
  regression if behavior crosses crate boundaries
- serialization/public API changes: contract regression and doc/example
  validation when the docs expose the behavior

## CI policy

### PR-required

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo doc --workspace --no-deps`
- `cargo xtask test impact --base origin/main`
- fast crate-local `nextest` run (`cargo test` fallback inside `xtask`)

### Scheduled/full

- `cargo xtask test full`
- `cargo xtask coverage`
- MSRV check
- `cargo bench --no-run`
- `cargo xtask bench-observe`

## Tooling

Recommended local installs:

```bash
cargo install cargo-nextest --locked
cargo install cargo-llvm-cov --locked
```

`xtask` falls back to plain `cargo test` when `cargo-nextest` is unavailable.
Coverage requires `cargo-llvm-cov`.

## Performance policy

Benchmarks are observation-only for now:

- scheduled runs collect artifacts and summaries
- PRs are not blocked on benchmark regressions yet
- hard thresholds can be added later once baselines are stable
