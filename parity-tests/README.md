# Parity test harness

A language-neutral corpus of CQELS workloads that both `cqels-rs` and
`cqels-java` can run, so we can verify the Rust port produces the same
results as the Java reference implementation.

## Why outside the cargo workspace?

The fixtures themselves are pure data files — they're meant to be
consumed by runners written in any language. The Rust runner
([`runner-rust/`](./runner-rust/)) is a standalone Cargo project with
its own `[workspace]` boundary, so `cargo test --workspace` at the
repo root does **not** automatically execute parity tests (they tend
to be slower and have heavier setup). Anyone implementing a Java
runner under `runner-java/` can mirror the same isolation.

## Workload layout

Each workload lives in its own directory under
[`fixtures/`](./fixtures/) with the following files:

```
fixtures/<workload-name>/
├── metadata.toml      # workload description + ground-truth source
├── query.cqels        # the CqelsQL query (1 query per workload)
├── streams.jsonl      # input events, one JSON object per line
└── expected.jsonl     # expected output bindings, one per line
```

### `metadata.toml`

```toml
name = "simple-select-now"
description = "Basic SELECT over a NOW window."

# How the expected outputs were obtained. One of:
#   "hand-spec"      — written by inspection of the query semantics
#   "rust-captured"  — captured from cqels-rs via `cargo xtask parity capture --engine rust`
#   "java-captured"  — captured from cqels-java via `cargo xtask parity capture --engine java`
#   "java-golden"    — historical hand-imported cqels-java reference
ground_truth = "hand-spec"

# When `ground_truth = "java-golden"`, also include:
# java_commit  = "abcdef0..."
# java_captured_at = "2026-05-17"
```

### `query.cqels`

A single CqelsQL query in plain text. Comments allowed. Trailing
whitespace ignored. Example:

```sparql
SELECT ?sensor ?temp
FROM STREAM sensors [NOW]
WHERE {
  STREAM sensors { ?sensor <http://ex.org/temp> ?temp . }
}
```

### `streams.jsonl`

Input events fed into the engine, one JSON object per line. Each
event represents one RDF triple with a timestamp:

```json
{"stream": "sensors", "ts": 1000, "s": "http://ex.org/sensor1", "p": "http://ex.org/temp", "o": "21"}
```

Fields:

- `stream` — the name of the stream the event flows on. Must match
  the `FROM STREAM <name>` declaration in the query.
- `ts` — event timestamp in milliseconds since epoch (integer).
- `s` / `p` / `o` — subject / predicate / object. IRIs are passed
  through verbatim if they look like one (`http://…`, `urn:…`,
  `<…>`); other values become RDF literals.

Events are pushed in line order. The runner controls the engine
lifecycle so timestamps reflect the in-stream order rather than
wall-clock arrival.

### `expected.jsonl`

Expected output bindings, one JSON object per line, in the order the
engine emits them. Each binding is a map of `variable_name → value`,
plus an optional `_ts` for the binding's timestamp:

```json
{"sensor": "http://ex.org/sensor1", "temp": "21", "_ts": 1000}
```

Variable names are written **without** the SPARQL `?` / `$` prefix.
The runner strips both at compare time.

If `_ts` is omitted in an expected line, the timestamp on the
produced binding is **not** checked — only the variable bindings
need to match.

## One command for the whole sweep

```bash
cargo xtask parity              # both runners, side-by-side pass/fail table
cargo xtask parity --rust-only  # only the Rust runner
cargo xtask parity --java-only  # only the Java runner
```

## Verifying parity on a new query — without writing an oracle

The sweep above compares each runner's output against a hand-written
`expected.jsonl`. For a new query, writing that file is the hard part.
Two helpers shortcut the workflow:

```bash
# "Do the engines agree on this fixture's query+events?"
# Runs both engines, diffs their captured bindings against each other,
# ignores expected.jsonl entirely. Exit 0 = agreement, exit nonzero +
# unified diff = divergence.
cargo xtask parity diff parity-tests/fixtures/<name>

# "Capture engine X's output as the fixture's expected.jsonl."
# Useful after `parity diff` shows agreement and you want to pin the
# snapshot for future regression-gating. Flips ground_truth in
# metadata.toml to `<engine>-captured`.
cargo xtask parity capture --engine rust parity-tests/fixtures/<name>
cargo xtask parity capture --engine java parity-tests/fixtures/<name>
```

The intended workflow for a new query:

1. Create `parity-tests/fixtures/<name>/` with `metadata.toml`,
   `query.cqels`, `streams.jsonl` (no `expected.jsonl` needed yet).
2. `cargo xtask parity diff parity-tests/fixtures/<name>` — see if
   the engines agree.
3. If they agree: `cargo xtask parity capture --engine rust …` to
   freeze a `rust-captured` golden, then `cargo xtask parity` to
   confirm both runners pass against the new fixture.
4. If they disagree: investigate the gap; either rewrite the query,
   capture one engine's output as the ground-truth with a description
   of why that engine is the oracle, or document the divergence and
   leave it in the known-failing set.

`cargo xtask parity` discovers every fixture under `fixtures/`, builds
both runners (release mode for Rust, `mvn package` for Java), runs
each fixture through both, and prints a compact table at the end:

```
Parity sweep — cqels-rs vs cqels-java

  Fixture                    | Rust | Java
  -------------------------- | ---- | ----
  cep-sequence-two-events    | ok   | FAIL
  range-window-low-volume    | ok   | ok
  ...
  -------------------------- | ---- | ----
  TOTAL                      | 7/7  | 4/7
```

Per-runner output is captured into `target/xtask/parity/parity-rust.log`
and `target/xtask/parity/parity-java.log` for drilling into failures.
The Java column reports `skip` if `mvn` or cqels/claude aren't
installed locally — useful for forks that don't have access to the
private cqels/claude repo.

## Running the Rust runner

```bash
cd parity-tests/runner-rust
cargo run --release -- ../fixtures/simple-select-now
```

Exit codes:

| Code | Meaning |
|-----:|---------|
| 0    | All bindings match expected, in order. |
| 1    | Mismatch — runner prints a unified diff and exits non-zero. |
| 2    | Workload loaded but the engine returned an error. |
| 3    | Workload failed to load (malformed JSON, missing files, …). |

Use `--all` to run every fixture in `fixtures/`:

```bash
cargo run --release -- --all ../fixtures
```

## Adding a workload

1. Pick a short kebab-case name. Create
   `fixtures/<name>/metadata.toml`, `query.cqels`, `streams.jsonl`,
   `expected.jsonl` per the spec above.
2. For hand-spec workloads, set `ground_truth = "hand-spec"` and
   write the expected output yourself.
3. For Java-derived golden, capture the cqels-java output, save as
   `expected.jsonl`, set `ground_truth = "java-golden"` plus the
   commit + capture date.
4. Run the Rust runner against your fixture — it should pass on
   commit `c4b9d12` or later. If it doesn't, file a bug under
   `cqels-rs` referencing the workload name.

## Java runner

A Maven module under [`runner-java/`](./runner-java/) drives the same
fixtures through a live **cqels-java** engine — specifically the engine
in [`cqels/claude`](https://github.com/cqels/claude), the same upstream
this Rust port has been tracking from the start. Exit codes match the
Rust runner so CI can treat both interchangeably. cqels/claude is a
private repo + not on Maven Central — clone it (with repo access) and
`mvn install -DskipTests` first, then:

```bash
cd parity-tests/runner-java
mvn -q -DskipTests package
java -jar target/cqels-parity-runner-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
     --all ../fixtures
```

Internal cqels-java forks with different Maven coordinates can override
them on the command line (`-Dcqels.dependency.groupId=…`,
`-Dcqels.dependency.artifactId=…`, `-Dcqels.dependency.version=…`).
See [`runner-java/README.md`](./runner-java/README.md) for the full
integration-verification checklist (package names, listener APIs,
stream identifier scheme).

A JMH benchmark uber-jar (`target/parity-bench.jar`) drives the same
fixtures for side-by-side comparison with the Rust criterion bench
under `cqels-benchmarks::parity_fixtures`.
