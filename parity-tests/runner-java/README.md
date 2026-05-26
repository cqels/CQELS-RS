# Java parity runner

Java counterpart of [`runner-rust/`](../runner-rust/). Consumes the
same fixture format (`metadata.toml` + `query.cqels` + `streams.jsonl`
+ `expected.jsonl`) and drives each workload through a live
**cqels-java** engine — the engine in
[`github.com/cqels/claude`](https://github.com/cqels/claude), which
the Rust port has been tracking from the start — so we can verify
the Rust port produces the same bindings as the Java reference
implementation.

The Maven module is intentionally isolated from the Rust cargo
workspace — `cargo test --workspace` at the repo root does not pull
in JVM tooling, and `mvn package` here does not pull in cargo.

## Prerequisites

cqels-java is **not** on Maven Central, and `cqels/claude` is a
private GitHub repository. The default coordinates in
[`pom.xml`](./pom.xml) target
`org.cqels:cqels-engine:2.0.0-SNAPSHOT`, so a local install of
cqels/claude is required first:

```bash
git clone https://github.com/cqels/claude  # requires repo access
cd claude
mvn install -DskipTests
```

That puts `org.cqels:cqels-engine:2.0.0-SNAPSHOT` (and its sibling
modules) into `~/.m2/repository/`. JDK 17 is required — cqels/claude
sets `maven.compiler.{source,target}=17`.

If your team maintains a fork with different coordinates, override
them on the Maven command line:

```bash
mvn package \
  -Dcqels.dependency.groupId=com.example.cqels \
  -Dcqels.dependency.artifactId=cqels-engine \
  -Dcqels.dependency.version=2.1.0
```

## Build

```bash
cd parity-tests/runner-java
mvn -q -DskipTests package
```

Two jars land in `target/`:

- `cqels-parity-runner-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar` — the runner CLI.
- `parity-bench.jar` — JMH benchmark uber-jar.

## Run

Single fixture:

```bash
java -jar target/cqels-parity-runner-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
     ../fixtures/simple-select-now
```

Every fixture under `parity-tests/fixtures/`:

```bash
java -jar target/cqels-parity-runner-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
     --all ../fixtures
```

Exit codes match the Rust runner exactly so CI can treat both
interchangeably:

| Code | Meaning                                                |
|-----:|--------------------------------------------------------|
| 0    | All bindings match expected, in order.                 |
| 1    | Mismatch — runner prints a diff and exits non-zero.    |
| 2    | Workload loaded but the engine returned an error.      |
| 3    | Workload failed to load (malformed JSON, missing files). |

## JMH benchmarks

`parity-bench.jar` discovers fixtures and runs every workload
end-to-end (engine spin-up → register query → push events → drain
bindings → stop). Numbers are average milliseconds per fixture and
are intended for side-by-side comparison with the Rust criterion
benchmarks under `cqels-benchmarks::parity_fixtures`.

```bash
# Run every fixture under ../fixtures
java -jar target/parity-bench.jar

# Limit to specific fixtures
java -jar target/parity-bench.jar -p fixtures=simple-select-now,range-window-low-volume

# Point at a different fixtures root
java -jar target/parity-bench.jar -p fixturesRoot=/abs/path/parity-tests/fixtures

# JSON output for post-processing (e.g. plotting against criterion)
java -jar target/parity-bench.jar -rf json -rff bench.json
```

## Integration-verification checklist (for cqels-java forks)

The adapter surface lives in two files:

1. **[`pom.xml`](./pom.xml)** — coordinates of the cqels-engine jar.
   Overridable via `-Dcqels.dependency.{groupId,artifactId,version}`.
2. **[`EngineDriver.java`](./src/main/java/cqels/parity/runner/EngineDriver.java)**
   — assumes the cqels/claude public façade:
   - `CQELSEngine.builder().withMemoryStore().build()` (AutoCloseable)
   - `engine.createStream(String name) → DataStream`
   - `engine.registerCqelsQuery(String query, QueryResultListener<Map<String, Object>>)`
   - `engine.start()` / `engine.stop()` / `engine.close()`
   - `DataStream.push(Statement statement, long timestampMs)` (event-time-aware)
   - `DataStream.complete()` to signal end-of-stream
   - RDF4J `org.eclipse.rdf4j.model.{Statement, Value, ValueFactory}`
     for term construction and result stringification

   Forks that renamed packages, changed the listener interface, or
   moved to a different RDF API (e.g. back to Jena) need the imports
   + method names in this file updated. The class doc lists the exact
   methods to point at.

Stream identifiers in cqels/claude are **bare names** (`FROM STREAM
Requests`, not `FROM STREAM <iri>`), matching the Rust runner's
convention; the runner passes the fixture's `stream` field through
verbatim with no rewriting.

## CI

A nightly job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
(`nightly-parity-java`) installs cqels/claude into a fresh Maven cache,
builds this module, and runs the matched fixtures as a regression gate
plus the full `--all` sweep as an informational artifact.

The job needs a GitHub repository secret named **`CQELS_CLAUDE_PAT`** —
a personal-access token with `repo:read` on `cqels/claude` — to clone
the private engine. If the secret is missing, the job exits 0 early
with a workflow-summary notice; this keeps PR runs green for forks
and contributors who don't have access.

The set of fixtures enforced by the regression gate lives in the
`parity_java_known_passing` env list at the top of the job. When you
reconcile a delta (flip a fixture to `java-golden`, or fix the
underlying engine bug), add its name to that list so CI starts
catching regressions on it.

## What's not (yet) covered

- **CEP sequence enforcement.** The `cep-sequence-two-events` fixture
  documents that SEQ isn't enforced via the standard
  `registerCqelsQuery` path — neither cqels-java nor cqels-rs treats
  SEQ as a join constraint on that code path; expected output mirrors
  that semantics gap.
- **Java-golden capture.** Today the harness drives cqels-java to
  *verify* the fixtures against the hand-spec expected output;
  capturing cqels-java's actual output as `expected.jsonl` (and
  setting `ground_truth = "java-golden"`) is the planned next step
  once a maintainer with a stable cqels/claude build runs the sweep.
