# CQELS fleet examples

Current release: **2.0.0-alpha.21**. These MCP scenarios use the electric-vehicle
fleet world from [CQELS4J examples](https://github.com/cqels/CQELS4J/tree/master/examples):
SOSA observations, VSS Speed, `https://example.org/fleet/` identifiers, EV-7Q2,
and the north depot. Deterministic timestamps and readings replace Java's random
or wall-clock inputs so the expected output is reproducible.

## Run with the released Rust server

From the repository root:

```bash
python3 mcp-server/install.py
python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp --report results.json
```

Windows: use `python` and `.cqels/bin/cqels-mcp.exe`. Python 3.9+ is the only
runtime dependency of the driver. It launches the real MCP server, checks its
version and discovery descriptors, uses a new temporary session per scenario,
asserts the expected output, and exits nonzero if behavior drifts.

| Scenario flag | Java demonstration | Expected Rust result |
| --- | --- | --- |
| `--scenario low-battery` | HelloCqels | Two low-battery alerts (18.5 and 12) |
| `--scenario aggregation` | WindowedAggregation | Three running rows: averages 60, 70, 60 |
| `--scenario static-join` | StreamStaticJoin | EV-7Q2 at depot/north |
| `--scenario cep` | ComplexEventPattern | One drop/spike match (1000–2000 ms) |
| `--scenario cep-reversed` | ComplexEventPattern negative control | No match |
| `--scenario rdfs` | RdfsReasoning | EV-7Q2 has inferred type Vehicle |

`all` is the default. All six scenarios assert the expected behavior, including
three aggregate rows and one static lookup row. The reversed CEP input remains
a negative control. These examples verify bounded cases rather than every
possible aggregate, join, or reasoning program.

The exact queries live in [fleet/](fleet/), expected results in
[expectations.json](fleet/expectations.json), and timestamped RDF inputs in
[mcp_fleet.py](mcp_fleet.py). The driver checks all rows, including duplicates.
It normalizes only the declared numeric columns, since Rust serializes those
values as strings while Java uses JSON numbers.

## Compare Java

Download and verify the Java shaded jar using [RELEASE.json](../RELEASE.json)
and install JDK 17+. Then run:

```bash
python3 examples/mcp_fleet.py --java-jar cqels-mcp-2.0.0-alpha.20-shaded.jar --report java.json
```

See [COMPATIBILITY.md](../COMPATIBILITY.md) for the complete measured results and
limits. Neither matching schemas nor this small suite establishes complete
behavioral parity.

## Rust API illustrations

`src/` and `Cargo.toml` are retained for API reference only. Their alpha.21 CQELS
crates are not available on crates.io, so these files are **not currently runnable
from a clean public checkout**. Do not use them as installation instructions.
The MCP examples above consume only public release artifacts. A Cargo lockfile
will be provided only when those public crate dependencies can actually resolve.

## Test the driver controls

These tests use synthetic responses and files; they need no server or account:

```bash
python3 -m unittest discover -s examples -p 'test_*.py'
python3 -m unittest discover -s mcp-server -p 'test_*.py'
```

The live reports also record each accepted-observation acknowledgement, the
static seed readback, and the CEP events encoding. These controls distinguish
accepted input from successful query execution.
