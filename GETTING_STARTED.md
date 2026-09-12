# Getting Started with CQELS 2.0 in Rust

Current release: **2.0.0-alpha.21**. Start with the prebuilt MCP executable.

## 1. Prerequisites

Use Python 3.9+ for the installer and fleet examples, and Git to clone this
repository. No Rust toolchain or Java runtime is needed to run the Rust binary.

| Platform | Target | Archive |
| --- | --- | --- |
| macOS, Apple Silicon | aarch64-apple-darwin | tar.gz |
| macOS, Intel | x86_64-apple-darwin | tar.gz |
| Linux, x86-64, glibc | x86_64-unknown-linux-gnu | tar.gz |
| Windows, x86-64 | x86_64-pc-windows-msvc | zip |

There is no Linux ARM or musl archive in this release. The Linux archive is
validated on Ubuntu 24.04 by the separately maintained release pipeline; compatibility with older glibc is not established.
The installer rejects unsupported platforms rather than selecting another CPU.

## 2. Download and verify

From this repository's root:

```bash
python3 mcp-server/install.py
```

Expected: `Verified and installed: .cqels/bin/cqels-mcp` (Windows: `cqels-mcp.exe`).
The installer downloads the archive and its checksum anonymously, verifies both
against `RELEASE.json`, and extracts only the executable. It replaces an existing
executable at that destination after successful verification. Stop a running
Windows server before replacing its executable.

For manual installation, select the exact archive and adjacent `.sha256` file
from the [release page](https://github.com/cqels/CQELS-RS/releases/tag/v2.0.0-alpha.21).
Verify them using [SUPPLY_CHAIN.md](SUPPLY_CHAIN.md) before extracting.

## 3. Run the fleet demos

```bash
python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp --scenario low-battery
python3 examples/mcp_fleet.py --server .cqels/bin/cqels-mcp --report results.json
```

The first command prints one line containing two low-battery alert rows. The second runs six probes:
low battery, aggregation, static lookup, CEP, reversed CEP, and RDFS inference.
It reports two `KNOWN DIFFERENCE` results, explained in
[COMPATIBILITY.md](COMPATIBILITY.md). A mismatch or protocol error exits nonzero.
Each scenario uses a fresh process and temporary state, leaving existing server
data untouched. CQELS environment variables are cleared for reproducible demos.

On Windows replace `python3` with `python` and use `.cqels/bin/cqels-mcp.exe`.

## 4. Connect an MCP client

Run the extracted executable as a stdio server, or use the HTTP setup in the
[MCP guide](mcp-server/README.md). On stdio, silence until the client sends
`initialize` is expected: stdout is exclusively JSON-RPC. Logs go to stderr.
The server is not an interactive command prompt.

## 5. Your first query, explained

[low-battery.rq](examples/fleet/low-battery.rq) selects each observation and its
battery percentage, with `[NOW]` and `FILTER(?soc < 20)`. The runnable driver:

1. Initializes MCP and creates `Telemetry`.
2. Registers the query before pushing any data; streams have no replay.
3. Sends five observations with explicit millisecond timestamps and typed numeric
   N-Quads literals. Ordinary `facts` string literals are not typed doubles.
4. Drains matches with `recall_memory(queryId)` and asserts exactly two alerts.
5. Removes the query and closes the server process.

## 6. Known limitations

See the [release-specific compatibility table](COMPATIBILITY.md). Most notably,
matching MCP descriptors do not imply identical query behavior. The documented
aggregate and static lookup probes register successfully but return no Rust rows.
The probes verify push acknowledgements and read back the static seed. Treat
registration and ingestion as separate checks from query execution. The native Rust example source files require unavailable crates and
are retained as API illustrations only.

## 7. Where to go next

- [Fleet scenarios and expected output](examples/README.md)
- [MCP configuration and schemas](mcp-server/README.md)
- [CQELS-QL reference and executable query files](CQELS-QL_SPEC.md)
- [Checksum verification](SUPPLY_CHAIN.md)
