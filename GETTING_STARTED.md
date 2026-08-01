# Getting Started with CQELS-RS

This guide takes you from zero to a running Rust query and the standalone MCP
server using the public alpha.16 distribution.

> **Current release:** `2.0.0-alpha.16` -- keep all CQELS crates on this version.

## Prerequisites

| Tool | Minimum |
|------|---------|
| Rust | 1.85+ |
| Cargo | shipped with Rust |
| Git | 2.30+ |

```bash
rustc --version
cargo --version
```

## Rust libraries

Add the crates you need to your `Cargo.toml`. Most applications start with
`cqels-model` and `cqels-core`; add `cqels-engine` for runtime and CEP, or
`cqels-reasoning` for incremental inference.

```toml
[dependencies]
cqels-model = "2.0.0-alpha.16"
cqels-core = "2.0.0-alpha.16"
cqels-engine = "2.0.0-alpha.16"
cqels-reasoning = "2.0.0-alpha.16"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

Use the crate documentation for the public Rust API:

- [cqels-engine on docs.rs](https://docs.rs/cqels-engine)
- [cqels-core on docs.rs](https://docs.rs/cqels-core)
- [cqels-reasoning on docs.rs](https://docs.rs/cqels-reasoning)

All CQELS crates should use the same fixed release version. The public
distribution does not serve version ranges or `LATEST` metadata.

## MCP server

1. Open the [CQELS-RS releases](https://github.com/cqels/CQELS-RS/releases).
2. Download the archive matching your operating system and CPU target.
3. Verify the adjacent `.sha256` file as described in [SUPPLY_CHAIN.md](SUPPLY_CHAIN.md).
4. Extract `cqels-mcp` and run it as a stdio MCP server.

The release notes identify the compatible CQELS-RS and MCP protocol version.

The alpha.16 server also supports opt-in Streamable HTTP through the
`CQELS_MCP_TRANSPORT=http` environment setting. See the release compatibility
guide in the private development repository for the full deployment surface.

## Examples

The public examples consume released crates and do not include engine source:

```bash
cargo run --manifest-path examples/Cargo.toml
```

## Source and development

The public repository is intentionally artifact-only. The implementation source
and engineering workflow are maintained privately in
[HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs).

The public repository is intentionally not a source checkout and should not be
used as the development workspace.
