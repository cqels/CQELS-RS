# Getting Started

## Rust libraries

Install the runtime from crates.io and select the same version for the CQELS
packages used by your application:

```bash
cargo add cqels-engine@2.0.0-alpha.14
cargo add cqels-reasoning@2.0.0-alpha.14
```

Use the crate documentation for the public Rust API:

- [cqels-engine on docs.rs](https://docs.rs/cqels-engine)
- [cqels-core on docs.rs](https://docs.rs/cqels-core)
- [cqels-reasoning on docs.rs](https://docs.rs/cqels-reasoning)

## MCP server

1. Open the [CQELS-RS releases](https://github.com/cqels/CQELS-RS/releases).
2. Download the archive matching your operating system and CPU target.
3. Verify the adjacent `.sha256` file.
4. Extract `cqels-mcp` and run it as a stdio MCP server.

The release notes identify the compatible CQELS-RS and MCP protocol version.

## Source and development

The public repository is intentionally artifact-only. The implementation source
and engineering workflow are maintained privately in
[HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs).
