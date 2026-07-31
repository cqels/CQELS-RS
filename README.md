# CQELS-RS

CQELS-RS is the Rust distribution of the CQELS continuous query engine.

This repository is the public artifact proxy. It intentionally does not contain
the engine workspace or implementation source. Development, source review,
design records, agent configuration, issues, and pull requests are maintained
in the private [HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs)
repository.

## Distribution

Use the published Rust crates for library applications. Select the version
listed by the release you are targeting, then add `cqels-engine` and any
supporting packages from crates.io.

The related packages include:

- `cqels-model` -- RDF terms, statements, values, and bindings
- `cqels-core` -- query parsing, windows, operators, and execution primitives
- `cqels-engine` -- runtime, stream lifecycle, and CEP
- `cqels-reasoning` -- RETE inference profiles
- `cqels-mcp` -- MCP server binary and protocol surface

For the standalone MCP server, download the platform archive from the
[GitHub Releases](https://github.com/cqels/CQELS-RS/releases) page. Each
archive includes a SHA-256 checksum.

See [GETTING_STARTED.md](GETTING_STARTED.md) for installation and release
selection guidance.

## Public contents

- [CQELS-QL specification](CQELS-QL_SPEC.md)
- [Runnable Rust examples](examples/)
- [MCP server distribution guide](mcp-server/README.md)
- [Release verification](SUPPLY_CHAIN.md)

## License

MIT
