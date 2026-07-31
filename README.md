# CQELS-RS

CQELS-RS is the Rust distribution of the CQELS continuous query engine.

This repository is the public artifact proxy. It intentionally does not contain
the engine workspace or implementation source. Development, source review,
design records, agent configuration, issues, and pull requests are maintained
in the private [HiveIntel/cqels-rs](https://github.com/HiveIntel/cqels-rs)
repository.

## Distribution

Use the published Rust crates for library applications:

```bash
cargo add cqels-engine@2.0.0-alpha.14
cargo add cqels-reasoning@2.0.0-alpha.14
```

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

## License

MIT
