# CQELS MCP Server

The CQELS MCP server is distributed as a platform archive with each
CQELS-RS release. The current public release is
[`2.0.0-alpha.20`](https://github.com/cqels/CQELS-RS/releases/tag/v2.0.0-alpha.20).

## Install

Download the archive matching your target, verify its adjacent checksum, and
run the extracted `cqels-mcp` executable as a stdio MCP server:

```bash
tar -xzf cqels-mcp-2.0.0-alpha.20-<target>.tar.gz
./cqels-mcp
```

The default transport is stdio. Set `CQELS_MCP_TRANSPORT=http` to expose the
opt-in Streamable HTTP transport; configure host, port, path, authentication,
and readiness settings with the `CQELS_MCP_HTTP_*` environment variables.

The alpha.20 server exposes CQELS-QL stream queries, CEP, reasoning, SHACL,
memory, prompts, resources, durable operator state, and stream lifecycle
operations. Its wire compatibility boundary is documented by the release
notes and the Java parity reports in the private development repository.

The server implementation is not mirrored in this public repository.
