//! Entry-point binary for the `cqels-mcp` stdio server.
//!
//! Run with `cargo run -p cqels-mcp --bin cqels-mcp-server` and pipe
//! line-delimited JSON-RPC requests on stdin. Responses are written to
//! stdout, one per line. The full tool surface (`parse_query`, `query`,
//! `reasoning_profiles`, `shacl_capabilities`) is registered by default.

use std::io::{self, BufReader};

use cqels_mcp::{
    parse_query_tool, query_tool, reasoning_profiles_tool, run_stdio, shacl_capabilities_tool,
    ToolRegistry,
};

fn main() -> io::Result<()> {
    let mut registry = ToolRegistry::new();
    registry.install(parse_query_tool());
    registry.install(query_tool());
    registry.install(reasoning_profiles_tool());
    registry.install(shacl_capabilities_tool());

    let stdin = io::stdin();
    let stdout = io::stdout();
    let reader = BufReader::new(stdin.lock());
    let writer = stdout.lock();

    run_stdio(&registry, reader, writer)
}
