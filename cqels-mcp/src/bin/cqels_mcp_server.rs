//! Entry-point binary for the `cqels-mcp` stdio server.
//!
//! Run with `cargo run -p cqels-mcp --bin cqels-mcp-server` and pipe
//! line-delimited JSON-RPC requests on stdin. Responses are written to
//! stdout, one per line.
//!
//! Registered tools:
//!
//! - Stateless: `parse_query`, `query`, `analyze_query`,
//!   `reasoning_profiles`, `shacl_capabilities`, `reason`
//!   (one-shot RETE inference).
//! - Memory tools: `store_memory`, `recall_memory`, `forget_memory`,
//!   backed by a [`SledMemoryStore`] when the `CQELS_MCP_MEMORY_DIR`
//!   environment variable is set, otherwise an [`InMemoryMemoryStore`].

use std::io::{self, BufReader};
use std::sync::Arc;

use cqels_mcp::{
    analyze_query_tool, forget_memory_tool, parse_query_tool, query_tool, reason_tool,
    reasoning_profiles_tool, recall_memory_tool, run_stdio, shacl_capabilities_tool,
    store_memory_tool, InMemoryMemoryStore, MemoryStore, SledMemoryStore, ToolRegistry,
};

fn main() -> io::Result<()> {
    let memory: Arc<dyn MemoryStore> = match std::env::var("CQELS_MCP_MEMORY_DIR") {
        Ok(path) if !path.is_empty() => match SledMemoryStore::open(&path) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                eprintln!(
                    "cqels-mcp: failed to open sled memory store at {path}: {e}; \
                     falling back to in-memory"
                );
                Arc::new(InMemoryMemoryStore::new())
            }
        },
        _ => Arc::new(InMemoryMemoryStore::new()),
    };

    let mut registry = ToolRegistry::new();
    registry.install(parse_query_tool());
    registry.install(query_tool());
    registry.install(analyze_query_tool());
    registry.install(reasoning_profiles_tool());
    registry.install(shacl_capabilities_tool());
    registry.install(reason_tool());
    registry.install(store_memory_tool(memory.clone()));
    registry.install(recall_memory_tool(memory.clone()));
    registry.install(forget_memory_tool(memory));

    let stdin = io::stdin();
    let stdout = io::stdout();
    let reader = BufReader::new(stdin.lock());
    let writer = stdout.lock();

    run_stdio(&registry, reader, writer)
}
