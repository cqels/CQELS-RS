//! Entry-point binary for the `cqels-mcp` stdio server.
//!
//! Run with `cargo run -p cqels-mcp --bin cqels_mcp_server` and pipe
//! line-delimited JSON-RPC requests on stdin. Responses are written to
//! stdout, one per line.
//!
//! Registered surface:
//!
//! - Stateless: `parse_query`, `query`, `analyze_query`,
//!   `reasoning_profiles`, `shacl_capabilities`, `reason`
//!   (one-shot RETE inference), `validate` (SHACL validation), and
//!   `solve` (direct ASP solve).
//! - Memory tools: `store_memory`, `recall_memory`, `forget_memory`,
//!   and `register_reasoning`, backed by a [`SledMemoryStore`] when
//!   the `CQELS_MCP_MEMORY_DIR` environment variable is set, otherwise
//!   an [`InMemoryMemoryStore`].
//! - Prompt templates: Java-compatible CQELS workflow/query prompts
//!   exposed through `prompts/list` and `prompts/get`.
//! - Resources:
//! - `cqels://kg/stats`
//! - `cqels://streams`
//! - `cqels://queries`
//! - `cqels://reasoning/capabilities`
//! - `cqels://queries/{queryId}/results` template

use std::io::{self, BufReader};
use std::sync::Arc;

use cqels_mcp::{
    analyze_query_tool, cqels_prompt_registry, cqels_resource_registry, forget_memory_tool,
    parse_query_tool, query_tool, reason_tool, reasoning_profiles_tool,
    recall_memory_tool_with_reasoning, register_reasoning_tool,
    run_stdio_with_prompts_and_resources, shacl_capabilities_tool, solve_tool, store_memory_tool,
    validate_tool, InMemoryMemoryStore, MemoryStore, ReasoningRegistration, SledMemoryStore,
    ToolRegistry,
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

    let reasoning = ReasoningRegistration::shared();
    let mut registry = ToolRegistry::new();
    registry.install(parse_query_tool());
    registry.install(query_tool());
    registry.install(analyze_query_tool());
    registry.install(reasoning_profiles_tool());
    registry.install(shacl_capabilities_tool());
    registry.install(reason_tool());
    registry.install(validate_tool());
    registry.install(solve_tool());
    registry.install(store_memory_tool(memory.clone()));
    registry.install(register_reasoning_tool(memory.clone(), reasoning.clone()));
    registry.install(recall_memory_tool_with_reasoning(memory.clone(), reasoning));
    registry.install(forget_memory_tool(memory));
    let prompts = cqels_prompt_registry();
    let resources = cqels_resource_registry();

    let stdin = io::stdin();
    let stdout = io::stdout();
    let reader = BufReader::new(stdin.lock());
    let writer = stdout.lock();

    run_stdio_with_prompts_and_resources(&registry, &prompts, &resources, reader, writer)
}
