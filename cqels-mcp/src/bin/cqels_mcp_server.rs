//! Entry-point binary for the `cqels-mcp` server.
//!
//! Run with `cargo run -p cqels-mcp --bin cqels_mcp_server` and pipe
//! line-delimited JSON-RPC requests on stdin. Responses are written to
//! stdout, one per line.
//!
//! Set `CQELS_MCP_TRANSPORT=http` to expose the same JSON-RPC surface at
//! an opt-in HTTP endpoint (default `127.0.0.1:3000/mcp`) with optional
//! bearer auth and health-check support.
//!
//! Registered surface:
//!
//! - Stateless/introspection: `parse_query`, `query`, `analyze_query`,
//!   `reasoning_profiles`, `shacl_capabilities`, `reason`, `validate`,
//!   and `solve`.
//! - Java alpha.8 memory surface: `store_memory`, `recall_memory`,
//!   `forget_memory`, `register_stream_query`, `forget_stream_query`,
//!   `save_procedure`, `list_procedures`, `run_procedure`, `record_event`,
//!   `recall_episodes`, `explain_decision`, `recall_decisions`,
//!   `register_reasoning`, `set_access_policy`, and `assemble_context`.
//!   Memory-backed tools use [`SledMemoryStore`] when `CQELS_MCP_MEMORY_DIR`
//!   is set, otherwise [`InMemoryMemoryStore`].
//! - Rust convenience stream tools: `list_stream_queries`,
//!   `unregister_stream_query`, and `poll_stream_results`.
//! - Prompt templates: Java-compatible CQELS workflow/query prompts
//!   exposed through `prompts/list` and `prompts/get`.
//! - Resources:
//! - `cqels://kg/stats`
//! - `cqels://streams`
//! - `cqels://queries`
//! - `cqels://reasoning/capabilities`
//! - `cqels://queries/{queryId}/results` template

use std::error::Error;
use std::io::{self, BufReader};
use std::sync::Arc;

use cqels_engine::CqelsEngine;
use cqels_mcp::{
    analyze_query_tool, assemble_context_tool_with_access_policy, cqels_prompt_registry,
    cqels_resource_registry_with_streams, explain_decision_tool, forget_memory_tool,
    forget_stream_query_tool, list_procedures_tool, list_stream_queries_tool, parse_query_tool,
    poll_stream_results_tool, query_tool, reason_tool, reasoning_profiles_tool,
    recall_decisions_tool, recall_episodes_tool,
    recall_memory_tool_with_reasoning_and_access_policy, record_event_tool,
    register_reasoning_tool, register_stream_query_tool, run_http_with_prompts_and_resources,
    run_procedure_tool, run_stdio_with_prompts_and_resources, save_procedure_tool,
    server_transport_from_env, set_access_policy_tool, shacl_capabilities_tool, solve_tool,
    store_memory_tool, unregister_stream_query_tool, validate_tool, AccessPolicyRegistry,
    InMemoryMemoryStore, MemoryStore, ReasoningRegistration, ServerTransport, SledMemoryStore,
    StreamQueryHub, ToolRegistry,
};

fn main() -> Result<(), Box<dyn Error>> {
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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let engine = {
        let engine = CqelsEngine::builder().id("cqels-mcp").build()?;
        runtime.block_on(engine.start())?;
        Arc::new(engine)
    };
    let stream_hub = StreamQueryHub::new(engine, runtime.handle().clone());
    let reasoning = ReasoningRegistration::shared();
    let access_policy = AccessPolicyRegistry::shared();
    let mut registry = ToolRegistry::new();
    registry.install(parse_query_tool());
    registry.install(query_tool());
    registry.install(analyze_query_tool());
    registry.install(register_stream_query_tool(stream_hub.clone()));
    registry.install(forget_stream_query_tool(stream_hub.clone()));
    registry.install(list_stream_queries_tool(stream_hub.clone()));
    registry.install(unregister_stream_query_tool(stream_hub.clone()));
    registry.install(poll_stream_results_tool(stream_hub.clone()));
    registry.install(reasoning_profiles_tool());
    registry.install(shacl_capabilities_tool());
    registry.install(reason_tool());
    registry.install(validate_tool());
    registry.install(solve_tool());
    registry.install(store_memory_tool(memory.clone()));
    registry.install(register_reasoning_tool(memory.clone(), reasoning.clone()));
    registry.install(recall_memory_tool_with_reasoning_and_access_policy(
        memory.clone(),
        reasoning,
        access_policy.clone(),
    ));
    registry.install(forget_memory_tool(memory.clone()));
    registry.install(save_procedure_tool(memory.clone()));
    registry.install(list_procedures_tool(memory.clone()));
    registry.install(run_procedure_tool(memory.clone()));
    registry.install(record_event_tool(memory.clone()));
    registry.install(recall_episodes_tool(memory.clone()));
    registry.install(explain_decision_tool(memory.clone()));
    registry.install(recall_decisions_tool(memory.clone()));
    registry.install(set_access_policy_tool(
        memory.clone(),
        access_policy.clone(),
    ));
    registry.install(assemble_context_tool_with_access_policy(
        memory,
        access_policy,
    ));
    let prompts = cqels_prompt_registry();
    let resources = cqels_resource_registry_with_streams(stream_hub);

    match server_transport_from_env()? {
        ServerTransport::Stdio => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            let reader = BufReader::new(stdin.lock());
            let writer = stdout.lock();
            run_stdio_with_prompts_and_resources(&registry, &prompts, &resources, reader, writer)?;
        }
        ServerTransport::Http(config) => {
            runtime.block_on(run_http_with_prompts_and_resources(
                Arc::new(registry),
                Arc::new(prompts),
                Arc::new(resources),
                config,
            ))?;
        }
        _ => return Err("unsupported cqels-mcp server transport".into()),
    }

    Ok(())
}
