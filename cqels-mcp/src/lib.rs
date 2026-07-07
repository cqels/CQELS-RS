//! Model Context Protocol (MCP) tool surface for CQELS.
//!
//! Mirrors Java's `cqels-mcp` module. Exposes the engine as a set of
//! MCP **tools**, **prompts**, and **resources** that an LLM agent can
//! invoke to query, register, reason over, validate, manage, and inspect
//! RDF data with CQELS.
//!
//! ## Scope
//!
//! This crate ships protocol-agnostic layers for tools and prompt
//! templates and resources: traits, registries, JSON-Schema-shaped input
//! contracts, and reference implementations. The bundled transports are
//! line-delimited JSON-RPC over stdio and an opt-in HTTP JSON-RPC endpoint.
//!
//! Mapping to Java's tool set:
//!
//! | Java tool          | Status here              |
//! |--------------------|--------------------------|
//! | `query`            | ✅ implemented (dry-run) |
//! | `parse_query`      | ✅ implemented (Rust-only) |
//! | `analyze_query`    | ✅ implemented (Rust-only; compiler view) |
//! | `reasoning_profiles` | ✅ implemented (metadata) |
//! | `shacl_capabilities` | ✅ implemented (metadata) |
//! | `store_memory`     | ✅ implemented (in-memory / sled) |
//! | `recall_memory`    | ✅ implemented (in-memory / sled) |
//! | `forget_memory`    | ✅ implemented (in-memory / sled) |
//! | `register_reasoning` | ✅ implemented (memory entailment config) |
//! | `register_stream_query` | ✅ implemented (engine-bound; see [`stream_query`]) |
//! | `forget_stream_query` | ✅ implemented (Java-compatible engine-bound alias) |
//! | `list_stream_queries` | ✅ implemented (engine-bound) |
//! | `unregister_stream_query` | ✅ implemented (engine-bound) |
//! | `poll_stream_results` | ✅ implemented (engine-bound) |
//! | `save_procedure`    | ✅ implemented (procedural memory) |
//! | `list_procedures`   | ✅ implemented (procedural memory) |
//! | `run_procedure`     | ✅ implemented (placeholder binding + dry-run dispatch) |
//! | `record_event`      | ✅ implemented (episodic memory) |
//! | `recall_episodes`   | ✅ implemented (episodic filters) |
//! | `explain_decision`  | ✅ implemented (decision lineage) |
//! | `recall_decisions`  | ✅ implemented (decision filters) |
//! | `set_access_policy` | ✅ implemented (role/label grants) |
//! | `assemble_context`  | ✅ implemented (lexical working-memory bundle) |
//! | `reason`           | ✅ implemented (one-shot RETE inference) |
//! | `validate`         | ✅ implemented (SHACL bridge; see [`validate`]) |
//! | `solve`            | ✅ implemented (ASP bridge; see [`solve`]) |

pub mod http_transport;
pub mod memory;
pub mod prompt;
pub mod registry;
pub mod resource;
pub mod solve;
pub mod stream_query;
pub mod tool;
pub mod tools;
pub mod transport;
pub mod validate;

pub use http_transport::{
    run_http, run_http_with_prompts_and_resources, server_transport_from_env, HttpConfigError,
    HttpTransportConfig, ServerTransport,
};
pub use memory::{InMemoryMemoryStore, MemoryError, MemoryFact, MemoryStore, SledMemoryStore};
pub use prompt::{
    cqels_prompt_registry, install_cqels_prompts, McpPrompt, PromptArgument, PromptContent,
    PromptDescriptor, PromptError, PromptInvocation, PromptMessage, PromptRegistry, PromptResult,
};
pub use registry::{McpError, ToolRegistry};
pub use resource::{
    cqels_resource_registry, cqels_resource_registry_with_streams, query_id_from_results_uri,
    query_results_updated_notification, query_results_uri, resource_updated_notification,
    ReadResourceResult, ResourceContent, ResourceDescriptor, ResourceError, ResourceRegistry,
    ResourceTemplateDescriptor, DEFAULT_STREAM, RESOURCE_KG_STATS, RESOURCE_QUERIES,
    RESOURCE_QUERY_RESULTS_TEMPLATE, RESOURCE_REASONING, RESOURCE_STREAMS,
};
pub use solve::{solve_tool, solve_tool_with_solver, SolveTool};
pub use stream_query::{
    forget_stream_query_tool, list_stream_queries_tool, poll_stream_results_tool,
    register_stream_query_tool, unregister_stream_query_tool, ForgetStreamQueryTool,
    ListStreamQueriesTool, PollStreamResultsTool, RegisterStreamQueryTool, StreamQueryHub,
    UnregisterStreamQueryTool,
};
pub use tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};
pub use tools::{
    analyze_query_tool, assemble_context_tool, assemble_context_tool_with_access_policy,
    explain_decision_tool, forget_memory_tool, list_procedures_tool, parse_query_tool, query_tool,
    reason_tool, reasoning_profiles_tool, recall_decisions_tool, recall_episodes_tool,
    recall_memory_tool, recall_memory_tool_with_reasoning,
    recall_memory_tool_with_reasoning_and_access_policy, record_event_tool,
    register_reasoning_tool, run_procedure_tool, save_procedure_tool, set_access_policy_tool,
    shacl_capabilities_tool, store_memory_tool, AccessPolicyRegistry, ReasoningRegistration,
};
pub use transport::{
    handle_request, handle_request_with_prompts, handle_request_with_prompts_and_resources,
    handle_request_with_resources, run_stdio, run_stdio_with_prompts,
    run_stdio_with_prompts_and_resources, run_stdio_with_resources, PROTOCOL_VERSION, SERVER_NAME,
};
pub use validate::{validate_tool, validate_tool_with_solver, ValidateTool};
