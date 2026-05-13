//! Model Context Protocol (MCP) tool surface for CQELS.
//!
//! Mirrors Java's `cqels-mcp` module. Exposes the engine as a set of
//! MCP **tools** that an LLM agent can invoke to query, register, reason
//! over, and validate RDF data managed by CQELS.
//!
//! ## Scope
//!
//! This crate ships the **protocol-agnostic tool layer**: trait
//! definitions, a registry, JSON-Schema-shaped input contracts, and
//! reference implementations of two foundational tools (`query` and
//! `parse_query`). The transport (JSON-RPC over stdio / HTTP / SSE)
//! that connects an MCP client to this registry is a follow-up and can
//! be implemented against any Rust MCP SDK (e.g. `rmcp`).
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
//! | `register_stream_query` | ✅ implemented (engine-bound; see [`stream_query`]) |
//! | `list_stream_queries` | ✅ implemented (engine-bound) |
//! | `unregister_stream_query` | ✅ implemented (engine-bound) |
//! | `poll_stream_results` | ✅ implemented (engine-bound) |
//! | `reason`           | ✅ implemented (one-shot RETE inference) |
//! | `validate`         | follow-up (needs cqels-shacl bridge) |
//! | `solve`            | follow-up (needs cqels-asp bridge) |

pub mod memory;
pub mod registry;
pub mod stream_query;
pub mod tool;
pub mod tools;
pub mod transport;

pub use memory::{InMemoryMemoryStore, MemoryError, MemoryFact, MemoryStore, SledMemoryStore};
pub use registry::{McpError, ToolRegistry};
pub use stream_query::{
    list_stream_queries_tool, poll_stream_results_tool, register_stream_query_tool,
    unregister_stream_query_tool, ListStreamQueriesTool, PollStreamResultsTool,
    RegisterStreamQueryTool, StreamQueryHub, UnregisterStreamQueryTool,
};
pub use tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};
pub use tools::{
    analyze_query_tool, forget_memory_tool, parse_query_tool, query_tool, reason_tool,
    reasoning_profiles_tool, recall_memory_tool, shacl_capabilities_tool, store_memory_tool,
};
pub use transport::{handle_request, run_stdio, PROTOCOL_VERSION, SERVER_NAME};
