//! Engine-bound MCP tools for managing live stream queries.
//!
//! Mirrors Java's `register_stream_query` tool surface. Unlike the
//! stateless tools in [`crate::tools`], these require a running
//! [`CqelsEngine`] and a tokio runtime to dispatch async calls from
//! synchronous MCP handlers.
//!
//! ## Components
//!
//! - [`StreamQueryHub`] — shared state: an `Arc<CqelsEngine>`, a tokio
//!   [`Handle`] for blocking on async calls, and a per-query result
//!   buffer that downstream MCP clients can drain.
//! - [`register_stream_query_tool`] — compiles and registers a CqelsQL
//!   query, returning the assigned `query_id`. Results stream into the
//!   hub's buffer.
//! - [`list_stream_queries_tool`] — returns the IDs of currently
//!   registered queries.
//! - [`forget_stream_query_tool`] — Java-compatible alias that cancels
//!   a query by `queryId` and reports `status: "forgotten"`.
//! - [`unregister_stream_query_tool`] — cancels and removes a query by
//!   ID.
//! - [`poll_stream_results_tool`] — drains the buffered results for a
//!   query.
//!
//! ## Host wiring
//!
//! The host program is responsible for building the engine, starting it,
//! and providing the tokio runtime handle. A typical wiring:
//!
//! ```ignore
//! let rt = tokio::runtime::Runtime::new()?;
//! let engine = rt.block_on(CqelsEngine::builder().build())?;
//! let hub = StreamQueryHub::new(Arc::new(engine), rt.handle().clone());
//! let mut reg = ToolRegistry::new();
//! reg.install(register_stream_query_tool(hub.clone()));
//! reg.install(forget_stream_query_tool(hub.clone()));
//! reg.install(list_stream_queries_tool(hub.clone()));
//! reg.install(unregister_stream_query_tool(hub.clone()));
//! reg.install(poll_stream_results_tool(hub));
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cqels_engine::listener::listener_from_fn;
use cqels_engine::CqelsEngine;
use cqels_model::{BindingSet, CqelsError};
use parking_lot::Mutex;
use serde_json::json;
use tokio::runtime::Handle;

use crate::tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};

/// Shared state for engine-bound MCP tools.
///
/// Cheap to clone — internally an `Arc` over the hub state.
#[derive(Clone)]
pub struct StreamQueryHub {
    inner: Arc<HubInner>,
}

struct HubInner {
    engine: Arc<CqelsEngine>,
    handle: Handle,
    results: Mutex<HashMap<String, VecDeque<BindingSet>>>,
    registrations: Mutex<HashMap<String, StreamRegistration>>,
    pending_registrations: Mutex<HashSet<String>>,
}

#[derive(Clone, Debug)]
struct StreamRegistration {
    engine_query_id: String,
    buffer_size: usize,
}

const DEFAULT_BUFFER_SIZE: usize = 100;
const MAX_BUFFER_SIZE: usize = 100_000;

impl StreamQueryHub {
    /// Constructs a new hub bound to `engine` and `handle`.
    ///
    /// `handle` is used to call the engine's async APIs from
    /// synchronous MCP tool handlers. It MUST belong to a multi-thread
    /// runtime, or be the handle of a runtime executing on a different
    /// thread — otherwise `block_on` will deadlock.
    pub fn new(engine: Arc<CqelsEngine>, handle: Handle) -> Self {
        Self {
            inner: Arc::new(HubInner {
                engine,
                handle,
                results: Mutex::new(HashMap::new()),
                registrations: Mutex::new(HashMap::new()),
                pending_registrations: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Returns the buffered result count for `query_id`.
    pub fn buffered_count(&self, query_id: &str) -> usize {
        self.inner
            .results
            .lock()
            .get(query_id)
            .map(VecDeque::len)
            .unwrap_or(0)
    }

    /// Returns the IDs of all currently registered stream queries.
    pub fn registered_query_ids(&self) -> Vec<String> {
        let mut ids = self
            .inner
            .registrations
            .lock()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    /// Returns the names of all currently registered input streams.
    pub fn registered_stream_names(&self) -> Vec<String> {
        let engine = self.inner.engine.clone();
        self.inner
            .handle
            .block_on(async move { engine.registered_stream_names().await })
    }

    /// Drains up to `max` buffered results for `query_id`. Returns the
    /// drained results in FIFO order.
    pub fn drain_results(&self, query_id: &str, max: usize) -> Vec<BindingSet> {
        let mut guard = self.inner.results.lock();
        let Some(buffer) = guard.get_mut(query_id) else {
            return Vec::new();
        };
        let take = max.min(buffer.len());
        buffer.drain(..take).collect()
    }

    fn record_result(&self, query_id: String, result: BindingSet) {
        let buffer_size = self
            .inner
            .registrations
            .lock()
            .get(&query_id)
            .map(|registration| registration.buffer_size)
            .unwrap_or(DEFAULT_BUFFER_SIZE);
        let mut guard = self.inner.results.lock();
        let buffer = guard.entry(query_id).or_default();
        buffer.push_back(result);
        while buffer.len() > buffer_size {
            buffer.pop_front();
        }
    }

    fn forget_results(&self, query_id: &str) {
        self.inner.results.lock().remove(query_id);
    }

    fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        let registration = self
            .inner
            .registrations
            .lock()
            .remove(query_id)
            .ok_or_else(|| CqelsError::Stream {
                message: format!("stream query '{query_id}' not found"),
            })?;
        let engine = self.inner.engine.clone();
        let id_for_async = registration.engine_query_id;
        self.inner
            .handle
            .block_on(async move { engine.unregister_query(&id_for_async).await })?;
        self.forget_results(query_id);
        Ok(())
    }
}

// ─── register_stream_query ──────────────────────────────────────────

/// Constructs the `register_stream_query` MCP tool.
pub fn register_stream_query_tool(hub: StreamQueryHub) -> RegisterStreamQueryTool {
    RegisterStreamQueryTool { hub }
}

pub struct RegisterStreamQueryTool {
    hub: StreamQueryHub,
}

impl McpTool for RegisterStreamQueryTool {
    fn name(&self) -> &str {
        "register_stream_query"
    }

    fn description(&self) -> &str {
        "Register a CqelsQL query against the live engine and stream its \
         results into an internal buffer. Returns the assigned `query_id`, \
         which callers use with `poll_stream_results`, \
         `forget_stream_query`, or `unregister_stream_query`."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "CqelsQL query text to register"
                }),
            )
            .with_property(
                "language",
                json!({
                    "type": "string",
                    "enum": ["cqelsql"],
                    "default": "cqelsql",
                    "description": "Query language. This Rust MCP server currently accepts cqelsql."
                }),
            )
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "Optional stable MCP-facing query ID. Defaults to the engine query ID."
                }),
            )
            .with_property(
                "bufferSize",
                json!({
                    "type": "integer",
                    "default": DEFAULT_BUFFER_SIZE,
                    "description": "Maximum buffered result rows retained for polling, clamped to [1, 100000]."
                }),
            )
            .with_property(
                "notify",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "Whether clients intend to use resource update notifications. The transport exposes notification payload helpers but does not push unsolicited messages."
                }),
            )
            .with_property(
                "cep",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "CEP registration flag from Java alpha.8. CEP-path registration is not yet available in this Rust MCP transport and fails loud when true."
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query) = invocation.get_str("query").map(str::to_string) else {
            return ToolResult::error("missing `query` argument");
        };
        let language = invocation
            .get_str("language")
            .map(str::trim)
            .filter(|language| !language.is_empty())
            .unwrap_or("cqelsql")
            .to_ascii_lowercase();
        if language != "cqelsql" {
            return ToolResult::error(format!(
                "unsupported register_stream_query language '{language}'; supported: cqelsql"
            ));
        }
        let cep = invocation
            .get("cep")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if cep {
            return ToolResult::error(
                "cep=true stream queries are not yet supported by cqels-rs MCP",
            );
        }
        let requested_query_id = invocation
            .get_str("queryId")
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
        if let Some(query_id) = requested_query_id.as_ref() {
            let registrations = self.hub.inner.registrations.lock();
            let mut pending = self.hub.inner.pending_registrations.lock();
            if registrations.contains_key(query_id) || !pending.insert(query_id.clone()) {
                return ToolResult::error(format!("Query already registered: {query_id}"));
            }
        }
        let reserved_query_id = requested_query_id.clone();
        let buffer_size = invocation
            .get("bufferSize")
            .and_then(|value| value.as_i64())
            .map(|value| value.clamp(1, MAX_BUFFER_SIZE as i64) as usize)
            .unwrap_or(DEFAULT_BUFFER_SIZE);
        let notify = invocation
            .get("notify")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let engine = self.hub.inner.engine.clone();
        let hub_for_listener = self.hub.clone();

        // `register_cqelsql_query` is async; block via the bound handle.
        // The query_id is assigned inside the call, so we capture it
        // *after* registration and wire it into the listener via a
        // late-binding cell.
        let registration = self.hub.inner.handle.block_on(async move {
            // We need to know the query_id before registering so the
            // listener can route results to the right buffer slot.
            // The engine assigns the ID, so we use a shared cell.
            let id_cell: Arc<parking_lot::Mutex<Option<String>>> =
                Arc::new(parking_lot::Mutex::new(None));
            let id_cell_listener = id_cell.clone();
            let hub_for_results = hub_for_listener.clone();
            let listener = listener_from_fn(move |result: BindingSet| {
                let id_guard = id_cell_listener.lock();
                if let Some(id) = id_guard.as_ref() {
                    hub_for_results.record_result(id.clone(), result);
                }
            });
            let engine_query_id = engine.register_cqelsql_query(&query, listener).await?;
            let query_id = requested_query_id.unwrap_or_else(|| engine_query_id.clone());
            hub_for_listener.inner.registrations.lock().insert(
                query_id.clone(),
                StreamRegistration {
                    engine_query_id: engine_query_id.clone(),
                    buffer_size,
                },
            );
            *id_cell.lock() = Some(query_id.clone());
            Ok::<(String, String), cqels_model::CqelsError>((query_id, engine_query_id))
        });
        if let Some(query_id) = reserved_query_id {
            self.hub
                .inner
                .pending_registrations
                .lock()
                .remove(&query_id);
        }

        match registration {
            Ok((query_id, engine_query_id)) => ToolResult::success(json!({
                "ok": true,
                "query_id": query_id,
                "queryId": query_id,
                "engineQueryId": engine_query_id,
                "status": "registered",
                "bufferSize": buffer_size,
                "notify": notify,
                "cep": cep,
            })),
            Err(e) => ToolResult::error(format!("register failed: {e}")),
        }
    }
}

// ─── list_stream_queries ────────────────────────────────────────────

/// Constructs the `list_stream_queries` MCP tool.
pub fn list_stream_queries_tool(hub: StreamQueryHub) -> ListStreamQueriesTool {
    ListStreamQueriesTool { hub }
}

pub struct ListStreamQueriesTool {
    hub: StreamQueryHub,
}

impl McpTool for ListStreamQueriesTool {
    fn name(&self) -> &str {
        "list_stream_queries"
    }

    fn description(&self) -> &str {
        "List the IDs of all currently registered stream queries."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
    }

    fn call(&self, _invocation: &ToolInvocation) -> ToolResult {
        let ids = self.hub.registered_query_ids();
        ToolResult::success(json!({
            "count": ids.len(),
            "query_ids": ids,
        }))
    }
}

// ─── forget_stream_query ────────────────────────────────────────────

/// Constructs the Java-compatible `forget_stream_query` MCP tool.
pub fn forget_stream_query_tool(hub: StreamQueryHub) -> ForgetStreamQueryTool {
    ForgetStreamQueryTool { hub }
}

pub struct ForgetStreamQueryTool {
    hub: StreamQueryHub,
}

impl McpTool for ForgetStreamQueryTool {
    fn name(&self) -> &str {
        "forget_stream_query"
    }

    fn description(&self) -> &str {
        "Stop and deregister a continuous query previously created with \
         register_stream_query: unregisters its engine subscription and \
         discards its result buffer."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "ID of the stream query to stop and deregister (as returned by register_stream_query)"
                }),
            )
            .require("queryId")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let query_id = match query_id_for_forget(invocation) {
            Some(query_id) => query_id,
            None => return ToolResult::error("Missing required parameter: queryId"),
        };

        match self.hub.unregister_query(&query_id) {
            Ok(()) => ToolResult::success(json!({
                "queryId": query_id,
                "status": "forgotten",
            })),
            Err(e) if stream_query_not_found(&e) => {
                ToolResult::error(format!("No such registered stream query: {query_id}"))
            }
            Err(e) => ToolResult::error(format!("forget_stream_query failed: {e}")),
        }
    }
}

fn query_id_for_forget(invocation: &ToolInvocation) -> Option<String> {
    // A present-but-invalid Java key should fail like Java rather than
    // falling through to the Rust snake_case compatibility alias.
    if invocation.get("queryId").is_some() {
        return invocation
            .get_str("queryId")
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
    }

    invocation
        .get_str("query_id")
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

fn stream_query_not_found(error: &CqelsError) -> bool {
    matches!(error, CqelsError::Stream { message } if message.contains("not found"))
}

// ─── unregister_stream_query ────────────────────────────────────────

/// Constructs the `unregister_stream_query` MCP tool.
pub fn unregister_stream_query_tool(hub: StreamQueryHub) -> UnregisterStreamQueryTool {
    UnregisterStreamQueryTool { hub }
}

pub struct UnregisterStreamQueryTool {
    hub: StreamQueryHub,
}

impl McpTool for UnregisterStreamQueryTool {
    fn name(&self) -> &str {
        "unregister_stream_query"
    }

    fn description(&self) -> &str {
        "Cancel and remove a previously registered stream query by ID. \
         Also clears any buffered results for that query."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query_id",
                json!({
                    "type": "string",
                    "description": "Query ID returned by register_stream_query"
                }),
            )
            .require("query_id")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query_id) = invocation.get_str("query_id").map(str::to_string) else {
            return ToolResult::error("missing `query_id` argument");
        };

        match self.hub.unregister_query(&query_id) {
            Ok(()) => ToolResult::success(json!({
                "ok": true,
                "query_id": query_id,
            })),
            Err(e) => ToolResult::error(format!("unregister failed: {e}")),
        }
    }
}

// ─── poll_stream_results ────────────────────────────────────────────

/// Constructs the `poll_stream_results` MCP tool.
pub fn poll_stream_results_tool(hub: StreamQueryHub) -> PollStreamResultsTool {
    PollStreamResultsTool { hub }
}

pub struct PollStreamResultsTool {
    hub: StreamQueryHub,
}

const DEFAULT_POLL_LIMIT: usize = 64;

impl McpTool for PollStreamResultsTool {
    fn name(&self) -> &str {
        "poll_stream_results"
    }

    fn description(&self) -> &str {
        "Drain buffered results for a registered stream query. Returns up \
         to `limit` bindings in FIFO order and removes them from the \
         buffer. Defaults to 64 per call."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query_id",
                json!({
                    "type": "string",
                    "description": "Query ID returned by register_stream_query"
                }),
            )
            .with_property(
                "limit",
                json!({
                    "type": "integer",
                    "description": "Maximum number of buffered results to drain. Defaults to 64.",
                    "default": DEFAULT_POLL_LIMIT
                }),
            )
            .require("query_id")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query_id) = invocation.get_str("query_id") else {
            return ToolResult::error("missing `query_id` argument");
        };
        let limit = invocation
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_POLL_LIMIT);
        let drained = self.hub.drain_results(query_id, limit);
        let remaining = self.hub.buffered_count(query_id);
        ToolResult::success(json!({
            "query_id": query_id,
            "count": drained.len(),
            "remaining": remaining,
            "results": drained,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ToolRegistry;
    use cqels_engine::CqelsEngine;
    use tokio::runtime::Runtime;

    /// Build a fresh engine + multi-thread runtime + hub for tests.
    fn fresh_hub() -> (StreamQueryHub, Arc<Runtime>) {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        let handle = runtime.handle().clone();
        let hub = StreamQueryHub::new(Arc::new(engine), handle);
        (hub, runtime)
    }

    /// Like [`fresh_hub`] but pre-creates a `sensors` stream so a
    /// query that subscribes to `FROM STREAM sensors` stays alive past
    /// the initial `execute()` poll. Avoids races between the
    /// drain-task auto-cleanup and explicit `unregister` in tests
    /// that don't actually feed data through the stream.
    fn fresh_hub_with_sensors_stream() -> (StreamQueryHub, Arc<Runtime>) {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let mut engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        // Create the `sensors` stream — we never push data into it,
        // we just need it registered so the query's drain task has
        // something to await rather than terminating on an empty
        // input.
        let _sender = runtime
            .block_on(async { engine.create_stream("sensors").await })
            .expect("create_stream");
        let handle = runtime.handle().clone();
        let hub = StreamQueryHub::new(Arc::new(engine), handle);
        (hub, runtime)
    }

    fn install_all(hub: &StreamQueryHub) -> ToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.install(register_stream_query_tool(hub.clone()));
        reg.install(forget_stream_query_tool(hub.clone()));
        reg.install(list_stream_queries_tool(hub.clone()));
        reg.install(unregister_stream_query_tool(hub.clone()));
        reg.install(poll_stream_results_tool(hub.clone()));
        reg
    }

    fn sample_query() -> &'static str {
        // Minimal valid CqelsQL query that the engine accepts.
        r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [RANGE 10s]
            WHERE { ?sensor <http://ex.org/temp> ?temp . }
        "#
    }

    #[test]
    fn register_then_list_includes_returned_id() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        assert!(!res.is_error, "register failed: {:?}", res.content);
        let id = res.content["query_id"]
            .as_str()
            .expect("query_id present")
            .to_string();

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        assert!(!list.is_error);
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(ids.contains(&id), "list should contain {id}; got {ids:?}");
    }

    #[test]
    fn register_accepts_java_query_id_and_buffer_options() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("queryId", json!("custom-q"))
                    .with_arg("bufferSize", json!(1))
                    .with_arg("notify", json!(true)),
            )
            .expect("dispatch");
        assert!(!res.is_error, "register failed: {:?}", res.content);
        assert_eq!(res.content["query_id"], "custom-q");
        assert_eq!(res.content["queryId"], "custom-q");
        assert_eq!(res.content["bufferSize"], 1);
        assert_eq!(res.content["notify"], true);
        assert!(res.content["engineQueryId"].is_string());

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        assert_eq!(list.content["query_ids"], json!(["custom-q"]));

        let duplicate = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("queryId", json!("custom-q")),
            )
            .expect("dispatch");
        assert!(duplicate.is_error);
        assert_eq!(
            duplicate.content["message"],
            "Query already registered: custom-q"
        );
    }

    #[test]
    fn register_rejects_unsupported_java_options_loudly() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let cypher = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!("MATCH (n) RETURN n"))
                    .with_arg("language", json!("cypher")),
            )
            .expect("dispatch");
        assert!(cypher.is_error);
        assert!(cypher.content["message"]
            .as_str()
            .unwrap()
            .contains("unsupported"));

        let cep = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("cep", json!(true)),
            )
            .expect("dispatch");
        assert!(cep.is_error);
        assert!(cep.content["message"]
            .as_str()
            .unwrap()
            .contains("cep=true"));
    }

    #[test]
    fn unregister_removes_query_from_list_and_clears_buffer() {
        // Pre-register `sensors` so the query's drain task doesn't
        // race to auto-cleanup before the explicit `unregister` call
        // — the test exercises the unregister path, not stream
        // lifecycle.
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let unreg = reg
            .call(
                "unregister_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!unreg.is_error, "unregister failed: {:?}", unreg.content);

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!ids.contains(&id), "id {id} should be gone from {ids:?}");
        // Buffer should also be empty.
        assert_eq!(hub.buffered_count(&id), 0);
    }

    #[test]
    fn forget_stream_query_accepts_java_query_id_and_returns_forgotten_status() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let forget = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!forget.is_error, "forget failed: {:?}", forget.content);
        assert_eq!(forget.content["queryId"], id);
        assert_eq!(forget.content["status"], "forgotten");

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!ids.contains(&id), "id {id} should be gone from {ids:?}");
        assert_eq!(hub.buffered_count(&id), 0);
    }

    #[test]
    fn forget_stream_query_second_call_reports_unknown_id() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let first = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!first.is_error, "first forget failed: {:?}", first.content);

        let second = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(second.is_error);
        assert_eq!(
            second.content["message"],
            format!("No such registered stream query: {id}")
        );
    }

    #[test]
    fn forget_stream_query_accepts_snake_case_alias() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let forget = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!forget.is_error, "forget failed: {:?}", forget.content);
        assert_eq!(forget.content["queryId"], id);
        assert_eq!(forget.content["status"], "forgotten");
    }

    #[test]
    fn poll_returns_empty_for_fresh_registration() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let poll = reg
            .call(
                "poll_stream_results",
                &ToolInvocation::new().with_arg("query_id", json!(id)),
            )
            .expect("dispatch");
        assert!(!poll.is_error);
        assert_eq!(poll.content["count"], 0);
        assert_eq!(poll.content["remaining"], 0);
        assert_eq!(poll.content["results"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn register_rejects_missing_query_argument() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call("register_stream_query", &ToolInvocation::new())
            .expect("dispatch");
        assert!(res.is_error);
    }

    #[test]
    fn register_propagates_parse_errors() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!("this is not cqelsql")),
            )
            .expect("dispatch");
        assert!(res.is_error, "garbage query should fail registration");
    }

    #[test]
    fn unregister_rejects_unknown_id() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "unregister_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!("nonexistent-id")),
            )
            .expect("dispatch");
        assert!(res.is_error);
    }

    #[test]
    fn forget_stream_query_rejects_missing_or_blank_query_id() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for invocation in [
            ToolInvocation::new(),
            ToolInvocation::new().with_arg("queryId", json!("")),
            ToolInvocation::new().with_arg("queryId", json!("   ")),
            ToolInvocation::new().with_arg("query_id", json!("")),
            ToolInvocation::new().with_arg("query_id", json!("   ")),
        ] {
            let res = reg
                .call("forget_stream_query", &invocation)
                .expect("dispatch");
            assert!(res.is_error);
            assert_eq!(
                res.content["message"],
                "Missing required parameter: queryId"
            );
        }
    }

    #[test]
    fn forget_stream_query_rejects_unknown_id_with_java_message() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!("nonexistent-id")),
            )
            .expect("dispatch");
        assert!(res.is_error);
        assert_eq!(
            res.content["message"],
            "No such registered stream query: nonexistent-id"
        );
    }

    #[test]
    fn drain_results_drains_buffer_in_fifo_order() {
        // Test the buffer plumbing directly without depending on engine
        // result delivery timing.
        let (hub, _rt) = fresh_hub();
        let id = "test-query".to_string();
        hub.record_result(id.clone(), BindingSet::new(1));
        hub.record_result(id.clone(), BindingSet::new(2));
        hub.record_result(id.clone(), BindingSet::new(3));
        assert_eq!(hub.buffered_count(&id), 3);
        let drained = hub.drain_results(&id, 2);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].timestamp(), 1);
        assert_eq!(drained[1].timestamp(), 2);
        assert_eq!(hub.buffered_count(&id), 1);
    }

    #[test]
    fn registered_tools_advertise_schemas() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for name in [
            "register_stream_query",
            "forget_stream_query",
            "list_stream_queries",
            "unregister_stream_query",
            "poll_stream_results",
        ] {
            let tool = reg.get(name).unwrap_or_else(|| panic!("{name} installed"));
            let schema = tool.input_schema();
            assert_eq!(schema.type_field, "object");
        }

        let forget_schema = reg
            .get("forget_stream_query")
            .expect("forget_stream_query installed")
            .input_schema();
        assert!(forget_schema.properties.contains_key("queryId"));
        assert_eq!(forget_schema.required, vec!["queryId".to_string()]);
    }
}
