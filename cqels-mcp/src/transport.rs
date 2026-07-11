//! JSON-RPC 2.0 transport for the MCP tool/prompt/resource surface.
//!
//! Implements a minimal subset of the [Model Context Protocol](https://modelcontextprotocol.io/)
//! over a line-delimited JSON-RPC 2.0 stream. The runtime reads one
//! request per line, dispatches it against a [`crate::ToolRegistry`],
//! and writes one response per line back to the writer.
//!
//! Supported MCP methods:
//!
//! - `initialize` — handshake. Returns the server's protocol version and
//!   advertised capabilities.
//! - `tools/list` — returns the registry's tools with their schemas.
//! - `tools/call` — dispatches a tool invocation by name.
//! - `prompts/list` — returns prompt templates, when a prompt registry
//!   is supplied.
//! - `prompts/get` — renders a prompt template by name.
//! - `resources/list` — returns CQELS metadata resources when a
//!   [`crate::ResourceRegistry`] is supplied.
//! - `resources/templates/list` — returns resource templates when a
//!   [`crate::ResourceRegistry`] is supplied.
//! - `resources/read` — reads a resource URI when a
//!   [`crate::ResourceRegistry`] is supplied.
//! - `resources/subscribe` / `resources/unsubscribe` — validate a resource
//!   URI and acknowledge subscription intent for hosts that emit resource
//!   update notifications.
//! - `ping` — connection liveness check.
//!
//! Notifications (no `id` field) are accepted but produce no response.
//! Any unknown method returns a JSON-RPC error with code `-32601`.

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::prompt::{PromptInvocation, PromptRegistry};
use crate::registry::ToolRegistry;
use crate::resource::{ResourceError, ResourceRegistry};
use crate::tool::ToolInvocation;

/// MCP protocol version advertised by [`handle_request`].
pub const PROTOCOL_VERSION: &str = "2025-03-26";

/// Server identity returned by `initialize`.
pub const SERVER_NAME: &str = "cqels-mcp";

/// Agent-facing instructions returned by `initialize`.
pub const SERVER_INSTRUCTIONS: &str = r#"CQELS is a continuous RDF stream and complex-event-processing reasoner exposed as agent memory.

Use store_memory for durable background facts and push_stream_events for live observations on named streams. For lossless stream workflows, create the stream, register standing queries or observers, then push events with explicit eventTime values.

Use query for one-shot repository snapshots. Use register_stream_query for standing CQELS-QL or Cypher queries and drain matches with poll_stream_results or cqels://queries/{queryId}/results. Use forget_stream_query to stop standing query, SHACL, or ASP observers.

Use watch_invariant for continuous SHACL validation over whole pushed observations. Use register_rules for continuous ASP rules over streamed rdf(S,P,O) atoms. Register either observer before pushing the events it should see.

CQELS-QL stream windows belong on FROM STREAM clauses, not inside WHERE stream patterns. validate_stream_query can check a draft before registration. FILTER(SEQ(...)) requires cep=true. The cqels://docs/cqelsql and cqels://docs/cep resources summarize the supported syntax.

Useful resources include cqels://engine/status, cqels://kg/stats, cqels://kg/namespaces, cqels://streams, cqels://queries, cqels://reasoning/capabilities, and cqels://queries/{queryId}/results."#;

/// JSON-RPC error codes used by this transport.
pub mod error_code {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;
}

#[derive(Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    jsonrpc: String,
    #[serde(default)]
    id: Option<JsonValue>,
    method: String,
    #[serde(default)]
    params: JsonValue,
}

#[derive(Serialize)]
#[serde(untagged)]
enum JsonRpcResponse {
    Success {
        jsonrpc: &'static str,
        id: JsonValue,
        result: JsonValue,
    },
    Error {
        jsonrpc: &'static str,
        id: JsonValue,
        error: JsonRpcError,
    },
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<JsonValue>,
}

/// Outcome of [`handle_request`]: either a serialized response or
/// `None` when the input was a notification.
pub fn handle_request(registry: &ToolRegistry, request: &str) -> Option<String> {
    handle_request_inner(registry, None, None, request)
}

/// Prompt-aware variant of [`handle_request`].
pub fn handle_request_with_prompts(
    registry: &ToolRegistry,
    prompts: &PromptRegistry,
    request: &str,
) -> Option<String> {
    handle_request_inner(registry, Some(prompts), None, request)
}

/// Like [`handle_request`], but also advertises and dispatches MCP resources.
pub fn handle_request_with_resources(
    registry: &ToolRegistry,
    resources: &ResourceRegistry,
    request: &str,
) -> Option<String> {
    handle_request_inner(registry, None, Some(resources), request)
}

/// Like [`handle_request`], but also advertises and dispatches prompts
/// and MCP resources.
pub fn handle_request_with_prompts_and_resources(
    registry: &ToolRegistry,
    prompts: &PromptRegistry,
    resources: &ResourceRegistry,
    request: &str,
) -> Option<String> {
    handle_request_inner(registry, Some(prompts), Some(resources), request)
}

fn handle_request_inner(
    registry: &ToolRegistry,
    prompts: Option<&PromptRegistry>,
    resources: Option<&ResourceRegistry>,
    request: &str,
) -> Option<String> {
    let parsed: Result<JsonRpcRequest, _> = serde_json::from_str(request);
    let request = match parsed {
        Ok(r) => r,
        Err(e) => {
            return Some(serialize_error(
                JsonValue::Null,
                error_code::PARSE_ERROR,
                format!("parse error: {e}"),
                None,
            ));
        }
    };

    if request.jsonrpc != "2.0" && !request.jsonrpc.is_empty() {
        // `jsonrpc` must be "2.0" — but be lenient and accept missing.
        return Some(serialize_error(
            request.id.unwrap_or(JsonValue::Null),
            error_code::INVALID_REQUEST,
            "jsonrpc version must be 2.0".into(),
            None,
        ));
    }

    let id = request.id.clone();
    let result = dispatch(registry, prompts, resources, &request);

    match (id, result) {
        (None, _) => None, // Notification — no response.
        (Some(id), Ok(value)) => Some(serialize_success(id, value)),
        (Some(id), Err((code, message, data))) => Some(serialize_error(id, code, message, data)),
    }
}

fn dispatch(
    registry: &ToolRegistry,
    prompts: Option<&PromptRegistry>,
    resources: Option<&ResourceRegistry>,
    request: &JsonRpcRequest,
) -> Result<JsonValue, (i32, String, Option<JsonValue>)> {
    match request.method.as_str() {
        "initialize" => {
            let mut capabilities = serde_json::Map::new();
            capabilities.insert("tools".into(), json!({}));
            if prompts.is_some() {
                capabilities.insert("prompts".into(), json!({}));
            }
            if resources.is_some() {
                capabilities.insert(
                    "resources".into(),
                    json!({
                        "subscribe": true,
                        "listChanged": false,
                    }),
                );
            }
            Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
                "capabilities": capabilities,
                "instructions": SERVER_INSTRUCTIONS,
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => {
            let mut tools_out = Vec::new();
            for name in registry.list() {
                let tool = registry.get(&name).expect("tool listed");
                tools_out.push(json!({
                    "name": tool.name(),
                    "description": tool.description(),
                    "inputSchema": tool.input_schema(),
                }));
            }
            Ok(json!({ "tools": tools_out }))
        }
        "tools/call" => {
            let name = request
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    (
                        error_code::INVALID_PARAMS,
                        "tools/call requires a `name` string parameter".into(),
                        None,
                    )
                })?;
            let arguments = request
                .params
                .get("arguments")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let invocation = ToolInvocation { arguments };
            match registry.call(name, &invocation) {
                Ok(result) => Ok(json!({
                    "content": result.content,
                    "isError": result.is_error,
                })),
                Err(e) => Err((error_code::METHOD_NOT_FOUND, e.to_string(), None)),
            }
        }
        "prompts/list" => {
            let prompts = prompts.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "prompts are not enabled".into(),
                    None,
                )
            })?;
            let mut prompts_out = Vec::new();
            for name in prompts.list() {
                let prompt = prompts.get(&name).expect("prompt listed");
                prompts_out.push(json!(prompt.descriptor()));
            }
            Ok(json!({ "prompts": prompts_out }))
        }
        "prompts/get" => {
            let prompts = prompts.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "prompts are not enabled".into(),
                    None,
                )
            })?;
            let name = request
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    (
                        error_code::INVALID_PARAMS,
                        "prompts/get requires a `name` string parameter".into(),
                        None,
                    )
                })?;
            let arguments = match request.params.get("arguments") {
                Some(value) => value.as_object().cloned().ok_or_else(|| {
                    (
                        error_code::INVALID_PARAMS,
                        "prompts/get `arguments` must be an object".into(),
                        None,
                    )
                })?,
                None => serde_json::Map::new(),
            };
            let invocation = PromptInvocation { arguments };
            match prompts.render(name, &invocation) {
                Ok(result) => Ok(json!(result)),
                Err(e) => Err((error_code::INVALID_PARAMS, e.to_string(), None)),
            }
        }
        "resources/list" => {
            let resources = resources.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "resources are not configured".to_string(),
                    None,
                )
            })?;
            Ok(json!({ "resources": resources.list() }))
        }
        "resources/templates/list" => {
            let resources = resources.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "resources are not configured".to_string(),
                    None,
                )
            })?;
            Ok(json!({ "resourceTemplates": resources.list_templates() }))
        }
        "resources/read" => {
            let resources = resources.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "resources are not configured".to_string(),
                    None,
                )
            })?;
            let uri = request
                .params
                .get("uri")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    (
                        error_code::INVALID_PARAMS,
                        "resources/read requires a `uri` string parameter".into(),
                        None,
                    )
                })?;
            match resources.read(uri) {
                Ok(result) => serde_json::to_value(result).map_err(|e| {
                    (
                        error_code::INTERNAL_ERROR,
                        format!("resource serialization failed: {e}"),
                        None,
                    )
                }),
                Err(ResourceError::UnknownResource(uri)) => Err((
                    error_code::INVALID_PARAMS,
                    format!("unknown resource '{uri}'"),
                    Some(json!({ "uri": uri })),
                )),
                Err(ResourceError::Serialize(message)) => Err((
                    error_code::INTERNAL_ERROR,
                    format!("resource serialization failed: {message}"),
                    None,
                )),
            }
        }
        "resources/subscribe" | "resources/unsubscribe" => {
            let resources = resources.ok_or_else(|| {
                (
                    error_code::METHOD_NOT_FOUND,
                    "resources are not configured".to_string(),
                    None,
                )
            })?;
            let uri = request
                .params
                .get("uri")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    (
                        error_code::INVALID_PARAMS,
                        "resource subscription requires a `uri` string parameter".into(),
                        None,
                    )
                })?;
            if resources.contains(uri) {
                Ok(json!({}))
            } else {
                Err((
                    error_code::INVALID_PARAMS,
                    format!("unknown resource '{uri}'"),
                    Some(json!({ "uri": uri })),
                ))
            }
        }
        other => Err((
            error_code::METHOD_NOT_FOUND,
            format!("unknown method '{other}'"),
            None,
        )),
    }
}

fn serialize_success(id: JsonValue, result: JsonValue) -> String {
    serde_json::to_string(&JsonRpcResponse::Success {
        jsonrpc: "2.0",
        id,
        result,
    })
    .expect("serialize success")
}

fn serialize_error(id: JsonValue, code: i32, message: String, data: Option<JsonValue>) -> String {
    serde_json::to_string(&JsonRpcResponse::Error {
        jsonrpc: "2.0",
        id,
        error: JsonRpcError {
            code,
            message,
            data,
        },
    })
    .expect("serialize error")
}

/// Runs an MCP server loop reading line-delimited JSON-RPC from `reader`
/// and writing responses to `writer`. Blocks until EOF on the reader.
///
/// Each input line is dispatched independently. Empty lines are skipped.
/// Notifications (no `id` field) are processed silently. Responses are
/// written one per line, followed by a flush.
pub fn run_stdio<R: BufRead, W: Write>(
    registry: &ToolRegistry,
    reader: R,
    mut writer: W,
) -> std::io::Result<()> {
    run_stdio_inner(registry, None, None, reader, &mut writer)
}

/// Runs an MCP server loop with both tool and prompt registries.
pub fn run_stdio_with_prompts<R: BufRead, W: Write>(
    registry: &ToolRegistry,
    prompts: &PromptRegistry,
    reader: R,
    mut writer: W,
) -> std::io::Result<()> {
    run_stdio_inner(registry, Some(prompts), None, reader, &mut writer)
}

/// Runs an MCP stdio loop with both tools and resources configured.
pub fn run_stdio_with_resources<R: BufRead, W: Write>(
    registry: &ToolRegistry,
    resources: &ResourceRegistry,
    reader: R,
    mut writer: W,
) -> std::io::Result<()> {
    run_stdio_inner(registry, None, Some(resources), reader, &mut writer)
}

/// Runs an MCP stdio loop with tools, prompts, and resources configured.
pub fn run_stdio_with_prompts_and_resources<R: BufRead, W: Write>(
    registry: &ToolRegistry,
    prompts: &PromptRegistry,
    resources: &ResourceRegistry,
    reader: R,
    mut writer: W,
) -> std::io::Result<()> {
    run_stdio_inner(
        registry,
        Some(prompts),
        Some(resources),
        reader,
        &mut writer,
    )
}

fn run_stdio_inner<R: BufRead, W: Write>(
    registry: &ToolRegistry,
    prompts: Option<&PromptRegistry>,
    resources: Option<&ResourceRegistry>,
    reader: R,
    writer: &mut W,
) -> std::io::Result<()> {
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(response) = handle_request_inner(registry, prompts, resources, trimmed) {
            writeln!(writer, "{response}")?;
            writer.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        analyze_query_tool, cqels_prompt_registry, cqels_resource_registry, parse_query_tool,
        query_results_uri, query_tool, reason_tool, reasoning_profiles_tool,
        shacl_capabilities_tool, RESOURCE_DOC_CEP, RESOURCE_DOC_CQELSQL, RESOURCE_ENGINE_STATUS,
        RESOURCE_KG_NAMESPACES, RESOURCE_KG_STATS, RESOURCE_QUERY_RESULTS_TEMPLATE,
        RESOURCE_REASONING,
    };

    fn make_registry() -> ToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.install(parse_query_tool());
        reg.install(query_tool());
        reg.install(analyze_query_tool());
        reg.install(reasoning_profiles_tool());
        reg.install(shacl_capabilities_tool());
        reg.install(reason_tool());
        reg
    }

    fn parse_response(body: &str) -> JsonValue {
        serde_json::from_str(body).expect("parse response")
    }

    #[test]
    fn initialize_returns_protocol_version_and_server_info() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["id"], 1);
        assert_eq!(value["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(value["result"]["serverInfo"]["name"], SERVER_NAME);
        assert_eq!(value["result"]["instructions"], SERVER_INSTRUCTIONS);
    }

    #[test]
    fn initialize_advertises_prompts_when_registry_is_supplied() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        assert!(value["result"]["capabilities"]["tools"].is_object());
        assert!(value["result"]["capabilities"]["prompts"].is_object());
    }

    #[test]
    fn initialize_advertises_prompts_and_resources_when_both_registries_are_supplied() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let resources = cqels_resource_registry();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let resp = handle_request_with_prompts_and_resources(&reg, &prompts, &resources, req)
            .expect("response");
        let value = parse_response(&resp);
        assert!(value["result"]["capabilities"]["tools"].is_object());
        assert!(value["result"]["capabilities"]["prompts"].is_object());
        assert_eq!(
            value["result"]["capabilities"]["resources"],
            json!({"subscribe": true, "listChanged": false})
        );
    }

    #[test]
    fn ping_returns_empty_result() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":"abc","method":"ping"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["id"], "abc");
        assert!(value["result"].is_object());
    }

    #[test]
    fn tools_list_returns_all_installed_tools_with_schemas() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        let tools = value["result"]["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 6);
        // Spot-check: every tool has name + description + inputSchema.
        for tool in tools {
            assert!(tool["name"].is_string());
            assert!(tool["description"].is_string());
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
    }

    #[test]
    fn tools_call_dispatches_to_named_tool() {
        let reg = make_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":3,"method":"tools/call",
            "params":{"name":"reasoning_profiles","arguments":{"profile":"RDFS"}}
        }"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["result"]["isError"], false);
        assert_eq!(value["result"]["content"]["name"], "RDFS");
    }

    #[test]
    fn tools_call_with_missing_name_returns_invalid_params() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{}}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::INVALID_PARAMS);
    }

    #[test]
    fn tools_call_with_unknown_tool_returns_method_not_found() {
        let reg = make_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":5,"method":"tools/call",
            "params":{"name":"nope","arguments":{}}
        }"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::METHOD_NOT_FOUND);
    }

    #[test]
    fn prompts_list_returns_all_installed_prompts() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{"jsonrpc":"2.0","id":8,"method":"prompts/list"}"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        let prompts = value["result"]["prompts"]
            .as_array()
            .expect("prompts array");
        assert_eq!(prompts.len(), 8);
        assert!(
            prompts.iter().any(|p| p["name"] == "recent_events_window"),
            "recent_events_window prompt should be advertised"
        );
        for prompt in prompts {
            assert!(prompt["name"].is_string());
            assert!(prompt["description"].is_string());
            assert!(prompt["arguments"].is_array());
        }
    }

    #[test]
    fn prompts_list_without_prompt_registry_returns_method_not_found() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":8,"method":"prompts/list"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::METHOD_NOT_FOUND);
    }

    #[test]
    fn empty_prompt_registry_is_advertised_and_lists_empty() {
        let reg = make_registry();
        let prompts = PromptRegistry::new();
        let init = r#"{"jsonrpc":"2.0","id":8,"method":"initialize"}"#;
        let init_resp = handle_request_with_prompts(&reg, &prompts, init).expect("response");
        let init_value = parse_response(&init_resp);
        assert!(init_value["result"]["capabilities"]["prompts"].is_object());

        let list = r#"{"jsonrpc":"2.0","id":9,"method":"prompts/list"}"#;
        let list_resp = handle_request_with_prompts(&reg, &prompts, list).expect("response");
        let list_value = parse_response(&list_resp);
        assert_eq!(
            list_value["result"]["prompts"]
                .as_array()
                .expect("prompts array")
                .len(),
            0
        );
    }

    #[test]
    fn prompts_get_renders_named_prompt() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":9,"method":"prompts/get",
            "params":{"name":"recent_events_window","arguments":{"stream":"SensorData","window":"RANGE 30s"}}
        }"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["id"], 9);
        let message = &value["result"]["messages"][0];
        assert_eq!(message["role"], "user");
        assert_eq!(message["content"]["type"], "text");
        assert!(message["content"]["text"]
            .as_str()
            .unwrap()
            .contains("FROM STREAM SensorData [RANGE 30s]"));
    }

    #[test]
    fn prompts_get_with_missing_name_returns_invalid_params() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{"jsonrpc":"2.0","id":10,"method":"prompts/get","params":{}}"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::INVALID_PARAMS);
    }

    #[test]
    fn prompts_get_with_unknown_prompt_returns_invalid_params() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":11,"method":"prompts/get",
            "params":{"name":"missing","arguments":{}}
        }"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::INVALID_PARAMS);
    }

    #[test]
    fn prompts_get_with_non_object_arguments_returns_invalid_params() {
        let reg = make_registry();
        let prompts = cqels_prompt_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":12,"method":"prompts/get",
            "params":{"name":"store_knowledge","arguments":["not","an","object"]}
        }"#;
        let resp = handle_request_with_prompts(&reg, &prompts, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::INVALID_PARAMS);
    }

    #[test]
    fn unknown_method_returns_method_not_found() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","id":6,"method":"some/unknown"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::METHOD_NOT_FOUND);
    }

    #[test]
    fn notification_without_id_produces_no_response() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"2.0","method":"ping"}"#;
        assert!(handle_request(&reg, req).is_none());
    }

    #[test]
    fn malformed_json_returns_parse_error() {
        let reg = make_registry();
        let resp = handle_request(&reg, "not json at all").expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::PARSE_ERROR);
    }

    #[test]
    fn invalid_jsonrpc_version_returns_invalid_request() {
        let reg = make_registry();
        let req = r#"{"jsonrpc":"1.0","id":7,"method":"ping"}"#;
        let resp = handle_request(&reg, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["error"]["code"], error_code::INVALID_REQUEST);
    }

    #[test]
    fn initialize_with_resources_advertises_resource_capability() {
        let reg = make_registry();
        let resources = cqels_resource_registry();
        let req = r#"{"jsonrpc":"2.0","id":8,"method":"initialize"}"#;
        let resp = handle_request_with_resources(&reg, &resources, req).expect("response");
        let value = parse_response(&resp);
        assert_eq!(value["result"]["capabilities"]["tools"], json!({}));
        assert_eq!(
            value["result"]["capabilities"]["resources"],
            json!({"subscribe": true, "listChanged": false})
        );
    }

    #[test]
    fn resources_list_and_templates_list_expose_alpha10_surface() {
        let reg = make_registry();
        let resources = cqels_resource_registry();

        let list = handle_request_with_resources(
            &reg,
            &resources,
            r#"{"jsonrpc":"2.0","id":9,"method":"resources/list"}"#,
        )
        .expect("response");
        let value = parse_response(&list);
        let resources_out = value["result"]["resources"].as_array().unwrap();
        assert_eq!(resources_out.len(), 8);
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_KG_STATS));
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_KG_NAMESPACES));
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_ENGINE_STATUS));
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_REASONING));
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_DOC_CQELSQL));
        assert!(resources_out
            .iter()
            .any(|resource| resource["uri"] == RESOURCE_DOC_CEP));

        let templates = handle_request_with_resources(
            &reg,
            &resources,
            r#"{"jsonrpc":"2.0","id":10,"method":"resources/templates/list"}"#,
        )
        .expect("response");
        let value = parse_response(&templates);
        assert_eq!(
            value["result"]["resourceTemplates"][0]["uriTemplate"],
            RESOURCE_QUERY_RESULTS_TEMPLATE
        );
    }

    #[test]
    fn resources_read_returns_text_content_for_static_resource() {
        let reg = make_registry();
        let resources = cqels_resource_registry();
        let req = r#"{
            "jsonrpc":"2.0","id":11,"method":"resources/read",
            "params":{"uri":"cqels://reasoning/capabilities"}
        }"#;

        let resp = handle_request_with_resources(&reg, &resources, req).expect("response");
        let value = parse_response(&resp);
        let content = &value["result"]["contents"][0];
        assert_eq!(content["uri"], RESOURCE_REASONING);
        assert_eq!(content["mimeType"], "application/json");
        let body: JsonValue = serde_json::from_str(content["text"].as_str().unwrap()).unwrap();
        assert!(body["profiles"].as_array().unwrap().len() >= 5);
    }

    #[test]
    fn resources_read_drains_query_results_template_uri() {
        let reg = make_registry();
        let resources = cqels_resource_registry();
        let uri = query_results_uri("q-read");
        let req = json!({
            "jsonrpc": "2.0",
            "id": 12,
            "method": "resources/read",
            "params": { "uri": uri },
        })
        .to_string();

        let resp = handle_request_with_resources(&reg, &resources, &req).expect("response");
        let value = parse_response(&resp);
        let body: JsonValue =
            serde_json::from_str(value["result"]["contents"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(body["queryId"], "q-read");
        assert!(body["results"].as_array().unwrap().is_empty());
    }

    #[test]
    fn resources_subscribe_acknowledges_known_uri_and_rejects_unknown() {
        let reg = make_registry();
        let resources = cqels_resource_registry();

        let known = handle_request_with_resources(
            &reg,
            &resources,
            r#"{
                "jsonrpc":"2.0","id":13,"method":"resources/subscribe",
                "params":{"uri":"cqels://kg/stats"}
            }"#,
        )
        .expect("response");
        let value = parse_response(&known);
        assert_eq!(value["result"], json!({}));

        let unknown = handle_request_with_resources(
            &reg,
            &resources,
            r#"{
                "jsonrpc":"2.0","id":14,"method":"resources/subscribe",
                "params":{"uri":"cqels://missing"}
            }"#,
        )
        .expect("response");
        let value = parse_response(&unknown);
        assert_eq!(value["error"]["code"], error_code::INVALID_PARAMS);
    }

    #[test]
    fn stdio_loop_processes_multiple_requests_and_skips_blanks() {
        let reg = make_registry();
        let input = b"\
{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
\n\
{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"initialize\"}\n\
{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}\n\
{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\"}\n\
";
        let mut output: Vec<u8> = Vec::new();
        run_stdio(&reg, &input[..], &mut output).expect("run stdio");
        let text = String::from_utf8(output).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        // Three lines back: ping reply, initialize reply, tools/list reply.
        // The middle notification produced no reply.
        assert_eq!(lines.len(), 3);
        let r1: JsonValue = serde_json::from_str(lines[0]).unwrap();
        let r2: JsonValue = serde_json::from_str(lines[1]).unwrap();
        let r3: JsonValue = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(r1["id"], 1);
        assert_eq!(r2["id"], 2);
        assert_eq!(r3["id"], 3);
        assert!(r3["result"]["tools"].is_array());
    }

    #[test]
    fn stdio_loop_with_resources_processes_resource_reads() {
        let reg = make_registry();
        let resources = cqels_resource_registry();
        let input = b"\
{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n\
{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"resources/list\"}\n\
{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/read\",\"params\":{\"uri\":\"cqels://kg/stats\"}}\n\
";
        let mut output: Vec<u8> = Vec::new();
        run_stdio_with_resources(&reg, &resources, &input[..], &mut output).expect("run stdio");
        let text = String::from_utf8(output).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let init: JsonValue = serde_json::from_str(lines[0]).unwrap();
        let list: JsonValue = serde_json::from_str(lines[1]).unwrap();
        let read: JsonValue = serde_json::from_str(lines[2]).unwrap();
        assert!(init["result"]["capabilities"]["resources"].is_object());
        assert_eq!(list["result"]["resources"].as_array().unwrap().len(), 8);
        assert_eq!(read["result"]["contents"][0]["uri"], RESOURCE_KG_STATS);
    }
}
