//! JSON-RPC 2.0 transport for the MCP tool surface.
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
//! - `ping` — connection liveness check.
//!
//! Notifications (no `id` field) are accepted but produce no response.
//! Any unknown method returns a JSON-RPC error with code `-32601`.

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::registry::ToolRegistry;
use crate::tool::ToolInvocation;

/// MCP protocol version advertised by [`handle_request`].
pub const PROTOCOL_VERSION: &str = "2025-03-26";

/// Server identity returned by `initialize`.
pub const SERVER_NAME: &str = "cqels-mcp";

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
    let result = dispatch(registry, &request);

    match (id, result) {
        (None, _) => None, // Notification — no response.
        (Some(id), Ok(value)) => Some(serialize_success(id, value)),
        (Some(id), Err((code, message, data))) => Some(serialize_error(id, code, message, data)),
    }
}

fn dispatch(
    registry: &ToolRegistry,
    request: &JsonRpcRequest,
) -> Result<JsonValue, (i32, String, Option<JsonValue>)> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "tools": {} },
        })),
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
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(response) = handle_request(registry, trimmed) {
            writeln!(writer, "{response}")?;
            writer.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_query_tool, query_tool, reasoning_profiles_tool, shacl_capabilities_tool};

    fn make_registry() -> ToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.install(parse_query_tool());
        reg.install(query_tool());
        reg.install(reasoning_profiles_tool());
        reg.install(shacl_capabilities_tool());
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
        assert_eq!(tools.len(), 4);
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
}
