//! Core tool trait and value types.
//!
//! Mirrors MCP's `tools/call` semantics: tools are uniquely named,
//! self-describing (`description`, `input_schema`), and invokable with a
//! JSON arguments object, returning either structured content or an
//! error.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// One MCP tool: a self-describing, invokable function.
///
/// Implementors should be `Send + Sync` so a single registry can be
/// shared across worker tasks.
pub trait McpTool: Send + Sync {
    /// Unique tool identifier — used in MCP `tools/call` payloads.
    fn name(&self) -> &str;

    /// Human-readable description displayed by MCP clients.
    fn description(&self) -> &str;

    /// JSON Schema describing the tool's `arguments` shape.
    fn input_schema(&self) -> ToolInputSchema;

    /// Invokes the tool. `arguments` is the raw JSON payload sent by the
    /// MCP client (typically an object whose keys match
    /// `input_schema.properties`).
    fn call(&self, invocation: &ToolInvocation) -> ToolResult;
}

/// JSON-Schema-shaped contract for a tool's arguments.
///
/// Modeled as a flat object schema: a set of named properties plus a
/// list of required names. Sufficient for the simple validation MCP
/// clients perform; matches Java's `ToolSchemas` flat layout.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolInputSchema {
    #[serde(rename = "type")]
    pub type_field: String,
    pub properties: serde_json::Map<String, JsonValue>,
    pub required: Vec<String>,
}

impl ToolInputSchema {
    pub fn object() -> Self {
        Self {
            type_field: "object".into(),
            properties: serde_json::Map::new(),
            required: Vec::new(),
        }
    }

    pub fn with_property(mut self, name: &str, schema: JsonValue) -> Self {
        self.properties.insert(name.into(), schema);
        self
    }

    pub fn require(mut self, name: &str) -> Self {
        self.required.push(name.into());
        self
    }
}

/// Arguments passed to a tool invocation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ToolInvocation {
    pub arguments: serde_json::Map<String, JsonValue>,
}

impl ToolInvocation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_arg(mut self, key: &str, value: JsonValue) -> Self {
        self.arguments.insert(key.into(), value);
        self
    }

    /// Returns the named argument or `None`.
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.arguments.get(key)
    }

    /// Returns the named argument as a string, or `None`.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(|v| v.as_str())
    }
}

/// Result of invoking an MCP tool.
///
/// `content` is the structured JSON the MCP client receives; `is_error`
/// surfaces tool-level failures distinct from transport errors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: JsonValue,
    pub is_error: bool,
}

impl ToolResult {
    pub fn success(content: JsonValue) -> Self {
        Self {
            content,
            is_error: false,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            content: serde_json::json!({ "message": message.into() }),
            is_error: true,
        }
    }
}
