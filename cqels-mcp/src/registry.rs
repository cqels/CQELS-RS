//! Tool registry for MCP `tools/list` and `tools/call` dispatch.
//!
//! A [`ToolRegistry`] holds a name-keyed map of [`crate::McpTool`]
//! implementations. The bound transport (rmcp / custom JSON-RPC / SSE)
//! calls `list()` to advertise the tool surface and `call(name, args)`
//! to dispatch an invocation.

use std::collections::HashMap;
use std::sync::Arc;

use crate::tool::{McpTool, ToolInvocation, ToolResult};

/// Errors that can occur during tool dispatch.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpError {
    #[error("tool '{0}' not found")]
    UnknownTool(String),
    #[error("tool '{name}' rejected arguments: {message}")]
    InvalidArguments { name: String, message: String },
}

/// Registry of installed tools.
#[derive(Default)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn McpTool>>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs a tool. Replaces any existing entry with the same name.
    pub fn install<T: McpTool + 'static>(&mut self, tool: T) {
        self.tools.insert(tool.name().to_string(), Arc::new(tool));
    }

    /// Number of installed tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Names of all installed tools, sorted lexicographically.
    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tools.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns a reference to the tool by name, if installed.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn McpTool>> {
        self.tools.get(name)
    }

    /// Dispatches a call to the named tool.
    pub fn call(&self, name: &str, invocation: &ToolInvocation) -> Result<ToolResult, McpError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| McpError::UnknownTool(name.to_string()))?;
        Ok(tool.call(invocation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::ToolInputSchema;

    struct EchoTool;

    impl McpTool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }

        fn description(&self) -> &str {
            "Returns the `message` argument verbatim."
        }

        fn input_schema(&self) -> ToolInputSchema {
            ToolInputSchema::object()
                .with_property("message", serde_json::json!({ "type": "string" }))
                .require("message")
        }

        fn call(&self, invocation: &ToolInvocation) -> ToolResult {
            match invocation.get_str("message") {
                Some(msg) => ToolResult::success(serde_json::json!({ "echo": msg })),
                None => ToolResult::error("missing `message` argument"),
            }
        }
    }

    #[test]
    fn install_and_list_orders_tools_alphabetically() {
        let mut reg = ToolRegistry::new();
        reg.install(EchoTool);
        assert_eq!(reg.list(), vec!["echo".to_string()]);
    }

    #[test]
    fn unknown_tool_returns_error() {
        let reg = ToolRegistry::new();
        let result = reg.call("missing", &ToolInvocation::new());
        assert!(matches!(result, Err(McpError::UnknownTool(_))));
    }

    #[test]
    fn call_dispatches_to_installed_tool() {
        let mut reg = ToolRegistry::new();
        reg.install(EchoTool);
        let inv = ToolInvocation::new().with_arg("message", serde_json::json!("hi"));
        let result = reg.call("echo", &inv).expect("dispatch");
        assert!(!result.is_error);
        assert_eq!(result.content["echo"], "hi");
    }

    #[test]
    fn tool_can_surface_argument_errors() {
        let mut reg = ToolRegistry::new();
        reg.install(EchoTool);
        let result = reg.call("echo", &ToolInvocation::new()).expect("dispatch");
        assert!(
            result.is_error,
            "missing arg should produce tool-level error"
        );
    }
}
