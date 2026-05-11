//! Reference MCP tool implementations.
//!
//! Two foundational tools that exercise the full pipeline without
//! requiring a running engine: `parse_query` (lexes/parses CqelsQL) and
//! `query` (parses + compiles to a definition). Full registration of
//! stream queries against a live engine is a follow-up that requires
//! wiring `cqels_engine::CqelsEngine` into the tool handler.

use cqels_core::parser::CqelsQlParser;
use serde_json::json;

use crate::tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};

// ─── parse_query ─────────────────────────────────────────────────────

/// Returns a stateless [`ParseQueryTool`] that lexes and parses a
/// CqelsQL query string, returning the AST as JSON.
pub fn parse_query_tool() -> ParseQueryTool {
    ParseQueryTool
}

pub struct ParseQueryTool;

impl McpTool for ParseQueryTool {
    fn name(&self) -> &str {
        "parse_query"
    }

    fn description(&self) -> &str {
        "Parse a CqelsQL query string into its AST representation. Returns \
         the structured query definition, including streams, pattern groups, \
         filters, and any SEQ() CEP constraint."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "Raw CqelsQL query text"
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query) = invocation.get_str("query") else {
            return ToolResult::error("missing `query` argument");
        };
        match CqelsQlParser::parse(query) {
            Ok(def) => {
                let summary = json!({
                    "query_type": format!("{:?}", def.query_type),
                    "streams": def.streams.iter().map(|s| &s.name).collect::<Vec<_>>(),
                    "select_count": def.select_elements.len(),
                    "pattern_groups": def.pattern_groups.len(),
                    "has_seq_constraint": def.seq_constraint.is_some(),
                    "has_group_by": def.has_group_by(),
                    "has_order_by": def.has_order_by(),
                    "has_limit": def.has_limit(),
                });
                ToolResult::success(summary)
            }
            Err(e) => ToolResult::error(format!("parse error: {e}")),
        }
    }
}

// ─── query ───────────────────────────────────────────────────────────

/// Returns a [`QueryTool`] that parses and validates a CqelsQL query
/// without executing it. (Live execution requires wiring an engine; see
/// the crate-level docs.)
pub fn query_tool() -> QueryTool {
    QueryTool
}

pub struct QueryTool;

impl McpTool for QueryTool {
    fn name(&self) -> &str {
        "query"
    }

    fn description(&self) -> &str {
        "Validate and analyze a CqelsQL query. Returns metadata about the \
         compiled plan (streams, windows, filters, SEQ constraints). \
         Execution against a live engine is configured separately."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "CqelsQL query text to compile"
                }),
            )
            .with_property(
                "dry_run",
                json!({
                    "type": "boolean",
                    "description": "If true, only parse and validate; do not execute. Defaults to true.",
                    "default": true
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query) = invocation.get_str("query") else {
            return ToolResult::error("missing `query` argument");
        };
        let dry_run = invocation
            .get("dry_run")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if !dry_run {
            return ToolResult::error(
                "live execution requires a running engine — wire \
                 cqels_engine::CqelsEngine into the QueryTool to enable",
            );
        }

        match CqelsQlParser::parse(query) {
            Ok(def) => ToolResult::success(json!({
                "ok": true,
                "dry_run": true,
                "streams": def.streams.iter().map(|s| &s.name).collect::<Vec<_>>(),
                "pattern_groups": def.pattern_groups.len(),
                "has_seq_constraint": def.seq_constraint.is_some(),
            })),
            Err(e) => ToolResult::error(format!("parse error: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ToolRegistry;

    fn sample_query() -> &'static str {
        r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [RANGE 10s]
            WHERE { ?sensor <http://ex.org/temp> ?temp . }
        "#
    }

    fn run(name: &str, args: ToolInvocation) -> ToolResult {
        let mut reg = ToolRegistry::new();
        reg.install(parse_query_tool());
        reg.install(query_tool());
        reg.call(name, &args).expect("dispatch")
    }

    #[test]
    fn parse_query_returns_ast_summary_for_valid_input() {
        let res = run(
            "parse_query",
            ToolInvocation::new().with_arg("query", serde_json::json!(sample_query())),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["query_type"], "Select");
        assert_eq!(res.content["streams"][0], "sensors");
        assert_eq!(res.content["has_seq_constraint"], false);
    }

    #[test]
    fn parse_query_reports_error_for_invalid_input() {
        let res = run(
            "parse_query",
            ToolInvocation::new().with_arg("query", serde_json::json!("THIS IS NOT VALID CQELS")),
        );
        assert!(res.is_error);
    }

    #[test]
    fn parse_query_reports_seq_constraint_when_present() {
        let cep_query = r#"
            SELECT ?a ?b
            FROM STREAM events [RANGE 5s]
            WHERE {
                ?a a <http://ex.org/A> .
                ?b a <http://ex.org/B> .
                FILTER(SEQ(?a; ?b))
            }
        "#;
        let res = run(
            "parse_query",
            ToolInvocation::new().with_arg("query", serde_json::json!(cep_query)),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["has_seq_constraint"], true);
    }

    #[test]
    fn query_tool_dry_run_validates_without_executing() {
        let res = run(
            "query",
            ToolInvocation::new().with_arg("query", serde_json::json!(sample_query())),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["ok"], true);
        assert_eq!(res.content["dry_run"], true);
    }

    #[test]
    fn query_tool_rejects_live_execution_for_now() {
        let res = run(
            "query",
            ToolInvocation::new()
                .with_arg("query", serde_json::json!(sample_query()))
                .with_arg("dry_run", serde_json::json!(false)),
        );
        assert!(res.is_error);
    }

    #[test]
    fn registered_tools_advertise_correct_schema() {
        let mut reg = ToolRegistry::new();
        reg.install(parse_query_tool());
        reg.install(query_tool());
        let parse = reg.get("parse_query").expect("parse_query installed");
        let schema = parse.input_schema();
        assert_eq!(schema.type_field, "object");
        assert!(schema.properties.contains_key("query"));
        assert!(schema.required.contains(&"query".to_string()));

        let query = reg.get("query").expect("query installed");
        let q_schema = query.input_schema();
        assert!(q_schema.properties.contains_key("query"));
        assert!(q_schema.properties.contains_key("dry_run"));
        assert!(q_schema.required.contains(&"query".to_string()));
    }
}
