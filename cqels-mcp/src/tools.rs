//! Reference MCP tool implementations.
//!
//! Two foundational tools that exercise the full pipeline without
//! requiring a running engine: `parse_query` (lexes/parses CqelsQL) and
//! `query` (parses + compiles to a definition). Full registration of
//! stream queries against a live engine is a follow-up that requires
//! wiring `cqels_engine::CqelsEngine` into the tool handler.

use cqels_core::parser::CqelsQlParser;
use cqels_reasoning::ReasoningProfile;
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

// ─── reasoning_profiles ──────────────────────────────────────────────

/// Returns a stateless [`ReasoningProfilesTool`] that lists or describes
/// available reasoning profiles. Mirrors a subset of Java's `reason`
/// tool — describing capabilities without performing live inference.
/// Live `reason()` execution against a working memory is a follow-up.
pub fn reasoning_profiles_tool() -> ReasoningProfilesTool {
    ReasoningProfilesTool
}

pub struct ReasoningProfilesTool;

fn all_profiles() -> [ReasoningProfile; 7] {
    [
        ReasoningProfile::None,
        ReasoningProfile::Rdfs,
        ReasoningProfile::RdfsFull,
        ReasoningProfile::OwlLite,
        ReasoningProfile::OwlQl,
        ReasoningProfile::Owl2El,
        ReasoningProfile::Owl2Rl,
    ]
}

fn profile_summary(profile: ReasoningProfile) -> serde_json::Value {
    let rules = profile.rules();
    json!({
        "name": profile.name(),
        "description": profile.description(),
        "rule_count": rules.len(),
        "requires_recursive_inference": profile.requires_recursive_inference(),
    })
}

impl McpTool for ReasoningProfilesTool {
    fn name(&self) -> &str {
        "reasoning_profiles"
    }

    fn description(&self) -> &str {
        "List supported reasoning profiles (RDFS, OWL-QL, OWL2-EL, OWL2-RL, \
         etc.) with their rule counts and capabilities, or describe one \
         specific profile in detail. Returns metadata only — live inference \
         against a working memory is a separate operation."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object().with_property(
            "profile",
            json!({
                "type": "string",
                "description": "Optional profile name (e.g., 'RDFS', 'OWL-QL', 'OWL2-RL'). If omitted, lists all profiles.",
            }),
        )
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        match invocation.get_str("profile") {
            None | Some("") => {
                let profiles: Vec<_> = all_profiles().iter().map(|p| profile_summary(*p)).collect();
                ToolResult::success(json!({ "profiles": profiles }))
            }
            Some(name) => match resolve_profile(name) {
                Some(p) => ToolResult::success(profile_summary(p)),
                None => ToolResult::error(format!(
                    "unknown reasoning profile '{name}'; try one of: NONE, RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL"
                )),
            },
        }
    }
}

fn resolve_profile(name: &str) -> Option<ReasoningProfile> {
    let normalized = name.to_uppercase().replace('_', "-");
    all_profiles()
        .into_iter()
        .find(|p| p.name().to_uppercase() == normalized)
}

// ─── shacl_capabilities ──────────────────────────────────────────────

/// Returns a [`ShaclCapabilitiesTool`] that describes the SHACL features
/// supported by `cqels-shacl`. Useful for LLM agents that need to know
/// which constraints they can author against the engine.
pub fn shacl_capabilities_tool() -> ShaclCapabilitiesTool {
    ShaclCapabilitiesTool
}

pub struct ShaclCapabilitiesTool;

impl McpTool for ShaclCapabilitiesTool {
    fn name(&self) -> &str {
        "shacl_capabilities"
    }

    fn description(&self) -> &str {
        "Describe the SHACL features supported by the engine: shape kinds, \
         constraint types, severity levels, and the repair-candidate \
         pipeline. Returns metadata only."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
    }

    fn call(&self, _invocation: &ToolInvocation) -> ToolResult {
        ToolResult::success(json!({
            "shape_kinds": ["NodeShape", "PropertyShape"],
            "node_kinds": ["IRI", "BlankNode", "Literal", "BlankNodeOrIri", "BlankNodeOrLiteral", "IriOrLiteral"],
            "supported_constraints": [
                "sh:targetClass",
                "sh:path",
                "sh:datatype",
                "sh:minCount",
                "sh:maxCount",
                "sh:nodeKind",
                "sh:class",
            ],
            "severities": ["Info", "Warning", "Violation"],
            "features": {
                "repair_candidates": true,
                "asp_compilation": true,
                "continuous_validation": true,
            }
        }))
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
        reg.install(reasoning_profiles_tool());
        reg.install(shacl_capabilities_tool());
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

    // ─── reasoning_profiles tests ────────────────────────────────────

    #[test]
    fn reasoning_profiles_lists_all_when_no_argument() {
        let res = run("reasoning_profiles", ToolInvocation::new());
        assert!(!res.is_error);
        let profiles = res.content["profiles"].as_array().expect("array");
        // Currently 7 profiles ported (None, RDFS, RDFS-Full, OWL-Lite,
        // OWL-QL, OWL2-EL, OWL2-RL).
        assert_eq!(profiles.len(), 7);
        // Every entry must include name + rule_count.
        for p in profiles {
            assert!(p["name"].is_string());
            assert!(p["rule_count"].is_number());
        }
    }

    #[test]
    fn reasoning_profiles_describes_named_profile() {
        let res = run(
            "reasoning_profiles",
            ToolInvocation::new().with_arg("profile", serde_json::json!("RDFS")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["name"], "RDFS");
        assert!(res.content["rule_count"].as_u64().unwrap() > 0);
    }

    #[test]
    fn reasoning_profiles_handles_case_insensitive_name() {
        let res = run(
            "reasoning_profiles",
            ToolInvocation::new().with_arg("profile", serde_json::json!("owl2-rl")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["name"], "OWL2-RL");
    }

    #[test]
    fn reasoning_profiles_rejects_unknown_profile_with_hint() {
        let res = run(
            "reasoning_profiles",
            ToolInvocation::new().with_arg("profile", serde_json::json!("nope")),
        );
        assert!(res.is_error);
        let msg = res.content["message"].as_str().unwrap();
        assert!(msg.contains("RDFS") && msg.contains("OWL2-RL"));
    }

    // ─── shacl_capabilities tests ────────────────────────────────────

    #[test]
    fn shacl_capabilities_returns_supported_constraints() {
        let res = run("shacl_capabilities", ToolInvocation::new());
        assert!(!res.is_error);
        let constraints = res.content["supported_constraints"]
            .as_array()
            .expect("array");
        // Spot-check a handful of the constraints we expose.
        let names: Vec<&str> = constraints.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(names.iter().any(|c| c == &"sh:targetClass"));
        assert!(names.iter().any(|c| c == &"sh:minCount"));
        assert_eq!(
            res.content["features"]["repair_candidates"],
            serde_json::Value::Bool(true),
            "repair candidates flag"
        );
    }
}
