//! Reference MCP tool implementations.
//!
//! Stateless tools that exercise the parser, compiler, reasoning, and
//! SHACL surfaces without requiring a running engine:
//! `parse_query` (lex/parse only), `query` (parse + dry-run validate),
//! `analyze_query` (full compile + planner-decision report),
//! `reasoning_profiles`, `shacl_capabilities`. Memory tools
//! (`store_memory`/`recall_memory`/`forget_memory`) are backed by
//! pluggable [`MemoryStore`] implementations.
//!
//! Full registration of stream queries against a live engine is a
//! follow-up that requires wiring `cqels_engine::CqelsEngine` into the
//! tool handler.

use std::sync::Arc;
use std::time::Duration;

use cqels_core::compiler::CqelsQueryCompiler;
use cqels_core::parser::CqelsQlParser;
use cqels_core::stream::RdfStreamElement;
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};
use cqels_reasoning::{ReasoningConfig, ReasoningProfile, ReteNetwork};
use serde_json::json;

use crate::memory::{MemoryFact, MemoryStore};
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

// ─── analyze_query ───────────────────────────────────────────────────

/// Returns a stateless [`AnalyzeQueryTool`] that runs the full CqelsQL
/// compile pipeline and reports planning decisions: pattern groups,
/// detected self-join optimization hints, fast-path eligibility, plus
/// the bound projection variables.
///
/// This complements [`parse_query_tool`] (AST only) and
/// [`query_tool`] (light validation). Use it when you want the
/// compiler's planner-level view: "will my query trigger the
/// indexed self-join fast path? which variables are projected?".
pub fn analyze_query_tool() -> AnalyzeQueryTool {
    AnalyzeQueryTool
}

pub struct AnalyzeQueryTool;

impl McpTool for AnalyzeQueryTool {
    fn name(&self) -> &str {
        "analyze_query"
    }

    fn description(&self) -> &str {
        "Compile a CqelsQL query and report planner decisions: stream \
         sources, pattern groups, detected self-join hints (and whether \
         the fast path applies), projection variables, GROUP BY / ORDER \
         BY presence, and FILTER counts. Does not execute the query."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "CqelsQL query text to compile and analyze"
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query) = invocation.get_str("query") else {
            return ToolResult::error("missing `query` argument");
        };
        let definition = match CqelsQlParser::parse(query) {
            Ok(def) => def,
            Err(e) => return ToolResult::error(format!("parse error: {e}")),
        };
        let compiled = match CqelsQueryCompiler::compile(query, definition) {
            Ok(c) => c,
            Err(e) => return ToolResult::error(format!("compile error: {e}")),
        };
        let def = compiled.definition();
        let hints: Vec<_> = compiled
            .self_join_hints()
            .iter()
            .map(|h| {
                json!({
                    "source": h.source,
                    "shared_variables": h.join_keys,
                    "pattern_indices": [h.pattern_indices.0, h.pattern_indices.1],
                })
            })
            .collect();
        let stream_sources: Vec<_> = def.streams.iter().map(|s| &s.name).collect();
        let select_vars: Vec<&str> = compiled.select_vars().iter().map(String::as_str).collect();

        ToolResult::success(json!({
            "query_type": format!("{:?}", def.query_type),
            "streams": stream_sources,
            "pattern_groups": def.pattern_groups.len(),
            "select_variables": select_vars,
            "self_join_hints": hints,
            "has_self_join_optimization": compiled.has_self_join_optimization(),
            "has_group_by": def.has_group_by(),
            "has_order_by": def.has_order_by(),
            "has_limit": def.has_limit(),
            "distinct": def.distinct,
            "has_seq_constraint": def.seq_constraint.is_some(),
        }))
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

// ─── reason ──────────────────────────────────────────────────────────

/// Returns a stateless [`ReasonTool`] that runs a one-shot RETE
/// inference over a small triple set using a built-in reasoning
/// profile (RDFS, OWL-RL, etc.).
///
/// Inputs:
/// - `profile`: profile name (`RDFS`, `RDFS-Full`, `OWL-Lite`,
///   `OWL-QL`, `OWL2-EL`, `OWL2-RL`). Required.
/// - `triples`: array of `{s, p, o}` objects. `s` and `p` must be IRIs;
///   `o` is treated as an IRI if it parses as a URL (`http(s)://...`)
///   or starts with `<` / contains `:`, otherwise as a literal. Required.
/// - `timestamp` (optional): integer ms timestamp applied to every
///   asserted fact. Defaults to `0`.
///
/// Returns the inferred (closure - asserted) triples plus rule
/// provenance per inference.
///
/// This is a *one-shot* tool: each invocation builds a fresh RETE
/// network, feeds the asserted triples through `process_element`, and
/// emits whatever new facts the rules deduce. It does **not**
/// preserve state between calls — for that, register the inference
/// against a live engine.
pub fn reason_tool() -> ReasonTool {
    ReasonTool
}

pub struct ReasonTool;

impl McpTool for ReasonTool {
    fn name(&self) -> &str {
        "reason"
    }

    fn description(&self) -> &str {
        "Run one-shot RETE inference over a triple set using a built-in \
         reasoning profile (RDFS / OWL-Lite / OWL-QL / OWL2-EL / OWL2-RL). \
         Inputs: `profile` (string) + `triples` (array of `{s, p, o}` \
         objects). Returns inferred triples with rule provenance. \
         Stateless — no engine wiring required."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "profile",
                json!({
                    "type": "string",
                    "description": "Reasoning profile name (RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL)."
                }),
            )
            .with_property(
                "triples",
                json!({
                    "type": "array",
                    "description": "Asserted triples to reason over. Each item is { s, p, o }.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "s": { "type": "string" },
                            "p": { "type": "string" },
                            "o": { "type": "string" }
                        },
                        "required": ["s", "p", "o"]
                    }
                }),
            )
            .with_property(
                "timestamp",
                json!({
                    "type": "integer",
                    "description": "Timestamp (ms) applied to each asserted fact. Defaults to 0.",
                    "default": 0
                }),
            )
            .require("profile")
            .require("triples")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(profile_name) = invocation.get_str("profile") else {
            return ToolResult::error("missing `profile` argument");
        };
        let Some(profile) = resolve_profile(profile_name) else {
            return ToolResult::error(format!(
                "unknown reasoning profile '{profile_name}'; try one of: \
                 RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL"
            ));
        };
        let Some(triples) = invocation.get("triples").and_then(|v| v.as_array()) else {
            return ToolResult::error("missing or non-array `triples` argument");
        };
        let timestamp = invocation
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let mut elements = Vec::with_capacity(triples.len());
        for (i, t) in triples.iter().enumerate() {
            let (s, p, o) = match (
                t.get("s").and_then(|v| v.as_str()),
                t.get("p").and_then(|v| v.as_str()),
                t.get("o").and_then(|v| v.as_str()),
            ) {
                (Some(s), Some(p), Some(o)) => (s, p, o),
                _ => {
                    return ToolResult::error(format!(
                        "triple #{i} must be {{ s, p, o }} of strings"
                    ));
                }
            };
            let stmt = Statement::new(parse_term(s), IriTerm::new(p), parse_term(o));
            elements.push(RdfStreamElement::new(stmt, timestamp));
        }

        // Default window covers a year — large enough that asserted
        // facts persist through the closure-computation pass.
        let config = ReasoningConfig::builder()
            .rule_set(profile.rule_set())
            .default_window(Duration::from_secs(60 * 60 * 24 * 365))
            .enable_recursive_inference(profile.requires_recursive_inference())
            .build();
        let mut network = ReteNetwork::compile(config);

        let mut inferred = Vec::new();
        for elem in &elements {
            for inf in network.process_element(elem) {
                let stmt = &inf.statement;
                inferred.push(json!({
                    "s": term_to_string(&stmt.subject),
                    "p": stmt.predicate.as_str(),
                    "o": term_to_string(&stmt.object),
                    "rule_id": inf.inferred_by,
                }));
            }
        }

        ToolResult::success(json!({
            "profile": profile.name(),
            "input_count": elements.len(),
            "inferred_count": inferred.len(),
            "inferred": inferred,
        }))
    }
}

/// Heuristic: treat strings that look like IRIs as IRIs, everything
/// else as plain literals. Brackets `<...>` are stripped. This is the
/// same heuristic used by `cqels-shacl`'s reference parser.
fn parse_term(s: &str) -> Term {
    let trimmed = s.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') && trimmed.len() >= 2 {
        return Term::Iri(IriTerm::new(&trimmed[1..trimmed.len() - 1]));
    }
    let looks_like_iri = trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("urn:")
        || trimmed.starts_with("file://");
    if looks_like_iri {
        Term::Iri(IriTerm::new(trimmed))
    } else {
        Term::Literal(LiteralTerm::new(trimmed))
    }
}

fn term_to_string(t: &Term) -> String {
    match t {
        Term::Iri(i) => i.as_str().to_string(),
        Term::Literal(l) => l.value().to_string(),
        other => other.to_string(),
    }
}

// ─── memory tools ────────────────────────────────────────────────────

const DEFAULT_NAMESPACE: &str = "default";

fn namespace_from(invocation: &ToolInvocation) -> String {
    invocation
        .get_str("namespace")
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string()
}

/// Constructs a `store_memory` tool backed by the supplied
/// [`MemoryStore`].
pub fn store_memory_tool(store: Arc<dyn MemoryStore>) -> StoreMemoryTool {
    StoreMemoryTool { store }
}

pub struct StoreMemoryTool {
    store: Arc<dyn MemoryStore>,
}

impl McpTool for StoreMemoryTool {
    fn name(&self) -> &str {
        "store_memory"
    }

    fn description(&self) -> &str {
        "Persist a fact to long-term memory keyed by (namespace, id). \
         Replaces any existing fact at the same key. Returns the stored \
         fact's metadata."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property("id", json!({
                "type": "string",
                "description": "Unique identifier for this fact within the namespace.",
            }))
            .with_property("content", json!({
                "type": "string",
                "description": "Raw content of the memory (free-form text, JSON, Turtle, etc.).",
            }))
            .with_property("namespace", json!({
                "type": "string",
                "description": "Logical bucket isolating memories (default: \"default\").",
                "default": DEFAULT_NAMESPACE,
            }))
            .require("id")
            .require("content")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(id) = invocation.get_str("id").map(str::to_string) else {
            return ToolResult::error("missing `id` argument");
        };
        let Some(content) = invocation.get_str("content").map(str::to_string) else {
            return ToolResult::error("missing `content` argument");
        };
        let namespace = namespace_from(invocation);
        let fact = MemoryFact::new(namespace.clone(), id.clone(), content);
        match self.store.store(fact.clone()) {
            Ok(()) => ToolResult::success(json!({
                "ok": true,
                "namespace": fact.namespace,
                "id": fact.id,
                "created_at_ms": fact.created_at_ms,
            })),
            Err(e) => ToolResult::error(format!("store failed: {e}")),
        }
    }
}

/// Constructs a `recall_memory` tool backed by the supplied
/// [`MemoryStore`].
pub fn recall_memory_tool(store: Arc<dyn MemoryStore>) -> RecallMemoryTool {
    RecallMemoryTool { store }
}

pub struct RecallMemoryTool {
    store: Arc<dyn MemoryStore>,
}

impl McpTool for RecallMemoryTool {
    fn name(&self) -> &str {
        "recall_memory"
    }

    fn description(&self) -> &str {
        "Retrieve memories from a namespace, optionally filtered to facts \
         whose content contains a query substring. Returns facts sorted \
         by id."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property("namespace", json!({
                "type": "string",
                "description": "Namespace to recall from. Defaults to \"default\".",
                "default": DEFAULT_NAMESPACE,
            }))
            .with_property("query", json!({
                "type": "string",
                "description": "Substring to match within fact content. Empty/missing returns all facts in the namespace.",
            }))
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let namespace = namespace_from(invocation);
        let query = invocation.get_str("query").unwrap_or("").to_string();
        match self.store.recall(&namespace, &query) {
            Ok(facts) => ToolResult::success(json!({
                "namespace": namespace,
                "query": query,
                "count": facts.len(),
                "facts": facts,
            })),
            Err(e) => ToolResult::error(format!("recall failed: {e}")),
        }
    }
}

/// Constructs a `forget_memory` tool backed by the supplied
/// [`MemoryStore`].
pub fn forget_memory_tool(store: Arc<dyn MemoryStore>) -> ForgetMemoryTool {
    ForgetMemoryTool { store }
}

pub struct ForgetMemoryTool {
    store: Arc<dyn MemoryStore>,
}

impl McpTool for ForgetMemoryTool {
    fn name(&self) -> &str {
        "forget_memory"
    }

    fn description(&self) -> &str {
        "Delete a memory fact by (namespace, id). Returns whether a \
         fact was actually removed."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "id",
                json!({
                    "type": "string",
                    "description": "Identifier of the memory to forget.",
                }),
            )
            .with_property(
                "namespace",
                json!({
                    "type": "string",
                    "description": "Namespace the memory lives in. Defaults to \"default\".",
                    "default": DEFAULT_NAMESPACE,
                }),
            )
            .require("id")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(id) = invocation.get_str("id").map(str::to_string) else {
            return ToolResult::error("missing `id` argument");
        };
        let namespace = namespace_from(invocation);
        match self.store.forget(&namespace, &id) {
            Ok(removed) => ToolResult::success(json!({
                "namespace": namespace,
                "id": id,
                "removed": removed,
            })),
            Err(e) => ToolResult::error(format!("forget failed: {e}")),
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
        reg.install(analyze_query_tool());
        reg.install(reasoning_profiles_tool());
        reg.install(shacl_capabilities_tool());
        reg.install(reason_tool());
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

    // ─── analyze_query tests ─────────────────────────────────────────

    #[test]
    fn analyze_query_reports_basic_plan_shape() {
        let res = run(
            "analyze_query",
            ToolInvocation::new().with_arg("query", serde_json::json!(sample_query())),
        );
        assert!(!res.is_error, "analyze_query should accept valid input");
        assert_eq!(res.content["query_type"], "Select");
        assert_eq!(res.content["streams"][0], "sensors");
        assert_eq!(res.content["has_self_join_optimization"], false);
        assert_eq!(
            res.content["self_join_hints"].as_array().unwrap().len(),
            0,
            "no self-join in single-pattern query"
        );
        // SELECT projects ?sensor + ?temp.
        let projected = res.content["select_variables"].as_array().unwrap();
        assert_eq!(projected.len(), 2);
    }

    #[test]
    fn analyze_query_detects_self_join_optimization() {
        let self_join_query = r#"
            SELECT ?driver ?passenger
            FROM STREAM rides [RANGE 10s]
            WHERE {
                STREAM rides { ?driver <http://ex.org/in> ?car . }
                STREAM rides { ?passenger <http://ex.org/in> ?car . }
            }
        "#;
        let res = run(
            "analyze_query",
            ToolInvocation::new().with_arg("query", serde_json::json!(self_join_query)),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["has_self_join_optimization"], true);
        let hints = res.content["self_join_hints"].as_array().unwrap();
        assert_eq!(hints.len(), 1, "one self-join hint detected");
        assert_eq!(hints[0]["source"], "rides");
        let shared = hints[0]["shared_variables"].as_array().unwrap();
        assert!(
            shared.iter().any(|v| v == "car"),
            "shared variable ?car appears in hint"
        );
    }

    #[test]
    fn analyze_query_reports_parse_error_clearly() {
        let res = run(
            "analyze_query",
            ToolInvocation::new().with_arg("query", serde_json::json!("THIS IS NOT VALID CQELS")),
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

    // ─── reason tool tests ───────────────────────────────────────────

    #[test]
    fn reason_runs_rdfs_subclass_inference() {
        // The classic RDFS test: assert `:Alice rdf:type :Person` and
        // `:Person rdfs:subClassOf :Animal`, expect the inferred
        // `:Alice rdf:type :Animal`.
        let triples = serde_json::json!([
            {
                "s": "http://ex.org/alice",
                "p": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "o": "http://ex.org/Person"
            },
            {
                "s": "http://ex.org/Person",
                "p": "http://www.w3.org/2000/01/rdf-schema#subClassOf",
                "o": "http://ex.org/Animal"
            }
        ]);
        let res = run(
            "reason",
            ToolInvocation::new()
                .with_arg("profile", serde_json::json!("RDFS"))
                .with_arg("triples", triples),
        );
        assert!(!res.is_error, "reason should run without error");
        assert_eq!(res.content["profile"], "RDFS");
        let inferred = res.content["inferred"].as_array().expect("inferred array");
        assert!(
            inferred.iter().any(|t| {
                t["s"].as_str() == Some("http://ex.org/alice")
                    && t["o"].as_str() == Some("http://ex.org/Animal")
            }),
            "RDFS should infer :Alice rdf:type :Animal; got: {inferred:?}"
        );
    }

    #[test]
    fn reason_rejects_unknown_profile() {
        let res = run(
            "reason",
            ToolInvocation::new()
                .with_arg("profile", serde_json::json!("Bogus-Profile"))
                .with_arg("triples", serde_json::json!([])),
        );
        assert!(res.is_error);
    }

    #[test]
    fn reason_rejects_missing_triples() {
        let res = run(
            "reason",
            ToolInvocation::new().with_arg("profile", serde_json::json!("RDFS")),
        );
        assert!(res.is_error);
    }

    #[test]
    fn reason_empty_triples_returns_zero_inferred() {
        let res = run(
            "reason",
            ToolInvocation::new()
                .with_arg("profile", serde_json::json!("RDFS"))
                .with_arg("triples", serde_json::json!([])),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["input_count"], 0);
        assert_eq!(res.content["inferred_count"], 0);
    }

    // ─── memory tools tests ──────────────────────────────────────────

    use crate::memory::InMemoryMemoryStore;

    fn run_with_memory(
        store: Arc<InMemoryMemoryStore>,
        name: &str,
        args: ToolInvocation,
    ) -> ToolResult {
        let mut reg = ToolRegistry::new();
        reg.install(store_memory_tool(store.clone()));
        reg.install(recall_memory_tool(store.clone()));
        reg.install(forget_memory_tool(store));
        reg.call(name, &args).expect("dispatch")
    }

    #[test]
    fn store_memory_persists_a_fact() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("greeting"))
                .with_arg("content", serde_json::json!("hello world")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["id"], "greeting");
        assert_eq!(res.content["namespace"], "default");
        // Direct check the store has the fact.
        let facts = store.recall("default", "").unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].content, "hello world");
    }

    #[test]
    fn store_memory_requires_id_and_content() {
        let store = InMemoryMemoryStore::shared();
        let no_id = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg("content", serde_json::json!("x")),
        );
        assert!(no_id.is_error);

        let no_content = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new().with_arg("id", serde_json::json!("k")),
        );
        assert!(no_content.is_error);
    }

    #[test]
    fn recall_memory_filters_by_substring_query() {
        let store = InMemoryMemoryStore::shared();
        store
            .store(MemoryFact::new("default", "a", "alpha bravo"))
            .unwrap();
        store
            .store(MemoryFact::new("default", "b", "charlie delta"))
            .unwrap();
        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("query", serde_json::json!("delta")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["count"], 1);
        assert_eq!(res.content["facts"][0]["id"], "b");
    }

    #[test]
    fn recall_memory_supports_namespaces() {
        let store = InMemoryMemoryStore::shared();
        store.store(MemoryFact::new("session-1", "k", "a")).unwrap();
        store.store(MemoryFact::new("session-2", "k", "b")).unwrap();
        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("namespace", serde_json::json!("session-2")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["count"], 1);
        assert_eq!(res.content["facts"][0]["content"], "b");
    }

    #[test]
    fn forget_memory_removes_existing_fact() {
        let store = InMemoryMemoryStore::shared();
        store.store(MemoryFact::new("default", "k", "v")).unwrap();
        let res = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new().with_arg("id", serde_json::json!("k")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["removed"], true);
        assert_eq!(store.len("default").unwrap(), 0);
    }

    #[test]
    fn forget_memory_returns_false_when_missing() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "forget_memory",
            ToolInvocation::new().with_arg("id", serde_json::json!("nope")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["removed"], false);
    }

    #[test]
    fn store_recall_forget_cycle_end_to_end() {
        let store = InMemoryMemoryStore::shared();
        // Store
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("k"))
                .with_arg("content", serde_json::json!("the answer is 42")),
        );
        // Recall
        let recall = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg("query", serde_json::json!("42")),
        );
        assert_eq!(recall.content["count"], 1);
        // Forget
        let forget = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new().with_arg("id", serde_json::json!("k")),
        );
        assert_eq!(forget.content["removed"], true);
        // Recall again — empty
        let recall2 = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("query", serde_json::json!("42")),
        );
        assert_eq!(recall2.content["count"], 0);
    }
}
