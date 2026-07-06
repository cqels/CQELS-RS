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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cqels_core::compiler::CqelsQueryCompiler;
use cqels_core::parser::CqelsQlParser;
use cqels_core::stream::RdfStreamElement;
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};
use cqels_reasoning::{ReasoningConfig, ReasoningProfile, ReteNetwork};
use serde_json::json;

use crate::memory::{MemoryFact, MemoryPayload, MemoryStatement, MemoryStore};
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
const LONGTERM_MEMORY: &str = "longterm";
const SHORTTERM_MEMORY: &str = "shortterm";
const LONGTERM_GRAPH: &str = "cqels://memory/longterm";
const DEFAULT_STREAM: &str = "shortterm";
const DEFAULT_RECALL_LIMIT: usize = 50;
const MAX_RECALL_LIMIT: usize = 1000;
const RESERVED_GRAPHS: &[&str] = &[
    "cqels://memory/annotations",
    "cqels://memory/policy",
    "cqels://memory/procedures",
    "cqels://memory/decisions",
    "cqels://memory/episodic",
    "cqels://memory/inferred",
];

static GENERATED_MEMORY_ID: AtomicU64 = AtomicU64::new(1);

fn namespace_from(invocation: &ToolInvocation) -> String {
    invocation
        .get_str("namespace")
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string()
}

fn generated_memory_id() -> String {
    let seq = GENERATED_MEMORY_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("memory-{nanos}-{seq}")
}

fn structured_memory_requested(invocation: &ToolInvocation) -> bool {
    ["facts", "turtle", "memory", "stream", "graph", "meta"]
        .iter()
        .any(|key| invocation.get(key).is_some())
}

fn parse_memory_payload(invocation: &ToolInvocation) -> Result<Option<MemoryPayload>, String> {
    if !structured_memory_requested(invocation) {
        return Ok(None);
    }

    let memory = invocation
        .get_str("memory")
        .unwrap_or(LONGTERM_MEMORY)
        .to_string();
    if memory != LONGTERM_MEMORY && memory != SHORTTERM_MEMORY {
        return Err(format!(
            "unknown memory type '{memory}'; supported: longterm, shortterm"
        ));
    }

    let graph = if memory == LONGTERM_MEMORY {
        let graph = invocation.get_str("graph").unwrap_or(LONGTERM_GRAPH);
        if RESERVED_GRAPHS.contains(&graph) {
            return Err(format!(
                "graph '{graph}' is reserved for CQELS system tools"
            ));
        }
        Some(graph.to_string())
    } else {
        None
    };

    let stream = (memory == SHORTTERM_MEMORY).then(|| {
        invocation
            .get_str("stream")
            .unwrap_or(DEFAULT_STREAM)
            .to_string()
    });
    let meta = invocation
        .get("meta")
        .map(|value| {
            if value.is_object() {
                Ok(value.clone())
            } else {
                Err("`meta` must be an object".to_string())
            }
        })
        .transpose()?;
    let facts = parse_memory_statements(invocation.get("facts"), meta.as_ref())?;
    let turtle = invocation
        .get_str("turtle")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    if facts.is_empty() && turtle.is_none() && invocation.get_str("content").is_none() {
        return Err("store_memory requires one of: content, facts, turtle".to_string());
    }

    Ok(Some(MemoryPayload {
        memory,
        graph,
        stream,
        facts,
        turtle,
        meta,
    }))
}

fn parse_memory_statements(
    value: Option<&serde_json::Value>,
    default_meta: Option<&serde_json::Value>,
) -> Result<Vec<MemoryStatement>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let facts = value
        .as_array()
        .ok_or_else(|| "`facts` must be an array".to_string())?;
    let mut out = Vec::with_capacity(facts.len());
    for (idx, fact) in facts.iter().enumerate() {
        let object = fact
            .as_object()
            .ok_or_else(|| format!("facts[{idx}] must be an object"))?;
        let required = |key: &str| {
            object
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("facts[{idx}] requires `{key}`"))
        };
        let object_type = object
            .get("objectType")
            .and_then(|v| v.as_str())
            .unwrap_or("literal");
        if object_type != "uri" && object_type != "literal" {
            return Err(format!(
                "facts[{idx}].objectType must be either 'uri' or 'literal'"
            ));
        }
        out.push(MemoryStatement {
            subject: required("subject")?,
            predicate: required("predicate")?,
            object: required("object")?,
            object_type: object_type.to_string(),
            meta: match object.get("meta") {
                Some(meta) if meta.is_object() => Some(meta.clone()),
                Some(_) => return Err(format!("facts[{idx}].meta must be an object")),
                None => default_meta.cloned(),
            },
        });
    }
    Ok(out)
}

fn canonical_structured_content(payload: &MemoryPayload) -> Result<String, String> {
    serde_json::to_string(&json!({
        "memory": payload.memory,
        "graph": payload.graph,
        "stream": payload.stream,
        "facts": payload.facts,
        "turtle": payload.turtle,
        "meta": payload.meta,
    }))
    .map_err(|e| format!("failed to encode structured memory content: {e}"))
}

fn recall_limit(invocation: &ToolInvocation) -> usize {
    invocation
        .get("limit")
        .and_then(|v| v.as_i64())
        .map(|n| n.clamp(1, MAX_RECALL_LIMIT as i64) as usize)
        .unwrap_or(DEFAULT_RECALL_LIMIT)
}

fn has_non_empty_pattern_field(pattern: &serde_json::Map<String, serde_json::Value>) -> bool {
    ["subject", "predicate", "object"].iter().any(|key| {
        pattern
            .get(*key)
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
    })
}

fn filter_by_pattern(
    facts: Vec<MemoryFact>,
    pattern_value: Option<&serde_json::Value>,
) -> Result<Vec<MemoryFact>, String> {
    let Some(pattern_value) = pattern_value else {
        return Ok(facts);
    };
    let pattern = pattern_value
        .as_object()
        .ok_or_else(|| "`pattern` must be an object".to_string())?;
    if !has_non_empty_pattern_field(pattern) {
        return Err("pattern must specify at least one of: subject, predicate, object".to_string());
    }
    Ok(facts
        .into_iter()
        .filter(|fact| {
            fact.facts
                .iter()
                .any(|stmt| statement_matches(stmt, pattern))
        })
        .collect())
}

fn statement_matches(
    statement: &MemoryStatement,
    pattern: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let matches_field = |key: &str, actual: &str| {
        pattern
            .get(key)
            .and_then(|v| v.as_str())
            .is_none_or(|expected| expected.is_empty() || expected == actual)
    };
    matches_field("subject", &statement.subject)
        && matches_field("predicate", &statement.predicate)
        && matches_field("object", &statement.object)
        && pattern
            .get("objectType")
            .and_then(|v| v.as_str())
            .is_none_or(|expected| expected == statement.object_type)
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
        "Persist memory keyed by (namespace, id). Supports legacy raw \
         content plus alpha.8-style RDF facts, Turtle payloads, graph/stream \
         targeting, and statement metadata."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property("id", json!({
                "type": "string",
                "description": "Unique identifier for this memory within the namespace. Generated for structured RDF payloads when omitted.",
            }))
            .with_property("content", json!({
                "type": "string",
                "description": "Raw content of the memory (free-form text, JSON, Turtle, etc.). Optional when `facts` or `turtle` is provided.",
            }))
            .with_property("facts", json!({
                "type": "array",
                "description": "Array of RDF facts to store.",
                "items": {
                    "type": "object",
                    "properties": {
                        "subject": {"type": "string"},
                        "predicate": {"type": "string"},
                        "object": {"type": "string"},
                        "objectType": {"type": "string", "enum": ["uri", "literal"], "default": "literal"},
                        "meta": {"type": "object"}
                    },
                    "required": ["subject", "predicate", "object"]
                }
            }))
            .with_property("turtle", json!({
                "type": "string",
                "description": "RDF data in Turtle format. Stored as raw Turtle until the full RDF repository-backed memory layer lands.",
            }))
            .with_property("memory", json!({
                "type": "string",
                "enum": ["longterm", "shortterm"],
                "default": LONGTERM_MEMORY,
                "description": "Memory type: longterm persistent memory or shortterm stream memory metadata.",
            }))
            .with_property("stream", json!({
                "type": "string",
                "description": "Target stream name for shortterm memory metadata.",
            }))
            .with_property("graph", json!({
                "type": "string",
                "description": "Named graph URI for longterm memory. CQELS system graphs are reserved.",
                "default": LONGTERM_GRAPH,
            }))
            .with_property("meta", json!({
                "type": "object",
                "description": "Default statement metadata such as source, confidence, validity interval, spatial scope, accessLabel, or decisionScope.",
            }))
            .with_property("namespace", json!({
                "type": "string",
                "description": "Logical bucket isolating memories (default: \"default\").",
                "default": DEFAULT_NAMESPACE,
            }))
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let namespace = namespace_from(invocation);
        let payload = match parse_memory_payload(invocation) {
            Ok(payload) => payload,
            Err(message) => return ToolResult::error(message),
        };

        let fact = match payload {
            Some(payload) => {
                let id = invocation
                    .get_str("id")
                    .map(str::to_string)
                    .unwrap_or_else(generated_memory_id);
                let content = match invocation.get_str("content") {
                    Some(content) => content.to_string(),
                    None => match canonical_structured_content(&payload) {
                        Ok(content) => content,
                        Err(message) => return ToolResult::error(message),
                    },
                };
                MemoryFact::with_structured_payload(namespace.clone(), id, content, payload)
            }
            None => {
                let Some(id) = invocation.get_str("id").map(str::to_string) else {
                    return ToolResult::error("missing `id` argument");
                };
                let Some(content) = invocation.get_str("content").map(str::to_string) else {
                    return ToolResult::error("missing `content` argument");
                };
                MemoryFact::new(namespace.clone(), id, content)
            }
        };
        match self.store.store(fact.clone()) {
            Ok(()) => ToolResult::success(json!({
                "ok": true,
                "namespace": fact.namespace,
                "id": fact.id,
                "created_at_ms": fact.created_at_ms,
                "memory": fact.memory,
                "graph": fact.graph,
                "stream": fact.stream,
                "fact_count": fact.facts.len(),
                "has_turtle": fact.turtle.is_some(),
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
        "Retrieve memories from a namespace, optionally filtered by text \
         substring or alpha.8-style RDF subject/predicate/object pattern. \
         Returns facts sorted by id."
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
            .with_property("text", json!({
                "type": "string",
                "description": "Alias for `query`, matching Java alpha.8's lexical recall argument.",
            }))
            .with_property("pattern", json!({
                "type": "object",
                "description": "Simple RDF graph pattern over stored structured facts.",
                "properties": {
                    "subject": {"type": "string"},
                    "predicate": {"type": "string"},
                    "object": {"type": "string"},
                    "objectType": {"type": "string", "enum": ["uri", "literal"]}
                }
            }))
            .with_property("limit", json!({
                "type": "integer",
                "description": "Maximum facts to return, clamped to [1, 1000].",
                "default": DEFAULT_RECALL_LIMIT,
                "maximum": MAX_RECALL_LIMIT,
            }))
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let namespace = namespace_from(invocation);
        let query = invocation
            .get_str("query")
            .or_else(|| invocation.get_str("text"))
            .unwrap_or("")
            .to_string();
        match self.store.recall(&namespace, &query) {
            Ok(facts) => {
                let mut facts = match filter_by_pattern(facts, invocation.get("pattern")) {
                    Ok(facts) => facts,
                    Err(message) => return ToolResult::error(message),
                };
                facts.truncate(recall_limit(invocation));
                ToolResult::success(json!({
                "namespace": namespace,
                "query": query,
                "count": facts.len(),
                "facts": facts,
                }))
            }
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
    fn store_memory_accepts_java_style_rdf_facts() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("namespace", serde_json::json!("semantic"))
                .with_arg("graph", serde_json::json!("cqels://memory/user/project"))
                .with_arg("meta", serde_json::json!({"source":"sensor-feed"}))
                .with_arg(
                    "facts",
                    serde_json::json!([
                        {
                            "subject": "http://ex/Alice",
                            "predicate": "http://ex/likes",
                            "object": "http://ex/Sensors",
                            "objectType": "uri",
                            "meta": {"accessLabel": "team"}
                        },
                        {
                            "subject": "http://ex/Alice",
                            "predicate": "http://ex/name",
                            "object": "Alice",
                            "objectType": "literal"
                        }
                    ]),
                ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["memory"], LONGTERM_MEMORY);
        assert_eq!(res.content["graph"], "cqels://memory/user/project");
        assert_eq!(res.content["fact_count"], 2);
        assert!(res.content["id"].as_str().unwrap().starts_with("memory-"));

        let stored = store.recall("semantic", "").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].facts.len(), 2);
        assert_eq!(stored[0].facts[0].object_type, "uri");
        assert_eq!(
            stored[0].facts[0].meta.as_ref().unwrap()["accessLabel"],
            "team"
        );
        assert_eq!(
            stored[0].facts[1].meta.as_ref().unwrap()["source"],
            "sensor-feed"
        );
        assert_eq!(stored[0].meta.as_ref().unwrap()["source"], "sensor-feed");
    }

    #[test]
    fn store_memory_generates_distinct_structured_ids() {
        let store = InMemoryMemoryStore::shared();
        let first = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/likes",
                    "object": "Sensors"
                }]),
            ),
        );
        let second = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Bob",
                    "predicate": "http://ex/likes",
                    "object": "Sensors"
                }]),
            ),
        );

        assert!(!first.is_error, "{:?}", first.content);
        assert!(!second.is_error, "{:?}", second.content);
        assert_ne!(first.content["id"], second.content["id"]);
    }

    #[test]
    fn store_memory_rejects_non_object_metadata() {
        let store = InMemoryMemoryStore::shared();
        let top_level = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("meta", serde_json::json!("not-an-object"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/likes",
                        "object": "Sensors"
                    }]),
                ),
        );
        assert!(top_level.is_error);

        let per_fact = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/likes",
                    "object": "Sensors",
                    "meta": "not-an-object"
                }]),
            ),
        );
        assert!(per_fact.is_error);
    }

    #[test]
    fn store_memory_rejects_reserved_system_graphs() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/policy"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/likes",
                        "object": "Sensors"
                    }]),
                ),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("reserved"));
    }

    #[test]
    fn store_memory_records_shortterm_stream_metadata() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("memory", serde_json::json!("shortterm"))
                .with_arg("stream", serde_json::json!("alerts"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["memory"], SHORTTERM_MEMORY);
        assert_eq!(res.content["stream"], "alerts");

        let stored = store.recall("default", "").unwrap();
        assert_eq!(stored[0].memory, SHORTTERM_MEMORY);
        assert_eq!(stored[0].stream.as_deref(), Some("alerts"));
        assert!(stored[0].graph.is_none());
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
    fn recall_memory_filters_structured_facts_by_pattern() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("alice"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/likes",
                        "object": "http://ex/Sensors",
                        "objectType": "uri"
                    }]),
                ),
        );
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("bob"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Bob",
                        "predicate": "http://ex/likes",
                        "object": "Sensors",
                        "objectType": "literal"
                    }]),
                ),
        );

        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/likes",
                    "object": "http://ex/Sensors",
                    "objectType": "uri"
                }),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["count"], 1);
        assert_eq!(res.content["facts"][0]["id"], "alice");
        assert_eq!(res.content["facts"][0]["facts"][0]["objectType"], "uri");
        assert!(res.content["facts"][0]["facts"][0]
            .get("object_type")
            .is_none());
    }

    #[test]
    fn recall_memory_supports_text_alias_and_limit_clamp() {
        let store = InMemoryMemoryStore::shared();
        for idx in 0..3 {
            run_with_memory(
                store.clone(),
                "store_memory",
                ToolInvocation::new()
                    .with_arg("id", serde_json::json!(format!("fact-{idx}")))
                    .with_arg(
                        "facts",
                        serde_json::json!([{
                            "subject": format!("http://ex/{idx}"),
                            "predicate": "http://ex/name",
                            "object": "shared literal"
                        }]),
                    ),
            );
        }

        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("text", serde_json::json!("shared literal"))
                .with_arg("limit", serde_json::json!(2)),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["count"], 2);
        assert_eq!(res.content["facts"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn recall_memory_defaults_to_java_limit() {
        let store = InMemoryMemoryStore::shared();
        for idx in 0..51 {
            store
                .store(MemoryFact::new("default", format!("fact-{idx:02}"), "same"))
                .unwrap();
        }

        let res = run_with_memory(store, "recall_memory", ToolInvocation::new());
        assert!(!res.is_error);
        assert_eq!(res.content["count"], DEFAULT_RECALL_LIMIT);
        assert_eq!(
            res.content["facts"].as_array().unwrap().len(),
            DEFAULT_RECALL_LIMIT
        );
    }

    #[test]
    fn recall_memory_rejects_empty_pattern() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("pattern", serde_json::json!({})),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("subject, predicate, object"));
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
