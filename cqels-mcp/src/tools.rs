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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cqels_core::compiler::CqelsQueryCompiler;
use cqels_core::parser::CqelsQlParser;
use cqels_core::stream::RdfStreamElement;
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};
use cqels_reasoning::{ReasoningConfig, ReasoningProfile, ReteNetwork};
#[allow(deprecated)]
use oxigraph::io::{GraphFormat, GraphParser};
use parking_lot::RwLock;
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

const PROFILE_HINT: &str = "NONE, RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL, \
                            RDFS_MINIMAL, RDFS_FULL, OWL2_QL, OWL2_EL, OWL2_RL";

fn java_alpha8_aliases(profile: ReasoningProfile) -> &'static [&'static str] {
    match profile {
        ReasoningProfile::None => &["NONE"],
        ReasoningProfile::Rdfs => &["RDFS_MINIMAL"],
        ReasoningProfile::RdfsFull => &["RDFS_FULL"],
        ReasoningProfile::OwlLite => &[],
        ReasoningProfile::OwlQl => &["OWL2_QL"],
        ReasoningProfile::Owl2El => &["OWL2_EL"],
        ReasoningProfile::Owl2Rl => &["OWL2_RL"],
        _ => &[],
    }
}

fn profile_summary(profile: ReasoningProfile) -> serde_json::Value {
    let rules = profile.rules();
    json!({
        "name": profile.name(),
        "java_alpha8_aliases": java_alpha8_aliases(profile),
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
        "List supported reasoning profiles (NONE, RDFS, RDFS-Full, OWL-Lite, \
         OWL-QL, OWL2-EL, OWL2-RL) with their rule counts and capabilities, \
         or describe one specific profile in detail. Accepts Java alpha.8 \
         aliases RDFS_MINIMAL, RDFS_FULL, OWL2_QL, OWL2_EL, and OWL2_RL. Returns \
         metadata only — live inference against a working memory is a \
         separate operation."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object().with_property(
            "profile",
            json!({
                "type": "string",
                "description": "Optional profile name. Accepts Rust profile names (NONE, RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL) and Java alpha.8 aliases (RDFS_MINIMAL, RDFS_FULL, OWL2_QL, OWL2_EL, OWL2_RL). If omitted, lists all profiles.",
            }),
        )
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        match invocation.get_str("profile").map(str::trim) {
            None | Some("") => {
                let profiles: Vec<_> = all_profiles().iter().map(|p| profile_summary(*p)).collect();
                ToolResult::success(json!({ "profiles": profiles }))
            }
            Some(name) => match resolve_profile(name) {
                Some(p) => ToolResult::success(profile_summary(p)),
                None => ToolResult::error(format!(
                    "unknown reasoning profile '{name}'; try one of: {PROFILE_HINT}"
                )),
            },
        }
    }
}

fn resolve_profile(name: &str) -> Option<ReasoningProfile> {
    let normalized = name.trim().to_uppercase().replace('_', "-");
    match normalized.as_str() {
        "RDFS-MINIMAL" => return Some(ReasoningProfile::Rdfs),
        "OWL2-QL" => return Some(ReasoningProfile::OwlQl),
        _ => {}
    }
    all_profiles()
        .into_iter()
        .find(|p| p.name().to_uppercase() == normalized)
}

fn default_reason_profile() -> ReasoningProfile {
    ReasoningProfile::RdfsFull
}

fn reasoning_max_recursion_depth(profile: ReasoningProfile) -> usize {
    match profile {
        ReasoningProfile::OwlLite | ReasoningProfile::Owl2Rl => 15,
        _ => 10,
    }
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
/// - `profile`: profile name (`NONE`, `RDFS`, `RDFS-Full`,
///   `OWL-Lite`, `OWL-QL`, `OWL2-EL`, `OWL2-RL`) or Java alpha.8
///   alias (`RDFS_MINIMAL`, `RDFS_FULL`, `OWL2_QL`, `OWL2_EL`,
///   `OWL2_RL`). Defaults to Java's `RDFS_FULL`.
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
         reasoning profile (NONE / RDFS / RDFS-Full / OWL-Lite / OWL-QL / \
         OWL2-EL / OWL2-RL). Inputs: optional `profile` (string; accepts \
         Java alpha.8 aliases RDFS_MINIMAL, RDFS_FULL, OWL2_QL, OWL2_EL, and OWL2_RL; \
         defaults to RDFS_FULL) + `triples` (array of `{s, p, o}` objects). \
         Returns inferred triples with rule provenance. \
         Stateless — no engine wiring required."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "profile",
                json!({
                    "type": "string",
                    "default": "RDFS_FULL",
                    "description": "Reasoning profile name. Accepts Rust profile names (NONE, RDFS, RDFS-Full, OWL-Lite, OWL-QL, OWL2-EL, OWL2-RL) and Java alpha.8 aliases (RDFS_MINIMAL, RDFS_FULL, OWL2_QL, OWL2_EL, OWL2_RL); defaults to RDFS_FULL."
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
            .require("triples")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let profile_arg = invocation
            .get_str("profile")
            .map(str::trim)
            .filter(|name| !name.is_empty());
        let profile = match profile_arg {
            Some(profile_name) => match resolve_profile(profile_name) {
                Some(profile) => profile,
                None => {
                    return ToolResult::error(format!(
                        "unknown reasoning profile '{profile_name}'; try one of: {PROFILE_HINT}"
                    ));
                }
            },
            None => default_reason_profile(),
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
            .max_recursion_depth(reasoning_max_recursion_depth(profile))
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
            "profile_java_alpha8_aliases": java_alpha8_aliases(profile),
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
const SCHEMA_GRAPH: &str = "cqels://memory/schema";
const INFERRED_GRAPH: &str = "cqels://memory/inferred";
const DEFAULT_STREAM: &str = "shortterm";
const DEFAULT_RECALL_LIMIT: usize = 50;
const MAX_RECALL_LIMIT: usize = 1000;
const RESERVED_GRAPHS: &[&str] = &[
    "cqels://memory/annotations",
    "cqels://memory/policy",
    "cqels://memory/procedures",
    "cqels://memory/decisions",
    "cqels://memory/episodic",
    INFERRED_GRAPH,
];

static GENERATED_MEMORY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct ReasoningRegistrationState {
    profile: ReasoningProfile,
    schema_graph: String,
    data_graph: String,
    registered: bool,
}

impl Default for ReasoningRegistrationState {
    fn default() -> Self {
        Self {
            profile: default_reason_profile(),
            schema_graph: SCHEMA_GRAPH.to_string(),
            data_graph: LONGTERM_GRAPH.to_string(),
            registered: false,
        }
    }
}

/// Shared MCP reasoning registration used by `register_reasoning` and
/// `recall_memory(entail:true)`.
pub struct ReasoningRegistration {
    inner: RwLock<HashMap<String, ReasoningRegistrationState>>,
}

impl ReasoningRegistration {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn current(&self, namespace: &str) -> ReasoningRegistrationState {
        self.inner
            .read()
            .get(namespace)
            .cloned()
            .unwrap_or_default()
    }

    fn register(
        &self,
        namespace: String,
        profile: ReasoningProfile,
        schema_graph: String,
        data_graph: String,
    ) {
        self.inner.write().insert(
            namespace,
            ReasoningRegistrationState {
                profile,
                schema_graph,
                data_graph,
                registered: true,
            },
        );
    }
}

impl Default for ReasoningRegistration {
    fn default() -> Self {
        Self::new()
    }
}

fn namespace_from(invocation: &ToolInvocation) -> String {
    invocation
        .get_str("namespace")
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string()
}

fn expand_memory_iri(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("IRI value must not be empty".to_string());
    }
    if trimmed.starts_with('<') || trimmed.ends_with('>') {
        if !(trimmed.starts_with('<') && trimmed.ends_with('>')) {
            return Err(format!("bracketed IRI '{value}' must use both '<' and '>'"));
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        if is_absolute_memory_iri(inner) {
            return Ok(inner.to_string());
        }
        return Err(format!("bracketed IRI '{value}' must be absolute"));
    }
    let Some((prefix, local)) = trimmed.split_once(':') else {
        return Err(format!(
            "'{value}' must be an absolute IRI or known prefixed name"
        ));
    };
    if local.is_empty() {
        return Err(format!("IRI '{value}' has an empty scheme-specific part"));
    }
    let base = match prefix {
        "rdf" => "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs" => "http://www.w3.org/2000/01/rdf-schema#",
        "owl" => "http://www.w3.org/2002/07/owl#",
        "xsd" => "http://www.w3.org/2001/XMLSchema#",
        "sh" => "http://www.w3.org/ns/shacl#",
        "cqels" => "cqels://ontology/",
        _ => {
            if is_valid_iri_scheme(prefix) {
                return Ok(trimmed.to_string());
            }
            return Err(format!(
                "unknown prefix '{prefix}' in '{value}'; supported: rdf, rdfs, owl, xsd, sh, cqels"
            ));
        }
    };
    Ok(format!("{base}{local}"))
}

fn is_valid_iri_scheme(prefix: &str) -> bool {
    let mut chars = prefix.chars();
    chars.next().is_some_and(|ch| ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

fn is_absolute_memory_iri(value: &str) -> bool {
    let Some((scheme, rest)) = value.split_once(':') else {
        return false;
    };
    !rest.is_empty() && is_valid_iri_scheme(scheme)
}

fn generated_memory_id() -> String {
    let seq = GENERATED_MEMORY_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("memory-{nanos}-{seq}")
}

fn validate_writable_graph(graph: &str) -> Result<(), String> {
    if RESERVED_GRAPHS.contains(&graph) {
        return Err(format!(
            "graph '{graph}' is reserved for CQELS system tools"
        ));
    }
    Ok(())
}

fn validate_recall_graph(graph: &str) -> Result<(), String> {
    validate_writable_graph(graph)
}

fn memory_fact_graph(fact: &MemoryFact) -> &str {
    fact.graph.as_deref().unwrap_or(LONGTERM_GRAPH)
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
        validate_writable_graph(graph)?;
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
    let turtle = invocation
        .get_str("turtle")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let mut facts = parse_memory_statements(invocation.get("facts"), meta.as_ref())?;
    if let Some(turtle) = &turtle {
        facts.extend(parse_turtle_statements(turtle, meta.as_ref())?);
    }

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
            subject: expand_memory_iri(&required("subject")?)?,
            predicate: expand_memory_iri(&required("predicate")?)?,
            object: if object_type == "uri" {
                expand_memory_iri(&required("object")?)?
            } else {
                required("object")?
            },
            object_type: object_type.to_string(),
            datatype: match object.get("datatype") {
                Some(value) => {
                    Some(expand_memory_iri(value.as_str().ok_or_else(|| {
                        format!("facts[{idx}].datatype must be a string")
                    })?)?)
                }
                None => None,
            },
            language: match object.get("language") {
                Some(value) => Some(
                    value
                        .as_str()
                        .ok_or_else(|| format!("facts[{idx}].language must be a string"))?
                        .to_string(),
                ),
                None => None,
            },
            meta: match object.get("meta") {
                Some(meta) if meta.is_object() => Some(meta.clone()),
                Some(_) => return Err(format!("facts[{idx}].meta must be an object")),
                None => default_meta.cloned(),
            },
        });
    }
    Ok(out)
}

#[allow(deprecated)]
fn parse_turtle_statements(
    turtle: &str,
    default_meta: Option<&serde_json::Value>,
) -> Result<Vec<MemoryStatement>, String> {
    let parser = GraphParser::from_format(GraphFormat::Turtle);
    let mut out = Vec::new();
    for (idx, triple) in parser.read_triples(turtle.as_bytes()).enumerate() {
        let triple = triple.map_err(|e| format!("turtle parse error at triple #{idx}: {e}"))?;
        let subject = match triple.subject {
            oxigraph::model::Subject::NamedNode(node) => node.as_str().to_string(),
            oxigraph::model::Subject::BlankNode(node) => skolemized_blank_node(node.as_str()),
            oxigraph::model::Subject::Triple(_) => {
                return Err("turtle RDF-star subjects are not supported yet".to_string());
            }
        };
        let (object, object_type, datatype, language) = match triple.object {
            oxigraph::model::Term::NamedNode(node) => {
                (node.as_str().to_string(), "uri", None, None)
            }
            oxigraph::model::Term::Literal(literal) => (
                literal.value().to_string(),
                "literal",
                Some(literal.datatype().as_str().to_string()),
                literal.language().map(str::to_string),
            ),
            oxigraph::model::Term::BlankNode(node) => {
                (skolemized_blank_node(node.as_str()), "uri", None, None)
            }
            oxigraph::model::Term::Triple(_) => {
                return Err("turtle RDF-star objects are not supported yet".to_string());
            }
        };
        out.push(MemoryStatement {
            subject,
            predicate: triple.predicate.as_str().to_string(),
            object,
            object_type: object_type.to_string(),
            datatype,
            language,
            meta: default_meta.cloned(),
        });
    }
    Ok(out)
}

fn skolemized_blank_node(id: &str) -> String {
    let mut out = String::from("urn:cqels:bnode:");
    for byte in id.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02x}"));
        }
    }
    out
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

fn recall_format(invocation: &ToolInvocation) -> Result<&str, String> {
    match invocation.get_str("format").unwrap_or("json") {
        "json" => Ok("json"),
        "turtle" | "natural" => {
            Err("recall_memory format support is currently limited to 'json'".to_string())
        }
        other => Err(format!(
            "unknown recall_memory format '{other}'; supported: json"
        )),
    }
}

#[derive(Debug)]
struct MemoryPattern {
    subject: Option<String>,
    predicate: Option<String>,
    object_candidates: Vec<String>,
    object_type: Option<String>,
    graph: Option<String>,
}

impl MemoryPattern {
    fn has_statement_field(&self) -> bool {
        self.subject.is_some() || self.predicate.is_some() || !self.object_candidates.is_empty()
    }
}

fn non_empty_str<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<&'a str>, String> {
    match object.get(key) {
        Some(value) => match value.as_str() {
            Some(text) if !text.is_empty() => Ok(Some(text)),
            Some(_) => Ok(None),
            None => Err(format!("pattern.{key} must be a string")),
        },
        None => Ok(None),
    }
}

fn pattern_str<'a>(
    pattern: Option<&'a serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> Result<Option<&'a str>, String> {
    match pattern {
        Some(pattern) => non_empty_str(pattern, key),
        None => Ok(None),
    }
}

fn parse_recall_pattern(
    pattern_value: Option<&serde_json::Value>,
    graph_value: Option<&str>,
) -> Result<Option<MemoryPattern>, String> {
    let pattern = match pattern_value {
        Some(pattern_value) => Some(
            pattern_value
                .as_object()
                .ok_or_else(|| "`pattern` must be an object".to_string())?,
        ),
        None => None,
    };
    if pattern.is_none() && graph_value.is_none() {
        return Ok(None);
    }

    let subject = match pattern_str(pattern, "subject")? {
        Some(value) => Some(expand_memory_iri(value)?),
        None => None,
    };
    let predicate = match pattern_str(pattern, "predicate")? {
        Some(value) => Some(expand_memory_iri(value)?),
        None => None,
    };
    let object_type = pattern_str(pattern, "objectType")?.map(str::to_string);
    if let Some(object_type) = &object_type {
        if object_type != "uri" && object_type != "literal" {
            return Err("pattern.objectType must be either 'uri' or 'literal'".to_string());
        }
    }
    let mut object_candidates = Vec::new();
    if let Some(object) = pattern_str(pattern, "object")? {
        match object_type.as_deref() {
            Some("uri") => object_candidates.push(expand_memory_iri(object)?),
            Some("literal") => object_candidates.push(object.to_string()),
            None => {
                if let Ok(expanded) = expand_memory_iri(object) {
                    object_candidates.push(expanded);
                } else {
                    object_candidates.push(object.to_string());
                }
            }
            _ => unreachable!("objectType already validated"),
        }
    }
    let graph = graph_value
        .filter(|graph| !graph.is_empty())
        .or(pattern_str(pattern, "graph")?)
        .map(str::to_string);
    if let Some(graph) = &graph {
        validate_recall_graph(graph)?;
    }
    let parsed = MemoryPattern {
        subject,
        predicate,
        object_candidates,
        object_type,
        graph,
    };
    if !parsed.has_statement_field() && parsed.graph.is_none() {
        return Err(
            "pattern must specify at least one of: subject, predicate, object, graph".to_string(),
        );
    }
    Ok(Some(parsed))
}

fn memory_fact_matches_pattern(fact: &MemoryFact, pattern: &MemoryPattern) -> bool {
    if fact.memory != LONGTERM_MEMORY {
        return false;
    }
    let graph = memory_fact_graph(fact);
    if let Some(expected_graph) = &pattern.graph {
        if graph != expected_graph {
            return false;
        }
    } else if validate_writable_graph(graph).is_err() {
        return false;
    }
    if !pattern.has_statement_field() {
        return true;
    }
    fact.facts
        .iter()
        .any(|stmt| statement_matches(stmt, pattern))
}

fn pattern_statement_rows(
    facts: Vec<MemoryFact>,
    pattern: &MemoryPattern,
    limit: usize,
) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for fact in facts {
        if !memory_fact_matches_pattern(&fact, pattern) {
            continue;
        }
        let graph = memory_fact_graph(&fact);
        for statement in &fact.facts {
            if pattern.has_statement_field() && !statement_matches(statement, pattern) {
                continue;
            }
            let mut row = serde_json::Map::new();
            row.insert("s".to_string(), json!(statement.subject.clone()));
            row.insert("p".to_string(), json!(statement.predicate.clone()));
            row.insert("o".to_string(), json!(statement.object.clone()));
            row.insert(
                "objectType".to_string(),
                json!(statement.object_type.clone()),
            );
            if let Some(datatype) = &statement.datatype {
                row.insert("datatype".to_string(), json!(datatype));
            }
            if let Some(language) = &statement.language {
                row.insert("language".to_string(), json!(language));
            }
            row.insert("graph".to_string(), json!(graph));
            row.insert("memory_id".to_string(), json!(fact.id.clone()));
            if let Some(meta) = &statement.meta {
                row.insert("meta".to_string(), meta.clone());
            }
            rows.push(serde_json::Value::Object(row));
            if rows.len() >= limit {
                return rows;
            }
        }
    }
    rows
}

fn statement_matches(statement: &MemoryStatement, pattern: &MemoryPattern) -> bool {
    pattern
        .subject
        .as_deref()
        .is_none_or(|expected| expected == statement.subject)
        && pattern
            .predicate
            .as_deref()
            .is_none_or(|expected| expected == statement.predicate)
        && (pattern.object_candidates.is_empty()
            || pattern
                .object_candidates
                .iter()
                .any(|expected| expected == &statement.object))
        && pattern
            .object_type
            .as_deref()
            .is_none_or(|expected| expected == statement.object_type)
}

#[derive(Clone, Debug)]
struct EntailedRow {
    statement: MemoryStatement,
    graph: String,
    inferred: bool,
}

fn memory_statement_to_statement(statement: &MemoryStatement) -> Statement {
    let mut literal = LiteralTerm::new(statement.object.clone());
    if let Some(datatype) = &statement.datatype {
        literal = literal.with_datatype(datatype.clone());
    }
    if let Some(language) = &statement.language {
        literal = literal.with_language(language.clone());
    }
    Statement::new(
        Term::Iri(IriTerm::new(statement.subject.clone())),
        IriTerm::new(statement.predicate.clone()),
        if statement.object_type == "uri" {
            Term::Iri(IriTerm::new(statement.object.clone()))
        } else {
            Term::Literal(literal)
        },
    )
}

fn statement_to_memory_statement(statement: &Statement) -> MemoryStatement {
    let (object, object_type, datatype, language) = match &statement.object {
        Term::Iri(iri) => (iri.as_str().to_string(), "uri".to_string(), None, None),
        Term::Literal(literal) => (
            literal.value().to_string(),
            "literal".to_string(),
            literal.datatype().map(str::to_string),
            literal.language().map(str::to_string),
        ),
        other => (other.to_string(), "literal".to_string(), None, None),
    };
    MemoryStatement {
        subject: term_to_string(&statement.subject),
        predicate: statement.predicate.as_str().to_string(),
        object,
        object_type,
        datatype,
        language,
        meta: None,
    }
}

fn graph_statements(
    store: &dyn MemoryStore,
    namespace: &str,
    graph: &str,
) -> Result<Vec<Statement>, String> {
    let facts = store
        .recall(namespace, "")
        .map_err(|e| format!("recall for reasoning graph '{graph}' failed: {e}"))?;
    Ok(facts
        .iter()
        .filter(|fact| fact.memory == LONGTERM_MEMORY && memory_fact_graph(fact) == graph)
        .flat_map(|fact| fact.facts.iter().map(memory_statement_to_statement))
        .collect())
}

fn infer_memory_view(
    store: &dyn MemoryStore,
    namespace: &str,
    config: &ReasoningRegistrationState,
    include_asserted: bool,
) -> Result<Vec<EntailedRow>, String> {
    let data_statements = graph_statements(store, namespace, &config.data_graph)?;
    let schema_statements = if config.schema_graph == config.data_graph {
        Vec::new()
    } else {
        graph_statements(store, namespace, &config.schema_graph)?
    };

    let reasoning_config = ReasoningConfig::builder()
        .rule_set(config.profile.rule_set())
        .default_window(Duration::from_secs(60 * 60 * 24 * 365))
        .enable_recursive_inference(config.profile.requires_recursive_inference())
        .max_recursion_depth(reasoning_max_recursion_depth(config.profile))
        .build();
    let mut network = ReteNetwork::compile(reasoning_config);

    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    for statement in &schema_statements {
        let element = RdfStreamElement::new(statement.clone(), 0);
        let _ = network.process_element(&element);
    }
    if include_asserted {
        for statement in &data_statements {
            if seen.insert(statement.clone()) {
                rows.push(EntailedRow {
                    statement: statement_to_memory_statement(statement),
                    graph: config.data_graph.clone(),
                    inferred: false,
                });
            }
        }
    }
    for statement in &data_statements {
        let element = RdfStreamElement::new(statement.clone(), 0);
        for inferred in network.process_element(&element) {
            if seen.insert(inferred.statement.clone()) {
                rows.push(EntailedRow {
                    statement: statement_to_memory_statement(&inferred.statement),
                    graph: INFERRED_GRAPH.to_string(),
                    inferred: true,
                });
            }
        }
    }
    Ok(rows)
}

fn entailed_pattern_rows(
    store: &dyn MemoryStore,
    namespace: &str,
    config: &ReasoningRegistrationState,
    pattern: &MemoryPattern,
    limit: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let mut rows: Vec<_> = infer_memory_view(store, namespace, config, true)?
        .into_iter()
        .filter(|row| statement_matches(&row.statement, pattern))
        .collect();
    rows.sort_by(|a, b| {
        (
            &a.statement.subject,
            &a.statement.predicate,
            &a.statement.object,
            &a.graph,
        )
            .cmp(&(
                &b.statement.subject,
                &b.statement.predicate,
                &b.statement.object,
                &b.graph,
            ))
    });
    rows.truncate(limit);
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut out = serde_json::Map::new();
            out.insert("s".to_string(), json!(row.statement.subject));
            out.insert("p".to_string(), json!(row.statement.predicate));
            out.insert("o".to_string(), json!(row.statement.object));
            out.insert("objectType".to_string(), json!(row.statement.object_type));
            if let Some(datatype) = row.statement.datatype {
                out.insert("datatype".to_string(), json!(datatype));
            }
            if let Some(language) = row.statement.language {
                out.insert("language".to_string(), json!(language));
            }
            out.insert("graph".to_string(), json!(row.graph));
            out.insert("inferred".to_string(), json!(row.inferred));
            serde_json::Value::Object(out)
        })
        .collect())
}

fn optional_graph_arg(invocation: &ToolInvocation, key: &str, default: &str) -> String {
    invocation
        .get_str(key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn statement_delete_matches(statement: &MemoryStatement, target: &MemoryStatement) -> bool {
    statement.subject == target.subject
        && statement.predicate == target.predicate
        && statement.object == target.object
        && statement.object_type == target.object_type
        && statement.datatype == target.datatype
        && statement.language == target.language
}

fn canonical_fact_content(fact: &MemoryFact) -> Result<String, String> {
    canonical_structured_content(&MemoryPayload {
        memory: fact.memory.clone(),
        graph: fact.graph.clone(),
        stream: fact.stream.clone(),
        facts: fact.facts.clone(),
        turtle: fact.turtle.clone(),
        meta: fact.meta.clone(),
    })
}

fn forget_graph_memories(
    store: &dyn MemoryStore,
    namespace: &str,
    graph: &str,
) -> Result<usize, String> {
    if graph.trim().is_empty() {
        return Err("forget_memory graph must not be empty".to_string());
    }
    validate_recall_graph(graph)?;
    let facts = store
        .recall(namespace, "")
        .map_err(|e| format!("recall before graph clear failed: {e}"))?;
    let mut removed = 0;
    for fact in facts {
        if fact.memory == LONGTERM_MEMORY
            && memory_fact_graph(&fact) == graph
            && match store.forget(namespace, &fact.id) {
                Ok(deleted) => deleted,
                Err(e) => {
                    return Err(format!(
                        "graph clear failed after removing {removed} records: {e}"
                    ));
                }
            }
        {
            removed += 1;
        }
    }
    Ok(removed)
}

#[derive(Debug, Default)]
struct StatementForgetSummary {
    statements_removed: usize,
    records_updated: usize,
    records_deleted: usize,
}

fn forget_matching_statements(
    store: &dyn MemoryStore,
    namespace: &str,
    targets: &[MemoryStatement],
) -> Result<StatementForgetSummary, String> {
    if targets.is_empty() {
        return Err("forget_memory facts must contain at least one fact".to_string());
    }
    let facts = store
        .recall(namespace, "")
        .map_err(|e| format!("recall before fact removal failed: {e}"))?;
    let mut summary = StatementForgetSummary::default();
    for mut fact in facts {
        if fact.memory != LONGTERM_MEMORY
            || memory_fact_graph(&fact) != LONGTERM_GRAPH
            || fact.facts.is_empty()
        {
            continue;
        }
        let before = fact.facts.len();
        fact.facts.retain(|statement| {
            !targets
                .iter()
                .any(|target| statement_delete_matches(statement, target))
        });
        let removed = before - fact.facts.len();
        if removed == 0 {
            continue;
        }
        summary.statements_removed += removed;
        if fact.facts.is_empty() {
            match store.forget(namespace, &fact.id) {
                Ok(true) => summary.records_deleted += 1,
                Ok(false) => {}
                Err(e) => {
                    return Err(format!(
                        "fact removal delete failed after removing {} statements, updating {} records, deleting {} records: {e}",
                        summary.statements_removed,
                        summary.records_updated,
                        summary.records_deleted
                    ));
                }
            }
        } else {
            fact.turtle = None;
            fact.content = canonical_fact_content(&fact)?;
            if let Err(e) = store.store(fact) {
                return Err(format!(
                    "fact removal update failed after removing {} statements, updating {} records, deleting {} records: {e}",
                    summary.statements_removed,
                    summary.records_updated,
                    summary.records_deleted
                ));
            }
            summary.records_updated += 1;
        }
    }
    Ok(summary)
}

/// Constructs a `register_reasoning` tool backed by the supplied memory
/// store and shared reasoning registration.
pub fn register_reasoning_tool(
    store: Arc<dyn MemoryStore>,
    registration: Arc<ReasoningRegistration>,
) -> RegisterReasoningTool {
    RegisterReasoningTool {
        store,
        registration,
    }
}

pub struct RegisterReasoningTool {
    store: Arc<dyn MemoryStore>,
    registration: Arc<ReasoningRegistration>,
}

impl McpTool for RegisterReasoningTool {
    fn name(&self) -> &str {
        "register_reasoning"
    }

    fn description(&self) -> &str {
        "Enable ontology-aware memory recall. Stores the active reasoning \
         profile plus longterm schema/data graph configuration; recall_memory with \
         entail:true then recomputes asserted plus entailed pattern rows on \
         demand."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "namespace",
                json!({
                    "type": "string",
                    "default": DEFAULT_NAMESPACE,
                    "description": "Logical memory namespace for this reasoning registration.",
                }),
            )
            .with_property(
                "profile",
                json!({
                    "type": "string",
                    "default": "RDFS_FULL",
                    "description": "Reasoning profile. Accepts Rust profile names and Java alpha.8 aliases; defaults to the current profile, initially RDFS_FULL."
                }),
            )
            .with_property(
                "schemaGraph",
                json!({
                    "type": "string",
                    "default": SCHEMA_GRAPH,
                    "description": "Longterm named graph holding ontology/schema triples."
                }),
            )
            .with_property(
                "dataGraph",
                json!({
                    "type": "string",
                    "default": LONGTERM_GRAPH,
                    "description": "Longterm named graph holding asserted data triples."
                }),
            )
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let namespace = namespace_from(invocation);
        let current = self.registration.current(&namespace);
        let profile = match invocation.get_str("profile").map(str::trim) {
            Some("") | None => current.profile,
            Some(name) => match resolve_profile(name) {
                Some(profile) => profile,
                None => {
                    return ToolResult::error(format!(
                        "unknown reasoning profile '{name}'; try one of: {PROFILE_HINT}"
                    ));
                }
            },
        };
        let schema_graph = optional_graph_arg(invocation, "schemaGraph", SCHEMA_GRAPH);
        let data_graph = optional_graph_arg(invocation, "dataGraph", LONGTERM_GRAPH);
        if let Err(message) = validate_recall_graph(&schema_graph) {
            return ToolResult::error(message);
        }
        if let Err(message) = validate_recall_graph(&data_graph) {
            return ToolResult::error(message);
        }
        let candidate = ReasoningRegistrationState {
            profile,
            schema_graph: schema_graph.clone(),
            data_graph: data_graph.clone(),
            registered: true,
        };
        let entailed_count =
            match infer_memory_view(self.store.as_ref(), &namespace, &candidate, true) {
                Ok(rows) => rows.into_iter().filter(|row| row.inferred).count(),
                Err(message) => return ToolResult::error(message),
            };
        self.registration.register(
            namespace.clone(),
            profile,
            schema_graph.clone(),
            data_graph.clone(),
        );
        ToolResult::success(json!({
            "ok": true,
            "registered": true,
            "namespace": namespace,
            "profile": profile.name(),
            "profile_java_alpha8_aliases": java_alpha8_aliases(profile),
            "schemaGraph": schema_graph,
            "dataGraph": data_graph,
            "inferredGraph": INFERRED_GRAPH,
            "entailed_count": entailed_count,
            "message": format!(
                "Reasoning registered with profile {}; {entailed_count} entailed statement(s) over {data_graph}.",
                profile.name()
            ),
        }))
    }
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
                "description": "RDF data in Turtle format. Parsed into graph-scoped structured statements and also retained on the memory record.",
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
    RecallMemoryTool {
        store,
        registration: None,
    }
}

/// Constructs a `recall_memory` tool with ontology-aware recall support
/// through a shared [`ReasoningRegistration`].
pub fn recall_memory_tool_with_reasoning(
    store: Arc<dyn MemoryStore>,
    registration: Arc<ReasoningRegistration>,
) -> RecallMemoryTool {
    RecallMemoryTool {
        store,
        registration: Some(registration),
    }
}

pub struct RecallMemoryTool {
    store: Arc<dyn MemoryStore>,
    registration: Option<Arc<ReasoningRegistration>>,
}

impl McpTool for RecallMemoryTool {
    fn name(&self) -> &str {
        "recall_memory"
    }

    fn description(&self) -> &str {
        "Retrieve memories from a namespace, optionally filtered by text \
         substring or alpha.8-style RDF subject/predicate/object pattern. \
         Legacy text/no-pattern recall returns memory records sorted by id; \
         pattern or graph recall returns Java-style RDF statement rows. \
         With register_reasoning, pattern recall may set entail:true to \
         return asserted plus entailed rows; graph filters are rejected in \
         entail mode and statement metadata is not emitted for entail-mode \
         asserted or inferred rows."
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
                "description": "Substring to match within fact content. Empty/missing legacy recall returns all memory records in the namespace.",
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
                    "objectType": {"type": "string", "enum": ["uri", "literal"]},
                    "graph": {"type": "string"}
                }
            }))
            .with_property("graph", json!({
                "type": "string",
                "description": "Optional named graph URI to search for pattern recall. Defaults to all non-reserved longterm RDF memory graphs.",
            }))
            .with_property("entail", json!({
                "type": "boolean",
                "default": false,
                "description": "When true, pattern recall returns asserted plus entailed triples from the active register_reasoning configuration.",
            }))
            .with_property("format", json!({
                "type": "string",
                "enum": ["json"],
                "default": "json",
                "description": "Output format. This Rust slice currently implements Java's json pattern rows; turtle and natural serializers are follow-up parity work.",
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
        let entail = invocation
            .get("entail")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let pattern =
            match parse_recall_pattern(invocation.get("pattern"), invocation.get_str("graph")) {
                Ok(pattern) => pattern,
                Err(message) => return ToolResult::error(message),
            };
        let format = match recall_format(invocation) {
            Ok(format) => format,
            Err(message) => return ToolResult::error(message),
        };
        if entail {
            if !query.is_empty() {
                return ToolResult::error(
                    "entail:true applies only to plain pattern recall, not query/text recall",
                );
            }
            let Some(pattern) = pattern.as_ref() else {
                return ToolResult::error("entail:true requires a pattern");
            };
            if pattern.graph.is_some() {
                return ToolResult::error(
                    "entail:true uses the active register_reasoning dataGraph; omit graph filters",
                );
            }
            let Some(registration) = self.registration.as_ref() else {
                return ToolResult::error(
                    "Ontology-aware recall is not available — call register_reasoning first",
                );
            };
            let config = registration.current(&namespace);
            if !config.registered {
                return ToolResult::error(
                    "Ontology-aware recall is not available — call register_reasoning first",
                );
            }
            let limit = recall_limit(invocation);
            let rows = match entailed_pattern_rows(
                self.store.as_ref(),
                &namespace,
                &config,
                pattern,
                limit,
            ) {
                Ok(rows) => rows,
                Err(message) => return ToolResult::error(message),
            };
            return ToolResult::success(json!({
                "namespace": namespace,
                "query": query,
                "graph": config.data_graph,
                "schemaGraph": config.schema_graph,
                "inferredGraph": INFERRED_GRAPH,
                "profile": config.profile.name(),
                "profile_java_alpha8_aliases": java_alpha8_aliases(config.profile),
                "entail": true,
                "format": format,
                "count": rows.len(),
                "facts": rows,
            }));
        }
        match self.store.recall(&namespace, &query) {
            Ok(facts) => {
                let limit = recall_limit(invocation);
                if let Some(pattern) = pattern.as_ref() {
                    let rows = pattern_statement_rows(facts, pattern, limit);
                    return ToolResult::success(json!({
                    "namespace": namespace,
                    "query": query,
                    "graph": pattern.graph.as_deref(),
                    "format": format,
                    "count": rows.len(),
                    "facts": rows,
                    }));
                }

                let mut facts = facts;
                facts.truncate(limit);
                ToolResult::success(json!({
                "namespace": namespace,
                "query": query,
                "format": format,
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
        "Delete memory by (namespace, id), clear a longterm named graph, \
         or remove specific RDF facts from the default longterm graph. \
         Graph clear removes longterm memory records only, including the \
         default longterm graph when that graph URI is supplied."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "id",
                json!({
                    "type": "string",
                    "description": "Identifier of the memory record to forget.",
                }),
            )
            .with_property("facts", json!({
                "type": "array",
                "description": "Array of RDF facts to remove from the default longterm graph.",
                "items": {
                    "type": "object",
                    "properties": {
                        "subject": {"type": "string"},
                        "predicate": {"type": "string"},
                        "object": {"type": "string"},
                        "objectType": {"type": "string", "enum": ["uri", "literal"], "default": "literal"},
                        "datatype": {"type": "string"},
                        "language": {"type": "string"}
                    },
                    "required": ["subject", "predicate", "object"]
                }
            }))
            .with_property(
                "graph",
                json!({
                    "type": "string",
                    "description": "Longterm named graph URI to clear.",
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
            .with_one_of(vec![
                json!({"required": ["id"]}),
                json!({"required": ["graph"]}),
                json!({"required": ["facts"]}),
            ])
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let namespace = namespace_from(invocation);
        let actions = [
            invocation.get("id").is_some(),
            invocation.get("graph").is_some(),
            invocation.get("facts").is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if actions != 1 {
            return ToolResult::error("provide exactly one of: id, graph, facts");
        }

        if let Some(graph) = invocation.get_str("graph") {
            return match forget_graph_memories(self.store.as_ref(), &namespace, graph) {
                Ok(removed) => ToolResult::success(json!({
                    "namespace": namespace,
                    "graph": graph,
                    "records_removed": removed,
                })),
                Err(message) => ToolResult::error(message),
            };
        } else if invocation.get("graph").is_some() {
            return ToolResult::error("forget_memory graph must be a string");
        }
        if invocation.get("facts").is_some() {
            let targets = match parse_memory_statements(invocation.get("facts"), None) {
                Ok(targets) => targets,
                Err(message) => return ToolResult::error(message),
            };
            return match forget_matching_statements(self.store.as_ref(), &namespace, &targets) {
                Ok(summary) => ToolResult::success(json!({
                    "namespace": namespace,
                    "graph": LONGTERM_GRAPH,
                    "statements_removed": summary.statements_removed,
                    "records_updated": summary.records_updated,
                    "records_deleted": summary.records_deleted,
                })),
                Err(message) => ToolResult::error(message),
            };
        }

        let Some(id) = invocation.get_str("id").map(str::to_string) else {
            return ToolResult::error("forget_memory id must be a string");
        };
        match self.store.forget(&namespace, &id) {
            Ok(removed) => ToolResult::success(json!({
                "namespace": namespace,
                "id": id,
                "deleted": removed,
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
            assert!(p["java_alpha8_aliases"].is_array());
            assert!(p["rule_count"].is_number());
        }
        let rdfs_full = profiles
            .iter()
            .find(|p| p["name"] == "RDFS-Full")
            .expect("RDFS-Full profile");
        assert_eq!(
            rdfs_full["java_alpha8_aliases"],
            serde_json::json!(["RDFS_FULL"])
        );
    }

    #[test]
    fn reasoning_profiles_describes_named_profile() {
        let res = run(
            "reasoning_profiles",
            ToolInvocation::new().with_arg("profile", serde_json::json!("RDFS")),
        );
        assert!(!res.is_error);
        assert_eq!(res.content["name"], "RDFS");
        assert_eq!(
            res.content["java_alpha8_aliases"],
            serde_json::json!(["RDFS_MINIMAL"])
        );
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
    fn reasoning_profiles_accepts_java_alpha8_aliases() {
        for (alias, canonical) in [
            ("NONE", "None"),
            ("RDFS_MINIMAL", "RDFS"),
            ("RDFS_FULL", "RDFS-Full"),
            ("OWL2_QL", "OWL-QL"),
            ("OWL2_EL", "OWL2-EL"),
            ("OWL2_RL", "OWL2-RL"),
        ] {
            let res = run(
                "reasoning_profiles",
                ToolInvocation::new().with_arg("profile", serde_json::json!(alias)),
            );
            assert!(!res.is_error, "{alias}: {:?}", res.content);
            assert_eq!(res.content["name"], canonical, "{alias}");
        }
    }

    #[test]
    fn reasoning_profiles_trims_java_alpha8_aliases() {
        let res = run(
            "reasoning_profiles",
            ToolInvocation::new().with_arg("profile", serde_json::json!(" RDFS_FULL ")),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["name"], "RDFS-Full");
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
        assert!(msg.contains("RDFS_FULL") && msg.contains("OWL2_QL"));
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
    fn reason_defaults_profile_to_java_alpha8_rdfs_full() {
        let res = run(
            "reason",
            ToolInvocation::new().with_arg("triples", serde_json::json!([])),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["profile"], "RDFS-Full");
        assert_eq!(
            res.content["profile_java_alpha8_aliases"],
            serde_json::json!(["RDFS_FULL"])
        );
        assert_eq!(res.content["input_count"], 0);
    }

    #[test]
    fn reason_accepts_java_alpha8_profile_aliases() {
        for (alias, canonical) in [
            ("NONE", "None"),
            ("RDFS_MINIMAL", "RDFS"),
            ("RDFS_FULL", "RDFS-Full"),
            ("OWL2_QL", "OWL-QL"),
            ("OWL2_EL", "OWL2-EL"),
            ("OWL2_RL", "OWL2-RL"),
        ] {
            let res = run(
                "reason",
                ToolInvocation::new()
                    .with_arg("profile", serde_json::json!(alias))
                    .with_arg("triples", serde_json::json!([])),
            );
            assert!(!res.is_error, "{alias}: {:?}", res.content);
            assert_eq!(res.content["profile"], canonical, "{alias}");
        }
    }

    #[test]
    fn reason_trims_java_alpha8_profile_aliases() {
        let res = run(
            "reason",
            ToolInvocation::new()
                .with_arg("profile", serde_json::json!(" OWL2_QL "))
                .with_arg("triples", serde_json::json!([])),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["profile"], "OWL-QL");
    }

    #[test]
    fn reason_schema_profile_is_optional_with_java_alpha8_default() {
        let schema = reason_tool().input_schema();
        assert!(!schema.required.contains(&"profile".to_string()));
        assert!(schema.required.contains(&"triples".to_string()));
        assert!(!schema.properties.contains_key("namespace"));
        assert_eq!(schema.properties["profile"]["default"], "RDFS_FULL");
        assert!(schema.properties["profile"].get("enum").is_none());
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
    fn reason_rejects_unadvertised_java_custom_profile() {
        let res = run(
            "reason",
            ToolInvocation::new()
                .with_arg("profile", serde_json::json!("CUSTOM"))
                .with_arg("triples", serde_json::json!([])),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("unknown reasoning profile"));
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

    fn run_with_memory_and_reasoning(
        store: Arc<InMemoryMemoryStore>,
        registration: Arc<ReasoningRegistration>,
        name: &str,
        args: ToolInvocation,
    ) -> ToolResult {
        let mut reg = ToolRegistry::new();
        reg.install(store_memory_tool(store.clone()));
        reg.install(register_reasoning_tool(store.clone(), registration.clone()));
        reg.install(recall_memory_tool_with_reasoning(
            store.clone(),
            registration,
        ));
        reg.install(forget_memory_tool(store));
        reg.call(name, &args).expect("dispatch")
    }

    fn store_fact(
        store: Arc<InMemoryMemoryStore>,
        graph: Option<&str>,
        subject: &str,
        predicate: &str,
        object: &str,
        object_type: &str,
    ) {
        let mut args = ToolInvocation::new().with_arg(
            "facts",
            serde_json::json!([{
                "subject": subject,
                "predicate": predicate,
                "object": object,
                "objectType": object_type
            }]),
        );
        if let Some(graph) = graph {
            args = args.with_arg("graph", serde_json::json!(graph));
        }
        let res = run_with_memory(store, "store_memory", args);
        assert!(!res.is_error, "{:?}", res.content);
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
                            "objectType": "literal",
                            "datatype": "xsd:string",
                            "language": "en"
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
        assert_eq!(
            stored[0].facts[1].datatype.as_deref(),
            Some("http://www.w3.org/2001/XMLSchema#string")
        );
        assert_eq!(stored[0].facts[1].language.as_deref(), Some("en"));
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
    fn store_memory_allows_non_user_non_reserved_graphs_and_pattern_recall_finds_them() {
        let store = InMemoryMemoryStore::shared();
        let store_result = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("http://ex/private-graph"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/likes",
                        "object": "Sensors"
                    }]),
                ),
        );
        assert!(!store_result.is_error, "{:?}", store_result.content);

        let recall = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/likes",
                    "object": "Sensors"
                }),
            ),
        );
        assert!(!recall.is_error, "{:?}", recall.content);
        assert_eq!(recall.content["count"], 1);
        assert_eq!(
            recall.content["facts"][0]["graph"],
            "http://ex/private-graph"
        );

        let scoped = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("http://ex/private-graph"))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "predicate": "http://ex/likes"
                    }),
                ),
        );
        assert!(!scoped.is_error, "{:?}", scoped.content);
        assert_eq!(scoped.content["count"], 1);
    }

    #[test]
    fn store_memory_expands_java_known_prefixes() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "cqels:Alice",
                    "predicate": "rdf:type",
                    "object": "cqels:Agent",
                    "objectType": "uri"
                }]),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);

        let stored = store.recall("default", "").unwrap();
        let stmt = &stored[0].facts[0];
        assert_eq!(stmt.subject, "cqels://ontology/Alice");
        assert_eq!(
            stmt.predicate,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
        assert_eq!(stmt.object, "cqels://ontology/Agent");
    }

    #[test]
    fn store_memory_accepts_other_absolute_iri_schemes() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "did:example:alice",
                    "predicate": "tag:example.org,2026:knows",
                    "object": "mailto:bob@example.org",
                    "objectType": "uri"
                }]),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);

        let stored = store.recall("default", "").unwrap();
        let stmt = &stored[0].facts[0];
        assert_eq!(stmt.subject, "did:example:alice");
        assert_eq!(stmt.predicate, "tag:example.org,2026:knows");
        assert_eq!(stmt.object, "mailto:bob@example.org");
        assert_eq!(stmt.object_type, "uri");
    }

    #[test]
    fn store_memory_validates_bracketed_iris_are_absolute() {
        let store = InMemoryMemoryStore::shared();
        let valid = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "<http://ex/Alice>",
                    "predicate": "<http://ex/likes>",
                    "object": "<http://ex/Sensors>",
                    "objectType": "uri"
                }]),
            ),
        );
        assert!(!valid.is_error, "{:?}", valid.content);

        let invalid = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "<Alice>",
                    "predicate": "http://ex/likes",
                    "object": "Sensors"
                }]),
            ),
        );
        assert!(invalid.is_error);
        assert!(invalid.content["message"]
            .as_str()
            .unwrap()
            .contains("must be absolute"));
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
        assert_eq!(res.content["format"], "json");
        assert_eq!(res.content["facts"][0]["memory_id"], "alice");
        assert_eq!(res.content["facts"][0]["objectType"], "uri");
        assert!(res.content["facts"][0].get("object_type").is_none());
    }

    #[test]
    fn recall_memory_uses_expanded_prefix_pattern() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "cqels:Alice",
                    "predicate": "rdf:type",
                    "object": "cqels:Agent",
                    "objectType": "uri"
                }]),
            ),
        );

        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "rdf:type",
                    "object": "cqels:Agent",
                    "objectType": "uri"
                }),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["count"], 1);
    }

    #[test]
    fn store_memory_parses_turtle_into_pattern_rows() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("ttl"))
                .with_arg("graph", serde_json::json!("cqels://memory/user/turtle"))
                .with_arg("meta", serde_json::json!({"source":"ttl-test"}))
                .with_arg(
                    "turtle",
                    serde_json::json!(
                        "@prefix cqels: <cqels://ontology/> . cqels:Alice cqels:knows cqels:Bob ."
                    ),
                ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["fact_count"], 1);

        let recall = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/turtle"))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "subject": "cqels:Alice",
                        "predicate": "cqels:knows",
                        "object": "cqels:Bob"
                    }),
                ),
        );
        assert!(!recall.is_error, "{:?}", recall.content);
        assert_eq!(recall.content["count"], 1);
        assert_eq!(recall.content["facts"][0]["s"], "cqels://ontology/Alice");
        assert_eq!(recall.content["facts"][0]["p"], "cqels://ontology/knows");
        assert_eq!(recall.content["facts"][0]["o"], "cqels://ontology/Bob");
        assert_eq!(recall.content["facts"][0]["objectType"], "uri");
        assert_eq!(recall.content["facts"][0]["meta"]["source"], "ttl-test");
    }

    #[test]
    fn store_memory_skolemizes_turtle_blank_nodes() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("blank-turtle"))
                .with_arg(
                    "turtle",
                    serde_json::json!(
                        "@prefix foaf: <http://xmlns.com/foaf/0.1/> . _:alice foaf:knows _:bob ."
                    ),
                ),
        );
        assert!(!res.is_error, "{:?}", res.content);

        let recall = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://xmlns.com/foaf/0.1/knows"
                }),
            ),
        );
        assert!(!recall.is_error, "{:?}", recall.content);
        assert_eq!(recall.content["count"], 1);
        assert!(recall.content["facts"][0]["s"]
            .as_str()
            .unwrap()
            .starts_with("urn:cqels:bnode:"));
        assert!(recall.content["facts"][0]["o"]
            .as_str()
            .unwrap()
            .starts_with("urn:cqels:bnode:"));
        assert_eq!(recall.content["facts"][0]["objectType"], "uri");
    }

    #[test]
    fn skolemized_blank_node_escapes_non_safe_bytes_injectively() {
        assert_eq!(skolemized_blank_node("a/b"), "urn:cqels:bnode:a%2fb");
        assert_ne!(skolemized_blank_node("a1"), skolemized_blank_node("a\u{1}"));
    }

    #[test]
    fn store_memory_preserves_turtle_literal_metadata() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "turtle",
                serde_json::json!(
                    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \
                     <http://ex/Alice> <http://ex/age> \"42\"^^xsd:integer ; \
                     <http://ex/name> \"Alice\"@en ."
                ),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["fact_count"], 2);

        let age = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/age"
                }),
            ),
        );
        assert!(!age.is_error, "{:?}", age.content);
        assert_eq!(age.content["count"], 1);
        assert_eq!(age.content["facts"][0]["o"], "42");
        assert_eq!(age.content["facts"][0]["objectType"], "literal");
        assert_eq!(
            age.content["facts"][0]["datatype"],
            "http://www.w3.org/2001/XMLSchema#integer"
        );

        let name = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/name"
                }),
            ),
        );
        assert!(!name.is_error, "{:?}", name.content);
        assert_eq!(name.content["count"], 1);
        assert_eq!(name.content["facts"][0]["o"], "Alice");
        assert_eq!(name.content["facts"][0]["language"], "en");
        assert_eq!(
            name.content["facts"][0]["datatype"],
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        );
    }

    #[test]
    fn store_memory_rejects_malformed_turtle() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "store_memory",
            ToolInvocation::new().with_arg("turtle", serde_json::json!("@prefix broken")),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("turtle parse error"));
    }

    #[test]
    fn recall_memory_scopes_patterns_to_user_fact_graphs() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/status",
                    "object": "active"
                }]),
            ),
        );
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Bob",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("memory", serde_json::json!("shortterm"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Charlie",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );

        let all_user_graphs = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/status",
                    "object": "active"
                }),
            ),
        );
        assert!(!all_user_graphs.is_error, "{:?}", all_user_graphs.content);
        assert_eq!(all_user_graphs.content["count"], 2);

        let project_graph = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project")),
        );
        assert!(!project_graph.is_error, "{:?}", project_graph.content);
        assert_eq!(project_graph.content["count"], 1);
        assert_eq!(
            project_graph.content["facts"][0]["graph"],
            "cqels://memory/user/project"
        );
    }

    #[test]
    fn recall_memory_supports_pattern_graph_and_top_level_graph_precedence() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Default",
                    "predicate": "http://ex/status",
                    "object": "active"
                }]),
            ),
        );
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Project",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );

        let pattern_graph = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "graph": "cqels://memory/user/project",
                    "predicate": "http://ex/status"
                }),
            ),
        );
        assert!(!pattern_graph.is_error, "{:?}", pattern_graph.content);
        assert_eq!(pattern_graph.content["count"], 1);
        assert_eq!(pattern_graph.content["facts"][0]["s"], "http://ex/Project");

        let top_level_wins = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/longterm"))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "graph": "cqels://memory/user/project",
                        "predicate": "http://ex/status"
                    }),
                ),
        );
        assert!(!top_level_wins.is_error, "{:?}", top_level_wins.content);
        assert_eq!(top_level_wins.content["count"], 1);
        assert_eq!(top_level_wins.content["facts"][0]["s"], "http://ex/Default");
    }

    #[test]
    fn recall_memory_rejects_reserved_graph_scope() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("graph", serde_json::json!("cqels://memory/policy")),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("reserved"));
    }

    #[test]
    fn recall_memory_rejects_unimplemented_non_json_format() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg("format", serde_json::json!("turtle")),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("limited to 'json'"));
    }

    #[test]
    fn recall_memory_schema_only_advertises_json_format() {
        let schema = recall_memory_tool(InMemoryMemoryStore::shared()).input_schema();
        let format_enum = schema.properties["format"]["enum"]
            .as_array()
            .expect("format enum");
        assert_eq!(format_enum.as_slice(), &[serde_json::json!("json")]);
    }

    #[test]
    fn register_reasoning_defaults_to_java_alpha8_rdfs_full() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "register_reasoning",
            ToolInvocation::new(),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["registered"], true);
        assert_eq!(res.content["namespace"], DEFAULT_NAMESPACE);
        assert_eq!(res.content["profile"], "RDFS-Full");
        assert_eq!(res.content["schemaGraph"], SCHEMA_GRAPH);
        assert_eq!(res.content["dataGraph"], LONGTERM_GRAPH);
    }

    #[test]
    fn register_reasoning_schema_advertises_alpha8_defaults() {
        let schema = register_reasoning_tool(
            InMemoryMemoryStore::shared(),
            ReasoningRegistration::shared(),
        )
        .input_schema();
        assert_eq!(schema.properties["namespace"]["default"], DEFAULT_NAMESPACE);
        assert_eq!(schema.properties["profile"]["default"], "RDFS_FULL");
        assert_eq!(schema.properties["schemaGraph"]["default"], SCHEMA_GRAPH);
        assert_eq!(schema.properties["dataGraph"]["default"], LONGTERM_GRAPH);
    }

    #[test]
    fn register_reasoning_blank_graph_args_reset_to_defaults() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "register_reasoning",
            ToolInvocation::new()
                .with_arg("schemaGraph", serde_json::json!(""))
                .with_arg("dataGraph", serde_json::json!(" ")),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["schemaGraph"], SCHEMA_GRAPH);
        assert_eq!(res.content["dataGraph"], LONGTERM_GRAPH);
    }

    #[test]
    fn register_reasoning_rejects_unadvertised_java_custom_profile() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "register_reasoning",
            ToolInvocation::new().with_arg("profile", serde_json::json!("CUSTOM")),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("unknown reasoning profile"));
    }

    #[test]
    fn register_reasoning_rejects_reserved_graphs() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "register_reasoning",
            ToolInvocation::new().with_arg("dataGraph", serde_json::json!(INFERRED_GRAPH)),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("reserved"));
    }

    #[test]
    fn recall_memory_entail_requires_register_reasoning() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg("pattern", serde_json::json!({"subject": "http://ex/Alice"})),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("call register_reasoning first"));
    }

    #[test]
    fn recall_memory_entail_rejects_text_mode() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let register = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "register_reasoning",
            ToolInvocation::new(),
        );
        assert!(!register.is_error, "{:?}", register.content);

        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg("text", serde_json::json!("Alice"))
                .with_arg("pattern", serde_json::json!({"subject": "http://ex/Alice"})),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("plain pattern recall"));
    }

    #[test]
    fn recall_memory_entail_rejects_graph_filters() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let register = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "register_reasoning",
            ToolInvocation::new(),
        );
        assert!(!register.is_error, "{:?}", register.content);

        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg("graph", serde_json::json!(LONGTERM_GRAPH))
                .with_arg("pattern", serde_json::json!({"subject": "http://ex/Alice"})),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("omit graph filters"));
    }

    #[test]
    fn recall_memory_entail_returns_asserted_plus_inferred_rows() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        store_fact(
            store.clone(),
            Some(SCHEMA_GRAPH),
            "http://ex/Person",
            "rdfs:subClassOf",
            "http://ex/Agent",
            "uri",
        );
        store_fact(
            store.clone(),
            None,
            "http://ex/Alice",
            "rdf:type",
            "http://ex/Person",
            "uri",
        );

        let register = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "register_reasoning",
            ToolInvocation::new().with_arg("profile", serde_json::json!("RDFS_FULL")),
        );
        assert!(!register.is_error, "{:?}", register.content);
        assert_eq!(register.content["profile"], "RDFS-Full");
        assert!(register.content["entailed_count"].as_u64().unwrap() >= 1);

        let inferred = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "subject": "http://ex/Alice",
                        "predicate": "rdf:type",
                        "object": "http://ex/Agent",
                        "objectType": "uri"
                    }),
                ),
        );
        assert!(!inferred.is_error, "{:?}", inferred.content);
        assert_eq!(inferred.content["entail"], true);
        assert_eq!(inferred.content["profile"], "RDFS-Full");
        assert_eq!(inferred.content["count"], 1);
        assert_eq!(inferred.content["facts"][0]["s"], "http://ex/Alice");
        assert_eq!(inferred.content["facts"][0]["o"], "http://ex/Agent");
        assert_eq!(inferred.content["facts"][0]["inferred"], true);

        let all_types = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "subject": "http://ex/Alice",
                        "predicate": "rdf:type"
                    }),
                ),
        );
        assert!(!all_types.is_error, "{:?}", all_types.content);
        let facts = all_types.content["facts"].as_array().expect("facts");
        assert!(facts
            .iter()
            .any(|row| row["o"] == "http://ex/Person" && row["inferred"] == false));
        assert!(facts
            .iter()
            .any(|row| row["o"] == "http://ex/Agent" && row["inferred"] == true));
    }

    #[test]
    fn recall_memory_entail_computes_two_hop_rdfs_closure() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        store_fact(
            store.clone(),
            Some(SCHEMA_GRAPH),
            "http://ex/Person",
            "rdfs:subClassOf",
            "http://ex/Human",
            "uri",
        );
        store_fact(
            store.clone(),
            Some(SCHEMA_GRAPH),
            "http://ex/Human",
            "rdfs:subClassOf",
            "http://ex/Agent",
            "uri",
        );
        store_fact(
            store.clone(),
            None,
            "http://ex/Alice",
            "rdf:type",
            "http://ex/Person",
            "uri",
        );
        let register = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "register_reasoning",
            ToolInvocation::new().with_arg("profile", serde_json::json!("RDFS_FULL")),
        );
        assert!(!register.is_error, "{:?}", register.content);

        let res = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("entail", serde_json::json!(true))
                .with_arg(
                    "pattern",
                    serde_json::json!({
                        "subject": "http://ex/Alice",
                        "predicate": "rdf:type",
                        "object": "http://ex/Agent",
                        "objectType": "uri"
                    }),
                ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["count"], 1);
        assert_eq!(res.content["facts"][0]["o"], "http://ex/Agent");
        assert_eq!(res.content["facts"][0]["inferred"], true);
    }

    #[test]
    fn recall_memory_entail_registration_is_namespace_scoped() {
        let store = InMemoryMemoryStore::shared();
        let registration = ReasoningRegistration::shared();
        let register = run_with_memory_and_reasoning(
            store.clone(),
            registration.clone(),
            "register_reasoning",
            ToolInvocation::new().with_arg("namespace", serde_json::json!("tenant-a")),
        );
        assert!(!register.is_error, "{:?}", register.content);
        assert_eq!(register.content["namespace"], "tenant-a");

        let other_namespace = run_with_memory_and_reasoning(
            store,
            registration,
            "recall_memory",
            ToolInvocation::new()
                .with_arg("namespace", serde_json::json!("tenant-b"))
                .with_arg("entail", serde_json::json!(true))
                .with_arg("pattern", serde_json::json!({"subject": "http://ex/Alice"})),
        );
        assert!(other_namespace.is_error);
        assert!(other_namespace.content["message"]
            .as_str()
            .unwrap()
            .contains("call register_reasoning first"));
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
        assert_eq!(res.content["format"], "json");
        assert_eq!(res.content["count"], DEFAULT_RECALL_LIMIT);
        assert_eq!(
            res.content["facts"].as_array().unwrap().len(),
            DEFAULT_RECALL_LIMIT
        );
        assert_eq!(res.content["facts"][0]["id"], "fact-00");
        assert!(res.content["facts"][0]["s"].is_null());
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
        assert_eq!(res.content["deleted"], true);
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
        assert_eq!(res.content["deleted"], false);
    }

    #[test]
    fn forget_memory_rejects_missing_action() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(store, "forget_memory", ToolInvocation::new());
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("provide exactly one"));
    }

    #[test]
    fn forget_memory_schema_requires_exactly_one_action() {
        let schema = forget_memory_tool(InMemoryMemoryStore::shared()).input_schema();
        assert_eq!(schema.one_of.len(), 3);
        assert_eq!(schema.one_of[0]["required"], serde_json::json!(["id"]));
        assert_eq!(schema.one_of[1]["required"], serde_json::json!(["graph"]));
        assert_eq!(schema.one_of[2]["required"], serde_json::json!(["facts"]));
    }

    #[test]
    fn forget_memory_rejects_conflicting_actions() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "forget_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("k"))
                .with_arg("graph", serde_json::json!("cqels://memory/longterm")),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("exactly one"));
    }

    #[test]
    fn forget_memory_rejects_empty_fact_removal() {
        let store = InMemoryMemoryStore::shared();
        let res = run_with_memory(
            store,
            "forget_memory",
            ToolInvocation::new().with_arg("facts", serde_json::json!([])),
        );
        assert!(res.is_error);
        assert!(res.content["message"]
            .as_str()
            .unwrap()
            .contains("at least one fact"));
    }

    #[test]
    fn forget_memory_clears_named_graph_records() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Default",
                    "predicate": "http://ex/status",
                    "object": "active"
                }]),
            ),
        );
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Project",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );

        let res = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project")),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["records_removed"], 1);

        let project = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new()
                .with_arg("graph", serde_json::json!("cqels://memory/user/project")),
        );
        assert!(!project.is_error, "{:?}", project.content);
        assert_eq!(project.content["count"], 0);

        let default = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/status"
                }),
            ),
        );
        assert!(!default.is_error, "{:?}", default.content);
        assert_eq!(default.content["count"], 1);
        assert_eq!(default.content["facts"][0]["s"], "http://ex/Default");
    }

    #[test]
    fn forget_memory_removes_matching_structured_facts() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("bundle"))
                .with_arg(
                    "facts",
                    serde_json::json!([
                        {
                            "subject": "http://ex/Alice",
                            "predicate": "http://ex/likes",
                            "object": "http://ex/Sensors",
                            "objectType": "uri"
                        },
                        {
                            "subject": "http://ex/Alice",
                            "predicate": "http://ex/name",
                            "object": "Alice",
                            "datatype": "xsd:string",
                            "language": "en"
                        }
                    ]),
                ),
        );

        let res = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/name",
                    "object": "Alice",
                    "datatype": "xsd:string",
                    "language": "en"
                }]),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["statements_removed"], 1);
        assert_eq!(res.content["records_updated"], 1);
        assert_eq!(res.content["records_deleted"], 0);

        let stored = store.recall("default", "").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].facts.len(), 1);
        assert_eq!(stored[0].facts[0].predicate, "http://ex/likes");

        let name = run_with_memory(
            store.clone(),
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/name"
                }),
            ),
        );
        assert!(!name.is_error, "{:?}", name.content);
        assert_eq!(name.content["count"], 0);

        let likes = run_with_memory(
            store,
            "recall_memory",
            ToolInvocation::new().with_arg(
                "pattern",
                serde_json::json!({
                    "predicate": "http://ex/likes"
                }),
            ),
        );
        assert!(!likes.is_error, "{:?}", likes.content);
        assert_eq!(likes.content["count"], 1);
    }

    #[test]
    fn forget_memory_clears_stale_turtle_after_structured_fact_removal() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("mixed"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                )
                .with_arg(
                    "turtle",
                    serde_json::json!("<http://ex/Bob> <http://ex/status> \"active\" ."),
                ),
        );
        let before = store.recall("default", "").unwrap();
        assert!(before[0].turtle.is_some());
        assert_eq!(before[0].facts.len(), 2);

        let res = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/status",
                    "object": "active"
                }]),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["statements_removed"], 1);

        let after = store.recall("default", "").unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].facts.len(), 1);
        assert_eq!(after[0].facts[0].subject, "http://ex/Bob");
        assert!(after[0].turtle.is_none());
        assert!(after[0].content.contains("\"turtle\":null"));
        assert!(!after[0].content.contains("http://ex/Alice"));
    }

    #[test]
    fn forget_memory_deletes_record_when_last_structured_fact_is_removed() {
        let store = InMemoryMemoryStore::shared();
        run_with_memory(
            store.clone(),
            "store_memory",
            ToolInvocation::new()
                .with_arg("id", serde_json::json!("single"))
                .with_arg(
                    "facts",
                    serde_json::json!([{
                        "subject": "http://ex/Alice",
                        "predicate": "http://ex/status",
                        "object": "active"
                    }]),
                ),
        );

        let res = run_with_memory(
            store.clone(),
            "forget_memory",
            ToolInvocation::new().with_arg(
                "facts",
                serde_json::json!([{
                    "subject": "http://ex/Alice",
                    "predicate": "http://ex/status",
                    "object": "active"
                }]),
            ),
        );
        assert!(!res.is_error, "{:?}", res.content);
        assert_eq!(res.content["statements_removed"], 1);
        assert_eq!(res.content["records_updated"], 0);
        assert_eq!(res.content["records_deleted"], 1);
        assert_eq!(store.len("default").unwrap(), 0);
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
