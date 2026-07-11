//! MCP resource descriptors and read handlers for CQELS metadata.
//!
//! Java alpha.8 exposes four knowledge-graph resources plus a query
//! results resource template. This module keeps the same protocol
//! surface independent of a concrete transport so stdio, HTTP/SSE, or
//! future SDK-based hosts can share the resource registry.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use cqels_reasoning::{capabilities_for, ReasoningCapability, ReasoningProfile};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};

use crate::stream_query::StreamQueryHub;

/// Default short-term memory stream name used by the Java MCP server.
pub const DEFAULT_STREAM: &str = "shortterm";

/// Knowledge graph statistics resource URI.
pub const RESOURCE_KG_STATS: &str = "cqels://kg/stats";

/// Known namespace prefix resource URI.
pub const RESOURCE_KG_NAMESPACES: &str = "cqels://kg/namespaces";

/// Engine status resource URI.
pub const RESOURCE_ENGINE_STATUS: &str = "cqels://engine/status";

/// CqelsQL authoring guide resource URI.
pub const RESOURCE_DOC_CQELSQL: &str = "cqels://docs/cqelsql";

/// CEP authoring guide resource URI.
pub const RESOURCE_DOC_CEP: &str = "cqels://docs/cep";

/// Active stream list resource URI.
pub const RESOURCE_STREAMS: &str = "cqels://streams";

/// Registered query list resource URI.
pub const RESOURCE_QUERIES: &str = "cqels://queries";

/// Reasoning profile capability resource URI.
pub const RESOURCE_REASONING: &str = "cqels://reasoning/capabilities";

/// Subscribable per-query results template URI.
pub const RESOURCE_QUERY_RESULTS_TEMPLATE: &str = "cqels://queries/{queryId}/results";

const RESOURCE_QUERY_RESULTS_PREFIX: &str = "cqels://queries/";
const RESOURCE_QUERY_RESULTS_SUFFIX: &str = "/results";

/// Constructs the concrete result-buffer resource URI for `query_id`.
pub fn query_results_uri(query_id: &str) -> String {
    format!("{RESOURCE_QUERY_RESULTS_PREFIX}{query_id}{RESOURCE_QUERY_RESULTS_SUFFIX}")
}

/// Extracts a query id from `cqels://queries/{queryId}/results`.
pub fn query_id_from_results_uri(uri: &str) -> Option<&str> {
    if !uri.starts_with(RESOURCE_QUERY_RESULTS_PREFIX)
        || !uri.ends_with(RESOURCE_QUERY_RESULTS_SUFFIX)
        || uri.len() < RESOURCE_QUERY_RESULTS_PREFIX.len() + RESOURCE_QUERY_RESULTS_SUFFIX.len()
    {
        return None;
    }
    let id =
        &uri[RESOURCE_QUERY_RESULTS_PREFIX.len()..uri.len() - RESOURCE_QUERY_RESULTS_SUFFIX.len()];
    (!id.is_empty()).then_some(id)
}

/// MCP resource descriptor returned by `resources/list`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResourceDescriptor {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

impl ResourceDescriptor {
    fn json(uri: &str, name: &str, description: &str) -> Self {
        Self {
            uri: uri.to_string(),
            name: name.to_string(),
            description: Some(description.to_string()),
            mime_type: Some("application/json".to_string()),
        }
    }

    fn markdown(uri: &str, name: &str, description: &str) -> Self {
        Self {
            uri: uri.to_string(),
            name: name.to_string(),
            description: Some(description.to_string()),
            mime_type: Some("text/markdown".to_string()),
        }
    }
}

/// MCP resource template descriptor returned by `resources/templates/list`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResourceTemplateDescriptor {
    #[serde(rename = "uriTemplate")]
    pub uri_template: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

impl ResourceTemplateDescriptor {
    fn json(uri_template: &str, name: &str, description: &str) -> Self {
        Self {
            uri_template: uri_template.to_string(),
            name: name.to_string(),
            description: Some(description.to_string()),
            mime_type: Some("application/json".to_string()),
        }
    }
}

/// Text resource content returned by `resources/read`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResourceContent {
    pub uri: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    pub text: String,
}

/// MCP `resources/read` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadResourceResult {
    pub contents: Vec<ResourceContent>,
}

/// Resource registry error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceError {
    UnknownResource(String),
    Serialize(String),
}

impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownResource(uri) => write!(f, "unknown resource '{uri}'"),
            Self::Serialize(message) => write!(f, "resource serialization failed: {message}"),
        }
    }
}

impl std::error::Error for ResourceError {}

struct StaticResource {
    descriptor: ResourceDescriptor,
    reader: Arc<dyn Fn() -> Result<ReadResourceResult, ResourceError> + Send + Sync>,
}

impl StaticResource {
    fn json(
        descriptor: ResourceDescriptor,
        reader: impl Fn() -> JsonValue + Send + Sync + 'static,
    ) -> Self {
        let uri = descriptor.uri.clone();
        Self {
            descriptor,
            reader: Arc::new(move || json_result(&uri, reader())),
        }
    }

    fn text(descriptor: ResourceDescriptor, text: &'static str) -> Self {
        let uri = descriptor.uri.clone();
        let mime_type = descriptor
            .mime_type
            .clone()
            .unwrap_or_else(|| "text/plain".to_string());
        Self {
            descriptor,
            reader: Arc::new(move || text_result(&uri, &mime_type, text)),
        }
    }

    fn read(&self) -> Result<ReadResourceResult, ResourceError> {
        (self.reader)()
    }
}

struct QueryResultsTemplate {
    descriptor: ResourceTemplateDescriptor,
    hub: Option<StreamQueryHub>,
}

impl QueryResultsTemplate {
    fn new(hub: Option<StreamQueryHub>) -> Self {
        Self {
            descriptor: ResourceTemplateDescriptor::json(
                RESOURCE_QUERY_RESULTS_TEMPLATE,
                "Stream Query Results",
                "Buffered results for a registered stream query; reading drains the buffer",
            ),
            hub,
        }
    }

    fn matches(&self, uri: &str) -> bool {
        query_id_from_results_uri(uri).is_some()
    }

    fn read(&self, uri: &str) -> Result<ReadResourceResult, ResourceError> {
        let query_id = query_id_from_results_uri(uri)
            .ok_or_else(|| ResourceError::UnknownResource(uri.to_string()))?;
        let results = self
            .hub
            .as_ref()
            .map(|hub| hub.drain_result_values(query_id, usize::MAX))
            .unwrap_or_default();

        json_result(
            uri,
            json!({
                "queryId": query_id,
                "results": results,
            }),
        )
    }
}

/// Registry for static resources plus dynamic resource templates.
pub struct ResourceRegistry {
    resources: BTreeMap<String, StaticResource>,
    resource_order: Vec<String>,
    templates: Vec<QueryResultsTemplate>,
}

impl ResourceRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            resources: BTreeMap::new(),
            resource_order: Vec::new(),
            templates: Vec::new(),
        }
    }

    fn install_json(
        mut self,
        descriptor: ResourceDescriptor,
        reader: impl Fn() -> JsonValue + Send + Sync + 'static,
    ) -> Self {
        let uri = descriptor.uri.clone();
        if !self.resources.contains_key(&uri) {
            self.resource_order.push(uri.clone());
        }
        self.resources
            .insert(uri, StaticResource::json(descriptor, reader));
        self
    }

    fn install_text(mut self, descriptor: ResourceDescriptor, text: &'static str) -> Self {
        let uri = descriptor.uri.clone();
        if !self.resources.contains_key(&uri) {
            self.resource_order.push(uri.clone());
        }
        self.resources
            .insert(uri, StaticResource::text(descriptor, text));
        self
    }

    fn install_query_results_template(mut self, hub: Option<StreamQueryHub>) -> Self {
        self.templates.push(QueryResultsTemplate::new(hub));
        self
    }

    /// Returns all static resource descriptors in registration order.
    pub fn list(&self) -> Vec<ResourceDescriptor> {
        self.resource_order
            .iter()
            .filter_map(|uri| self.resources.get(uri))
            .map(|resource| resource.descriptor.clone())
            .collect()
    }

    /// Returns all resource template descriptors.
    pub fn list_templates(&self) -> Vec<ResourceTemplateDescriptor> {
        self.templates
            .iter()
            .map(|template| template.descriptor.clone())
            .collect()
    }

    /// Returns whether `uri` can be read by this registry.
    pub fn contains(&self, uri: &str) -> bool {
        self.resources.contains_key(uri) || self.templates.iter().any(|t| t.matches(uri))
    }

    /// Reads a static resource or matching resource template.
    pub fn read(&self, uri: &str) -> Result<ReadResourceResult, ResourceError> {
        if let Some(resource) = self.resources.get(uri) {
            return resource.read();
        }
        for template in &self.templates {
            if template.matches(uri) {
                return template.read(uri);
            }
        }
        Err(ResourceError::UnknownResource(uri.to_string()))
    }
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the stateless CQELS resource registry used by the shipped stdio binary.
pub fn cqels_resource_registry() -> ResourceRegistry {
    cqels_resource_registry_with_hub(None)
}

/// Builds a CQELS resource registry backed by live stream-query state.
pub fn cqels_resource_registry_with_streams(hub: StreamQueryHub) -> ResourceRegistry {
    cqels_resource_registry_with_hub(Some(hub))
}

fn cqels_resource_registry_with_hub(hub: Option<StreamQueryHub>) -> ResourceRegistry {
    let stats_hub = hub.clone();
    let namespaces_hub = hub.clone();
    let status_hub = hub.clone();
    let streams_hub = hub.clone();
    let queries_hub = hub.clone();

    ResourceRegistry::new()
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_KG_STATS,
                "Knowledge Graph Statistics",
                "Triple count, named graphs, active streams and queries",
            ),
            move || {
                let query_ids = query_ids(stats_hub.as_ref());
                json!({
                    "tripleCount": 0,
                    "namedGraphs": 0,
                    "registeredQueries": query_ids.len(),
                    "queryIds": query_ids,
                })
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_KG_NAMESPACES,
                "Knowledge Graph Namespaces",
                "Known namespace prefixes available to MCP tools",
            ),
            move || {
                let mut namespaces = known_namespaces();
                if namespaces_hub.is_some() {
                    namespaces.insert("stream".to_string(), "cqels://stream/".to_string());
                }
                json!({ "namespaces": namespaces })
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_ENGINE_STATUS,
                "Engine Status",
                "Runtime status, active streams, and registered query IDs",
            ),
            move || {
                let query_ids = query_ids(status_hub.as_ref());
                let streams = stream_names(status_hub.as_ref());
                let stream_reasoning = status_hub
                    .as_ref()
                    .and_then(StreamQueryHub::stream_reasoning_profile);
                json!({
                    "running": status_hub.is_some(),
                    "registeredQueries": query_ids.len(),
                    "queryIds": query_ids,
                    "streams": streams,
                    "streamReasoning": stream_reasoning,
                    "features": {
                        "rdfMessages": true,
                        "pushStreamEvents": true,
                        "watchInvariant": true,
                        "registerRules": true,
                    }
                })
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_STREAMS,
                "Active Streams",
                "List of active data streams",
            ),
            move || {
                let mut streams = stream_names(streams_hub.as_ref());
                if streams.is_empty() {
                    streams.push(DEFAULT_STREAM.to_string());
                }
                json!({ "streams": streams })
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_QUERIES,
                "Registered Queries",
                "List of registered continuous queries with buffered result counts",
            ),
            move || {
                let queries = match queries_hub.as_ref() {
                    Some(hub) => query_ids(Some(hub))
                        .into_iter()
                        .map(|query_id| {
                            json!({
                                "queryId": query_id,
                                "buffered": hub.buffered_count(&query_id),
                            })
                        })
                        .collect::<Vec<_>>(),
                    None => Vec::new(),
                };
                json!({ "queries": queries })
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_REASONING,
                "Reasoning Capabilities",
                "Available reasoning profiles and their inference capabilities",
            ),
            || {
                let profiles = [
                    ReasoningProfile::None,
                    ReasoningProfile::Rdfs,
                    ReasoningProfile::RdfsFull,
                    ReasoningProfile::OwlLite,
                    ReasoningProfile::OwlQl,
                    ReasoningProfile::Owl2El,
                    ReasoningProfile::Owl2Rl,
                ]
                .into_iter()
                .map(|profile| {
                    let mut capabilities = capabilities_for(profile)
                        .into_iter()
                        .map(capability_name)
                        .collect::<Vec<_>>();
                    capabilities.sort();
                    json!({
                        "name": profile.name(),
                        "capabilities": capabilities,
                    })
                })
                .collect::<Vec<_>>();
                json!({ "profiles": profiles })
            },
        )
        .install_text(
            ResourceDescriptor::markdown(
                RESOURCE_DOC_CQELSQL,
                "CqelsQL Guide",
                "Compact CqelsQL syntax guide for MCP agents",
            ),
            CQELSQL_DOC,
        )
        .install_text(
            ResourceDescriptor::markdown(
                RESOURCE_DOC_CEP,
                "CEP Guide",
                "Compact CEP syntax guide for MCP agents",
            ),
            CEP_DOC,
        )
        .install_query_results_template(hub)
}

fn query_ids(hub: Option<&StreamQueryHub>) -> Vec<String> {
    let mut ids = hub
        .map(StreamQueryHub::registered_query_ids)
        .unwrap_or_default();
    ids.sort();
    ids
}

fn stream_names(hub: Option<&StreamQueryHub>) -> Vec<String> {
    let mut names = hub
        .map(StreamQueryHub::registered_stream_names)
        .unwrap_or_default();
    names.sort();
    names
}

fn capability_name(capability: ReasoningCapability) -> String {
    format!("{capability:?}")
}

fn known_namespaces() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        ),
        (
            "rdfs".to_string(),
            "http://www.w3.org/2000/01/rdf-schema#".to_string(),
        ),
        (
            "owl".to_string(),
            "http://www.w3.org/2002/07/owl#".to_string(),
        ),
        (
            "xsd".to_string(),
            "http://www.w3.org/2001/XMLSchema#".to_string(),
        ),
        ("sh".to_string(), "http://www.w3.org/ns/shacl#".to_string()),
        ("ex".to_string(), "http://example.org/".to_string()),
        ("cqels".to_string(), "cqels://ontology/".to_string()),
        ("sosa".to_string(), "http://www.w3.org/ns/sosa/".to_string()),
        (
            "saref".to_string(),
            "https://saref.etsi.org/core/".to_string(),
        ),
        (
            "qudt".to_string(),
            "http://qudt.org/schema/qudt/".to_string(),
        ),
        (
            "unit".to_string(),
            "http://qudt.org/vocab/unit/".to_string(),
        ),
    ])
}

fn json_result(uri: &str, value: JsonValue) -> Result<ReadResourceResult, ResourceError> {
    let text =
        serde_json::to_string(&value).map_err(|e| ResourceError::Serialize(e.to_string()))?;
    Ok(ReadResourceResult {
        contents: vec![ResourceContent {
            uri: uri.to_string(),
            mime_type: "application/json".to_string(),
            text,
        }],
    })
}

fn text_result(
    uri: &str,
    mime_type: &str,
    text: &'static str,
) -> Result<ReadResourceResult, ResourceError> {
    Ok(ReadResourceResult {
        contents: vec![ResourceContent {
            uri: uri.to_string(),
            mime_type: mime_type.to_string(),
            text: text.to_string(),
        }],
    })
}

const CQELSQL_DOC: &str = r#"# CqelsQL

Use `SELECT ... FROM STREAM name [RANGE 10s] WHERE { ... }` for live RDF streams.
Register durable live queries with `register_stream_query`; validate first with
`validate_stream_query`. Push data with `push_stream_events`.

Example:

```sparql
SELECT ?sensor ?value
FROM STREAM sensors [RANGE 10s]
WHERE { ?sensor <http://example.org/value> ?value . }
```
"#;

const CEP_DOC: &str = r#"# CQELS CEP

CEP registration through `register_stream_query` is intentionally fail-loud in
this Rust MCP transport until the CEP query path is wired to the live engine.
Use CqelsQL stream windows for alpha.10 live ingestion and continuous observers.
"#;

/// Canonical MCP notification payload for `notifications/resources/updated`.
pub fn resource_updated_notification(uri: &str) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/resources/updated",
        "params": { "uri": uri },
    })
}

/// Canonical resource update notification for a query result buffer.
pub fn query_results_updated_notification(query_id: &str) -> JsonValue {
    resource_updated_notification(&query_results_uri(query_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use cqels_engine::CqelsEngine;
    use tokio::runtime::Runtime;

    use crate::{register_stream_query_tool, ToolInvocation, ToolRegistry};

    fn read_json(registry: &ResourceRegistry, uri: &str) -> JsonValue {
        let result = registry.read(uri).expect("read resource");
        assert_eq!(result.contents.len(), 1);
        assert_eq!(result.contents[0].uri, uri);
        assert_eq!(result.contents[0].mime_type, "application/json");
        serde_json::from_str(&result.contents[0].text).expect("valid JSON resource text")
    }

    #[test]
    fn registry_lists_static_resources_and_query_results_template() {
        let registry = cqels_resource_registry();

        let uris = registry
            .list()
            .into_iter()
            .map(|resource| resource.uri)
            .collect::<Vec<_>>();
        assert_eq!(
            uris,
            vec![
                RESOURCE_KG_STATS.to_string(),
                RESOURCE_KG_NAMESPACES.to_string(),
                RESOURCE_ENGINE_STATUS.to_string(),
                RESOURCE_STREAMS.to_string(),
                RESOURCE_QUERIES.to_string(),
                RESOURCE_REASONING.to_string(),
                RESOURCE_DOC_CQELSQL.to_string(),
                RESOURCE_DOC_CEP.to_string(),
            ]
        );

        let templates = registry.list_templates();
        assert_eq!(templates.len(), 1);
        assert_eq!(
            templates[0].uri_template,
            RESOURCE_QUERY_RESULTS_TEMPLATE.to_string()
        );
    }

    #[test]
    fn static_resources_return_java_alpha8_keys() {
        let registry = cqels_resource_registry();

        let stats = read_json(&registry, RESOURCE_KG_STATS);
        assert!(stats["tripleCount"].is_number());
        assert!(stats["namedGraphs"].is_number());
        assert_eq!(stats["registeredQueries"], 0);
        assert!(stats["queryIds"].as_array().unwrap().is_empty());

        let streams = read_json(&registry, RESOURCE_STREAMS);
        assert_eq!(streams["streams"], json!([DEFAULT_STREAM]));

        let namespaces = read_json(&registry, RESOURCE_KG_NAMESPACES);
        assert_eq!(
            namespaces["namespaces"]["rdf"],
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        );

        let status = read_json(&registry, RESOURCE_ENGINE_STATUS);
        assert_eq!(status["running"], false);
        assert_eq!(status["registeredQueries"], 0);

        let queries = read_json(&registry, RESOURCE_QUERIES);
        assert!(queries["queries"].as_array().unwrap().is_empty());

        let reasoning = read_json(&registry, RESOURCE_REASONING);
        assert_eq!(reasoning["profiles"].as_array().unwrap().len(), 7);
        assert!(reasoning["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .any(|profile| profile["name"] == "OWL2-RL"));
    }

    #[test]
    fn query_result_template_matches_and_reads_empty_buffer_without_hub() {
        let registry = cqels_resource_registry();
        let uri = query_results_uri("q1");

        assert!(registry.contains(&uri));
        assert_eq!(query_id_from_results_uri(&uri), Some("q1"));

        let body = read_json(&registry, &uri);
        assert_eq!(body["queryId"], "q1");
        assert!(body["results"].as_array().unwrap().is_empty());
    }

    #[test]
    fn malformed_query_results_uri_does_not_match_template() {
        assert_eq!(query_id_from_results_uri("cqels://queries/results"), None);
        assert_eq!(query_id_from_results_uri("cqels://queries//results"), None);
        assert_eq!(query_id_from_results_uri("cqels://queries/q1"), None);
    }

    #[test]
    fn resource_updated_notifications_use_mcp_method_name() {
        assert_eq!(
            query_results_updated_notification("q1"),
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/resources/updated",
                "params": { "uri": "cqels://queries/q1/results" },
            })
        );
    }

    #[test]
    fn unknown_resource_returns_error() {
        let registry = cqels_resource_registry();
        let err = registry
            .read("cqels://missing")
            .expect_err("unknown resource");
        assert_eq!(
            err,
            ResourceError::UnknownResource("cqels://missing".to_string())
        );
    }

    #[test]
    fn docs_resources_return_markdown() {
        let registry = cqels_resource_registry();
        let result = registry.read(RESOURCE_DOC_CQELSQL).expect("read docs");
        assert_eq!(result.contents[0].mime_type, "text/markdown");
        assert!(result.contents[0].text.contains("CqelsQL"));
    }

    #[test]
    fn hub_backed_resources_report_live_streams_and_queries() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = CqelsEngine::builder().build().expect("engine builds");
        let _sender = runtime
            .block_on(async { engine.create_stream("sensors").await })
            .expect("create stream");
        let hub = StreamQueryHub::new(Arc::new(engine), runtime.handle().clone());
        let registry = cqels_resource_registry_with_streams(hub.clone());

        let streams = read_json(&registry, RESOURCE_STREAMS);
        assert_eq!(streams["streams"], json!(["sensors"]));

        let mut tools = ToolRegistry::new();
        tools.install(register_stream_query_tool(hub.clone()));
        let query = r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [RANGE 10s]
            WHERE { ?sensor <http://ex.org/temp> ?temp . }
        "#;
        let registered = tools
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(query)),
            )
            .expect("dispatch");
        assert!(
            !registered.is_error,
            "registration failed: {:?}",
            registered.content
        );
        let query_id = registered.content["query_id"].as_str().unwrap();

        let queries = read_json(&registry, RESOURCE_QUERIES);
        assert_eq!(queries["queries"].as_array().unwrap().len(), 1);
        assert_eq!(queries["queries"][0]["queryId"], query_id);
        assert_eq!(queries["queries"][0]["buffered"], 0);

        let stats = read_json(&registry, RESOURCE_KG_STATS);
        assert_eq!(stats["registeredQueries"], 1);
        assert!(stats["queryIds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id.as_str() == Some(query_id)));
    }
}
