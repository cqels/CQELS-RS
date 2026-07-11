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
use crate::tools::{governance_active, AccessPolicyRegistry};

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
const GOVERNED_METADATA_DENIAL: &str = "An access policy is active; resource metadata is withheld.";

/// Static runtime facts surfaced by the Java alpha.10
/// `cqels://engine/status` resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceRuntimeInfo {
    pub server_version: String,
    pub transport: String,
    pub persistence_enabled: bool,
    pub storage_backend: Option<String>,
    pub rdf_store_persistent: bool,
    pub max_registered_queries: Option<usize>,
}

impl Default for ResourceRuntimeInfo {
    fn default() -> Self {
        Self {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            transport: "unknown".to_string(),
            persistence_enabled: false,
            storage_backend: None,
            rdf_store_persistent: false,
            max_registered_queries: None,
        }
    }
}

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
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl QueryResultsTemplate {
    fn new(hub: Option<StreamQueryHub>, access_policy: Option<Arc<AccessPolicyRegistry>>) -> Self {
        Self {
            descriptor: ResourceTemplateDescriptor::json(
                RESOURCE_QUERY_RESULTS_TEMPLATE,
                "Stream Query Results",
                "Buffered results for a registered stream query; reading drains the buffer",
            ),
            hub,
            access_policy,
        }
    }

    fn matches(&self, uri: &str) -> bool {
        query_id_from_results_uri(uri).is_some()
    }

    fn read(&self, uri: &str) -> Result<ReadResourceResult, ResourceError> {
        let query_id = query_id_from_results_uri(uri)
            .ok_or_else(|| ResourceError::UnknownResource(uri.to_string()))?;
        if governance_active(self.access_policy.as_ref()) {
            return json_result(
                uri,
                json!({
                    "queryId": query_id,
                    "denied": GOVERNED_METADATA_DENIAL,
                    "results": [],
                }),
            );
        }
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

    fn drain_notifications(&self) -> Vec<JsonValue> {
        self.hub
            .as_ref()
            .map(|hub| {
                hub.drain_result_notification_query_ids()
                    .into_iter()
                    .map(|query_id| query_results_updated_notification(&query_id))
                    .collect()
            })
            .unwrap_or_default()
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

    fn install_query_results_template(
        mut self,
        hub: Option<StreamQueryHub>,
        access_policy: Option<Arc<AccessPolicyRegistry>>,
    ) -> Self {
        self.templates
            .push(QueryResultsTemplate::new(hub, access_policy));
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

    /// Drains queued resource update notifications from live templates.
    ///
    /// This is used by transports that can emit unsolicited MCP
    /// notifications. Each result row queues one
    /// `notifications/resources/updated` signal for the corresponding
    /// query-results resource, matching Java alpha.10's no-coalescing
    /// notifier contract.
    pub fn drain_notifications(&self) -> Vec<JsonValue> {
        self.templates
            .iter()
            .flat_map(QueryResultsTemplate::drain_notifications)
            .collect()
    }
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the stateless CQELS resource registry used by the shipped stdio binary.
pub fn cqels_resource_registry() -> ResourceRegistry {
    cqels_resource_registry_with_hub(None, None, ResourceRuntimeInfo::default())
}

/// Builds a CQELS resource registry backed by live stream-query state.
pub fn cqels_resource_registry_with_streams(hub: StreamQueryHub) -> ResourceRegistry {
    cqels_resource_registry_with_hub(Some(hub), None, ResourceRuntimeInfo::default())
}

/// Builds a live stream-query resource registry with Java alpha.10 governance.
pub fn cqels_resource_registry_with_streams_and_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> ResourceRegistry {
    cqels_resource_registry_with_hub(
        Some(hub),
        Some(access_policy),
        ResourceRuntimeInfo::default(),
    )
}

/// Builds a live stream-query resource registry with explicit runtime facts.
pub fn cqels_resource_registry_with_streams_access_policy_and_runtime(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
    runtime: ResourceRuntimeInfo,
) -> ResourceRegistry {
    cqels_resource_registry_with_hub(Some(hub), Some(access_policy), runtime)
}

fn cqels_resource_registry_with_hub(
    hub: Option<StreamQueryHub>,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
    runtime: ResourceRuntimeInfo,
) -> ResourceRegistry {
    let stats_hub = hub.clone();
    let namespaces_hub = hub.clone();
    let status_hub = hub.clone();
    let streams_hub = hub.clone();
    let queries_hub = hub.clone();
    let stats_policy = access_policy.clone();
    let namespaces_policy = access_policy.clone();
    let status_policy = access_policy.clone();
    let streams_policy = access_policy.clone();
    let queries_policy = access_policy.clone();
    let reasoning_policy = access_policy.clone();
    let results_policy = access_policy.clone();
    let status_runtime = runtime.clone();

    ResourceRegistry::new()
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_KG_STATS,
                "Knowledge Graph Statistics",
                "Triple count, named graphs, active streams and queries",
            ),
            move || {
                if let Some(denied) = governed_metadata(stats_policy.as_ref()) {
                    return denied;
                }
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
                if let Some(denied) = governed_metadata(namespaces_policy.as_ref()) {
                    return denied;
                }
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
                let stream_reasoning = status_hub
                    .as_ref()
                    .and_then(StreamQueryHub::stream_reasoning_profile)
                    .unwrap_or_else(|| "off".to_string());
                let mut payload = serde_json::Map::new();
                payload.insert("running".to_string(), json!(status_hub.is_some()));
                payload.insert("version".to_string(), json!(status_runtime.server_version));
                payload.insert("transport".to_string(), json!(status_runtime.transport));
                if governance_active(status_policy.as_ref()) {
                    payload.insert("registeredQueryCount".to_string(), json!("withheld"));
                    payload.insert("registeredQueries".to_string(), json!("withheld"));
                    payload.insert("queryIds".to_string(), json!("withheld"));
                    payload.insert("streams".to_string(), json!("withheld"));
                } else {
                    let query_ids = query_ids(status_hub.as_ref());
                    let streams = stream_names(status_hub.as_ref());
                    payload.insert("registeredQueryCount".to_string(), json!(query_ids.len()));
                    payload.insert("registeredQueries".to_string(), json!(query_ids.len()));
                    if let Some(max) = status_runtime.max_registered_queries {
                        payload.insert("maxRegisteredQueries".to_string(), json!(max));
                    }
                    payload.insert("queryIds".to_string(), json!(query_ids));
                    payload.insert("streams".to_string(), json!(streams));
                }

                let mut persistence = serde_json::Map::new();
                persistence.insert(
                    "enabled".to_string(),
                    json!(status_runtime.persistence_enabled),
                );
                if status_runtime.persistence_enabled {
                    if let Some(backend) = &status_runtime.storage_backend {
                        persistence.insert("backend".to_string(), json!(backend));
                    }
                }
                payload.insert("persistence".to_string(), JsonValue::Object(persistence));
                payload.insert(
                    "rdfStore".to_string(),
                    json!({ "persistent": status_runtime.rdf_store_persistent }),
                );
                payload.insert("streamReasoning".to_string(), json!(stream_reasoning));
                payload.insert(
                    "features".to_string(),
                    json!({
                        "rdfMessages": true,
                        "pushStreamEvents": true,
                        "watchInvariant": true,
                        "registerRules": true,
                    }),
                );
                JsonValue::Object(payload)
            },
        )
        .install_json(
            ResourceDescriptor::json(
                RESOURCE_STREAMS,
                "Active Streams",
                "List of active data streams",
            ),
            move || {
                if let Some(denied) = governed_metadata(streams_policy.as_ref()) {
                    return denied;
                }
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
                if let Some(denied) = governed_metadata(queries_policy.as_ref()) {
                    return denied;
                }
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
            move || {
                if let Some(denied) = governed_metadata(reasoning_policy.as_ref()) {
                    return denied;
                }
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
        .install_query_results_template(hub, results_policy)
}

fn governed_metadata(access_policy: Option<&Arc<AccessPolicyRegistry>>) -> Option<JsonValue> {
    governance_active(access_policy).then(|| json!({ "denied": GOVERNED_METADATA_DENIAL }))
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

    use async_trait::async_trait;
    use cqels_asp::{AnswerSet, AspError, AspSolver, Atom};
    use cqels_engine::CqelsEngine;
    use tokio::runtime::Runtime;

    use crate::memory::{InMemoryMemoryStore, MemoryStore};
    use crate::tools::{set_access_policy_tool, AccessPolicyRegistry};
    use crate::{
        push_stream_events_tool, register_rules_tool_with_solver, register_stream_query_tool,
        ToolInvocation, ToolRegistry,
    };

    struct StaticSolver;

    #[async_trait]
    impl AspSolver for StaticSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Ok(vec![AnswerSet::new(vec![Atom::new(
                "alert",
                vec!["alice".to_string()],
            )])])
        }
    }

    fn read_json(registry: &ResourceRegistry, uri: &str) -> JsonValue {
        let result = registry.read(uri).expect("read resource");
        assert_eq!(result.contents.len(), 1);
        assert_eq!(result.contents[0].uri, uri);
        assert_eq!(result.contents[0].mime_type, "application/json");
        serde_json::from_str(&result.contents[0].text).expect("valid JSON resource text")
    }

    fn active_policy() -> Arc<AccessPolicyRegistry> {
        let policy = AccessPolicyRegistry::shared();
        activate_policy(policy.clone());
        policy
    }

    fn activate_policy(access_policy: Arc<AccessPolicyRegistry>) {
        let store: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let mut reg = ToolRegistry::new();
        reg.install(set_access_policy_tool(store, access_policy));
        let res = reg
            .call(
                "set_access_policy",
                &ToolInvocation::new()
                    .with_arg("role", json!("analyst"))
                    .with_arg("labels", json!(["public"])),
            )
            .expect("dispatch");
        assert!(!res.is_error, "{:?}", res.content);
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
    fn static_resources_return_java_alpha10_keys() {
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
        assert_eq!(status["registeredQueryCount"], 0);
        assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(status["transport"], "unknown");
        assert_eq!(status["persistence"]["enabled"], false);
        assert_eq!(status["rdfStore"]["persistent"], false);
        assert_eq!(status["streamReasoning"], "off");

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
    fn engine_status_reports_explicit_java_runtime_facts() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = CqelsEngine::builder().build().expect("engine builds");
        let hub = StreamQueryHub::new_with_stream_reasoning(
            Arc::new(engine),
            runtime.handle().clone(),
            Some(ReasoningProfile::RdfsFull),
        );
        let access_policy = AccessPolicyRegistry::shared();
        let registry = cqels_resource_registry_with_streams_access_policy_and_runtime(
            hub,
            access_policy,
            ResourceRuntimeInfo {
                server_version: "2.0.0-alpha.10-rust".to_string(),
                transport: "HTTP".to_string(),
                persistence_enabled: true,
                storage_backend: Some("sled".to_string()),
                rdf_store_persistent: false,
                max_registered_queries: Some(256),
            },
        );

        let status = read_json(&registry, RESOURCE_ENGINE_STATUS);
        assert_eq!(status["running"], true);
        assert_eq!(status["version"], "2.0.0-alpha.10-rust");
        assert_eq!(status["transport"], "HTTP");
        assert_eq!(status["registeredQueryCount"], 0);
        assert_eq!(status["maxRegisteredQueries"], 256);
        assert_eq!(status["persistence"]["enabled"], true);
        assert_eq!(status["persistence"]["backend"], "sled");
        assert_eq!(status["rdfStore"]["persistent"], false);
        assert_eq!(status["streamReasoning"], "rdfs-full");
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
    fn governed_resources_withhold_metadata_but_keep_status_and_docs_readable() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = CqelsEngine::builder().build().expect("engine builds");
        let _sender = runtime
            .block_on(async { engine.create_stream("sensors").await })
            .expect("create stream");
        let hub = StreamQueryHub::new(Arc::new(engine), runtime.handle().clone());
        let registry = cqels_resource_registry_with_streams_and_access_policy(hub, active_policy());

        for uri in [
            RESOURCE_KG_STATS,
            RESOURCE_KG_NAMESPACES,
            RESOURCE_STREAMS,
            RESOURCE_QUERIES,
            RESOURCE_REASONING,
        ] {
            let body = read_json(&registry, uri);
            assert!(body["denied"]
                .as_str()
                .unwrap()
                .contains("access policy is active"));
        }

        let status = read_json(&registry, RESOURCE_ENGINE_STATUS);
        assert_eq!(status["running"], true);
        assert_eq!(status["registeredQueryCount"], "withheld");
        assert_eq!(status["registeredQueries"], "withheld");
        assert_eq!(status["queryIds"], "withheld");
        assert_eq!(status["streams"], "withheld");
        assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(status["transport"], "unknown");
        assert_eq!(status["persistence"]["enabled"], false);
        assert_eq!(status["rdfStore"]["persistent"], false);
        assert_eq!(status["streamReasoning"], "off");
        assert_eq!(status["features"]["pushStreamEvents"], true);

        let docs = registry.read(RESOURCE_DOC_CQELSQL).expect("read docs");
        assert_eq!(docs.contents[0].mime_type, "text/markdown");
        assert!(docs.contents[0].text.contains("CqelsQL"));
    }

    #[test]
    fn governed_query_results_resource_does_not_drain_buffer() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = CqelsEngine::builder().build().expect("engine builds");
        let hub = StreamQueryHub::new(Arc::new(engine), runtime.handle().clone());
        let access_policy = AccessPolicyRegistry::shared();
        let registry = cqels_resource_registry_with_streams_and_access_policy(
            hub.clone(),
            access_policy.clone(),
        );
        let mut tools = ToolRegistry::new();
        tools.install(register_rules_tool_with_solver(
            hub.clone(),
            Arc::new(StaticSolver),
        ));
        tools.install(push_stream_events_tool(hub.clone()));

        let registered = tools
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("rl-resource"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);

        let pushed = tools
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "facts": [{
                                "subject": "ex:s1",
                                "predicate": "ex:p",
                                "object": "ex:o",
                                "objectType": "uri"
                            }]
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "{:?}", pushed.content);
        assert_eq!(hub.buffered_count("rl-resource"), 1);

        activate_policy(access_policy);
        let body = read_json(&registry, &query_results_uri("rl-resource"));
        assert!(body["denied"]
            .as_str()
            .unwrap()
            .contains("access policy is active"));
        assert!(body["results"].as_array().unwrap().is_empty());
        assert_eq!(hub.buffered_count("rl-resource"), 1);
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
