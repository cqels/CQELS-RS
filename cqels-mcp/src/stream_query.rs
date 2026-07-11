//! Engine-bound MCP tools for managing live stream queries.
//!
//! Mirrors Java's `register_stream_query` tool surface. Unlike the
//! stateless tools in [`crate::tools`], these require a running
//! [`CqelsEngine`] and a tokio runtime to dispatch async calls from
//! synchronous MCP handlers.
//!
//! ## Components
//!
//! - [`StreamQueryHub`] — shared state: an `Arc<CqelsEngine>`, a tokio
//!   [`Handle`] for blocking on async calls, and a per-query result
//!   buffer that downstream MCP clients can drain.
//! - [`register_stream_query_tool`] — compiles and registers a CqelsQL
//!   query, returning the assigned `query_id`. Results stream into the
//!   hub's buffer.
//! - [`list_stream_queries_tool`] — returns the IDs of currently
//!   registered queries.
//! - [`forget_stream_query_tool`] — Java-compatible alias that cancels
//!   a query by `queryId` and reports `status: "forgotten"`.
//! - [`unregister_stream_query_tool`] — cancels and removes a query by
//!   ID.
//! - [`poll_stream_results_tool`] — drains the buffered results for a
//!   query.
//!
//! ## Host wiring
//!
//! The host program is responsible for building the engine, starting it,
//! and providing the tokio runtime handle. A typical wiring:
//!
//! ```ignore
//! let rt = tokio::runtime::Runtime::new()?;
//! let engine = rt.block_on(CqelsEngine::builder().build())?;
//! let hub = StreamQueryHub::new(Arc::new(engine), rt.handle().clone());
//! let mut reg = ToolRegistry::new();
//! reg.install(register_stream_query_tool(hub.clone()));
//! reg.install(forget_stream_query_tool(hub.clone()));
//! reg.install(list_stream_queries_tool(hub.clone()));
//! reg.install(unregister_stream_query_tool(hub.clone()));
//! reg.install(poll_stream_results_tool(hub));
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cqels_asp::{
    validate_program_syntax, AnswerSet, AspFactMapper, AspSolver, ClingoSubprocessSolver,
};
use cqels_core::parser::{CqelsQlParser, CypherQlParser};
use cqels_core::stream::{GraphStreamElement, RdfStreamElement, StreamElement};
use cqels_engine::listener::listener_from_fn;
use cqels_engine::CqelsEngine;
use cqels_model::{BindingSet, CqelsError, IriTerm, LiteralTerm, Statement, Term};
use cqels_reasoning::{ReasoningProfile, ReteNetwork};
use cqels_shacl::{
    ShaclShapeGraph, ShaclShapeParser, ShaclStreamSolveConfig, ShaclValidationEngine,
    ShaclViolation,
};
use oxttl::NQuadsParser;
use parking_lot::Mutex;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Handle;

use crate::tool::{McpTool, ToolInputSchema, ToolInvocation, ToolResult};
use crate::tools::{governance_active, governance_denial, AccessPolicyRegistry};

/// Shared state for engine-bound MCP tools.
///
/// Cheap to clone — internally an `Arc` over the hub state.
#[derive(Clone)]
pub struct StreamQueryHub {
    inner: Arc<HubInner>,
}

struct HubInner {
    engine: Arc<CqelsEngine>,
    handle: Handle,
    results: Mutex<HashMap<String, VecDeque<BindingSet>>>,
    json_results: Mutex<HashMap<String, VecDeque<JsonValue>>>,
    result_notifications: Mutex<VecDeque<String>>,
    registrations: Mutex<HashMap<String, StreamRegistration>>,
    observers: Mutex<HashMap<String, ObserverRegistration>>,
    observer_evaluation: Mutex<()>,
    pending_registrations: Mutex<HashSet<String>>,
    stream_reasoning: Option<StreamReasoningState>,
}

#[derive(Clone, Debug)]
struct StreamRegistration {
    engine_query_id: String,
    buffer_size: usize,
    notify: bool,
}

#[derive(Clone)]
struct ObserverRegistration {
    stream: String,
    buffer_size: usize,
    notify: bool,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
    kind: ObserverKind,
}

#[derive(Clone)]
enum ObserverKind {
    WatchInvariant {
        shape_graph: ShaclShapeGraph,
        report_conforming: bool,
        solver: Arc<dyn AspSolver>,
    },
    Rules {
        rules: String,
        result_predicate: String,
        arg_names: Vec<String>,
        emit_delta: bool,
        max_facts: usize,
        facts: Vec<Statement>,
        emitted: HashSet<String>,
        emitted_order: VecDeque<String>,
        solver: Arc<dyn AspSolver>,
    },
}

struct StreamReasoningState {
    profile: ReasoningProfile,
    networks: Mutex<HashMap<String, ReteNetwork>>,
}

#[derive(Clone)]
struct DynAspSolver(Arc<dyn AspSolver>);

#[async_trait]
impl AspSolver for DynAspSolver {
    async fn solve(
        &self,
        program: &str,
        max_models: usize,
    ) -> Result<Vec<AnswerSet>, cqels_asp::AspError> {
        self.0.solve(program, max_models).await
    }
}

struct PendingRegistrationGuard {
    hub: StreamQueryHub,
    query_id: String,
}

impl PendingRegistrationGuard {
    fn new(hub: StreamQueryHub, query_id: String) -> Self {
        Self { hub, query_id }
    }
}

impl Drop for PendingRegistrationGuard {
    fn drop(&mut self) {
        self.hub
            .inner
            .pending_registrations
            .lock()
            .remove(&self.query_id);
    }
}

const DEFAULT_BUFFER_SIZE: usize = 100;
const MAX_BUFFER_SIZE: usize = 100_000;
const MAX_PENDING_RESULT_NOTIFICATIONS: usize = 100_000;
const MAX_SHAPES_CHARS: usize = 500_000;
const MAX_WATCH_REGISTRATIONS: usize = 16;
const WATCH_PREFIX: &str = "wi";
const MAX_RULES_CHARS: usize = 100_000;
const MAX_RULE_REGISTRATIONS: usize = 8;
const RULE_PREFIX: &str = "rl";
const MAX_ARG_NAMES: usize = 32;
const DEFAULT_MAX_FACTS: usize = 5_000;
const MAX_MAX_FACTS: usize = 50_000;
const DELTA_MEMORY_CAP: usize = 100_000;

impl StreamQueryHub {
    /// Constructs a new hub bound to `engine` and `handle`.
    ///
    /// `handle` is used to call the engine's async APIs from
    /// synchronous MCP tool handlers. It MUST belong to a multi-thread
    /// runtime, or be the handle of a runtime executing on a different
    /// thread — otherwise `block_on` will deadlock.
    pub fn new(engine: Arc<CqelsEngine>, handle: Handle) -> Self {
        Self::new_with_stream_reasoning(engine, handle, stream_reasoning_profile_from_env())
    }

    /// Constructs a new hub with an explicit stream-reasoning profile.
    ///
    /// `None` keeps reasoning disabled. `Some(ReasoningProfile::Rdfs)` or
    /// `Some(ReasoningProfile::RdfsFull)` mirrors Java alpha.10's
    /// `CQELS_MCP_REASONING` opt-in stream reasoning path.
    pub fn new_with_stream_reasoning(
        engine: Arc<CqelsEngine>,
        handle: Handle,
        stream_reasoning_profile: Option<ReasoningProfile>,
    ) -> Self {
        Self {
            inner: Arc::new(HubInner {
                engine,
                handle,
                results: Mutex::new(HashMap::new()),
                json_results: Mutex::new(HashMap::new()),
                result_notifications: Mutex::new(VecDeque::new()),
                registrations: Mutex::new(HashMap::new()),
                observers: Mutex::new(HashMap::new()),
                observer_evaluation: Mutex::new(()),
                pending_registrations: Mutex::new(HashSet::new()),
                stream_reasoning: stream_reasoning_profile.map(|profile| StreamReasoningState {
                    profile,
                    networks: Mutex::new(HashMap::new()),
                }),
            }),
        }
    }

    /// Returns the buffered result count for `query_id`.
    pub fn buffered_count(&self, query_id: &str) -> usize {
        let binding_count = self
            .inner
            .results
            .lock()
            .get(query_id)
            .map(VecDeque::len)
            .unwrap_or(0);
        let json_count = self
            .inner
            .json_results
            .lock()
            .get(query_id)
            .map(VecDeque::len)
            .unwrap_or(0);
        binding_count + json_count
    }

    /// Returns the IDs of all currently registered stream queries.
    pub fn registered_query_ids(&self) -> Vec<String> {
        let mut ids = {
            let mut ids = self
                .inner
                .registrations
                .lock()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            ids.extend(self.inner.observers.lock().keys().cloned());
            ids
        };
        ids.sort();
        ids.dedup();
        ids
    }

    /// Returns the names of all currently registered input streams.
    pub fn registered_stream_names(&self) -> Vec<String> {
        let engine = self.inner.engine.clone();
        self.inner
            .handle
            .block_on(async move { engine.registered_stream_names().await })
    }

    /// Returns the configured stream-reasoning profile name, if enabled.
    pub fn stream_reasoning_profile(&self) -> Option<String> {
        self.inner
            .stream_reasoning
            .as_ref()
            .map(|state| match state.profile {
                ReasoningProfile::Rdfs => "rdfs".to_string(),
                ReasoningProfile::RdfsFull => "rdfs-full".to_string(),
                _ => state.profile.name().to_string(),
            })
    }

    /// Drains up to `max` buffered results for `query_id`. Returns the
    /// drained results in FIFO order.
    pub fn drain_results(&self, query_id: &str, max: usize) -> Vec<BindingSet> {
        let mut guard = self.inner.results.lock();
        let Some(buffer) = guard.get_mut(query_id) else {
            return Vec::new();
        };
        let take = max.min(buffer.len());
        buffer.drain(..take).collect()
    }

    /// Drains up to `max` buffered results as JSON values. Engine query
    /// [`BindingSet`] values are serialized through serde; observer tools
    /// already buffer JSON payloads.
    pub fn drain_result_values(&self, query_id: &str, max: usize) -> Vec<JsonValue> {
        let mut out = {
            let mut guard = self.inner.json_results.lock();
            if let Some(buffer) = guard.get_mut(query_id) {
                let take = max.min(buffer.len());
                buffer.drain(..take).collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        if out.len() >= max {
            return out;
        }
        let remaining = max.saturating_sub(out.len());
        out.extend(
            self.drain_results(query_id, remaining)
                .into_iter()
                .map(|binding| {
                    serde_json::to_value(binding).unwrap_or_else(|e| {
                        json!({
                            "serialization_error": e.to_string(),
                        })
                    })
                }),
        );
        out
    }

    /// Drains queued query-result update notifications. Each entry is a
    /// query id whose `cqels://queries/{id}/results` resource changed.
    pub fn drain_result_notification_query_ids(&self) -> Vec<String> {
        self.inner.result_notifications.lock().drain(..).collect()
    }

    /// Creates a stream if it is not already registered. Returns `true`
    /// when a new stream was created and `false` when it already existed.
    pub fn create_stream(&self, name: &str) -> Result<bool, CqelsError> {
        if self.inner.engine.get_stream(name).is_some() {
            return Ok(false);
        }
        let engine = self.inner.engine.clone();
        let stream_name = name.to_string();
        self.inner.handle.block_on(async move {
            match engine.create_stream(&stream_name).await {
                Ok(_) => Ok(true),
                Err(e @ CqelsError::Stream { .. }) => {
                    if engine
                        .registered_stream_names()
                        .await
                        .iter()
                        .any(|registered| registered == &stream_name)
                    {
                        Ok(false)
                    } else {
                        Err(e)
                    }
                }
                Err(e) => Err(e),
            }
        })
    }

    /// Pushes one RDF observation to a named stream and then feeds the
    /// same observation into MCP-level continuous observers.
    pub fn push_observation(
        &self,
        stream: &str,
        statements: Vec<Statement>,
        timestamp: i64,
    ) -> Result<usize, CqelsError> {
        self.create_stream(stream)?;
        let Some(data_stream) = self.inner.engine.get_stream(stream) else {
            return Err(CqelsError::Stream {
                message: format!("stream '{stream}' could not be resolved after creation"),
            });
        };
        let inferred = self.apply_stream_reasoning(stream, &statements, timestamp);
        let pushed_statement_count = statements.len() + inferred.len();
        let mut elements = Vec::new();
        match statements.as_slice() {
            [] => {}
            [statement] => elements.push(StreamElement::Rdf(RdfStreamElement::new(
                statement.clone(),
                timestamp,
            ))),
            _ => elements.push(StreamElement::Graph(GraphStreamElement::new(
                statements.clone(),
                timestamp,
            ))),
        }
        elements.extend(
            inferred
                .iter()
                .cloned()
                .map(|statement| StreamElement::Rdf(RdfStreamElement::new(statement, timestamp))),
        );
        self.inner.handle.block_on(async {
            for element in elements {
                data_stream.push(element).await?;
            }
            Ok::<(), CqelsError>(())
        })?;
        if !statements.is_empty() {
            self.process_observation(stream, &statements, timestamp);
        }
        for inferred_statement in inferred {
            self.process_observation(stream, std::slice::from_ref(&inferred_statement), timestamp);
        }
        Ok(pushed_statement_count)
    }

    fn apply_stream_reasoning(
        &self,
        stream: &str,
        statements: &[Statement],
        timestamp: i64,
    ) -> Vec<Statement> {
        let Some(reasoning) = self.inner.stream_reasoning.as_ref() else {
            return Vec::new();
        };
        let mut networks = reasoning.networks.lock();
        let network = networks
            .entry(stream.to_string())
            .or_insert_with(|| ReteNetwork::compile(reasoning.profile.create_config()));
        statements
            .iter()
            .flat_map(|statement| {
                let element = RdfStreamElement::new(statement.clone(), timestamp);
                network
                    .process_element(&element)
                    .into_iter()
                    .map(|inferred| inferred.statement)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn record_result(&self, query_id: String, result: BindingSet) {
        let (buffer_size, notify) = self
            .inner
            .registrations
            .lock()
            .get(&query_id)
            .map(|registration| (registration.buffer_size, registration.notify))
            .unwrap_or((DEFAULT_BUFFER_SIZE, false));
        let mut guard = self.inner.results.lock();
        let buffer = guard.entry(query_id.clone()).or_default();
        buffer.push_back(result);
        while buffer.len() > buffer_size {
            buffer.pop_front();
        }
        if notify {
            self.queue_result_notification(&query_id);
        }
    }

    fn record_json_result_with_size(
        &self,
        query_id: String,
        result: JsonValue,
        buffer_size: usize,
        notify: bool,
    ) {
        let mut guard = self.inner.json_results.lock();
        let buffer = guard.entry(query_id.clone()).or_default();
        buffer.push_back(result);
        while buffer.len() > buffer_size {
            buffer.pop_front();
        }
        if notify {
            self.queue_result_notification(&query_id);
        }
    }

    fn forget_results(&self, query_id: &str) {
        self.inner.results.lock().remove(query_id);
        self.inner.json_results.lock().remove(query_id);
        self.inner
            .result_notifications
            .lock()
            .retain(|queued| queued != query_id);
    }

    fn queue_result_notification(&self, query_id: &str) {
        let mut notifications = self.inner.result_notifications.lock();
        notifications.push_back(query_id.to_string());
        while notifications.len() > MAX_PENDING_RESULT_NOTIFICATIONS {
            notifications.pop_front();
        }
    }

    fn unregister_query(&self, query_id: &str) -> Result<(), CqelsError> {
        if self.inner.observers.lock().remove(query_id).is_some() {
            self.forget_results(query_id);
            return Ok(());
        }
        let registration = self
            .inner
            .registrations
            .lock()
            .remove(query_id)
            .ok_or_else(|| CqelsError::Stream {
                message: format!("stream query '{query_id}' not found"),
            })?;
        let engine = self.inner.engine.clone();
        let id_for_async = registration.engine_query_id;
        self.inner
            .handle
            .block_on(async move { engine.unregister_query(&id_for_async).await })?;
        self.forget_results(query_id);
        Ok(())
    }

    fn register_observer(
        &self,
        query_id: String,
        registration: ObserverRegistration,
        prefix: &str,
        max_registrations: usize,
        cap_message: &str,
    ) -> Result<(), String> {
        let registrations = self.inner.registrations.lock();
        let mut observers = self.inner.observers.lock();
        if registrations.contains_key(&query_id) || observers.contains_key(&query_id) {
            return Err(format!("Query already registered: {query_id}"));
        }
        let prefix_with_dash = format!("{prefix}-");
        // Java alpha.10 counts the shared registry namespace for these
        // prefixes, so a normal stream query with a `wi-`/`rl-` id also
        // consumes the corresponding soft cap. Keep that visible overlap here.
        let live = registrations
            .keys()
            .filter(|id| id.starts_with(&prefix_with_dash))
            .count()
            + observers
                .keys()
                .filter(|id| id.starts_with(&prefix_with_dash))
                .count();
        if live >= max_registrations {
            return Err(cap_message.to_string());
        }
        observers.insert(query_id, registration);
        Ok(())
    }

    fn process_observation(&self, stream: &str, statements: &[Statement], timestamp: i64) {
        let _evaluation_guard = self.inner.observer_evaluation.lock();
        let query_ids = {
            let observers = self.inner.observers.lock();
            observers
                .iter()
                .filter(|(_, registration)| registration.stream == stream)
                .map(|(query_id, _)| query_id.clone())
                .collect::<Vec<_>>()
        };

        for query_id in query_ids {
            let Some(mut registration) = self.inner.observers.lock().get(&query_id).cloned() else {
                continue;
            };
            if registration.stream != stream {
                continue;
            }
            let result =
                self.evaluate_observer(&query_id, &mut registration, statements, timestamp);
            let mut observers = self.inner.observers.lock();
            let Some(current) = observers.get_mut(&query_id) else {
                continue;
            };
            let governed = governance_active(registration.access_policy.as_ref());
            *current = registration;
            if governed {
                continue;
            }
            if let Some(result) = result {
                self.record_json_result_with_size(
                    query_id,
                    result,
                    current.buffer_size,
                    current.notify,
                );
            }
        }
    }

    fn evaluate_observer(
        &self,
        query_id: &str,
        registration: &mut ObserverRegistration,
        statements: &[Statement],
        timestamp: i64,
    ) -> Option<JsonValue> {
        match &mut registration.kind {
            ObserverKind::WatchInvariant {
                shape_graph,
                report_conforming,
                solver,
            } => {
                let engine = ShaclValidationEngine::new(
                    ShaclStreamSolveConfig::default(),
                    DynAspSolver(solver.clone()),
                );
                let result = self.inner.handle.block_on(async {
                    engine
                        .validate(shape_graph, statements, timestamp, query_id)
                        .await
                });
                match result {
                    Ok(validation) => {
                        if validation.conforms && !*report_conforming {
                            return None;
                        }
                        Some(json!({
                            "queryId": query_id,
                            "type": "watch_invariant",
                            "stream": registration.stream,
                            "timestamp": timestamp,
                            "conforms": validation.conforms,
                            "status": format!("{:?}", validation.status),
                            "violation_count": validation.violations.len(),
                            "violations": violations_to_json(&validation.violations),
                        }))
                    }
                    Err(e) => Some(json!({
                        "queryId": query_id,
                        "type": "watch_invariant",
                        "stream": registration.stream,
                        "timestamp": timestamp,
                        "error": e.to_string(),
                    })),
                }
            }
            ObserverKind::Rules {
                rules,
                result_predicate,
                arg_names,
                emit_delta,
                max_facts,
                facts,
                emitted,
                emitted_order,
                solver,
            } => {
                facts.extend(statements.iter().cloned());
                if facts.len() > *max_facts {
                    let drop_count = facts.len() - *max_facts;
                    facts.drain(..drop_count);
                }
                let fact_program = AspFactMapper::statements_to_program(facts);
                let full_program = if fact_program.is_empty() {
                    rules.clone()
                } else {
                    format!("{rules}\n{fact_program}")
                };
                let answer_sets = self
                    .inner
                    .handle
                    .block_on(async { solver.solve(&full_program, 1).await });
                match answer_sets {
                    Ok(answer_sets) => {
                        let mut rows = Vec::new();
                        for answer_set in &answer_sets {
                            for atom in answer_set.query_predicate(result_predicate) {
                                let atom_key = atom.to_string();
                                if *emit_delta {
                                    if !emitted.insert(atom_key.clone()) {
                                        continue;
                                    }
                                    emitted_order.push_back(atom_key);
                                    while emitted_order.len() > DELTA_MEMORY_CAP {
                                        if let Some(oldest) = emitted_order.pop_front() {
                                            emitted.remove(&oldest);
                                        }
                                    }
                                }
                                rows.push(json!({
                                    "predicate": atom.predicate,
                                    "terms": atom.terms,
                                    "bindings": atom_terms_to_bindings(&atom.terms, arg_names),
                                }));
                            }
                        }
                        Some(json!({
                            "queryId": query_id,
                            "type": "register_rules",
                            "stream": registration.stream,
                            "timestamp": timestamp,
                            "resultPredicate": result_predicate,
                            "count": rows.len(),
                            "results": rows,
                        }))
                    }
                    Err(e) => Some(json!({
                        "queryId": query_id,
                        "type": "register_rules",
                        "stream": registration.stream,
                        "timestamp": timestamp,
                        "error": e.to_string(),
                    })),
                }
            }
        }
    }
}

// ─── create_stream ──────────────────────────────────────────────────

/// Constructs the Java alpha.9-compatible `create_stream` MCP tool.
pub fn create_stream_tool(hub: StreamQueryHub) -> CreateStreamTool {
    CreateStreamTool {
        hub,
        access_policy: None,
    }
}

/// Constructs `create_stream` with Java alpha.9/alpha.10 governance.
pub fn create_stream_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> CreateStreamTool {
    CreateStreamTool {
        hub,
        access_policy: Some(access_policy),
    }
}

pub struct CreateStreamTool {
    hub: StreamQueryHub,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl McpTool for CreateStreamTool {
    fn name(&self) -> &str {
        "create_stream"
    }

    fn description(&self) -> &str {
        "Create a named RDF input stream if it does not already exist. \
         Idempotent: existing streams are reported as already available."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "stream",
                json!({
                    "type": "string",
                    "description": "Input stream name to create."
                }),
            )
            .require("stream")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(stream) = required_nonempty_str(invocation, "stream") else {
            return ToolResult::error("missing `stream` argument");
        };
        let stream_names = self.hub.registered_stream_names();
        if !stream_names.iter().any(|name| name == &stream) && stream_names.len() >= MAX_STREAMS {
            return ToolResult::error(format!(
                "stream limit reached: {MAX_STREAMS} distinct streams"
            ));
        }
        match self.hub.create_stream(&stream) {
            Ok(created) => ToolResult::success(json!({
                "ok": true,
                "stream": stream,
                "created": created,
                "status": if created { "created" } else { "exists" },
            })),
            Err(e) => ToolResult::error(format!("create_stream failed: {e}")),
        }
    }
}

// ─── push_stream_events ─────────────────────────────────────────────

/// Constructs the Java alpha.9-compatible `push_stream_events` MCP tool.
pub fn push_stream_events_tool(hub: StreamQueryHub) -> PushStreamEventsTool {
    PushStreamEventsTool {
        hub,
        access_policy: None,
    }
}

/// Constructs `push_stream_events` with Java alpha.9/alpha.10 governance.
pub fn push_stream_events_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> PushStreamEventsTool {
    PushStreamEventsTool {
        hub,
        access_policy: Some(access_policy),
    }
}

pub struct PushStreamEventsTool {
    hub: StreamQueryHub,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

const MAX_PUSH_EVENTS: usize = 1000;
const MAX_STATEMENTS_PER_MESSAGE: usize = 10_000;
const MAX_TOTAL_OBSERVATIONS: usize = 10_000;
const MAX_PUSH_STATEMENTS: usize = 100_000;
const MAX_NQUADS_CHARS: usize = 2_000_000;
const MAX_TOTAL_CHARS: usize = 8_000_000;
const MAX_STREAMS: usize = 1024;
const MAX_EVENT_TIME: i64 = 7_258_118_400_000;

impl McpTool for PushStreamEventsTool {
    fn name(&self) -> &str {
        "push_stream_events"
    }

    fn description(&self) -> &str {
        "Push one or more RDF observations into a named stream. Each event \
         may contain `facts` ({subject,predicate,object,objectType}) and/or \
         RDF-message N-Quads in `nquads`; all statements in an observation \
         share the event timestamp."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "stream",
                json!({
                    "type": "string",
                    "description": "Input stream name. Created automatically if needed."
                }),
            )
            .with_property(
                "events",
                json!({
                    "type": "array",
                    "maxItems": MAX_PUSH_EVENTS,
                    "minItems": 1,
                    "description": "Events to push. Each item may include eventTime plus facts or RDF-message nquads.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "eventTime": { "description": "Unix milliseconds, numeric string, or UTC RFC3339 timestamp." },
                            "facts": {
                                "type": "array",
                                "description": "The observation as {subject, predicate, object, objectType} triples (objectType 'uri' or 'literal', default 'literal'). Use this OR 'nquads'.",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "subject": { "type": "string" },
                                        "predicate": { "type": "string" },
                                        "object": { "type": "string" },
                                        "objectType": {
                                            "type": "string",
                                            "enum": ["uri", "literal"],
                                            "default": "literal"
                                        }
                                    },
                                    "required": ["subject", "predicate", "object"]
                                }
                            },
                            "nquads": { "type": "string" }
                        }
                    }
                }),
            )
            .require("stream")
            .require("events")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(stream) = required_nonempty_str(invocation, "stream") else {
            return ToolResult::error("missing `stream` argument");
        };
        let Some(events) = invocation.get("events").and_then(JsonValue::as_array) else {
            return ToolResult::error("missing or non-array `events` argument");
        };
        if events.is_empty() {
            return ToolResult::error("push_stream_events requires a non-empty `events` array");
        }
        if events.len() > MAX_PUSH_EVENTS {
            return ToolResult::error(format!(
                "too many events: {} > {MAX_PUSH_EVENTS}",
                events.len()
            ));
        }

        let mut observations = Vec::new();
        let mut statement_count = 0usize;
        let mut total_chars = 0usize;
        for (event_index, event) in events.iter().enumerate() {
            let Some(object) = event.as_object() else {
                return ToolResult::error(format!("events[{event_index}] must be an object"));
            };
            total_chars = match total_chars.checked_add(event_payload_chars(object)) {
                Some(total) => total,
                None => {
                    return ToolResult::error("too many characters in one call".to_string());
                }
            };
            if total_chars > MAX_TOTAL_CHARS {
                return ToolResult::error(format!(
                    "too many characters in one call: {total_chars} > {MAX_TOTAL_CHARS}"
                ));
            }
            let timestamp = match parse_event_time(object.get("eventTime")) {
                Ok(timestamp) => timestamp,
                Err(e) => return ToolResult::error(format!("events[{event_index}].{e}")),
            };
            let mut event_observations = Vec::new();
            if let Some(nquads) = object
                .get("nquads")
                .and_then(JsonValue::as_str)
                .filter(|nquads| !nquads.trim().is_empty())
            {
                if nquads.len() > MAX_NQUADS_CHARS {
                    return ToolResult::error(format!(
                        "events[{event_index}].nquads exceeds {MAX_NQUADS_CHARS} characters"
                    ));
                }
                match parse_nquads_messages(nquads) {
                    Ok(mut messages) => event_observations.append(&mut messages),
                    Err(e) => {
                        return ToolResult::error(format!("events[{event_index}].nquads: {e}"));
                    }
                }
            } else if let Some(facts) = object.get("facts") {
                let Some(items) = facts.as_array() else {
                    return ToolResult::error(format!(
                        "events[{event_index}].facts must be an array"
                    ));
                };
                if items.len() > MAX_STATEMENTS_PER_MESSAGE {
                    return ToolResult::error(format!(
                        "events[{event_index}].facts array too large: {} > {MAX_STATEMENTS_PER_MESSAGE}",
                        items.len()
                    ));
                }
                let fact_chars = fact_array_payload_chars(items);
                if fact_chars > MAX_NQUADS_CHARS {
                    return ToolResult::error(format!(
                        "events[{event_index}].facts payload exceeds {MAX_NQUADS_CHARS} characters"
                    ));
                }
                match parse_fact_array(facts, &format!("events[{event_index}].facts")) {
                    Ok(statements) if statements.is_empty() => {
                        return ToolResult::error(format!(
                            "events[{event_index}] requires non-empty `facts`"
                        ));
                    }
                    Ok(statements) => event_observations.push(statements),
                    Err(e) => return ToolResult::error(e),
                }
            }
            if event_observations.is_empty() {
                return ToolResult::error(format!(
                    "events[{event_index}] requires non-empty `facts` or `nquads`"
                ));
            }
            for statements in event_observations {
                if statements.is_empty() {
                    continue;
                }
                if let Err(e) = validate_statement_graphs(&statements) {
                    return ToolResult::error(e);
                }
                if statements.len() > MAX_STATEMENTS_PER_MESSAGE {
                    return ToolResult::error(format!(
                        "events[{event_index}] message too large: {} > {MAX_STATEMENTS_PER_MESSAGE}",
                        statements.len()
                    ));
                }
                if observations.len() >= MAX_TOTAL_OBSERVATIONS {
                    return ToolResult::error(format!(
                        "too many observations: {} >= {MAX_TOTAL_OBSERVATIONS}",
                        observations.len()
                    ));
                }
                statement_count += statements.len();
                if statement_count > MAX_PUSH_STATEMENTS {
                    return ToolResult::error(format!(
                        "too many statements: {statement_count} > {MAX_PUSH_STATEMENTS}"
                    ));
                }
                observations.push((timestamp, statements));
            }
        }

        let observation_count = observations.len();
        let stream_names = self.hub.registered_stream_names();
        if !stream_names.iter().any(|name| name == &stream) && stream_names.len() >= MAX_STREAMS {
            return ToolResult::error(format!(
                "stream limit reached: {MAX_STREAMS} distinct streams"
            ));
        }
        let mut pushed_statements = 0usize;
        for (timestamp, statements) in observations {
            match self
                .hub
                .push_observation(&stream, statements.clone(), timestamp)
            {
                Ok(count) => pushed_statements += count,
                Err(e) => return ToolResult::error(format!("push_stream_events failed: {e}")),
            }
        }

        ToolResult::success(json!({
            "ok": true,
            "stream": stream,
            "eventCount": events.len(),
            "observationCount": observation_count,
            "inputStatementCount": statement_count,
            "statementCount": pushed_statements,
        }))
    }
}

// ─── validate_stream_query ──────────────────────────────────────────

/// Constructs the Java alpha.9-compatible `validate_stream_query` MCP tool.
pub fn validate_stream_query_tool() -> ValidateStreamQueryTool {
    ValidateStreamQueryTool
}

pub struct ValidateStreamQueryTool;

impl McpTool for ValidateStreamQueryTool {
    fn name(&self) -> &str {
        "validate_stream_query"
    }

    fn description(&self) -> &str {
        "Validate stream query syntax without registering it. Supports \
         `cqelsql` and `cypher` language values."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "Query text to validate."
                }),
            )
            .with_property(
                "language",
                json!({
                    "type": "string",
                    "enum": ["cqelsql", "cypher"],
                    "default": "cqelsql"
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query) = invocation.get_str("query") else {
            return ToolResult::error("missing `query` argument");
        };
        let language = invocation
            .get_str("language")
            .unwrap_or("cqelsql")
            .trim()
            .to_ascii_lowercase();
        match language.as_str() {
            "cqelsql" => match CqelsQlParser::parse(query) {
                Ok(def) => ToolResult::success(json!({
                    "valid": true,
                    "language": "cqelsql",
                    "streams": def.streams.iter().map(|s| &s.name).collect::<Vec<_>>(),
                    "patternGroups": def.pattern_groups.len(),
                })),
                Err(e) => ToolResult::success(json!({
                    "valid": false,
                    "language": "cqelsql",
                    "error": e.to_string(),
                })),
            },
            "cypher" | "cypherql" => match CypherQlParser::parse(query) {
                Ok(def) => ToolResult::success(json!({
                    "valid": true,
                    "language": "cypher",
                    "streams": def.streams.iter().map(|s| &s.name).collect::<Vec<_>>(),
                })),
                Err(e) => ToolResult::success(json!({
                    "valid": false,
                    "language": "cypher",
                    "error": e.to_string(),
                })),
            },
            other => ToolResult::error(format!(
                "unsupported validate_stream_query language '{other}'; supported: cqelsql, cypher"
            )),
        }
    }
}

// ─── watch_invariant ────────────────────────────────────────────────

/// Constructs the Java alpha.10-compatible `watch_invariant` MCP tool.
pub fn watch_invariant_tool(hub: StreamQueryHub) -> WatchInvariantTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    watch_invariant_tool_with_solver(hub, solver)
}

/// Constructs `watch_invariant` with Java alpha.10 governance.
pub fn watch_invariant_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> WatchInvariantTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    watch_invariant_tool_with_solver_and_access_policy(hub, solver, access_policy)
}

/// Constructs `watch_invariant` with a caller-supplied ASP solver.
pub fn watch_invariant_tool_with_solver(
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
) -> WatchInvariantTool {
    WatchInvariantTool {
        hub,
        solver,
        access_policy: None,
    }
}

/// Constructs `watch_invariant` with a caller-supplied solver and governance.
pub fn watch_invariant_tool_with_solver_and_access_policy(
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
    access_policy: Arc<AccessPolicyRegistry>,
) -> WatchInvariantTool {
    WatchInvariantTool {
        hub,
        solver,
        access_policy: Some(access_policy),
    }
}

pub struct WatchInvariantTool {
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl McpTool for WatchInvariantTool {
    fn name(&self) -> &str {
        "watch_invariant"
    }

    fn description(&self) -> &str {
        "Register a continuous SHACL invariant over an MCP stream. Each \
         observation pushed through `push_stream_events` is validated and \
         any report is buffered for `poll_stream_results`."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property("stream", json!({ "type": "string" }))
            .with_property(
                "shapes",
                json!({
                    "description": "SHACL shapes as an array of fact/triple objects or RDF-message N-Quads string. Payload is capped at 500000 characters."
                }),
            )
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "Optional custom id suffix; the returned queryId is always prefixed 'wi-'."
                }),
            )
            .with_property(
                "bufferSize",
                json!({
                    "type": "integer",
                    "default": DEFAULT_BUFFER_SIZE,
                    "minimum": 1,
                    "maximum": MAX_BUFFER_SIZE
                }),
            )
            .with_property(
                "reportConforming",
                json!({
                    "type": "boolean",
                    "default": false
                }),
            )
            .with_property(
                "notify",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "When true, queue result-resource update signals. The bundled server emits them over stdio; HTTP clients should poll results."
                }),
            )
            .require("stream")
            .require("shapes")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(stream) = required_nonempty_str(invocation, "stream") else {
            return ToolResult::error("missing `stream` argument");
        };
        let Some(shapes_value) = invocation.get("shapes") else {
            return ToolResult::error("missing `shapes` argument");
        };
        let shapes_chars = shapes_payload_chars(shapes_value);
        if shapes_chars > MAX_SHAPES_CHARS {
            return ToolResult::error(format!(
                "shapes too large: {shapes_chars} chars (max {MAX_SHAPES_CHARS})"
            ));
        }
        let shapes = match parse_shapes_argument(shapes_value) {
            Ok(statements) => statements,
            Err(e) => return ToolResult::error(e),
        };
        let shape_graph = match ShaclShapeParser::parse(&shapes) {
            Ok(graph) => graph,
            Err(e) => return ToolResult::error(format!("shape parse error: {e}")),
        };
        let query_id = observer_query_id(WATCH_PREFIX, invocation);
        let buffer_size = buffer_size_from(invocation);
        let report_conforming = invocation
            .get("reportConforming")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        let notify = invocation
            .get("notify")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        if let Err(e) = self.hub.register_observer(
            query_id.clone(),
            ObserverRegistration {
                stream: stream.clone(),
                buffer_size,
                notify,
                access_policy: self.access_policy.clone(),
                kind: ObserverKind::WatchInvariant {
                    shape_graph,
                    report_conforming,
                    solver: self.solver.clone(),
                },
            },
            WATCH_PREFIX,
            MAX_WATCH_REGISTRATIONS,
            &format!(
                "watch_invariant limit reached (max {MAX_WATCH_REGISTRATIONS} live invariants)"
            ),
        ) {
            return ToolResult::error(e);
        }
        if let Err(e) = self.hub.create_stream(&stream) {
            let _ = self.hub.unregister_query(&query_id);
            return ToolResult::error(format!("create stream failed: {e}"));
        }
        ToolResult::success(json!({
            "ok": true,
            "query_id": query_id,
            "queryId": query_id,
            "stream": stream,
            "status": "registered",
            "bufferSize": buffer_size,
            "reportConforming": report_conforming,
            "notify": notify,
        }))
    }
}

// ─── register_rules ─────────────────────────────────────────────────

/// Constructs the Java alpha.10-compatible `register_rules` MCP tool.
pub fn register_rules_tool(hub: StreamQueryHub) -> RegisterRulesTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    register_rules_tool_with_solver(hub, solver)
}

/// Constructs `register_rules` with Java alpha.10 governance.
pub fn register_rules_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> RegisterRulesTool {
    let solver: Arc<dyn AspSolver> = Arc::new(ClingoSubprocessSolver::new());
    register_rules_tool_with_solver_and_access_policy(hub, solver, access_policy)
}

/// Constructs `register_rules` with a caller-supplied ASP solver.
pub fn register_rules_tool_with_solver(
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
) -> RegisterRulesTool {
    RegisterRulesTool {
        hub,
        solver,
        access_policy: None,
    }
}

/// Constructs `register_rules` with a caller-supplied solver and governance.
pub fn register_rules_tool_with_solver_and_access_policy(
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
    access_policy: Arc<AccessPolicyRegistry>,
) -> RegisterRulesTool {
    RegisterRulesTool {
        hub,
        solver,
        access_policy: Some(access_policy),
    }
}

pub struct RegisterRulesTool {
    hub: StreamQueryHub,
    solver: Arc<dyn AspSolver>,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl McpTool for RegisterRulesTool {
    fn name(&self) -> &str {
        "register_rules"
    }

    fn description(&self) -> &str {
        "Register a continuous ASP rule program over an MCP stream. RDF \
         observations pushed through `push_stream_events` become `rdf/3` \
         facts, and atoms matching `resultPredicate` are buffered."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property("stream", json!({ "type": "string" }))
            .with_property(
                "rules",
                json!({
                    "type": "string",
                    "description": "ASP Core 2 program, capped at 100000 characters."
                }),
            )
            .with_property("resultPredicate", json!({ "type": "string" }))
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "Optional custom id suffix; the returned queryId is always prefixed 'rl-'."
                }),
            )
            .with_property(
                "argNames",
                json!({
                    "type": "array",
                    "maxItems": MAX_ARG_NAMES,
                    "items": { "type": "string" }
                }),
            )
            .with_property(
                "emit",
                json!({
                    "type": "string",
                    "enum": ["delta", "full"],
                    "default": "delta"
                }),
            )
            .with_property(
                "maxFacts",
                json!({
                    "type": "integer",
                    "default": DEFAULT_MAX_FACTS,
                    "minimum": 1,
                    "maximum": MAX_MAX_FACTS
                }),
            )
            .with_property(
                "bufferSize",
                json!({
                    "type": "integer",
                    "default": DEFAULT_BUFFER_SIZE,
                    "minimum": 1,
                    "maximum": MAX_BUFFER_SIZE
                }),
            )
            .with_property(
                "notify",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "When true, queue result-resource update signals. The bundled server emits them over stdio; HTTP clients should poll results."
                }),
            )
            .require("stream")
            .require("rules")
            .require("resultPredicate")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(stream) = required_nonempty_str(invocation, "stream") else {
            return ToolResult::error("missing `stream` argument");
        };
        let Some(rules) = required_nonempty_str(invocation, "rules") else {
            return ToolResult::error("missing `rules` argument");
        };
        if rules.len() > MAX_RULES_CHARS {
            return ToolResult::error(format!(
                "rules too large: {} chars (max {MAX_RULES_CHARS})",
                rules.len()
            ));
        }
        let Some(result_predicate) = required_nonempty_str(invocation, "resultPredicate") else {
            return ToolResult::error("missing `resultPredicate` argument");
        };
        let arg_names = match parse_arg_names(invocation.get("argNames")) {
            Ok(names) => names,
            Err(e) => return ToolResult::error(e),
        };
        let emit = invocation
            .get_str("emit")
            .unwrap_or("delta")
            .trim()
            .to_ascii_lowercase();
        let emit_delta = match emit.as_str() {
            "delta" => true,
            "full" => false,
            other => return ToolResult::error(format!("unsupported emit value '{other}'")),
        };
        let max_facts = invocation
            .get("maxFacts")
            .and_then(JsonValue::as_i64)
            .map(|value| value.clamp(1, MAX_MAX_FACTS as i64) as usize)
            .unwrap_or(DEFAULT_MAX_FACTS);
        let buffer_size = buffer_size_from(invocation);
        let notify = invocation
            .get("notify")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);
        if let Err(e) = validate_program_syntax(&rules) {
            return ToolResult::error(format!("Invalid ASP program: {e}"));
        }
        let query_id = observer_query_id(RULE_PREFIX, invocation);
        if let Err(e) = self.hub.register_observer(
            query_id.clone(),
            ObserverRegistration {
                stream: stream.clone(),
                buffer_size,
                notify,
                access_policy: self.access_policy.clone(),
                kind: ObserverKind::Rules {
                    rules,
                    result_predicate: result_predicate.clone(),
                    arg_names,
                    emit_delta,
                    max_facts,
                    facts: Vec::new(),
                    emitted: HashSet::new(),
                    emitted_order: VecDeque::new(),
                    solver: self.solver.clone(),
                },
            },
            RULE_PREFIX,
            MAX_RULE_REGISTRATIONS,
            &format!(
                "register_rules limit reached (max {MAX_RULE_REGISTRATIONS} live rule programs)"
            ),
        ) {
            return ToolResult::error(e);
        }
        if let Err(e) = self.hub.create_stream(&stream) {
            let _ = self.hub.unregister_query(&query_id);
            return ToolResult::error(format!("create stream failed: {e}"));
        }
        ToolResult::success(json!({
            "ok": true,
            "query_id": query_id,
            "queryId": query_id,
            "stream": stream,
            "status": "registered",
            "resultPredicate": result_predicate,
            "emit": emit,
            "maxFacts": max_facts,
            "bufferSize": buffer_size,
            "notify": notify,
        }))
    }
}

// ─── register_stream_query ──────────────────────────────────────────

/// Constructs the `register_stream_query` MCP tool.
pub fn register_stream_query_tool(hub: StreamQueryHub) -> RegisterStreamQueryTool {
    RegisterStreamQueryTool {
        hub,
        access_policy: None,
    }
}

/// Constructs `register_stream_query` with Java alpha.9/alpha.10 governance.
pub fn register_stream_query_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> RegisterStreamQueryTool {
    RegisterStreamQueryTool {
        hub,
        access_policy: Some(access_policy),
    }
}

pub struct RegisterStreamQueryTool {
    hub: StreamQueryHub,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl McpTool for RegisterStreamQueryTool {
    fn name(&self) -> &str {
        "register_stream_query"
    }

    fn description(&self) -> &str {
        "Register a CqelsQL query against the live engine and stream its \
         results into an internal buffer. Returns the assigned `query_id`, \
         which callers use with `poll_stream_results`, \
         `forget_stream_query`, or `unregister_stream_query`."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query",
                json!({
                    "type": "string",
                    "description": "CqelsQL query text to register"
                }),
            )
            .with_property(
                "language",
                json!({
                    "type": "string",
                    "enum": ["cqelsql"],
                    "default": "cqelsql",
                    "description": "Query language. This Rust MCP server currently accepts cqelsql."
                }),
            )
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "Optional stable MCP-facing query ID. Defaults to the engine query ID."
                }),
            )
            .with_property(
                "bufferSize",
                json!({
                    "type": "integer",
                    "default": DEFAULT_BUFFER_SIZE,
                    "description": "Maximum buffered result rows retained for polling, clamped to [1, 100000]."
                }),
            )
            .with_property(
                "notify",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "When true, queue result-resource update signals. The bundled server emits them over stdio; HTTP clients should poll results."
                }),
            )
            .with_property(
                "cep",
                json!({
                    "type": "boolean",
                    "default": false,
                    "description": "CEP registration flag from Java alpha.8. CEP-path registration is not yet available in this Rust MCP transport and fails loud when true."
                }),
            )
            .require("query")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(query) = invocation.get_str("query").map(str::to_string) else {
            return ToolResult::error("missing `query` argument");
        };
        let language = invocation
            .get_str("language")
            .map(str::trim)
            .filter(|language| !language.is_empty())
            .unwrap_or("cqelsql")
            .to_ascii_lowercase();
        if language != "cqelsql" {
            return ToolResult::error(format!(
                "unsupported register_stream_query language '{language}'; supported: cqelsql"
            ));
        }
        let cep = invocation
            .get("cep")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if cep {
            return ToolResult::error(
                "cep=true stream queries are not yet supported by cqels-rs MCP",
            );
        }
        let requested_query_id = invocation
            .get_str("queryId")
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
        if let Some(query_id) = requested_query_id.as_ref() {
            let registrations = self.hub.inner.registrations.lock();
            let mut pending = self.hub.inner.pending_registrations.lock();
            if registrations.contains_key(query_id) || !pending.insert(query_id.clone()) {
                return ToolResult::error(format!("Query already registered: {query_id}"));
            }
        }
        let _pending_guard = requested_query_id
            .as_ref()
            .map(|query_id| PendingRegistrationGuard::new(self.hub.clone(), query_id.clone()));
        let buffer_size = invocation
            .get("bufferSize")
            .and_then(|value| value.as_i64())
            .map(|value| value.clamp(1, MAX_BUFFER_SIZE as i64) as usize)
            .unwrap_or(DEFAULT_BUFFER_SIZE);
        let notify = invocation
            .get("notify")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let engine = self.hub.inner.engine.clone();
        let hub_for_listener = self.hub.clone();
        let access_policy_for_listener = self.access_policy.clone();

        // `register_cqelsql_query` is async; block via the bound handle.
        // The query_id is assigned inside the call, so we capture it
        // *after* registration and wire it into the listener via a
        // late-binding cell.
        let registration = self.hub.inner.handle.block_on(async move {
            // We need to know the query_id before registering so the
            // listener can route results to the right buffer slot.
            // The engine assigns the ID, so we use a shared cell.
            let id_cell: Arc<parking_lot::Mutex<Option<String>>> =
                Arc::new(parking_lot::Mutex::new(None));
            let id_cell_listener = id_cell.clone();
            let hub_for_results = hub_for_listener.clone();
            let listener = listener_from_fn(move |result: BindingSet| {
                if governance_active(access_policy_for_listener.as_ref()) {
                    return;
                }
                let id_guard = id_cell_listener.lock();
                if let Some(id) = id_guard.as_ref() {
                    hub_for_results.record_result(id.clone(), result);
                }
            });
            let engine_query_id = engine.register_cqelsql_query(&query, listener).await?;
            let query_id = requested_query_id.unwrap_or_else(|| engine_query_id.clone());
            hub_for_listener.inner.registrations.lock().insert(
                query_id.clone(),
                StreamRegistration {
                    engine_query_id: engine_query_id.clone(),
                    buffer_size,
                    notify,
                },
            );
            *id_cell.lock() = Some(query_id.clone());
            Ok::<(String, String), cqels_model::CqelsError>((query_id, engine_query_id))
        });
        match registration {
            Ok((query_id, engine_query_id)) => ToolResult::success(json!({
                "ok": true,
                "query_id": query_id,
                "queryId": query_id,
                "engineQueryId": engine_query_id,
                "status": "registered",
                "bufferSize": buffer_size,
                "notify": notify,
                "cep": cep,
            })),
            Err(e) => ToolResult::error(format!("register failed: {e}")),
        }
    }
}

// ─── list_stream_queries ────────────────────────────────────────────

/// Constructs the `list_stream_queries` MCP tool.
pub fn list_stream_queries_tool(hub: StreamQueryHub) -> ListStreamQueriesTool {
    ListStreamQueriesTool {
        hub,
        access_policy: None,
    }
}

/// Constructs `list_stream_queries` with Java alpha.9/alpha.10 governance.
pub fn list_stream_queries_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> ListStreamQueriesTool {
    ListStreamQueriesTool {
        hub,
        access_policy: Some(access_policy),
    }
}

pub struct ListStreamQueriesTool {
    hub: StreamQueryHub,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

impl McpTool for ListStreamQueriesTool {
    fn name(&self) -> &str {
        "list_stream_queries"
    }

    fn description(&self) -> &str {
        "List the IDs of all currently registered stream queries."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
    }

    fn call(&self, _invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let ids = self.hub.registered_query_ids();
        ToolResult::success(json!({
            "count": ids.len(),
            "query_ids": ids,
            "queryIds": ids,
        }))
    }
}

// ─── forget_stream_query ────────────────────────────────────────────

/// Constructs the Java-compatible `forget_stream_query` MCP tool.
pub fn forget_stream_query_tool(hub: StreamQueryHub) -> ForgetStreamQueryTool {
    ForgetStreamQueryTool { hub }
}

pub struct ForgetStreamQueryTool {
    hub: StreamQueryHub,
}

impl McpTool for ForgetStreamQueryTool {
    fn name(&self) -> &str {
        "forget_stream_query"
    }

    fn description(&self) -> &str {
        "Stop and deregister a continuous query previously created with \
         register_stream_query: unregisters its engine subscription and \
         discards its result buffer."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "ID of the stream query to stop and deregister (as returned by register_stream_query)"
                }),
            )
            .require("queryId")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let query_id = match query_id_for_forget(invocation) {
            Some(query_id) => query_id,
            None => return ToolResult::error("Missing required parameter: queryId"),
        };

        match self.hub.unregister_query(&query_id) {
            Ok(()) => ToolResult::success(json!({
                "queryId": query_id,
                "status": "forgotten",
            })),
            Err(e) if stream_query_not_found(&e) => {
                ToolResult::error(format!("No such registered stream query: {query_id}"))
            }
            Err(e) => ToolResult::error(format!("forget_stream_query failed: {e}")),
        }
    }
}

fn query_id_for_forget(invocation: &ToolInvocation) -> Option<String> {
    // A present-but-invalid Java key should fail like Java rather than
    // falling through to the Rust snake_case compatibility alias.
    if invocation.get("queryId").is_some() {
        return invocation
            .get_str("queryId")
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
    }

    invocation
        .get_str("query_id")
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

fn stream_query_not_found(error: &CqelsError) -> bool {
    matches!(error, CqelsError::Stream { message } if message.contains("not found"))
}

// ─── unregister_stream_query ────────────────────────────────────────

/// Constructs the `unregister_stream_query` MCP tool.
pub fn unregister_stream_query_tool(hub: StreamQueryHub) -> UnregisterStreamQueryTool {
    UnregisterStreamQueryTool { hub }
}

pub struct UnregisterStreamQueryTool {
    hub: StreamQueryHub,
}

impl McpTool for UnregisterStreamQueryTool {
    fn name(&self) -> &str {
        "unregister_stream_query"
    }

    fn description(&self) -> &str {
        "Cancel and remove a previously registered stream query by ID. \
         Also clears any buffered results for that query."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query_id",
                json!({
                    "type": "string",
                    "description": "Query ID returned by register_stream_query"
                }),
            )
            .require("query_id")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        let Some(query_id) = invocation.get_str("query_id").map(str::to_string) else {
            return ToolResult::error("missing `query_id` argument");
        };

        match self.hub.unregister_query(&query_id) {
            Ok(()) => ToolResult::success(json!({
                "ok": true,
                "query_id": query_id,
            })),
            Err(e) => ToolResult::error(format!("unregister failed: {e}")),
        }
    }
}

// ─── poll_stream_results ────────────────────────────────────────────

/// Constructs the `poll_stream_results` MCP tool.
pub fn poll_stream_results_tool(hub: StreamQueryHub) -> PollStreamResultsTool {
    PollStreamResultsTool {
        hub,
        access_policy: None,
    }
}

/// Constructs `poll_stream_results` with Java alpha.9/alpha.10 governance.
pub fn poll_stream_results_tool_with_access_policy(
    hub: StreamQueryHub,
    access_policy: Arc<AccessPolicyRegistry>,
) -> PollStreamResultsTool {
    PollStreamResultsTool {
        hub,
        access_policy: Some(access_policy),
    }
}

pub struct PollStreamResultsTool {
    hub: StreamQueryHub,
    access_policy: Option<Arc<AccessPolicyRegistry>>,
}

const DEFAULT_POLL_LIMIT: usize = 64;

impl McpTool for PollStreamResultsTool {
    fn name(&self) -> &str {
        "poll_stream_results"
    }

    fn description(&self) -> &str {
        "Drain buffered results for a registered stream query. Returns up \
         to `limit` bindings in FIFO order and removes them from the \
         buffer. Defaults to 64 per call."
    }

    fn input_schema(&self) -> ToolInputSchema {
        ToolInputSchema::object()
            .with_property(
                "query_id",
                json!({
                    "type": "string",
                    "description": "Query ID returned by register_stream_query"
                }),
            )
            .with_property(
                "queryId",
                json!({
                    "type": "string",
                    "description": "Java-compatible query ID alias"
                }),
            )
            .with_property(
                "limit",
                json!({
                    "type": "integer",
                    "description": "Maximum number of buffered results to drain. Defaults to 64.",
                    "default": DEFAULT_POLL_LIMIT
                }),
            )
            .require("query_id")
    }

    fn call(&self, invocation: &ToolInvocation) -> ToolResult {
        if governance_active(self.access_policy.as_ref()) {
            return governance_denial(self.name());
        }
        let Some(query_id) = invocation
            .get_str("query_id")
            .or_else(|| invocation.get_str("queryId"))
        else {
            return ToolResult::error("missing `query_id` argument");
        };
        let limit = invocation
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_POLL_LIMIT);
        let drained = self.hub.drain_result_values(query_id, limit);
        let remaining = self.hub.buffered_count(query_id);
        ToolResult::success(json!({
            "query_id": query_id,
            "queryId": query_id,
            "count": drained.len(),
            "remaining": remaining,
            "results": drained,
        }))
    }
}

fn required_nonempty_str(invocation: &ToolInvocation, key: &str) -> Option<String> {
    invocation
        .get_str(key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn buffer_size_from(invocation: &ToolInvocation) -> usize {
    invocation
        .get("bufferSize")
        .and_then(JsonValue::as_i64)
        .map(|value| value.clamp(1, MAX_BUFFER_SIZE as i64) as usize)
        .unwrap_or(DEFAULT_BUFFER_SIZE)
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn generated_query_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn observer_query_id(prefix: &str, invocation: &ToolInvocation) -> String {
    // Mirrors Java alpha.10: `queryId` is a caller-provided suffix, not a full
    // id. Supplying `rl-foo` intentionally returns `rl-rl-foo`.
    invocation
        .get_str("queryId")
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(|suffix| format!("{prefix}-{suffix}"))
        .unwrap_or_else(|| generated_query_id(prefix))
}

fn shapes_payload_chars(value: &JsonValue) -> usize {
    if let Some(nquads) = value.as_str() {
        return nquads.len();
    }
    if let Some(nquads) = value.get("nquads").and_then(JsonValue::as_str) {
        return nquads.len();
    }
    serde_json::to_string(value)
        .map(|text| text.len())
        .unwrap_or(usize::MAX)
}

fn stream_reasoning_profile_from_env() -> Option<ReasoningProfile> {
    let value = std::env::var("CQELS_MCP_REASONING").ok()?;
    match parse_stream_reasoning_profile(&value) {
        Ok(profile) => profile,
        Err(message) => {
            eprintln!("cqels-mcp: {message}; stream reasoning disabled");
            None
        }
    }
}

fn parse_stream_reasoning_profile(value: &str) -> Result<Option<ReasoningProfile>, String> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "off" | "none" => Ok(None),
        "rdfs" => Ok(Some(ReasoningProfile::Rdfs)),
        "rdfs-full" | "rdfs_full" => Ok(Some(ReasoningProfile::RdfsFull)),
        other => Err(format!(
            "unknown CQELS_MCP_REASONING value '{other}' (expected rdfs, rdfs-full, or off)"
        )),
    }
}

fn valid_event_time(ms: i64) -> Result<i64, String> {
    if !(0..=MAX_EVENT_TIME).contains(&ms) {
        return Err(format!(
            "eventTime out of range [0, {MAX_EVENT_TIME}]: {ms}"
        ));
    }
    Ok(ms)
}

fn parse_event_time(value: Option<&JsonValue>) -> Result<i64, String> {
    let Some(value) = value else {
        return valid_event_time(current_timestamp_ms());
    };
    if let Some(ms) = value.as_i64() {
        return valid_event_time(ms);
    }
    if let Some(ms) = value.as_u64() {
        if ms > MAX_EVENT_TIME as u64 {
            return Err(format!(
                "eventTime out of range [0, {MAX_EVENT_TIME}]: {ms}"
            ));
        }
        return Ok(ms as i64);
    }
    if let Some(ms) = value.as_f64() {
        if !ms.is_finite() {
            return Err(format!("eventTime is not a finite number: {ms}"));
        }
        if ms.fract() != 0.0 {
            return Err(format!(
                "eventTime must be a whole number of epoch millis, got: {ms}"
            ));
        }
        if ms < 0.0 || ms > MAX_EVENT_TIME as f64 {
            return Err(format!(
                "eventTime out of range [0, {MAX_EVENT_TIME}]: {ms}"
            ));
        }
        return Ok(ms as i64);
    }
    if let Some(text) = value.as_str() {
        let trimmed = text.trim();
        if let Ok(ms) = trimmed.parse::<i64>() {
            return valid_event_time(ms);
        }
        if let Some(ms) = parse_rfc3339_utc_millis(trimmed) {
            return valid_event_time(ms);
        }
    }
    Err("eventTime must be Unix milliseconds or UTC RFC3339".to_string())
}

fn event_payload_chars(event: &serde_json::Map<String, JsonValue>) -> usize {
    if let Some(nquads) = event
        .get("nquads")
        .and_then(JsonValue::as_str)
        .filter(|nquads| !nquads.trim().is_empty())
    {
        return nquads.len();
    }
    event
        .get("facts")
        .and_then(JsonValue::as_array)
        .map(|facts| fact_array_payload_chars(facts))
        .unwrap_or(0)
}

fn fact_array_payload_chars(facts: &[JsonValue]) -> usize {
    facts.iter().map(fact_payload_chars).sum()
}

fn fact_payload_chars(fact: &JsonValue) -> usize {
    let Some(object) = fact.as_object() else {
        return 0;
    };
    ["subject", "predicate", "object"]
        .into_iter()
        .filter_map(|key| object.get(key).and_then(JsonValue::as_str))
        .map(str::len)
        .sum()
}

fn parse_rfc3339_utc_millis(value: &str) -> Option<i64> {
    let value = value.strip_suffix('Z')?;
    let (date, time) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second_part = time_parts.next()?;
    if time_parts.next().is_some() {
        return None;
    }
    let (second_text, fraction_text) = second_part
        .split_once('.')
        .map(|(second, fraction)| (second, Some(fraction)))
        .unwrap_or((second_part, None));
    let second = second_text.parse::<u32>().ok()?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let mut millis = 0u32;
    if let Some(fraction) = fraction_text {
        if fraction.is_empty() || !fraction.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        let digits = fraction.chars().take(3).collect::<String>();
        millis = format!("{digits:0<3}").parse::<u32>().ok()?;
    }
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400_000)?
        .checked_add(hour as i64 * 3_600_000)?
        .checked_add(minute as i64 * 60_000)?
        .checked_add(second as i64 * 1000)?
        .checked_add(millis as i64)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_shapes_argument(value: &JsonValue) -> Result<Vec<Statement>, String> {
    if let Some(nquads) = value.as_str() {
        return parse_nquads_messages(nquads)
            .map(|messages| messages.into_iter().flatten().collect());
    }
    if value.is_array() {
        return parse_fact_array(value, "shapes");
    }
    if let Some(facts) = value.get("facts") {
        return parse_fact_array(facts, "shapes.facts");
    }
    if let Some(nquads) = value.get("nquads").and_then(JsonValue::as_str) {
        return parse_nquads_messages(nquads)
            .map(|messages| messages.into_iter().flatten().collect());
    }
    Err(
        "`shapes` must be an array, an RDF-message N-Quads string, or an object with facts/nquads"
            .to_string(),
    )
}

fn parse_fact_array(value: &JsonValue, field: &str) -> Result<Vec<Statement>, String> {
    let Some(items) = value.as_array() else {
        return Err(format!("{field} must be an array"));
    };
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| parse_fact_statement(item, &format!("{field}[{idx}]")))
        .collect()
}

fn parse_fact_statement(value: &JsonValue, field: &str) -> Result<Statement, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{field} must be an object"))?;
    let get = |primary: &str, alias: &str| {
        object
            .get(primary)
            .or_else(|| object.get(alias))
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("{field}.{primary} must be a non-empty string"))
    };
    let subject =
        subject_term(&get("subject", "s")?).map_err(|e| format!("{field}.subject: {e}"))?;
    let predicate = IriTerm::new(
        expand_iri(&get("predicate", "p")?).map_err(|e| format!("{field}.predicate: {e}"))?,
    );
    let object_value = get("object", "o")?;
    let object_type = java_fact_object_type(object.get("objectType"), field)?;
    let object_term = match object_type.as_str() {
        "uri" => Term::Iri(IriTerm::new(
            expand_iri(&object_value).map_err(|e| format!("{field}.object: {e}"))?,
        )),
        "literal" => {
            let mut literal = LiteralTerm::new(object_value);
            if let Some(datatype) = object.get("datatype").and_then(JsonValue::as_str) {
                literal = literal.with_datatype(
                    expand_iri(datatype).map_err(|e| format!("{field}.datatype: {e}"))?,
                );
            }
            if let Some(language) = object.get("language").and_then(JsonValue::as_str) {
                literal = literal.with_language(language);
            }
            Term::Literal(literal)
        }
        _ => unreachable!("java_fact_object_type only returns uri or literal"),
    };
    Ok(Statement::new(subject, predicate, object_term))
}

fn java_fact_object_type(value: Option<&JsonValue>, field: &str) -> Result<String, String> {
    let Some(value) = value else {
        return Ok("literal".to_string());
    };
    if value.is_null() {
        return Ok("literal".to_string());
    }
    let object_type = value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    if object_type == "uri" || object_type == "literal" {
        return Ok(object_type);
    }
    Err(format!(
        "{field}.objectType must be 'uri' or 'literal', got: {object_type}"
    ))
}

fn subject_term(value: &str) -> Result<Term, String> {
    let trimmed = value.trim();
    if trimmed.starts_with("_:") {
        return Ok(Term::BlankNode(cqels_model::BlankNodeTerm::new(
            blank_node_id(trimmed),
        )));
    }
    Ok(Term::Iri(IriTerm::new(expand_iri(trimmed)?)))
}

fn blank_node_id(value: &str) -> String {
    value.trim().trim_start_matches("_:").to_string()
}

fn expand_iri(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed == "a" {
        return Ok("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string());
    }
    if trimmed.starts_with('<') && trimmed.ends_with('>') && trimmed.len() >= 2 {
        return Ok(trimmed[1..trimmed.len() - 1].to_string());
    }
    if is_absolute_iri(trimmed) {
        return Ok(trimmed.to_string());
    }
    let Some((prefix, local)) = trimmed.split_once(':') else {
        return Err(format!(
            "'{trimmed}' must be an absolute IRI, bracketed IRI, or known prefixed name"
        ));
    };
    let base = match prefix {
        "rdf" => "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs" => "http://www.w3.org/2000/01/rdf-schema#",
        "owl" => "http://www.w3.org/2002/07/owl#",
        "xsd" => "http://www.w3.org/2001/XMLSchema#",
        "sh" => "http://www.w3.org/ns/shacl#",
        "ex" => "http://example.org/",
        "cqels" => "cqels://ontology/",
        "sosa" => "http://www.w3.org/ns/sosa/",
        "saref" => "https://saref.etsi.org/core/",
        "qudt" => "http://qudt.org/schema/qudt/",
        "unit" => "http://qudt.org/vocab/unit/",
        _ if is_valid_iri_scheme(prefix) => return Ok(trimmed.to_string()),
        _ => {
            return Err(format!("unknown prefix '{prefix}' in '{trimmed}'"));
        }
    };
    Ok(format!("{base}{local}"))
}

fn is_absolute_iri(value: &str) -> bool {
    let Some((scheme, rest)) = value.split_once(':') else {
        return false;
    };
    !rest.is_empty() && is_valid_iri_scheme(scheme)
}

fn is_valid_iri_scheme(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(|ch| ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

fn parse_nquads_messages(input: &str) -> Result<Vec<Vec<Statement>>, String> {
    let mut messages = Vec::new();
    let mut current = Vec::<String>::new();
    let mut opened = false;
    let mut last_was_delimiter = false;

    for line in input.lines() {
        let directive = line
            .split_once('#')
            .map(|(before, _)| before)
            .unwrap_or(line)
            .trim();
        if directive.is_empty() {
            continue;
        }
        if is_version_directive(directive) {
            continue;
        }
        if directive == "MESSAGE" {
            if opened || last_was_delimiter {
                messages.push(parse_nquads_block(&current.join("\n"))?);
            } else {
                messages.push(Vec::new());
            }
            current.clear();
            opened = true;
            last_was_delimiter = true;
            continue;
        }
        current.push(line.to_string());
        opened = true;
        last_was_delimiter = false;
    }

    if opened && !last_was_delimiter {
        messages.push(parse_nquads_block(&current.join("\n"))?);
    }
    Ok(messages)
}

fn is_version_directive(line: &str) -> bool {
    line.starts_with("VERSION ") || line.starts_with("@version ")
}

fn parse_nquads_block(input: &str) -> Result<Vec<Statement>, String> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut statements = Vec::new();
    for (idx, quad) in NQuadsParser::new().for_reader(input.as_bytes()).enumerate() {
        let quad = quad.map_err(|e| format!("N-Quads parse error at #{idx}: {e}"))?;
        statements.push(Statement::from(quad));
    }
    validate_statement_graphs(&statements)?;
    Ok(statements)
}

fn validate_statement_graphs(statements: &[Statement]) -> Result<(), String> {
    for statement in statements {
        if let Some(graph) = &statement.graph {
            if graph.as_str().starts_with("cqels://") {
                return Err(format!(
                    "named graph '{}' is reserved for CQELS system data",
                    graph.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn parse_arg_names(value: Option<&JsonValue>) -> Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err("argNames must be an array".to_string());
    };
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for (idx, item) in items.iter().enumerate() {
        let Some(name) = item.as_str().map(str::trim).filter(|name| !name.is_empty()) else {
            return Err(format!("argNames[{idx}] must be a non-empty string"));
        };
        if !seen.insert(name.to_string()) {
            return Err(format!("duplicate argNames entry '{name}'"));
        }
        names.push(name.to_string());
    }
    if names.len() > MAX_ARG_NAMES {
        return Err(format!(
            "argNames must contain at most {MAX_ARG_NAMES} names"
        ));
    }
    Ok(names)
}

fn atom_terms_to_bindings(terms: &[String], arg_names: &[String]) -> JsonValue {
    let mut bindings = serde_json::Map::new();
    for (idx, term) in terms.iter().enumerate() {
        let key = arg_names
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("arg{}", idx + 1));
        bindings.insert(key, json!(term));
    }
    JsonValue::Object(bindings)
}

fn violations_to_json(violations: &[ShaclViolation]) -> Vec<JsonValue> {
    violations
        .iter()
        .map(|violation| {
            json!({
                "shape": violation.shape,
                "focus": violation.focus,
                "constraint": violation.constraint,
                "detail": violation.detail,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{InMemoryMemoryStore, MemoryStore};
    use crate::registry::ToolRegistry;
    use crate::tools::set_access_policy_tool;
    use cqels_asp::{AnswerSet, AspError, Atom};
    use cqels_engine::CqelsEngine;
    use std::time::Duration;
    use tokio::runtime::Runtime;

    struct StaticSolver {
        answer_sets: Vec<AnswerSet>,
    }

    #[async_trait]
    impl AspSolver for StaticSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            Ok(self.answer_sets.clone())
        }
    }

    struct CyclingSolver {
        values: Mutex<VecDeque<&'static str>>,
    }

    #[async_trait]
    impl AspSolver for CyclingSolver {
        async fn solve(
            &self,
            _program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            let value = self
                .values
                .lock()
                .pop_front()
                .unwrap_or("fallback")
                .to_string();
            Ok(vec![AnswerSet::new(vec![Atom::new("alert", vec![value])])])
        }
    }

    struct ProgramRecordingSolver {
        programs: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl AspSolver for ProgramRecordingSolver {
        async fn solve(
            &self,
            program: &str,
            _max_models: usize,
        ) -> Result<Vec<AnswerSet>, AspError> {
            self.programs.lock().push(program.to_string());
            Ok(vec![AnswerSet::new(Vec::new())])
        }
    }

    /// Build a fresh engine + multi-thread runtime + hub for tests.
    fn fresh_hub() -> (StreamQueryHub, Arc<Runtime>) {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        runtime.block_on(engine.start()).expect("engine starts");
        let handle = runtime.handle().clone();
        let hub = StreamQueryHub::new(Arc::new(engine), handle);
        (hub, runtime)
    }

    /// Like [`fresh_hub`] but pre-creates a `sensors` stream so a
    /// query that subscribes to `FROM STREAM sensors` stays alive past
    /// the initial `execute()` poll. Avoids races between the
    /// drain-task auto-cleanup and explicit `unregister` in tests
    /// that don't actually feed data through the stream.
    fn fresh_hub_with_sensors_stream() -> (StreamQueryHub, Arc<Runtime>) {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        runtime.block_on(engine.start()).expect("engine starts");
        // Create the `sensors` stream — we never push data into it,
        // we just need it registered so the query's drain task has
        // something to await rather than terminating on an empty
        // input.
        let _sender = runtime
            .block_on(async { engine.create_stream("sensors").await })
            .expect("create_stream");
        let handle = runtime.handle().clone();
        let hub = StreamQueryHub::new(Arc::new(engine), handle);
        (hub, runtime)
    }

    fn install_all(hub: &StreamQueryHub) -> ToolRegistry {
        let mut reg = ToolRegistry::new();
        reg.install(create_stream_tool(hub.clone()));
        reg.install(push_stream_events_tool(hub.clone()));
        reg.install(validate_stream_query_tool());
        reg.install(register_stream_query_tool(hub.clone()));
        reg.install(forget_stream_query_tool(hub.clone()));
        reg.install(list_stream_queries_tool(hub.clone()));
        reg.install(unregister_stream_query_tool(hub.clone()));
        reg.install(poll_stream_results_tool(hub.clone()));
        reg.install(watch_invariant_tool(hub.clone()));
        reg.install(register_rules_tool(hub.clone()));
        reg
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

    fn sample_query() -> &'static str {
        // Minimal valid CqelsQL query that the engine accepts.
        r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [RANGE 10s]
            WHERE { ?sensor <http://ex.org/temp> ?temp . }
        "#
    }

    fn poll_until_nonempty(reg: &ToolRegistry, query_id: &str) -> ToolResult {
        for _ in 0..50 {
            let poll = reg
                .call(
                    "poll_stream_results",
                    &ToolInvocation::new().with_arg("queryId", json!(query_id)),
                )
                .expect("dispatch");
            if poll.is_error || poll.content["count"].as_u64().unwrap_or(0) > 0 {
                return poll;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        reg.call(
            "poll_stream_results",
            &ToolInvocation::new().with_arg("queryId", json!(query_id)),
        )
        .expect("dispatch")
    }

    fn drain_notifications_until_nonempty(hub: &StreamQueryHub) -> Vec<String> {
        for _ in 0..50 {
            let notifications = hub.drain_result_notification_query_ids();
            if !notifications.is_empty() {
                return notifications;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        hub.drain_result_notification_query_ids()
    }

    #[test]
    fn stream_reasoning_profile_parser_accepts_java_alpha10_values() {
        assert_eq!(
            parse_stream_reasoning_profile("rdfs").unwrap(),
            Some(ReasoningProfile::Rdfs)
        );
        assert_eq!(
            parse_stream_reasoning_profile("RDFS_FULL").unwrap(),
            Some(ReasoningProfile::RdfsFull)
        );
        assert_eq!(parse_stream_reasoning_profile("off").unwrap(), None);
        assert!(parse_stream_reasoning_profile("owl").is_err());
    }

    #[test]
    fn observer_query_id_always_prepends_java_prefix() {
        let rule_id = observer_query_id(
            RULE_PREFIX,
            &ToolInvocation::new().with_arg("queryId", json!("rl-existing")),
        );
        assert_eq!(rule_id, "rl-rl-existing");

        let watch_id = observer_query_id(
            WATCH_PREFIX,
            &ToolInvocation::new().with_arg("queryId", json!("  custom  ")),
        );
        assert_eq!(watch_id, "wi-custom");

        let generated = observer_query_id(WATCH_PREFIX, &ToolInvocation::new());
        assert!(generated.starts_with("wi-"));
    }

    #[test]
    fn rfc3339_event_time_parser_rejects_invalid_calendar_dates() {
        assert_eq!(
            parse_rfc3339_utc_millis("1970-01-01T00:00:00.001Z"),
            Some(1)
        );
        assert_eq!(
            parse_rfc3339_utc_millis("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000)
        );
        assert_eq!(parse_rfc3339_utc_millis("2026-02-31T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_utc_millis("2026-04-31T00:00:00Z"), None);
        assert_eq!(parse_rfc3339_utc_millis("2026-07-11T12:00:60Z"), None);
        assert_eq!(parse_rfc3339_utc_millis("2026-07-11T12:00:00.1xZ"), None);
    }

    #[test]
    fn opt_in_stream_reasoning_emits_rdfs_inferred_triples() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        let hub = StreamQueryHub::new_with_stream_reasoning(
            Arc::new(engine),
            runtime.handle().clone(),
            Some(ReasoningProfile::Rdfs),
        );
        let child = "http://example.org/Child";
        let person = "http://example.org/Person";
        let alice = "http://example.org/alice";
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let rdfs_subclass = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

        let schema = Statement::new(
            Term::Iri(IriTerm::new(child)),
            IriTerm::new(rdfs_subclass),
            Term::Iri(IriTerm::new(person)),
        );
        let instance = Statement::new(
            Term::Iri(IriTerm::new(alice)),
            IriTerm::new(rdf_type),
            Term::Iri(IriTerm::new(child)),
        );

        let first = hub.apply_stream_reasoning("sensors", &[schema], 1);
        assert!(first.is_empty());
        let inferred = hub.apply_stream_reasoning("sensors", &[instance], 2);
        assert!(inferred.iter().any(|statement| {
            statement.subject == Term::Iri(IriTerm::new(alice))
                && statement.predicate == IriTerm::new(rdf_type)
                && statement.object == Term::Iri(IriTerm::new(person))
        }));
    }

    #[test]
    fn stream_reasoning_dispatches_original_then_inferred_observations() {
        let runtime = Arc::new(Runtime::new().expect("tokio runtime"));
        let engine = runtime
            .block_on(async { CqelsEngine::builder().build() })
            .expect("engine builds");
        let hub = StreamQueryHub::new_with_stream_reasoning(
            Arc::new(engine),
            runtime.handle().clone(),
            Some(ReasoningProfile::Rdfs),
        );
        let programs = Arc::new(Mutex::new(Vec::new()));
        let solver: Arc<dyn AspSolver> = Arc::new(ProgramRecordingSolver {
            programs: programs.clone(),
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("reasoned-rules"))
                    .with_arg("rules", json!("alert(X) :- rdf(X,P,O)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);

        let child = "http://example.org/Child";
        let person = "http://example.org/Person";
        let alice = "http://example.org/alice";
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let rdfs_subclass = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
        let schema = Statement::new(
            Term::Iri(IriTerm::new(child)),
            IriTerm::new(rdfs_subclass),
            Term::Iri(IriTerm::new(person)),
        );
        let instance = Statement::new(
            Term::Iri(IriTerm::new(alice)),
            IriTerm::new(rdf_type),
            Term::Iri(IriTerm::new(child)),
        );

        assert_eq!(
            hub.push_observation("sensors", vec![schema], 1)
                .expect("schema push"),
            1
        );
        assert_eq!(
            hub.push_observation("sensors", vec![instance], 2)
                .expect("instance push"),
            2
        );

        let programs = programs.lock();
        let inferred_type_fact = r#"rdf(iri("http://example.org/alice"),iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),iri("http://example.org/Person"))."#;
        assert_eq!(programs.len(), 3);
        assert!(programs[1].contains("http://example.org/Child"));
        assert!(!programs[1].contains(inferred_type_fact));
        assert!(programs[2].contains(inferred_type_fact));
    }

    #[test]
    fn create_stream_is_idempotent_and_push_accepts_facts_and_rdf_messages() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);

        let created = reg
            .call(
                "create_stream",
                &ToolInvocation::new().with_arg("stream", json!("sensors")),
            )
            .expect("dispatch");
        assert!(!created.is_error, "create failed: {:?}", created.content);
        assert_eq!(created.content["created"], true);

        let exists = reg
            .call(
                "create_stream",
                &ToolInvocation::new().with_arg("stream", json!("sensors")),
            )
            .expect("dispatch");
        assert!(
            !exists.is_error,
            "idempotent create failed: {:?}",
            exists.content
        );
        assert_eq!(exists.content["created"], false);

        let nquads = r#"VERSION "1.2-messages"
<http://example.org/s2> <http://example.org/p> "v2" .
MESSAGE
<http://example.org/s3> <http://example.org/p> "v3" .
"#;
        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([
                            {
                                "eventTime": "2026-07-11T12:00:00.123Z",
                                "facts": [{
                                    "subject": "ex:s1",
                                    "predicate": "ex:p",
                                    "object": "v1",
                                    "objectType": "literal"
                                }]
                            },
                            {
                                "eventTime": "2026-07-11T12:00:00.123Z",
                                "nquads": nquads
                            }
                        ]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);
        assert_eq!(pushed.content["eventCount"], 2);
        assert_eq!(pushed.content["observationCount"], 3);
        assert_eq!(pushed.content["inputStatementCount"], 3);
        assert_eq!(pushed.content["statementCount"], 3);
        assert!(hub
            .registered_stream_names()
            .iter()
            .any(|name| name == "sensors"));
    }

    #[test]
    fn push_stream_events_uses_nquads_payload_when_facts_are_also_present() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 10,
                            "facts": [{
                                "subject": "ex:ignored",
                                "predicate": "ex:p",
                                "object": "ignored",
                                "objectType": "literal"
                            }],
                            "nquads": "<http://example.org/s> <http://example.org/p> \"v\" .\n"
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);
        assert_eq!(pushed.content["observationCount"], 1);
        assert_eq!(pushed.content["inputStatementCount"], 1);
        assert_eq!(pushed.content["statementCount"], 1);
    }

    #[test]
    fn push_stream_events_skips_empty_rdf_messages() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 10,
                            "nquads": "VERSION \"1.2-messages\"\nMESSAGE\n<http://example.org/s> <http://example.org/p> \"v\" .\n"
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);
        assert_eq!(pushed.content["observationCount"], 1);
        assert_eq!(pushed.content["inputStatementCount"], 1);
        assert_eq!(pushed.content["statementCount"], 1);
    }

    #[test]
    fn push_stream_events_rejects_empty_events_array() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("events", json!([])),
            )
            .expect("dispatch");
        assert!(pushed.is_error);
        assert!(pushed.content["message"]
            .as_str()
            .unwrap()
            .contains("non-empty"));
    }

    #[test]
    fn push_stream_events_rejects_empty_facts_array() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1,
                            "facts": []
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(pushed.is_error);
        assert!(pushed.content["message"]
            .as_str()
            .unwrap()
            .contains("non-empty `facts`"));
    }

    #[test]
    fn push_stream_events_fact_object_type_defaults_to_literal_like_java() {
        for fact in [
            json!({
                "subject": "ex:s1",
                "predicate": "ex:p",
                "object": "ex:o"
            }),
            json!({
                "subject": "ex:s1",
                "predicate": "ex:p",
                "object": "ex:o",
                "objectType": null
            }),
            json!({
                "subject": "ex:s1",
                "predicate": "ex:p",
                "object": "ex:o",
                "object_type": "uri"
            }),
        ] {
            let statement = parse_fact_statement(&fact, "facts[0]").expect("fact parses");
            let literal = statement
                .object
                .as_literal()
                .expect("Java defaults absent/null/alias objectType to literal");
            assert_eq!(literal.value(), "ex:o");
        }
    }

    #[test]
    fn push_stream_events_rejects_java_invalid_object_type_aliases() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for object_type in [
            json!("iri"),
            json!("blank"),
            json!("bnode"),
            json!("URI"),
            json!(true),
        ] {
            let pushed = reg
                .call(
                    "push_stream_events",
                    &ToolInvocation::new()
                        .with_arg("stream", json!("sensors"))
                        .with_arg(
                            "events",
                            json!([{
                                "eventTime": 1,
                                "facts": [{
                                    "subject": "ex:s1",
                                    "predicate": "ex:p",
                                    "object": "ex:o",
                                    "objectType": object_type
                                }]
                            }]),
                        ),
                )
                .expect("dispatch");
            assert!(pushed.is_error, "{:?} should fail", object_type);
            assert!(
                pushed.content["message"]
                    .as_str()
                    .unwrap()
                    .contains("objectType must be 'uri' or 'literal'"),
                "{object_type:?} should use Java error wording: {:?}",
                pushed.content
            );
        }
    }

    #[test]
    fn push_stream_events_schema_advertises_java_fact_object_type_contract() {
        let (hub, _rt) = fresh_hub();
        let schema = push_stream_events_tool(hub).input_schema();
        let object_type_schema = &schema.properties["events"]["items"]["properties"]["facts"]
            ["items"]["properties"]["objectType"];
        assert_eq!(object_type_schema["enum"], json!(["uri", "literal"]));
        assert_eq!(object_type_schema["default"], "literal");
    }

    #[test]
    fn push_stream_events_rejects_invalid_event_time_bounds() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for (event_time, expected) in [
            (json!(1.9), "whole number"),
            (json!(-1), "out of range"),
            (json!(MAX_EVENT_TIME + 1), "out of range"),
            (json!("-1"), "out of range"),
            (json!("1969-12-31T23:59:59Z"), "out of range"),
        ] {
            let pushed = reg
                .call(
                    "push_stream_events",
                    &ToolInvocation::new()
                        .with_arg("stream", json!("sensors"))
                        .with_arg(
                            "events",
                            json!([{
                                "eventTime": event_time,
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
            assert!(pushed.is_error, "{event_time:?} should fail");
            assert!(
                pushed.content["message"]
                    .as_str()
                    .unwrap()
                    .contains(expected),
                "{event_time:?} should contain {expected}: {:?}",
                pushed.content
            );
        }
    }

    #[test]
    fn push_stream_events_rejects_message_and_observation_caps() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);

        let too_many_facts = (0..=MAX_STATEMENTS_PER_MESSAGE)
            .map(|idx| {
                json!({
                    "subject": format!("http://example.org/s{idx}"),
                    "predicate": "http://example.org/p",
                    "object": "http://example.org/o",
                    "objectType": "uri"
                })
            })
            .collect::<Vec<_>>();
        let facts_result = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1,
                            "facts": too_many_facts
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(facts_result.is_error);
        assert!(facts_result.content["message"]
            .as_str()
            .unwrap()
            .contains("facts array too large"));

        let mut nquads = String::from("VERSION \"1.2-messages\"\n");
        for idx in 0..=MAX_TOTAL_OBSERVATIONS {
            if idx > 0 {
                nquads.push_str("MESSAGE\n");
            }
            nquads.push_str(&format!(
                "<http://example.org/s{idx}> <http://example.org/p> <http://example.org/o> .\n"
            ));
        }
        let observations_result = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1,
                            "nquads": nquads
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(observations_result.is_error);
        assert!(observations_result.content["message"]
            .as_str()
            .unwrap()
            .contains("too many observations"));
    }

    #[test]
    fn push_stream_events_rejects_character_budgets() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);

        let huge_literal = "x".repeat(MAX_NQUADS_CHARS + 1);
        let per_event = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1,
                            "facts": [{
                                "subject": "http://example.org/s",
                                "predicate": "http://example.org/p",
                                "object": huge_literal,
                                "objectType": "literal"
                            }]
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(per_event.is_error);
        assert!(per_event.content["message"]
            .as_str()
            .unwrap()
            .contains("facts payload exceeds"));

        let big_literal = "y".repeat(1_800_000);
        let events = (0..5)
            .map(|idx| {
                json!({
                    "eventTime": idx + 1,
                    "facts": [{
                        "subject": format!("http://example.org/s{idx}"),
                        "predicate": "http://example.org/p",
                        "object": big_literal,
                        "objectType": "literal"
                    }]
                })
            })
            .collect::<Vec<_>>();
        let total = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("events", JsonValue::Array(events)),
            )
            .expect("dispatch");
        assert!(total.is_error);
        assert!(total.content["message"]
            .as_str()
            .unwrap()
            .contains("too many characters in one call"));
    }

    #[test]
    fn create_and_push_stream_events_enforce_stream_cap() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for idx in 0..MAX_STREAMS {
            hub.create_stream(&format!("filler-{idx}"))
                .expect("stream creation below cap");
        }

        let create = reg
            .call(
                "create_stream",
                &ToolInvocation::new().with_arg("stream", json!("one-too-many")),
            )
            .expect("dispatch");
        assert!(create.is_error);
        assert!(create.content["message"]
            .as_str()
            .unwrap()
            .contains("stream limit reached"));

        let push = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("one-too-many"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1,
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
        assert!(push.is_error);
        assert!(push.content["message"]
            .as_str()
            .unwrap()
            .contains("stream limit reached"));
    }

    #[test]
    fn push_stream_events_preserves_multi_statement_observation_for_triples_window() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let query = r#"
            SELECT ?sensor ?temp ?status
            FROM STREAM sensors [TRIPLES 1]
            WHERE {
                STREAM sensors {
                    ?sensor <http://example.org/temp> ?temp .
                    ?sensor <http://example.org/status> ?status .
                }
            }
        "#;

        let registered = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(query))
                    .with_arg("queryId", json!("graph-window")),
            )
            .expect("dispatch");
        assert!(
            !registered.is_error,
            "register failed: {:?}",
            registered.content
        );

        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1000,
                            "facts": [
                                {
                                    "subject": "http://example.org/s1",
                                    "predicate": "http://example.org/temp",
                                    "object": "42",
                                    "objectType": "literal"
                                },
                                {
                                    "subject": "http://example.org/s1",
                                    "predicate": "http://example.org/status",
                                    "object": "ok",
                                    "objectType": "literal"
                                }
                            ]
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);
        assert_eq!(pushed.content["observationCount"], 1);
        assert_eq!(pushed.content["statementCount"], 2);

        let poll = poll_until_nonempty(&reg, "graph-window");
        assert!(!poll.is_error, "poll failed: {:?}", poll.content);
        assert_eq!(poll.content["count"], 1);
        assert!(poll.content["results"][0]["bindings"].get("temp").is_some());
        assert!(poll.content["results"][0]["bindings"]
            .get("status")
            .is_some());
    }

    #[test]
    fn validate_stream_query_reports_validity_without_registering() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);

        let valid = reg
            .call(
                "validate_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        assert!(!valid.is_error);
        assert_eq!(valid.content["valid"], true);
        assert_eq!(hub.registered_query_ids().len(), 0);

        let invalid = reg
            .call(
                "validate_stream_query",
                &ToolInvocation::new().with_arg("query", json!("not a query")),
            )
            .expect("dispatch");
        assert!(!invalid.is_error);
        assert_eq!(invalid.content["valid"], false);
        assert!(invalid.content["error"].is_string());
    }

    #[test]
    fn watch_invariant_buffers_conforming_reports_when_requested() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(watch_invariant_tool_with_solver(hub.clone(), solver));
        reg.install(push_stream_events_tool(hub.clone()));
        reg.install(poll_stream_results_tool(hub.clone()));

        let registered = reg
            .call(
                "watch_invariant",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("test"))
                    .with_arg("reportConforming", json!(true))
                    .with_arg(
                        "shapes",
                        json!([
                            {
                                "subject": "ex:PersonShape",
                                "predicate": "a",
                                "object": "sh:NodeShape",
                                "objectType": "uri"
                            },
                            {
                                "subject": "ex:PersonShape",
                                "predicate": "sh:targetNode",
                                "object": "ex:alice",
                                "objectType": "uri"
                            }
                        ]),
                    ),
            )
            .expect("dispatch");
        assert!(
            !registered.is_error,
            "watch failed: {:?}",
            registered.content
        );
        assert_eq!(registered.content["queryId"], "wi-test");

        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "facts": [{
                                "subject": "ex:alice",
                                "predicate": "ex:name",
                                "object": "Alice",
                                "objectType": "literal"
                            }]
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);

        let poll = reg
            .call(
                "poll_stream_results",
                &ToolInvocation::new().with_arg("queryId", json!("wi-test")),
            )
            .expect("dispatch");
        assert!(!poll.is_error, "poll failed: {:?}", poll.content);
        assert_eq!(poll.content["count"], 1);
        assert_eq!(poll.content["results"][0]["type"], "watch_invariant");
        assert_eq!(poll.content["results"][0]["conforms"], true);
    }

    #[test]
    fn watch_invariant_rejects_java_alpha10_shape_size_and_registration_cap() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(watch_invariant_tool_with_solver(hub.clone(), solver));

        let too_large = "x".repeat(MAX_SHAPES_CHARS + 1);
        let rejected = reg
            .call(
                "watch_invariant",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("shapes", json!(too_large)),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        assert!(rejected.content["message"]
            .as_str()
            .unwrap()
            .contains("shapes too large"));

        for idx in 0..MAX_WATCH_REGISTRATIONS {
            let registered = reg
                .call(
                    "watch_invariant",
                    &ToolInvocation::new()
                        .with_arg("stream", json!("sensors"))
                        .with_arg("queryId", json!(format!("cap-{idx}")))
                        .with_arg(
                            "shapes",
                            json!([{
                                "subject": "ex:Shape",
                                "predicate": "a",
                                "object": "sh:NodeShape",
                                "objectType": "uri"
                            }]),
                        ),
                )
                .expect("dispatch");
            assert!(!registered.is_error, "{:?}", registered.content);
            assert_eq!(registered.content["queryId"], format!("wi-cap-{idx}"));
        }

        let over_cap = reg
            .call(
                "watch_invariant",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("cap-overflow"))
                    .with_arg(
                        "shapes",
                        json!([{
                            "subject": "ex:Shape",
                            "predicate": "a",
                            "object": "sh:NodeShape",
                            "objectType": "uri"
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(over_cap.is_error);
        assert!(over_cap.content["message"]
            .as_str()
            .unwrap()
            .contains("watch_invariant limit reached"));
    }

    #[test]
    fn register_rules_buffers_matching_answer_atoms() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![Atom::new(
                "alert",
                vec!["alice".to_string()],
            )])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));
        reg.install(push_stream_events_tool(hub.clone()));
        reg.install(poll_stream_results_tool(hub.clone()));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("test"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert"))
                    .with_arg("argNames", json!(["who"])),
            )
            .expect("dispatch");
        assert!(
            !registered.is_error,
            "register_rules failed: {:?}",
            registered.content
        );
        assert_eq!(registered.content["queryId"], "rl-test");

        let pushed = reg
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
        assert!(!pushed.is_error, "push failed: {:?}", pushed.content);

        let poll = reg
            .call(
                "poll_stream_results",
                &ToolInvocation::new().with_arg("query_id", json!("rl-test")),
            )
            .expect("dispatch");
        assert!(!poll.is_error, "poll failed: {:?}", poll.content);
        assert_eq!(poll.content["count"], 1);
        assert_eq!(poll.content["results"][0]["type"], "register_rules");
        assert_eq!(poll.content["results"][0]["count"], 1);
        assert_eq!(
            poll.content["results"][0]["results"][0]["bindings"]["who"],
            "alice"
        );
    }

    #[test]
    fn register_rules_rejects_java_alpha10_rules_args_and_registration_cap() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));

        let too_large = "a".repeat(MAX_RULES_CHARS + 1);
        let rejected = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("rules", json!(too_large))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        assert!(rejected.content["message"]
            .as_str()
            .unwrap()
            .contains("rules too large"));

        let too_many_args: Vec<String> =
            (0..=MAX_ARG_NAMES).map(|idx| format!("arg{idx}")).collect();
        let rejected = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert"))
                    .with_arg("argNames", json!(too_many_args)),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        assert!(rejected.content["message"]
            .as_str()
            .unwrap()
            .contains("at most 32"));

        let rejected = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert"))
                    .with_arg("argNames", json!(["who", "who"])),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        assert!(rejected.content["message"]
            .as_str()
            .unwrap()
            .contains("duplicate"));

        let rejected = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("syntax-stream"))
                    .with_arg("queryId", json!("syntax"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)"))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        let message = rejected.content["message"].as_str().unwrap();
        assert!(message.contains("Invalid ASP program"), "{message}");
        assert!(message.contains("terminating"), "{message}");
        assert!(hub.registered_query_ids().is_empty());
        assert!(hub.registered_stream_names().is_empty());

        let rejected = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("syntax-stream-2"))
                    .with_arg("queryId", json!("syntax-2"))
                    .with_arg("rules", json!("alert((alice)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(rejected.is_error);
        let message = rejected.content["message"].as_str().unwrap();
        assert!(message.contains("Invalid ASP program"), "{message}");
        assert!(message.contains("unclosed '('"), "{message}");
        assert!(hub.registered_query_ids().is_empty());
        assert!(hub.registered_stream_names().is_empty());

        for idx in 0..MAX_RULE_REGISTRATIONS {
            let registered = reg
                .call(
                    "register_rules",
                    &ToolInvocation::new()
                        .with_arg("stream", json!("sensors"))
                        .with_arg("queryId", json!(format!("cap-{idx}")))
                        .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                        .with_arg("resultPredicate", json!("alert")),
                )
                .expect("dispatch");
            assert!(!registered.is_error, "{:?}", registered.content);
            assert_eq!(registered.content["queryId"], format!("rl-cap-{idx}"));
        }

        let over_cap = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("cap-overflow"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(over_cap.is_error);
        assert!(over_cap.content["message"]
            .as_str()
            .unwrap()
            .contains("register_rules limit reached"));
    }

    #[test]
    fn notify_true_queues_result_resource_update_for_observers() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![Atom::new(
                "alert",
                vec!["alice".to_string()],
            )])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));
        reg.install(push_stream_events_tool(hub.clone()));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("notify-rules"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert"))
                    .with_arg("notify", json!(true)),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);
        assert!(hub.drain_result_notification_query_ids().is_empty());

        let pushed = reg
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
        assert_eq!(
            hub.drain_result_notification_query_ids(),
            vec!["rl-notify-rules".to_string()]
        );
    }

    #[test]
    fn notify_true_queues_result_resource_update_for_stream_queries() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let query = r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [TRIPLES 1]
            WHERE {
                STREAM sensors { ?sensor <http://ex.org/temp> ?temp . }
            }
        "#;

        let registered = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(query))
                    .with_arg("queryId", json!("notify-stream"))
                    .with_arg("notify", json!(true)),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);
        assert!(hub.drain_result_notification_query_ids().is_empty());

        let pushed = reg
            .call(
                "push_stream_events",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg(
                        "events",
                        json!([{
                            "eventTime": 1000,
                            "facts": [{
                                "subject": "http://ex.org/s1",
                                "predicate": "http://ex.org/temp",
                                "object": "21",
                                "objectType": "literal"
                            }]
                        }]),
                    ),
            )
            .expect("dispatch");
        assert!(!pushed.is_error, "{:?}", pushed.content);

        let poll = poll_until_nonempty(&reg, "notify-stream");
        assert!(!poll.is_error, "{:?}", poll.content);
        assert_eq!(poll.content["count"], 1);
        assert_eq!(
            drain_notifications_until_nonempty(&hub),
            vec!["notify-stream".to_string()]
        );
    }

    #[test]
    fn notify_false_does_not_queue_result_resource_update() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![Atom::new(
                "alert",
                vec!["alice".to_string()],
            )])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));
        reg.install(push_stream_events_tool(hub.clone()));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("poll-only-rules"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);

        let pushed = reg
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
        assert!(hub.drain_result_notification_query_ids().is_empty());
        assert_eq!(hub.buffered_count("rl-poll-only-rules"), 1);
    }

    #[test]
    fn governed_stream_tools_fail_closed() {
        let (hub, _rt) = fresh_hub();
        let access_policy = active_policy();
        let mut reg = ToolRegistry::new();
        reg.install(create_stream_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(push_stream_events_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(register_stream_query_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(list_stream_queries_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(poll_stream_results_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(watch_invariant_tool_with_solver_and_access_policy(
            hub.clone(),
            Arc::new(StaticSolver {
                answer_sets: Vec::new(),
            }),
            access_policy.clone(),
        ));
        reg.install(register_rules_tool_with_solver_and_access_policy(
            hub,
            Arc::new(StaticSolver {
                answer_sets: Vec::new(),
            }),
            access_policy,
        ));

        for (name, invocation) in [
            (
                "create_stream",
                ToolInvocation::new().with_arg("stream", json!("sensors")),
            ),
            (
                "push_stream_events",
                ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("events", json!([])),
            ),
            (
                "register_stream_query",
                ToolInvocation::new().with_arg("query", json!(sample_query())),
            ),
            ("list_stream_queries", ToolInvocation::new()),
            (
                "poll_stream_results",
                ToolInvocation::new().with_arg("queryId", json!("q1")),
            ),
            ("watch_invariant", ToolInvocation::new()),
            ("register_rules", ToolInvocation::new()),
        ] {
            let res = reg.call(name, &invocation).expect("dispatch");
            assert!(res.is_error, "{name} should fail closed: {:?}", res.content);
            assert!(res.content["message"]
                .as_str()
                .unwrap()
                .contains("denied by active access policy"));
        }
    }

    #[test]
    fn governed_observer_drops_results_after_policy_activation() {
        let (hub, _rt) = fresh_hub();
        let access_policy = AccessPolicyRegistry::shared();
        let solver: Arc<dyn AspSolver> = Arc::new(StaticSolver {
            answer_sets: vec![AnswerSet::new(vec![Atom::new(
                "alert",
                vec!["alice".to_string()],
            )])],
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver_and_access_policy(
            hub.clone(),
            solver,
            access_policy.clone(),
        ));
        reg.install(push_stream_events_tool(hub.clone()));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("governed"))
                    .with_arg("rules", json!("alert(alice) :- rdf(_,_,_)."))
                    .with_arg("resultPredicate", json!("alert")),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);

        activate_policy(access_policy);
        let pushed = reg
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
        assert_eq!(hub.buffered_count("rl-governed"), 0);
    }

    #[test]
    fn governed_cleanup_tools_remain_available_for_teardown() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let access_policy = AccessPolicyRegistry::shared();
        let mut reg = ToolRegistry::new();
        reg.install(register_stream_query_tool_with_access_policy(
            hub.clone(),
            access_policy.clone(),
        ));
        reg.install(forget_stream_query_tool(hub.clone()));
        reg.install(unregister_stream_query_tool(hub.clone()));

        for query_id in ["cleanup-forget", "cleanup-unregister"] {
            let registered = reg
                .call(
                    "register_stream_query",
                    &ToolInvocation::new()
                        .with_arg("query", json!(sample_query()))
                        .with_arg("queryId", json!(query_id)),
                )
                .expect("dispatch");
            assert!(!registered.is_error, "{:?}", registered.content);
        }

        activate_policy(access_policy);
        let forgotten = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!("cleanup-forget")),
            )
            .expect("dispatch");
        assert!(!forgotten.is_error, "{:?}", forgotten.content);

        let unregistered = reg
            .call(
                "unregister_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!("cleanup-unregister")),
            )
            .expect("dispatch");
        assert!(!unregistered.is_error, "{:?}", unregistered.content);
        assert!(hub.registered_query_ids().is_empty());
    }

    #[test]
    fn register_rules_delta_dedup_is_bounded_by_fact_horizon() {
        let (hub, _rt) = fresh_hub();
        let solver: Arc<dyn AspSolver> = Arc::new(CyclingSolver {
            values: Mutex::new(VecDeque::from(["a", "b", "c", "a"])),
        });
        let mut reg = ToolRegistry::new();
        reg.install(register_rules_tool_with_solver(hub.clone(), solver));
        reg.install(push_stream_events_tool(hub.clone()));
        reg.install(poll_stream_results_tool(hub.clone()));

        let registered = reg
            .call(
                "register_rules",
                &ToolInvocation::new()
                    .with_arg("stream", json!("sensors"))
                    .with_arg("queryId", json!("rules-bounded"))
                    .with_arg("rules", json!("alert(X) :- rdf(X,p,o)."))
                    .with_arg("resultPredicate", json!("alert"))
                    .with_arg("maxFacts", json!(2)),
            )
            .expect("dispatch");
        assert!(!registered.is_error, "{:?}", registered.content);

        for subject in ["ex:s1", "ex:s2", "ex:s3", "ex:s4"] {
            let pushed = reg
                .call(
                    "push_stream_events",
                    &ToolInvocation::new()
                        .with_arg("stream", json!("sensors"))
                        .with_arg(
                            "events",
                            json!([{
                                "facts": [{
                                    "subject": subject,
                                    "predicate": "ex:p",
                                    "object": "ex:o",
                                    "objectType": "uri"
                                }]
                            }]),
                        ),
                )
                .expect("dispatch");
            assert!(!pushed.is_error, "{:?}", pushed.content);
        }

        let polled = reg
            .call(
                "poll_stream_results",
                &ToolInvocation::new()
                    .with_arg("queryId", json!("rl-rules-bounded"))
                    .with_arg("limit", json!(10)),
            )
            .expect("dispatch");
        assert_eq!(polled.content["count"], 4);
    }

    #[test]
    fn register_then_list_includes_returned_id() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        assert!(!res.is_error, "register failed: {:?}", res.content);
        let id = res.content["query_id"]
            .as_str()
            .expect("query_id present")
            .to_string();

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        assert!(!list.is_error);
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(ids.contains(&id), "list should contain {id}; got {ids:?}");
    }

    #[test]
    fn register_accepts_java_query_id_and_buffer_options() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("queryId", json!("custom-q"))
                    .with_arg("bufferSize", json!(1))
                    .with_arg("notify", json!(true)),
            )
            .expect("dispatch");
        assert!(!res.is_error, "register failed: {:?}", res.content);
        assert_eq!(res.content["query_id"], "custom-q");
        assert_eq!(res.content["queryId"], "custom-q");
        assert_eq!(res.content["bufferSize"], 1);
        assert_eq!(res.content["notify"], true);
        assert!(res.content["engineQueryId"].is_string());

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        assert_eq!(list.content["query_ids"], json!(["custom-q"]));

        let duplicate = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("queryId", json!("custom-q")),
            )
            .expect("dispatch");
        assert!(duplicate.is_error);
        assert_eq!(
            duplicate.content["message"],
            "Query already registered: custom-q"
        );
    }

    #[test]
    fn pending_registration_guard_removes_reserved_query_id() {
        let (hub, _rt) = fresh_hub();
        hub.inner
            .pending_registrations
            .lock()
            .insert("custom-q".to_string());

        {
            let _guard = PendingRegistrationGuard::new(hub.clone(), "custom-q".to_string());
            assert!(hub.inner.pending_registrations.lock().contains("custom-q"));
        }

        assert!(!hub.inner.pending_registrations.lock().contains("custom-q"));
    }

    #[test]
    fn register_rejects_unsupported_java_options_loudly() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let cypher = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!("MATCH (n) RETURN n"))
                    .with_arg("language", json!("cypher")),
            )
            .expect("dispatch");
        assert!(cypher.is_error);
        assert!(cypher.content["message"]
            .as_str()
            .unwrap()
            .contains("unsupported"));

        let cep = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new()
                    .with_arg("query", json!(sample_query()))
                    .with_arg("cep", json!(true)),
            )
            .expect("dispatch");
        assert!(cep.is_error);
        assert!(cep.content["message"]
            .as_str()
            .unwrap()
            .contains("cep=true"));
    }

    #[test]
    fn unregister_removes_query_from_list_and_clears_buffer() {
        // Pre-register `sensors` so the query's drain task doesn't
        // race to auto-cleanup before the explicit `unregister` call
        // — the test exercises the unregister path, not stream
        // lifecycle.
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let unreg = reg
            .call(
                "unregister_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!unreg.is_error, "unregister failed: {:?}", unreg.content);

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!ids.contains(&id), "id {id} should be gone from {ids:?}");
        // Buffer should also be empty.
        assert_eq!(hub.buffered_count(&id), 0);
    }

    #[test]
    fn forget_stream_query_accepts_java_query_id_and_returns_forgotten_status() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let forget = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!forget.is_error, "forget failed: {:?}", forget.content);
        assert_eq!(forget.content["queryId"], id);
        assert_eq!(forget.content["status"], "forgotten");

        let list = reg
            .call("list_stream_queries", &ToolInvocation::new())
            .expect("dispatch");
        let ids: Vec<String> = list.content["query_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(!ids.contains(&id), "id {id} should be gone from {ids:?}");
        assert_eq!(hub.buffered_count(&id), 0);
    }

    #[test]
    fn forget_stream_query_second_call_reports_unknown_id() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let first = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!first.is_error, "first forget failed: {:?}", first.content);

        let second = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(second.is_error);
        assert_eq!(
            second.content["message"],
            format!("No such registered stream query: {id}")
        );
    }

    #[test]
    fn forget_stream_query_accepts_snake_case_alias() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let forget = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!(id.clone())),
            )
            .expect("dispatch");
        assert!(!forget.is_error, "forget failed: {:?}", forget.content);
        assert_eq!(forget.content["queryId"], id);
        assert_eq!(forget.content["status"], "forgotten");
    }

    #[test]
    fn poll_returns_empty_for_fresh_registration() {
        let (hub, _rt) = fresh_hub_with_sensors_stream();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!(sample_query())),
            )
            .expect("dispatch");
        let id = res.content["query_id"].as_str().unwrap().to_string();

        let poll = reg
            .call(
                "poll_stream_results",
                &ToolInvocation::new().with_arg("query_id", json!(id)),
            )
            .expect("dispatch");
        assert!(!poll.is_error);
        assert_eq!(poll.content["count"], 0);
        assert_eq!(poll.content["remaining"], 0);
        assert_eq!(poll.content["results"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn register_rejects_missing_query_argument() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call("register_stream_query", &ToolInvocation::new())
            .expect("dispatch");
        assert!(res.is_error);
    }

    #[test]
    fn register_propagates_parse_errors() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "register_stream_query",
                &ToolInvocation::new().with_arg("query", json!("this is not cqelsql")),
            )
            .expect("dispatch");
        assert!(res.is_error, "garbage query should fail registration");
    }

    #[test]
    fn unregister_rejects_unknown_id() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "unregister_stream_query",
                &ToolInvocation::new().with_arg("query_id", json!("nonexistent-id")),
            )
            .expect("dispatch");
        assert!(res.is_error);
    }

    #[test]
    fn forget_stream_query_rejects_missing_or_blank_query_id() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for invocation in [
            ToolInvocation::new(),
            ToolInvocation::new().with_arg("queryId", json!("")),
            ToolInvocation::new().with_arg("queryId", json!("   ")),
            ToolInvocation::new().with_arg("query_id", json!("")),
            ToolInvocation::new().with_arg("query_id", json!("   ")),
        ] {
            let res = reg
                .call("forget_stream_query", &invocation)
                .expect("dispatch");
            assert!(res.is_error);
            assert_eq!(
                res.content["message"],
                "Missing required parameter: queryId"
            );
        }
    }

    #[test]
    fn forget_stream_query_rejects_unknown_id_with_java_message() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        let res = reg
            .call(
                "forget_stream_query",
                &ToolInvocation::new().with_arg("queryId", json!("nonexistent-id")),
            )
            .expect("dispatch");
        assert!(res.is_error);
        assert_eq!(
            res.content["message"],
            "No such registered stream query: nonexistent-id"
        );
    }

    #[test]
    fn drain_results_drains_buffer_in_fifo_order() {
        // Test the buffer plumbing directly without depending on engine
        // result delivery timing.
        let (hub, _rt) = fresh_hub();
        let id = "test-query".to_string();
        hub.record_result(id.clone(), BindingSet::new(1));
        hub.record_result(id.clone(), BindingSet::new(2));
        hub.record_result(id.clone(), BindingSet::new(3));
        assert_eq!(hub.buffered_count(&id), 3);
        let drained = hub.drain_results(&id, 2);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].timestamp(), 1);
        assert_eq!(drained[1].timestamp(), 2);
        assert_eq!(hub.buffered_count(&id), 1);
    }

    #[test]
    fn registered_tools_advertise_schemas() {
        let (hub, _rt) = fresh_hub();
        let reg = install_all(&hub);
        for name in [
            "create_stream",
            "push_stream_events",
            "validate_stream_query",
            "register_stream_query",
            "forget_stream_query",
            "list_stream_queries",
            "unregister_stream_query",
            "poll_stream_results",
            "watch_invariant",
            "register_rules",
        ] {
            let tool = reg.get(name).unwrap_or_else(|| panic!("{name} installed"));
            let schema = tool.input_schema();
            assert_eq!(schema.type_field, "object");
        }

        let forget_schema = reg
            .get("forget_stream_query")
            .expect("forget_stream_query installed")
            .input_schema();
        assert!(forget_schema.properties.contains_key("queryId"));
        assert_eq!(forget_schema.required, vec!["queryId".to_string()]);

        let watch_schema = reg
            .get("watch_invariant")
            .expect("watch_invariant installed")
            .input_schema();
        assert_eq!(
            watch_schema.properties["bufferSize"]["maximum"],
            MAX_BUFFER_SIZE
        );
        assert_eq!(watch_schema.properties["bufferSize"]["minimum"], 1);
        assert!(watch_schema.properties["queryId"]["description"]
            .as_str()
            .unwrap()
            .contains("prefixed 'wi-'"));

        let rules_schema = reg
            .get("register_rules")
            .expect("register_rules installed")
            .input_schema();
        assert_eq!(
            rules_schema.properties["bufferSize"]["maximum"],
            MAX_BUFFER_SIZE
        );
        assert_eq!(rules_schema.properties["bufferSize"]["minimum"], 1);
        assert_eq!(
            rules_schema.properties["argNames"]["maxItems"],
            MAX_ARG_NAMES
        );
        assert_eq!(
            rules_schema.properties["maxFacts"]["maximum"],
            MAX_MAX_FACTS
        );
        assert_eq!(rules_schema.properties["maxFacts"]["minimum"], 1);
        assert!(rules_schema.properties["queryId"]["description"]
            .as_str()
            .unwrap()
            .contains("prefixed 'rl-'"));
    }
}
