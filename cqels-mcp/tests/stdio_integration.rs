//! End-to-end JSON-RPC stdio integration test for the full
//! `cqels-mcp` server tool, prompt, and resource surface.
//!
//! Mirrors the registration order of `src/bin/cqels_mcp_server.rs` and
//! drives the registries through `run_stdio_with_prompts_and_resources` with a real
//! line-delimited request batch. Asserts the default 26-tool stdio surface
//! is advertised, exercises representative stateless, memory, stream,
//! procedure, episodic, decision, governance, and working-memory calls,
//! and verifies Java-compatible CQELS prompt templates and resources are
//! advertised and renderable.
//! Catches regressions in the transport / registration order that
//! per-tool, per-prompt, or per-resource unit tests still pass.
//!
//! Runs as a binary integration test (in `cqels-mcp/tests/`), so it
//! only depends on the crate's public API.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use cqels_asp::{AnswerSet, AspError, AspSolver, Atom};
use cqels_engine::CqelsEngine;
use cqels_mcp::{
    analyze_query_tool, assemble_context_tool_with_access_policy, cqels_prompt_registry,
    cqels_resource_registry_with_streams, explain_decision_tool, forget_memory_tool,
    forget_stream_query_tool, list_procedures_tool, list_stream_queries_tool, parse_query_tool,
    poll_stream_results_tool, query_tool, reason_tool, reasoning_profiles_tool,
    recall_decisions_tool, recall_episodes_tool,
    recall_memory_tool_with_reasoning_and_access_policy, record_event_tool,
    register_reasoning_tool, register_stream_query_tool, run_procedure_tool,
    run_stdio_with_prompts_and_resources, save_procedure_tool, set_access_policy_tool,
    shacl_capabilities_tool, solve_tool_with_solver, store_memory_tool,
    unregister_stream_query_tool, validate_tool_with_solver, AccessPolicyRegistry,
    InMemoryMemoryStore, MemoryStore, ReasoningRegistration, StreamQueryHub, ToolRegistry,
    RESOURCE_DOC_CEP, RESOURCE_DOC_CQELSQL, RESOURCE_ENGINE_STATUS, RESOURCE_KG_NAMESPACES,
    RESOURCE_KG_STATS, RESOURCE_QUERY_RESULTS_TEMPLATE, RESOURCE_REASONING,
};
use serde_json::{json, Value};

struct StdioSolveMockSolver;

#[async_trait]
impl AspSolver for StdioSolveMockSolver {
    async fn solve(&self, _program: &str, _max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        Ok(vec![AnswerSet::new(vec![Atom::new("demo", vec![])])])
    }
}

struct StdioValidateMockSolver;

#[async_trait]
impl AspSolver for StdioValidateMockSolver {
    async fn solve(&self, _program: &str, _max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        Ok(vec![AnswerSet::new(Vec::new())])
    }
}

fn make_full_registry(stream_hub: StreamQueryHub) -> ToolRegistry {
    let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
    let reasoning = ReasoningRegistration::shared();
    let access_policy = AccessPolicyRegistry::shared();
    let validate_solver: Arc<dyn AspSolver> = Arc::new(StdioValidateMockSolver);
    let solve_solver: Arc<dyn AspSolver> = Arc::new(StdioSolveMockSolver);
    let mut reg = ToolRegistry::new();
    reg.install(parse_query_tool());
    reg.install(query_tool());
    reg.install(analyze_query_tool());
    reg.install(register_stream_query_tool(stream_hub.clone()));
    reg.install(forget_stream_query_tool(stream_hub.clone()));
    reg.install(list_stream_queries_tool(stream_hub.clone()));
    reg.install(unregister_stream_query_tool(stream_hub.clone()));
    reg.install(poll_stream_results_tool(stream_hub));
    reg.install(reasoning_profiles_tool());
    reg.install(shacl_capabilities_tool());
    reg.install(reason_tool());
    reg.install(validate_tool_with_solver(validate_solver));
    reg.install(solve_tool_with_solver(solve_solver));
    reg.install(store_memory_tool(memory.clone()));
    reg.install(register_reasoning_tool(memory.clone(), reasoning.clone()));
    reg.install(recall_memory_tool_with_reasoning_and_access_policy(
        memory.clone(),
        reasoning,
        access_policy.clone(),
    ));
    reg.install(forget_memory_tool(memory.clone()));
    reg.install(save_procedure_tool(memory.clone()));
    reg.install(list_procedures_tool(memory.clone()));
    reg.install(run_procedure_tool(memory.clone()));
    reg.install(record_event_tool(memory.clone()));
    reg.install(recall_episodes_tool(memory.clone()));
    reg.install(explain_decision_tool(memory.clone()));
    reg.install(recall_decisions_tool(memory.clone()));
    reg.install(set_access_policy_tool(
        memory.clone(),
        access_policy.clone(),
    ));
    reg.install(assemble_context_tool_with_access_policy(
        memory,
        access_policy,
    ));
    reg
}

/// Wraps a `tools/call` request line.
fn call_line(id: i64, name: &str, args: Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": { "name": name, "arguments": args }
    })
    .to_string()
}

#[test]
fn stdio_dispatches_every_tool_in_one_session() {
    let self_join_query = r#"
        SELECT ?driver ?passenger
        FROM STREAM rides [RANGE 10s]
        WHERE {
            STREAM rides { ?driver <http://ex.org/in> ?car . }
            STREAM rides { ?passenger <http://ex.org/in> ?car . }
        }
    "#;
    let parse_args = json!({ "query": self_join_query });
    let reason_args = json!({
        "profile": "RDFS",
        "triples": [
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
        ]
    });
    let solve_args = json!({ "program": "demo.", "max_models": 1 });
    let validate_args = json!({ "shapes": [], "data": [] });
    let store_args = json!({
        "id": "alice-likes-stream",
        "content": "Alice prefers IoT sensor streams",
    });
    let register_reasoning_args = json!({});
    let recall_args = json!({ "query": "IoT sensor" });
    let forget_args = json!({ "id": "alice-likes-stream" });
    let save_proc_args = json!({
        "name": "ancestor",
        "kind": "asp",
        "body": "ancestor(tom,bob).",
        "description": "ancestor rule"
    });
    let run_proc_args = json!({
        "name": "ancestor",
        "inputs": ["cqels://memory/event/demo"],
        "policy": "demo-policy"
    });
    let record_event_args = json!({
        "subject": "http://ex.org/alice",
        "predicate": "http://ex.org/observed",
        "object": "42",
        "time": "2026-07-07T00:00:00Z"
    });
    let access_policy_args = json!({
        "role": "analyst",
        "labels": ["*"]
    });

    let mut lines = Vec::new();
    lines.push(json!({"jsonrpc":"2.0","id":0,"method":"initialize"}).to_string());
    lines.push(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string());
    lines.push(call_line(2, "parse_query", parse_args.clone()));
    lines.push(call_line(3, "query", parse_args.clone()));
    lines.push(call_line(4, "analyze_query", parse_args));
    lines.push(call_line(5, "reasoning_profiles", json!({})));
    lines.push(call_line(6, "shacl_capabilities", json!({})));
    lines.push(call_line(7, "reason", reason_args));
    lines.push(call_line(8, "validate", validate_args));
    lines.push(call_line(9, "solve", solve_args));
    lines.push(call_line(10, "store_memory", store_args));
    lines.push(call_line(11, "register_reasoning", register_reasoning_args));
    lines.push(call_line(12, "recall_memory", recall_args));
    lines.push(call_line(13, "forget_memory", forget_args));
    lines.push(call_line(14, "list_stream_queries", json!({})));
    lines.push(call_line(15, "save_procedure", save_proc_args));
    lines.push(call_line(16, "list_procedures", json!({})));
    lines.push(call_line(17, "run_procedure", run_proc_args));
    lines.push(call_line(18, "record_event", record_event_args));
    lines.push(call_line(
        19,
        "recall_episodes",
        json!({"entity": "http://ex.org/alice"}),
    ));
    lines.push(call_line(
        20,
        "recall_decisions",
        json!({"policy": "demo-policy"}),
    ));
    lines.push(call_line(
        21,
        "assemble_context",
        json!({"task": "ancestor"}),
    ));
    lines.push(call_line(22, "set_access_policy", access_policy_args));
    lines.push(json!({"jsonrpc":"2.0","id":23,"method":"prompts/list"}).to_string());
    lines.push(
        json!({
            "jsonrpc":"2.0",
            "id":24,
            "method":"prompts/get",
            "params": {
                "name": "recent_events_window",
                "arguments": { "stream": "SensorData", "window": "RANGE 30s" }
            }
        })
        .to_string(),
    );
    lines.push(json!({"jsonrpc":"2.0","id":25,"method":"resources/list"}).to_string());
    lines.push(json!({"jsonrpc":"2.0","id":26,"method":"resources/templates/list"}).to_string());
    lines.push(
        json!({
            "jsonrpc": "2.0",
            "id": 27,
            "method": "resources/read",
            "params": { "uri": RESOURCE_KG_STATS }
        })
        .to_string(),
    );
    lines.push(
        json!({
            "jsonrpc": "2.0",
            "id": 28,
            "method": "resources/read",
            "params": { "uri": RESOURCE_REASONING }
        })
        .to_string(),
    );
    let input = lines.join("\n") + "\n";

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let engine = CqelsEngine::builder()
        .id("stdio-integration")
        .build()
        .expect("engine");
    runtime.block_on(engine.start()).expect("engine start");
    let stream_hub = StreamQueryHub::new(Arc::new(engine), runtime.handle().clone());
    let reg = make_full_registry(stream_hub.clone());
    let prompts = cqels_prompt_registry();
    let resources = cqels_resource_registry_with_streams(stream_hub);
    let mut output: Vec<u8> = Vec::new();
    run_stdio_with_prompts_and_resources(
        &reg,
        &prompts,
        &resources,
        Cursor::new(input.as_bytes()),
        &mut output,
    )
    .expect("run_stdio");

    let text = String::from_utf8(output).expect("utf8");
    let responses: Vec<Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse response"))
        .collect();
    assert_eq!(responses.len(), 29, "one response per request");

    assert_eq!(responses[0]["id"], 0);
    assert!(responses[0]["result"]["protocolVersion"].is_string());
    assert!(responses[0]["result"]["capabilities"]["prompts"].is_object());
    assert!(responses[0]["result"]["capabilities"]["resources"].is_object());

    let tools = responses[1]["result"]["tools"]
        .as_array()
        .expect("tools array");
    assert_eq!(tools.len(), 26, "26 tools registered");
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "parse_query",
        "query",
        "analyze_query",
        "register_stream_query",
        "forget_stream_query",
        "list_stream_queries",
        "unregister_stream_query",
        "poll_stream_results",
        "reasoning_profiles",
        "shacl_capabilities",
        "reason",
        "validate",
        "solve",
        "store_memory",
        "register_reasoning",
        "recall_memory",
        "forget_memory",
        "save_procedure",
        "list_procedures",
        "run_procedure",
        "record_event",
        "recall_episodes",
        "explain_decision",
        "recall_decisions",
        "set_access_policy",
        "assemble_context",
    ] {
        assert!(
            names.contains(&expected),
            "expected tool '{expected}' to be advertised; got {names:?}"
        );
    }

    fn assert_tool_ok(resp: &Value, expected_id: i64) -> &Value {
        assert_eq!(resp["id"], expected_id);
        assert!(
            resp["error"].is_null(),
            "tool call should not return JSON-RPC error: {resp:?}"
        );
        let content = &resp["result"]["content"];
        assert!(
            !resp["result"]["isError"].as_bool().unwrap_or(true),
            "tool call returned isError=true: content = {content:?}"
        );
        content
    }

    let parse = assert_tool_ok(&responses[2], 2);
    assert_eq!(parse["query_type"], "Select");
    assert_eq!(parse["streams"][0], "rides");

    let query = assert_tool_ok(&responses[3], 3);
    assert_eq!(query["ok"], true);
    assert_eq!(query["dry_run"], true);

    let analyze = assert_tool_ok(&responses[4], 4);
    assert_eq!(analyze["has_self_join_optimization"], true);
    let hints = analyze["self_join_hints"].as_array().expect("hint array");
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0]["source"], "rides");

    let profiles = assert_tool_ok(&responses[5], 5);
    assert_eq!(
        profiles["profiles"]
            .as_array()
            .expect("profiles array")
            .len(),
        7
    );

    let shacl = assert_tool_ok(&responses[6], 6);
    assert!(shacl["supported_constraints"].as_array().unwrap().len() >= 5);

    let reasoned = assert_tool_ok(&responses[7], 7);
    assert_eq!(reasoned["profile"], "RDFS");
    let inferred = reasoned["inferred"].as_array().expect("inferred array");
    assert!(
        inferred.iter().any(|t| {
            t["s"].as_str() == Some("http://ex.org/alice")
                && t["o"].as_str() == Some("http://ex.org/Animal")
        }),
        "expected :alice rdf:type :Animal in inferred set; got {inferred:?}"
    );

    let validated = assert_tool_ok(&responses[8], 8);
    assert_eq!(validated["conforms"], true);
    assert_eq!(validated["violation_count"], 0);

    let solved = assert_tool_ok(&responses[9], 9);
    assert_eq!(solved["model_count"], 1);
    let solve_atoms = solved["answer_sets"][0]["atoms"].as_array().unwrap();
    assert_eq!(solve_atoms[0]["predicate"], "demo");

    let stored = assert_tool_ok(&responses[10], 10);
    assert_eq!(stored["ok"], true);
    assert_eq!(stored["id"], "alice-likes-stream");

    let registered = assert_tool_ok(&responses[11], 11);
    assert_eq!(registered["registered"], true);
    assert_eq!(registered["profile"], "RDFS-Full");

    let recalled = assert_tool_ok(&responses[12], 12);
    assert_eq!(recalled["count"], 1, "exactly one fact matches the query");
    let facts = recalled["facts"].as_array().expect("facts array");
    assert_eq!(facts[0]["content"], "Alice prefers IoT sensor streams");
    assert_eq!(facts[0]["id"], "alice-likes-stream");

    let forgotten = assert_tool_ok(&responses[13], 13);
    assert_eq!(forgotten["removed"], true);

    let stream_queries = assert_tool_ok(&responses[14], 14);
    assert_eq!(stream_queries["count"], 0);

    let saved_proc = assert_tool_ok(&responses[15], 15);
    assert_eq!(saved_proc["name"], "ancestor");
    assert_eq!(saved_proc["kind"], "asp");

    let listed_proc = assert_tool_ok(&responses[16], 16);
    assert_eq!(listed_proc["procedures"].as_array().unwrap().len(), 1);

    let ran_proc = assert_tool_ok(&responses[17], 17);
    assert!(ran_proc["decision"]
        .as_str()
        .unwrap()
        .starts_with("cqels://memory/decision/"));
    assert_eq!(ran_proc["result"]["program"], "ancestor(tom,bob).");

    let recorded_event = assert_tool_ok(&responses[18], 18);
    assert_eq!(recorded_event["recorded"], true);

    let episodes = assert_tool_ok(&responses[19], 19);
    assert_eq!(episodes["events"].as_array().unwrap().len(), 1);

    let decisions = assert_tool_ok(&responses[20], 20);
    assert_eq!(decisions["decisions"].as_array().unwrap().len(), 1);

    let context = assert_tool_ok(&responses[21], 21);
    assert_eq!(context["task"], "ancestor");
    assert_eq!(context["procedures"].as_array().unwrap().len(), 1);

    let policy = assert_tool_ok(&responses[22], 22);
    assert_eq!(policy["active"], true);
    assert_eq!(policy["role"], "analyst");

    let prompt_list = responses[23]["result"]["prompts"]
        .as_array()
        .expect("prompts array");
    assert_eq!(prompt_list.len(), 8, "8 prompts registered");
    let prompt_names: Vec<&str> = prompt_list
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    for expected in [
        "store_knowledge",
        "recall_about",
        "validate_data",
        "reasoning_workflow",
        "recent_events_window",
        "entity_by_type",
        "value_over_window",
        "spatial_recall",
    ] {
        assert!(prompt_names.contains(&expected));
    }

    assert_eq!(responses[24]["id"], 24);
    let message = &responses[24]["result"]["messages"][0];
    assert_eq!(message["role"], "user");
    assert_eq!(message["content"]["type"], "text");
    assert!(message["content"]["text"]
        .as_str()
        .unwrap()
        .contains("FROM STREAM SensorData [RANGE 30s]"));

    let resources_list = responses[25]["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert_eq!(resources_list.len(), 8, "8 static resources registered");
    let resource_uris: Vec<&str> = resources_list
        .iter()
        .map(|resource| resource["uri"].as_str().unwrap())
        .collect();
    assert!(resource_uris.contains(&RESOURCE_KG_STATS));
    assert!(resource_uris.contains(&RESOURCE_KG_NAMESPACES));
    assert!(resource_uris.contains(&RESOURCE_ENGINE_STATUS));
    assert!(resource_uris.contains(&RESOURCE_REASONING));
    assert!(resource_uris.contains(&RESOURCE_DOC_CQELSQL));
    assert!(resource_uris.contains(&RESOURCE_DOC_CEP));

    let templates = responses[26]["result"]["resourceTemplates"]
        .as_array()
        .expect("resourceTemplates array");
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0]["uriTemplate"], RESOURCE_QUERY_RESULTS_TEMPLATE);

    let stats_content = &responses[27]["result"]["contents"][0];
    assert_eq!(stats_content["uri"], RESOURCE_KG_STATS);
    assert_eq!(stats_content["mimeType"], "application/json");
    let stats_body: Value = serde_json::from_str(stats_content["text"].as_str().unwrap()).unwrap();
    assert!(stats_body["tripleCount"].is_number());
    assert_eq!(stats_body["registeredQueries"], 0);

    let reasoning_content = &responses[28]["result"]["contents"][0];
    assert_eq!(reasoning_content["uri"], RESOURCE_REASONING);
    let reasoning_body: Value =
        serde_json::from_str(reasoning_content["text"].as_str().unwrap()).unwrap();
    assert!(reasoning_body["profiles"].as_array().unwrap().len() >= 5);
}
