//! End-to-end JSON-RPC stdio integration test for the full
//! `cqels-mcp` server tool, prompt, and resource surface.
//!
//! Mirrors the registration order of `src/bin/cqels_mcp_server.rs` and
//! drives the registries through `run_stdio_with_prompts_and_resources` with a real
//! line-delimited request batch. Asserts each of the 12 default stdio
//! tools (`parse_query`, `query`, `analyze_query`, `reasoning_profiles`,
//! `shacl_capabilities`, `reason`, `validate`, `solve`, `store_memory`,
//! `recall_memory`, `register_reasoning`, `forget_memory`) returns a
//! non-error response with the expected shape, and verifies
//! Java-compatible CQELS prompt templates and resources are advertised
//! and renderable.
//! Catches regressions in the transport / registration order that
//! per-tool, per-prompt, or per-resource unit tests still pass.
//!
//! Runs as a binary integration test (in `cqels-mcp/tests/`), so it
//! only depends on the crate's public API.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use cqels_asp::{AnswerSet, AspError, AspSolver, Atom};
use cqels_mcp::{
    analyze_query_tool, cqels_prompt_registry, cqels_resource_registry, forget_memory_tool,
    parse_query_tool, query_tool, reason_tool, reasoning_profiles_tool,
    recall_memory_tool_with_reasoning, register_reasoning_tool,
    run_stdio_with_prompts_and_resources, shacl_capabilities_tool, solve_tool_with_solver,
    store_memory_tool, validate_tool_with_solver, InMemoryMemoryStore, MemoryStore,
    ReasoningRegistration, ToolRegistry, RESOURCE_KG_STATS, RESOURCE_QUERY_RESULTS_TEMPLATE,
    RESOURCE_REASONING,
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

fn make_full_registry() -> ToolRegistry {
    let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
    let reasoning = ReasoningRegistration::shared();
    let validate_solver: Arc<dyn AspSolver> = Arc::new(StdioValidateMockSolver);
    let solve_solver: Arc<dyn AspSolver> = Arc::new(StdioSolveMockSolver);
    let mut reg = ToolRegistry::new();
    reg.install(parse_query_tool());
    reg.install(query_tool());
    reg.install(analyze_query_tool());
    reg.install(reasoning_profiles_tool());
    reg.install(shacl_capabilities_tool());
    reg.install(reason_tool());
    reg.install(validate_tool_with_solver(validate_solver));
    reg.install(solve_tool_with_solver(solve_solver));
    reg.install(store_memory_tool(memory.clone()));
    reg.install(register_reasoning_tool(memory.clone(), reasoning.clone()));
    reg.install(recall_memory_tool_with_reasoning(memory.clone(), reasoning));
    reg.install(forget_memory_tool(memory));
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
    lines.push(json!({"jsonrpc":"2.0","id":14,"method":"prompts/list"}).to_string());
    lines.push(
        json!({
            "jsonrpc":"2.0",
            "id":15,
            "method":"prompts/get",
            "params": {
                "name": "recent_events_window",
                "arguments": { "stream": "SensorData", "window": "RANGE 30s" }
            }
        })
        .to_string(),
    );
    lines.push(json!({"jsonrpc":"2.0","id":16,"method":"resources/list"}).to_string());
    lines.push(json!({"jsonrpc":"2.0","id":17,"method":"resources/templates/list"}).to_string());
    lines.push(
        json!({
            "jsonrpc": "2.0",
            "id": 18,
            "method": "resources/read",
            "params": { "uri": RESOURCE_KG_STATS }
        })
        .to_string(),
    );
    lines.push(
        json!({
            "jsonrpc": "2.0",
            "id": 19,
            "method": "resources/read",
            "params": { "uri": RESOURCE_REASONING }
        })
        .to_string(),
    );
    let input = lines.join("\n") + "\n";

    let reg = make_full_registry();
    let prompts = cqels_prompt_registry();
    let resources = cqels_resource_registry();
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
    assert_eq!(responses.len(), 20, "one response per request");

    assert_eq!(responses[0]["id"], 0);
    assert!(responses[0]["result"]["protocolVersion"].is_string());
    assert!(responses[0]["result"]["capabilities"]["prompts"].is_object());
    assert!(responses[0]["result"]["capabilities"]["resources"].is_object());

    let tools = responses[1]["result"]["tools"]
        .as_array()
        .expect("tools array");
    assert_eq!(tools.len(), 12, "12 tools registered");
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "parse_query",
        "query",
        "analyze_query",
        "reasoning_profiles",
        "shacl_capabilities",
        "reason",
        "validate",
        "solve",
        "store_memory",
        "register_reasoning",
        "recall_memory",
        "forget_memory",
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

    let prompt_list = responses[14]["result"]["prompts"]
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

    assert_eq!(responses[15]["id"], 15);
    let message = &responses[15]["result"]["messages"][0];
    assert_eq!(message["role"], "user");
    assert_eq!(message["content"]["type"], "text");
    assert!(message["content"]["text"]
        .as_str()
        .unwrap()
        .contains("FROM STREAM SensorData [RANGE 30s]"));

    let resources_list = responses[16]["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert_eq!(resources_list.len(), 4, "4 static resources registered");
    let resource_uris: Vec<&str> = resources_list
        .iter()
        .map(|resource| resource["uri"].as_str().unwrap())
        .collect();
    assert!(resource_uris.contains(&RESOURCE_KG_STATS));
    assert!(resource_uris.contains(&RESOURCE_REASONING));

    let templates = responses[17]["result"]["resourceTemplates"]
        .as_array()
        .expect("resourceTemplates array");
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0]["uriTemplate"], RESOURCE_QUERY_RESULTS_TEMPLATE);

    let stats_content = &responses[18]["result"]["contents"][0];
    assert_eq!(stats_content["uri"], RESOURCE_KG_STATS);
    assert_eq!(stats_content["mimeType"], "application/json");
    let stats_body: Value = serde_json::from_str(stats_content["text"].as_str().unwrap()).unwrap();
    assert!(stats_body["tripleCount"].is_number());
    assert_eq!(stats_body["registeredQueries"], 0);

    let reasoning_content = &responses[19]["result"]["contents"][0];
    assert_eq!(reasoning_content["uri"], RESOURCE_REASONING);
    let reasoning_body: Value =
        serde_json::from_str(reasoning_content["text"].as_str().unwrap()).unwrap();
    assert!(reasoning_body["profiles"].as_array().unwrap().len() >= 5);
}
