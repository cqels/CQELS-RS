//! End-to-end JSON-RPC stdio integration test for the full
//! `cqels-mcp` server tool surface.
//!
//! Mirrors the registration order of `src/bin/cqels_mcp_server.rs` and
//! drives the registry through `run_stdio` with a real
//! line-delimited request batch. Asserts each of the 12 default stdio
//! tools (`parse_query`, `query`, `analyze_query`, `reasoning_profiles`,
//! `shacl_capabilities`, `reason`, `validate`, `solve`, `store_memory`,
//! `recall_memory`, `register_reasoning`, `forget_memory`) returns a
//! non-error response with the expected shape. Catches regressions in
//! the transport / registration order that per-tool unit tests still
//! pass.
//!
//! Runs as a binary integration test (in `cqels-mcp/tests/`), so it
//! only depends on the crate's public API.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use cqels_asp::{AnswerSet, AspError, AspSolver, Atom};
use cqels_mcp::{
    analyze_query_tool, forget_memory_tool, parse_query_tool, query_tool, reason_tool,
    reasoning_profiles_tool, recall_memory_tool_with_reasoning, register_reasoning_tool, run_stdio,
    shacl_capabilities_tool, solve_tool_with_solver, store_memory_tool, validate_tool_with_solver,
    InMemoryMemoryStore, MemoryStore, ReasoningRegistration, ToolRegistry,
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
    // A sensor self-join query — exercises analyze_query's self-join
    // detection path so we hit both `parse_query`/`query` and the
    // compiler-side hint output.
    let self_join_query = r#"
        SELECT ?driver ?passenger
        FROM STREAM rides [RANGE 10s]
        WHERE {
            STREAM rides { ?driver <http://ex.org/in> ?car . }
            STREAM rides { ?passenger <http://ex.org/in> ?car . }
        }
    "#;
    let parse_args = json!({ "query": self_join_query });

    // RDFS classic subClassOf chain.
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
    let solve_args = json!({
        "program": "demo.",
        "max_models": 1,
    });
    let validate_args = json!({
        "shapes": [],
        "data": [],
    });

    // Memory tools: store → recall → forget. Use a stable id so
    // assertions check the same record across operations. `recall_memory`
    // filters by substring match on content (not by id), so we pass a
    // unique phrase that only this fact contains.
    let store_args = json!({
        "id": "alice-likes-stream",
        "content": "Alice prefers IoT sensor streams",
    });
    let register_reasoning_args = json!({});
    let recall_args = json!({ "query": "IoT sensor" });
    let forget_args = json!({ "id": "alice-likes-stream" });

    let mut lines = Vec::new();
    // Handshake first.
    lines.push(json!({"jsonrpc":"2.0","id":0,"method":"initialize"}).to_string());
    // tools/list to verify the surface.
    lines.push(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string());
    // Per-tool dispatch.
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
    let input = lines.join("\n") + "\n";

    let reg = make_full_registry();
    let mut output: Vec<u8> = Vec::new();
    run_stdio(&reg, Cursor::new(input.as_bytes()), &mut output).expect("run_stdio");

    let text = String::from_utf8(output).expect("utf8");
    let responses: Vec<Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("parse response"))
        .collect();

    // 14 requests in, 14 responses out (every line had an `id`).
    assert_eq!(responses.len(), 14, "one response per request");

    // ─── initialize ──────────────────────────────────────────────
    assert_eq!(responses[0]["id"], 0);
    assert!(
        responses[0]["result"]["protocolVersion"].is_string(),
        "initialize returns a protocolVersion"
    );

    // ─── tools/list ──────────────────────────────────────────────
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

    // Helper to assert a successful tools/call response.
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

    // ─── parse_query ─────────────────────────────────────────────
    let parse = assert_tool_ok(&responses[2], 2);
    assert_eq!(parse["query_type"], "Select");
    assert_eq!(parse["streams"][0], "rides");

    // ─── query (dry-run) ────────────────────────────────────────
    let query = assert_tool_ok(&responses[3], 3);
    assert_eq!(query["ok"], true);
    assert_eq!(query["dry_run"], true);

    // ─── analyze_query — self-join hint must be detected ─────────
    let analyze = assert_tool_ok(&responses[4], 4);
    assert_eq!(
        analyze["has_self_join_optimization"], true,
        "self-join query should trigger the planner hint"
    );
    let hints = analyze["self_join_hints"].as_array().expect("hint array");
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0]["source"], "rides");

    // ─── reasoning_profiles — all 7 listed ───────────────────────
    let profiles = assert_tool_ok(&responses[5], 5);
    let profile_list = profiles["profiles"].as_array().expect("profiles array");
    assert_eq!(profile_list.len(), 7);

    // ─── shacl_capabilities ──────────────────────────────────────
    let shacl = assert_tool_ok(&responses[6], 6);
    assert!(shacl["supported_constraints"].as_array().unwrap().len() >= 5);

    // ─── reason — classic RDFS subClassOf inference ──────────────
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

    // ─── validate — SHACL bridge is installed in stdio ──────────────
    let validated = assert_tool_ok(&responses[8], 8);
    assert_eq!(validated["conforms"], true);
    assert_eq!(validated["violation_count"], 0);

    // ─── solve — direct ASP bridge is installed in stdio ────────────
    let solved = assert_tool_ok(&responses[9], 9);
    assert_eq!(solved["model_count"], 1);
    let solve_atoms = solved["answer_sets"][0]["atoms"].as_array().unwrap();
    assert_eq!(solve_atoms[0]["predicate"], "demo");

    // ─── store_memory + recall_memory + forget_memory ────────────
    let stored = assert_tool_ok(&responses[10], 10);
    assert_eq!(stored["ok"], true);
    assert_eq!(stored["id"], "alice-likes-stream");

    let registered = assert_tool_ok(&responses[11], 11);
    assert_eq!(registered["registered"], true);
    assert_eq!(registered["profile"], "RDFS-Full");

    let recalled = assert_tool_ok(&responses[12], 12);
    assert_eq!(recalled["count"], 1, "exactly one fact matches the query");
    let facts = recalled["facts"].as_array().expect("facts array");
    assert_eq!(
        facts[0]["content"], "Alice prefers IoT sensor streams",
        "recall_memory must return the content stored earlier in the same session"
    );
    assert_eq!(facts[0]["id"], "alice-likes-stream");

    let forgotten = assert_tool_ok(&responses[13], 13);
    assert_eq!(
        forgotten["removed"], true,
        "forget_memory should report the fact was removed"
    );
}
