//! #83 guard: the parity benchmark must load static-graph fixtures'
//! `static.trig` into the engine before running their queries. The benchmark's
//! own loader lives in a `harness = false` bench, whose `#[cfg(test)]` tests do
//! not run under `cargo test`, so this integration test independently verifies
//! that the representative static fixture parses and loads into the engine
//! without error. A regression in the static-loading path (wrong format, an
//! engine API change, or a malformed fixture) is caught here.
#![allow(deprecated)] // oxigraph 0.4 deprecates DatasetParser; mirrors the bench + runner.

use std::collections::HashMap;
use std::path::Path;

use cqels_engine::CqelsEngine;
use cqels_model::Statement;
use oxigraph::io::{DatasetFormat, DatasetParser};
use oxigraph::model::GraphName;

#[tokio::test]
async fn static_fixture_loads_into_engine() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("parity-tests/fixtures/stream-static-lookup-join");
    let trig = std::fs::read_to_string(dir.join("static.trig")).expect("read static.trig");
    assert!(!trig.trim().is_empty(), "static.trig must be non-empty");

    let parser = DatasetParser::from_format(DatasetFormat::TriG);
    let mut by_graph: HashMap<Option<String>, Vec<Statement>> = HashMap::new();
    for quad in parser.read_quads(trig.as_bytes()) {
        let quad = quad.expect("parse static.trig quad");
        let key = match &quad.graph_name {
            GraphName::DefaultGraph => None,
            other => Some(other.to_string()),
        };
        by_graph.entry(key).or_default().push(quad.into());
    }

    let total: usize = by_graph.values().map(Vec::len).sum();
    assert!(total > 0, "static.trig should parse to >0 statements");
    assert!(
        by_graph.keys().any(Option::is_some),
        "static.trig should contain at least one named graph"
    );

    let engine = CqelsEngine::builder().build().expect("build engine");
    for (graph, stmts) in by_graph {
        match graph {
            None => engine.load_statements(&stmts).expect("load_statements"),
            Some(iri) => {
                let plain = iri
                    .strip_prefix('<')
                    .and_then(|s| s.strip_suffix('>'))
                    .unwrap_or(&iri)
                    .to_string();
                engine
                    .load_named_graph(&plain, &stmts)
                    .expect("load_named_graph");
            }
        }
    }
}
