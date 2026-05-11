//! End-to-end test for the self-join fast path in
//! `CompiledCqelsQuery::execute`. Confirms that a query of the shape:
//!
//! ```text
//! SELECT ?driver ?passenger
//! FROM STREAM rides [RANGE 10s]
//! WHERE {
//!     STREAM rides { ?driver <ex:in> ?car . }
//!     STREAM rides { ?passenger <ex:in> ?car . }
//! }
//! ```
//!
//! produces the expected pair-bindings when fed events whose objects
//! share a key.

use std::pin::Pin;

use cqels_core::compiler::CqelsQueryCompiler;
use cqels_core::parser::CqelsQlParser;
use cqels_core::query::{ContinuousQuery, QueryInputs};
use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_model::{IriTerm, Statement, Term};
use futures::stream::{Stream, StreamExt};

fn ride_event(person: &str, car: &str, ts: i64) -> StreamElement {
    StreamElement::Rdf(RdfStreamElement::new(
        Statement::new(
            Term::Iri(IriTerm::new(person)),
            IriTerm::new("http://ex.org/in"),
            Term::Iri(IriTerm::new(car)),
        ),
        ts,
    ))
}

#[tokio::test]
async fn self_join_query_emits_expected_pairs() {
    let query = r#"
        SELECT ?driver ?passenger
        FROM STREAM rides [RANGE 10s]
        WHERE {
            STREAM rides { ?driver <http://ex.org/in> ?car . }
            STREAM rides { ?passenger <http://ex.org/in> ?car . }
        }
    "#;
    let def = CqelsQlParser::parse(query).expect("parse");
    let compiled = CqelsQueryCompiler::compile(query, def).expect("compile");
    assert!(
        compiled.has_self_join_optimization(),
        "this query must trigger the self-join hint"
    );

    // Feed three events: alice and bob share car v1; carol is on v2.
    // The fast path should emit one pair: (alice, bob) on v1.
    let events = vec![
        ride_event("http://ex.org/alice", "http://ex.org/v1", 1_000),
        ride_event("http://ex.org/bob", "http://ex.org/v1", 2_000),
        ride_event("http://ex.org/carol", "http://ex.org/v2", 3_000),
    ];
    let event_stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>> =
        Box::pin(futures::stream::iter(events));

    let mut inputs = QueryInputs::new();
    inputs.add_stream("rides", event_stream);

    let mut out_stream = compiled.execute(inputs);
    let mut bindings = Vec::new();
    while let Some(b) = out_stream.next().await {
        bindings.push(b);
    }

    assert_eq!(bindings.len(), 1, "expected exactly one self-join pair");
    let bs = &bindings[0];
    // SELECT projects ?driver and ?passenger; the fast path stores both
    // as String values containing the IRI string.
    let driver = bs
        .get("driver")
        .and_then(|v| v.as_string())
        .expect("driver bound");
    let passenger = bs
        .get("passenger")
        .and_then(|v| v.as_string())
        .expect("passenger bound");
    assert_eq!(driver, "http://ex.org/alice");
    assert_eq!(passenger, "http://ex.org/bob");
}

#[tokio::test]
async fn self_join_outside_window_does_not_pair() {
    // The two matching events are 11s apart, outside the 10s RANGE.
    let query = r#"
        SELECT ?a ?b
        FROM STREAM rides [RANGE 10s]
        WHERE {
            STREAM rides { ?a <http://ex.org/in> ?car . }
            STREAM rides { ?b <http://ex.org/in> ?car . }
        }
    "#;
    let def = CqelsQlParser::parse(query).expect("parse");
    let compiled = CqelsQueryCompiler::compile(query, def).expect("compile");

    let events = vec![
        ride_event("http://ex.org/alice", "http://ex.org/v1", 0),
        ride_event("http://ex.org/bob", "http://ex.org/v1", 11_001),
    ];
    let event_stream: Pin<Box<dyn Stream<Item = StreamElement> + Send>> =
        Box::pin(futures::stream::iter(events));

    let mut inputs = QueryInputs::new();
    inputs.add_stream("rides", event_stream);

    let bindings: Vec<_> = compiled.execute(inputs).collect().await;
    assert!(
        bindings.is_empty(),
        "events 11s apart must not pair under a RANGE 10s window"
    );
}

#[tokio::test]
async fn self_join_disabled_for_queries_with_aggregates() {
    // The hint is detected, but the fast path's strict shape check
    // rejects queries with aggregates / group-by — so this query must
    // fall through to the default pipeline (which currently also runs
    // it correctly, just slower). Here we only assert that
    // `has_self_join_optimization()` is true but the actual execution
    // does not crash — the fast path is silently skipped.
    let query = r#"
        SELECT (COUNT(?driver) AS ?n)
        FROM STREAM rides [RANGE 10s]
        WHERE {
            STREAM rides { ?driver <http://ex.org/in> ?car . }
            STREAM rides { ?passenger <http://ex.org/in> ?car . }
        }
    "#;
    let def = CqelsQlParser::parse(query).expect("parse");
    let compiled = CqelsQueryCompiler::compile(query, def).expect("compile");
    assert!(
        compiled.has_self_join_optimization(),
        "hint is detected at compile time even when fast path won't be used"
    );
    // Execution should not panic.
    let mut inputs = QueryInputs::new();
    inputs.add_stream(
        "rides",
        Box::pin(futures::stream::iter(Vec::<StreamElement>::new())),
    );
    let _ = compiled.execute(inputs).collect::<Vec<_>>().await;
}
