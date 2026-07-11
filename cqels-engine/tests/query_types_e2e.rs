//! End-to-end tests for ASK, CONSTRUCT, DESCRIBE queries and facade lifecycle.

use std::sync::{Arc, Mutex};

use cqels_engine::facade::CqelsEngine;
use cqels_engine::listener::QueryResultListener;
use cqels_model::{BindingSet, Statement};

// ─── Collector listeners ────────────────────────────────────────────────────

struct BoolCollector {
    results: Arc<Mutex<Vec<bool>>>,
}

impl BoolCollector {
    fn new() -> (Self, Arc<Mutex<Vec<bool>>>) {
        let results = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                results: results.clone(),
            },
            results,
        )
    }
}

impl QueryResultListener<bool> for BoolCollector {
    fn on_result(&self, result: bool) {
        self.results.lock().unwrap().push(result);
    }
}

struct StatementCollector {
    results: Arc<Mutex<Vec<Statement>>>,
}

impl StatementCollector {
    fn new() -> (Self, Arc<Mutex<Vec<Statement>>>) {
        let results = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                results: results.clone(),
            },
            results,
        )
    }
}

impl QueryResultListener<Statement> for StatementCollector {
    fn on_result(&self, result: Statement) {
        self.results.lock().unwrap().push(result);
    }
}

struct BindingCollector {
    results: Arc<Mutex<Vec<BindingSet>>>,
}

impl BindingCollector {
    fn new() -> (Self, Arc<Mutex<Vec<BindingSet>>>) {
        let results = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                results: results.clone(),
            },
            results,
        )
    }
}

impl QueryResultListener<BindingSet> for BindingCollector {
    fn on_result(&self, result: BindingSet) {
        self.results.lock().unwrap().push(result);
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ask_query_via_facade() {
    let engine = CqelsEngine::builder().id("ask-e2e").build().unwrap();

    let stream = engine.create_stream("sensors").await.unwrap();

    let (listener, results) = BoolCollector::new();
    let query = "ASK FROM STREAM sensors [NOW] WHERE { ?x <http://ex.org/temp> ?y . }";
    let query_id = engine.register_ask_query(query, listener).await.unwrap();
    assert!(!query_id.is_empty());

    engine.start().await.unwrap();

    // Push matching data
    stream
        .push_f64("http://sensor/1", "http://ex.org/temp", 25.0)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    {
        let collected = results.lock().unwrap();
        assert!(
            !collected.is_empty(),
            "ASK should produce at least one true"
        );
        assert!(collected[0], "ASK result should be true for matching data");
    }

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_construct_query_via_facade() {
    let engine = CqelsEngine::builder().id("construct-e2e").build().unwrap();

    let stream = engine.create_stream("sensors").await.unwrap();

    let (listener, results) = StatementCollector::new();
    let query = r#"
        CONSTRUCT { ?s <http://ex.org/hasReading> ?v . }
        FROM STREAM sensors [NOW]
        WHERE { ?s <http://ex.org/temp> ?v . }
    "#;
    let query_id = engine
        .register_construct_query(query, listener)
        .await
        .unwrap();
    assert!(!query_id.is_empty());

    engine.start().await.unwrap();

    stream
        .push_f64("http://sensor/1", "http://ex.org/temp", 42.0)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    {
        let collected = results.lock().unwrap();
        assert!(
            !collected.is_empty(),
            "CONSTRUCT should produce at least one Statement"
        );
    }

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_describe_query_via_facade() {
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::Term;

    let engine = CqelsEngine::builder().id("describe-e2e").build().unwrap();

    // Load static data that DESCRIBE will look up
    let stmts = vec![
        Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/alice")),
            IriTerm::new("http://ex.org/name"),
            Term::Literal(LiteralTerm::new("Alice")),
        ),
        Statement::new(
            Term::Iri(IriTerm::new("http://ex.org/alice")),
            IriTerm::new("http://ex.org/age"),
            Term::Literal(LiteralTerm::new("30")),
        ),
    ];
    engine.load_statements(&stmts).unwrap();

    let stream = engine.create_stream("people").await.unwrap();

    let (listener, results) = StatementCollector::new();
    let query = r#"
        DESCRIBE ?x FROM STREAM people [NOW]
        WHERE { ?x <http://ex.org/knows> ?y . }
    "#;
    let _query_id = engine
        .register_describe_query(query, listener)
        .await
        .unwrap();

    engine.start().await.unwrap();

    // Push a stream event that matches the WHERE pattern
    stream
        .push_triple(
            "http://ex.org/alice",
            "http://ex.org/knows",
            "http://ex.org/bob",
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    {
        let collected = results.lock().unwrap();
        // DESCRIBE should produce statements about alice from the store
        // (at least the two we loaded: name and age)
        assert!(
            !collected.is_empty(),
            "DESCRIBE should produce statements about the described resource"
        );
    }

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_query_lifecycle_register_unregister() {
    let engine = CqelsEngine::builder().id("lifecycle-e2e").build().unwrap();

    let _stream = engine.create_stream("s").await.unwrap();

    let (listener, _results) = BindingCollector::new();
    let query = "SELECT ?x ?y FROM STREAM s [NOW] WHERE { STREAM s { ?x <http://ex.org/p> ?y . } }";
    let qid = engine
        .register_cqelsql_query(query, listener)
        .await
        .unwrap();

    // Verify registered
    let ids = engine.registered_query_ids().await;
    assert!(
        ids.contains(&qid),
        "query should be in registered_query_ids"
    );

    // Unregister
    engine.unregister_query(&qid).await.unwrap();

    let ids = engine.registered_query_ids().await;
    assert!(
        !ids.contains(&qid),
        "query should be removed after unregister"
    );
}

#[tokio::test]
async fn test_facade_load_statements() {
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::Term;

    let engine = CqelsEngine::builder().id("load-stmts-e2e").build().unwrap();

    let stmts = vec![Statement::new(
        Term::Iri(IriTerm::new("http://ex.org/s")),
        IriTerm::new("http://ex.org/p"),
        Term::Literal(LiteralTerm::new("value")),
    )];
    engine.load_statements(&stmts).unwrap();

    engine.load_named_graph("http://ex.org/g1", &stmts).unwrap();
}
