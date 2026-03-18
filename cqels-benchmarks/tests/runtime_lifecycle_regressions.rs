mod common;

use common::*;

#[tokio::test]
async fn test_engine_register_and_process() {
    let engine = ReactiveStreamEngine::new();

    let events: Vec<StreamElement> = generate_sensor_readings(10)
        .into_iter()
        .map(StreamElement::Rdf)
        .collect();

    let stream = Box::pin(futures::stream::iter(events));
    engine.register_stream("sensors", stream).await.unwrap();

    assert!(!engine.is_running());
    engine.start().await.expect("start failed");
    assert!(engine.is_running());
    engine.stop().await.expect("stop failed");
    assert!(!engine.is_running());
}

#[tokio::test]
async fn test_multi_stream_merge() {
    let engine = ReactiveStreamEngine::new();
    let (tx1, stream1) = create_stream_pair(32);
    let (tx2, stream2) = create_stream_pair(32);

    engine.register_stream("stream_a", stream1).await.unwrap();
    engine.register_stream("stream_b", stream2).await.unwrap();
    engine.start().await.unwrap();

    let rx_a = engine.get_stream_receiver("stream_a").await.unwrap();
    let rx_b = engine.get_stream_receiver("stream_b").await.unwrap();
    let mut sa = receiver_to_stream(rx_a);
    let mut sb = receiver_to_stream(rx_b);

    let elem_a = StreamElement::Rdf(make_rdf_element(
        "http://ex.org/A",
        "http://ex.org/p",
        "http://ex.org/v1",
        1000,
    ));
    let elem_b = StreamElement::Rdf(make_rdf_element(
        "http://ex.org/B",
        "http://ex.org/p",
        "http://ex.org/v2",
        2000,
    ));

    tx1.send(elem_a).await.unwrap();
    tx2.send(elem_b).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let ra = tokio::time::timeout(Duration::from_millis(200), sa.next()).await;
    let rb = tokio::time::timeout(Duration::from_millis(200), sb.next()).await;

    assert!(ra.is_ok() && ra.unwrap().is_some());
    assert!(rb.is_ok() && rb.unwrap().is_some());

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_engine_register_binding_query() {
    let engine = ReactiveStreamEngine::new();
    let (tx, stream) = create_stream_pair(32);
    engine.register_stream("sensors", stream).await.unwrap();
    engine.start().await.unwrap();

    let query_str = r#"
        SELECT ?sensor ?temp
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
        }
    "#;
    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let mut result_stream = engine
        .register_binding_query(Box::new(compiled))
        .await
        .unwrap();

    let elem = stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    );
    tx.send(elem).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first = tokio::time::timeout(Duration::from_secs(2), result_stream.next())
        .await
        .expect("timed out");
    assert!(first.is_some());
    let bs = first.unwrap();
    assert!(bs.contains("sensor"));
    assert!(bs.contains("temp"));

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_runtime_cqelsql_query() {
    let runtime = runtime();
    let (tx, stream) = create_stream_pair(32);

    runtime.register_stream("sensors", stream).await.unwrap();
    runtime.start().await.unwrap();

    let query_str = r#"
        SELECT ?sensor ?temp
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
        }
    "#;

    let reg = runtime.register_cqelsql_query(query_str).await.unwrap();
    let mut result_stream = reg.stream;

    let elem = stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    );
    tx.send(elem).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first = tokio::time::timeout(Duration::from_secs(2), result_stream.next())
        .await
        .expect("timed out");
    assert!(first.is_some());

    runtime.stop().await.unwrap();
}

#[tokio::test]
async fn test_runtime_with_static_store() {
    let runtime = runtime();

    runtime
        .load_statements(&[Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s1")),
            IriTerm::new("http://example.org/type"),
            Term::Iri(IriTerm::new("http://example.org/TemperatureSensor")),
        )])
        .unwrap();

    let (tx, stream) = create_stream_pair(32);
    runtime.register_stream("sensors", stream).await.unwrap();
    runtime.start().await.unwrap();

    let query_str = r#"
        SELECT ?sensor ?temp ?type
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
            STATIC {
                ?sensor <http://example.org/type> ?type .
            }
        }
    "#;

    let reg = runtime.register_cqelsql_query(query_str).await.unwrap();
    let mut result_stream = reg.stream;

    let elem = stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    );
    tx.send(elem).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first = tokio::time::timeout(Duration::from_secs(2), result_stream.next())
        .await
        .expect("timed out");
    assert!(first.is_some());
    let bs = first.unwrap();
    assert!(bs.contains("type"));

    runtime.stop().await.unwrap();
}

#[tokio::test]
async fn test_runtime_cypherql_with_store() {
    let runtime = runtime();
    let (tx, stream) = create_stream_pair(32);
    runtime.register_stream("events", stream).await.unwrap();
    runtime.start().await.unwrap();

    let query_str = r#"
        FROM STREAM events [NOW]
        MATCH (n)-[r:temp]->(m)
        RETURN n, m
    "#;

    let result = runtime.register_cypherql_query(query_str).await;
    assert!(result.is_ok(), "{:?}", result.err());

    drop(tx);
    runtime.stop().await.unwrap();
}

#[tokio::test]
async fn test_runtime_cep_pattern() {
    let runtime = runtime();
    let (tx, stream) = create_stream_pair(32);
    runtime.register_stream("events", stream).await.unwrap();
    runtime.start().await.unwrap();

    let pattern = Pattern::<StreamElement>::begin("first")
        .where_cond(|_| true)
        .followed_by("second")
        .where_cond(|_| true);

    let mut match_stream = runtime
        .register_stream_cep_pattern("events", pattern)
        .await
        .unwrap();

    let elem1 = stream_elem_literal("http://ex.org/s1", "http://ex.org/p", "v1", 1000);
    let elem2 = stream_elem_literal("http://ex.org/s2", "http://ex.org/p", "v2", 2000);
    tx.send(elem1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send(elem2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first_match = tokio::time::timeout(Duration::from_secs(2), match_stream.next())
        .await
        .expect("timed out waiting for CEP match");

    assert!(first_match.is_some());
    assert_eq!(first_match.unwrap().events().len(), 2);

    runtime.stop().await.unwrap();
}

#[test]
fn issue_10_drop_outside_runtime_does_not_panic() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (engine, stream) = rt.block_on(async {
        let engine = ReactiveStreamEngine::new();
        engine.start().await.unwrap();
        let query_str = r#"
            SELECT ?sensor ?temp
            FROM STREAM sensors [NOW]
            WHERE {
                STREAM sensors {
                    ?sensor <http://example.org/temp> ?temp .
                }
            }
        "#;
        let definition = CqelsQlParser::parse(query_str).unwrap();
        let compiled = CqelsQueryCompiler::compile(query_str, definition).unwrap();
        let stream = engine
            .register_binding_query(Box::new(compiled))
            .await
            .unwrap();
        (engine, stream)
    });

    drop(rt);
    drop(stream);

    let rt2 = tokio::runtime::Runtime::new().unwrap();
    let ids = rt2.block_on(async { engine.registered_query_ids().await });
    assert!(ids.is_empty(), "outside-runtime drop should clean registry");
}

#[tokio::test]
async fn issue_11_duplicate_stream_name_rejected() {
    let engine = ReactiveStreamEngine::new();
    let (_tx1, stream1) = create_stream_pair(32);
    let (_tx2, stream2) = create_stream_pair(32);

    engine.register_stream("dup", stream1).await.unwrap();
    let result = engine.register_stream("dup", stream2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn issue_12_same_query_text_can_register_twice() {
    let runtime = runtime();
    runtime.start().await.unwrap();

    let query = r#"
        SELECT ?x
        FROM STREAM s [NOW]
        WHERE { ?x <http://example.org/p> ?y . }
    "#;

    let first = runtime.register_cqelsql_query(query).await;
    let second = runtime.register_cqelsql_query(query).await;

    assert!(first.is_ok());
    assert!(second.is_ok());

    runtime.stop().await.unwrap();
}
