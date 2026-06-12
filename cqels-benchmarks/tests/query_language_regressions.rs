mod common;

use common::*;

#[test]
fn test_parse_then_filter_then_bind() {
    let query_str = r#"
        FROM STREAM social [RANGE 5m]
        MATCH (p:Person)-[:FOLLOWS]->(q:Person)
        WHERE p.age > 18
        RETURN p.name, p.age, p.age * 2 AS double_age
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams.len(), 1);
    assert!(parsed.where_expression.is_some());
    assert_eq!(parsed.return_expressions.len(), 3);

    let bindings: Vec<BindingSet> = vec![
        {
            let mut bs = BindingSet::new(1000);
            bs.insert("name".to_string(), Value::String("Alice".to_string()));
            bs.insert("age".to_string(), Value::Integer(25));
            bs
        },
        {
            let mut bs = BindingSet::new(2000);
            bs.insert("name".to_string(), Value::String("Bob".to_string()));
            bs.insert("age".to_string(), Value::Integer(16));
            bs
        },
        {
            let mut bs = BindingSet::new(3000);
            bs.insert("name".to_string(), Value::String("Carol".to_string()));
            bs.insert("age".to_string(), Value::Integer(30));
            bs
        },
    ];

    let filter = FilterOperator::new(|bs: &BindingSet| {
        bs.get("age")
            .and_then(|v| v.as_integer())
            .is_some_and(|age| age > 18)
    });
    let filtered: Vec<_> = bindings
        .into_iter()
        .filter(|bs| filter.evaluate(bs))
        .collect();
    assert_eq!(filtered.len(), 2);

    let bind = BindOperator::new("double_age", |bs: &BindingSet| {
        bs.get("age")
            .and_then(|v| v.as_integer())
            .map(|age| Value::Integer(age * 2))
    });

    let mut enriched = filtered;
    for bs in &mut enriched {
        bind.evaluate(bs);
    }

    assert_eq!(
        enriched[0].get("double_age").and_then(|v| v.as_integer()),
        Some(50)
    );
    assert_eq!(
        enriched[1].get("double_age").and_then(|v| v.as_integer()),
        Some(60)
    );
}

#[test]
fn test_parse_cypher_then_graph_pattern_join() {
    let query_str = r#"
        FROM STREAM social [NOW]
        MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIKES]->(c:Post)
        RETURN a, b, c
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.pattern_groups.len(), 1);
    let pattern = &parsed.pattern_groups[0].patterns[0];
    assert_eq!(pattern.nodes.len(), 3);
    assert_eq!(pattern.relationships.len(), 2);

    let mut graph = GraphPatternJoinState::new();
    graph.add_statement(
        &stmt(
            "http://ex.org/Alice",
            "http://ex.org/follows",
            "http://ex.org/Bob",
        ),
        100,
    );
    graph.add_statement(
        &stmt(
            "http://ex.org/Bob",
            "http://ex.org/likes",
            "http://ex.org/Post1",
        ),
        300,
    );

    assert!(graph.has_edge("<http://ex.org/Alice>", "<http://ex.org/follows>"));
    let bob_targets = graph.get_objects("<http://ex.org/Alice>", "<http://ex.org/follows>");
    assert!(bob_targets.contains(&"<http://ex.org/Bob>"));
}

#[test]
fn test_cypher_parse_then_variable_length_path() {
    let query_str = r#"
        FROM STREAM social [NOW]
        MATCH (a:Person)-[:FOLLOWS*1..3]->(b:Person)
        RETURN a, b
    "#;

    let parsed = CypherQlParser::parse(query_str).expect("parse failed");
    let pattern = &parsed.pattern_groups[0].patterns[0];
    assert_eq!(pattern.relationships.len(), 1);

    let mut graph = GraphPatternJoinState::new();
    let people = ["Alice", "Bob", "Carol", "Dave", "Eve"];
    for i in 0..people.len() - 1 {
        graph.add_statement(
            &stmt(
                &format!("http://ex.org/{}", people[i]),
                "http://ex.org/follows",
                &format!("http://ex.org/{}", people[i + 1]),
            ),
            (i as i64) * 1000,
        );
    }

    let path_op =
        VariableLengthPathOperator::new(1, 3).with_relationship_type("<http://ex.org/follows>");
    let paths = path_op.find_paths("<http://ex.org/Alice>", &graph);
    assert!(paths.len() >= 3);
}

#[tokio::test]
async fn issue_4_single_stream_missing_pattern_returns_no_results() {
    let query_str = r#"
        SELECT ?sensor ?temp ?humidity
        FROM STREAM sensors [TRIPLES 2]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
                ?sensor <http://example.org/humidity> ?humidity .
            }
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "43",
            2000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_e2e_cqelsql_basic_parse_compile_execute() {
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

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "35",
            2000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_e2e_cqelsql_with_filter() {
    let query_str = r#"
        SELECT ?sensor ?temp
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
            FILTER(?temp > 30)
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_int_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_int_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "25",
            2000,
        ),
        stream_elem_int_literal(
            "http://example.org/s3",
            "http://example.org/temp",
            "35",
            3000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_e2e_cqelsql_with_prefix() {
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor ?temp
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor ex:temp ?temp .
            }
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_e2e_cqelsql_group_by_avg() {
    let query_str = r#"
        SELECT ?sensor (AVG(?temp) AS ?avg_temp)
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
        }
        GROUP BY ?sensor
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "40",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "50",
            2000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "30",
            3000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_e2e_cqelsql_order_by_desc_limit() {
    let query_str = r#"
        SELECT ?sensor ?temp
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
        }
        ORDER BY ?temp DESC
        LIMIT 2
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "10",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "50",
            2000,
        ),
        stream_elem_literal(
            "http://example.org/s3",
            "http://example.org/temp",
            "30",
            3000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_e2e_cqelsql_multi_stream_join() {
    let query_str = r#"
        SELECT ?sensor ?temp ?humidity
        FROM STREAM temp_stream [NOW]
        FROM STREAM humidity_stream [NOW]
        WHERE {
            STREAM temp_stream {
                ?sensor <http://example.org/temp> ?temp .
            }
            STREAM humidity_stream {
                ?sensor <http://example.org/humidity> ?humidity .
            }
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let temp_elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];
    let humidity_elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/humidity",
        "65",
        2000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream(
        "temp_stream",
        Box::pin(futures::stream::iter(temp_elements)),
    );
    inputs.add_stream(
        "humidity_stream",
        Box::pin(futures::stream::iter(humidity_elements)),
    );

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_e2e_cypherql_basic_parse_compile_execute() {
    let query_str = r#"
        FROM STREAM events [NOW]
        MATCH (a)-[:KNOWS]->(b)
        RETURN a, b
    "#;

    let definition = CypherQlParser::parse(query_str).expect("parse failed");
    let compiled = CypherQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![stream_elem_iri(
        "http://example.org/alice",
        "http://example.org/KNOWS",
        "http://example.org/bob",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("events", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_e2e_cypherql_with_where_filter() {
    let query_str = r#"
        FROM STREAM events [NOW]
        MATCH (a)-[:KNOWS]->(b)
        WHERE a.name <> "unknown"
        RETURN a, b
    "#;

    let definition = CypherQlParser::parse(query_str).expect("parse failed");
    let compiled = CypherQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![stream_elem_iri(
        "http://example.org/alice",
        "http://example.org/KNOWS",
        "http://example.org/bob",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("events", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(results.len() <= 1);
}

#[tokio::test]
async fn test_e2e_empty_stream_produces_empty_results() {
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

    let mut inputs = QueryInputs::new();
    inputs.add_stream(
        "sensors",
        Box::pin(futures::stream::empty::<StreamElement>()),
    );

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_serialization_roundtrip_contracts() {
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

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    let json = cqels_model::serialization::to_sparql_json_string(&results);
    assert!(json.contains("\"sensor\""));
    assert!(json.contains("\"temp\""));

    let triples =
        cqels_model::serialization::bindings_to_ntriples(&results, "sensor", "temp", "temp");
    assert!(
        triples.is_empty(),
        "non-IRI predicate bindings should be skipped safely"
    );
}

#[tokio::test]
async fn test_store_static_pattern_store_lookup() {
    let store = arc_store();
    store
        .load_statements(&[
            Statement::new(
                iri("http://example.org/s1"),
                iri_pred("http://example.org/type"),
                iri("http://example.org/TemperatureSensor"),
            ),
            Statement::new(
                iri("http://example.org/s2"),
                iri_pred("http://example.org/type"),
                iri("http://example.org/HumiditySensor"),
            ),
        ])
        .unwrap();

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

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile_with_store(query_str, definition, Some(store))
        .expect("compile failed");

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "65",
            2000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(result.contains("type"));
    }
}

#[tokio::test]
async fn test_named_graph_lookup() {
    let store = arc_store();
    store
        .load_named_graph(
            "http://example.org/metadata",
            &[Statement::new(
                iri("http://example.org/s1"),
                iri_pred("http://example.org/location"),
                iri("http://example.org/RoomA"),
            )],
        )
        .unwrap();
    store
        .load_statements(&[Statement::new(
            iri("http://example.org/s1"),
            iri_pred("http://example.org/location"),
            iri("http://example.org/RoomB"),
        )])
        .unwrap();

    let query_str = r#"
        SELECT ?sensor ?temp ?location
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
            GRAPH <http://example.org/metadata> {
                ?sensor <http://example.org/location> ?location .
            }
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled =
        CqelsQueryCompiler::compile_with_store(query_str, definition, Some(store)).unwrap();

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 1);
    let loc = results[0].get("location").unwrap();
    match loc {
        Value::Term(Term::Iri(iri_term)) => {
            assert_eq!(iri_term.as_str(), "http://example.org/RoomA");
        }
        _ => panic!("expected IRI term for location, got {:?}", loc),
    }
}

// A mandatory STATIC pattern that matches nothing makes the conjunctive
// static side empty, so the query yields no results — the stream row does
// not pass through (see #81).
#[tokio::test]
async fn test_static_no_match_yields_no_results() {
    let store = arc_store();

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

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled =
        CqelsQueryCompiler::compile_with_store(query_str, definition, Some(store)).unwrap();

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(
        results.len(),
        0,
        "unmatched mandatory STATIC pattern yields no results; got {results:?}"
    );
}

#[tokio::test]
async fn test_multiple_static_patterns_joined() {
    let store = arc_store();
    store
        .load_statements(&[
            Statement::new(
                iri("http://example.org/s1"),
                iri_pred("http://example.org/type"),
                iri("http://example.org/TemperatureSensor"),
            ),
            Statement::new(
                iri("http://example.org/s1"),
                iri_pred("http://example.org/location"),
                iri("http://example.org/RoomA"),
            ),
        ])
        .unwrap();

    let query_str = r#"
        SELECT ?sensor ?temp ?type ?location
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
            STATIC {
                ?sensor <http://example.org/type> ?type .
                ?sensor <http://example.org/location> ?location .
            }
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled =
        CqelsQueryCompiler::compile_with_store(query_str, definition, Some(store)).unwrap();

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("type"));
    assert!(results[0].contains("location"));
}

#[tokio::test]
async fn test_engine_stream_to_query_execution() {
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

    let rx = engine.get_stream_receiver("sensors").await.unwrap();
    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", receiver_to_stream(rx));

    let elem = stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    );
    tx.send(elem).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut result_stream = compiled.execute(inputs);
    let first = tokio::time::timeout(Duration::from_secs(2), result_stream.next())
        .await
        .expect("timed out waiting for result");

    assert!(first.is_some());
    let bs = first.unwrap();
    assert!(bs.contains("sensor"));
    assert!(bs.contains("temp"));

    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_optional_left_outer_join() {
    use cqels_core::compiler::pipeline::apply_optional;

    let evaluator = ExpressionEvaluator::new();
    let prefixes: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    let mut bs1 = BindingSet::new(100);
    bs1.insert("sensor", Value::String("http://ex.org/s1".into()));
    let mut bs2 = BindingSet::new(200);
    bs2.insert("sensor", Value::String("http://ex.org/s2".into()));

    let stream = Box::pin(futures::stream::iter(vec![bs1.clone(), bs2.clone()]));
    let optional_groups = vec![vec![cqels_core::parser::ast::CqelsPatternGroup::Default {
        patterns: vec![cqels_core::parser::ast::TriplePattern {
            subject: "?x".to_string(),
            predicate: "?y".to_string(),
            object: "?z".to_string(),
        }],
    }]];

    let results: Vec<_> = apply_optional(stream, &optional_groups, &prefixes, &evaluator)
        .collect()
        .await;

    assert_eq!(results.len(), 2);
    assert!(results[0].get("x").is_none());
    assert!(results[1].get("x").is_none());
}

#[tokio::test]
async fn test_union_two_branches() {
    use cqels_core::compiler::pipeline::apply_union;

    let evaluator = ExpressionEvaluator::new();
    let prefixes: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    let mut bs = BindingSet::new(100);
    bs.insert("sensor", Value::String("http://ex.org/s1".into()));

    let stream = Box::pin(futures::stream::iter(vec![bs]));
    let union_blocks = vec![(
        vec![cqels_core::parser::ast::CqelsPatternGroup::Default {
            patterns: vec![cqels_core::parser::ast::TriplePattern {
                subject: "?sensor".into(),
                predicate: "<http://ex.org/temp>".into(),
                object: "?temp".into(),
            }],
        }],
        vec![cqels_core::parser::ast::CqelsPatternGroup::Default {
            patterns: vec![cqels_core::parser::ast::TriplePattern {
                subject: "?sensor".into(),
                predicate: "<http://ex.org/humidity>".into(),
                object: "?hum".into(),
            }],
        }],
    )];

    let results: Vec<_> = apply_union(stream, &union_blocks, &prefixes, &evaluator)
        .collect()
        .await;

    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_minus_anti_join() {
    use cqels_core::compiler::pipeline::apply_minus;

    let prefixes: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    let mut bs1 = BindingSet::new(100);
    bs1.insert("sensor", Value::String("http://ex.org/s1".into()));
    bs1.insert("temp", Value::Integer(42));

    let mut bs2 = BindingSet::new(200);
    bs2.insert("other", Value::String("http://ex.org/x".into()));

    let mut bs3 = BindingSet::new(300);
    bs3.insert("sensor", Value::String("http://ex.org/s2".into()));
    bs3.insert("temp", Value::Integer(37));

    let stream = Box::pin(futures::stream::iter(vec![bs1, bs2, bs3]));
    let minus_blocks = vec![vec![cqels_core::parser::ast::TriplePattern {
        subject: "?sensor".into(),
        predicate: "<http://ex.org/badStatus>".into(),
        object: "?status".into(),
    }]];

    let results: Vec<_> = apply_minus(stream, &minus_blocks, &prefixes)
        .collect()
        .await;

    assert_eq!(results.len(), 1);
    assert!(results[0].get("other").is_some());
}

// ─── Additional compiler pipeline integration tests ─────────────────────────

#[tokio::test]
async fn test_e2e_cqelsql_with_bind() {
    let query_str = r#"
        SELECT ?sensor ?temp ?label
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
            BIND(CONCAT("sensor:", STR(?sensor)) AS ?label)
        }
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![stream_elem_literal(
        "http://example.org/s1",
        "http://example.org/temp",
        "42",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("sensor"));
    assert!(results[0].contains("temp"));
}

#[tokio::test]
async fn test_e2e_cqelsql_single_pattern_multiple_matches() {
    // Tests that a single-pattern query correctly matches multiple elements.
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

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "35",
            2000,
        ),
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "43",
            3000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(
        results.len(),
        3,
        "each element should match the single pattern"
    );
}

#[tokio::test]
async fn test_e2e_cypherql_different_relationship_types() {
    // Tests Cypher compilation with a different relationship type.
    let query_str = r#"
        FROM STREAM events [NOW]
        MATCH (a)-[:FOLLOWS]->(b)
        RETURN a, b
    "#;

    let definition = CypherQlParser::parse(query_str).expect("parse failed");
    let compiled = CypherQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_iri(
            "http://example.org/alice",
            "http://example.org/FOLLOWS",
            "http://example.org/bob",
            1000,
        ),
        stream_elem_iri(
            "http://example.org/carol",
            "http://example.org/FOLLOWS",
            "http://example.org/alice",
            2000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("events", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_e2e_cqelsql_count_aggregation() {
    let query_str = r#"
        SELECT (COUNT(?temp) AS ?cnt)
        FROM STREAM sensors [NOW]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/temp> ?temp .
            }
        }
        GROUP BY ?sensor
    "#;

    let definition = CqelsQlParser::parse(query_str).expect("parse failed");
    let compiled = CqelsQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "42",
            1000,
        ),
        stream_elem_literal(
            "http://example.org/s1",
            "http://example.org/temp",
            "43",
            2000,
        ),
        stream_elem_literal(
            "http://example.org/s2",
            "http://example.org/temp",
            "35",
            3000,
        ),
    ];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("sensors", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(
        !results.is_empty(),
        "COUNT aggregation should produce results"
    );
}

#[tokio::test]
async fn test_e2e_cypherql_with_return_alias() {
    let query_str = r#"
        FROM STREAM events [NOW]
        MATCH (a)-[:LIKES]->(b)
        RETURN a AS source, b AS target
    "#;

    let definition = CypherQlParser::parse(query_str).expect("parse failed");
    let compiled = CypherQueryCompiler::compile(query_str, definition).expect("compile failed");

    let elements = vec![stream_elem_iri(
        "http://example.org/alice",
        "http://example.org/LIKES",
        "http://example.org/bob",
        1000,
    )];

    let mut inputs = QueryInputs::new();
    inputs.add_stream("events", Box::pin(futures::stream::iter(elements)));

    let results: Vec<BindingSet> = compiled.execute(inputs).collect().await;
    assert!(!results.is_empty());
}
