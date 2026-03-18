mod common;

use common::*;

#[tokio::test]
async fn test_parse_then_window_then_aggregate() {
    let query_str = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor ?temp
        FROM STREAM sensors [RANGE 10s]
        WHERE {
            ?sensor ex:temperature ?temp .
        }
        ORDER BY ?temp DESC
        LIMIT 5
    "#;

    let parsed = CqelsQlParser::parse(query_str).expect("parse failed");
    assert_eq!(parsed.streams.len(), 1);
    assert_eq!(parsed.streams[0].name, "sensors");
    assert!(parsed.has_order_by());
    assert_eq!(parsed.limit, Some(5));

    let readings = generate_sensor_readings(100);
    let stream = Box::pin(futures::stream::iter(readings));
    let window = ten_second_window();
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert!(!batches.is_empty());

    let count_op =
        WindowedAggregateOperator::new(CountAggregate, |_: &RdfStreamElement| None, 10_000);

    let mut total_elements = 0i64;
    for batch in &batches {
        let results = count_op.process_batch(&batch.elements);
        for res in &results {
            assert!(res.value > 0);
            total_elements += res.value;
        }
    }

    assert_eq!(total_elements, 100);
}

#[tokio::test]
async fn test_window_then_filter_then_rank() {
    let events = generate_social_events(200);
    let stream = Box::pin(futures::stream::iter(events));
    let window = ten_second_window();
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert!(!batches.is_empty());

    for batch in &batches {
        let mut top_k = TopKOperator::new(
            5,
            |e: &RdfStreamElement| e.timestamp() as f64,
            SortDirection::Descending,
        );

        for elem in &batch.elements {
            top_k.add(elem.clone());
        }

        let top = top_k.get_top_k();
        assert!(top.len() <= 5);
        for pair in top.windows(2) {
            assert!(pair[0].timestamp() >= pair[1].timestamp());
        }
    }
}

#[tokio::test]
async fn test_window_grouped_aggregate_then_rank() {
    let readings = generate_sensor_readings(200);
    let stream = Box::pin(futures::stream::iter(readings));
    let window = ten_second_window();
    let batches: Vec<_> = window.apply(stream).collect().await;
    assert!(!batches.is_empty());

    let count_op = WindowedAggregateOperator::new(
        CountAggregate,
        |e: &RdfStreamElement| Some(GroupKey::single(e.statement.predicate.as_str().to_string())),
        10_000,
    );

    for batch in &batches {
        let results = count_op.process_batch(&batch.elements);
        assert!(!results.is_empty());

        let mut ranked: Vec<_> = results.iter().collect();
        ranked.sort_by(|a, b| b.value.cmp(&a.value));
        if ranked.len() > 1 {
            assert!(ranked[0].value >= ranked[1].value);
        }
    }
}

#[tokio::test]
async fn test_session_window_with_aggregates() {
    let events: Vec<RdfStreamElement> = (0..30)
        .map(|i| {
            let session = i / 10;
            let ts = session * 15000 + (i % 10) * 500;
            make_rdf_literal_element(
                &format!("http://ex.org/sensor/{}", i % 3),
                "http://ex.org/value",
                &format!("{}", 20.0 + (i as f64) * 0.5),
                ts,
            )
        })
        .collect();

    let stream = Box::pin(futures::stream::iter(events));
    let session = SessionWindow::new(Duration::from_secs(6));
    let batches: Vec<_> = session.apply(stream).collect().await;

    assert!(batches.len() >= 2);

    let sum_agg = SumAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let avg_agg = AvgAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let min_agg = MinAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let max_agg = MaxAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);

    for batch in &batches {
        let mut sum_acc = sum_agg.create_accumulator();
        let mut avg_acc = avg_agg.create_accumulator();
        let mut min_acc = min_agg.create_accumulator();
        let mut max_acc = max_agg.create_accumulator();

        for elem in &batch.elements {
            sum_acc = sum_agg.add(elem, sum_acc);
            avg_acc = avg_agg.add(elem, avg_acc);
            min_acc = min_agg.add(elem, min_acc);
            max_acc = max_agg.add(elem, max_acc);
        }

        let avg = avg_agg.get_result(&avg_acc);
        let min_ts = min_agg.get_result(&min_acc);
        let max_ts = max_agg.get_result(&max_acc);

        assert!(batch.size() > 0);
        assert!(max_ts >= min_ts);
        assert!(avg >= batch.window_start as f64);
    }
}

#[tokio::test]
async fn test_sliding_window_overlap_consistency() {
    let readings = generate_sensor_readings(100);
    let total_count = readings.len();

    let stream = Box::pin(futures::stream::iter(readings));
    let sliding = SlidingWindow::new(Duration::from_secs(15), Duration::from_secs(5));
    let batches: Vec<_> = sliding.apply(stream).collect().await;

    assert!(batches.len() > 1);

    let total_in_windows: usize = batches.iter().map(|b| b.size()).sum();
    assert!(total_in_windows >= total_count);

    for batch in &batches {
        assert_eq!(batch.window_end - batch.window_start, 15000);
    }
}

#[tokio::test]
async fn test_count_window_then_aggregate() {
    let readings = generate_sensor_readings(50);

    let stream = Box::pin(futures::stream::iter(readings));
    let window = TumblingCountWindow::new(10);
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert_eq!(batches.len(), 5);
    for batch in &batches {
        assert_eq!(batch.size(), 10);
    }

    let min_agg = MinAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);
    let max_agg = MaxAggregate::new(|e: &RdfStreamElement| e.timestamp() as f64);

    for batch in &batches {
        let mut min_acc = min_agg.create_accumulator();
        let mut max_acc = max_agg.create_accumulator();

        for elem in &batch.elements {
            min_acc = min_agg.add(elem, min_acc);
            max_acc = max_agg.add(elem, max_acc);
        }

        let min_ts = min_agg.get_result(&min_acc);
        let max_ts = max_agg.get_result(&max_acc);
        assert!(max_ts >= min_ts);
    }
}

#[tokio::test]
async fn test_session_window_final_flush() {
    let elements: Vec<TimestampedValue<i64>> = vec![
        TimestampedValue::new(100, 100),
        TimestampedValue::new(200, 200),
        TimestampedValue::new(300, 300),
    ];

    let stream = Box::pin(futures::stream::iter(elements));
    let window = SessionWindow::new(Duration::from_secs(1));
    let batches: Vec<_> = window.apply(stream).collect().await;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].elements.len(), 3);
}

#[tokio::test]
async fn test_session_window_empty_stream_no_flush() {
    let elements: Vec<TimestampedValue<i64>> = vec![];
    let stream = Box::pin(futures::stream::iter(elements));
    let window = SessionWindow::new(Duration::from_secs(1));
    let batches: Vec<_> = window.apply(stream).collect().await;
    assert_eq!(batches.len(), 0);
}

#[test]
fn test_group_concat_aggregate() {
    let evaluator = ExpressionEvaluator::new();

    let elements = vec![
        {
            let mut bs = BindingSet::new(0);
            bs.insert("city", Value::String("NYC".into()));
            bs.insert("sensor", Value::String("s1".into()));
            bs
        },
        {
            let mut bs = BindingSet::new(0);
            bs.insert("city", Value::String("NYC".into()));
            bs.insert("sensor", Value::String("s2".into()));
            bs
        },
        {
            let mut bs = BindingSet::new(0);
            bs.insert("city", Value::String("NYC".into()));
            bs.insert("sensor", Value::String("s3".into()));
            bs
        },
        {
            let mut bs = BindingSet::new(0);
            bs.insert("city", Value::String("LA".into()));
            bs.insert("sensor", Value::String("s4".into()));
            bs
        },
    ];

    let group_by = vec!["city".to_string()];
    let aggregates = vec![PipelineAggregateSpec {
        function: AggregateExprFunction::GroupConcat,
        argument: Expression::Variable("sensor".into()),
        alias: "sensors".into(),
        distinct: false,
        separator: Some("; ".into()),
    }];

    let results = apply_group_by_aggregates(elements, &group_by, &aggregates, &evaluator);
    assert_eq!(results.len(), 2);

    for result in &results {
        let city = result.get("city").unwrap().as_string().unwrap();
        let sensors = result.get("sensors").unwrap().as_string().unwrap();
        match city {
            "NYC" => {
                assert_eq!(sensors.matches("; ").count(), 2);
                assert!(sensors.contains("s1"));
                assert!(sensors.contains("s2"));
                assert!(sensors.contains("s3"));
            }
            "LA" => assert_eq!(sensors, "s4"),
            _ => panic!("unexpected city: {}", city),
        }
    }
}
