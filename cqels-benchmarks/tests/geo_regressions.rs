mod common;

use common::*;

#[test]
fn issue_2_geo_reasoner_uses_ontology_namespace_predicates() {
    let reasoner = GeoSparqlSimpleReasoner;
    let facts = vec![Statement::new(
        iri("http://example.org/a"),
        iri_pred("http://www.opengis.net/ont/geosparql#sfWithin"),
        iri("http://example.org/b"),
    )];

    let inferred = reasoner.infer(&facts);
    assert_eq!(inferred.len(), 1);
    assert_eq!(
        inferred[0].predicate.as_str(),
        "http://www.opengis.net/ont/geosparql#sfContains"
    );
}

#[test]
fn issue_3_spatial_index_contains_query_point() {
    let mut idx = GeoSpatialIndex::new();
    idx.put(
        "poly",
        &wkt_literal("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
    );
    idx.put("small", &wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));

    let query = wkt_literal("POINT(0.5 0.5)");
    let matches = idx.query_matches("geof:sfContains", &query);

    assert!(matches.contains(&"poly".to_string()));
    assert!(matches.contains(&"small".to_string()));
}

#[test]
fn issue_9_boundary_point_intersects_polygon() {
    let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
    let boundary_pt = wkt_literal("POINT(0 0)");

    assert_eq!(sf_equals(&poly, &poly), Some(Value::Boolean(true)));
    assert_eq!(
        sf_intersects(&poly, &boundary_pt),
        Some(Value::Boolean(true))
    );
    assert_eq!(
        sf_intersects(&boundary_pt, &poly),
        Some(Value::Boolean(true))
    );
}

#[test]
fn test_geo_function_dispatch_and_index_candidates() {
    let p = wkt_literal("POINT(1 2)");
    let dispatch = evaluate_function("geof:sfEquals", &[&p, &p]);
    assert_eq!(dispatch, Some(Value::Boolean(true)));

    let mut idx = GeoSpatialIndex::new();
    idx.put("p1", &wkt_literal("POINT(1 2)"));
    idx.put("p2", &wkt_literal("POINT(5 5)"));
    let matches = idx.query_candidates(&wkt_literal("POLYGON((0 0, 2 0, 2 3, 0 3, 0 0))"));
    assert!(matches.contains(&"p1".to_string()));
    assert!(!matches.contains(&"p2".to_string()));
}
