//! GeoSPARQL function implementations.
//!
//! Provides spatial computation functions compliant with the GeoSPARQL
//! standard, including distance calculation and Simple Features topological
//! relations. Functions accept `cqels_model::Value` arguments containing
//! WKT geometry literals.

use std::collections::HashMap;
use std::sync::OnceLock;

use geo::algorithm::{Contains, Intersects};
use geo::{Distance, Euclidean, Geometry, Haversine, Point};

use cqels_model::Value;

use crate::geometry::value_to_geometry;
use crate::vocabulary;

// ─── Type alias for function dispatch ────────────────────────────────────────

type GeoFn = fn(&[&Value]) -> Option<Value>;

static FUNCTION_REGISTRY: OnceLock<HashMap<String, GeoFn>> = OnceLock::new();

fn function_registry() -> &'static HashMap<String, GeoFn> {
    FUNCTION_REGISTRY.get_or_init(|| {
        let mut m: HashMap<String, GeoFn> = HashMap::new();
        m.insert("geof:distance".into(), dispatch_distance);
        m.insert("geof:sfwithin".into(), dispatch_sf_within);
        m.insert("geof:sfcontains".into(), dispatch_sf_contains);
        m.insert("geof:sfintersects".into(), dispatch_sf_intersects);
        m.insert("geof:sfdisjoint".into(), dispatch_sf_disjoint);
        m.insert("geof:sfequals".into(), dispatch_sf_equals);
        m
    })
}

// ─── Dispatch wrappers ───────────────────────────────────────────────────────

fn dispatch_distance(args: &[&Value]) -> Option<Value> {
    if args.len() < 3 {
        return None;
    }
    distance(args[0], args[1], args[2])
}

fn dispatch_sf_within(args: &[&Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    sf_within(args[0], args[1])
}

fn dispatch_sf_contains(args: &[&Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    sf_contains(args[0], args[1])
}

fn dispatch_sf_intersects(args: &[&Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    sf_intersects(args[0], args[1])
}

fn dispatch_sf_disjoint(args: &[&Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    sf_disjoint(args[0], args[1])
}

fn dispatch_sf_equals(args: &[&Value]) -> Option<Value> {
    if args.len() < 2 {
        return None;
    }
    sf_equals(args[0], args[1])
}

// ─── Public API ──────────────────────────────────────────────────────────────

/// Computes the distance between two geometries.
///
/// The `unit` parameter determines the distance calculation method:
/// - `uom:metre` -- Haversine distance in metres (for geographic coordinates)
/// - `uom:degree` -- Euclidean distance in degrees
/// - `uom:radian` -- Euclidean distance converted to radians
///
/// Returns `None` if geometries cannot be parsed or unit is unrecognized.
pub fn distance(geom1: &Value, geom2: &Value, unit: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;

    let unit_str = extract_iri_str(unit)?;

    let p1 = geometry_to_point(&g1)?;
    let p2 = geometry_to_point(&g2)?;

    let dist = if unit_str == vocabulary::UOM_METRE || unit_str.ends_with("metre") {
        Haversine::distance(p1, p2)
    } else if unit_str == vocabulary::UOM_DEGREE || unit_str.ends_with("degree") {
        Euclidean::distance(p1, p2)
    } else if unit_str == vocabulary::UOM_RADIAN || unit_str.ends_with("radian") {
        Euclidean::distance(p1, p2).to_radians()
    } else {
        return None;
    };

    Some(Value::Float(dist))
}

/// Tests whether `geom1` is spatially within `geom2` (SF Within).
pub fn sf_within(geom1: &Value, geom2: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;
    Some(Value::Boolean(geometry_contains(&g2, &g1)))
}

/// Tests whether `geom1` spatially contains `geom2` (SF Contains).
pub fn sf_contains(geom1: &Value, geom2: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;
    Some(Value::Boolean(geometry_contains(&g1, &g2)))
}

/// Tests whether `geom1` spatially intersects `geom2` (SF Intersects).
pub fn sf_intersects(geom1: &Value, geom2: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;
    Some(Value::Boolean(geometry_intersects(&g1, &g2)))
}

/// Tests whether `geom1` is spatially disjoint from `geom2` (SF Disjoint).
pub fn sf_disjoint(geom1: &Value, geom2: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;
    Some(Value::Boolean(!geometry_intersects(&g1, &g2)))
}

/// Tests whether `geom1` is spatially equal to `geom2` (SF Equals).
///
/// Two geometries are considered equal if they contain each other.
pub fn sf_equals(geom1: &Value, geom2: &Value) -> Option<Value> {
    let g1 = value_to_geometry(geom1)?;
    let g2 = value_to_geometry(geom2)?;
    let equal = geometry_contains(&g1, &g2) && geometry_contains(&g2, &g1);
    Some(Value::Boolean(equal))
}

/// Evaluates a GeoSPARQL function by canonical name.
///
/// The function name is normalized (lowercased, angle brackets stripped,
/// GEOF namespace resolved to `geof:` prefix).
///
/// Returns `None` if the function is unknown or evaluation fails.
pub fn evaluate_function(function_name: &str, args: &[&Value]) -> Option<Value> {
    let canonical = vocabulary::canonical_function_name(function_name);
    let f = function_registry().get(&canonical)?;
    f(args)
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// Extracts a centroid point from a geometry (for distance calculations).
fn geometry_to_point(geom: &Geometry<f64>) -> Option<Point<f64>> {
    match geom {
        Geometry::Point(p) => Some(*p),
        other => {
            use geo::algorithm::Centroid;
            other.centroid()
        }
    }
}

/// Tests if `container` spatially contains `contained` (DE-9IM: T*****FF*).
///
/// Delegates to the `geo` crate's `Contains` trait which correctly handles
/// all geometry type combinations including boundary semantics.
fn geometry_contains(container: &Geometry<f64>, contained: &Geometry<f64>) -> bool {
    container.contains(contained)
}

/// Tests if two geometries spatially intersect (DE-9IM: T******** etc).
///
/// Delegates to the `geo` crate's `Intersects` trait which correctly handles
/// boundary-boundary, boundary-interior, and interior-interior intersections.
fn geometry_intersects(g1: &Geometry<f64>, g2: &Geometry<f64>) -> bool {
    g1.intersects(g2)
}

/// Extracts an IRI string from a Value.
fn extract_iri_str(value: &Value) -> Option<&str> {
    match value {
        Value::Term(cqels_model::Term::Iri(iri)) => Some(iri.as_str()),
        Value::String(s) => Some(s.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::wkt_literal;
    use cqels_model::term::IriTerm;
    use cqels_model::Term;

    fn uom_metre() -> Value {
        Value::Term(Term::Iri(IriTerm::new(vocabulary::UOM_METRE)))
    }

    fn uom_degree() -> Value {
        Value::Term(Term::Iri(IriTerm::new(vocabulary::UOM_DEGREE)))
    }

    #[test]
    fn test_distance_in_metres() {
        let p1 = wkt_literal("POINT(0 0)");
        let p2 = wkt_literal("POINT(1 1)");
        let result = distance(&p1, &p2, &uom_metre()).unwrap();
        if let Value::Float(d) = result {
            assert!(d > 100_000.0, "distance should be >100km, got {d}");
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_distance_in_degrees() {
        let p1 = wkt_literal("POINT(0 0)");
        let p2 = wkt_literal("POINT(3 4)");
        let result = distance(&p1, &p2, &uom_degree()).unwrap();
        if let Value::Float(d) = result {
            assert!((d - 5.0).abs() < 0.01, "expected ~5 degrees, got {d}");
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_sf_equals_same_point() {
        let p = wkt_literal("POINT(1 2)");
        assert_eq!(sf_equals(&p, &p), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_sf_within_point_in_polygon() {
        let point = wkt_literal("POINT(0.5 0.5)");
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert_eq!(sf_within(&point, &poly), Some(Value::Boolean(true)));
        assert_eq!(sf_within(&poly, &point), Some(Value::Boolean(false)));
    }

    #[test]
    fn test_sf_contains_polygon_point() {
        let point = wkt_literal("POINT(0.5 0.5)");
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert_eq!(sf_contains(&poly, &point), Some(Value::Boolean(true)));
        assert_eq!(sf_contains(&point, &poly), Some(Value::Boolean(false)));
    }

    #[test]
    fn test_sf_intersects() {
        let p1 = wkt_literal("POINT(0.5 0.5)");
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert_eq!(sf_intersects(&p1, &poly), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_sf_disjoint() {
        let p_inside = wkt_literal("POINT(0.5 0.5)");
        let p_outside = wkt_literal("POINT(5 5)");
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert_eq!(sf_disjoint(&p_inside, &poly), Some(Value::Boolean(false)));
        assert_eq!(sf_disjoint(&p_outside, &poly), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_evaluate_function_dispatch() {
        let p = wkt_literal("POINT(1 2)");
        let result = evaluate_function("geof:sfEquals", &[&p, &p]);
        assert_eq!(result, Some(Value::Boolean(true)));
    }

    #[test]
    fn test_evaluate_function_full_iri() {
        let p = wkt_literal("POINT(1 2)");
        let result = evaluate_function(
            "<http://www.opengis.net/def/function/geosparql/sfEquals>",
            &[&p, &p],
        );
        assert_eq!(result, Some(Value::Boolean(true)));
    }

    #[test]
    fn test_evaluate_function_unknown() {
        let p = wkt_literal("POINT(1 2)");
        assert_eq!(evaluate_function("geof:unknownFunc", &[&p, &p]), None);
    }

    // ─── Regression tests for boundary semantics (#9) ───────────────────

    #[test]
    fn test_sf_equals_identical_polygon() {
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert_eq!(sf_equals(&poly, &poly), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_sf_intersects_boundary_point() {
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let boundary_pt = wkt_literal("POINT(0 0)");
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
    fn test_sf_equals_different_polygons() {
        let poly1 = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let poly2 = wkt_literal("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
        assert_eq!(sf_equals(&poly1, &poly2), Some(Value::Boolean(false)));
    }

    #[test]
    fn test_sf_disjoint_boundary_point() {
        let poly = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let boundary_pt = wkt_literal("POINT(0 0)");
        // A boundary point intersects the polygon, so it is NOT disjoint
        assert_eq!(
            sf_disjoint(&poly, &boundary_pt),
            Some(Value::Boolean(false))
        );
    }
}
