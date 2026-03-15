//! WKT geometry parsing and conversion utilities.
//!
//! Handles GeoSPARQL WKT literal formats including:
//! - Plain WKT: `POINT(1.0 2.0)`
//! - CRS-prefixed: `<http://...crs/EPSG/0/4326> POINT(1.0 2.0)`
//! - EWKT: `SRID=4326;POINT(1.0 2.0)`

use geo::Geometry;
use wkt::ToWkt;
use wkt::TryFromWkt;

use cqels_model::term::LiteralTerm;
use cqels_model::{Term, Value};

use crate::vocabulary::WKT_LITERAL;

/// Normalizes a WKT label by stripping CRS prefixes and EWKT SRID prefixes.
///
/// Supports three formats:
/// 1. CRS-prefixed: `<http://...> POINT(...)` -> `POINT(...)`
/// 2. EWKT: `SRID=4326;POINT(...)` -> `POINT(...)`
/// 3. Plain WKT: returned as-is (trimmed)
pub fn normalize_wkt_label(label: &str) -> Option<&str> {
    let label = label.trim();
    if label.is_empty() {
        return None;
    }

    // CRS-prefixed: <http://...> POINT(...)
    if label.starts_with('<') {
        if let Some(gt_pos) = label.find('>') {
            let after = label[gt_pos + 1..].trim();
            if after.is_empty() {
                return None;
            }
            return Some(after);
        }
    }

    // EWKT: SRID=4326;POINT(...)
    if label.to_uppercase().starts_with("SRID") {
        if let Some(semi_pos) = label.find(';') {
            let after = label[semi_pos + 1..].trim();
            if after.is_empty() {
                return None;
            }
            return Some(after);
        }
    }

    Some(label)
}

/// Parses a WKT string (possibly CRS-prefixed or EWKT) into a `geo::Geometry`.
pub fn parse_wkt(wkt_string: &str) -> Option<Geometry<f64>> {
    let normalized = normalize_wkt_label(wkt_string)?;
    Geometry::<f64>::try_from_wkt_str(normalized).ok()
}

/// Extracts a geometry from a `cqels_model::Value`.
///
/// The value must be a `Term::Literal` containing a WKT string. The literal's
/// datatype is not strictly checked -- any literal with parseable WKT content
/// will succeed.
pub fn value_to_geometry(value: &Value) -> Option<Geometry<f64>> {
    match value {
        Value::Term(Term::Literal(lit)) => parse_wkt(lit.value()),
        Value::String(s) => parse_wkt(s),
        _ => None,
    }
}

/// Converts a `geo::Geometry` to a `cqels_model::Value` as a WKT literal.
pub fn geometry_to_value(geom: &Geometry<f64>) -> Value {
    let wkt_string = geom.wkt_string();
    Value::Term(Term::Literal(
        LiteralTerm::new(wkt_string).with_datatype(WKT_LITERAL),
    ))
}

/// Creates a WKT literal `Value` from a raw WKT string.
pub fn wkt_literal(wkt_string: &str) -> Value {
    Value::Term(Term::Literal(
        LiteralTerm::new(wkt_string).with_datatype(WKT_LITERAL),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_plain_wkt() {
        assert_eq!(normalize_wkt_label("POINT(1 2)"), Some("POINT(1 2)"));
    }

    #[test]
    fn test_normalize_crs_prefixed() {
        let input = "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(0.5 0.5)";
        assert_eq!(normalize_wkt_label(input), Some("POINT(0.5 0.5)"));
    }

    #[test]
    fn test_normalize_ewkt() {
        assert_eq!(
            normalize_wkt_label("SRID=4326;POINT(0.75 0.75)"),
            Some("POINT(0.75 0.75)")
        );
    }

    #[test]
    fn test_normalize_empty() {
        assert_eq!(normalize_wkt_label(""), None);
        assert_eq!(normalize_wkt_label("   "), None);
    }

    #[test]
    fn test_parse_wkt_point() {
        let geom = parse_wkt("POINT(1.5 2.5)");
        assert!(geom.is_some());
    }

    #[test]
    fn test_parse_wkt_polygon() {
        let geom = parse_wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        assert!(geom.is_some());
    }

    #[test]
    fn test_parse_crs_prefixed_wkt() {
        let geom = parse_wkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(0.5 0.5)");
        assert!(geom.is_some());
    }

    #[test]
    fn test_parse_ewkt() {
        let geom = parse_wkt("SRID=4326;POINT(0.75 0.75)");
        assert!(geom.is_some());
    }

    #[test]
    fn test_parse_invalid() {
        assert!(parse_wkt("NOT_A_GEOMETRY").is_none());
        assert!(parse_wkt("").is_none());
    }

    #[test]
    fn test_value_roundtrip() {
        let wkt = "POINT(10 20)";
        let val = wkt_literal(wkt);
        let geom = value_to_geometry(&val);
        assert!(geom.is_some());

        let back = geometry_to_value(&geom.unwrap());
        let geom2 = value_to_geometry(&back);
        assert!(geom2.is_some());
    }
}
