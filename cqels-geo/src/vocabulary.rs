//! GeoSPARQL vocabulary constants and prefix resolution utilities.
//!
//! Provides namespace IRIs, topological relation constants, unit-of-measure
//! IRIs, and helper functions for resolving prefixed names and normalizing
//! function identifiers.

use std::collections::HashMap;
use std::sync::OnceLock;

// ─── Namespace constants ─────────────────────────────────────────────────────

/// GeoSPARQL ontology namespace.
pub const GEO_NS: &str = "http://www.opengis.net/ont/geosparql#";

/// GeoSPARQL function namespace.
pub const GEOF_NS: &str = "http://www.opengis.net/def/function/geosparql/";

/// OGC unit-of-measure namespace.
pub const UOM_NS: &str = "http://www.opengis.net/def/uom/OGC/1.0/";

/// Default CRS (WGS 84 / CRS84).
pub const CRS84: &str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";

// ─── Datatype IRIs ───────────────────────────────────────────────────────────

/// WKT literal datatype IRI.
pub const WKT_LITERAL: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";

// ─── Simple Features topological relation IRIs ───────────────────────────────

pub const SF_WITHIN: &str = "http://www.opengis.net/def/function/geosparql/sfWithin";
pub const SF_CONTAINS: &str = "http://www.opengis.net/def/function/geosparql/sfContains";
pub const SF_INTERSECTS: &str = "http://www.opengis.net/def/function/geosparql/sfIntersects";
pub const SF_DISJOINT: &str = "http://www.opengis.net/def/function/geosparql/sfDisjoint";
pub const SF_EQUALS: &str = "http://www.opengis.net/def/function/geosparql/sfEquals";
pub const SF_TOUCHES: &str = "http://www.opengis.net/def/function/geosparql/sfTouches";
pub const SF_CROSSES: &str = "http://www.opengis.net/def/function/geosparql/sfCrosses";
pub const SF_OVERLAPS: &str = "http://www.opengis.net/def/function/geosparql/sfOverlaps";

/// GeoSPARQL distance function IRI.
pub const DISTANCE: &str = "http://www.opengis.net/def/function/geosparql/distance";

// ─── Unit-of-measure IRIs ────────────────────────────────────────────────────

pub const UOM_METRE: &str = "http://www.opengis.net/def/uom/OGC/1.0/metre";
pub const UOM_DEGREE: &str = "http://www.opengis.net/def/uom/OGC/1.0/degree";
pub const UOM_RADIAN: &str = "http://www.opengis.net/def/uom/OGC/1.0/radian";

// ─── Prefix map ──────────────────────────────────────────────────────────────

static PREFIX_MAP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();

fn prefix_map() -> &'static HashMap<&'static str, &'static str> {
    PREFIX_MAP.get_or_init(|| {
        let mut m = HashMap::new();
        m.insert("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        m.insert("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        m.insert("owl", "http://www.w3.org/2002/07/owl#");
        m.insert("xsd", "http://www.w3.org/2001/XMLSchema#");
        m.insert("geo", GEO_NS);
        m.insert("geof", GEOF_NS);
        m.insert("uom", UOM_NS);
        m
    })
}

// ─── Functions ───────────────────────────────────────────────────────────────

/// Resolves a prefixed name (e.g. `"geo:wktLiteral"`) to a full IRI string.
///
/// Returns `None` if the input is invalid, the prefix is unknown, or the
/// local name is empty.
pub fn resolve_prefixed_name(prefixed: &str) -> Option<String> {
    let prefixed = prefixed.trim();
    let colon = prefixed.find(':')?;
    if colon == 0 || colon >= prefixed.len() - 1 {
        return None;
    }

    let prefix = &prefixed[..colon];
    let local = &prefixed[colon + 1..];
    if local.is_empty() {
        return None;
    }

    let ns = prefix_map().get(prefix)?;
    Some(format!("{ns}{local}"))
}

/// Normalizes a function identifier into a canonical `geof:localName` form.
///
/// - Strips outer angle brackets (`<...>`)
/// - Lowercases the result
/// - Converts full GEOF namespace URIs to `geof:localName`
///
/// Returns an empty string if the input is empty/null.
pub fn canonical_function_name(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        return String::new();
    }

    // Strip angle brackets
    let name = if name.starts_with('<') && name.ends_with('>') {
        &name[1..name.len() - 1]
    } else {
        name
    };

    let lower = name.to_lowercase();

    // If it's a full GEOF URI, extract the local name
    let geof_lower = GEOF_NS.to_lowercase();
    if lower.starts_with(&geof_lower) {
        let local = &lower[geof_lower.len()..];
        return format!("geof:{local}");
    }

    lower
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_prefixed_name() {
        assert_eq!(
            resolve_prefixed_name("geo:wktLiteral"),
            Some(format!("{GEO_NS}wktLiteral"))
        );
        assert_eq!(
            resolve_prefixed_name("geof:sfWithin"),
            Some(SF_WITHIN.to_string())
        );
        assert_eq!(
            resolve_prefixed_name("uom:metre"),
            Some(UOM_METRE.to_string())
        );
        assert_eq!(
            resolve_prefixed_name("xsd:double"),
            Some("http://www.w3.org/2001/XMLSchema#double".to_string())
        );
    }

    #[test]
    fn test_resolve_invalid() {
        assert_eq!(resolve_prefixed_name(""), None);
        assert_eq!(resolve_prefixed_name("noprefix"), None);
        assert_eq!(resolve_prefixed_name(":empty"), None);
        assert_eq!(resolve_prefixed_name("unknown:foo"), None);
        assert_eq!(resolve_prefixed_name("geo:"), None);
    }

    #[test]
    fn test_canonical_function_name() {
        assert_eq!(canonical_function_name("geof:sfWithin"), "geof:sfwithin");
        assert_eq!(
            canonical_function_name("<http://www.opengis.net/def/function/geosparql/sfWithin>"),
            "geof:sfwithin"
        );
        assert_eq!(canonical_function_name("SfContains"), "sfcontains");
        assert_eq!(canonical_function_name(""), "");
    }
}
