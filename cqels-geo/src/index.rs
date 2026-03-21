//! Spatial index for GeoSPARQL WKT literals.
//!
//! Uses an R*-tree ([`rstar`]) for envelope-based candidate pruning with
//! optional exact topological relation matching via
//! [`crate::functions::evaluate_function`].

use std::collections::HashMap;

use geo::algorithm::BoundingRect;
use geo::Geometry;
use rstar::{RTree, RTreeObject, AABB};

use cqels_model::Value;

use crate::functions;
use crate::geometry::{parse_wkt, value_to_geometry};
use crate::vocabulary;

// ─── Disjoint function names (require full scan) ─────────────────────────────

const DISJOINT_FUNCTIONS: &[&str] = &["sfdisjoint", "ehdisjoint", "rcc8dc"];

// ─── IndexEntry for R-tree ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct IndexEntry {
    id: String,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

// ─── IndexedGeometry ─────────────────────────────────────────────────────────

/// A geometry stored in the spatial index with its original WKT literal.
#[derive(Debug, Clone)]
pub struct IndexedGeometry {
    /// Identifier for this geometry.
    pub id: String,
    /// Original WKT literal string.
    pub wkt_literal: String,
    /// Parsed geometry.
    pub geometry: Geometry<f64>,
}

// ─── GeoSpatialIndex ────────────────────────────────────────────────────────

/// Spatial index for efficient geometry lookup and topological queries.
///
/// Geometries are stored by string ID and indexed in an R*-tree for
/// envelope-based candidate pruning. Exact topological matching is done
/// via GeoSPARQL functions.
///
/// # Thread Safety
///
/// This type is not `Sync` by default. Wrap in `std::sync::Mutex` or
/// `parking_lot::Mutex` for concurrent access.
///
/// # Examples
///
/// ```
/// use cqels_geo::GeoSpatialIndex;
/// use cqels_geo::geometry::wkt_literal;
///
/// let mut idx = GeoSpatialIndex::new();
/// idx.put("building_a", &wkt_literal("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"));
/// idx.put("sensor_1", &wkt_literal("POINT(5 5)"));
/// assert_eq!(idx.size(), 2);
///
/// // Find geometries that intersect with a query region
/// let query = wkt_literal("POLYGON((3 3, 7 3, 7 7, 3 7, 3 3))");
/// let matches = idx.query_matches("geof:sfIntersects", &query);
/// assert!(matches.contains(&"building_a".to_string()));
/// assert!(matches.contains(&"sensor_1".to_string()));
/// ```
pub struct GeoSpatialIndex {
    geometries: HashMap<String, IndexedGeometry>,
    tree: RTree<IndexEntry>,
    dirty: bool,
}

impl Default for GeoSpatialIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GeoSpatialIndex {
    /// Creates an empty spatial index.
    pub fn new() -> Self {
        Self {
            geometries: HashMap::new(),
            tree: RTree::new(),
            dirty: false,
        }
    }

    /// Inserts or replaces a geometry in the index.
    ///
    /// The `geometry_value` must be a `Value` containing a WKT literal.
    /// Returns `true` if the geometry was successfully parsed and indexed.
    pub fn put(&mut self, id: &str, geometry_value: &Value) -> bool {
        if id.is_empty() {
            return false;
        }

        let wkt_str = match geometry_value {
            Value::Term(cqels_model::Term::Literal(lit)) => lit.value().to_string(),
            Value::String(s) => s.clone(),
            _ => return false,
        };

        let geom = match parse_wkt(&wkt_str) {
            Some(g) => g,
            None => return false,
        };

        self.geometries.insert(
            id.to_string(),
            IndexedGeometry {
                id: id.to_string(),
                wkt_literal: wkt_str,
                geometry: geom,
            },
        );
        self.dirty = true;
        true
    }

    /// Removes a geometry from the index.
    ///
    /// Returns `true` if the geometry was found and removed.
    pub fn remove(&mut self, id: &str) -> bool {
        if self.geometries.remove(id).is_some() {
            self.dirty = true;
            true
        } else {
            false
        }
    }

    /// Removes all geometries from the index.
    pub fn clear(&mut self) {
        self.geometries.clear();
        self.tree = RTree::new();
        self.dirty = false;
    }

    /// Returns the number of indexed geometries.
    pub fn size(&self) -> usize {
        self.geometries.len()
    }

    /// Returns candidate geometry IDs whose envelopes intersect with the
    /// query geometry's envelope.
    ///
    /// This is a fast approximate filter -- candidates may not actually
    /// satisfy exact topological relations.
    pub fn query_candidates(&mut self, query_geom: &Value) -> Vec<String> {
        let geom = match value_to_geometry(query_geom) {
            Some(g) => g,
            None => return Vec::new(),
        };

        self.ensure_tree_built();

        let env = match geometry_to_aabb(&geom) {
            Some(e) => e,
            None => return Vec::new(),
        };

        self.tree
            .locate_in_envelope_intersecting(&env)
            .map(|entry| entry.id.clone())
            .collect()
    }

    /// Returns geometry IDs that satisfy a GeoSPARQL boolean relation
    /// against the query geometry.
    ///
    /// Uses envelope-based candidate pruning first, then exact evaluation.
    /// For disjoint-family functions, a full scan is performed (since
    /// envelope intersection cannot prune disjoint candidates).
    pub fn query_matches(&mut self, function_name: &str, query_geom: &Value) -> Vec<String> {
        if self.geometries.is_empty() {
            return Vec::new();
        }

        let local = normalize_function_local_name(function_name);
        let is_disjoint = DISJOINT_FUNCTIONS.iter().any(|d| *d == local);

        let candidates: Vec<String> = if is_disjoint {
            // Full scan for disjoint functions
            self.geometries.keys().cloned().collect()
        } else {
            self.query_candidates(query_geom)
        };

        let canonical = vocabulary::canonical_function_name(function_name);
        let mut results = Vec::new();

        for id in &candidates {
            if let Some(indexed) = self.geometries.get(id) {
                let indexed_val = crate::geometry::wkt_literal(&indexed.wkt_literal);
                let eval_result =
                    functions::evaluate_function(&canonical, &[&indexed_val, query_geom]);
                if matches!(eval_result, Some(Value::Boolean(true))) {
                    results.push(id.clone());
                }
            }
        }

        results
    }

    /// Returns a snapshot of all indexed geometry literals keyed by ID.
    pub fn snapshot_literals(&self) -> HashMap<String, String> {
        self.geometries
            .iter()
            .map(|(id, ig)| (id.clone(), ig.wkt_literal.clone()))
            .collect()
    }

    // ─── Private helpers ─────────────────────────────────────────────────

    fn ensure_tree_built(&mut self) {
        if !self.dirty {
            return;
        }

        let entries: Vec<IndexEntry> = self
            .geometries
            .values()
            .filter_map(|ig| {
                geometry_to_aabb(&ig.geometry).map(|envelope| IndexEntry {
                    id: ig.id.clone(),
                    envelope,
                })
            })
            .collect();

        self.tree = RTree::bulk_load(entries);
        self.dirty = false;
    }
}

// ─── Helper functions ────────────────────────────────────────────────────────

fn geometry_to_aabb(geom: &Geometry<f64>) -> Option<AABB<[f64; 2]>> {
    let rect = geom.bounding_rect()?;
    Some(AABB::from_corners(
        [rect.min().x, rect.min().y],
        [rect.max().x, rect.max().y],
    ))
}

fn normalize_function_local_name(function_name: &str) -> String {
    let lower = function_name.to_lowercase();
    if let Some(pos) = lower.rfind(':') {
        lower[pos + 1..].to_string()
    } else {
        lower
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::wkt_literal;

    fn make_index_with_points() -> GeoSpatialIndex {
        let mut idx = GeoSpatialIndex::new();
        idx.put("p1", &wkt_literal("POINT(0.5 0.5)"));
        idx.put("p2", &wkt_literal("POINT(0.8 0.8)"));
        idx.put("p3", &wkt_literal("POINT(5 5)"));
        idx
    }

    #[test]
    fn test_put_and_size() {
        let idx = make_index_with_points();
        assert_eq!(idx.size(), 3);
    }

    #[test]
    fn test_put_invalid() {
        let mut idx = GeoSpatialIndex::new();
        assert!(!idx.put("", &wkt_literal("POINT(1 1)")));
        assert!(!idx.put("x", &Value::Null));
        assert!(!idx.put("x", &Value::from("NOT_GEOMETRY")));
        assert_eq!(idx.size(), 0);
    }

    #[test]
    fn test_remove() {
        let mut idx = make_index_with_points();
        assert!(idx.remove("p1"));
        assert!(!idx.remove("nonexistent"));
        assert_eq!(idx.size(), 2);
    }

    #[test]
    fn test_clear() {
        let mut idx = make_index_with_points();
        idx.clear();
        assert_eq!(idx.size(), 0);
    }

    #[test]
    fn test_query_candidates_by_envelope() {
        let mut idx = make_index_with_points();
        let query = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let candidates = idx.query_candidates(&query);
        // p1 and p2 are inside the polygon envelope, p3 is outside
        assert!(candidates.contains(&"p1".to_string()));
        assert!(candidates.contains(&"p2".to_string()));
        assert!(!candidates.contains(&"p3".to_string()));
    }

    #[test]
    fn test_query_matches_intersects() {
        let mut idx = make_index_with_points();
        let query = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let matches = idx.query_matches("geof:sfIntersects", &query);
        assert!(matches.contains(&"p1".to_string()));
        assert!(matches.contains(&"p2".to_string()));
        assert!(!matches.contains(&"p3".to_string()));
    }

    #[test]
    fn test_query_matches_disjoint_scans_all() {
        let mut idx = make_index_with_points();
        let query = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let matches = idx.query_matches("geof:sfDisjoint", &query);
        // p3 is disjoint from the polygon; p1, p2 are not
        assert!(matches.contains(&"p3".to_string()));
        assert!(!matches.contains(&"p1".to_string()));
        assert!(!matches.contains(&"p2".to_string()));
    }

    #[test]
    fn test_snapshot_literals() {
        let idx = make_index_with_points();
        let snap = idx.snapshot_literals();
        assert_eq!(snap.len(), 3);
        assert!(snap.contains_key("p1"));
    }

    #[test]
    fn test_query_matches_sf_within() {
        let mut idx = make_index_with_points();
        let query = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let matches = idx.query_matches("geof:sfWithin", &query);
        // p1 (0.5,0.5) and p2 (0.8,0.8) are within the polygon; p3 (5,5) is not
        assert!(matches.contains(&"p1".to_string()));
        assert!(matches.contains(&"p2".to_string()));
        assert!(!matches.contains(&"p3".to_string()));
    }

    #[test]
    fn test_query_matches_sf_contains() {
        let mut idx = GeoSpatialIndex::new();
        idx.put(
            "poly",
            &wkt_literal("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
        );
        idx.put("small", &wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));
        let query = wkt_literal("POINT(0.5 0.5)");
        let matches = idx.query_matches("geof:sfContains", &query);
        // sfContains(indexed_polygon, query_point) = true for both polygons
        assert!(matches.contains(&"poly".to_string()));
        assert!(matches.contains(&"small".to_string()));
    }

    #[test]
    fn test_crs_and_ewkt_parsing() {
        let mut idx = GeoSpatialIndex::new();
        assert!(idx.put(
            "crs",
            &wkt_literal("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(0.5 0.5)")
        ));
        assert!(idx.put("ewkt", &wkt_literal("SRID=4326;POINT(0.75 0.75)")));
        assert_eq!(idx.size(), 2);
    }

    #[test]
    fn test_put_replaces_existing() {
        let mut idx = GeoSpatialIndex::new();
        idx.put("p1", &wkt_literal("POINT(1 1)"));
        idx.put("p1", &wkt_literal("POINT(2 2)"));
        assert_eq!(idx.size(), 1);
        let snap = idx.snapshot_literals();
        assert!(snap["p1"].contains("2 2"));
    }

    #[test]
    fn test_query_candidates_empty_index() {
        let mut idx = GeoSpatialIndex::new();
        let query = wkt_literal("POINT(1 1)");
        let candidates = idx.query_candidates(&query);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_query_candidates_invalid_query_geom() {
        let mut idx = make_index_with_points();
        let candidates = idx.query_candidates(&Value::Null);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_query_matches_empty_index() {
        let mut idx = GeoSpatialIndex::new();
        let query = wkt_literal("POINT(1 1)");
        let matches = idx.query_matches("geof:sfIntersects", &query);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_query_matches_sf_equals() {
        let mut idx = GeoSpatialIndex::new();
        idx.put("p1", &wkt_literal("POINT(1 1)"));
        idx.put("p2", &wkt_literal("POINT(2 2)"));
        let query = wkt_literal("POINT(1 1)");
        let matches = idx.query_matches("geof:sfEquals", &query);
        assert!(matches.contains(&"p1".to_string()));
        assert!(!matches.contains(&"p2".to_string()));
    }

    #[test]
    fn test_linestring_geometry() {
        let mut idx = GeoSpatialIndex::new();
        assert!(idx.put("line", &wkt_literal("LINESTRING(0 0, 1 1, 2 0)")));
        assert_eq!(idx.size(), 1);
        let query = wkt_literal("POINT(0.5 0.5)");
        let candidates = idx.query_candidates(&query);
        assert!(candidates.contains(&"line".to_string()));
    }

    #[test]
    fn test_polygon_overlap_candidates() {
        let mut idx = GeoSpatialIndex::new();
        idx.put("poly1", &wkt_literal("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"));
        idx.put("poly2", &wkt_literal("POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))"));
        idx.put(
            "poly3",
            &wkt_literal("POLYGON((10 10, 12 10, 12 12, 10 12, 10 10))"),
        );
        let query = wkt_literal("POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))");
        let candidates = idx.query_candidates(&query);
        // poly1 and poly2 overlap the query envelope, poly3 does not
        assert!(candidates.contains(&"poly1".to_string()));
        assert!(candidates.contains(&"poly2".to_string()));
        assert!(!candidates.contains(&"poly3".to_string()));
    }

    #[test]
    fn test_remove_then_query() {
        let mut idx = make_index_with_points();
        idx.remove("p1");
        idx.remove("p2");
        let query = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
        let matches = idx.query_matches("geof:sfIntersects", &query);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_default_trait() {
        let idx = GeoSpatialIndex::default();
        assert_eq!(idx.size(), 0);
    }

    #[test]
    fn test_normalize_function_local_name() {
        assert_eq!(normalize_function_local_name("geof:sfWithin"), "sfwithin");
        // Full IRIs use rfind(':') which finds the last colon
        assert_eq!(normalize_function_local_name("sfWithin"), "sfwithin");
        assert_eq!(
            normalize_function_local_name("GEOF:sfDisjoint"),
            "sfdisjoint"
        );
    }
}
