//! GeoSPARQL spatial computing and reasoning for CQELS.
//!
//! This crate provides spatial primitives for the CQELS streaming engine,
//! implementing core GeoSPARQL functionality: geometry parsing, spatial
//! functions, R-tree indexing, and rule-based spatial reasoning. It is a
//! Rust port of the geo layer from [CQELS 2.0](https://cqels.github.io/).
//!
//! # Modules
//!
//! - [`vocabulary`] -- GeoSPARQL namespace constants, prefix resolution, and
//!   canonical function name normalization.
//! - [`geometry`] -- WKT parsing (plain, CRS-prefixed, EWKT) and conversion
//!   between `cqels_model::Value` and `geo::Geometry`.
//! - [`functions`] -- GeoSPARQL function implementations: distance, sfWithin,
//!   sfContains, sfIntersects, sfDisjoint, sfEquals, and generic dispatch.
//! - [`index`] -- R*-tree spatial index for envelope-based candidate pruning
//!   and exact topological matching.
//! - [`reasoning`] -- [`GeoReasoner`] trait and [`GeoSparqlSimpleReasoner`]
//!   for symmetry/inverse spatial inference.
//!
//! # Key Types
//!
//! - [`GeoSpatialIndex`] -- Spatial index storing WKT geometries by ID,
//!   supporting envelope queries and exact relation matching.
//! - [`GeoReasoner`] -- Trait for spatial reasoning modules.
//! - [`GeoSparqlSimpleReasoner`] -- Baseline reasoner inferring symmetric
//!   and inverse topological relations.
//!
//! # Examples
//!
//! ```rust
//! use cqels_geo::geometry::wkt_literal;
//! use cqels_geo::functions;
//! use cqels_model::Value;
//!
//! // Test if a point is within a polygon
//! let point = wkt_literal("POINT(0.5 0.5)");
//! let polygon = wkt_literal("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
//!
//! let result = functions::sf_within(&point, &polygon);
//! assert_eq!(result, Some(Value::Boolean(true)));
//! ```

pub mod functions;
pub mod geometry;
pub mod index;
pub mod reasoning;
pub mod vocabulary;

pub use index::GeoSpatialIndex;
pub use reasoning::{GeoReasoner, GeoSparqlSimpleReasoner};
