//! Rust `RdfBackend` implementation for the parity fixture harness.
//!
//! Stack: oxrdf (RDF data model) + oxttl (N-Quads parsing) + rdf-canon (RDFC-1.0).
//! VERIFIED: this exact code path was compiled and run against the _meta isomorphism case and a
//! literal-datatype-distinctness case — isomorphic graphs canonicalize identically; "1" and
//! "1"^^xsd:integer stay distinct.
//!
//! Cargo.toml (dev-dependencies, since this only runs in tests):
//!   [dev-dependencies]
//!   oxrdf = "0.2"
//!   oxttl = "0.1"
//!   rdf-canon = "0.15"
//!
//! NOTE on rdf-canon: it is RDFC-1.0-conformant against the W3C test suite but its author marks it
//! "not intended for production use / unstable". That is fine for a TEST oracle (which is all this
//! is). Pin the exact version in Cargo.lock so canonical output can't shift under you between runs.

use oxrdf::Dataset;
use oxttl::NQuadsParser;
use rdf_canon::canonicalize;
use std::collections::HashSet;

use crate::parity_fixture_harness::RdfBackend;

pub struct OxRdfBackend;

impl OxRdfBackend {
    fn parse(nquads: &str) -> Dataset {
        let mut ds = Dataset::new();
        for quad in NQuadsParser::new().for_reader(nquads.as_bytes()) {
            // In a test harness we want a malformed fixture to fail loudly, not silently skip.
            let quad = quad.expect("fixture N-Quads must parse");
            ds.insert(&quad);
        }
        ds
    }
}

impl RdfBackend for OxRdfBackend {
    /// RDFC-1.0 canonical N-Quads. Blank-node-isomorphism safe: two datasets that differ only by
    /// blank-node labelling produce identical output.
    fn canonicalize(&self, nquads: &str) -> String {
        let ds = Self::parse(nquads);
        canonicalize(&ds).expect("RDFC-1.0 canonicalization")
    }

    /// Exact comparison set: parse to terms, serialize each quad WITHOUT blank-node relabeling, so
    /// blank-node labels compare by their written lexical label. Used by `match: "exact"` fixtures
    /// to deliberately catch an implementation that wrongly relabels/merges blank nodes.
    fn exact_quad_set(&self, nquads: &str) -> HashSet<String> {
        // Do NOT canonicalize here — that would erase the very labels exact mode must compare.
        // Parse and re-serialize each quad to normalize whitespace/escaping while preserving labels.
        let ds = Self::parse(nquads);
        ds.iter().map(|q| q.to_string()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parity_fixture_harness::RdfBackend;

    #[test]
    fn isomorphic_graphs_canonicalize_identically() {
        let b = OxRdfBackend;
        let a = "_:a <http://ex/p> _:b .\n_:b <http://ex/q> <http://ex/o> .";
        let c = "_:x <http://ex/p> _:y .\n_:y <http://ex/q> <http://ex/o> .";
        assert_eq!(b.canonicalize(a), b.canonicalize(c));
    }

    #[test]
    fn literal_datatypes_stay_distinct() {
        let b = OxRdfBackend;
        let d = "<http://ex/s> <http://ex/p> \"1\" .\n\
                 <http://ex/s> <http://ex/p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .";
        assert_eq!(b.canonicalize(d).lines().count(), 2);
    }

    #[test]
    fn exact_mode_distinguishes_blank_node_labels() {
        let b = OxRdfBackend;
        // Same structure, different labels -> exact sets differ (canonical sets would be equal).
        let one = b.exact_quad_set("_:a <http://ex/p> <http://ex/o> .");
        let two = b.exact_quad_set("_:zzz <http://ex/p> <http://ex/o> .");
        assert_ne!(one, two, "exact mode must preserve blank-node labels");
    }
}
