//! Extended RDF stream data generators for profile-aware benchmarks.
//!
//! Generates stream data that exercises specific reasoning profile features,
//! such as subclass instances, property chains, transitive links, etc.

use cqels_core::stream::RdfStreamElement;
use cqels_model::statement::Statement;
use cqels_model::term::{IriTerm, LiteralTerm, Term};
use rand::Rng;

/// Generates RDF stream elements that exercise RDFS subclass inference.
///
/// Produces `rdf:type` triples for instances of classes at various levels
/// of the hierarchy, along with property usage triples.
pub fn generate_rdfs_stream(count: usize, num_classes: usize) -> Vec<RdfStreamElement> {
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let ex = "http://example.org/";
    let mut rng = rand::thread_rng();
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let instance = format!("{ex}instance_{i}");
        let class_idx = rng.gen_range(0..num_classes);
        let class = format!("{ex}Class_{class_idx}");

        let stmt = Statement::new(
            Term::Iri(IriTerm::new(&instance)),
            IriTerm::new(rdf_type),
            Term::Iri(IriTerm::new(&class)),
        );
        let ts = (i as i64) * 10;
        elements.push(RdfStreamElement::new(stmt, ts));
    }

    elements
}

/// Generates RDF stream elements with property-value triples using
/// properties from a hierarchy, suitable for subproperty propagation benchmarks.
pub fn generate_property_stream(count: usize, num_properties: usize) -> Vec<RdfStreamElement> {
    let ex = "http://example.org/";
    let mut rng = rand::thread_rng();
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let subject = format!("{ex}entity_{}", i % 200);
        let prop_idx = rng.gen_range(0..num_properties);
        let property = format!("{ex}prop_{prop_idx}");
        let value: f64 = rng.gen_range(0.0..1000.0);

        let stmt = Statement::new(
            Term::Iri(IriTerm::new(&subject)),
            IriTerm::new(&property),
            Term::Literal(
                LiteralTerm::new(format!("{value:.2}"))
                    .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
            ),
        );
        let ts = (i as i64) * 10;
        elements.push(RdfStreamElement::new(stmt, ts));
    }

    elements
}

/// Generates transitive relationship triples (e.g., `:knows` chains)
/// for OWL transitive property benchmarks.
pub fn generate_transitive_chain(count: usize, chain_length: usize) -> Vec<RdfStreamElement> {
    let ex = "http://example.org/";
    let mut elements = Vec::with_capacity(count);
    let chains = count / chain_length.max(1);

    for chain in 0..chains {
        for link in 0..chain_length {
            let subject = format!("{ex}node_{}_{link}", chain);
            let object = format!("{ex}node_{}_{}", chain, link + 1);

            let stmt = Statement::new(
                Term::Iri(IriTerm::new(&subject)),
                IriTerm::new(format!("{ex}knows")),
                Term::Iri(IriTerm::new(&object)),
            );
            let ts = (chain * chain_length + link) as i64 * 10;
            elements.push(RdfStreamElement::new(stmt, ts));

            if elements.len() >= count {
                return elements;
            }
        }
    }

    elements
}

/// Generates symmetric relationship triples for OWL symmetric property benchmarks.
pub fn generate_symmetric_relations(count: usize) -> Vec<RdfStreamElement> {
    let ex = "http://example.org/";
    let mut rng = rand::thread_rng();
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let a = rng.gen_range(0..100);
        let b = rng.gen_range(0..100);

        let stmt = Statement::new(
            Term::Iri(IriTerm::new(format!("{ex}person_{a}"))),
            IriTerm::new(format!("{ex}friendOf")),
            Term::Iri(IriTerm::new(format!("{ex}person_{b}"))),
        );
        let ts = (i as i64) * 10;
        elements.push(RdfStreamElement::new(stmt, ts));
    }

    elements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_rdfs_stream() {
        let elems = generate_rdfs_stream(100, 5);
        assert_eq!(elems.len(), 100);
    }

    #[test]
    fn test_generate_property_stream() {
        let elems = generate_property_stream(50, 3);
        assert_eq!(elems.len(), 50);
    }

    #[test]
    fn test_generate_transitive_chain() {
        let elems = generate_transitive_chain(100, 10);
        assert_eq!(elems.len(), 100);
    }

    #[test]
    fn test_generate_symmetric_relations() {
        let elems = generate_symmetric_relations(75);
        assert_eq!(elems.len(), 75);
    }
}
