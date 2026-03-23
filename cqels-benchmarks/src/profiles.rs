//! Benchmark scenario definitions for reasoning profiles.
//!
//! Provides pre-built scenarios pairing a [`ReasoningProfile`] with
//! schema data suitable for benchmarking profile-specific inference.

use cqels_model::statement::Statement;
use cqels_model::term::{IriTerm, Term};
use cqels_reasoning::ReasoningProfile;

/// A benchmark scenario: a profile and its associated schema triples.
pub struct ProfileScenario {
    /// The reasoning profile.
    pub profile: ReasoningProfile,
    /// Schema triples that define the ontology (subclass hierarchy, etc.).
    pub schema: Vec<Statement>,
    /// Description of the scenario.
    pub description: &'static str,
}

/// Returns RDFS benchmark scenario with a subclass/subproperty hierarchy.
pub fn rdfs_scenario() -> ProfileScenario {
    let rdfs = "http://www.w3.org/2000/01/rdf-schema#";
    let ex = "http://example.org/";

    let mut schema = Vec::new();

    // Class hierarchy: A < B < C < D < E
    for (sub, sup) in &[("A", "B"), ("B", "C"), ("C", "D"), ("D", "E")] {
        schema.push(Statement::new(
            Term::Iri(IriTerm::new(format!("{ex}{sub}"))),
            IriTerm::new(format!("{rdfs}subClassOf")),
            Term::Iri(IriTerm::new(format!("{ex}{sup}"))),
        ));
    }

    // Property hierarchy: p1 < p2 < p3
    for (sub, sup) in &[("p1", "p2"), ("p2", "p3")] {
        schema.push(Statement::new(
            Term::Iri(IriTerm::new(format!("{ex}{sub}"))),
            IriTerm::new(format!("{rdfs}subPropertyOf")),
            Term::Iri(IriTerm::new(format!("{ex}{sup}"))),
        ));
    }

    // Domain/range
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}p1"))),
        IriTerm::new(format!("{rdfs}domain")),
        Term::Iri(IriTerm::new(format!("{ex}A"))),
    ));
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}p1"))),
        IriTerm::new(format!("{rdfs}range")),
        Term::Iri(IriTerm::new(format!("{ex}B"))),
    ));

    ProfileScenario {
        profile: ReasoningProfile::Rdfs,
        schema,
        description: "RDFS scenario with 4-level class/property hierarchies and domain/range",
    }
}

/// Returns OWL-Lite benchmark scenario with transitive/symmetric properties.
pub fn owl_lite_scenario() -> ProfileScenario {
    let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    let owl = "http://www.w3.org/2002/07/owl#";
    let ex = "http://example.org/";

    let mut schema = rdfs_scenario().schema;

    // Mark knows as TransitiveProperty
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}knows"))),
        IriTerm::new(format!("{rdf}type")),
        Term::Iri(IriTerm::new(format!("{owl}TransitiveProperty"))),
    ));

    // Mark friendOf as SymmetricProperty
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}friendOf"))),
        IriTerm::new(format!("{rdf}type")),
        Term::Iri(IriTerm::new(format!("{owl}SymmetricProperty"))),
    ));

    // inverseOf relationship
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}parentOf"))),
        IriTerm::new(format!("{owl}inverseOf")),
        Term::Iri(IriTerm::new(format!("{ex}childOf"))),
    ));

    ProfileScenario {
        profile: ReasoningProfile::OwlLite,
        schema,
        description: "OWL-Lite scenario with transitive, symmetric, and inverse properties",
    }
}

/// Returns OWL 2 RL benchmark scenario with hasValue restrictions.
pub fn owl2_rl_scenario() -> ProfileScenario {
    let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    let owl = "http://www.w3.org/2002/07/owl#";
    let ex = "http://example.org/";

    let mut schema = owl_lite_scenario().schema;

    // HasValue restriction: PremiumCustomer owl:onProperty status, owl:hasValue "premium"
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}PremiumCustomer"))),
        IriTerm::new(format!("{owl}onProperty")),
        Term::Iri(IriTerm::new(format!("{ex}status"))),
    ));
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}PremiumCustomer"))),
        IriTerm::new(format!("{owl}hasValue")),
        Term::Iri(IriTerm::new(format!("{ex}PremiumStatus"))),
    ));

    // ReflexiveProperty
    schema.push(Statement::new(
        Term::Iri(IriTerm::new(format!("{ex}relatedTo"))),
        IriTerm::new(format!("{rdf}type")),
        Term::Iri(IriTerm::new(format!("{owl}ReflexiveProperty"))),
    ));

    ProfileScenario {
        profile: ReasoningProfile::Owl2Rl,
        schema,
        description: "OWL 2 RL scenario with hasValue restrictions and reflexive properties",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdfs_scenario() {
        let s = rdfs_scenario();
        assert_eq!(s.profile, ReasoningProfile::Rdfs);
        assert!(!s.schema.is_empty());
    }

    #[test]
    fn test_owl_lite_scenario() {
        let s = owl_lite_scenario();
        assert_eq!(s.profile, ReasoningProfile::OwlLite);
        assert!(s.schema.len() > rdfs_scenario().schema.len());
    }

    #[test]
    fn test_owl2_rl_scenario() {
        let s = owl2_rl_scenario();
        assert_eq!(s.profile, ReasoningProfile::Owl2Rl);
    }
}
