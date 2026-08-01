use cqels_core::stream::RdfStreamElement;
use cqels_model::term::IriTerm;
use cqels_model::{Statement, Term};
use cqels_reasoning::{ReasoningProfile, ReteNetwork};

fn iri(value: &str) -> Term {
    Term::Iri(IriTerm::new(value))
}

fn main() {
    let profile = ReasoningProfile::Rdfs;
    let mut network = ReteNetwork::compile(profile.create_config());

    let subclass = Statement::new(
        iri("http://example.org/Person"),
        IriTerm::new("http://www.w3.org/2000/01/rdf-schema#subClassOf"),
        iri("http://example.org/Agent"),
    );
    let instance = Statement::new(
        iri("http://example.org/alice"),
        IriTerm::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        iri("http://example.org/Person"),
    );

    network.process_element(&RdfStreamElement::new(subclass, 1_000));
    let inferred = network.process_element(&RdfStreamElement::new(instance, 2_000));
    println!("{} inferred {} fact(s)", profile, inferred.len());
    for fact in inferred {
        println!("  {}", fact.statement);
    }
}
