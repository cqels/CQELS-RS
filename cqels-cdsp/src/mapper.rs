//! Mapping VSS signals to RDF stream elements.
//!
//! A [`VssSignalMapper`] turns a [`VssSignal`] into a single
//! [`RdfStreamElement`] whose statement has:
//!
//! - subject = the configured vehicle IRI (one mapper instance per
//!   vehicle is the expected usage)
//! - predicate = an IRI derived from the signal path via the active
//!   [`PredicateStrategy`]
//! - object = a literal containing the rendered value
//! - timestamp = the signal's timestamp
//!
//! This is intentionally a one-to-one mapping. Richer encodings
//! (typed literals, reified observations, etc.) belong in user code.

use cqels_core::stream::RdfStreamElement;
use cqels_model::{IriTerm, LiteralTerm, Statement, Term};

use crate::signal::VssSignal;

/// Strategy for turning a dotted VSS path into a predicate IRI.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PredicateStrategy {
    /// Prefix the path with the supplied base IRI verbatim, e.g.
    /// `"http://covesa.org/vss#"` + `"Vehicle.Speed"` →
    /// `"http://covesa.org/vss#Vehicle.Speed"`.
    PrefixedPath { base: String },
    /// Prefix the path with the supplied base IRI and replace dots
    /// with slashes, e.g. `"http://covesa.org/vss/"` +
    /// `"Vehicle.Speed"` → `"http://covesa.org/vss/Vehicle/Speed"`.
    Slashed { base: String },
}

impl PredicateStrategy {
    fn build_iri(&self, path: &str) -> String {
        match self {
            PredicateStrategy::PrefixedPath { base } => format!("{base}{path}"),
            PredicateStrategy::Slashed { base } => {
                format!("{base}{}", path.replace('.', "/"))
            }
        }
    }
}

/// Maps [`VssSignal`]s onto [`RdfStreamElement`]s for the configured
/// vehicle subject.
#[derive(Clone, Debug)]
pub struct VssSignalMapper {
    vehicle_iri: String,
    strategy: PredicateStrategy,
}

impl VssSignalMapper {
    /// Constructs a mapper with the default `PrefixedPath` strategy
    /// using `"http://covesa.org/vss#"` as the base predicate IRI.
    pub fn new(vehicle_iri: impl Into<String>) -> Self {
        Self {
            vehicle_iri: vehicle_iri.into(),
            strategy: PredicateStrategy::PrefixedPath {
                base: "http://covesa.org/vss#".to_string(),
            },
        }
    }

    /// Overrides the predicate strategy.
    pub fn with_strategy(mut self, strategy: PredicateStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Returns the vehicle subject IRI.
    pub fn vehicle_iri(&self) -> &str {
        &self.vehicle_iri
    }

    /// Maps `signal` into a single [`RdfStreamElement`].
    pub fn to_rdf_stream_element(&self, signal: &VssSignal) -> RdfStreamElement {
        let subject = Term::Iri(IriTerm::new(&self.vehicle_iri));
        let predicate = IriTerm::new(self.strategy.build_iri(&signal.path));
        let object = Term::Literal(LiteralTerm::new(signal.value.as_literal_str()));
        RdfStreamElement::new(Statement::new(subject, predicate, object), signal.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::VssValue;

    fn sample_signal() -> VssSignal {
        VssSignal::new("Vehicle.Speed", VssValue::from(72.0), 1_700_000_000_000)
    }

    #[test]
    fn default_strategy_uses_prefixed_path() {
        let mapper = VssSignalMapper::new("urn:vehicle:abc");
        let element = mapper.to_rdf_stream_element(&sample_signal());
        assert_eq!(element.timestamp, 1_700_000_000_000);
        assert_eq!(
            element.statement.predicate.as_str(),
            "http://covesa.org/vss#Vehicle.Speed"
        );
    }

    #[test]
    fn vehicle_iri_is_used_as_subject() {
        let mapper = VssSignalMapper::new("urn:vehicle:abc");
        let element = mapper.to_rdf_stream_element(&sample_signal());
        match &element.statement.subject {
            Term::Iri(i) => assert_eq!(i.as_str(), "urn:vehicle:abc"),
            other => panic!("expected IRI subject, got {other:?}"),
        }
    }

    #[test]
    fn slashed_strategy_converts_dots_to_slashes() {
        let mapper =
            VssSignalMapper::new("urn:vehicle:abc").with_strategy(PredicateStrategy::Slashed {
                base: "http://covesa.org/vss/".into(),
            });
        let element = mapper.to_rdf_stream_element(&VssSignal::new(
            "Vehicle.Powertrain.FuelSystem.Level",
            VssValue::from(0.42),
            0,
        ));
        assert_eq!(
            element.statement.predicate.as_str(),
            "http://covesa.org/vss/Vehicle/Powertrain/FuelSystem/Level"
        );
    }

    #[test]
    fn object_renders_value_as_literal_string() {
        let mapper = VssSignalMapper::new("urn:vehicle:abc");
        let element =
            mapper.to_rdf_stream_element(&VssSignal::new("Vehicle.IsOn", VssValue::Bool(true), 0));
        match &element.statement.object {
            Term::Literal(l) => assert_eq!(l.value(), "true"),
            other => panic!("expected literal object, got {other:?}"),
        }
    }

    #[test]
    fn mapper_preserves_timestamp() {
        let mapper = VssSignalMapper::new("urn:vehicle:abc");
        let element = mapper.to_rdf_stream_element(&VssSignal::new(
            "Vehicle.Speed",
            VssValue::from(0_i64),
            42,
        ));
        assert_eq!(element.timestamp, 42);
    }
}
