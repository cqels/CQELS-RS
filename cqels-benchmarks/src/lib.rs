//! Benchmark data generators and utilities for the CQELS streaming engine.
//!
//! This crate provides synthetic data generators that produce RDF stream
//! elements for benchmarking and testing the CQELS query engine. The
//! generators cover several common streaming scenarios:
//!
//! - **Sensor telemetry** ([`generate_sensor_readings`]) -- Temperature,
//!   humidity, and pressure readings from simulated IoT sensors.
//! - **Generic RDF batches** ([`generate_rdf_stream_batch`]) -- Simple
//!   sensor-value triples suitable for throughput benchmarks.
//! - **Social network events** ([`generate_social_events`]) -- Follows,
//!   likes, and posts relationships for graph pattern benchmarks.
//!
//! All generators produce deterministic stream layouts (sensor IDs cycle
//! through fixed ranges) with randomized values, making them suitable for
//! reproducible performance comparisons.
//!
//! # Examples
//!
//! ```rust
//! use cqels_benchmarks::{generate_rdf_stream_batch, generate_sensor_readings};
//!
//! // Generate 1000 generic RDF stream elements
//! let batch = generate_rdf_stream_batch(1000);
//! assert_eq!(batch.len(), 1000);
//!
//! // Generate 500 typed sensor readings
//! let readings = generate_sensor_readings(500);
//! assert_eq!(readings.len(), 500);
//! ```

use std::time::Duration;

use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_model::statement::Statement;
use cqels_model::term::{IriTerm, LiteralTerm, Term};
use rand::Rng;

/// Generates a batch of RDF stream elements for benchmarking.
///
/// Creates triples of the form:
///   `<http://sensor/{id}> <http://example.org/value> "{value}"^^xsd:double`
pub fn generate_rdf_stream_batch(count: usize) -> Vec<StreamElement> {
    let mut rng = rand::thread_rng();
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let subject = Term::Iri(IriTerm::new(format!("http://sensor/{}", i % 100)));
        let predicate = IriTerm::new("http://example.org/value");
        let value: f64 = rng.gen_range(0.0..100.0);
        let object = Term::Literal(
            LiteralTerm::new(format!("{value:.2}"))
                .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
        );
        let statement = Statement::new(subject, predicate, object);
        let timestamp = i as i64;
        elements.push(StreamElement::Rdf(RdfStreamElement::new(
            statement, timestamp,
        )));
    }

    elements
}

/// Generates RDF stream elements with typed sensor readings.
///
/// Creates triples across multiple predicates: temperature, humidity, pressure.
pub fn generate_sensor_readings(count: usize) -> Vec<RdfStreamElement> {
    let mut rng = rand::thread_rng();
    let predicates = [
        "http://example.org/temperature",
        "http://example.org/humidity",
        "http://example.org/pressure",
    ];
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let sensor_id = i % 50;
        let pred_idx = i % predicates.len();

        let subject = Term::Iri(IriTerm::new(format!("http://sensor/{sensor_id}")));
        let predicate = IriTerm::new(predicates[pred_idx]);
        let value: f64 = match pred_idx {
            0 => rng.gen_range(-20.0..50.0),   // temperature
            1 => rng.gen_range(0.0..100.0),    // humidity
            2 => rng.gen_range(950.0..1050.0), // pressure
            _ => 0.0,
        };
        let object = Term::Literal(
            LiteralTerm::new(format!("{value:.2}"))
                .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
        );

        let statement = Statement::new(subject, predicate, object);
        let timestamp = (i as i64) * 100; // 100ms apart
        elements.push(RdfStreamElement::new(statement, timestamp));
    }

    elements
}

/// Generates social network event triples for graph pattern benchmarks.
///
/// Creates `:follows`, `:likes`, `:posts` relationships.
pub fn generate_social_events(count: usize) -> Vec<RdfStreamElement> {
    let mut rng = rand::thread_rng();
    let predicates = [
        "http://social.org/follows",
        "http://social.org/likes",
        "http://social.org/posts",
    ];
    let mut elements = Vec::with_capacity(count);

    for i in 0..count {
        let user_a = rng.gen_range(0..200);
        let user_b = rng.gen_range(0..200);
        let pred_idx = rng.gen_range(0..predicates.len());

        let subject = Term::Iri(IriTerm::new(format!("http://user/{user_a}")));
        let predicate = IriTerm::new(predicates[pred_idx]);
        let object = Term::Iri(IriTerm::new(format!("http://user/{user_b}")));

        let statement = Statement::new(subject, predicate, object);
        let timestamp = (i as i64) * 50;
        elements.push(RdfStreamElement::new(statement, timestamp));
    }

    elements
}

/// Default window duration for benchmarks.
pub fn default_window_duration() -> Duration {
    Duration::from_secs(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_rdf_batch() {
        let batch = generate_rdf_stream_batch(100);
        assert_eq!(batch.len(), 100);
        assert!(batch[0].is_rdf());
    }

    #[test]
    fn test_generate_sensor_readings() {
        let readings = generate_sensor_readings(150);
        assert_eq!(readings.len(), 150);
    }

    #[test]
    fn test_generate_social_events() {
        let events = generate_social_events(200);
        assert_eq!(events.len(), 200);
    }
}
