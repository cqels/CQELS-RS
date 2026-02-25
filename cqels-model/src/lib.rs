//! RDF data model types for the CQELS streaming engine.
//!
//! This crate provides the foundational RDF (Resource Description Framework)
//! types used throughout the CQELS engine. It is a Rust port of the data model
//! layer from [CQELS 2.0](https://cqels.github.io/) (Continuous Query Evaluation
//! over Linked Streams).
//!
//! # Key Types
//!
//! - [`Term`] -- An RDF term: IRI, blank node, or literal.
//! - [`Statement`] -- An RDF triple (subject, predicate, object) with optional
//!   named graph, forming the atomic unit of RDF data.
//! - [`Value`] -- A dynamically typed runtime value used in query evaluation.
//! - [`BindingSet`] -- A set of variable-to-value bindings produced by query
//!   pattern matching.
//! - [`CqelsError`] -- Unified error type for the CQELS engine.
//!
//! # Interoperability
//!
//! The types in this crate provide bidirectional conversions with the
//! [`oxrdf`](https://docs.rs/oxrdf) crate, allowing seamless integration with
//! the wider Rust RDF ecosystem.
//!
//! # Examples
//!
//! ```rust
//! use cqels_model::{Term, Statement};
//! use cqels_model::term::{IriTerm, LiteralTerm};
//!
//! // Build an RDF triple: <http://sensor/1> <http://example.org/value> "42.0"
//! let subject = Term::Iri(IriTerm::new("http://sensor/1"));
//! let predicate = IriTerm::new("http://example.org/value");
//! let object = Term::Literal(
//!     LiteralTerm::new("42.0")
//!         .with_datatype("http://www.w3.org/2001/XMLSchema#double"),
//! );
//! let stmt = Statement::new(subject, predicate, object);
//! assert!(!stmt.is_quad());
//! ```

pub mod error;
pub mod term;
pub mod statement;
pub mod value;
pub mod binding;

pub use error::{CqelsError, CqelsResult, ParseErrorDetail, ParseErrorKind};
pub use term::Term;
pub use statement::Statement;
pub use value::Value;
pub use binding::BindingSet;
