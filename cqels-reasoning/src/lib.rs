//! RETE-based forward-chaining incremental reasoning for CQELS.
//!
//! This crate implements a RETE network adapted for streaming RDF data,
//! enabling real-time rule-based inference over continuously arriving
//! triples. It is designed as an add-on reasoning layer for the CQELS
//! engine, inspired by the classic RETE algorithm with adaptations for
//! stream processing semantics.
//!
//! # Architecture
//!
//! The reasoning pipeline follows the RETE pattern-matching architecture:
//!
//! 1. **Alpha network** ([`AlphaNetwork`]) -- Filters individual RDF triples
//!    against single-triple conditions in rules.
//! 2. **Beta network** ([`BetaNetwork`]) -- Joins partial matches across
//!    multiple triple patterns using shared variable bindings.
//! 3. **Working memory** ([`WorkingMemory`]) -- Indexed storage of currently
//!    active facts, supporting efficient lookup by subject, predicate, and
//!    object positions.
//! 4. **Conflict resolution** ([`ConflictResolver`]) -- Selects which
//!    activated rules to fire when multiple rules match simultaneously.
//! 5. **Production** ([`Activation`]) -- Represents a fully matched rule
//!    ready to produce new inferred triples.
//!
//! # Key Types
//!
//! - [`ReteNetwork`] -- The top-level reasoning engine that accepts RDF
//!   stream elements and emits [`InferredRdfStreamElement`] values with
//!   provenance metadata (which rule fired, which facts triggered it).
//! - [`Rule`] / [`RuleSet`] -- Definitions of if-then rules with triple
//!   pattern conditions and triple template consequents.
//! - [`TriplePattern`] -- A pattern that may contain variables
//!   ([`PatternTerm`]) for matching against RDF triples.
//! - [`ReasoningConfig`] -- Configuration for the reasoning engine
//!   (e.g., conflict resolution strategy, truth maintenance).
//!
//! # Examples
//!
//! ```rust
//! use cqels_reasoning::{TriplePattern, PatternTerm};
//! use cqels_model::term::{IriTerm, Term};
//!
//! // A pattern matching any triple with predicate rdf:type
//! let pattern = TriplePattern::new(
//!     PatternTerm::Variable("x".into()),
//!     PatternTerm::Constant(Term::Iri(IriTerm::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))),
//!     PatternTerm::Variable("cls".into()),
//! );
//! ```

mod pattern;
mod rule;
mod alpha;
mod beta;
mod memory;
mod production;
mod conflict;
mod network;
mod config;
pub mod stream_adapter;

pub use pattern::{PatternTerm, TriplePattern, TripleTemplate};
pub use rule::{Rule, RuleCondition, RuleConsequent, RuleSet};
pub use alpha::{AlphaMatch, AlphaNetwork, AlphaNode};
pub use beta::{BetaNetwork, BetaNode};
pub use memory::{FactIndex, WorkingMemory};
pub use production::Activation;
pub use conflict::{ConflictResolution, ConflictResolver};
pub use network::{InferredRdfStreamElement, ReteNetwork};
pub use config::ReasoningConfig;
pub use stream_adapter::ReteStreamOperator;
