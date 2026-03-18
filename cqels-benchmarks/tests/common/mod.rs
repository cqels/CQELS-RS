#![allow(dead_code, unused_imports)]

use std::sync::Arc;

pub use futures::StreamExt;
pub use std::time::Duration;

pub use cqels_benchmarks::{generate_sensor_readings, generate_social_events};
pub use cqels_core::compiler::pipeline::{
    apply_group_by_aggregates, match_cypher_pattern, PipelineAggregateSpec,
};
pub use cqels_core::compiler::{CqelsQueryCompiler, CypherQueryCompiler};
pub use cqels_core::expression::ast::{AggregateExprFunction, Expression};
pub use cqels_core::expression::evaluator::ExpressionEvaluator;
pub use cqels_core::operator::aggregate::{
    AggregateFunction, AvgAggregate, CountAggregate, GroupKey, MaxAggregate, MinAggregate,
    SumAggregate, WindowedAggregateOperator,
};
pub use cqels_core::operator::bind::BindOperator;
pub use cqels_core::operator::filter::FilterOperator;
pub use cqels_core::operator::join::{GraphPatternJoinState, VariableLengthPathOperator};
pub use cqels_core::operator::ranking::{SortDirection, TopKOperator};
pub use cqels_core::parser::ast::{CypherPattern, NodePattern, RelDirection, RelationshipPattern};
pub use cqels_core::parser::{CqelsQlParser, CypherQlParser};
pub use cqels_core::query::{ContinuousQuery, QueryInputs};
pub use cqels_core::store::{OxigraphRdfStore, RdfStore};
pub use cqels_core::stream::{RdfStreamElement, StreamElement, Timestamped, TimestampedValue};
pub use cqels_core::window::{
    SessionWindow, SlidingWindow, TumblingCountWindow, TumblingWindow, Window,
};
pub use cqels_engine::{
    create_stream_pair, receiver_to_stream, CqelsRuntime, NfaPatternProcessor, Pattern,
    ReactiveStreamEngine, StreamEngine,
};
pub use cqels_geo::functions::{evaluate_function, sf_equals, sf_intersects};
pub use cqels_geo::geometry::wkt_literal;
pub use cqels_geo::{GeoReasoner, GeoSparqlSimpleReasoner, GeoSpatialIndex};
pub use cqels_model::term::{IriTerm, LiteralTerm, Term};
pub use cqels_model::{BindingSet, Statement, Value};
pub use cqels_reasoning::{
    PatternTerm, ReasoningConfig, ReteNetwork, ReteStreamOperator, Rule, RuleSet, TriplePattern,
    TripleTemplate,
};

pub fn iri(s: &str) -> Term {
    Term::Iri(IriTerm::new(s))
}

pub fn iri_pred(s: &str) -> IriTerm {
    IriTerm::new(s)
}

pub fn lit(s: &str) -> Term {
    Term::Literal(LiteralTerm::new(s))
}

pub fn stmt(subj: &str, pred: &str, obj: &str) -> Statement {
    Statement::new(iri(subj), iri_pred(pred), iri(obj))
}

pub fn make_rdf_element(subj: &str, pred: &str, obj: &str, ts: i64) -> RdfStreamElement {
    RdfStreamElement::new(stmt(subj, pred, obj), ts)
}

pub fn make_rdf_literal_element(
    subj: &str,
    pred: &str,
    obj_val: &str,
    ts: i64,
) -> RdfStreamElement {
    RdfStreamElement::new(Statement::new(iri(subj), iri_pred(pred), lit(obj_val)), ts)
}

pub fn term_as_str(t: &Term) -> &str {
    match t {
        Term::Iri(iri) => iri.as_str(),
        Term::Literal(lit) => lit.value(),
        Term::BlankNode(bn) => bn.id(),
        _ => "",
    }
}

pub fn stream_elem_literal(subj: &str, pred: &str, obj_val: &str, ts: i64) -> StreamElement {
    StreamElement::Rdf(RdfStreamElement::new(
        Statement::new(iri(subj), iri_pred(pred), lit(obj_val)),
        ts,
    ))
}

pub fn stream_elem_iri(subj: &str, pred: &str, obj: &str, ts: i64) -> StreamElement {
    StreamElement::Rdf(RdfStreamElement::new(stmt(subj, pred, obj), ts))
}

#[derive(Debug, Clone)]
pub struct TempEvent {
    pub sensor: String,
    pub value: f64,
    pub timestamp: i64,
}

impl Timestamped for TempEvent {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

pub fn runtime() -> CqelsRuntime {
    CqelsRuntime::new()
}

pub fn arc_store() -> Arc<OxigraphRdfStore> {
    Arc::new(OxigraphRdfStore::new().unwrap())
}

pub fn ten_second_window() -> TumblingWindow {
    TumblingWindow::new(Duration::from_secs(10))
}
