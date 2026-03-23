//! Integration tests for the cqels-asp crate.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use cqels_asp::query::AspContinuousQuery;
use cqels_asp::{
    AnswerSet, AspError, AspFactMapper, AspSolver, AspStreamSolveConfig, AspTermCodec, Atom,
    IncrementalAspSession,
};
use cqels_core::query::{ContinuousQuery, QueryInputs, QueryType};
use cqels_core::stream::{RdfStreamElement, StreamElement};
use cqels_model::term::{IriTerm, LiteralTerm};
use cqels_model::{Statement, Term};

// -- Mock solver -----------------------------------------------------------

struct MockSolver {
    response: Vec<AnswerSet>,
}

#[async_trait]
impl AspSolver for MockSolver {
    async fn solve(&self, _program: &str, _max_models: usize) -> Result<Vec<AnswerSet>, AspError> {
        Ok(self.response.clone())
    }
}

fn sample_statement() -> Statement {
    Statement::new(
        Term::Iri(IriTerm::new("http://example.org/alice")),
        IriTerm::new("http://example.org/name"),
        Term::Literal(LiteralTerm::new("Alice")),
    )
}

// -- AspTermCodec round-trip -----------------------------------------------

#[test]
fn codec_iri_roundtrip() {
    let term = Term::Iri(IriTerm::new("http://example.org/test"));
    let encoded = AspTermCodec::encode_term(&term);
    let decoded = AspTermCodec::decode_term(&encoded).unwrap();
    assert_eq!(term, decoded);
}

#[test]
fn codec_literal_roundtrip() {
    let term = Term::Literal(LiteralTerm::new("hello world"));
    let encoded = AspTermCodec::encode_term(&term);
    let decoded = AspTermCodec::decode_term(&encoded).unwrap();
    assert_eq!(term, decoded);
}

#[test]
fn codec_language_literal_roundtrip() {
    let term = Term::Literal(LiteralTerm::new("bonjour").with_language("fr"));
    let encoded = AspTermCodec::encode_term(&term);
    let decoded = AspTermCodec::decode_term(&encoded).unwrap();
    assert_eq!(term, decoded);
}

// -- AspFactMapper ---------------------------------------------------------

#[test]
fn fact_mapper_statement_roundtrip_format() {
    let stmt = sample_statement();
    let fact = AspFactMapper::statement_to_fact(&stmt);
    assert!(fact.starts_with("rdf("));
    assert!(fact.ends_with(")."));
    assert!(fact.contains("alice"));
    assert!(fact.contains("name"));
}

#[test]
fn fact_mapper_batch() {
    let stmts = vec![sample_statement(), sample_statement()];
    let program = AspFactMapper::statements_to_program(&stmts);
    assert_eq!(program.lines().count(), 2);
}

// -- IncrementalAspSession -------------------------------------------------

#[tokio::test]
async fn session_multi_step() {
    let solver = MockSolver {
        response: vec![AnswerSet::new(vec![Atom::new("ok", vec![])])],
    };
    let mut session = IncrementalAspSession::new(solver, "base.".into());

    // Step 1: add a fact
    let delta1 = cqels_asp::AspFactDelta {
        additions: vec![sample_statement()],
        deletions: vec![],
    };
    let r1 = session.solve_with_delta(&delta1).await.unwrap();
    assert_eq!(r1.len(), 1);

    // Step 2: add another fact
    let stmt2 = Statement::new(
        Term::Iri(IriTerm::new("http://example.org/bob")),
        IriTerm::new("http://example.org/name"),
        Term::Literal(LiteralTerm::new("Bob")),
    );
    let delta2 = cqels_asp::AspFactDelta {
        additions: vec![stmt2],
        deletions: vec![],
    };
    let r2 = session.solve_with_delta(&delta2).await.unwrap();
    assert_eq!(r2.len(), 1);

    // Step 3: delete the first
    let delta3 = cqels_asp::AspFactDelta {
        additions: vec![],
        deletions: vec![sample_statement()],
    };
    let r3 = session.solve_with_delta(&delta3).await.unwrap();
    assert_eq!(r3.len(), 1);
}

// -- AspContinuousQuery integration ----------------------------------------

#[tokio::test]
async fn continuous_query_produces_output() {
    let solver = Arc::new(MockSolver {
        response: vec![AnswerSet::new(vec![Atom::new(
            "path",
            vec!["a".into(), "b".into()],
        )])],
    });
    let config = AspStreamSolveConfig::builder()
        .base_program("path(X,Y) :- edge(X,Y).")
        .build();
    let query = AspContinuousQuery::with_solver("q1", config, solver);

    assert_eq!(query.query_type(), QueryType::Custom);

    let mut inputs = QueryInputs::new();
    let elem = StreamElement::Rdf(RdfStreamElement::new(sample_statement(), 1000));
    inputs.add_stream("data", Box::pin(futures::stream::iter(vec![elem])));

    let results: Vec<_> = query.execute(inputs).collect().await;
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn continuous_query_empty_without_solver() {
    let config = AspStreamSolveConfig::default();
    let query = AspContinuousQuery::new("q2", config);

    let mut inputs = QueryInputs::new();
    inputs.add_stream("data", Box::pin(futures::stream::empty()));

    let results: Vec<_> = query.execute(inputs).collect().await;
    assert!(results.is_empty());
}
