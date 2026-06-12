//! Bridges `cqels_core::expression::ExpressionEvaluator` to the parity fixture
//! harness's [`ExpressionFixtureOperator`] interface (the `expression` payload
//! kind, unit 5).
//!
//! Pure function — no streaming, no terminal. Each `evaluate` call:
//!   1. parse `expr` with the engine's own parser
//!      ([`ExpressionParser::parse_cqelsql`]),
//!   2. build a `BindingSet` from the solution wire format (reusing
//!      [`parse_term_string`] from `solution_codec`),
//!   3. evaluate with the engine's own [`ExpressionEvaluator`],
//!   4. canonicalize the resulting `Value` to an N-Triples term string
//!      ([`value_to_canonical_nt`]).
//!
//! Per spec deviation **D-E1**, a parse failure, an unbound result, or a
//! type-error all collapse to [`EvalOutcome::Null`] — neither engine
//! distinguishes them. `Value::Null` is intercepted BEFORE
//! `value_to_canonical_nt` (which `unreachable!`s on Null).
//!
//! Mirrors `java/.../parity/fixtures/ExpressionEvalAdapter.java`.

use std::collections::HashMap;

use cqels_core::compiler::pipeline::term_to_value;
use cqels_core::expression::{ExpressionEvaluator, ExpressionParser};
use cqels_model::{BindingSet, Value};
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{EvalOutcome, ExpressionFixtureOperator, Solution};
use crate::solution_codec::{parse_term_string, value_to_canonical_nt};

pub(crate) struct ExpressionEvalAdapter {
    evaluator: ExpressionEvaluator,
}

impl ExpressionEvalAdapter {
    pub(crate) fn new() -> Self {
        Self {
            evaluator: ExpressionEvaluator::new(),
        }
    }

    /// Canonicalizes a `Value` to an `EvalOutcome`, intercepting `Null` so the
    /// shared `value_to_canonical_nt` (which `unreachable!`s on Null) is only
    /// ever handed a bound value.
    fn outcome(v: Value) -> EvalOutcome {
        match v {
            Value::Null => EvalOutcome::Null,
            bound => EvalOutcome::Value(value_to_canonical_nt(&bound)),
        }
    }
}

impl ExpressionFixtureOperator for ExpressionEvalAdapter {
    fn configure(&mut self, config: &JsonValue) {
        let mut prefixes: HashMap<String, String> = HashMap::new();
        if let Some(obj) = config.get("prefixes").and_then(JsonValue::as_object) {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    prefixes.insert(k.clone(), s.to_string());
                }
            }
        }
        self.evaluator = ExpressionEvaluator::with_prefixes(prefixes);
    }

    fn evaluate(&mut self, expr: &str, bindings: &Solution) -> EvalOutcome {
        // Parse with the engine's own parser; a parse failure is Null (D-E1).
        let ast = match ExpressionParser::parse_cqelsql(expr) {
            Ok(a) => a,
            Err(_) => return EvalOutcome::Null,
        };
        // Build the BindingSet from the solution wire format, LIFTING typed RDF
        // literals to native Value variants exactly as production does (via the
        // engine's own `term_to_value`). Without this, an RDF-typed binding like
        // "2"^^xsd:integer would stay a Value::Term(Literal), and the evaluator's
        // arithmetic — which only operates on Value::Integer / Value::Float —
        // would spuriously yield null. parse_term_string always produces
        // Value::Term, so we unwrap that and re-lift; a malformed term is a
        // fixture-authoring bug surfaced as Null rather than a panic.
        let mut bs = BindingSet::new(0);
        for (var, term) in bindings {
            match parse_term_string(term) {
                Ok(Value::Term(t)) => {
                    bs.insert(var.as_str(), term_to_value(&t));
                }
                Ok(other) => {
                    bs.insert(var.as_str(), other);
                }
                Err(_) => return EvalOutcome::Null,
            }
        }
        Self::outcome(self.evaluator.evaluate(&ast, &bs))
    }

    fn canonicalize_expected(&self, expected_term: &str) -> EvalOutcome {
        // Round-trip the fixture's expected term through the same parse +
        // canonicalize path the actual value goes through, so the comparison is
        // byte-vs-byte on canonical N-Triples regardless of the lexical form the
        // fixture author wrote. A term that won't parse falls back to verbatim so
        // a malformed expectation surfaces as a mismatch, not a panic.
        match parse_term_string(expected_term) {
            Ok(v) => Self::outcome(v),
            Err(_) => EvalOutcome::Value(expected_term.to_string()),
        }
    }
}
