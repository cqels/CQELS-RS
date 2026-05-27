//! Shared Solution ↔ BindingSet conversion helpers for solutions-payload
//! parity adapters (MinusOperator, Distinct, and any future SPARQL operator
//! that consumes / produces binding sets via the [`SolutionFixtureOperator`]
//! trait).
//!
//! Each fixture solution carries variable→N-Triples-style strings. Parsing
//! wraps each term in a stub triple and runs `oxttl::NTriplesParser`,
//! producing an `oxrdf::Term`. The existing `From<oxrdf::Term>` for
//! `cqels_model::Term` folds it into the model type; we wrap in `Value::Term`.
//!
//! `xsd:string`-typed plain literals are emitted as bare `"foo"` rather than
//! `"foo"^^<http://...XMLSchema#string>` so the round-trip lines up with the
//! RDF 1.1 N-Triples canonical form used by the fixture authors.
//!
//! **LL1 note** (cross-language design item, parity-session decision pending):
//! the canonicalization asymmetry between fixture-`expected` (verbatim) and
//! adapter-`actual` (canonicalized) is xsd:string-specific. When the LL1 fix
//! lands — likely normalizing `expected` solutions through the same
//! canonicalizer — this module is the single place to apply the change for
//! all solutions-payload adapters.

use cqels_model::{BindingSet, Term, Value};
use oxttl::NTriplesParser;

use crate::parity_fixture_harness::Solution;

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Parses each variable's N-Triples-style term string into a
/// [`cqels_model::Value`] and assembles a [`BindingSet`] at timestamp 0.
///
/// Timestamp is fixed at 0 because Solution wire format carries no
/// timestamp field. Operators whose semantics depend on timestamps
/// (e.g. JOIN over windowed streams) need a richer adapter shape.
pub(crate) fn solution_to_bindingset(sol: &Solution) -> BindingSet {
    let mut bs = BindingSet::new(0);
    for (var, term_str) in sol {
        let value = parse_term_string(term_str).unwrap_or_else(|err| {
            panic!("parse failed for variable '{var}' = {term_str:?}: {err}")
        });
        bs.insert(var.as_str(), value);
    }
    bs
}

/// Serializes a [`BindingSet`] back to the Solution wire format (variable
/// name → canonical N-Triples term string). Iteration order is
/// HashMap-undefined, but Solution is a BTreeMap so the final result is
/// sorted by variable name regardless.
pub(crate) fn bindingset_to_solution(bs: &BindingSet) -> Solution {
    bs.iter()
        .map(|(var, val)| (var.to_string(), value_to_canonical_nt(val)))
        .collect()
}

/// Parses an N-Triples-style term string into a [`Value`].
///
/// Accepts the four N-Triples shapes: `<iri>`, `"literal"`, `"literal"@lang`,
/// `"literal"^^<datatype>`, and `_:bnode`. Wraps the term in a stub triple to
/// reuse oxttl's parser rather than rolling a single-term tokenizer.
pub(crate) fn parse_term_string(s: &str) -> Result<Value, String> {
    let stub = format!("<urn:s> <urn:p> {s} .\n");
    let mut iter = NTriplesParser::new().for_reader(stub.as_bytes());
    let triple = iter
        .next()
        .ok_or_else(|| format!("expected one triple from stub for term {s:?}"))?
        .map_err(|e| format!("NTriples parse failed for term {s:?}: {e}"))?;
    Ok(Value::Term(Term::from(triple.object)))
}

/// Serializes a [`Value`] to N-Triples-style notation matching the fixture
/// format. `xsd:string`-typed plain literals collapse to bare `"foo"` (RDF
/// 1.1 canonical form); other typed/lang literals serialize verbatim via
/// [`cqels_model::Term`]'s `Display` impl.
pub(crate) fn value_to_canonical_nt(v: &Value) -> String {
    let term = match v {
        Value::Term(t) => t.clone(),
        other => match other.to_term() {
            Some(t) => t,
            None => return "null".to_string(),
        },
    };
    if let Term::Literal(lit) = &term {
        if lit.language().is_none() && lit.datatype() == Some(XSD_STRING) {
            return format!("\"{}\"", lit.value());
        }
    }
    term.to_string()
}
