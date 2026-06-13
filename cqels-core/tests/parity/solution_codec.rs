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

const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_FLOAT: &str = "http://www.w3.org/2001/XMLSchema#float";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

/// Shared canonical lexical form for a double-valued result so the Rust engine's
/// `"4"` and the Java engine's `"4.0"` (both xsd:double 4.0) compare equal at the
/// VALUE level (parity unit 5). Whole numbers get a trailing `.0`; others use the
/// shortest round-trippable decimal. The matching Java side is
/// `ExpressionEvalAdapter.canonicalDouble`.
fn canonical_double(f: f64) -> String {
    if f.is_finite() && f == f.floor() {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

/// Serializes a [`Value`] to N-Triples-style notation matching the fixture
/// format. `xsd:string`-typed plain literals collapse to bare `"foo"` (RDF
/// 1.1 canonical form); other typed/lang literals serialize verbatim via
/// [`cqels_model::Term`]'s `Display` impl.
///
/// Defensive on `Value::Null`: per spec D5 (parity-root, 2026-05-27), the
/// parity surface uses absence-only UNBOUND — `Value::Null` is not a valid
/// binding value in this adapter's pipeline. Hits `unreachable!` with a
/// diagnostic rather than silently emitting the literal string `"null"`
/// (cqels-rs#78). Today's fixtures never produce `Value::Null` because all
/// term-strings parse to `Value::Term`; if a future code path leaks a null
/// binding through, this fails loudly.
pub(crate) fn value_to_canonical_nt(v: &Value) -> String {
    let term = match v {
        Value::Term(t) => t.clone(),
        // A native float result: canonicalize to a shared double lexical form.
        Value::Float(f) => {
            return format!("\"{}\"^^<{XSD_DOUBLE}>", canonical_double(*f));
        }
        Value::Null => unreachable!(
            "Value::Null in solutions-payload adapter output — parity surface uses \
             absence-only UNBOUND (spec D5, cqels-rs#78). All fixture inputs parse to \
             Value::Term; a null binding indicates a latent bug in the adapter or engine."
        ),
        other => match other.to_term() {
            Some(t) => t,
            None => panic!(
                "value_to_canonical_nt received a non-Term Value with no to_term() conversion: \
                 {other:?} — extend the match arm if a new Value variant is added."
            ),
        },
    };
    if let Term::Literal(lit) = &term {
        if lit.language().is_none() && lit.datatype() == Some(XSD_STRING) {
            return format!("\"{}\"", lit.value());
        }
        // A double/float/decimal-typed literal (e.g. the parsed `expected` term):
        // route it through the same double canonicalizer so expected vs actual
        // compare on identical lexical form regardless of engine serialization.
        if let Some(dt) = lit.datatype() {
            if dt == XSD_DOUBLE || dt == XSD_FLOAT || dt == XSD_DECIMAL {
                if let Ok(f) = lit.value().parse::<f64>() {
                    return format!("\"{}\"^^<{XSD_DOUBLE}>", canonical_double(f));
                }
            }
        }
    }
    term.to_string()
}
