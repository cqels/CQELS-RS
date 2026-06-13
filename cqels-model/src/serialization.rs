//! Output serialization for CQELS query results.
//!
//! Provides standard output formats for interoperability with external systems:
//!
//! - **SPARQL JSON Results Format** ([W3C spec](https://www.w3.org/TR/sparql11-results-json/)):
//!   [`to_sparql_json`], [`to_sparql_json_string`]
//! - **N-Triples** ([W3C spec](https://www.w3.org/TR/n-triples/)):
//!   [`to_ntriples`], [`write_ntriple`], [`bindings_to_ntriples`]

use std::collections::BTreeSet;

use serde_json::{json, Value as JsonValue};

use crate::term::{IriTerm, Term};
use crate::value::Value;
use crate::{BindingSet, Statement};

// ── XSD datatype constants ────────────────────────────────────────────────

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

// ── SPARQL JSON Results Format ────────────────────────────────────────────

/// Serializes a list of [`BindingSet`] results to the
/// [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/).
///
/// Variable names are collected across all result rows and sorted
/// alphabetically for deterministic output. Unbound variables (null) are
/// omitted from each binding object per the W3C spec.
///
/// # Examples
///
/// ```
/// use cqels_model::{BindingSet, Value};
/// use cqels_model::serialization::to_sparql_json;
///
/// let mut bs = BindingSet::new(0);
/// bs.insert("name", Value::String("Alice".into()));
/// bs.insert("age", Value::Integer(30));
///
/// let json = to_sparql_json(&[bs]);
/// assert!(json["head"]["vars"].as_array().unwrap().contains(&serde_json::json!("name")));
/// assert_eq!(json["results"]["bindings"][0]["name"]["value"], "Alice");
/// ```
pub fn to_sparql_json(results: &[BindingSet]) -> JsonValue {
    // Collect all variable names (sorted for determinism).
    let vars: BTreeSet<&str> = results.iter().flat_map(|bs| bs.variables()).collect();
    let var_list: Vec<&str> = vars.into_iter().collect();

    let bindings: Vec<JsonValue> = results
        .iter()
        .map(|bs| {
            let mut obj = serde_json::Map::new();
            for &var in &var_list {
                if let Some(val) = bs.get(var) {
                    if let Some(json_val) = value_to_sparql_json(val) {
                        obj.insert(var.to_string(), json_val);
                    }
                }
            }
            JsonValue::Object(obj)
        })
        .collect();

    json!({
        "head": { "vars": var_list },
        "results": { "bindings": bindings }
    })
}

/// Convenience wrapper that returns a pretty-printed JSON string.
pub fn to_sparql_json_string(results: &[BindingSet]) -> String {
    serde_json::to_string_pretty(&to_sparql_json(results))
        .expect("SPARQL JSON serialization should not fail")
}

/// Converts a single [`Value`] to its SPARQL JSON binding representation.
///
/// Returns `None` for [`Value::Null`] (unbound variables are omitted per spec).
fn value_to_sparql_json(value: &Value) -> Option<JsonValue> {
    match value {
        Value::Term(term) => term_to_sparql_json(term),
        Value::String(s) => Some(json!({ "type": "literal", "value": s })),
        Value::Integer(i) => Some(json!({
            "type": "literal",
            "value": i.to_string(),
            "datatype": XSD_INTEGER
        })),
        Value::Float(f) => Some(json!({
            "type": "literal",
            "value": f.to_string(),
            "datatype": XSD_DOUBLE
        })),
        Value::Boolean(b) => Some(json!({
            "type": "literal",
            "value": b.to_string(),
            "datatype": XSD_BOOLEAN
        })),
        Value::Null => None,
    }
}

/// Converts an RDF [`Term`] to its SPARQL JSON representation.
fn term_to_sparql_json(term: &Term) -> Option<JsonValue> {
    match term {
        Term::Iri(iri) => Some(json!({ "type": "uri", "value": iri.as_str() })),
        Term::BlankNode(bn) => Some(json!({ "type": "bnode", "value": bn.id() })),
        Term::Literal(lit) => {
            let mut obj = serde_json::Map::new();
            obj.insert("type".to_string(), json!("literal"));
            obj.insert("value".to_string(), json!(lit.value()));
            if let Some(lang) = lit.language() {
                obj.insert("xml:lang".to_string(), json!(lang));
            } else if let Some(dt) = lit.datatype() {
                obj.insert("datatype".to_string(), json!(dt));
            }
            Some(JsonValue::Object(obj))
        }
    }
}

// ── N-Triples Serialization ──────────────────────────────────────────────

/// Serializes a slice of [`Statement`]s to
/// [N-Triples](https://www.w3.org/TR/n-triples/) format.
///
/// Each statement is written as one line: `<s> <p> <o> .\n`.
/// Quad graph information is ignored (N-Triples is for triples only).
///
/// # Examples
///
/// ```
/// use cqels_model::{Statement, Term};
/// use cqels_model::term::{IriTerm, LiteralTerm};
/// use cqels_model::serialization::to_ntriples;
///
/// let stmt = Statement::new(
///     Term::Iri(IriTerm::new("http://example.org/s")),
///     IriTerm::new("http://example.org/p"),
///     Term::Literal(LiteralTerm::new("hello")),
/// );
/// let nt = to_ntriples(&[stmt]);
/// assert_eq!(nt, "<http://example.org/s> <http://example.org/p> \"hello\" .\n");
/// ```
pub fn to_ntriples(statements: &[Statement]) -> String {
    let mut out = String::new();
    for stmt in statements {
        write_ntriple(stmt, &mut out);
    }
    out
}

/// Writes a single [`Statement`] in N-Triples format, appending to `out`.
pub fn write_ntriple(stmt: &Statement, out: &mut String) {
    write_ntriple_term(&stmt.subject, out);
    out.push(' ');
    // Predicate is always an IRI.
    write_ntriple_iri(&stmt.predicate, out);
    out.push(' ');
    write_ntriple_term(&stmt.object, out);
    out.push_str(" .\n");
}

/// Writes a [`Term`] in N-Triples syntax.
fn write_ntriple_term(term: &Term, out: &mut String) {
    match term {
        Term::Iri(iri) => write_ntriple_iri(iri, out),
        Term::BlankNode(bn) => {
            out.push_str("_:");
            out.push_str(bn.id());
        }
        Term::Literal(lit) => {
            out.push('"');
            escape_ntriples(lit.value(), out);
            out.push('"');
            if let Some(lang) = lit.language() {
                out.push('@');
                out.push_str(lang);
            } else if let Some(dt) = lit.datatype() {
                out.push_str("^^<");
                out.push_str(dt);
                out.push('>');
            }
        }
    }
}

/// Writes an [`IriTerm`] in N-Triples syntax: `<iri>`.
fn write_ntriple_iri(iri: &IriTerm, out: &mut String) {
    out.push('<');
    out.push_str(iri.as_str());
    out.push('>');
}

/// Escapes a string value for N-Triples literal content.
fn escape_ntriples(s: &str, out: &mut String) {
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
}

// ── BindingSet → N-Triples ───────────────────────────────────────────────

/// Extracts RDF triples from [`BindingSet`] results and serializes as N-Triples.
///
/// For each binding set, the values of `subject_var`, `predicate_var`, and
/// `object_var` are extracted and converted to RDF terms. Rows where any
/// of the three variables is missing or cannot be converted are skipped.
///
/// # Examples
///
/// ```
/// use cqels_model::{BindingSet, Value, Term};
/// use cqels_model::term::IriTerm;
/// use cqels_model::serialization::bindings_to_ntriples;
///
/// let mut bs = BindingSet::new(0);
/// bs.insert("s", Value::Term(Term::Iri(IriTerm::new("http://ex.org/a"))));
/// bs.insert("p", Value::Term(Term::Iri(IriTerm::new("http://ex.org/p"))));
/// bs.insert("o", Value::Integer(42));
///
/// let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
/// assert!(nt.contains("<http://ex.org/a>"));
/// assert!(nt.contains("<http://ex.org/p>"));
/// ```
pub fn bindings_to_ntriples(
    results: &[BindingSet],
    subject_var: &str,
    predicate_var: &str,
    object_var: &str,
) -> String {
    let mut out = String::new();
    for bs in results {
        let Some(subj_val) = bs.get(subject_var) else {
            continue;
        };
        let Some(pred_val) = bs.get(predicate_var) else {
            continue;
        };
        let Some(obj_val) = bs.get(object_var) else {
            continue;
        };

        // Subject must be IRI or blank node.
        let Some(subj_term) = subj_val.to_term() else {
            continue;
        };
        if subj_term.is_literal() {
            continue;
        }

        // Predicate must be an IRI. A Value::String is a literal binding (the
        // pipeline lifts plain/xsd:string literals to String), and coercing
        // literal text into a predicate IRI would fabricate triples like
        // `<s> <42> <o>` — skip it like any other non-IRI binding.
        let pred_iri = match pred_val {
            Value::Term(Term::Iri(iri)) => iri.clone(),
            _ => continue,
        };

        // Object can be any term.
        let Some(obj_term) = obj_val.to_term() else {
            continue;
        };

        let stmt = Statement::new(subj_term, pred_iri, obj_term);
        write_ntriple(&stmt, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::{BlankNodeTerm, LiteralTerm};

    // ── SPARQL JSON Tests ─────────────────────────────────────────────────

    #[test]
    fn test_sparql_json_basic() {
        let mut bs = BindingSet::new(0);
        bs.insert("name", Value::String("Alice".into()));
        bs.insert("age", Value::Integer(30));

        let json = to_sparql_json(&[bs]);
        let head = &json["head"]["vars"];
        assert!(head.as_array().unwrap().contains(&json!("name")));
        assert!(head.as_array().unwrap().contains(&json!("age")));

        let binding = &json["results"]["bindings"][0];
        assert_eq!(binding["name"]["type"], "literal");
        assert_eq!(binding["name"]["value"], "Alice");
        assert_eq!(binding["age"]["type"], "literal");
        assert_eq!(binding["age"]["value"], "30");
        assert_eq!(binding["age"]["datatype"], XSD_INTEGER);
    }

    #[test]
    fn test_sparql_json_all_types() {
        let mut bs = BindingSet::new(0);
        bs.insert(
            "iri",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/x"))),
        );
        bs.insert(
            "bnode",
            Value::Term(Term::BlankNode(BlankNodeTerm::new("b0"))),
        );
        bs.insert(
            "lang_lit",
            Value::Term(Term::Literal(
                LiteralTerm::new("bonjour").with_language("fr"),
            )),
        );
        bs.insert(
            "typed_lit",
            Value::Term(Term::Literal(
                LiteralTerm::new("42").with_datatype(XSD_INTEGER),
            )),
        );
        bs.insert("str", Value::String("hello".into()));
        bs.insert("int", Value::Integer(7));
        bs.insert("float", Value::Float(2.5));
        bs.insert("bool", Value::Boolean(true));
        bs.insert("null", Value::Null);

        let json = to_sparql_json(&[bs]);
        let b = &json["results"]["bindings"][0];

        assert_eq!(b["iri"]["type"], "uri");
        assert_eq!(b["iri"]["value"], "http://example.org/x");

        assert_eq!(b["bnode"]["type"], "bnode");
        assert_eq!(b["bnode"]["value"], "b0");

        assert_eq!(b["lang_lit"]["type"], "literal");
        assert_eq!(b["lang_lit"]["value"], "bonjour");
        assert_eq!(b["lang_lit"]["xml:lang"], "fr");

        assert_eq!(b["typed_lit"]["type"], "literal");
        assert_eq!(b["typed_lit"]["value"], "42");
        assert_eq!(b["typed_lit"]["datatype"], XSD_INTEGER);

        assert_eq!(b["str"]["type"], "literal");
        assert_eq!(b["str"]["value"], "hello");

        assert_eq!(b["int"]["datatype"], XSD_INTEGER);
        assert_eq!(b["float"]["datatype"], XSD_DOUBLE);
        assert_eq!(b["bool"]["datatype"], XSD_BOOLEAN);
        assert_eq!(b["bool"]["value"], "true");

        // Null should be absent
        assert!(b.get("null").is_none());
    }

    #[test]
    fn test_sparql_json_null_omitted() {
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(1));
        bs.insert("y", Value::Null);

        let json = to_sparql_json(&[bs]);
        let b = &json["results"]["bindings"][0];
        assert!(b.get("x").is_some());
        assert!(b.get("y").is_none());
    }

    #[test]
    fn test_sparql_json_empty_results() {
        let json = to_sparql_json(&[]);
        assert_eq!(json["head"]["vars"], json!([]));
        assert_eq!(json["results"]["bindings"], json!([]));
    }

    #[test]
    fn test_sparql_json_string_output() {
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(42));
        let s = to_sparql_json_string(&[bs]);
        // Should be valid JSON
        let parsed: serde_json::Result<JsonValue> = serde_json::from_str(&s);
        assert!(parsed.is_ok());
    }

    // ── N-Triples Tests ──────────────────────────────────────────────────

    #[test]
    fn test_ntriples_basic() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("hello")),
        );
        let nt = to_ntriples(&[stmt]);
        assert_eq!(
            nt,
            "<http://example.org/s> <http://example.org/p> \"hello\" .\n"
        );
    }

    #[test]
    fn test_ntriples_literal_escaping() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("line1\nline2\ttab\"quote\\backslash")),
        );
        let nt = to_ntriples(&[stmt]);
        assert!(nt.contains("\\n"));
        assert!(nt.contains("\\t"));
        assert!(nt.contains("\\\""));
        assert!(nt.contains("\\\\"));
        assert!(!nt.contains('\n') || nt.matches('\n').count() == 1); // only trailing newline
    }

    #[test]
    fn test_ntriples_typed_literal() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("42").with_datatype(XSD_INTEGER)),
        );
        let nt = to_ntriples(&[stmt]);
        assert!(nt.contains(&format!("\"42\"^^<{XSD_INTEGER}>")));
    }

    #[test]
    fn test_ntriples_language_tagged() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("hola").with_language("es")),
        );
        let nt = to_ntriples(&[stmt]);
        assert!(nt.contains("\"hola\"@es"));
    }

    #[test]
    fn test_ntriples_blank_node() {
        let stmt = Statement::new(
            Term::BlankNode(BlankNodeTerm::new("b0")),
            IriTerm::new("http://example.org/p"),
            Term::BlankNode(BlankNodeTerm::new("b1")),
        );
        let nt = to_ntriples(&[stmt]);
        assert_eq!(nt, "_:b0 <http://example.org/p> _:b1 .\n");
    }

    #[test]
    fn test_ntriples_multiple() {
        let stmts = vec![
            Statement::new(
                Term::Iri(IriTerm::new("http://example.org/a")),
                IriTerm::new("http://example.org/p"),
                Term::Literal(LiteralTerm::new("1")),
            ),
            Statement::new(
                Term::Iri(IriTerm::new("http://example.org/b")),
                IriTerm::new("http://example.org/q"),
                Term::Literal(LiteralTerm::new("2")),
            ),
        ];
        let nt = to_ntriples(&stmts);
        let lines: Vec<&str> = nt.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_bindings_to_ntriples() {
        let mut bs = BindingSet::new(0);
        bs.insert(
            "s",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/sensor1"))),
        );
        bs.insert(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/value"))),
        );
        bs.insert("o", Value::Integer(42));

        let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
        assert!(nt.contains("<http://example.org/sensor1>"));
        assert!(nt.contains("<http://example.org/value>"));
        assert!(nt.contains(&format!("\"42\"^^<{XSD_INTEGER}>")));
        assert!(nt.ends_with(".\n"));
    }

    #[test]
    fn test_sparql_json_multiple_results() {
        let mut bs1 = BindingSet::new(0);
        bs1.insert("x", Value::Integer(1));
        let mut bs2 = BindingSet::new(0);
        bs2.insert("x", Value::Integer(2));
        bs2.insert("y", Value::String("extra".into()));

        let json = to_sparql_json(&[bs1, bs2]);
        let vars = json["head"]["vars"].as_array().unwrap();
        // Both x and y should appear in vars (union across all results)
        assert!(vars.contains(&json!("x")));
        assert!(vars.contains(&json!("y")));
        let bindings = json["results"]["bindings"].as_array().unwrap();
        assert_eq!(bindings.len(), 2);
        // First result has no "y"
        assert!(bindings[0].get("y").is_none());
        // Second result has "y"
        assert!(bindings[1].get("y").is_some());
    }

    #[test]
    fn test_sparql_json_float_value() {
        let mut bs = BindingSet::new(0);
        bs.insert("f", Value::Float(2.5));
        let json = to_sparql_json(&[bs]);
        let b = &json["results"]["bindings"][0];
        assert_eq!(b["f"]["datatype"], XSD_DOUBLE);
        assert_eq!(b["f"]["value"], "2.5");
    }

    #[test]
    fn test_sparql_json_boolean_false() {
        let mut bs = BindingSet::new(0);
        bs.insert("b", Value::Boolean(false));
        let json = to_sparql_json(&[bs]);
        let b = &json["results"]["bindings"][0];
        assert_eq!(b["b"]["value"], "false");
        assert_eq!(b["b"]["datatype"], XSD_BOOLEAN);
    }

    #[test]
    fn test_sparql_json_plain_literal() {
        let mut bs = BindingSet::new(0);
        bs.insert(
            "lit",
            Value::Term(Term::Literal(LiteralTerm::new("plain text"))),
        );
        let json = to_sparql_json(&[bs]);
        let b = &json["results"]["bindings"][0];
        assert_eq!(b["lit"]["type"], "literal");
        assert_eq!(b["lit"]["value"], "plain text");
        // No datatype or xml:lang for plain literal
        assert!(b["lit"].get("datatype").is_none());
        assert!(b["lit"].get("xml:lang").is_none());
    }

    #[test]
    fn test_ntriples_empty() {
        let nt = to_ntriples(&[]);
        assert!(nt.is_empty());
    }

    #[test]
    fn test_ntriples_carriage_return_escape() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/s")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("line\r")),
        );
        let nt = to_ntriples(&[stmt]);
        assert!(nt.contains("\\r"));
    }

    #[test]
    fn test_bindings_to_ntriples_missing_variable() {
        let mut bs = BindingSet::new(0);
        bs.insert(
            "s",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/s"))),
        );
        // Missing "p" and "o" — should produce empty output
        let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
        assert!(nt.is_empty());
    }

    #[test]
    fn test_bindings_to_ntriples_literal_subject_skipped() {
        let mut bs = BindingSet::new(0);
        bs.insert(
            "s",
            Value::Term(Term::Literal(LiteralTerm::new("not-a-subject"))),
        );
        bs.insert(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/p"))),
        );
        bs.insert("o", Value::Integer(42));
        // Literal subject is invalid in RDF — should be skipped
        let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
        assert!(nt.is_empty());
    }

    #[test]
    fn test_bindings_to_ntriples_string_predicate_skipped() {
        // A Value::String binding is a literal (the pipeline lifts plain and
        // xsd:string literals to String), so it must NOT be coerced into a
        // predicate IRI — even when its text looks like one.
        let mut bs = BindingSet::new(0);
        bs.insert(
            "s",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/s"))),
        );
        bs.insert("p", Value::String("http://example.org/p".into()));
        bs.insert("o", Value::String("hello".into()));
        let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
        assert!(nt.is_empty());
    }

    #[test]
    fn test_bindings_to_ntriples_blank_node_subject() {
        let mut bs = BindingSet::new(0);
        bs.insert("s", Value::Term(Term::BlankNode(BlankNodeTerm::new("b0"))));
        bs.insert(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/p"))),
        );
        bs.insert(
            "o",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/o"))),
        );
        let nt = bindings_to_ntriples(&[bs], "s", "p", "o");
        assert!(nt.starts_with("_:b0"));
    }

    #[test]
    fn test_bindings_to_ntriples_multiple() {
        let mut bs1 = BindingSet::new(0);
        bs1.insert(
            "s",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/a"))),
        );
        bs1.insert(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/p"))),
        );
        bs1.insert("o", Value::Integer(1));

        let mut bs2 = BindingSet::new(0);
        bs2.insert(
            "s",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/b"))),
        );
        bs2.insert(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/q"))),
        );
        bs2.insert("o", Value::Integer(2));

        let nt = bindings_to_ntriples(&[bs1, bs2], "s", "p", "o");
        let lines: Vec<&str> = nt.lines().collect();
        assert_eq!(lines.len(), 2);
    }
}
