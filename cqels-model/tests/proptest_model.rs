//! Property-based tests for cqels-model types.
//!
//! Verifies invariants for Term, Value, BindingSet, and Statement types
//! using proptest to generate random inputs.

use proptest::prelude::*;

use cqels_model::term::{BlankNodeTerm, IriTerm, LiteralTerm, Term};
use cqels_model::{BindingSet, Statement, Value};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn arbitrary_iri_term() -> impl Strategy<Value = IriTerm> {
    "[a-z][a-zA-Z0-9/._-]{0,50}".prop_map(|s| IriTerm::new(format!("http://example.org/{s}")))
}

fn arbitrary_literal_term() -> impl Strategy<Value = LiteralTerm> {
    "[\\w\\s]{0,50}".prop_map(LiteralTerm::new)
}

fn arbitrary_blank_node_term() -> impl Strategy<Value = BlankNodeTerm> {
    "[a-zA-Z0-9]{1,20}".prop_map(BlankNodeTerm::new)
}

fn arbitrary_term() -> impl Strategy<Value = Term> {
    prop_oneof![
        arbitrary_iri_term().prop_map(Term::Iri),
        arbitrary_literal_term().prop_map(Term::Literal),
        arbitrary_blank_node_term().prop_map(Term::BlankNode),
    ]
}

fn arbitrary_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        // Use finite floats only to avoid NaN comparison issues
        (-1e10f64..1e10f64).prop_map(Value::Float),
        "[\\w\\s]{0,50}".prop_map(Value::String),
        any::<bool>().prop_map(Value::Boolean),
        Just(Value::Null),
        arbitrary_term().prop_map(Value::Term),
    ]
}

// ---------------------------------------------------------------------------
// 1. Term invariants
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn iri_term_roundtrip(s in "[a-z][a-zA-Z0-9/._-]{0,50}") {
        let iri = IriTerm::new(&s);
        prop_assert_eq!(iri.as_str(), s.as_str());
    }

    #[test]
    fn iri_term_display_wraps_in_angle_brackets(s in "[a-z][a-zA-Z0-9]{0,30}") {
        let iri = IriTerm::new(&s);
        let display = format!("{iri}");
        prop_assert!(display.starts_with('<'));
        prop_assert!(display.ends_with('>'));
        prop_assert_eq!(&display[1..display.len()-1], s.as_str());
    }

    #[test]
    fn blank_node_id_roundtrip(id in "[a-zA-Z0-9]{1,20}") {
        let bn = BlankNodeTerm::new(&id);
        prop_assert_eq!(bn.id(), id.as_str());
    }

    #[test]
    fn literal_value_roundtrip(val in "[\\w\\s]{0,50}") {
        let lit = LiteralTerm::new(&val);
        prop_assert_eq!(lit.value(), val.as_str());
        prop_assert!(lit.datatype().is_none());
        prop_assert!(lit.language().is_none());
    }

    #[test]
    fn literal_with_language_preserves_both(
        val in "[\\w]{0,30}",
        lang in "[a-z]{2}"
    ) {
        let lit = LiteralTerm::new(&val).with_language(&lang);
        prop_assert_eq!(lit.value(), val.as_str());
        prop_assert_eq!(lit.language(), Some(lang.as_str()));
    }

    #[test]
    fn literal_with_datatype_preserves_both(
        val in "[0-9]{1,10}",
        dt in "[a-z]{3,10}"
    ) {
        let dt_uri = format!("http://example.org/{dt}");
        let lit = LiteralTerm::new(&val).with_datatype(&dt_uri);
        prop_assert_eq!(lit.value(), val.as_str());
        prop_assert_eq!(lit.datatype(), Some(dt_uri.as_str()));
    }

    #[test]
    fn term_equality_is_reflexive(term in arbitrary_term()) {
        prop_assert_eq!(&term, &term);
    }

    #[test]
    fn term_clone_equals_original(term in arbitrary_term()) {
        let cloned = term.clone();
        prop_assert_eq!(&term, &cloned);
    }

    #[test]
    fn term_debug_never_panics(term in arbitrary_term()) {
        let _ = format!("{:?}", term);
    }

    #[test]
    fn term_display_never_panics(term in arbitrary_term()) {
        let _ = format!("{}", term);
    }
}

// ---------------------------------------------------------------------------
// 2. Value invariants
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn value_integer_roundtrip(n in -1000000i64..1000000) {
        let val = Value::Integer(n);
        prop_assert_eq!(val.as_integer(), Some(n));
        prop_assert!(val.as_string().is_none());
        prop_assert!(val.as_boolean().is_none());
    }

    #[test]
    fn value_string_roundtrip(s in "[\\w\\s]{0,50}") {
        let val = Value::String(s.clone());
        prop_assert_eq!(val.as_string(), Some(s.as_str()));
        prop_assert!(val.as_integer().is_none());
    }

    #[test]
    fn value_boolean_roundtrip(b in 0u8..2) {
        let b = b == 1;
        let val = Value::Boolean(b);
        prop_assert_eq!(val.as_boolean(), Some(b));
    }

    #[test]
    fn value_non_null_is_not_null(val in arbitrary_value()) {
        match &val {
            Value::Null => prop_assert!(val.is_null()),
            _ => prop_assert!(!val.is_null()),
        }
    }

    #[test]
    fn value_equality_is_reflexive(val in arbitrary_value()) {
        prop_assert_eq!(&val, &val);
    }

    #[test]
    fn value_clone_equals_original(val in arbitrary_value()) {
        let cloned = val.clone();
        prop_assert_eq!(&val, &cloned);
    }

    #[test]
    fn value_display_never_panics(val in arbitrary_value()) {
        let _ = format!("{}", val);
    }

    #[test]
    fn value_to_term_never_panics(val in arbitrary_value()) {
        let _ = val.to_term();
    }
}

// ---------------------------------------------------------------------------
// 3. BindingSet invariants
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn binding_set_insert_get_roundtrip(
        key in "[a-z]{1,10}",
        val in arbitrary_value(),
        ts in -100000i64..100000,
    ) {
        let mut bs = BindingSet::new(ts);
        bs.insert(key.clone(), val.clone());
        prop_assert_eq!(bs.get(&key), Some(&val));
    }

    #[test]
    fn binding_set_variables_lists_all_keys(
        keys in prop::collection::hash_set("[a-z]{1,5}", 1..10),
        ts in -100000i64..100000,
    ) {
        let mut bs = BindingSet::new(ts);
        for key in &keys {
            bs.insert(key.clone(), Value::Null);
        }
        let vars: std::collections::HashSet<String> = bs.variables().map(|s| s.to_string()).collect();
        prop_assert_eq!(vars.len(), keys.len());
        for key in &keys {
            prop_assert!(vars.contains(key.as_str()));
        }
    }

    #[test]
    fn binding_set_missing_key_returns_none(
        ts in -100000i64..100000,
    ) {
        let bs = BindingSet::new(ts);
        prop_assert!(bs.get("nonexistent").is_none());
    }
}

// ---------------------------------------------------------------------------
// 4. Statement invariants
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn statement_fields_accessible(
        subj in arbitrary_term(),
        pred in arbitrary_iri_term(),
        obj in arbitrary_term(),
    ) {
        let stmt = Statement::new(subj.clone(), pred.clone(), obj.clone());
        prop_assert_eq!(&stmt.subject, &subj);
        prop_assert_eq!(&stmt.predicate, &pred);
        prop_assert_eq!(&stmt.object, &obj);
    }

    #[test]
    fn statement_equality_is_reflexive(
        subj in arbitrary_term(),
        pred in arbitrary_iri_term(),
        obj in arbitrary_term(),
    ) {
        let stmt = Statement::new(subj, pred, obj);
        prop_assert_eq!(&stmt, &stmt);
    }

    #[test]
    fn statement_display_never_panics(
        subj in arbitrary_term(),
        pred in arbitrary_iri_term(),
        obj in arbitrary_term(),
    ) {
        let stmt = Statement::new(subj, pred, obj);
        let _ = format!("{}", stmt);
    }
}
