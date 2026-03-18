//! Property-based tests for expression parsing and evaluation.
//!
//! Verifies:
//! - Expression parser never panics on arbitrary input
//! - Evaluator never panics on any expression + binding combination
//! - Arithmetic and comparison identities hold

use proptest::prelude::*;

use cqels_core::expression::evaluator::ExpressionEvaluator;
use cqels_core::expression::parser::ExpressionParser;
use cqels_model::{BindingSet, Value};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn variable_name() -> impl Strategy<Value = String> {
    "[a-z][a-zA-Z0-9_]{0,8}".prop_map(|s| format!("?{s}"))
}

fn numeric_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        (0i64..10000).prop_map(|n| n.to_string()),
        (0.0f64..1000.0)
            .prop_map(|f| format!("{:.2}", f))
            .prop_filter("avoid approx_constant", |s| {
                !s.starts_with("3.14") && !s.starts_with("2.71")
            }),
    ]
}

fn simple_expression() -> impl Strategy<Value = String> {
    prop_oneof![
        variable_name(),
        numeric_literal(),
        (variable_name(), numeric_literal()).prop_map(|(v, n)| format!("{v} + {n}")),
        (variable_name(), numeric_literal()).prop_map(|(v, n)| format!("{v} * {n}")),
        (variable_name(), numeric_literal()).prop_map(|(v, n)| format!("{v} > {n}")),
        (variable_name(), numeric_literal()).prop_map(|(v, n)| format!("{v} < {n}")),
        (variable_name(), variable_name()).prop_map(|(a, b)| format!("{a} = {b}")),
        (variable_name(), variable_name()).prop_map(|(a, b)| format!("{a} != {b}")),
    ]
}

fn random_bindings() -> impl Strategy<Value = BindingSet> {
    proptest::collection::vec(
        (
            "[a-z][a-zA-Z0-9_]{0,8}",
            prop_oneof![
                (0i64..10000).prop_map(Value::Integer),
                (0.0f64..1000.0).prop_map(Value::Float),
                "[a-zA-Z ]{0,20}".prop_map(Value::String),
                proptest::bool::ANY.prop_map(Value::Boolean),
            ],
        ),
        0..5,
    )
    .prop_map(|pairs| {
        let mut bs = BindingSet::new(0);
        for (name, val) in pairs {
            bs.insert(&name, val);
        }
        bs
    })
}

// ---------------------------------------------------------------------------
// Parser safety tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn cqelsql_expression_parser_never_panics(input in "\\PC{0,100}") {
        let _ = ExpressionParser::parse_cqelsql(&input);
    }

    #[test]
    fn cypherql_expression_parser_never_panics(input in "\\PC{0,100}") {
        let _ = ExpressionParser::parse_cypherql(&input);
    }

    #[test]
    fn valid_expression_always_parses(expr in simple_expression()) {
        let result = ExpressionParser::parse_cqelsql(&expr);
        prop_assert!(result.is_ok(), "failed to parse: {expr}: {:?}", result.err());
    }
}

// ---------------------------------------------------------------------------
// Evaluator safety tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn evaluator_never_panics_on_valid_expression(
        expr_str in simple_expression(),
        bindings in random_bindings(),
    ) {
        let evaluator = ExpressionEvaluator::new();
        if let Ok(expr) = ExpressionParser::parse_cqelsql(&expr_str) {
            // Should never panic, even with missing bindings
            let _ = evaluator.evaluate(&expr, &bindings);
            let _ = evaluator.evaluate_as_bool(&expr, &bindings);
        }
    }

    #[test]
    fn evaluator_never_panics_on_arbitrary_input(
        input in "\\PC{0,50}",
        bindings in random_bindings(),
    ) {
        let evaluator = ExpressionEvaluator::new();
        if let Ok(expr) = ExpressionParser::parse_cqelsql(&input) {
            let _ = evaluator.evaluate(&expr, &bindings);
        }
    }
}

// ---------------------------------------------------------------------------
// Arithmetic identity tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn addition_with_zero_is_identity(n in 0i64..10000) {
        let evaluator = ExpressionEvaluator::new();
        let expr_str = "?x + 0".to_string();
        let expr = ExpressionParser::parse_cqelsql(&expr_str).unwrap();
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(n));
        let result = evaluator.evaluate(&expr, &bs);
        prop_assert_eq!(result, Value::Integer(n));
    }

    #[test]
    fn multiplication_by_one_is_identity(n in 0i64..10000) {
        let evaluator = ExpressionEvaluator::new();
        let expr_str = "?x * 1".to_string();
        let expr = ExpressionParser::parse_cqelsql(&expr_str).unwrap();
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(n));
        let result = evaluator.evaluate(&expr, &bs);
        prop_assert_eq!(result, Value::Integer(n));
    }

    #[test]
    fn comparison_reflexivity(n in 0i64..10000) {
        let evaluator = ExpressionEvaluator::new();
        let expr = ExpressionParser::parse_cqelsql("?x = ?x").unwrap();
        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(n));
        let result = evaluator.evaluate_as_bool(&expr, &bs);
        prop_assert!(result, "x = x should always be true for x={n}");
    }
}
