//! Expression parser that converts expression strings into `Expression` AST trees.
//!
//! Uses the pest grammars from the parser module to parse standalone expressions
//! from both CqelsQL (SPARQL-style) and CypherQL (Cypher-style) syntaxes.
//!
//! Each grammar's pest parser struct lives in its own submodule to avoid
//! conflicting `Rule` enum generation by pest_derive.

use super::ast::Expression;
use crate::parser::ParseResult;

/// Parses expression strings into `Expression` AST trees.
pub struct ExpressionParser;

impl ExpressionParser {
    /// Parses a CqelsQL (SPARQL-style) expression string.
    pub fn parse_cqelsql(input: &str) -> ParseResult<Expression> {
        super::cqelsql_parser::parse(input)
    }

    /// Parses a CypherQL expression string.
    pub fn parse_cypherql(input: &str) -> ParseResult<Expression> {
        super::cypherql_parser::parse(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::ast::{AggregateExprFunction, BinaryOp, UnaryOp};
    use cqels_model::Value;

    #[test]
    fn test_parse_cqelsql_simple_comparison() {
        let expr = ExpressionParser::parse_cqelsql("?temp > 30").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Gt, ..
            } => {}
            other => panic!("expected BinaryOp::Gt, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_and() {
        let expr = ExpressionParser::parse_cqelsql("?x > 10 && ?x < 100").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::And, ..
            } => {}
            other => panic!("expected BinaryOp::And, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_arithmetic() {
        let expr = ExpressionParser::parse_cqelsql("?x + ?y").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Add, ..
            } => {}
            other => panic!("expected BinaryOp::Add, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_literal_integer() {
        let expr = ExpressionParser::parse_cqelsql("42").unwrap();
        assert_eq!(expr, Expression::Literal(Value::Integer(42)));
    }

    #[test]
    fn test_parse_cqelsql_literal_string() {
        let expr = ExpressionParser::parse_cqelsql("\"hello\"").unwrap();
        assert_eq!(expr, Expression::Literal(Value::String("hello".into())));
    }

    #[test]
    fn test_parse_cqelsql_literal_boolean() {
        let expr = ExpressionParser::parse_cqelsql("true").unwrap();
        assert_eq!(expr, Expression::Literal(Value::Boolean(true)));
    }

    #[test]
    fn test_parse_cqelsql_variable() {
        let expr = ExpressionParser::parse_cqelsql("?x").unwrap();
        assert_eq!(expr, Expression::Variable("?x".to_string()));
    }

    #[test]
    fn test_parse_cqelsql_function_call() {
        let expr = ExpressionParser::parse_cqelsql("strlen(?name)").unwrap();
        match expr {
            Expression::FunctionCall { name, args } => {
                assert_eq!(name, "strlen");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_bound() {
        let expr = ExpressionParser::parse_cqelsql("bound(?x)").unwrap();
        match expr {
            Expression::Bound(var) => assert_eq!(var, "?x"),
            other => panic!("expected Bound, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_negation() {
        let expr = ExpressionParser::parse_cqelsql("!true").unwrap();
        match expr {
            Expression::UnaryOp {
                op: UnaryOp::Not, ..
            } => {}
            other => panic!("expected UnaryOp::Not, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cqelsql_multiplication() {
        let expr = ExpressionParser::parse_cqelsql("?x * 2").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Mul, ..
            } => {}
            other => panic!("expected BinaryOp::Mul, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_simple_comparison() {
        let expr = ExpressionParser::parse_cypherql("n.temp > 30").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Gt, ..
            } => {}
            other => panic!("expected BinaryOp::Gt, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_and() {
        let expr = ExpressionParser::parse_cypherql("x > 10 AND x < 100").unwrap();
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::And, ..
            } => {}
            other => panic!("expected BinaryOp::And, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_property_access() {
        let expr = ExpressionParser::parse_cypherql("n.name").unwrap();
        match expr {
            Expression::PropertyAccess { variable, property } => {
                assert_eq!(variable, "n");
                assert_eq!(property, "name");
            }
            other => panic!("expected PropertyAccess, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_not() {
        let expr = ExpressionParser::parse_cypherql("NOT x > 5").unwrap();
        match expr {
            Expression::UnaryOp {
                op: UnaryOp::Not, ..
            } => {}
            other => panic!("expected UnaryOp::Not, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_string_literal() {
        let expr = ExpressionParser::parse_cypherql("'hello'").unwrap();
        assert_eq!(expr, Expression::Literal(Value::String("hello".into())));
    }

    #[test]
    fn test_parse_cypherql_null() {
        let expr = ExpressionParser::parse_cypherql("null").unwrap();
        assert_eq!(expr, Expression::Literal(Value::Null));
    }

    #[test]
    fn test_parse_cypherql_function() {
        let expr = ExpressionParser::parse_cypherql("toUpper(n.name)").unwrap();
        match expr {
            Expression::FunctionCall { name, args } => {
                assert_eq!(name, "toUpper");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cypherql_aggregate() {
        let expr = ExpressionParser::parse_cypherql("count(DISTINCT n)").unwrap();
        match expr {
            Expression::Aggregate {
                function: AggregateExprFunction::Count,
                distinct: true,
                ..
            } => {}
            other => panic!("expected Count aggregate, got: {other:?}"),
        }
    }
}
