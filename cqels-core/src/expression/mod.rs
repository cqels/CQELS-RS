//! Expression evaluation for CQELS continuous queries.
//!
//! This module bridges parsed query expressions and operator execution by
//! providing:
//!
//! - [`ast`] — Expression tree types (`Expression`, `BinaryOp`, `UnaryOp`)
//! - [`functions`] — SPARQL 1.1 built-in function implementations
//! - [`evaluator`] — Expression evaluation against `BindingSet` bindings
//! - [`parser`] — Parsing expression strings into `Expression` trees

pub mod ast;
mod cqelsql_parser;
mod cypherql_parser;
pub mod evaluator;
pub mod functions;
pub mod parser;

pub use ast::{AggregateExprFunction, BinaryOp, Expression, UnaryOp};
pub use evaluator::ExpressionEvaluator;
pub use parser::ExpressionParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_exports_accessible() {
        // Verify that key expression types are re-exported.
        let _eval = ExpressionEvaluator::new();

        // AST types are accessible
        fn _assert_expr(_: &Expression) {}
        fn _assert_binop(_: &BinaryOp) {}
        fn _assert_unop(_: &UnaryOp) {}
        fn _assert_agg(_: &AggregateExprFunction) {}

        // Parser is accessible
        fn _assert_parser(_: &ExpressionParser) {}
    }

    #[test]
    fn test_evaluator_from_re_export() {
        use cqels_model::{BindingSet, Value};

        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Variable("x".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expression::Literal(Value::Integer(10))),
        };

        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(20));
        let result = eval.evaluate(&expr, &bs);
        assert_eq!(result, Value::Boolean(true));
    }
}
