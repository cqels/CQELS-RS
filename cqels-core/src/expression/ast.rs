//! Expression AST types for query evaluation.
//!
//! Defines the expression tree used by the expression evaluator to bridge
//! parsed query expressions and operator execution.

use std::fmt;

use cqels_model::Value;

/// A compiled expression tree node.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// A literal constant value.
    Literal(Value),
    /// A variable reference (e.g., `?x` or `$x`).
    Variable(String),
    /// Property access on a variable (Cypher-style: `n.name`).
    PropertyAccess {
        variable: String,
        property: String,
    },
    /// A binary operation.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    /// A unary operation.
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expression>,
    },
    /// A function call (e.g., `strlen(?x)`, `abs(?val)`).
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    /// BOUND(?var) — tests whether a variable is bound.
    Bound(String),
    /// IF(condition, then, else).
    If {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
    /// An aggregate expression (e.g., COUNT(?x), SUM(?val)).
    Aggregate {
        function: AggregateExprFunction,
        argument: Box<Expression>,
        distinct: bool,
    },
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal(v) => write!(f, "{v}"),
            Expression::Variable(v) => write!(f, "?{v}"),
            Expression::PropertyAccess { variable, property } => {
                write!(f, "{variable}.{property}")
            }
            Expression::BinaryOp { op, left, right } => {
                write!(f, "({left} {op} {right})")
            }
            Expression::UnaryOp { op, operand } => write!(f, "{op}{operand}"),
            Expression::FunctionCall { name, args } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            Expression::Bound(var) => write!(f, "BOUND(?{var})"),
            Expression::If {
                condition,
                then_expr,
                else_expr,
            } => write!(f, "IF({condition}, {then_expr}, {else_expr})"),
            Expression::Aggregate {
                function,
                argument,
                distinct,
            } => {
                write!(f, "{function}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                write!(f, "{argument})")
            }
        }
    }
}

/// Binary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    // Logical
    And,
    Or,

    // Comparison
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,

    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // String
    Contains,
    StartsWith,
    EndsWith,
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::And => write!(f, "&&"),
            BinaryOp::Or => write!(f, "||"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::Neq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::Lte => write!(f, "<="),
            BinaryOp::Gte => write!(f, ">="),
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Mod => write!(f, "%"),
            BinaryOp::Contains => write!(f, "CONTAINS"),
            BinaryOp::StartsWith => write!(f, "STARTS WITH"),
            BinaryOp::EndsWith => write!(f, "ENDS WITH"),
        }
    }
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    UnaryPlus,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Not => write!(f, "!"),
            UnaryOp::Negate => write!(f, "-"),
            UnaryOp::UnaryPlus => write!(f, "+"),
        }
    }
}

/// Aggregate function type used in expression AST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateExprFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

impl fmt::Display for AggregateExprFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateExprFunction::Count => write!(f, "COUNT"),
            AggregateExprFunction::Sum => write!(f, "SUM"),
            AggregateExprFunction::Avg => write!(f, "AVG"),
            AggregateExprFunction::Min => write!(f, "MIN"),
            AggregateExprFunction::Max => write!(f, "MAX"),
            AggregateExprFunction::Collect => write!(f, "COLLECT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_expression() {
        let expr = Expression::Literal(Value::Integer(42));
        assert_eq!(expr.to_string(), "42");
    }

    #[test]
    fn test_variable_expression() {
        let expr = Expression::Variable("x".to_string());
        assert_eq!(expr.to_string(), "?x");
    }

    #[test]
    fn test_binary_op_display() {
        let expr = Expression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(1))),
        };
        assert_eq!(expr.to_string(), "(?x + 1)");
    }

    #[test]
    fn test_function_call_display() {
        let expr = Expression::FunctionCall {
            name: "strlen".to_string(),
            args: vec![Expression::Variable("name".to_string())],
        };
        assert_eq!(expr.to_string(), "strlen(?name)");
    }

    #[test]
    fn test_aggregate_display() {
        let expr = Expression::Aggregate {
            function: AggregateExprFunction::Count,
            argument: Box::new(Expression::Variable("x".to_string())),
            distinct: true,
        };
        assert_eq!(expr.to_string(), "COUNT(DISTINCT ?x)");
    }

    #[test]
    fn test_property_access_display() {
        let expr = Expression::PropertyAccess {
            variable: "n".to_string(),
            property: "name".to_string(),
        };
        assert_eq!(expr.to_string(), "n.name");
    }

    #[test]
    fn test_nested_expression() {
        let expr = Expression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expression::Variable("temp".to_string())),
                right: Box::new(Expression::Literal(Value::Integer(30))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOp::Lt,
                left: Box::new(Expression::Variable("temp".to_string())),
                right: Box::new(Expression::Literal(Value::Integer(100))),
            }),
        };
        let display = expr.to_string();
        assert!(display.contains("&&"));
        assert!(display.contains(">"));
        assert!(display.contains("<"));
    }
}
