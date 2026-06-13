//! Expression evaluator that evaluates `Expression` trees against `BindingSet` bindings.
//!
//! Follows SPARQL semantics:
//! - Variable lookup strips `?`/`$` prefix, returns `Value::Null` if unbound
//! - Null propagation: null in arithmetic/comparison → null
//! - Three-valued logic: `null AND false = false`, `null OR true = true`
//! - Type coercion: numeric comparison with Integer→Float promotion

use std::collections::HashMap;

use cqels_model::{BindingSet, IriTerm, Term, Value};

use super::ast::{AggregateExprFunction, BinaryOp, Expression, UnaryOp};
use super::functions::{call_builtin, value_to_bool};

/// Evaluates expression trees against variable bindings.
///
/// # Examples
///
/// ```
/// use cqels_core::expression::evaluator::ExpressionEvaluator;
/// use cqels_core::expression::ast::{Expression, BinaryOp};
/// use cqels_model::{BindingSet, Value};
///
/// let evaluator = ExpressionEvaluator::new();
///
/// // Evaluate a variable lookup
/// let mut bs = BindingSet::new(0);
/// bs.insert("x", Value::Integer(42));
/// let expr = Expression::Variable("?x".into());
/// assert_eq!(evaluator.evaluate(&expr, &bs), Value::Integer(42));
///
/// // Evaluate a comparison
/// let gt_expr = Expression::BinaryOp {
///     op: BinaryOp::Gt,
///     left: Box::new(Expression::Variable("?x".into())),
///     right: Box::new(Expression::Literal(Value::Integer(10))),
/// };
/// assert!(evaluator.evaluate_as_bool(&gt_expr, &bs));
/// ```
#[derive(Clone)]
pub struct ExpressionEvaluator {
    prefixes: HashMap<String, String>,
}

impl ExpressionEvaluator {
    /// Creates a new evaluator with no prefixes.
    pub fn new() -> Self {
        Self {
            prefixes: HashMap::new(),
        }
    }

    /// Creates a new evaluator with the given prefix mappings.
    pub fn with_prefixes(prefixes: HashMap<String, String>) -> Self {
        Self { prefixes }
    }

    /// Returns a reference to the prefix map.
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Evaluates an expression against bindings and returns the result value.
    pub fn evaluate(&self, expr: &Expression, bindings: &BindingSet) -> Value {
        match expr {
            Expression::Literal(v) => v.clone(),
            Expression::PrefixedIri(name) => {
                // Same fallback contract as prefixed FUNCTION names: unknown
                // prefixes keep the raw text (a CURIE like `ex:x` is itself a
                // syntactically valid IRI with scheme `ex`).
                Value::Term(Term::Iri(IriTerm::new(self.resolve_prefixed_name(name))))
            }

            Expression::Variable(name) => {
                let var_name = name
                    .strip_prefix('?')
                    .or_else(|| name.strip_prefix('$'))
                    .unwrap_or(name);
                bindings.get(var_name).cloned().unwrap_or(Value::Null)
            }

            Expression::PropertyAccess { variable, property } => {
                // Cypher-style: look up "variable.property" as a binding key
                let key = format!("{variable}.{property}");
                bindings.get(&key).cloned().unwrap_or(Value::Null)
            }

            Expression::BinaryOp { op, left, right } => {
                self.eval_binary_op(*op, left, right, bindings)
            }

            Expression::UnaryOp { op, operand } => {
                let val = self.evaluate(operand, bindings);
                match op {
                    UnaryOp::Not => {
                        if val.is_null() {
                            Value::Null
                        } else {
                            Value::Boolean(!value_to_bool(&val))
                        }
                    }
                    UnaryOp::Negate => match val {
                        // checked_neg: -i64::MIN overflows (numeric error →
                        // Null), and `-i` would panic in debug builds.
                        Value::Integer(i) => {
                            i.checked_neg().map(Value::Integer).unwrap_or(Value::Null)
                        }
                        Value::Float(f) => Value::Float(-f),
                        Value::Null => Value::Null,
                        _ => Value::Null,
                    },
                    UnaryOp::UnaryPlus => match val {
                        Value::Integer(_) | Value::Float(_) => val,
                        Value::Null => Value::Null,
                        _ => Value::Null,
                    },
                }
            }

            Expression::FunctionCall { name, args } => {
                let evaluated_args: Vec<Value> =
                    args.iter().map(|a| self.evaluate(a, bindings)).collect();

                // Try to resolve prefixed function name
                let resolved_name = self.resolve_prefixed_name(name);
                call_builtin(&resolved_name, &evaluated_args)
            }

            Expression::Bound(var_name) => {
                let name = var_name
                    .strip_prefix('?')
                    .or_else(|| var_name.strip_prefix('$'))
                    .unwrap_or(var_name);
                let val = bindings.get(name);
                Value::Boolean(val.is_some() && !matches!(val, Some(Value::Null)))
            }

            Expression::If {
                condition,
                then_expr,
                else_expr,
            } => {
                let cond = self.evaluate(condition, bindings);
                if value_to_bool(&cond) {
                    self.evaluate(then_expr, bindings)
                } else {
                    self.evaluate(else_expr, bindings)
                }
            }

            Expression::Aggregate {
                function, argument, ..
            } => {
                // Single-row aggregate evaluation: just return the argument value
                // Actual aggregation happens at the pipeline level
                let val = self.evaluate(argument, bindings);
                match function {
                    AggregateExprFunction::Count => {
                        if val.is_null() {
                            Value::Integer(0)
                        } else {
                            Value::Integer(1)
                        }
                    }
                    _ => val,
                }
            }
        }
    }

    /// Evaluates an expression and coerces the result to a boolean.
    ///
    /// SPARQL effective boolean value (EBV):
    /// - `null` → `false`
    /// - Boolean values → as-is
    /// - Numeric 0 / NaN → false, others → true
    /// - Empty string → false, non-empty → true
    pub fn evaluate_as_bool(&self, expr: &Expression, bindings: &BindingSet) -> bool {
        let val = self.evaluate(expr, bindings);
        value_to_bool(&val)
    }

    /// Evaluates a binary operation with proper SPARQL semantics.
    fn eval_binary_op(
        &self,
        op: BinaryOp,
        left: &Expression,
        right: &Expression,
        bindings: &BindingSet,
    ) -> Value {
        // Short-circuit evaluation for logical operators (three-valued logic)
        match op {
            BinaryOp::And => return self.eval_and(left, right, bindings),
            BinaryOp::Or => return self.eval_or(left, right, bindings),
            _ => {}
        }

        let lval = self.evaluate(left, bindings);
        let rval = self.evaluate(right, bindings);

        // Null propagation for non-logical operators
        // SPARQL three-valued logic: null in any comparison/arithmetic → null
        if lval.is_null() || rval.is_null() {
            return Value::Null;
        }

        match op {
            // Comparison operators
            BinaryOp::Eq => Value::Boolean(values_equal(&lval, &rval)),
            BinaryOp::Neq => Value::Boolean(!values_equal(&lval, &rval)),
            // Ordering on IRIs/blank nodes is a SPARQL type error (§17.3
            // defines no ordering for them) → null per D-E1; do NOT fall
            // through to Value::PartialOrd's lexicographic Term ordering.
            // parity unit 5.
            BinaryOp::Lt | BinaryOp::Gt | BinaryOp::Lte | BinaryOp::Gte
                if matches!(lval, Value::Term(_)) || matches!(rval, Value::Term(_)) =>
            {
                Value::Null
            }
            BinaryOp::Lt => Value::Boolean(lval < rval),
            BinaryOp::Gt => Value::Boolean(lval > rval),
            BinaryOp::Lte => Value::Boolean(lval <= rval),
            BinaryOp::Gte => Value::Boolean(lval >= rval),

            // Arithmetic operators (exact i64 fast-path first — the f64 path
            // rounds beyond 2^53, so i64::MAX - 1 silently lost the unit)
            BinaryOp::Add => eval_int_arithmetic(&lval, &rval, i64::checked_add)
                .unwrap_or_else(|| eval_arithmetic(&lval, &rval, |a, b| a + b)),
            BinaryOp::Sub => eval_int_arithmetic(&lval, &rval, i64::checked_sub)
                .unwrap_or_else(|| eval_arithmetic(&lval, &rval, |a, b| a - b)),
            BinaryOp::Mul => eval_int_arithmetic(&lval, &rval, i64::checked_mul)
                .unwrap_or_else(|| eval_arithmetic(&lval, &rval, |a, b| a * b)),
            BinaryOp::Div => match (&lval, &rval) {
                // Exact integer division: a whole quotient stays integer and
                // must not round through f64 (9223372036854775806 / 1 lost
                // precision). checked_rem is None for rhs == 0 (division by
                // zero → null) and for the i64::MIN / -1 overflow (numeric
                // error → null), covering both edge cases in one match.
                (Value::Integer(a), Value::Integer(b)) => match a.checked_rem(*b) {
                    Some(0) => a.checked_div(*b).map(Value::Integer).unwrap_or(Value::Null),
                    Some(_) => Value::Float(*a as f64 / *b as f64),
                    None => Value::Null,
                },
                _ => match rval.as_numeric() {
                    // Division by zero → null
                    Some(0.0) => Value::Null,
                    _ => eval_arithmetic(&lval, &rval, |a, b| a / b),
                },
            },
            BinaryOp::Mod => match (&lval, &rval) {
                // Same exact-i64 treatment as Add/Sub/Mul/Div (local review on
                // cqels-rs#94): the f64 path loses exactness beyond 2^53.
                // checked_rem is None for rhs == 0 and the i64::MIN % -1
                // overflow — both numeric errors → null. Unreachable from the
                // CqelsQL grammar today (no `%` in multiplicative_op) but kept
                // consistent so it's correct the day a dialect exposes it.
                (Value::Integer(a), Value::Integer(b)) => {
                    a.checked_rem(*b).map(Value::Integer).unwrap_or(Value::Null)
                }
                _ => match rval.as_numeric() {
                    Some(0.0) => Value::Null,
                    _ => eval_arithmetic(&lval, &rval, |a, b| a % b),
                },
            },

            // String operators
            BinaryOp::Contains => match (lval.as_string(), rval.as_string()) {
                (Some(h), Some(n)) => Value::Boolean(h.contains(n)),
                _ => Value::Null,
            },
            BinaryOp::StartsWith => match (lval.as_string(), rval.as_string()) {
                (Some(s), Some(p)) => Value::Boolean(s.starts_with(p)),
                _ => Value::Null,
            },
            BinaryOp::EndsWith => match (lval.as_string(), rval.as_string()) {
                (Some(s), Some(sf)) => Value::Boolean(s.ends_with(sf)),
                _ => Value::Null,
            },

            // Logical handled above
            BinaryOp::And | BinaryOp::Or => unreachable!(),
        }
    }

    /// Three-valued AND: `null AND false = false`, `null AND true = null`
    fn eval_and(&self, left: &Expression, right: &Expression, bindings: &BindingSet) -> Value {
        let lval = self.evaluate(left, bindings);
        let l_bool = if lval.is_null() {
            None
        } else {
            Some(value_to_bool(&lval))
        };

        // Short-circuit: false AND anything = false
        if l_bool == Some(false) {
            return Value::Boolean(false);
        }

        let rval = self.evaluate(right, bindings);
        let r_bool = if rval.is_null() {
            None
        } else {
            Some(value_to_bool(&rval))
        };

        match (l_bool, r_bool) {
            (Some(true), Some(true)) => Value::Boolean(true),
            (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
            _ => Value::Null, // at least one is null, neither is false
        }
    }

    /// Three-valued OR: `null OR true = true`, `null OR false = null`
    fn eval_or(&self, left: &Expression, right: &Expression, bindings: &BindingSet) -> Value {
        let lval = self.evaluate(left, bindings);
        let l_bool = if lval.is_null() {
            None
        } else {
            Some(value_to_bool(&lval))
        };

        // Short-circuit: true OR anything = true
        if l_bool == Some(true) {
            return Value::Boolean(true);
        }

        let rval = self.evaluate(right, bindings);
        let r_bool = if rval.is_null() {
            None
        } else {
            Some(value_to_bool(&rval))
        };

        match (l_bool, r_bool) {
            (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
            (Some(false), Some(false)) => Value::Boolean(false),
            _ => Value::Null, // at least one is null, neither is true
        }
    }

    /// Resolves a prefixed name (e.g., `xsd:integer`) using the prefix map.
    fn resolve_prefixed_name(&self, name: &str) -> String {
        if let Some(colon_pos) = name.find(':') {
            let prefix = &name[..colon_pos];
            let local = &name[colon_pos + 1..];
            if let Some(uri) = self.prefixes.get(prefix) {
                return format!("{uri}{local}");
            }
        }
        name.to_string()
    }
}

impl Default for ExpressionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// Equality comparison with type promotion (Integer ↔ Float).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // Direct equality
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,

        // Numeric promotion
        (Value::Integer(x), Value::Float(y)) => (*x as f64) == *y,
        (Value::Float(x), Value::Integer(y)) => *x == (*y as f64),

        // Term equality (SPARQL RDFterm-equal / sameTerm). A string literal
        // and an IRI are DIFFERENT term kinds and never equal — the old
        // String-Term text comparison made "http://x" = <http://x> true,
        // which both SPARQL and the Java engine reject. parity unit 5.
        (Value::Term(x), Value::Term(y)) => x == y,

        _ => false,
    }
}

/// Exact i64 fast-path for `+ - *` when both operands are integers, keeping
/// results exact beyond f64's 2^53 integer window (parity unit 5). Overflow
/// is a numeric error (Null) per XPath err:FOAR0002 — never silently rounded
/// through the float path. Returns None when either operand is not an
/// Integer so the caller falls back to float arithmetic.
fn eval_int_arithmetic(a: &Value, b: &Value, op: fn(i64, i64) -> Option<i64>) -> Option<Value> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => {
            Some(op(*x, *y).map(Value::Integer).unwrap_or(Value::Null))
        }
        _ => None,
    }
}

/// Arithmetic with Integer→Float promotion.
fn eval_arithmetic(a: &Value, b: &Value, f: fn(f64, f64) -> f64) -> Value {
    match (a, b) {
        // Both integers: try to stay integer
        (Value::Integer(x), Value::Integer(y)) => {
            let result = f(*x as f64, *y as f64);
            if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
                Value::Integer(result as i64)
            } else {
                Value::Float(result)
            }
        }
        // At least one float → float result
        _ => match (a.as_numeric(), b.as_numeric()) {
            (Some(x), Some(y)) => Value::Float(f(x, y)),
            _ => Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bindings(vars: &[(&str, Value)]) -> BindingSet {
        let mut bs = BindingSet::new(0);
        for (name, val) in vars {
            bs.insert(*name, val.clone());
        }
        bs
    }

    #[test]
    fn test_prefixed_iri_resolves_to_term_via_prefix_map() {
        // codex P2 + local review on cqels-rs#94: `ex:x` primaries previously
        // evaluated to Value::String("ex:x") and could never equal an IRI
        // binding; they must resolve through the evaluator's prefix map to
        // the same Term the angle-bracket iri_ref path produces.
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());
        let eval = ExpressionEvaluator::with_prefixes(prefixes);

        let expr = Expression::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expression::Variable("p".to_string())),
            right: Box::new(Expression::PrefixedIri("ex:x".to_string())),
        };
        let bs = make_bindings(&[(
            "p",
            Value::Term(Term::Iri(IriTerm::new("http://example.org/x"))),
        )]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));

        // Unknown prefix keeps the raw text (CURIE is itself a valid IRI) —
        // same fallback contract as prefixed function names.
        let raw = Expression::PrefixedIri("nope:x".to_string());
        assert_eq!(
            eval.evaluate(&raw, &make_bindings(&[])),
            Value::Term(Term::Iri(IriTerm::new("nope:x")))
        );
    }

    #[test]
    fn test_evaluate_literal() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Literal(Value::Integer(42));
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(42));
    }

    #[test]
    fn test_evaluate_variable() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Variable("x".to_string());
        let bs = make_bindings(&[("x", Value::Integer(10))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(10));
    }

    #[test]
    fn test_evaluate_variable_with_prefix() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Variable("?x".to_string());
        let bs = make_bindings(&[("x", Value::Integer(10))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(10));
    }

    #[test]
    fn test_evaluate_unbound_variable() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Variable("y".to_string());
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_binary_add() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = make_bindings(&[("x", Value::Integer(10))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(15));
    }

    #[test]
    fn test_evaluate_binary_comparison() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expression::Variable("temp".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(30))),
        };

        let bs_high = make_bindings(&[("temp", Value::Integer(42))]);
        assert_eq!(eval.evaluate(&expr, &bs_high), Value::Boolean(true));

        let bs_low = make_bindings(&[("temp", Value::Integer(20))]);
        assert_eq!(eval.evaluate(&expr, &bs_low), Value::Boolean(false));
    }

    #[test]
    fn test_evaluate_numeric_promotion() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expression::Literal(Value::Integer(5))),
            right: Box::new(Expression::Literal(Value::Float(5.0))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));
    }

    #[test]
    fn test_null_propagation_arithmetic() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0); // x is unbound → null
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_null_comparison_returns_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0); // x is unbound → null
                                     // SPARQL: null in comparison → null (evaluates to false in FILTER context)
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_null_equality_returns_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Eq,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);

        // !(null = 5) should also be null, not true
        let not_expr = Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(expr),
        };
        assert_eq!(eval.evaluate(&not_expr, &bs), Value::Null);
    }

    #[test]
    fn test_three_valued_and() {
        let eval = ExpressionEvaluator::new();

        // null AND false = false
        let expr = Expression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expression::Variable("x".to_string())), // null
            right: Box::new(Expression::Literal(Value::Boolean(false))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(false));

        // null AND true = null
        let expr2 = Expression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(Expression::Variable("x".to_string())), // null
            right: Box::new(Expression::Literal(Value::Boolean(true))),
        };
        assert_eq!(eval.evaluate(&expr2, &bs), Value::Null);
    }

    #[test]
    fn test_three_valued_or() {
        let eval = ExpressionEvaluator::new();

        // null OR true = true
        let expr = Expression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(Expression::Variable("x".to_string())), // null
            right: Box::new(Expression::Literal(Value::Boolean(true))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));

        // null OR false = null
        let expr2 = Expression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(Expression::Variable("x".to_string())), // null
            right: Box::new(Expression::Literal(Value::Boolean(false))),
        };
        assert_eq!(eval.evaluate(&expr2, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_not() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expression::Literal(Value::Boolean(true))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(false));
    }

    #[test]
    fn test_evaluate_negate() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(-5));
    }

    #[test]
    fn test_evaluate_bound() {
        let eval = ExpressionEvaluator::new();

        let expr = Expression::Bound("x".to_string());

        let bs_bound = make_bindings(&[("x", Value::Integer(1))]);
        assert_eq!(eval.evaluate(&expr, &bs_bound), Value::Boolean(true));

        let bs_unbound = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs_unbound), Value::Boolean(false));
    }

    #[test]
    fn test_evaluate_if() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::If {
            condition: Box::new(Expression::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expression::Variable("x".to_string())),
                right: Box::new(Expression::Literal(Value::Integer(10))),
            }),
            then_expr: Box::new(Expression::Literal(Value::String("high".into()))),
            else_expr: Box::new(Expression::Literal(Value::String("low".into()))),
        };

        let bs_high = make_bindings(&[("x", Value::Integer(20))]);
        assert_eq!(eval.evaluate(&expr, &bs_high), Value::String("high".into()));

        let bs_low = make_bindings(&[("x", Value::Integer(5))]);
        assert_eq!(eval.evaluate(&expr, &bs_low), Value::String("low".into()));
    }

    #[test]
    fn test_evaluate_function_call() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::FunctionCall {
            name: "strlen".to_string(),
            args: vec![Expression::Variable("name".to_string())],
        };
        let bs = make_bindings(&[("name", Value::String("hello".into()))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(5));
    }

    #[test]
    fn test_evaluate_as_bool() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expression::Variable("x".to_string())),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };

        let bs = make_bindings(&[("x", Value::Integer(10))]);
        assert!(eval.evaluate_as_bool(&expr, &bs));

        let bs2 = make_bindings(&[("x", Value::Integer(3))]);
        assert!(!eval.evaluate_as_bool(&expr, &bs2));
    }

    #[test]
    fn test_division_by_zero() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(Expression::Literal(Value::Integer(10))),
            right: Box::new(Expression::Literal(Value::Integer(0))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_string_contains_operator() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Contains,
            left: Box::new(Expression::Variable("s".to_string())),
            right: Box::new(Expression::Literal(Value::String("world".into()))),
        };
        let bs = make_bindings(&[("s", Value::String("hello world".into()))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));
    }

    #[test]
    fn test_property_access() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::PropertyAccess {
            variable: "n".to_string(),
            property: "name".to_string(),
        };
        let bs = make_bindings(&[("n.name", Value::String("Alice".into()))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::String("Alice".into()));
    }

    #[test]
    fn test_with_prefixes() {
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());
        let eval = ExpressionEvaluator::with_prefixes(prefixes);
        assert_eq!(
            eval.resolve_prefixed_name("ex:test"),
            "http://example.org/test"
        );
    }

    #[test]
    fn test_complex_expression() {
        let eval = ExpressionEvaluator::new();
        // (?temp > 30) && (?temp < 100)
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

        let bs = make_bindings(&[("temp", Value::Integer(50))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));

        let bs2 = make_bindings(&[("temp", Value::Integer(20))]);
        assert_eq!(eval.evaluate(&expr, &bs2), Value::Boolean(false));

        let bs3 = make_bindings(&[("temp", Value::Integer(150))]);
        assert_eq!(eval.evaluate(&expr, &bs3), Value::Boolean(false));
    }

    #[test]
    fn test_evaluate_subtraction() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Sub,
            left: Box::new(Expression::Literal(Value::Integer(10))),
            right: Box::new(Expression::Literal(Value::Integer(3))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(7));
    }

    #[test]
    fn test_evaluate_multiplication() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(Expression::Literal(Value::Integer(4))),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(20));
    }

    #[test]
    fn test_evaluate_modulo() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Mod,
            left: Box::new(Expression::Literal(Value::Integer(10))),
            right: Box::new(Expression::Literal(Value::Integer(3))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(1));
    }

    #[test]
    fn test_evaluate_modulo_by_zero() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Mod,
            left: Box::new(Expression::Literal(Value::Integer(10))),
            right: Box::new(Expression::Literal(Value::Integer(0))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_mixed_int_float_arithmetic() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expression::Literal(Value::Integer(5))),
            right: Box::new(Expression::Literal(Value::Float(2.5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Float(7.5));
    }

    #[test]
    fn test_evaluate_string_starts_with() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::StartsWith,
            left: Box::new(Expression::Literal(Value::String("hello world".into()))),
            right: Box::new(Expression::Literal(Value::String("hello".into()))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_string_ends_with() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::EndsWith,
            left: Box::new(Expression::Literal(Value::String("hello world".into()))),
            right: Box::new(Expression::Literal(Value::String("world".into()))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_unary_plus() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(Expression::Literal(Value::Integer(5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(5));
    }

    #[test]
    fn test_evaluate_unary_plus_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(Expression::Variable("x".to_string())),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_negate_float() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(Expression::Literal(Value::Float(2.5))),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Float(-2.5));
    }

    #[test]
    fn test_evaluate_negate_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(Expression::Variable("x".to_string())),
        };
        let bs = BindingSet::new(0);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_not_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expression::Variable("x".to_string())),
        };
        let bs = BindingSet::new(0); // x is unbound
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_bound_with_null_value() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Bound("x".to_string());
        let bs = make_bindings(&[("x", Value::Null)]);
        // BOUND returns false for null-valued bindings
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(false));
    }

    #[test]
    fn test_evaluate_bound_dollar_prefix() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Bound("$x".to_string());
        let bs = make_bindings(&[("x", Value::Integer(1))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_variable_dollar_prefix() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Variable("$y".to_string());
        let bs = make_bindings(&[("y", Value::Integer(99))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(99));
    }

    #[test]
    fn test_evaluate_aggregate_count_non_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Aggregate {
            function: AggregateExprFunction::Count,
            argument: Box::new(Expression::Variable("x".to_string())),
            distinct: false,
        };
        let bs = make_bindings(&[("x", Value::Integer(5))]);
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(1));
    }

    #[test]
    fn test_evaluate_aggregate_count_null() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Aggregate {
            function: AggregateExprFunction::Count,
            argument: Box::new(Expression::Variable("x".to_string())),
            distinct: false,
        };
        let bs = BindingSet::new(0); // x is unbound
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(0));
    }

    #[test]
    fn test_evaluate_aggregate_sum() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::Aggregate {
            function: AggregateExprFunction::Sum,
            argument: Box::new(Expression::Variable("x".to_string())),
            distinct: false,
        };
        let bs = make_bindings(&[("x", Value::Integer(42))]);
        // Single-row: just returns the value
        assert_eq!(eval.evaluate(&expr, &bs), Value::Integer(42));
    }

    #[test]
    fn test_evaluate_comparison_lte_gte() {
        let eval = ExpressionEvaluator::new();

        let lte = Expression::BinaryOp {
            op: BinaryOp::Lte,
            left: Box::new(Expression::Literal(Value::Integer(5))),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        assert_eq!(
            eval.evaluate(&lte, &BindingSet::new(0)),
            Value::Boolean(true)
        );

        let gte = Expression::BinaryOp {
            op: BinaryOp::Gte,
            left: Box::new(Expression::Literal(Value::Integer(5))),
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        assert_eq!(
            eval.evaluate(&gte, &BindingSet::new(0)),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_evaluate_neq() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Neq,
            left: Box::new(Expression::Literal(Value::Integer(1))),
            right: Box::new(Expression::Literal(Value::Integer(2))),
        };
        assert_eq!(
            eval.evaluate(&expr, &BindingSet::new(0)),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_evaluate_default() {
        let eval = ExpressionEvaluator::default();
        assert!(eval.prefixes().is_empty());
    }

    #[test]
    fn test_evaluate_as_bool_null_is_false() {
        let eval = ExpressionEvaluator::new();
        // Unbound variable evaluates to null, which is false in EBV
        let expr = Expression::Variable("x".to_string());
        assert!(!eval.evaluate_as_bool(&expr, &BindingSet::new(0)));
    }

    #[test]
    fn test_evaluate_string_contains_non_string() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::BinaryOp {
            op: BinaryOp::Contains,
            left: Box::new(Expression::Literal(Value::Integer(42))),
            right: Box::new(Expression::Literal(Value::String("4".into()))),
        };
        let bs = BindingSet::new(0);
        // Non-string operand → Null
        assert_eq!(eval.evaluate(&expr, &bs), Value::Null);
    }

    #[test]
    fn test_evaluate_if_null_condition() {
        let eval = ExpressionEvaluator::new();
        let expr = Expression::If {
            condition: Box::new(Expression::Variable("x".to_string())), // null
            then_expr: Box::new(Expression::Literal(Value::String("yes".into()))),
            else_expr: Box::new(Expression::Literal(Value::String("no".into()))),
        };
        let bs = BindingSet::new(0);
        // null is falsy → else branch
        assert_eq!(eval.evaluate(&expr, &bs), Value::String("no".into()));
    }
}
