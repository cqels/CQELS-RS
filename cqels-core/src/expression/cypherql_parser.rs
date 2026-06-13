//! CypherQL expression parser submodule.
//!
//! Contains the pest parser struct for CypherQL grammar and all
//! CypherQL-specific expression parsing functions.

use pest::Parser;
use pest_derive::Parser;

use cqels_model::Value;

use super::ast::{AggregateExprFunction, BinaryOp, Expression, UnaryOp};
use crate::parser::{ParseError, ParseResult};

#[derive(Parser)]
#[grammar = "parser/cypherql.pest"]
struct CypherQlExprParser;

/// Parses a CypherQL expression string into an Expression tree.
pub(crate) fn parse(input: &str) -> ParseResult<Expression> {
    let pairs = CypherQlExprParser::parse(Rule::standalone_expression, input)
        .map_err(|e| ParseError::Syntax(format!("expression parse error: {e}")))?;

    for pair in pairs {
        if pair.as_rule() == Rule::standalone_expression {
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::expression {
                    return parse_expression(inner);
                }
            }
        }
    }
    Err(ParseError::Syntax("empty expression".into()))
}

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let inner = pair.into_inner();
    for child in inner {
        if child.as_rule() == Rule::or_expression {
            return parse_or(child);
        }
    }
    Err(ParseError::Syntax("invalid Cypher expression".into()))
}

fn parse_or(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty OR expression".into()));
    }

    let operands: Vec<pest::iterators::Pair<Rule>> = children
        .into_iter()
        .filter(|c| c.as_rule() != Rule::or_kw)
        .collect();

    let mut iter = operands.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing OR operand".into()))?;
    let mut result = parse_and(first)?;

    for child in iter {
        let right = parse_and(child)?;
        result = Expression::BinaryOp {
            op: BinaryOp::Or,
            left: Box::new(result),
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn parse_and(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty AND expression".into()));
    }

    let operands: Vec<pest::iterators::Pair<Rule>> = children
        .into_iter()
        .filter(|c| c.as_rule() != Rule::and_kw)
        .collect();

    let mut iter = operands.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing AND operand".into()))?;
    let mut result = parse_not(first)?;

    for child in iter {
        let right = parse_not(child)?;
        result = Expression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(result),
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn parse_not(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty NOT expression".into()));
    }

    let has_not = children.iter().any(|c| c.as_rule() == Rule::not_kw);
    let comparison = children
        .into_iter()
        .find(|c| c.as_rule() == Rule::comparison_expression)
        .ok_or_else(|| ParseError::Syntax("expected comparison expression".into()))?;

    let expr = parse_comparison(comparison)?;

    if has_not {
        Ok(Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(expr),
        })
    } else {
        Ok(expr)
    }
}

fn parse_comparison(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty comparison".into()));
    }

    let mut iter = children.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing comparison operand".into()))?;
    let left = parse_add(first)?;

    if let Some(op_pair) = iter.next() {
        let op = parse_comparison_op(op_pair)?;
        let right = parse_add(
            iter.next()
                .ok_or_else(|| ParseError::Syntax("expected right operand".into()))?,
        )?;
        Ok(Expression::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_comparison_op(pair: pest::iterators::Pair<Rule>) -> ParseResult<BinaryOp> {
    let text = pair.as_str().trim();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::contains_kw => return Ok(BinaryOp::Contains),
            Rule::starts_with_kw => return Ok(BinaryOp::StartsWith),
            Rule::ends_with_kw => return Ok(BinaryOp::EndsWith),
            _ => {}
        }
    }

    match text {
        "<>" | "!=" => Ok(BinaryOp::Neq),
        "<=" => Ok(BinaryOp::Lte),
        ">=" => Ok(BinaryOp::Gte),
        "=" => Ok(BinaryOp::Eq),
        "<" => Ok(BinaryOp::Lt),
        ">" => Ok(BinaryOp::Gt),
        _ => {
            let lower = text.to_lowercase();
            if lower.contains("contains") {
                Ok(BinaryOp::Contains)
            } else if lower.contains("starts") {
                Ok(BinaryOp::StartsWith)
            } else if lower.contains("ends") {
                Ok(BinaryOp::EndsWith)
            } else {
                Err(ParseError::Syntax(format!(
                    "unknown comparison operator: {}",
                    text
                )))
            }
        }
    }
}

fn parse_add(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty add expression".into()));
    }

    let mut iter = children.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing add operand".into()))?;
    let mut result = parse_multiply(first)?;

    while let Some(next) = iter.next() {
        let text = next.as_str();
        if text == "+" || text == "-" {
            if let Some(operand) = iter.next() {
                let op = if text == "+" {
                    BinaryOp::Add
                } else {
                    BinaryOp::Sub
                };
                let right = parse_multiply(operand)?;
                result = Expression::BinaryOp {
                    op,
                    left: Box::new(result),
                    right: Box::new(right),
                };
            }
        } else {
            // Not an operator — treat as next multiply term
            let right = parse_multiply(next)?;
            result = Expression::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(result),
                right: Box::new(right),
            };
        }
    }

    Ok(result)
}

fn parse_multiply(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty multiply expression".into()));
    }

    let mut iter = children.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing multiply operand".into()))?;
    let mut result = parse_power(first)?;

    while let Some(next) = iter.next() {
        let text = next.as_str();
        if text == "*" || text == "/" || text == "%" {
            if let Some(operand) = iter.next() {
                let op = match text {
                    "*" => BinaryOp::Mul,
                    "/" => BinaryOp::Div,
                    "%" => BinaryOp::Mod,
                    _ => unreachable!(),
                };
                let right = parse_power(operand)?;
                result = Expression::BinaryOp {
                    op,
                    left: Box::new(result),
                    right: Box::new(right),
                };
            }
        } else {
            // Not an operator — treat as next power term
            let right = parse_power(next)?;
            result = Expression::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(result),
                right: Box::new(right),
            };
        }
    }

    Ok(result)
}

fn parse_power(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut iter = pair.into_inner();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("empty power expression".into()))?;
    let base = parse_unary(first)?;

    // The grammar surfaces an optional `power_op` ("^") pair followed by the
    // exponent operand: `unary_expression ~ (power_op ~ unary_expression)?`.
    // Consume the operator pair by content (like parse_add/parse_multiply — no
    // index games) and the exponent that follows it. Single-exponent only: the
    // grammar uses `?`, not a repeat, so there is no chaining at this level.
    match iter.next() {
        Some(next) if next.as_str() == "^" => {
            let exponent_pair = iter
                .next()
                .ok_or_else(|| ParseError::Syntax("missing power exponent".into()))?;
            let exponent = parse_unary(exponent_pair)?;
            Ok(Expression::FunctionCall {
                name: "power".to_string(),
                args: vec![base, exponent],
            })
        }
        _ => Ok(base),
    }
}

fn parse_unary(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let text = pair.as_str().trim();
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::Syntax("empty unary expression".into()));
    }

    let first_text = children[0].as_str();
    if first_text == "-" && children.len() > 1 {
        let operand_pair = children
            .into_iter()
            .last()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }
    if first_text == "+" && children.len() > 1 {
        let operand_pair = children
            .into_iter()
            .last()
            .ok_or_else(|| ParseError::Syntax("missing unary plus operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(operand),
        });
    }

    if text.starts_with('-') && children.len() == 1 {
        let operand_pair = children
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }

    let first = children
        .into_iter()
        .next()
        .ok_or_else(|| ParseError::Syntax("missing unary operand".into()))?;
    parse_atom(first)
}

fn parse_atom(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    match pair.as_rule() {
        Rule::atom => {
            let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
            if children.is_empty() {
                return Err(ParseError::Syntax("empty atom".into()));
            }

            // Check for property access: identifier ~ ("." ~ identifier)*
            if children[0].as_rule() == Rule::identifier {
                if children.len() == 1 {
                    return Ok(Expression::Variable(children[0].as_str().to_string()));
                }
                let variable = children[0].as_str().to_string();
                let properties: Vec<&str> = children[1..]
                    .iter()
                    .filter(|c| c.as_rule() == Rule::identifier)
                    .map(|c| c.as_str())
                    .collect();
                if !properties.is_empty() {
                    return Ok(Expression::PropertyAccess {
                        variable,
                        property: properties.join("."),
                    });
                }
                return Ok(Expression::Variable(variable));
            }

            let first = children
                .into_iter()
                .next()
                .ok_or_else(|| ParseError::Syntax("empty atom".into()))?;
            parse_atom(first)
        }

        Rule::literal => parse_literal(pair),

        Rule::function_invocation => {
            let mut children = pair.into_inner();
            let name = children
                .next()
                .ok_or_else(|| ParseError::Syntax("function requires name".into()))?
                .as_str()
                .to_string();

            let name_lower = name.to_lowercase();
            let agg_fn = match name_lower.as_str() {
                "count" => Some(AggregateExprFunction::Count),
                "sum" => Some(AggregateExprFunction::Sum),
                "avg" => Some(AggregateExprFunction::Avg),
                "min" => Some(AggregateExprFunction::Min),
                "max" => Some(AggregateExprFunction::Max),
                "collect" => Some(AggregateExprFunction::Collect),
                "group_concat" => Some(AggregateExprFunction::GroupConcat),
                _ => None,
            };

            let mut distinct = false;
            let mut args = Vec::new();
            for child in children {
                if child.as_rule() == Rule::distinct_kw {
                    distinct = true;
                } else if child.as_rule() == Rule::expression {
                    args.push(parse_expression(child)?);
                }
            }

            if let Some(func) = agg_fn {
                let argument = args
                    .into_iter()
                    .next()
                    .unwrap_or(Expression::Literal(Value::String("*".to_string())));
                Ok(Expression::Aggregate {
                    function: func,
                    argument: Box::new(argument),
                    distinct,
                })
            } else {
                Ok(Expression::FunctionCall { name, args })
            }
        }

        Rule::identifier => Ok(Expression::Variable(pair.as_str().to_string())),

        Rule::expression => parse_expression(pair),
        Rule::or_expression => parse_or(pair),
        Rule::and_expression => parse_and(pair),
        Rule::not_expression => parse_not(pair),
        Rule::comparison_expression => parse_comparison(pair),
        Rule::add_expression => parse_add(pair),
        Rule::multiply_expression => parse_multiply(pair),
        Rule::power_expression => parse_power(pair),
        Rule::unary_expression => parse_unary(pair),
        Rule::parameter => {
            let var = pair
                .into_inner()
                .next()
                .map(|p| p.as_str().to_string())
                .unwrap_or_default();
            Ok(Expression::Variable(format!("${var}")))
        }

        Rule::null_literal => Ok(Expression::Literal(Value::Null)),
        Rule::boolean_literal => {
            let b = pair.as_str().to_lowercase() == "true";
            Ok(Expression::Literal(Value::Boolean(b)))
        }
        Rule::number_literal | Rule::integer_literal | Rule::decimal_literal => parse_number(pair),
        Rule::string_literal => {
            let s = pair.as_str();
            let unquoted = &s[1..s.len() - 1];
            Ok(Expression::Literal(Value::String(
                unquoted.replace("\\'", "'").replace("\\\"", "\""),
            )))
        }

        _ => {
            let text = pair.as_str().trim();
            if let Ok(i) = text.parse::<i64>() {
                Ok(Expression::Literal(Value::Integer(i)))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(Expression::Literal(Value::Float(f)))
            } else {
                Err(ParseError::Syntax(format!(
                    "unexpected Cypher expression rule: {:?} = '{}'",
                    pair.as_rule(),
                    text
                )))
            }
        }
    }
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("empty literal".into()))?;

    match inner.as_rule() {
        Rule::number_literal => parse_number(inner),
        Rule::string_literal => {
            let s = inner.as_str();
            let unquoted = &s[1..s.len() - 1];
            Ok(Expression::Literal(Value::String(
                unquoted.replace("\\'", "'").replace("\\\"", "\""),
            )))
        }
        Rule::boolean_literal => {
            let b = inner.as_str().to_lowercase() == "true";
            Ok(Expression::Literal(Value::Boolean(b)))
        }
        Rule::null_literal => Ok(Expression::Literal(Value::Null)),
        Rule::list_literal | Rule::map_literal => Ok(Expression::Literal(Value::String(
            inner.as_str().to_string(),
        ))),
        _ => Err(ParseError::Syntax(format!(
            "unexpected Cypher literal rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_number(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let text = pair.as_str();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::integer_literal => {
                let i: i64 = inner
                    .as_str()
                    .parse()
                    .map_err(|_| ParseError::Syntax("invalid integer".into()))?;
                return Ok(Expression::Literal(Value::Integer(i)));
            }
            Rule::decimal_literal => {
                let f: f64 = inner
                    .as_str()
                    .parse()
                    .map_err(|_| ParseError::Syntax("invalid decimal".into()))?;
                return Ok(Expression::Literal(Value::Float(f)));
            }
            _ => {}
        }
    }

    if text.contains('.') {
        let f: f64 = text
            .parse()
            .map_err(|_| ParseError::Syntax("invalid number".into()))?;
        Ok(Expression::Literal(Value::Float(f)))
    } else {
        let i: i64 = text
            .parse()
            .map_err(|_| ParseError::Syntax("invalid number".into()))?;
        Ok(Expression::Literal(Value::Integer(i)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(input: &str) -> Expression {
        parse(input).unwrap_or_else(|e| panic!("failed to parse '{input}': {e}"))
    }

    fn assert_binop(input: &str, expected_op: BinaryOp) {
        match parse_ok(input) {
            Expression::BinaryOp { op, .. } => assert_eq!(op, expected_op, "for input: {input}"),
            other => panic!("expected BinaryOp::{expected_op:?} for '{input}', got: {other:?}"),
        }
    }

    // ─── Literals ────────────────────────────────────────────────────────

    #[test]
    fn test_integer_literal() {
        assert_eq!(parse_ok("42"), Expression::Literal(Value::Integer(42)));
    }

    #[test]
    fn test_negative_integer() {
        match parse_ok("-7") {
            Expression::UnaryOp {
                op: UnaryOp::Negate,
                operand,
            } => assert_eq!(*operand, Expression::Literal(Value::Integer(7))),
            other => panic!("expected Negate(7), got: {other:?}"),
        }
    }

    #[test]
    fn test_float_literal() {
        match parse_ok("3.5") {
            Expression::Literal(Value::Float(f)) => {
                assert!((f - 3.5).abs() < f64::EPSILON)
            }
            other => panic!("expected Float, got: {other:?}"),
        }
    }

    #[test]
    fn test_string_single_quotes() {
        assert_eq!(
            parse_ok("'hello'"),
            Expression::Literal(Value::String("hello".into()))
        );
    }

    #[test]
    fn test_string_double_quotes() {
        assert_eq!(
            parse_ok("\"world\""),
            Expression::Literal(Value::String("world".into()))
        );
    }

    #[test]
    fn test_boolean_true() {
        assert_eq!(parse_ok("true"), Expression::Literal(Value::Boolean(true)));
    }

    #[test]
    fn test_boolean_false() {
        assert_eq!(
            parse_ok("false"),
            Expression::Literal(Value::Boolean(false))
        );
    }

    #[test]
    fn test_null_literal() {
        assert_eq!(parse_ok("null"), Expression::Literal(Value::Null));
    }

    // ─── Identifiers and property access ─────────────────────────────────

    #[test]
    fn test_identifier() {
        assert_eq!(parse_ok("x"), Expression::Variable("x".to_string()));
    }

    #[test]
    fn test_property_access() {
        match parse_ok("n.name") {
            Expression::PropertyAccess { variable, property } => {
                assert_eq!(variable, "n");
                assert_eq!(property, "name");
            }
            other => panic!("expected PropertyAccess, got: {other:?}"),
        }
    }

    // ─── Comparison operators ────────────────────────────────────────────

    #[test]
    fn test_comparison_operators() {
        assert_binop("x = 1", BinaryOp::Eq);
        assert_binop("x <> 1", BinaryOp::Neq);
        assert_binop("x < 1", BinaryOp::Lt);
        assert_binop("x > 1", BinaryOp::Gt);
        assert_binop("x <= 1", BinaryOp::Lte);
        assert_binop("x >= 1", BinaryOp::Gte);
    }

    // ─── String operators ────────────────────────────────────────────────

    #[test]
    fn test_contains_operator() {
        assert_binop("x CONTAINS 'foo'", BinaryOp::Contains);
    }

    #[test]
    fn test_starts_with_operator() {
        assert_binop("x STARTS WITH 'pre'", BinaryOp::StartsWith);
    }

    #[test]
    fn test_ends_with_operator() {
        assert_binop("x ENDS WITH 'suf'", BinaryOp::EndsWith);
    }

    // ─── Arithmetic operators ────────────────────────────────────────────

    #[test]
    fn test_addition() {
        assert_binop("x + 1", BinaryOp::Add);
        assert_binop("x + y", BinaryOp::Add);
    }

    #[test]
    fn test_subtraction() {
        // Regression (#96): the additive/multiplicative operators were inlined
        // in the grammar (not surfaced as child pairs), so the parser never saw
        // "-" and fell back to Add. Now `additive_op`/`multiplicative_op` are
        // named rules (same fix as #94 for cqelsql.pest).
        assert_binop("x - 1", BinaryOp::Sub);
        assert_binop("x - y", BinaryOp::Sub);
    }

    #[test]
    fn test_multiplication() {
        assert_binop("x * 2", BinaryOp::Mul);
    }

    #[test]
    fn test_division() {
        // Regression (#96, same root cause as subtraction): "/" used to parse as Mul.
        assert_binop("x / 2", BinaryOp::Div);
        assert_binop("x / y", BinaryOp::Div);
    }

    #[test]
    fn test_modulo() {
        // Regression (#96, same root cause): "%" used to parse as Mul.
        assert_binop("x % 2", BinaryOp::Mod);
    }

    // ─── Arithmetic evaluation (regression for #96) ──────────────────────
    //
    // Before the grammar fix, '7 - 3' evaluated to 10 (parsed as Add),
    // '7 / 2' and '7 % 2' to 14 (parsed as Mul). Pin the corrected results
    // end-to-end through the evaluator.

    fn eval_const(input: &str) -> Value {
        use crate::expression::evaluator::ExpressionEvaluator;
        use cqels_model::BindingSet;

        let expr = parse_ok(input);
        ExpressionEvaluator::new().evaluate(&expr, &BindingSet::new(0))
    }

    #[test]
    fn test_eval_subtraction() {
        assert_eq!(eval_const("7 - 3"), Value::Integer(4));
    }

    #[test]
    fn test_eval_division_non_whole_is_float() {
        // The #94 whole-quotient rule: non-whole integer division → Float.
        match eval_const("7 / 2") {
            Value::Float(f) => assert!((f - 3.5).abs() < f64::EPSILON),
            other => panic!("expected Float(3.5), got: {other:?}"),
        }
    }

    #[test]
    fn test_eval_modulo() {
        assert_eq!(eval_const("7 % 2"), Value::Integer(1));
    }

    #[test]
    fn test_eval_precedence() {
        assert_eq!(eval_const("2 + 3 * 4"), Value::Integer(14));
    }

    #[test]
    fn test_eval_subtraction_left_associative() {
        assert_eq!(eval_const("2 - 3 - 4"), Value::Integer(-5));
    }

    // ─── Power operator ──────────────────────────────────────────────────
    //
    // Regression (#100, same inlined-operator disease as #94/#96): the grammar
    // inlined the `^` as a string literal, so it surfaced no pest pair. The old
    // `parse_power` did `children.remove(0)` to "skip the ^ token" that wasn't
    // there, its `children.len() >= 2` guard then failed (only the exponent pair
    // remained, len 1), and the exponent was silently dropped — `a ^ b` parsed
    // as bare `a`. Naming the operator (`power_op = { "^" }`) surfaces it as a
    // pair so the parser's content dispatch can consume it. The old tolerant
    // catch-all test masked the drop; replaced here with exact assertions.

    #[test]
    fn test_power_operator_captures_exponent() {
        // The exponent must actually be captured: a power() call with BOTH
        // operands, not a bare base. (x) ^ (2) → power(x, 2).
        match parse_ok("(x) ^ (2)") {
            Expression::FunctionCall { name, args } => {
                assert_eq!(name, "power");
                assert_eq!(
                    args.len(),
                    2,
                    "exponent was dropped — only the base survived"
                );
            }
            other => panic!("expected power() FunctionCall with 2 args, got: {other:?}"),
        }
    }

    #[test]
    fn test_eval_power() {
        // CypherQL power dispatches to the `power`/`pow` builtin, which computes
        // f64 base.powf(exp) → Value::Float (NOT the #94 checked-integer Pow).
        match eval_const("2 ^ 3") {
            Value::Float(f) => assert!(
                (f - 8.0).abs() < f64::EPSILON,
                "2 ^ 3 should be 8.0, got {f}"
            ),
            other => panic!("expected Float(8.0), got: {other:?}"),
        }
    }

    #[test]
    fn test_eval_power_zero_exponent() {
        match eval_const("2 ^ 0") {
            Value::Float(f) => assert!(
                (f - 1.0).abs() < f64::EPSILON,
                "2 ^ 0 should be 1.0, got {f}"
            ),
            other => panic!("expected Float(1.0), got: {other:?}"),
        }
    }

    #[test]
    fn test_power_is_single_exponent_only() {
        // The grammar uses `(power_op ~ unary_expression)?` — an *optional*
        // single exponent, not a repeat. So `2 ^ 3 ^ 2` is neither left- nor
        // right-associative: the trailing `^ 2` cannot be consumed and, because
        // standalone_expression requires EOI, it is a hard parse error. This
        // pins the grammar's actual (single-power) intent; chaining is not
        // supported. Use explicit nesting — power(2, power(3, 2)) — if needed.
        assert!(
            parse("2 ^ 3 ^ 2").is_err(),
            "chained `^` is single-exponent-only and must be a parse error"
        );
    }

    // ─── Logical operators ───────────────────────────────────────────────

    #[test]
    fn test_and_keyword() {
        assert_binop("x > 0 AND x < 10", BinaryOp::And);
    }

    #[test]
    fn test_or_keyword() {
        assert_binop("x = 1 OR x = 2", BinaryOp::Or);
    }

    #[test]
    fn test_not_keyword() {
        match parse_ok("NOT x > 5") {
            Expression::UnaryOp {
                op: UnaryOp::Not, ..
            } => {}
            other => panic!("expected Not, got: {other:?}"),
        }
    }

    // ─── Operator precedence ─────────────────────────────────────────────

    #[test]
    fn test_mul_before_add() {
        let expr = parse_ok("a + b * c");
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Add,
                right,
                ..
            } => match *right {
                Expression::BinaryOp {
                    op: BinaryOp::Mul, ..
                } => {}
                other => panic!("right of Add should be Mul, got: {other:?}"),
            },
            other => panic!("expected Add at top, got: {other:?}"),
        }
    }

    #[test]
    fn test_parenthesized_expression() {
        let expr = parse_ok("(a + b) * c");
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Mul,
                left,
                ..
            } => match *left {
                Expression::BinaryOp {
                    op: BinaryOp::Add, ..
                } => {}
                other => panic!("left of Mul should be Add, got: {other:?}"),
            },
            other => panic!("expected Mul at top, got: {other:?}"),
        }
    }

    // ─── Function calls ──────────────────────────────────────────────────

    #[test]
    fn test_function_call() {
        match parse_ok("toUpper(n.name)") {
            Expression::FunctionCall { name, args } => {
                assert_eq!(name, "toUpper");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got: {other:?}"),
        }
    }

    // ─── Aggregates ──────────────────────────────────────────────────────

    #[test]
    fn test_count_aggregate() {
        match parse_ok("count(n)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Count,
                distinct: false,
                ..
            } => {}
            other => panic!("expected Count, got: {other:?}"),
        }
    }

    #[test]
    fn test_count_distinct() {
        match parse_ok("count(DISTINCT n)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Count,
                distinct: true,
                ..
            } => {}
            other => panic!("expected Count DISTINCT, got: {other:?}"),
        }
    }

    #[test]
    fn test_sum_aggregate() {
        match parse_ok("sum(n.val)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Sum,
                ..
            } => {}
            other => panic!("expected Sum, got: {other:?}"),
        }
    }

    #[test]
    fn test_collect_aggregate() {
        match parse_ok("collect(n)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Collect,
                ..
            } => {}
            other => panic!("expected Collect, got: {other:?}"),
        }
    }

    // ─── Complex expressions ─────────────────────────────────────────────

    #[test]
    fn test_nested_and_or() {
        let expr = parse_ok("a > 1 AND b < 2 OR c = 3");
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Or, ..
            } => {}
            other => panic!("expected Or at top, got: {other:?}"),
        }
    }

    #[test]
    fn test_comparison_with_arithmetic() {
        let expr = parse_ok("x + 1 > y * 2");
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Gt, ..
            } => {}
            other => panic!("expected Gt at top, got: {other:?}"),
        }
    }

    // ─── Error cases ─────────────────────────────────────────────────────

    #[test]
    fn test_empty_input_fails() {
        assert!(parse("").is_err());
    }

    #[test]
    fn test_invalid_syntax_fails() {
        assert!(parse("@@@ !!!").is_err());
    }
}
