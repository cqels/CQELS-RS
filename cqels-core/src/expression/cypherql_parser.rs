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
        match child.as_rule() {
            Rule::or_expression => return parse_or(child),
            _ => {}
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
    let first = iter.next()
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
    let first = iter.next()
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
    let first = iter.next()
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
    let first = iter.next()
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
    let first = iter.next()
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
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty power expression".into()));
    }
    let first = children.remove(0);
    let base = parse_unary(first)?;
    // If there's a "^" and a right operand, emit as a power() function call
    if children.len() >= 2 {
        // Skip the "^" token
        children.remove(0);
        let exponent = parse_unary(children.remove(0))?;
        Ok(Expression::FunctionCall {
            name: "power".to_string(),
            args: vec![base, exponent],
        })
    } else {
        Ok(base)
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
        let operand_pair = children.into_iter().last()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }
    if first_text == "+" && children.len() > 1 {
        let operand_pair = children.into_iter().last()
            .ok_or_else(|| ParseError::Syntax("missing unary plus operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(operand),
        });
    }

    if text.starts_with('-') && children.len() == 1 {
        let operand_pair = children.into_iter().next()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        let operand = parse_atom(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }

    let first = children.into_iter().next()
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

            let first = children.into_iter().next()
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
        Rule::number_literal | Rule::integer_literal | Rule::decimal_literal => {
            parse_number(pair)
        }
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
        Rule::list_literal | Rule::map_literal => {
            Ok(Expression::Literal(Value::String(inner.as_str().to_string())))
        }
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
