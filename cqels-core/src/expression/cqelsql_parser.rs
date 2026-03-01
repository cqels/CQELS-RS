//! CqelsQL expression parser submodule.
//!
//! Contains the pest parser struct for CqelsQL grammar and all
//! CqelsQL-specific expression parsing functions.

use pest::Parser;
use pest_derive::Parser;

use cqels_model::Value;

use super::ast::{AggregateExprFunction, BinaryOp, Expression, UnaryOp};
use crate::parser::{ParseError, ParseResult};

#[derive(Parser)]
#[grammar = "parser/cqelsql.pest"]
struct CqelsQlExprParser;

/// Parses a CqelsQL (SPARQL-style) expression string into an Expression tree.
pub(crate) fn parse(input: &str) -> ParseResult<Expression> {
    let pairs = CqelsQlExprParser::parse(Rule::standalone_expression, input)
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
            Rule::conditional_or_expr => return parse_or(child),
            _ => {}
        }
    }
    Err(ParseError::Syntax("invalid expression".into()))
}

fn parse_or(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty OR expression".into()));
    }

    let mut result = parse_and(children.remove(0))?;
    for child in children {
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
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty AND expression".into()));
    }

    let mut result = parse_relational(children.remove(0))?;
    for child in children {
        let right = parse_relational(child)?;
        result = Expression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(result),
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn parse_relational(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty relational expression".into()));
    }

    let left = parse_additive(children.remove(0))?;

    if children.len() >= 2 {
        let op_pair = children.remove(0);
        let op = match op_pair.as_str() {
            "<=" => BinaryOp::Lte,
            ">=" => BinaryOp::Gte,
            "!=" => BinaryOp::Neq,
            "=" => BinaryOp::Eq,
            "<" => BinaryOp::Lt,
            ">" => BinaryOp::Gt,
            _ => {
                return Err(ParseError::Syntax(format!(
                    "unknown operator: {}",
                    op_pair.as_str()
                )))
            }
        };
        let right = parse_additive(children.remove(0))?;
        Ok(Expression::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_additive(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty additive expression".into()));
    }

    let mut iter = children.into_iter();
    let mut result = parse_multiplicative(iter.next().unwrap())?;

    while let Some(next) = iter.next() {
        let text = next.as_str();
        if text == "+" || text == "-" {
            if let Some(operand) = iter.next() {
                let op = if text == "+" {
                    BinaryOp::Add
                } else {
                    BinaryOp::Sub
                };
                let right = parse_multiplicative(operand)?;
                result = Expression::BinaryOp {
                    op,
                    left: Box::new(result),
                    right: Box::new(right),
                };
            }
        } else {
            // Not an operator — treat as next multiplicative term
            let right = parse_multiplicative(next)?;
            result = Expression::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(result),
                right: Box::new(right),
            };
        }
    }

    Ok(result)
}

fn parse_multiplicative(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    if children.is_empty() {
        return Err(ParseError::Syntax("empty multiplicative expression".into()));
    }

    let mut iter = children.into_iter();
    let mut result = parse_unary(iter.next().unwrap())?;

    while let Some(next) = iter.next() {
        let text = next.as_str();
        if text == "*" || text == "/" {
            if let Some(operand) = iter.next() {
                let op = if text == "*" {
                    BinaryOp::Mul
                } else {
                    BinaryOp::Div
                };
                let right = parse_unary(operand)?;
                result = Expression::BinaryOp {
                    op,
                    left: Box::new(result),
                    right: Box::new(right),
                };
            }
        } else {
            // Not an operator — treat as next unary term
            let right = parse_unary(next)?;
            result = Expression::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(result),
                right: Box::new(right),
            };
        }
    }

    Ok(result)
}

fn parse_unary(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let text = pair.as_str().trim();
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::Syntax("empty unary expression".into()));
    }

    let first_text = children[0].as_str();
    if first_text == "!" && children.len() > 1 {
        let operand = parse_primary(children.into_iter().last().unwrap())?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(operand),
        });
    }
    if first_text == "-" && children.len() > 1 {
        let operand = parse_primary(children.into_iter().last().unwrap())?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }
    if first_text == "+" && children.len() > 1 {
        let operand = parse_primary(children.into_iter().last().unwrap())?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(operand),
        });
    }

    if text.starts_with('!') && children.len() == 1 {
        let operand = parse_primary(children.into_iter().next().unwrap())?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(operand),
        });
    }
    if text.starts_with('-') && children.len() == 1 {
        let operand = parse_primary(children.into_iter().next().unwrap())?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Negate,
            operand: Box::new(operand),
        });
    }

    parse_primary(children.into_iter().next().unwrap())
}

fn parse_primary(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    match pair.as_rule() {
        Rule::primary_expr => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("empty primary expression".into()))?;
            parse_primary(inner)
        }

        Rule::variable => {
            let var_name = pair.as_str().to_string();
            Ok(Expression::Variable(var_name))
        }

        Rule::literal => parse_literal(pair),

        Rule::iri_ref => {
            let iri = pair.as_str().to_string();
            Ok(Expression::Literal(Value::String(iri)))
        }

        Rule::prefixed_name => {
            let name = pair.as_str().to_string();
            Ok(Expression::Literal(Value::String(name)))
        }

        Rule::expression => parse_expression(pair),

        Rule::built_in_call => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("empty built-in call".into()))?;
            parse_primary(inner)
        }

        Rule::bound_call => {
            let var = pair
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("BOUND requires variable".into()))?;
            Ok(Expression::Bound(var.as_str().to_string()))
        }

        Rule::if_call => {
            let mut children = pair.into_inner();
            let condition = children
                .next()
                .ok_or_else(|| ParseError::Syntax("IF requires condition".into()))?;
            let then_expr = children
                .next()
                .ok_or_else(|| ParseError::Syntax("IF requires then expression".into()))?;
            let else_expr = children
                .next()
                .ok_or_else(|| ParseError::Syntax("IF requires else expression".into()))?;
            Ok(Expression::If {
                condition: Box::new(parse_expression(condition)?),
                then_expr: Box::new(parse_expression(then_expr)?),
                else_expr: Box::new(parse_expression(else_expr)?),
            })
        }

        Rule::function_call => {
            let mut children = pair.into_inner();
            let name_pair = children
                .next()
                .ok_or_else(|| ParseError::Syntax("function requires name".into()))?;
            let name = name_pair.as_str().to_string();

            let mut args = Vec::new();
            for child in children {
                if child.as_rule() == Rule::expression_list {
                    for expr_pair in child.into_inner() {
                        args.push(parse_expression(expr_pair)?);
                    }
                } else if child.as_rule() == Rule::expression {
                    args.push(parse_expression(child)?);
                }
            }

            Ok(Expression::FunctionCall { name, args })
        }

        Rule::aggregate_function => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("empty aggregate".into()))?;
            parse_aggregate(inner)
        }

        Rule::count_agg | Rule::sum_agg | Rule::avg_agg | Rule::min_agg | Rule::max_agg => {
            parse_aggregate(pair)
        }

        Rule::unary_expr => parse_unary(pair),
        Rule::multiplicative_expr => parse_multiplicative(pair),
        Rule::additive_expr => parse_additive(pair),
        Rule::relational_expr => parse_relational(pair),
        Rule::conditional_and_expr => parse_and(pair),
        Rule::conditional_or_expr => parse_or(pair),

        _ => {
            let text = pair.as_str().trim();
            if let Ok(i) = text.parse::<i64>() {
                Ok(Expression::Literal(Value::Integer(i)))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(Expression::Literal(Value::Float(f)))
            } else {
                Err(ParseError::Syntax(format!(
                    "unexpected expression rule: {:?} = '{}'",
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
        Rule::string_literal => {
            let s = inner.as_str();
            let unquoted = &s[1..s.len() - 1];
            Ok(Expression::Literal(Value::String(
                unquoted.replace("\\\"", "\"").replace("\\'", "'"),
            )))
        }
        Rule::integer => {
            let i: i64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::Syntax("invalid integer".into()))?;
            Ok(Expression::Literal(Value::Integer(i)))
        }
        Rule::decimal => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::Syntax("invalid decimal".into()))?;
            Ok(Expression::Literal(Value::Float(f)))
        }
        Rule::double => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::Syntax("invalid double".into()))?;
            Ok(Expression::Literal(Value::Float(f)))
        }
        Rule::boolean_literal => {
            let b = inner.as_str().to_lowercase() == "true";
            Ok(Expression::Literal(Value::Boolean(b)))
        }
        _ => Err(ParseError::Syntax(format!(
            "unexpected literal rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let rule = pair.as_rule();
    let function = match rule {
        Rule::count_agg => AggregateExprFunction::Count,
        Rule::sum_agg => AggregateExprFunction::Sum,
        Rule::avg_agg => AggregateExprFunction::Avg,
        Rule::min_agg => AggregateExprFunction::Min,
        Rule::max_agg => AggregateExprFunction::Max,
        _ => return Err(ParseError::Syntax(format!("unknown aggregate: {:?}", rule))),
    };

    let inner = pair.into_inner().next();
    let argument = match inner {
        Some(p) if p.as_rule() == Rule::star => {
            Expression::Literal(Value::String("*".to_string()))
        }
        Some(p) if p.as_rule() == Rule::variable => {
            Expression::Variable(p.as_str().to_string())
        }
        Some(p) => Expression::Variable(p.as_str().to_string()),
        None => Expression::Literal(Value::String("*".to_string())),
    };

    Ok(Expression::Aggregate {
        function,
        argument: Box::new(argument),
        distinct: false,
    })
}
