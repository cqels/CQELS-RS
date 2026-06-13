//! CqelsQL expression parser submodule.
//!
//! Contains the pest parser struct for CqelsQL grammar and all
//! CqelsQL-specific expression parsing functions.

use pest::Parser;
use pest_derive::Parser;

use cqels_model::term::{IriTerm, Term};
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
        if child.as_rule() == Rule::conditional_or_expr {
            return parse_or(child);
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
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing additive operand".into()))?;
    let mut result = parse_multiplicative(first)?;

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
    let first = iter
        .next()
        .ok_or_else(|| ParseError::Syntax("missing multiplicative operand".into()))?;
    let mut result = parse_unary(first)?;

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
        let operand_pair = children
            .into_iter()
            .last()
            .ok_or_else(|| ParseError::Syntax("missing NOT operand".into()))?;
        let operand = parse_primary(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(operand),
        });
    }
    if first_text == "-" && children.len() > 1 {
        let operand_pair = children
            .into_iter()
            .last()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        return parse_negate(operand_pair);
    }
    if first_text == "+" && children.len() > 1 {
        let operand_pair = children
            .into_iter()
            .last()
            .ok_or_else(|| ParseError::Syntax("missing unary plus operand".into()))?;
        let operand = parse_primary(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(operand),
        });
    }

    if text.starts_with('!') && children.len() == 1 {
        let operand_pair = children
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::Syntax("missing NOT operand".into()))?;
        let operand = parse_primary(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(operand),
        });
    }
    if text.starts_with('-') && children.len() == 1 {
        let operand_pair = children
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::Syntax("missing negation operand".into()))?;
        return parse_negate(operand_pair);
    }
    // Live unary-plus path (local review round 2 on cqels-rs#94): the inlined
    // `+` prefix produces no pest pair — the same no-pair disease this PR
    // fixed for the binary operators — so only this text-prefix branch can
    // fire. Without it, `+expr` silently dropped the operator and
    // UnaryOp::UnaryPlus (whose evaluator arm type-errors non-numerics to
    // Null per XPath op:numeric-unary-plus) was unreachable from the grammar.
    if text.starts_with('+') && children.len() == 1 {
        let operand_pair = children
            .into_iter()
            .next()
            .ok_or_else(|| ParseError::Syntax("missing unary plus operand".into()))?;
        let operand = parse_primary(operand_pair)?;
        return Ok(Expression::UnaryOp {
            op: UnaryOp::UnaryPlus,
            operand: Box::new(operand),
        });
    }

    let first = children
        .into_iter()
        .next()
        .ok_or_else(|| ParseError::Syntax("missing unary operand".into()))?;
    parse_primary(first)
}

/// Builds the negation of a unary operand, folding `-<digits>` whose
/// magnitude alone does not fit i64 into a single signed constant. The
/// magnitude of i64::MIN (9223372036854775808) overflows i64 unsigned, so
/// parsing the literal before applying the sign would reject the lower
/// boundary that Java's BigInteger path accepts (spec D-E4 is
/// boundary-inclusive). Ordinary negations keep the UnaryOp::Negate shape.
fn parse_negate(operand_pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let text = operand_pair.as_str().trim();
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) && text.parse::<i64>().is_err()
    {
        if let Ok(i) = format!("-{text}").parse::<i64>() {
            return Ok(Expression::Literal(Value::Integer(i)));
        }
    }
    let operand = parse_primary(operand_pair)?;
    Ok(Expression::UnaryOp {
        op: UnaryOp::Negate,
        operand: Box::new(operand),
    })
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
            // Strip the surrounding angle brackets. An IRI expression
            // evaluates to an IRI TERM — matching Java's parseIriRef →
            // RDF4J IRI — not a string literal of the IRI text.
            // (parity unit 5)
            let s = pair.as_str();
            let iri = &s[1..s.len() - 1];
            Ok(Expression::Literal(Value::Term(Term::Iri(IriTerm::new(
                iri,
            )))))
        }

        Rule::prefixed_name => {
            // Resolved against the evaluator's prefix map at eval time —
            // matching the iri_ref arm's Term semantics rather than the old
            // raw-text Value::String (which could never equal an IRI binding).
            let name = pair.as_str().to_string();
            Ok(Expression::PrefixedIri(name))
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
        Rule::group_concat_agg => AggregateExprFunction::GroupConcat,
        _ => return Err(ParseError::Syntax(format!("unknown aggregate: {:?}", rule))),
    };

    let inner = pair.into_inner().next();
    let argument = match inner {
        Some(p) if p.as_rule() == Rule::star => Expression::Literal(Value::String("*".to_string())),
        Some(p) if p.as_rule() == Rule::variable => Expression::Variable(p.as_str().to_string()),
        Some(p) => Expression::Variable(p.as_str().to_string()),
        None => Expression::Literal(Value::String("*".to_string())),
    };

    Ok(Expression::Aggregate {
        function,
        argument: Box::new(argument),
        distinct: false,
    })
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
    fn test_negative_integer_literal() {
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
            Expression::Literal(Value::Float(f)) => assert!((f - 3.5).abs() < f64::EPSILON),
            other => panic!("expected Float(3.5), got: {other:?}"),
        }
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(
            parse_ok("\"hello world\""),
            Expression::Literal(Value::String("hello world".into()))
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

    // ─── Variables ───────────────────────────────────────────────────────

    #[test]
    fn test_variable_question_mark() {
        assert_eq!(parse_ok("?x"), Expression::Variable("?x".to_string()));
    }

    #[test]
    fn test_variable_dollar() {
        assert_eq!(parse_ok("$var"), Expression::Variable("$var".to_string()));
    }

    // ─── Comparison operators ────────────────────────────────────────────

    #[test]
    fn test_comparison_operators() {
        assert_binop("?x = 1", BinaryOp::Eq);
        assert_binop("?x != 1", BinaryOp::Neq);
        assert_binop("?x < 1", BinaryOp::Lt);
        assert_binop("?x > 1", BinaryOp::Gt);
        assert_binop("?x <= 1", BinaryOp::Lte);
        assert_binop("?x >= 1", BinaryOp::Gte);
    }

    // ─── Arithmetic operators ────────────────────────────────────────────

    #[test]
    fn test_addition() {
        assert_binop("?x + 1", BinaryOp::Add);
        assert_binop("?x + ?y", BinaryOp::Add);
    }

    #[test]
    fn test_subtraction() {
        // Regression: the multiplicative/additive operators were inlined in the
        // grammar (not surfaced as child pairs), so the parser never saw "-" and
        // fell back to Add. Now `additive_op`/`multiplicative_op` are named rules.
        assert_binop("?x - 1", BinaryOp::Sub);
        assert_binop("?x - ?y", BinaryOp::Sub);
    }

    #[test]
    fn test_multiplication() {
        assert_binop("?x * 2", BinaryOp::Mul);
    }

    #[test]
    fn test_division() {
        // Regression (same root cause as subtraction): "/" used to parse as Mul.
        assert_binop("?x / 2", BinaryOp::Div);
        assert_binop("?x / ?y", BinaryOp::Div);
    }

    // ─── Logical operators ───────────────────────────────────────────────

    #[test]
    fn test_logical_and() {
        assert_binop("?x > 0 && ?x < 10", BinaryOp::And);
    }

    #[test]
    fn test_logical_or() {
        assert_binop("?x = 1 || ?x = 2", BinaryOp::Or);
    }

    #[test]
    fn test_logical_not() {
        match parse_ok("!true") {
            Expression::UnaryOp {
                op: UnaryOp::Not, ..
            } => {}
            other => panic!("expected Not, got: {other:?}"),
        }
    }

    // ─── Operator precedence ─────────────────────────────────────────────

    #[test]
    fn test_mul_before_add() {
        // ?a + ?b * ?c should parse as ?a + (?b * ?c)
        let expr = parse_ok("?a + ?b * ?c");
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
        // (?a + ?b) * ?c should parse as Mul(Add(...), ...)
        let expr = parse_ok("(?a + ?b) * ?c");
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

    // ─── Unary operators ─────────────────────────────────────────────────

    #[test]
    fn test_unary_negate_variable() {
        match parse_ok("-?x") {
            Expression::UnaryOp {
                op: UnaryOp::Negate,
                ..
            } => {}
            other => panic!("expected Negate, got: {other:?}"),
        }
    }

    // ─── Function calls ──────────────────────────────────────────────────

    #[test]
    fn test_function_call_single_arg() {
        match parse_ok("strlen(?name)") {
            Expression::FunctionCall { name, args } => {
                assert_eq!(name, "strlen");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got: {other:?}"),
        }
    }

    #[test]
    fn test_bound_call() {
        match parse_ok("bound(?x)") {
            Expression::Bound(var) => assert_eq!(var, "?x"),
            other => panic!("expected Bound, got: {other:?}"),
        }
    }

    #[test]
    fn test_if_expression() {
        match parse_ok("IF(?x > 0, ?x, 0)") {
            Expression::If { .. } => {}
            other => panic!("expected If, got: {other:?}"),
        }
    }

    // ─── Aggregates ──────────────────────────────────────────────────────

    #[test]
    fn test_count_star() {
        match parse_ok("COUNT(*)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Count,
                ..
            } => {}
            other => panic!("expected Count aggregate, got: {other:?}"),
        }
    }

    #[test]
    fn test_sum_variable() {
        match parse_ok("SUM(?val)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Sum,
                ..
            } => {}
            other => panic!("expected Sum aggregate, got: {other:?}"),
        }
    }

    #[test]
    fn test_avg_variable() {
        match parse_ok("AVG(?val)") {
            Expression::Aggregate {
                function: AggregateExprFunction::Avg,
                ..
            } => {}
            other => panic!("expected Avg aggregate, got: {other:?}"),
        }
    }

    // ─── Complex expressions ─────────────────────────────────────────────

    #[test]
    fn test_nested_and_or() {
        // ?a > 1 && ?b < 2 || ?c = 3
        let expr = parse_ok("?a > 1 && ?b < 2 || ?c = 3");
        match expr {
            Expression::BinaryOp {
                op: BinaryOp::Or, ..
            } => {}
            other => panic!("expected Or at top (lower precedence), got: {other:?}"),
        }
    }

    #[test]
    fn test_chained_comparisons_with_arithmetic() {
        let expr = parse_ok("?x + 1 > ?y * 2");
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
        assert!(parse("??? +++").is_err());
    }
}
