//! Lightweight ASP program structure validation.
//!
//! This is not a full ASP-Core-2 parser. It catches structural mistakes that
//! should fail before a standing program is registered: unterminated strings,
//! unbalanced grouping delimiters, and non-comment statements missing their
//! terminating top-level period.

use crate::AspError;

/// Validates the basic statement envelope of an ASP program.
///
/// The check deliberately avoids rejecting richer Clingo/ASP constructs that
/// this crate may not parse itself. Full syntax and grounding validation still
/// belongs to the configured solver backend.
pub fn validate_program_syntax(program: &str) -> Result<(), AspError> {
    let mut delimiter_stack = Vec::<(char, usize)>::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut block_comment_saw_star = false;
    let mut token_since_terminator = false;

    for (idx, ch) in program.char_indices() {
        if in_block_comment {
            if block_comment_saw_star && ch == '%' {
                in_block_comment = false;
                block_comment_saw_star = false;
            } else {
                block_comment_saw_star = ch == '*';
            }
            continue;
        }

        if in_line_comment {
            if ch == '\n' || ch == '\r' {
                in_line_comment = false;
            }
            continue;
        }

        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '%' if program[idx + 1..].starts_with('*') => {
                in_block_comment = true;
                block_comment_saw_star = false;
            }
            '%' => in_line_comment = true,
            '"' => {
                in_string = true;
                token_since_terminator = true;
            }
            '(' => {
                delimiter_stack.push(('(', idx));
                token_since_terminator = true;
            }
            ')' => {
                close_delimiter(&mut delimiter_stack, '(', ')', idx)?;
                token_since_terminator = true;
            }
            '[' => {
                delimiter_stack.push(('[', idx));
                token_since_terminator = true;
            }
            ']' => {
                close_delimiter(&mut delimiter_stack, '[', ']', idx)?;
                token_since_terminator = true;
            }
            '{' => {
                delimiter_stack.push(('{', idx));
                token_since_terminator = true;
            }
            '}' => {
                close_delimiter(&mut delimiter_stack, '{', '}', idx)?;
                token_since_terminator = true;
            }
            '.' if delimiter_stack.is_empty() => {
                if is_part_of_range(program, idx) {
                    token_since_terminator = true;
                    continue;
                }
                if !token_since_terminator {
                    return Err(AspError::ParseError {
                        message: format!("empty ASP statement before byte {idx}"),
                    });
                }
                token_since_terminator = false;
            }
            ch if ch.is_whitespace() => {}
            _ => token_since_terminator = true,
        }
    }

    if in_string {
        return Err(AspError::ParseError {
            message: "unterminated string literal".to_string(),
        });
    }
    if in_block_comment {
        return Err(AspError::ParseError {
            message: "unterminated block comment".to_string(),
        });
    }
    if let Some((open, _)) = delimiter_stack.last() {
        return Err(AspError::ParseError {
            message: format!("unclosed '{open}' in ASP program"),
        });
    }
    if token_since_terminator {
        return Err(AspError::ParseError {
            message: "ASP statement is missing terminating '.'".to_string(),
        });
    }

    Ok(())
}

fn close_delimiter(
    delimiter_stack: &mut Vec<(char, usize)>,
    expected_open: char,
    close: char,
    close_idx: usize,
) -> Result<(), AspError> {
    match delimiter_stack.pop() {
        Some((open, _)) if open == expected_open => Ok(()),
        Some((open, open_idx)) => Err(AspError::ParseError {
            message: format!(
                "mismatched '{close}' at byte {close_idx}; opened '{open}' at byte {open_idx}"
            ),
        }),
        None => Err(AspError::ParseError {
            message: format!("unmatched '{close}' at byte {close_idx}"),
        }),
    }
}

fn is_part_of_range(program: &str, idx: usize) -> bool {
    let prev_is_dot = program[..idx].ends_with('.');
    let next_is_dot = program[idx + 1..].starts_with('.');
    prev_is_dot || next_is_dot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_valid(program: &str) {
        validate_program_syntax(program).unwrap_or_else(|e| panic!("{program:?}: {e}"));
    }

    fn assert_invalid(program: &str, message: &str) {
        let err = validate_program_syntax(program).expect_err(program);
        assert!(
            err.to_string().contains(message),
            "expected {message:?}, got {err}"
        );
    }

    #[test]
    fn validates_common_rule_fact_choice_and_constraint_shapes() {
        assert_valid("alert(alice) :- rdf(_,_,_).");
        assert_valid("rdf(iri(\"s\"), iri(\"p\"), literal_dt(\"x\",\"dt\")).");
        assert_valid(":- blocked(X), rdf(X,p,o).");
        assert_valid("1 { choose(a); choose(b) } 1.");
        assert_valid("#const horizon=1..3.");
        assert_valid("% comment-only lines are ignored\nalert(alice). % trailing comment\n");
        assert_valid("%* block comment with . and ()\nacross lines *%\nalert(alice).");
    }

    #[test]
    fn rejects_missing_statement_terminator() {
        assert_invalid("alert(alice) :- rdf(_,_,_)", "terminating");
    }

    #[test]
    fn rejects_unbalanced_grouping_and_strings() {
        assert_invalid("alert((alice).", "unclosed '('");
        assert_invalid("alert(alice)).", "unmatched ')'");
        assert_invalid("{[}].", "mismatched '}'");
        assert_invalid("label(\"alice).", "unterminated string");
        assert_invalid(
            "%* missing close\nalert(alice).",
            "unterminated block comment",
        );
    }

    #[test]
    fn ignores_periods_inside_terms_strings_and_comments() {
        assert_valid("range(1..3).\nlabel(\"a.b\").\n% not_a_statement.\nnext(ok).");
    }
}
