//! Named-window lowering pass.
//!
//! RSP-QL `FROM NAMED WINDOW :W ON STREAM <s> [<spec>]` declarations
//! and `WINDOW :W { ... }` pattern groups are parsed as first-class
//! AST nodes by [`crate::parser::cqelsql::CqelsQlParser`]. The runtime
//! engine and downstream operator pipeline only understand ordinary
//! [`CqelsStreamDefinition`]s and [`CqelsPatternGroup::Stream`] groups,
//! so a thin lowering pass — run at the top of the compiler — rewrites
//! named windows into their stream equivalents.
//!
//! ## Semantics
//!
//! Each named window declaration is equivalent to a `FROM STREAM` on
//! the same source stream with the same window spec; the IRI is a
//! label that ties one or more `WINDOW {}` pattern groups in the WHERE
//! clause to that stream / spec pair.
//!
//! - Two named windows declared on the same source stream with
//!   identical specs collapse to a single stream entry. (Idempotent.)
//! - Two named windows declared on the same source stream with
//!   different specs are rejected at compile time. Supporting that
//!   case requires aliased stream views in the runtime, which is a
//!   future expansion; this lowering is the conservative subset that
//!   doesn't require any engine-level changes.
//! - A `WINDOW :W { ... }` referencing an IRI that wasn't declared in
//!   the FROM clause is rejected at compile time.
//! - If the stream-name conflict involves a window spec already
//!   bound via a plain `FROM STREAM` declaration with a different
//!   spec, that's also rejected.
//!
//! ## Why a compile-time pass and not a parser pass?
//!
//! The grammar surface is parser-only. The semantic checks
//! (declared-vs-referenced IRI mismatch, conflicting specs) inhere to
//! compilation, and the lowering result is what every other compiler
//! stage already consumes — so doing it here means zero changes to
//! `pipeline.rs`, `self_join.rs`, `compiled.rs`, or the engine.

use crate::parser::ast::{
    CqelsPatternGroup, CqelsQueryDefinition, CqelsStreamDefinition, WindowSpec,
};
use crate::parser::{ParseError, ParseResult};

/// Rewrites `definition.named_windows` + `Window {}` pattern groups
/// into ordinary `streams` + `Stream {}` pattern groups in place.
///
/// On return:
/// - `definition.named_windows` is empty.
/// - Every `CqelsPatternGroup::Window` has been replaced by an
///   equivalent `CqelsPatternGroup::Stream`.
/// - Every declared named-window source stream is present in
///   `definition.streams` (added if not already there).
///
/// Returns an error if specs conflict or a `WINDOW {}` references an
/// undeclared IRI.
pub fn lower_named_windows(definition: &mut CqelsQueryDefinition) -> ParseResult<()> {
    if definition.named_windows.is_empty() {
        // Even with no declarations, a stray `WINDOW :W {}` reference
        // is an error — catch it before the rest of compilation
        // mistakes the missing IRI for something else.
        for group in &definition.pattern_groups {
            if let CqelsPatternGroup::Window { window_iri, .. } = group {
                return Err(ParseError::Semantic(format!(
                    "WINDOW <{window_iri}> references an undeclared named window"
                )));
            }
        }
        return Ok(());
    }

    // Step 1: build the (window_iri → stream_name) map and ensure each
    // referenced stream is registered with the right spec.
    let mut iri_to_stream: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let windows = std::mem::take(&mut definition.named_windows);
    for win in windows {
        ensure_stream_with_spec(&mut definition.streams, &win.stream, &win.window)
            .map_err(ParseError::Semantic)?;
        if let Some(existing) = iri_to_stream.get(&win.iri) {
            if existing != &win.stream {
                return Err(ParseError::Semantic(format!(
                    "named window <{}> is declared on two different streams: \
                     `{}` and `{}`",
                    win.iri, existing, win.stream
                )));
            }
            // Same IRI re-declared on same stream — idempotent.
        } else {
            iri_to_stream.insert(win.iri, win.stream);
        }
    }

    // Step 2: rewrite each WINDOW pattern group to a Stream pattern
    // group using the resolved source stream name.
    rewrite_window_groups(&mut definition.pattern_groups, &iri_to_stream)?;

    Ok(())
}

/// Ensures `streams` contains an entry for `name` with window `spec`.
/// Adds a new entry if absent; errors if an entry exists with a
/// differing spec.
fn ensure_stream_with_spec(
    streams: &mut Vec<CqelsStreamDefinition>,
    name: &str,
    spec: &WindowSpec,
) -> Result<(), String> {
    if let Some(existing) = streams.iter().find(|s| s.name == name) {
        if &existing.window != spec {
            return Err(format!(
                "stream `{}` is bound to {:?} but a named window declares {:?}; \
                 multiple distinct window specs on the same source stream are not \
                 yet supported",
                name, existing.window, spec
            ));
        }
        return Ok(());
    }
    streams.push(CqelsStreamDefinition {
        name: name.to_string(),
        window: spec.clone(),
    });
    Ok(())
}

/// Walks `groups`, replacing every `Window { window_iri, patterns }`
/// with `Stream { source: <resolved>, patterns }`. Recurses into
/// `Optional` / `Union` so nested groups are covered.
fn rewrite_window_groups(
    groups: &mut [CqelsPatternGroup],
    iri_to_stream: &std::collections::HashMap<String, String>,
) -> ParseResult<()> {
    for group in groups.iter_mut() {
        match group {
            CqelsPatternGroup::Window {
                window_iri,
                patterns,
            } => {
                let Some(source) = iri_to_stream.get(window_iri) else {
                    return Err(ParseError::Semantic(format!(
                        "WINDOW <{window_iri}> references an undeclared named window"
                    )));
                };
                let patterns = std::mem::take(patterns);
                *group = CqelsPatternGroup::Stream {
                    source: source.clone(),
                    patterns,
                };
            }
            CqelsPatternGroup::Optional { groups: inner } => {
                rewrite_window_groups(inner, iri_to_stream)?;
            }
            CqelsPatternGroup::Union { left, right } => {
                rewrite_window_groups(left, iri_to_stream)?;
                rewrite_window_groups(right, iri_to_stream)?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{NamedWindowDefinition, TriplePattern};
    use std::time::Duration;

    fn pattern(s: &str, p: &str, o: &str) -> TriplePattern {
        TriplePattern {
            subject: s.into(),
            predicate: p.into(),
            object: o.into(),
        }
    }

    fn def_with(
        named_windows: Vec<NamedWindowDefinition>,
        groups: Vec<CqelsPatternGroup>,
        streams: Vec<CqelsStreamDefinition>,
    ) -> CqelsQueryDefinition {
        let mut d = CqelsQueryDefinition::builder().build();
        d.named_windows = named_windows;
        d.pattern_groups = groups;
        d.streams = streams;
        d
    }

    #[test]
    fn lowers_single_named_window_to_stream() {
        let mut d = def_with(
            vec![NamedWindowDefinition {
                iri: "http://ex.org/w1".into(),
                stream: "sensors".into(),
                window: WindowSpec::range(Duration::from_secs(10)),
            }],
            vec![CqelsPatternGroup::Window {
                window_iri: "http://ex.org/w1".into(),
                patterns: vec![pattern("?s", "<http://ex.org/p>", "?o")],
            }],
            vec![],
        );

        lower_named_windows(&mut d).expect("lowers");

        assert!(d.named_windows.is_empty());
        assert_eq!(d.streams.len(), 1);
        assert_eq!(d.streams[0].name, "sensors");
        assert_eq!(d.pattern_groups.len(), 1);
        match &d.pattern_groups[0] {
            CqelsPatternGroup::Stream { source, patterns } => {
                assert_eq!(source, "sensors");
                assert_eq!(patterns.len(), 1);
            }
            other => panic!("expected Stream group, got {other:?}"),
        }
    }

    #[test]
    fn two_windows_on_same_stream_same_spec_collapse() {
        let spec = WindowSpec::range(Duration::from_secs(10));
        let mut d = def_with(
            vec![
                NamedWindowDefinition {
                    iri: "http://ex.org/w1".into(),
                    stream: "s".into(),
                    window: spec.clone(),
                },
                NamedWindowDefinition {
                    iri: "http://ex.org/w2".into(),
                    stream: "s".into(),
                    window: spec,
                },
            ],
            vec![
                CqelsPatternGroup::Window {
                    window_iri: "http://ex.org/w1".into(),
                    patterns: vec![pattern("?a", "<http://ex.org/p>", "?b")],
                },
                CqelsPatternGroup::Window {
                    window_iri: "http://ex.org/w2".into(),
                    patterns: vec![pattern("?a", "<http://ex.org/q>", "?c")],
                },
            ],
            vec![],
        );

        lower_named_windows(&mut d).expect("lowers");
        assert_eq!(d.streams.len(), 1, "same stream + spec collapses");
        let sources: Vec<&str> = d
            .pattern_groups
            .iter()
            .filter_map(|g| match g {
                CqelsPatternGroup::Stream { source, .. } => Some(source.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(sources, vec!["s", "s"]);
    }

    #[test]
    fn conflicting_specs_on_same_stream_are_rejected() {
        let mut d = def_with(
            vec![
                NamedWindowDefinition {
                    iri: "http://ex.org/w1".into(),
                    stream: "s".into(),
                    window: WindowSpec::range(Duration::from_secs(10)),
                },
                NamedWindowDefinition {
                    iri: "http://ex.org/w2".into(),
                    stream: "s".into(),
                    window: WindowSpec::range(Duration::from_secs(30)),
                },
            ],
            vec![],
            vec![],
        );
        let err = lower_named_windows(&mut d).unwrap_err();
        assert!(matches!(err, ParseError::Semantic(_)));
        assert!(err.to_string().contains("multiple distinct window specs"));
    }

    #[test]
    fn window_reference_to_undeclared_iri_is_rejected() {
        let mut d = def_with(
            vec![NamedWindowDefinition {
                iri: "http://ex.org/declared".into(),
                stream: "s".into(),
                window: WindowSpec::now(),
            }],
            vec![CqelsPatternGroup::Window {
                window_iri: "http://ex.org/undeclared".into(),
                patterns: vec![pattern("?a", "<http://ex.org/p>", "?b")],
            }],
            vec![],
        );
        let err = lower_named_windows(&mut d).unwrap_err();
        assert!(err.to_string().contains("undeclared named window"));
    }

    #[test]
    fn window_group_without_any_declarations_is_rejected() {
        let mut d = def_with(
            vec![],
            vec![CqelsPatternGroup::Window {
                window_iri: "http://ex.org/w".into(),
                patterns: vec![pattern("?a", "<http://ex.org/p>", "?b")],
            }],
            vec![],
        );
        assert!(lower_named_windows(&mut d).is_err());
    }

    #[test]
    fn empty_definitions_are_a_noop() {
        let mut d = def_with(vec![], vec![], vec![]);
        lower_named_windows(&mut d).expect("no-op");
        assert!(d.named_windows.is_empty());
        assert!(d.streams.is_empty());
        assert!(d.pattern_groups.is_empty());
    }

    #[test]
    fn window_clash_with_existing_from_stream_is_rejected() {
        let mut d = def_with(
            vec![NamedWindowDefinition {
                iri: "http://ex.org/w".into(),
                stream: "s".into(),
                window: WindowSpec::range(Duration::from_secs(30)),
            }],
            vec![],
            vec![CqelsStreamDefinition {
                name: "s".into(),
                window: WindowSpec::range(Duration::from_secs(10)),
            }],
        );
        let err = lower_named_windows(&mut d).unwrap_err();
        assert!(err.to_string().contains("multiple distinct window specs"));
    }

    #[test]
    fn nested_window_inside_optional_is_rewritten() {
        let mut d = def_with(
            vec![NamedWindowDefinition {
                iri: "http://ex.org/w".into(),
                stream: "s".into(),
                window: WindowSpec::now(),
            }],
            vec![CqelsPatternGroup::Optional {
                groups: vec![CqelsPatternGroup::Window {
                    window_iri: "http://ex.org/w".into(),
                    patterns: vec![pattern("?a", "<http://ex.org/p>", "?b")],
                }],
            }],
            vec![],
        );
        lower_named_windows(&mut d).expect("lowers");
        match &d.pattern_groups[0] {
            CqelsPatternGroup::Optional { groups } => match &groups[0] {
                CqelsPatternGroup::Stream { source, .. } => assert_eq!(source, "s"),
                other => panic!("expected nested Stream, got {other:?}"),
            },
            other => panic!("expected Optional, got {other:?}"),
        }
    }
}
