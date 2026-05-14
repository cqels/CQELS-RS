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

use crate::parser::ast::{CqelsPatternGroup, CqelsQueryDefinition, CqelsStreamDefinition};
use crate::parser::{ParseError, ParseResult};

/// Rewrites `definition.named_windows` + `Window {}` pattern groups
/// into ordinary `streams` + `Stream {}` pattern groups in place.
///
/// ## Same-source, same-spec
///
/// Idempotent: a single root [`CqelsStreamDefinition`] is added (if
/// not already there), all matching `WINDOW {}` groups rewrite to
/// `Stream { source: <name> }` referencing that root.
///
/// ## Same-source, different-spec — aliased views
///
/// Each named window with a distinct spec materializes as a
/// **separate synthetic stream entry** with a deterministic name
/// (`__nw__<sanitized_iri>`) and `source_stream: Some(parent)` set
/// to the original source. The engine fans the parent's broadcast
/// out under each synthetic name so per-window state stays
/// independent. The `WINDOW :W {}` pattern group rewrites to
/// `Stream { source: __nw__<sanitized_iri> }`.
///
/// The root source stream is preserved with its current binding (if
/// any from a `FROM STREAM` declaration). If the root is referenced
/// only through named windows, no root entry is added — the engine
/// only needs the synthetic ones plus the parent name for fan-out.
///
/// ## Error cases
///
/// - `WINDOW :W {}` referencing an IRI not declared by any
///   `FROM NAMED WINDOW`.
/// - The same IRI declared on two different source streams.
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

    // Step 1: map each window IRI to the stream name it lowers to.
    // The mapping picks the cheapest representation: if the window's
    // spec matches an existing root stream definition for the same
    // source, the IRI lowers to that root; otherwise it lowers to a
    // fresh synthetic name (an aliased view).
    let mut iri_to_stream: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let windows = std::mem::take(&mut definition.named_windows);
    for win in windows {
        // Same IRI re-declared — must agree on source.
        if let Some(existing) = iri_to_stream.get(&win.iri).cloned() {
            // existing is the *lowered* name (root or synthetic). We
            // need to ensure the underlying source matches `win.stream`.
            let existing_source =
                root_source(&definition.streams, &existing).unwrap_or(existing.clone());
            if existing_source != win.stream {
                return Err(ParseError::Semantic(format!(
                    "named window <{}> is declared on two different streams: \
                     `{}` and `{}`",
                    win.iri, existing_source, win.stream
                )));
            }
            // Idempotent re-declaration — skip.
            continue;
        }

        let lowered = lower_one_window(&mut definition.streams, &win);
        iri_to_stream.insert(win.iri, lowered);
    }

    // Step 2: rewrite each WINDOW pattern group to a Stream pattern
    // group using the resolved (root-or-synthetic) stream name.
    rewrite_window_groups(&mut definition.pattern_groups, &iri_to_stream)?;

    Ok(())
}

/// Picks the lowered stream name for `win` and ensures `streams`
/// carries the corresponding entry. Returns the chosen name.
///
/// The lowering prefers reusing existing entries before adding new
/// ones:
/// 1. **Root reuse** — an existing root with matching name + spec is
///    reused; the window IRI lowers to the root name.
/// 2. **Synthetic reuse** — an existing aliased view of `win.stream`
///    with the same spec is reused; the window IRI lowers to that
///    synthetic name (so two windows with identical (source, spec)
///    share state).
/// 3. **Fresh root** — if no entry exists for `win.stream` yet, a new
///    root is added and the IRI lowers to `win.stream`. Most common
///    case when there's no `FROM STREAM` plus exactly one named
///    window.
/// 4. **Fresh alias** — otherwise, a synthetic
///    `__nw__<sanitized_iri>` entry is added with
///    `source_stream: Some(win.stream)`. The engine fans data from
///    `win.stream` into the synthetic name so the new window state
///    stays independent of any conflicting spec on the root.
fn lower_one_window(
    streams: &mut Vec<CqelsStreamDefinition>,
    win: &crate::parser::ast::NamedWindowDefinition,
) -> String {
    // Case 1: root with matching spec — reuse.
    if let Some(existing) = streams
        .iter()
        .find(|s| s.name == win.stream && s.source_stream.is_none())
    {
        if existing.window == win.window {
            return win.stream.clone();
        }
    }

    // Case 2: synthetic alias of the same source with the same spec
    // already exists — reuse it so duplicate windows share state.
    if let Some(existing) = streams
        .iter()
        .find(|s| s.source_stream.as_deref() == Some(win.stream.as_str()) && s.window == win.window)
    {
        return existing.name.clone();
    }

    // Case 3: no entry for `win.stream` yet — add a root.
    if !streams.iter().any(|s| s.name == win.stream) {
        streams.push(CqelsStreamDefinition::root(
            win.stream.clone(),
            win.window.clone(),
        ));
        return win.stream.clone();
    }

    // Case 4: fresh synthetic alias.
    let synthetic = synthetic_view_name(&win.iri);
    streams.push(CqelsStreamDefinition::aliased(
        synthetic.clone(),
        win.stream.clone(),
        win.window.clone(),
    ));
    synthetic
}

/// Returns the underlying root stream name for `lowered`: either
/// `lowered` itself (if it's a root) or the `source_stream` of an
/// aliased entry.
fn root_source(streams: &[CqelsStreamDefinition], lowered: &str) -> Option<String> {
    streams.iter().find_map(|s| {
        if s.name == lowered {
            Some(s.source_stream.clone().unwrap_or_else(|| s.name.clone()))
        } else {
            None
        }
    })
}

/// Deterministic synthetic name for an aliased view of a named
/// window. Picks `__nw__` plus a sanitized version of the IRI so the
/// name is stable across runs and free of grammar-significant
/// characters.
fn synthetic_view_name(iri: &str) -> String {
    let mut out = String::from("__nw__");
    for ch in iri.chars() {
        match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' => out.push(ch),
            _ => out.push('_'),
        }
    }
    out
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
    use crate::parser::ast::{NamedWindowDefinition, TriplePattern, WindowSpec};
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
    fn different_specs_on_same_stream_produce_root_and_alias() {
        let spec_w1 = WindowSpec::range(Duration::from_secs(10));
        let spec_w2 = WindowSpec::range(Duration::from_secs(30));
        let mut d = def_with(
            vec![
                NamedWindowDefinition {
                    iri: "http://ex.org/w1".into(),
                    stream: "s".into(),
                    window: spec_w1.clone(),
                },
                NamedWindowDefinition {
                    iri: "http://ex.org/w2".into(),
                    stream: "s".into(),
                    window: spec_w2.clone(),
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

        // The first window (`:w1`) inlines as the root for `s`. The
        // second window (`:w2`) gets a synthetic alias so the engine
        // can fan events from `s` into independently-windowed views.
        assert_eq!(d.streams.len(), 2);
        let root = d.streams.iter().find(|s| s.name == "s").unwrap();
        assert!(root.source_stream.is_none(), "root has no aliased source");
        assert_eq!(root.window, spec_w1);
        let alias = d.streams.iter().find(|s| s.name != "s").unwrap();
        assert_eq!(alias.source_stream.as_deref(), Some("s"));
        assert_eq!(alias.window, spec_w2);
        assert!(alias.name.starts_with("__nw__"));

        let sources: Vec<&str> = d
            .pattern_groups
            .iter()
            .filter_map(|g| match g {
                CqelsPatternGroup::Stream { source, .. } => Some(source.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"s"));
        assert!(sources.iter().any(|s| s.starts_with("__nw__")));
    }

    #[test]
    fn duplicate_named_windows_with_identical_source_and_spec_share_synthetic() {
        // Three named windows on the same source — :w1 with a unique
        // spec inlines as root, :w2 and :w3 with another shared spec
        // collapse onto a single synthetic alias.
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
                NamedWindowDefinition {
                    iri: "http://ex.org/w3".into(),
                    stream: "s".into(),
                    window: WindowSpec::range(Duration::from_secs(30)),
                },
            ],
            vec![],
            vec![],
        );
        lower_named_windows(&mut d).expect("lowers");
        assert_eq!(
            d.streams.len(),
            2,
            "root + single synthetic for both 30s windows"
        );
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
    fn window_with_distinct_spec_from_existing_from_stream_aliases() {
        // `FROM STREAM s [RANGE 10s]` is already declared. A named
        // window asking for [RANGE 30s] on the same source no longer
        // conflicts — it gets a synthetic aliased entry, leaving the
        // root `s` stream untouched.
        let mut d = def_with(
            vec![NamedWindowDefinition {
                iri: "http://ex.org/w".into(),
                stream: "s".into(),
                window: WindowSpec::range(Duration::from_secs(30)),
            }],
            vec![CqelsPatternGroup::Window {
                window_iri: "http://ex.org/w".into(),
                patterns: vec![pattern("?a", "<http://ex.org/p>", "?b")],
            }],
            vec![CqelsStreamDefinition::root(
                "s",
                WindowSpec::range(Duration::from_secs(10)),
            )],
        );
        lower_named_windows(&mut d).expect("lowers");

        assert_eq!(d.streams.len(), 2, "root + synthetic alias");
        let root = d.streams.iter().find(|s| s.name == "s").unwrap();
        assert!(root.source_stream.is_none());
        assert_eq!(root.window, WindowSpec::range(Duration::from_secs(10)));

        let alias = d.streams.iter().find(|s| s.name != "s").unwrap();
        assert_eq!(alias.source_stream.as_deref(), Some("s"));
        assert_eq!(alias.window, WindowSpec::range(Duration::from_secs(30)));
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
