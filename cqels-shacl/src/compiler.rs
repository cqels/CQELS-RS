//! SHACL-to-ASP program compiler.
//!
//! Translates a [`ShaclShapeGraph`] into ASP programs for validation and
//! repair search.

use crate::model::{PropertyShape, ShaclShapeGraph};

/// Compiles SHACL shapes into ASP program text.
///
/// Two compilation modes are provided:
///
/// - [`compile_validation`](ShaclAspProgramCompiler::compile_validation) --
///   generates a pure checking program that derives `violation/4` atoms.
/// - [`compile_repair`](ShaclAspProgramCompiler::compile_repair) --
///   extends the validation program with `repair_add/3` and `repair_del/3`
///   choice rules so the solver can search for minimal repairs.
pub struct ShaclAspProgramCompiler;

impl ShaclAspProgramCompiler {
    /// Compiles a validation-only ASP program from the given shapes graph.
    ///
    /// The generated program contains:
    /// - Shape facts (`shape/1`, `target_node/2`, `target_class/2`)
    /// - Property constraint facts (`property_constraint/3`, `min_count/3`, etc.)
    /// - Generic violation derivation rules
    pub fn compile_validation(shapes: &ShaclShapeGraph) -> String {
        let mut prog = String::new();

        prog.push_str("% SHACL validation program (auto-generated)\n\n");

        // Emit shape facts.
        for ns in &shapes.node_shapes {
            let sid = sanitize_id(&ns.id);
            prog.push_str(&format!("shape({sid}).\n"));

            for tn in &ns.target_nodes {
                let tid = sanitize_id(tn);
                prog.push_str(&format!("target_node({sid},{tid}).\n"));
            }
            for tc in &ns.target_classes {
                let cid = sanitize_id(tc);
                prog.push_str(&format!("target_class({sid},{cid}).\n"));
            }
            if ns.closed {
                prog.push_str(&format!("closed_shape({sid}).\n"));
                for ap in ns.allowed_properties() {
                    let pid = sanitize_id(&ap);
                    prog.push_str(&format!("allowed_property({sid},{pid}).\n"));
                }
            }
            prog.push('\n');

            // Emit property constraint facts.
            for ps in &ns.property_shapes {
                emit_property_constraints(&mut prog, &sid, ps);
            }
        }

        // Generic violation rules.
        prog.push_str("% --- Violation rules ---\n\n");

        // Target resolution: focus nodes from target_node and target_class.
        prog.push_str(
            "focus(S,N) :- target_node(S,N).\n\
             focus(S,N) :- target_class(S,C), triple(N,\"rdf:type\",C).\n\n",
        );

        // min_count violation
        prog.push_str(
            "violation(S,N,\"minCount\",P) :-\n  \
             focus(S,N), min_count(S,P,Min),\n  \
             #count{ V : triple(N,P,V) } < Min.\n\n",
        );

        // max_count violation
        prog.push_str(
            "violation(S,N,\"maxCount\",P) :-\n  \
             focus(S,N), max_count(S,P,Max),\n  \
             #count{ V : triple(N,P,V) } > Max.\n\n",
        );

        // datatype violation
        prog.push_str(
            "violation(S,N,\"datatype\",P) :-\n  \
             focus(S,N), datatype_constraint(S,P,D),\n  \
             triple(N,P,V), not has_datatype(V,D).\n\n",
        );

        // nodeKind violation
        prog.push_str(
            "violation(S,N,\"nodeKind\",P) :-\n  \
             focus(S,N), node_kind_constraint(S,P,K),\n  \
             triple(N,P,V), not node_matches_kind(V,K).\n\n",
        );

        // hasValue violation
        prog.push_str(
            "violation(S,N,\"hasValue\",P) :-\n  \
             focus(S,N), has_value_constraint(S,P,Val),\n  \
             not triple(N,P,Val).\n\n",
        );

        // closed shape violation
        prog.push_str(
            "violation(S,N,\"closed\",P) :-\n  \
             focus(S,N), closed_shape(S),\n  \
             triple(N,P,_), not allowed_property(S,P).\n\n",
        );

        prog
    }

    /// Compiles a repair-search ASP program from the given shapes graph.
    ///
    /// This extends the validation program with choice rules that allow the
    /// solver to add or delete triples in order to eliminate violations.
    pub fn compile_repair(shapes: &ShaclShapeGraph) -> String {
        let mut prog = Self::compile_validation(shapes);

        prog.push_str("% --- Repair search rules ---\n\n");

        // Choice rules for adding/deleting triples.
        prog.push_str(
            "{ repair_add(S,P,O) } :- candidate_add(S,P,O).\n\
             { repair_del(S,P,O) } :- candidate_del(S,P,O).\n\n",
        );

        // Effective triple after repair.
        prog.push_str(
            "effective_triple(S,P,O) :- triple(S,P,O), not repair_del(S,P,O).\n\
             effective_triple(S,P,O) :- repair_add(S,P,O).\n\n",
        );

        // Integrity constraint: no violations in repaired graph.
        prog.push_str(
            "repair_violation(S,N,C,P) :-\n  \
             focus(S,N), min_count(S,P,Min),\n  \
             #count{ V : effective_triple(N,P,V) } < Min.\n\
             repair_violation(S,N,C,P) :-\n  \
             focus(S,N), max_count(S,P,Max),\n  \
             #count{ V : effective_triple(N,P,V) } > Max.\n\n",
        );

        // Minimize edits.
        prog.push_str(
            "#minimize { 1,S,P,O : repair_add(S,P,O) }.\n\
             #minimize { 1,S,P,O : repair_del(S,P,O) }.\n\n",
        );

        // Integrity: no repair violations allowed.
        prog.push_str(":- repair_violation(_,_,_,_).\n");

        prog
    }
}

/// Emits ASP facts for a single property constraint.
fn emit_property_constraints(prog: &mut String, shape_id: &str, ps: &PropertyShape) {
    let path = sanitize_id(&ps.path);

    prog.push_str(&format!(
        "property_constraint({shape_id},{path},\"{}\").\n",
        ps.id
    ));

    if let Some(min) = ps.min_count {
        prog.push_str(&format!("min_count({shape_id},{path},{min}).\n"));
    }
    if let Some(max) = ps.max_count {
        prog.push_str(&format!("max_count({shape_id},{path},{max}).\n"));
    }
    if let Some(dt) = &ps.datatype {
        let dtid = sanitize_id(dt);
        prog.push_str(&format!("datatype_constraint({shape_id},{path},{dtid}).\n"));
    }
    if let Some(cls) = &ps.class {
        let cid = sanitize_id(cls);
        prog.push_str(&format!("class_constraint({shape_id},{path},{cid}).\n"));
    }
    if let Some(nk) = &ps.node_kind {
        prog.push_str(&format!(
            "node_kind_constraint({shape_id},{path},{}).\n",
            nk.asp_symbol()
        ));
    }
    for val in &ps.in_values {
        let vid = sanitize_id(val);
        prog.push_str(&format!("in_constraint({shape_id},{path},{vid}).\n"));
    }
    if let Some(hv) = &ps.has_value {
        let hvid = sanitize_id(hv);
        prog.push_str(&format!(
            "has_value_constraint({shape_id},{path},{hvid}).\n"
        ));
    }
    prog.push('\n');
}

/// Sanitizes an IRI or value for use as an ASP constant.
///
/// Wraps the value in double quotes to handle special characters.
fn sanitize_id(iri: &str) -> String {
    format!("\"{}\"", iri.replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeKind, NodeShape, PropertyShape, ShaclShapeGraph};

    fn sample_graph() -> ShaclShapeGraph {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "http://example.org/PersonShape".into(),
            target_classes: vec!["http://example.org/Person".into()],
            target_nodes: vec![],
            property_shapes: vec![
                PropertyShape {
                    id: "ps-name".into(),
                    path: "http://example.org/name".into(),
                    min_count: Some(1),
                    max_count: Some(1),
                    datatype: Some("http://www.w3.org/2001/XMLSchema#string".into()),
                    class: None,
                    node_kind: Some(NodeKind::Literal),
                    in_values: vec![],
                    has_value: None,
                },
                PropertyShape {
                    id: "ps-age".into(),
                    path: "http://example.org/age".into(),
                    min_count: None,
                    max_count: Some(1),
                    datatype: Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                    class: None,
                    node_kind: None,
                    in_values: vec![],
                    has_value: None,
                },
            ],
            closed: false,
            ignored_properties: vec![],
        });
        g
    }

    #[test]
    fn test_compile_validation_contains_shape_facts() {
        let prog = ShaclAspProgramCompiler::compile_validation(&sample_graph());
        assert!(prog.contains("shape("));
        assert!(prog.contains("target_class("));
        assert!(prog.contains("property_constraint("));
        assert!(prog.contains("min_count("));
        assert!(prog.contains("max_count("));
        assert!(prog.contains("datatype_constraint("));
        assert!(prog.contains("node_kind_constraint("));
    }

    #[test]
    fn test_compile_validation_contains_violation_rules() {
        let prog = ShaclAspProgramCompiler::compile_validation(&sample_graph());
        assert!(prog.contains("violation("));
        assert!(prog.contains("focus("));
        assert!(prog.contains("\"minCount\""));
        assert!(prog.contains("\"maxCount\""));
        assert!(prog.contains("\"datatype\""));
    }

    #[test]
    fn test_compile_repair_contains_choice_rules() {
        let prog = ShaclAspProgramCompiler::compile_repair(&sample_graph());
        assert!(prog.contains("repair_add("));
        assert!(prog.contains("repair_del("));
        assert!(prog.contains("candidate_add("));
        assert!(prog.contains("candidate_del("));
        assert!(prog.contains("effective_triple("));
        assert!(prog.contains("#minimize"));
    }

    #[test]
    fn test_compile_validation_closed_shape() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "http://example.org/Closed".into(),
            target_nodes: vec!["http://example.org/n1".into()],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/p1".into(),
                min_count: None,
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: true,
            ignored_properties: vec!["http://www.w3.org/1999/02/22-rdf-syntax-ns#type".into()],
        });
        let prog = ShaclAspProgramCompiler::compile_validation(&g);
        assert!(prog.contains("closed_shape("));
        assert!(prog.contains("allowed_property("));
        assert!(prog.contains("\"closed\""));
    }

    #[test]
    fn test_compile_validation_has_value() {
        let mut g = ShaclShapeGraph::new();
        g.add_shape(NodeShape {
            id: "http://example.org/S".into(),
            target_nodes: vec![],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/status".into(),
                min_count: None,
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: Some("active".into()),
            }],
            closed: false,
            ignored_properties: vec![],
        });
        let prog = ShaclAspProgramCompiler::compile_validation(&g);
        assert!(prog.contains("has_value_constraint("));
        assert!(prog.contains("\"hasValue\""));
    }

    #[test]
    fn test_compile_empty_graph() {
        let g = ShaclShapeGraph::new();
        let prog = ShaclAspProgramCompiler::compile_validation(&g);
        assert!(prog.contains("% SHACL validation program"));
        assert!(prog.contains("violation("));
    }

    #[test]
    fn test_sanitize_id_with_quotes() {
        let result = sanitize_id("http://example.org/\"test\"");
        assert_eq!(result, "\"http://example.org/\\\"test\\\"\"");
    }
}
