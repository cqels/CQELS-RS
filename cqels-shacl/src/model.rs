//! SHACL shape model types.
//!
//! Provides the core data structures for representing SHACL shapes:
//! [`NodeShape`], [`PropertyShape`], [`NodeKind`], and [`ShaclShapeGraph`].

/// The kind of node a shape constrains.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NodeKind {
    /// Only IRI nodes.
    Iri,
    /// Only blank nodes.
    BlankNode,
    /// Only literal nodes.
    Literal,
    /// Blank nodes or IRIs.
    BlankNodeOrIri,
    /// Blank nodes or literals.
    BlankNodeOrLiteral,
    /// IRIs or literals.
    IriOrLiteral,
}

impl NodeKind {
    /// Returns the ASP symbol string for this node kind.
    pub fn asp_symbol(&self) -> &str {
        match self {
            NodeKind::Iri => "iri_kind",
            NodeKind::BlankNode => "blank_kind",
            NodeKind::Literal => "literal_kind",
            NodeKind::BlankNodeOrIri => "blank_or_iri_kind",
            NodeKind::BlankNodeOrLiteral => "blank_or_literal_kind",
            NodeKind::IriOrLiteral => "iri_or_literal_kind",
        }
    }
}

/// A SHACL property shape constraining values at a specific property path.
#[derive(Clone, Debug)]
pub struct PropertyShape {
    /// The identifier for this property shape.
    pub id: String,
    /// The IRI of the property path.
    pub path: String,
    /// Minimum number of values required (`sh:minCount`).
    pub min_count: Option<u32>,
    /// Maximum number of values allowed (`sh:maxCount`).
    pub max_count: Option<u32>,
    /// Required datatype IRI (`sh:datatype`).
    pub datatype: Option<String>,
    /// Required class IRI (`sh:class`).
    pub class: Option<String>,
    /// Required node kind (`sh:nodeKind`).
    pub node_kind: Option<NodeKind>,
    /// Allowed values (`sh:in`).
    pub in_values: Vec<String>,
    /// Required value (`sh:hasValue`).
    pub has_value: Option<String>,
}

/// A SHACL node shape targeting specific nodes in the data graph.
#[derive(Clone, Debug)]
pub struct NodeShape {
    /// The identifier for this node shape.
    pub id: String,
    /// Nodes targeted by `sh:targetNode`.
    pub target_nodes: Vec<String>,
    /// Classes targeted by `sh:targetClass`.
    pub target_classes: Vec<String>,
    /// The property shapes belonging to this node shape.
    pub property_shapes: Vec<PropertyShape>,
    /// Whether this shape is closed (`sh:closed`).
    pub closed: bool,
    /// Properties ignored when checking closedness (`sh:ignoredProperties`).
    pub ignored_properties: Vec<String>,
}

impl NodeShape {
    /// Returns the set of properties allowed by this shape when closed.
    ///
    /// This includes the path of each property shape plus any explicitly
    /// ignored properties.
    pub fn allowed_properties(&self) -> Vec<String> {
        let mut props: Vec<String> = self
            .property_shapes
            .iter()
            .map(|ps| ps.path.clone())
            .collect();
        for ignored in &self.ignored_properties {
            if !props.contains(ignored) {
                props.push(ignored.clone());
            }
        }
        props
    }
}

/// A collection of SHACL node shapes forming a shapes graph.
#[derive(Clone, Debug, Default)]
pub struct ShaclShapeGraph {
    /// The node shapes in this shapes graph.
    pub node_shapes: Vec<NodeShape>,
}

impl ShaclShapeGraph {
    /// Creates a new empty shapes graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a node shape to this shapes graph.
    pub fn add_shape(&mut self, shape: NodeShape) {
        self.node_shapes.push(shape);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_kind_asp_symbols() {
        assert_eq!(NodeKind::Iri.asp_symbol(), "iri_kind");
        assert_eq!(NodeKind::BlankNode.asp_symbol(), "blank_kind");
        assert_eq!(NodeKind::Literal.asp_symbol(), "literal_kind");
        assert_eq!(NodeKind::BlankNodeOrIri.asp_symbol(), "blank_or_iri_kind");
        assert_eq!(
            NodeKind::BlankNodeOrLiteral.asp_symbol(),
            "blank_or_literal_kind"
        );
        assert_eq!(NodeKind::IriOrLiteral.asp_symbol(), "iri_or_literal_kind");
    }

    #[test]
    fn test_node_kind_clone_eq_hash() {
        let k1 = NodeKind::Iri;
        let k2 = k1.clone();
        assert_eq!(k1, k2);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(NodeKind::Iri);
        set.insert(NodeKind::Iri);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_property_shape_fields() {
        let ps = PropertyShape {
            id: "ps1".into(),
            path: "http://example.org/name".into(),
            min_count: Some(1),
            max_count: Some(3),
            datatype: Some("http://www.w3.org/2001/XMLSchema#string".into()),
            class: None,
            node_kind: Some(NodeKind::Literal),
            in_values: vec!["a".into(), "b".into()],
            has_value: None,
        };
        assert_eq!(ps.id, "ps1");
        assert_eq!(ps.min_count, Some(1));
        assert_eq!(ps.max_count, Some(3));
        assert_eq!(ps.in_values.len(), 2);
    }

    #[test]
    fn test_node_shape_allowed_properties() {
        let shape = NodeShape {
            id: "shape1".into(),
            target_nodes: vec![],
            target_classes: vec![],
            property_shapes: vec![
                PropertyShape {
                    id: "ps1".into(),
                    path: "http://example.org/name".into(),
                    min_count: None,
                    max_count: None,
                    datatype: None,
                    class: None,
                    node_kind: None,
                    in_values: vec![],
                    has_value: None,
                },
                PropertyShape {
                    id: "ps2".into(),
                    path: "http://example.org/age".into(),
                    min_count: None,
                    max_count: None,
                    datatype: None,
                    class: None,
                    node_kind: None,
                    in_values: vec![],
                    has_value: None,
                },
            ],
            closed: true,
            ignored_properties: vec!["http://www.w3.org/1999/02/22-rdf-syntax-ns#type".into()],
        };
        let allowed = shape.allowed_properties();
        assert_eq!(allowed.len(), 3);
        assert!(allowed.contains(&"http://example.org/name".to_string()));
        assert!(allowed.contains(&"http://example.org/age".to_string()));
        assert!(allowed.contains(&"http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string()));
    }

    #[test]
    fn test_node_shape_allowed_properties_no_duplicates() {
        let shape = NodeShape {
            id: "s1".into(),
            target_nodes: vec![],
            target_classes: vec![],
            property_shapes: vec![PropertyShape {
                id: "ps1".into(),
                path: "http://example.org/p".into(),
                min_count: None,
                max_count: None,
                datatype: None,
                class: None,
                node_kind: None,
                in_values: vec![],
                has_value: None,
            }],
            closed: true,
            // ignored property same as property shape path
            ignored_properties: vec!["http://example.org/p".into()],
        };
        let allowed = shape.allowed_properties();
        assert_eq!(allowed.len(), 1);
    }

    #[test]
    fn test_shacl_shape_graph_default() {
        let graph = ShaclShapeGraph::new();
        assert!(graph.node_shapes.is_empty());
    }

    #[test]
    fn test_shacl_shape_graph_add_shape() {
        let mut graph = ShaclShapeGraph::new();
        graph.add_shape(NodeShape {
            id: "s1".into(),
            target_nodes: vec!["http://example.org/n1".into()],
            target_classes: vec![],
            property_shapes: vec![],
            closed: false,
            ignored_properties: vec![],
        });
        assert_eq!(graph.node_shapes.len(), 1);
        assert_eq!(graph.node_shapes[0].id, "s1");
    }
}
