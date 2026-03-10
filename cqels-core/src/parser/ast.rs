//! Abstract Syntax Tree types shared across query languages.
//!
//! These types mirror the Java AST definitions in `CqelsQueryDefinition` and
//! `CypherQueryDefinition`, providing a unified representation for parsed queries.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// ─── Common types ────────────────────────────────────────────────────────────

/// Window type for stream sources.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WindowType {
    /// Process elements immediately without buffering.
    Now,
    /// Time-based window with specified duration.
    Range,
    /// Sliding time window with duration and step.
    Slide,
    /// Count-based window with specified number of elements.
    Triples,
    /// Sliding count-based window with element count and slide.
    TriplesSlide,
}

impl fmt::Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::Now => write!(f, "NOW"),
            WindowType::Range => write!(f, "RANGE"),
            WindowType::Slide => write!(f, "SLIDE"),
            WindowType::Triples => write!(f, "TRIPLES"),
            WindowType::TriplesSlide => write!(f, "TRIPLES_SLIDE"),
        }
    }
}

/// Window specification for stream sources.
///
/// Maps to Java's `WindowSpec`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    pub window_type: WindowType,
    pub duration: Option<Duration>,
    pub step: Option<Duration>,
    pub triple_count: Option<u64>,
    pub triple_slide: Option<u64>,
}

impl WindowSpec {
    /// Creates a NOW window.
    pub fn now() -> Self {
        Self {
            window_type: WindowType::Now,
            duration: None,
            step: None,
            triple_count: None,
            triple_slide: None,
        }
    }

    /// Creates a RANGE window with the specified duration.
    pub fn range(duration: Duration) -> Self {
        Self {
            window_type: WindowType::Range,
            duration: Some(duration),
            step: None,
            triple_count: None,
            triple_slide: None,
        }
    }

    /// Creates a SLIDE window with duration and step.
    pub fn slide(duration: Duration, step: Duration) -> Self {
        Self {
            window_type: WindowType::Slide,
            duration: Some(duration),
            step: Some(step),
            triple_count: None,
            triple_slide: None,
        }
    }

    /// Creates a TRIPLES window with the specified count.
    pub fn triples(count: u64) -> Self {
        Self {
            window_type: WindowType::Triples,
            duration: None,
            step: None,
            triple_count: Some(count),
            triple_slide: None,
        }
    }

    /// Creates a TRIPLES SLIDE window with count and slide.
    pub fn triples_slide(count: u64, slide: u64) -> Self {
        Self {
            window_type: WindowType::TriplesSlide,
            duration: None,
            step: None,
            triple_count: Some(count),
            triple_slide: Some(slide),
        }
    }
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.window_type {
            WindowType::Now => write!(f, "NOW"),
            WindowType::Range => {
                write!(f, "RANGE {}", format_duration(self.duration.as_ref()))
            }
            WindowType::Slide => {
                write!(
                    f,
                    "SLIDE {} STEP {}",
                    format_duration(self.duration.as_ref()),
                    format_duration(self.step.as_ref())
                )
            }
            WindowType::Triples => {
                write!(f, "TRIPLES {}", self.triple_count.unwrap_or(0))
            }
            WindowType::TriplesSlide => {
                write!(
                    f,
                    "TRIPLES {} SLIDE {}",
                    self.triple_count.unwrap_or(0),
                    self.triple_slide.unwrap_or(0)
                )
            }
        }
    }
}

fn format_duration(d: Option<&Duration>) -> String {
    match d {
        Some(d) => {
            let secs = d.as_secs();
            if secs % 86400 == 0 && secs > 0 {
                format!("{}d", secs / 86400)
            } else if secs % 3600 == 0 && secs > 0 {
                format!("{}h", secs / 3600)
            } else if secs % 60 == 0 && secs > 0 {
                format!("{}m", secs / 60)
            } else if secs > 0 {
                format!("{secs}s")
            } else {
                format!("{}ms", d.as_millis())
            }
        }
        None => "?".to_string(),
    }
}

/// Sort direction for ORDER BY clauses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// ORDER BY condition.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderCondition {
    pub expression: String,
    pub direction: SortDirection,
}

/// Graph definition for static and named graphs.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDefinition {
    pub uri: String,
    pub depth: Option<u32>,
    pub cache_duration: Option<Duration>,
}

/// Aggregate function type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    GroupConcat,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT"),
            AggregateFunction::Sum => write!(f, "SUM"),
            AggregateFunction::Avg => write!(f, "AVG"),
            AggregateFunction::Min => write!(f, "MIN"),
            AggregateFunction::Max => write!(f, "MAX"),
            AggregateFunction::Collect => write!(f, "COLLECT"),
            AggregateFunction::GroupConcat => write!(f, "GROUP_CONCAT"),
        }
    }
}

// ─── CqelsQL AST ────────────────────────────────────────────────────────────

/// Query type for CqelsQL queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CqelsQueryType {
    Select,
    Construct,
    Ask,
    Describe,
}

/// Stream definition with window semantics for CqelsQL.
#[derive(Clone, Debug, PartialEq)]
pub struct CqelsStreamDefinition {
    pub name: String,
    pub window: WindowSpec,
}

/// Aggregate specification for CqelsQL SELECT.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub argument: String,
    pub alias: String,
}

impl AggregateSpec {
    pub fn is_count_star(&self) -> bool {
        self.function == AggregateFunction::Count && self.argument == "*"
    }
}

impl fmt::Display for AggregateSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({}) AS {}", self.function, self.argument, self.alias)
    }
}

/// Triple pattern (subject predicate object) for SPARQL-style queries.
#[derive(Clone, Debug, PartialEq)]
pub struct TriplePattern {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

impl TriplePattern {
    pub fn is_variable(term: &str) -> bool {
        term.starts_with('?') || term.starts_with('$')
    }

    pub fn has_variable_subject(&self) -> bool {
        Self::is_variable(&self.subject)
    }

    pub fn has_variable_predicate(&self) -> bool {
        Self::is_variable(&self.predicate)
    }

    pub fn has_variable_object(&self) -> bool {
        Self::is_variable(&self.object)
    }
}

impl fmt::Display for TriplePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.subject, self.predicate, self.object)
    }
}

/// A pattern group in the WHERE clause.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CqelsPatternGroup {
    /// Patterns from a named stream source.
    Stream {
        source: String,
        patterns: Vec<TriplePattern>,
    },
    /// Patterns from static data.
    Static { patterns: Vec<TriplePattern> },
    /// Patterns from a named graph.
    NamedGraph {
        graph_uri: String,
        patterns: Vec<TriplePattern>,
    },
    /// Default (unscoped) triple patterns.
    Default { patterns: Vec<TriplePattern> },
    /// FILTER constraint.
    Filter { expression: String },
    /// BIND expression.
    Bind {
        expression: String,
        variable: String,
    },
    /// OPTIONAL pattern group.
    Optional { groups: Vec<CqelsPatternGroup> },
    /// UNION — two alternative pattern groups, either may match.
    Union {
        left: Vec<CqelsPatternGroup>,
        right: Vec<CqelsPatternGroup>,
    },
    /// MINUS — anti-join: filter out results where patterns match.
    Minus { patterns: Vec<TriplePattern> },
}

/// Operator execution hints for CqelsQL.
#[derive(Clone, Debug, PartialEq)]
pub struct OperatorHints {
    pub lookup_cache: Duration,
    pub lookup_max_hops: u32,
    pub lookup_join_mode: String,
    pub lookup_join_key_variable: Option<String>,
}

impl Default for OperatorHints {
    fn default() -> Self {
        Self {
            lookup_cache: Duration::from_secs(300),
            lookup_max_hops: 2,
            lookup_join_mode: "INNER".to_string(),
            lookup_join_key_variable: None,
        }
    }
}

/// A select element in the SELECT clause.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum SelectElement {
    /// A simple variable projection.
    Variable(String),
    /// An expression with alias: (expr AS ?var).
    Expression { expression: String, alias: String },
}

/// CqelsQL query AST definition.
///
/// Maps to Java's `CqelsQueryDefinition`.
#[derive(Clone, Debug, PartialEq)]
pub struct CqelsQueryDefinition {
    pub name: Option<String>,
    pub description: Option<String>,
    pub query_type: CqelsQueryType,
    pub prefixes: HashMap<String, String>,
    pub streams: Vec<CqelsStreamDefinition>,
    pub static_graphs: Vec<GraphDefinition>,
    pub named_graphs: Vec<GraphDefinition>,
    pub select_elements: Vec<SelectElement>,
    pub distinct: bool,
    pub pattern_groups: Vec<CqelsPatternGroup>,
    pub aggregates: Vec<AggregateSpec>,
    pub group_by_variables: Vec<String>,
    pub order_by_conditions: Vec<OrderCondition>,
    pub limit: Option<u64>,
    pub operator_hints: OperatorHints,
}

impl CqelsQueryDefinition {
    pub fn builder() -> CqelsQueryDefinitionBuilder {
        CqelsQueryDefinitionBuilder::default()
    }

    pub fn has_group_by(&self) -> bool {
        !self.group_by_variables.is_empty()
    }

    pub fn has_order_by(&self) -> bool {
        !self.order_by_conditions.is_empty()
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    pub fn has_aggregates(&self) -> bool {
        !self.aggregates.is_empty()
    }
}

/// Builder for `CqelsQueryDefinition`.
#[derive(Clone, Debug, Default)]
pub struct CqelsQueryDefinitionBuilder {
    name: Option<String>,
    description: Option<String>,
    query_type: Option<CqelsQueryType>,
    prefixes: HashMap<String, String>,
    streams: Vec<CqelsStreamDefinition>,
    static_graphs: Vec<GraphDefinition>,
    named_graphs: Vec<GraphDefinition>,
    select_elements: Vec<SelectElement>,
    distinct: bool,
    pattern_groups: Vec<CqelsPatternGroup>,
    aggregates: Vec<AggregateSpec>,
    group_by_variables: Vec<String>,
    order_by_conditions: Vec<OrderCondition>,
    limit: Option<u64>,
    operator_hints: OperatorHints,
}

impl CqelsQueryDefinitionBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn query_type(mut self, qt: CqelsQueryType) -> Self {
        self.query_type = Some(qt);
        self
    }

    pub fn add_prefix(mut self, prefix: impl Into<String>, uri: impl Into<String>) -> Self {
        self.prefixes.insert(prefix.into(), uri.into());
        self
    }

    pub fn add_stream(mut self, stream: CqelsStreamDefinition) -> Self {
        self.streams.push(stream);
        self
    }

    pub fn add_static_graph(mut self, graph: GraphDefinition) -> Self {
        self.static_graphs.push(graph);
        self
    }

    pub fn add_named_graph(mut self, graph: GraphDefinition) -> Self {
        self.named_graphs.push(graph);
        self
    }

    pub fn add_select_element(mut self, elem: SelectElement) -> Self {
        self.select_elements.push(elem);
        self
    }

    pub fn distinct(mut self, d: bool) -> Self {
        self.distinct = d;
        self
    }

    pub fn add_pattern_group(mut self, group: CqelsPatternGroup) -> Self {
        self.pattern_groups.push(group);
        self
    }

    pub fn add_aggregate(mut self, agg: AggregateSpec) -> Self {
        self.aggregates.push(agg);
        self
    }

    pub fn add_group_by(mut self, var: impl Into<String>) -> Self {
        self.group_by_variables.push(var.into());
        self
    }

    pub fn add_order_by(mut self, cond: OrderCondition) -> Self {
        self.order_by_conditions.push(cond);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn operator_hints(mut self, hints: OperatorHints) -> Self {
        self.operator_hints = hints;
        self
    }

    pub fn build(self) -> CqelsQueryDefinition {
        CqelsQueryDefinition {
            name: self.name,
            description: self.description,
            query_type: self.query_type.unwrap_or(CqelsQueryType::Select),
            prefixes: self.prefixes,
            streams: self.streams,
            static_graphs: self.static_graphs,
            named_graphs: self.named_graphs,
            select_elements: self.select_elements,
            distinct: self.distinct,
            pattern_groups: self.pattern_groups,
            aggregates: self.aggregates,
            group_by_variables: self.group_by_variables,
            order_by_conditions: self.order_by_conditions,
            limit: self.limit,
            operator_hints: self.operator_hints,
        }
    }
}

// ─── CypherQL AST ───────────────────────────────────────────────────────────

/// Query type for CypherQL queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CypherQueryType {
    Select,
    Construct,
    Update,
}

/// Stream definition for CypherQL.
#[derive(Clone, Debug, PartialEq)]
pub struct CypherStreamDefinition {
    pub name: String,
    pub window: WindowSpec,
}

/// Pattern source context in CypherQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PatternSource {
    Stream,
    Static,
    Graph,
    Default,
}

/// Node pattern in a Cypher graph pattern.
#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: HashMap<String, String>,
}

impl fmt::Display for NodePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        if let Some(v) = &self.variable {
            write!(f, "{v}")?;
        }
        for label in &self.labels {
            write!(f, ":{label}")?;
        }
        if !self.properties.is_empty() {
            write!(f, " {{{} props}}", self.properties.len())?;
        }
        write!(f, ")")
    }
}

/// Direction of a relationship in a Cypher pattern.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelDirection {
    Outgoing,
    Incoming,
    Undirected,
}

/// Path length specification for variable-length relationships.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathLength {
    pub min_length: Option<u32>,
    pub max_length: Option<u32>,
}

impl PathLength {
    pub fn is_unbounded(&self) -> bool {
        self.max_length.is_none()
    }

    pub fn has_minimum(&self) -> bool {
        self.min_length.is_some()
    }
}

impl fmt::Display for PathLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.min_length, self.max_length) {
            (None, None) => write!(f, "*"),
            (Some(min), Some(max)) if min == max => write!(f, "*{min}"),
            (Some(min), Some(max)) => write!(f, "*{min}..{max}"),
            (Some(min), None) => write!(f, "*{min}.."),
            (None, Some(max)) => write!(f, "*..{max}"),
        }
    }
}

/// Relationship pattern in a Cypher graph pattern.
#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub direction: RelDirection,
    pub properties: HashMap<String, String>,
    pub start_node: Option<String>,
    pub end_node: Option<String>,
    pub path_length: Option<PathLength>,
}

impl RelationshipPattern {
    pub fn is_variable_length(&self) -> bool {
        self.path_length.is_some()
    }
}

/// A Cypher graph pattern consisting of nodes and relationships.
#[derive(Clone, Debug, PartialEq)]
pub struct CypherPattern {
    pub nodes: Vec<NodePattern>,
    pub relationships: Vec<RelationshipPattern>,
}

/// A pattern group with source context.
#[derive(Clone, Debug, PartialEq)]
pub struct PatternGroup {
    pub source: PatternSource,
    pub source_name: Option<String>,
    pub patterns: Vec<CypherPattern>,
}

/// Property filter for WHERE/HAVING clauses.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyFilter {
    pub variable: String,
    pub property: String,
    pub operator: FilterOperator,
    pub value: String,
}

impl fmt::Display for PropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} {} {}",
            self.variable, self.property, self.operator, self.value
        )
    }
}

/// Filter operator for property comparisons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterOperator {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Contains,
    StartsWith,
    EndsWith,
    In,
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterOperator::Eq => write!(f, "="),
            FilterOperator::Neq => write!(f, "<>"),
            FilterOperator::Lt => write!(f, "<"),
            FilterOperator::Gt => write!(f, ">"),
            FilterOperator::Lte => write!(f, "<="),
            FilterOperator::Gte => write!(f, ">="),
            FilterOperator::Contains => write!(f, "CONTAINS"),
            FilterOperator::StartsWith => write!(f, "STARTS WITH"),
            FilterOperator::EndsWith => write!(f, "ENDS WITH"),
            FilterOperator::In => write!(f, "IN"),
        }
    }
}

/// Return expression for CypherQL RETURN clause.
#[derive(Clone, Debug, PartialEq)]
pub struct ReturnExpression {
    pub expression: String,
    pub alias: Option<String>,
    pub aggregate_function: Option<AggregateFunction>,
    pub distinct: bool,
}

impl fmt::Display for ReturnExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(func) = &self.aggregate_function {
            write!(f, "{func}(")?;
            if self.distinct {
                write!(f, "DISTINCT ")?;
            }
        }
        write!(f, "{}", self.expression)?;
        if self.aggregate_function.is_some() {
            write!(f, ")")?;
        }
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// CypherQL query AST definition.
///
/// Maps to Java's `CypherQueryDefinition`.
#[derive(Clone, Debug, PartialEq)]
pub struct CypherQueryDefinition {
    pub name: Option<String>,
    pub query_type: CypherQueryType,
    pub streams: Vec<CypherStreamDefinition>,
    pub static_graphs: Vec<GraphDefinition>,
    pub named_graphs: Vec<GraphDefinition>,
    pub pattern_groups: Vec<PatternGroup>,
    pub where_expression: Option<String>,
    pub return_expressions: Vec<ReturnExpression>,
    pub distinct: bool,
    pub group_by_expressions: Vec<String>,
    pub having_filters: Vec<String>,
    pub order_by_conditions: Vec<OrderCondition>,
    pub limit: Option<u64>,
}

impl CypherQueryDefinition {
    pub fn builder() -> CypherQueryDefinitionBuilder {
        CypherQueryDefinitionBuilder::default()
    }
}

/// Builder for `CypherQueryDefinition`.
#[derive(Clone, Debug, Default)]
pub struct CypherQueryDefinitionBuilder {
    name: Option<String>,
    query_type: Option<CypherQueryType>,
    streams: Vec<CypherStreamDefinition>,
    static_graphs: Vec<GraphDefinition>,
    named_graphs: Vec<GraphDefinition>,
    pattern_groups: Vec<PatternGroup>,
    where_expression: Option<String>,
    return_expressions: Vec<ReturnExpression>,
    distinct: bool,
    group_by_expressions: Vec<String>,
    having_filters: Vec<String>,
    order_by_conditions: Vec<OrderCondition>,
    limit: Option<u64>,
}

impl CypherQueryDefinitionBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn query_type(mut self, qt: CypherQueryType) -> Self {
        self.query_type = Some(qt);
        self
    }

    pub fn add_stream(mut self, stream: CypherStreamDefinition) -> Self {
        self.streams.push(stream);
        self
    }

    pub fn add_static_graph(mut self, graph: GraphDefinition) -> Self {
        self.static_graphs.push(graph);
        self
    }

    pub fn add_named_graph(mut self, graph: GraphDefinition) -> Self {
        self.named_graphs.push(graph);
        self
    }

    pub fn add_pattern_group(mut self, group: PatternGroup) -> Self {
        self.pattern_groups.push(group);
        self
    }

    pub fn where_expression(mut self, expr: impl Into<String>) -> Self {
        self.where_expression = Some(expr.into());
        self
    }

    pub fn add_return_expression(mut self, expr: ReturnExpression) -> Self {
        self.return_expressions.push(expr);
        self
    }

    pub fn distinct(mut self, d: bool) -> Self {
        self.distinct = d;
        self
    }

    pub fn add_group_by(mut self, expr: impl Into<String>) -> Self {
        self.group_by_expressions.push(expr.into());
        self
    }

    pub fn add_having_filter(mut self, filter: impl Into<String>) -> Self {
        self.having_filters.push(filter.into());
        self
    }

    pub fn add_order_by(mut self, cond: OrderCondition) -> Self {
        self.order_by_conditions.push(cond);
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn build(self) -> CypherQueryDefinition {
        CypherQueryDefinition {
            name: self.name,
            query_type: self.query_type.unwrap_or(CypherQueryType::Select),
            streams: self.streams,
            static_graphs: self.static_graphs,
            named_graphs: self.named_graphs,
            pattern_groups: self.pattern_groups,
            where_expression: self.where_expression,
            return_expressions: self.return_expressions,
            distinct: self.distinct,
            group_by_expressions: self.group_by_expressions,
            having_filters: self.having_filters,
            order_by_conditions: self.order_by_conditions,
            limit: self.limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_spec_now() {
        let w = WindowSpec::now();
        assert_eq!(w.window_type, WindowType::Now);
        assert_eq!(w.to_string(), "NOW");
    }

    #[test]
    fn test_window_spec_range() {
        let w = WindowSpec::range(Duration::from_secs(10));
        assert_eq!(w.window_type, WindowType::Range);
        assert_eq!(w.to_string(), "RANGE 10s");
    }

    #[test]
    fn test_window_spec_slide() {
        let w = WindowSpec::slide(Duration::from_secs(60), Duration::from_secs(30));
        assert_eq!(w.window_type, WindowType::Slide);
        assert_eq!(w.to_string(), "SLIDE 1m STEP 30s");
    }

    #[test]
    fn test_window_spec_triples() {
        let w = WindowSpec::triples(100);
        assert_eq!(w.window_type, WindowType::Triples);
        assert_eq!(w.to_string(), "TRIPLES 100");
    }

    #[test]
    fn test_triple_pattern_variables() {
        let tp = TriplePattern {
            subject: "?s".to_string(),
            predicate: "<http://example.org/p>".to_string(),
            object: "?o".to_string(),
        };
        assert!(tp.has_variable_subject());
        assert!(!tp.has_variable_predicate());
        assert!(tp.has_variable_object());
    }

    #[test]
    fn test_aggregate_spec() {
        let agg = AggregateSpec {
            function: AggregateFunction::Count,
            argument: "*".to_string(),
            alias: "?cnt".to_string(),
        };
        assert!(agg.is_count_star());
        assert_eq!(agg.to_string(), "COUNT(*) AS ?cnt");
    }

    #[test]
    fn test_cqels_query_builder() {
        let query = CqelsQueryDefinition::builder()
            .name("test-query")
            .query_type(CqelsQueryType::Select)
            .add_stream(CqelsStreamDefinition {
                name: "sensor".to_string(),
                window: WindowSpec::now(),
            })
            .add_select_element(SelectElement::Variable("?x".to_string()))
            .distinct(true)
            .limit(10)
            .build();

        assert_eq!(query.name, Some("test-query".to_string()));
        assert_eq!(query.query_type, CqelsQueryType::Select);
        assert!(query.distinct);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.streams.len(), 1);
        assert_eq!(query.streams[0].name, "sensor");
    }

    #[test]
    fn test_cypher_query_builder() {
        let query = CypherQueryDefinition::builder()
            .name("cypher-test")
            .query_type(CypherQueryType::Select)
            .add_stream(CypherStreamDefinition {
                name: "events".to_string(),
                window: WindowSpec::range(Duration::from_secs(30)),
            })
            .add_return_expression(ReturnExpression {
                expression: "n.name".to_string(),
                alias: Some("name".to_string()),
                aggregate_function: None,
                distinct: false,
            })
            .limit(5)
            .build();

        assert_eq!(query.name, Some("cypher-test".to_string()));
        assert_eq!(query.streams.len(), 1);
        assert_eq!(query.return_expressions.len(), 1);
        assert_eq!(query.limit, Some(5));
    }

    #[test]
    fn test_node_pattern_display() {
        let np = NodePattern {
            variable: Some("n".to_string()),
            labels: vec!["Person".to_string(), "Employee".to_string()],
            properties: HashMap::new(),
        };
        assert_eq!(np.to_string(), "(n:Person:Employee)");
    }

    #[test]
    fn test_path_length_display() {
        assert_eq!(
            PathLength {
                min_length: Some(1),
                max_length: Some(3)
            }
            .to_string(),
            "*1..3"
        );
        assert_eq!(
            PathLength {
                min_length: None,
                max_length: None
            }
            .to_string(),
            "*"
        );
        assert_eq!(
            PathLength {
                min_length: Some(2),
                max_length: Some(2)
            }
            .to_string(),
            "*2"
        );
    }

    #[test]
    fn test_return_expression_display() {
        let expr = ReturnExpression {
            expression: "n.age".to_string(),
            alias: Some("avg_age".to_string()),
            aggregate_function: Some(AggregateFunction::Avg),
            distinct: false,
        };
        assert_eq!(expr.to_string(), "AVG(n.age) AS avg_age");

        let expr_distinct = ReturnExpression {
            expression: "n.name".to_string(),
            alias: Some("cnt".to_string()),
            aggregate_function: Some(AggregateFunction::Count),
            distinct: true,
        };
        assert_eq!(expr_distinct.to_string(), "COUNT(DISTINCT n.name) AS cnt");
    }

    #[test]
    fn test_filter_operator_display() {
        assert_eq!(FilterOperator::Eq.to_string(), "=");
        assert_eq!(FilterOperator::Neq.to_string(), "<>");
        assert_eq!(FilterOperator::Contains.to_string(), "CONTAINS");
        assert_eq!(FilterOperator::StartsWith.to_string(), "STARTS WITH");
    }
}
