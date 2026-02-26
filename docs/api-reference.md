# API Reference

> Quick-scan reference of all public types and traits. For full method
> signatures, run `cargo doc --workspace --no-deps --open`.

## cqels-model

| Type | Kind | Description |
|------|------|-------------|
| `Term` | enum | RDF term: `Iri`, `BlankNode`, `Literal` |
| `IriTerm` | struct | Internationalized Resource Identifier |
| `BlankNodeTerm` | struct | Anonymous node with local ID |
| `LiteralTerm` | struct | String value with optional datatype/language |
| `Statement` | struct | RDF triple (subject, predicate, object) + optional graph |
| `Value` | enum | Dynamic runtime value: `Term`, `String`, `Integer`, `Float`, `Boolean`, `Null` |
| `BindingSet` | struct | Variable-to-value mappings with timestamp |
| `CqelsError` | enum | Unified error type for the engine |
| `CqelsResult<T>` | alias | `Result<T, CqelsError>` |
| `ParseErrorDetail` | struct | Parse error with kind, message, line, column |
| `ParseErrorKind` | enum | `Syntax`, `Semantic`, `Unsupported` |

**Docs**: [Data Model](data-model.md)

## cqels-core::stream

| Type | Kind | Description |
|------|------|-------------|
| `Timestamped` | trait | `fn timestamp(&self) -> i64` |
| `StreamElement` | enum | `Rdf(RdfStreamElement)` or `Record(StreamRecord)` |
| `RdfStreamElement` | struct | Statement + timestamp |
| `StreamRecord` | struct | String payload + timestamp |
| `StreamEvent<T>` | enum | `Record { value, timestamp }` or `Watermark { timestamp }` |
| `TimestampedValue<T>` | struct | Generic value + timestamp |

**Docs**: [Stream Processing](stream-processing.md)

## cqels-core::window

| Type | Kind | Description |
|------|------|-------------|
| `Window<T>` | trait | `fn apply(stream) -> Stream<WindowedBatch<T>>` |
| `WindowType` | enum | `TumblingTime`, `SlidingTime`, `TumblingCount`, `Session`, `Custom` |
| `WindowedBatch<T>` | struct | Elements + window boundaries |
| `WindowSpec` | enum | `Now`, `Range`, `RangeSlide`, `Rows`, `RowsSlide` |
| `TumblingWindow` | struct | Fixed-duration non-overlapping window |
| `SlidingWindow` | struct | Fixed-duration overlapping window |
| `SessionWindow` | struct | Gap-based activity window |
| `TumblingCountWindow` | struct | Fixed-count window |

Factory functions: `tumbling()`, `sliding()`, `session()`, `tumbling_count()`

**Docs**: [Stream Processing](stream-processing.md)

## cqels-core::operator

### Aggregation

| Type | Kind | Description |
|------|------|-------------|
| `AggregateFunction<T, ACC, R>` | trait | Core aggregation trait |
| `RetractableAggregateFunction<T, ACC, R>` | trait | Adds `retract()` for sliding windows |
| `CountAggregate` | struct | Count elements |
| `SumAggregate<T, F>` | struct | Sum via extractor function |
| `AvgAggregate<T, F>` | struct | Mean via extractor |
| `MinAggregate<T, F>` | struct | Minimum via extractor |
| `MaxAggregate<T, F>` | struct | Maximum via extractor |
| `WindowedAggregateOperator<T, F, ACC, R>` | struct | Grouped windowed aggregation |
| `GroupKey` | struct | Aggregation group identifier |
| `AggregateResult<R>` | struct | Aggregation result with optional group key |

### SWAG

| Type | Kind | Description |
|------|------|-------------|
| `SwagOp<In, Partial, Out>` | trait | Monoidal sliding window aggregation |
| `SwagCountOp<T>` | struct | SWAG count |
| `SwagSumOp<T, F>` | struct | SWAG sum |
| `SwagAvgOp<T, F>` | struct | SWAG average |
| `SwagMinOp<T, F>` | struct | SWAG minimum |
| `SwagMaxOp<T, F>` | struct | SWAG maximum |

### Filter, Bind, Join, Ranking

| Type | Kind | Description |
|------|------|-------------|
| `FilterOperator<F>` | struct | Predicate-based binding set filter |
| `BindOperator<F>` | struct | Computed variable binding |
| `JoinFunction<L, R, Out>` | trait | Join predicate |
| `WindowedJoinState<L, R>` | struct | Time-windowed join with indexed buffers |
| `JoinResult<Out>` | struct | Join match with timestamp |
| `SortKey` | struct | Sort key with name + direction |
| `SortDirection` | enum | `Ascending`, `Descending` |
| `TopKOperator<T, F>` | struct | Ranked top-K subset |
| `RankedElement<T>` | struct | Element with rank |

### RSP-QL

| Type | Kind | Description |
|------|------|-------------|
| `IStreamOperator` | struct | Insert stream (new elements) |
| `DStreamOperator` | struct | Delete stream (evicted elements) |
| `RStreamOperator` | struct | Relation stream (full snapshot) |

### Parallel

| Type | Kind | Description |
|------|------|-------------|
| `ParallelExecutionConfig` | struct | Parallelism settings |
| `AggregationBackend` | enum | `Legacy`, `Swag(SwagConfig)` |
| `SwagConfig` | struct | SWAG algorithm parameters |

**Docs**: [Stream Processing](stream-processing.md), [Advanced](advanced.md)

## cqels-core::parser

| Type | Kind | Description |
|------|------|-------------|
| `CqelsQlParser` | struct | CqelsQL (SPARQL-based) parser |
| `CypherQlParser` | struct | CypherQL (Cypher-based) parser |
| `ParseError` | enum | `Syntax(String)`, `Semantic(String)`, `Unsupported(String)` |
| `ParseResult<T>` | alias | `Result<T, ParseError>` |

**Docs**: [Query Languages](query-languages.md)

## cqels-core::parser::ast

| Type | Kind | Description |
|------|------|-------------|
| `CqelsQueryDefinition` | struct | CqelsQL parsed AST |
| `CypherQueryDefinition` | struct | CypherQL parsed AST |
| `StreamSource` | struct | Stream name + window spec |
| `SelectElement` | struct | SELECT clause element |
| `PatternGroup` | struct | Group of patterns with source context |
| `PatternSource` | enum | `Stream`, `Static`, `Graph`, `Default` |
| `CypherPattern` | struct | Nodes + relationships |
| `NodePattern` | struct | Node variable, labels, properties |
| `RelationshipPattern` | struct | Relationship types, direction, path length |
| `ReturnExpression` | struct | RETURN clause item |
| `AggregateSpec` | struct | Aggregate function specification |
| `OrderByCondition` | struct | ORDER BY expression + direction |
| `WindowSpec` | struct | Window type + duration/count |

## cqels-core::query

| Type | Kind | Description |
|------|------|-------------|
| `ContinuousQuery` | trait | Async continuous query interface |
| `QueryInputs` | struct | Named stream inputs for query execution |
| `QueryType` | enum | `Sparql`, `Cypher`, `Custom` |

## cqels-engine

| Type | Kind | Description |
|------|------|-------------|
| `StreamEngine` | trait | Async engine lifecycle (register, start, stop) |
| `ReactiveStreamEngine` | struct | Tokio broadcast-based engine |

**Docs**: [Advanced](advanced.md)

## cqels-engine::cep

| Type | Kind | Description |
|------|------|-------------|
| `Pattern<T>` | struct | Fluent CEP pattern builder |
| `NfaPatternProcessor<T>` | struct | NFA compiler + stream processor |
| `PatternMatch<T>` | struct | Completed match with events and metadata |
| `Contiguity` | enum | `Strict`, `Relaxed`, `NonDeterministic` |
| `Quantifier` | enum | `One`, `OneOrMore`, `Times(usize)` |

**Docs**: [CEP](cep.md)

## cqels-reasoning

| Type | Kind | Description |
|------|------|-------------|
| `ReteNetwork` | struct | Top-level RETE reasoning engine |
| `InferredRdfStreamElement` | struct | Inferred triple with provenance |
| `Rule` | struct | If-then rule with patterns and templates |
| `RuleSet` | struct | Collection of rules |
| `RuleCondition` | struct | Rule LHS: patterns + filters |
| `RuleConsequent` | struct | Rule RHS: triple templates |
| `TriplePattern` | struct | Pattern with variables/constants |
| `TripleTemplate` | struct | Template for producing triples |
| `PatternTerm` | enum | `Variable(String)`, `Constant(Term)` |
| `AlphaNetwork` | struct | Single-pattern filter network |
| `AlphaNode` | struct | Individual pattern filter |
| `AlphaMatch` | struct | Matched pattern + bindings |
| `BetaNetwork` | struct | Multi-pattern join network |
| `BetaNode` | struct | Individual join node |
| `WorkingMemory` | struct | Indexed fact storage |
| `FactIndex` | struct | Term-to-statement index |
| `Activation` | struct | Matched rule ready to fire |
| `ConflictResolution` | enum | `FIFO`, `Lex`, `MEA` |
| `ConflictResolver` | struct | Conflict resolution strategy |
| `ReasoningConfig` | struct | Engine configuration (builder pattern) |

**Docs**: [Reasoning](reasoning.md)

## cqels-benchmarks

| Function | Description |
|----------|-------------|
| `generate_rdf_stream_batch(count)` | Generic sensor-value triples |
| `generate_sensor_readings(count)` | Temperature, humidity, pressure readings |
| `generate_social_events(count)` | Follows, likes, posts relationships |
| `default_window_duration()` | Returns 10 seconds |

## Generating Full API Docs

```bash
cargo doc --workspace --no-deps --open
```

This generates and opens the complete API documentation with all method
signatures, trait implementations, and inline code examples.
