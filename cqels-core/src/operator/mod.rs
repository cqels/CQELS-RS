//! Stream processing operators for the CQELS query pipeline.
//!
//! This module provides the core operators used during query execution:
//! aggregation, filtering, joins, ranking, RSP-QL stream semantics,
//! sliding-window aggregation (SWAG), and parallel execution configuration.

pub mod aggregate;
pub mod bind;
pub mod filter;
pub mod join;
pub mod minus;
pub mod parallel;
pub mod ranking;
pub mod rspql;
pub mod swag;

// Re-export key types for ergonomic imports.
pub use aggregate::{
    AggregateFunction, AggregateResult, AvgAccumulator, AvgAggregate, CountAggregate, GroupKey,
    MaxAggregate, MinAggregate, RetractableAggregateFunction, SumAggregate,
    WindowedAggregateOperator,
};
pub use bind::BindOperator;
pub use filter::FilterOperator;
pub use join::{
    EdgeInfo, GraphPatternJoinState, IntervalJoinState, JoinFunction, JoinResult, TaggedUnion,
    VariableLengthPathOperator, WindowedJoinState,
};
pub use minus::{compatible, MinusOperator};
pub use parallel::{
    AggregationBackend, ParallelExecutionConfig, ParallelExecutionConfigBuilder, SwagConfig,
};
pub use ranking::{RankedElement, SortDirection, SortKey, TopKOperator};
pub use rspql::{DStreamOperator, IStreamOperator, RStreamOperator, WindowSnapshot, WindowUpdate};
pub use swag::{
    MeanPartial, SwagCountOp, SwagMaxOp, SwagMeanOp, SwagMinOp, SwagOp, SwagSumOp,
    TwoStacksLiteWindow,
};
