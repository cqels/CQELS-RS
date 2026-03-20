pub mod aggregate;
pub mod bind;
pub mod filter;
pub mod join;
pub mod parallel;
pub mod ranking;
pub mod rspql;
pub mod swag;

// Re-export key types for ergonomic imports.
pub use aggregate::{
    AggregateFunction, AggregateResult, AvgAggregate, CountAggregate, GroupKey, MaxAggregate,
    MinAggregate, RetractableAggregateFunction, SumAggregate, WindowedAggregateOperator,
};
pub use bind::BindOperator;
pub use filter::FilterOperator;
pub use join::{
    GraphPatternJoinState, IntervalJoinState, JoinFunction, JoinResult, TaggedUnion,
    VariableLengthPathOperator, WindowedJoinState,
};
pub use parallel::{
    AggregationBackend, ParallelExecutionConfig, ParallelExecutionConfigBuilder, SwagConfig,
};
pub use ranking::{RankedElement, SortDirection, SortKey, TopKOperator};
pub use rspql::{DStreamOperator, IStreamOperator, RStreamOperator, WindowSnapshot, WindowUpdate};
pub use swag::{SwagCountOp, SwagMaxOp, SwagMeanOp, SwagMinOp, SwagOp, SwagSumOp};
