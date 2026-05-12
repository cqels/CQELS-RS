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
pub mod parallel_aggregate;
pub mod parallel_hash_join;
pub mod ranking;
pub mod rspql;
pub mod swag;
pub mod swag_ctrie;
pub mod swag_join;
pub mod swag_join_state;

// Re-export key types for ergonomic imports.
pub use aggregate::{
    AggregateFunction, AggregateResult, AvgAccumulator, AvgAggregate, CountAggregate, GroupKey,
    MaxAggregate, MinAggregate, RetractableAggregateFunction, SumAggregate,
    WindowedAggregateOperator,
};
pub use bind::BindOperator;
pub use filter::FilterOperator;
pub use join::{
    EdgeInfo, GraphPatternJoinState, IntervalJoinState, JoinFunction, JoinResult, SelfJoinPair,
    TaggedUnion, VariableLengthPathOperator, WindowedJoinState, WindowedSelfJoinState,
};
pub use minus::{compatible, MinusOperator};
pub use parallel::{
    AggregationBackend, ParallelExecutionConfig, ParallelExecutionConfigBuilder, SwagConfig,
};
pub use parallel_aggregate::ParallelWindowedAggregateOperator;
pub use parallel_hash_join::ParallelHashJoinOperator;
pub use ranking::{RankedElement, SortDirection, SortKey, TopKOperator};
pub use rspql::{DStreamOperator, IStreamOperator, RStreamOperator, WindowSnapshot, WindowUpdate};
pub use swag::{
    MeanPartial, SwagCountOp, SwagMaxOp, SwagMeanOp, SwagMinOp, SwagOp, SwagSumOp,
    TwoStacksLiteWindow,
};
pub use swag_ctrie::{CTrieIndex, RangeResult, TimeIndex, TimeIndexEntry};
pub use swag_join::{
    IndexStats, JoinRecord, KeyExtractor, KeyIndexedTimeIndex, VariableOrder, VariableOrderMethod,
};
pub use swag_join_state::{IntervalBounds, JoinAggregateSpec, PayloadType, WindowJoinState};
