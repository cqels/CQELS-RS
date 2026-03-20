/// Configuration for parallel execution of stream operators.
///
/// Controls how the engine distributes stream processing across threads:
/// - **`parallelism`**: Number of worker threads (defaults to available CPUs).
/// - **`auto_parallelize`**: When `true`, streams exceeding `min_stream_size`
///   are automatically distributed across workers.
/// - **`min_stream_size`**: Threshold below which parallelization is skipped
///   (default: 1000 elements). Avoids overhead for small batches.
/// - **`aggregation_backend`**: Choose `Legacy` for full aggregate support
///   or `Swag` for high-throughput sliding-window aggregation (SUM/AVG/MAX/MIN/COUNT only).
///
/// # Example
///
/// ```
/// use cqels_core::operator::parallel::ParallelExecutionConfig;
///
/// let config = ParallelExecutionConfig::builder()
///     .parallelism(8)
///     .min_stream_size(500)
///     .auto_parallelize(true)
///     .build();
///
/// assert!(config.should_parallelize(1000));
/// assert!(!config.should_parallelize(100));
/// ```
#[derive(Clone, Debug)]
pub struct ParallelExecutionConfig {
    /// Number of parallel workers (defaults to available CPUs).
    pub parallelism: usize,
    /// Prefetch buffer size per worker (default: 256).
    pub prefetch: usize,
    /// Whether to automatically parallelize large streams (default: `true`).
    pub auto_parallelize: bool,
    /// Minimum stream size before parallelization kicks in (default: 1000).
    pub min_stream_size: usize,
    /// Which aggregation backend to use (default: `Legacy`).
    pub aggregation_backend: AggregationBackend,
    /// SWAG-specific configuration (used when `aggregation_backend` is `Swag`).
    pub swag_config: SwagConfig,
}

impl Default for ParallelExecutionConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        Self {
            parallelism,
            prefetch: 256,
            auto_parallelize: true,
            min_stream_size: 1000,
            aggregation_backend: AggregationBackend::Legacy,
            swag_config: SwagConfig::default(),
        }
    }
}

impl ParallelExecutionConfig {
    pub fn builder() -> ParallelExecutionConfigBuilder {
        ParallelExecutionConfigBuilder::default()
    }

    /// Returns whether a stream of the given size should be parallelized.
    pub fn should_parallelize(&self, stream_size: usize) -> bool {
        self.auto_parallelize && stream_size >= self.min_stream_size
    }
}

/// Builder for `ParallelExecutionConfig`.
#[derive(Default)]
pub struct ParallelExecutionConfigBuilder {
    config: ParallelExecutionConfig,
}

impl ParallelExecutionConfigBuilder {
    pub fn parallelism(mut self, parallelism: usize) -> Self {
        self.config.parallelism = parallelism;
        self
    }

    pub fn prefetch(mut self, prefetch: usize) -> Self {
        self.config.prefetch = prefetch;
        self
    }

    pub fn auto_parallelize(mut self, auto: bool) -> Self {
        self.config.auto_parallelize = auto;
        self
    }

    pub fn min_stream_size(mut self, min: usize) -> Self {
        self.config.min_stream_size = min;
        self
    }

    pub fn aggregation_backend(mut self, backend: AggregationBackend) -> Self {
        self.config.aggregation_backend = backend;
        self
    }

    pub fn swag_config(mut self, config: SwagConfig) -> Self {
        self.config.swag_config = config;
        self
    }

    pub fn build(self) -> ParallelExecutionConfig {
        self.config
    }
}

/// Aggregation backend selection.
///
/// - **`Legacy`**: Standard aggregation with full support for all aggregate
///   functions (SUM, AVG, MIN, MAX, COUNT, GROUP_CONCAT, SAMPLE, etc.).
/// - **`Swag`**: High-performance Sliding Window AGgregation using the
///   two-stacks-lite algorithm. Supports only commutative/invertible
///   aggregates: SUM, AVG, MAX, MIN, COUNT. Falls back to `Legacy` for
///   unsupported operations when `SwagConfig::fallback_to_legacy` is `true`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AggregationBackend {
    /// Standard aggregation — all aggregate types supported.
    Legacy,
    /// High-performance SWAG — only SUM, AVG, MAX, MIN, COUNT.
    Swag,
}

/// SWAG-specific configuration.
///
/// Maps to Java's `SwagConfig`.
#[derive(Clone, Debug)]
pub struct SwagConfig {
    /// Ordering mode.
    pub ordering: Ordering,
    /// FIFO algorithm selection.
    pub fifo_algorithm: FifoAlgorithm,
    /// Out-of-order algorithm selection.
    pub ooo_algorithm: OooAlgorithm,
    /// Parallel mode.
    pub parallel_mode: ParallelMode,
    /// Minimum elements before splitting for parallelism.
    pub split_threshold: usize,
    /// Maximum concurrent tasks.
    pub max_tasks: usize,
    /// Whether to use exponential split thresholds.
    pub exponential_split: bool,
    /// Whether to use C-trie snapshots for lock-free reads.
    pub use_ctrie_snapshots: bool,
    /// Whether to fall back to legacy on unsupported operations.
    pub fallback_to_legacy: bool,
}

impl Default for SwagConfig {
    fn default() -> Self {
        let max_tasks = std::thread::available_parallelism()
            .map(|p| p.get() * 2)
            .unwrap_or(8);
        Self {
            ordering: Ordering::InOrder,
            fifo_algorithm: FifoAlgorithm::TwoStacksLite,
            ooo_algorithm: OooAlgorithm::Reactive,
            parallel_mode: ParallelMode::KeyPartitionedOnly,
            split_threshold: 1000,
            max_tasks,
            exponential_split: true,
            use_ctrie_snapshots: true,
            fallback_to_legacy: true,
        }
    }
}

/// Stream ordering assumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Ordering {
    InOrder,
    OutOfOrder,
}

/// FIFO window algorithm selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FifoAlgorithm {
    TwoStacks,
    TwoStacksLite,
}

/// Out-of-order algorithm selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OooAlgorithm {
    Reactive,
}

/// Parallel execution mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParallelMode {
    KeyPartitionedOnly,
    BatchOnClose,
    Mixed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParallelExecutionConfig::default();
        assert!(config.parallelism > 0);
        assert_eq!(config.prefetch, 256);
        assert!(config.auto_parallelize);
        assert_eq!(config.min_stream_size, 1000);
        assert_eq!(config.aggregation_backend, AggregationBackend::Legacy);
    }

    #[test]
    fn test_builder() {
        let config = ParallelExecutionConfig::builder()
            .parallelism(8)
            .prefetch(512)
            .auto_parallelize(false)
            .aggregation_backend(AggregationBackend::Swag)
            .build();

        assert_eq!(config.parallelism, 8);
        assert_eq!(config.prefetch, 512);
        assert!(!config.auto_parallelize);
        assert_eq!(config.aggregation_backend, AggregationBackend::Swag);
    }

    #[test]
    fn test_should_parallelize() {
        let config = ParallelExecutionConfig::builder()
            .auto_parallelize(true)
            .min_stream_size(100)
            .build();

        assert!(!config.should_parallelize(50));
        assert!(config.should_parallelize(100));
        assert!(config.should_parallelize(200));
    }

    #[test]
    fn test_swag_config_default() {
        let config = SwagConfig::default();
        assert_eq!(config.ordering, Ordering::InOrder);
        assert_eq!(config.fifo_algorithm, FifoAlgorithm::TwoStacksLite);
        assert_eq!(config.parallel_mode, ParallelMode::KeyPartitionedOnly);
        assert!(config.use_ctrie_snapshots);
        assert!(config.fallback_to_legacy);
    }
}
