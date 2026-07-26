//! Deliberate extra partition-count state outside the closed ownership sequence.

enum ClassicGroupExecutionState {
    PreparedPartitionCounts,
    PartitionCountHandoff,
    PartitionCountDriverOwned,
    PartitionCountCompletionFault,
    PartitionCountsPostCore,
    PartitionCountHiddenRetry,
}
