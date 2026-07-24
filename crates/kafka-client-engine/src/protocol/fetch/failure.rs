//! Exact local failures for one bounded Fetch response normalization.

use kafka_wire_records::RecordError;

/// Partition offset fact whose only absent sentinel is exactly `-1`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchPartitionOffset {
    HighWatermark,
    LastStableOffset,
    LogStartOffset,
}

/// Why a bounded generated Fetch response could not become engine values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FetchDecodeFailure {
    ResponseRetainedBytes {
        actual: usize,
        limit: usize,
    },
    ResponseAllocations {
        actual: usize,
        limit: usize,
    },
    TopicCount {
        actual: usize,
        limit: usize,
    },
    PartitionCount {
        actual: usize,
        limit: usize,
    },
    EndpointCount {
        actual: usize,
        limit: usize,
    },
    BatchCount {
        actual: usize,
        limit: usize,
    },
    RecordCount {
        actual: usize,
        limit: usize,
    },
    HeaderCount {
        actual: usize,
        limit: usize,
    },
    AbortedTransactionCount {
        actual: usize,
        limit: usize,
    },
    LogicalRecordBytes {
        actual: usize,
        limit: usize,
    },
    CompressedBackingBytes {
        actual: usize,
        limit: usize,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    NegativeSessionId {
        actual: i32,
    },
    NegativePartitionIndex {
        actual: i32,
    },
    InvalidCurrentLeader {
        leader_id: i32,
        leader_epoch: i32,
    },
    InvalidPreferredReplica {
        actual: i32,
    },
    InvalidPartitionOffset {
        fact: FetchPartitionOffset,
        actual: i64,
    },
    InvalidEpochEndOffset {
        epoch: i32,
        end_offset: i64,
    },
    InvalidEndpointNodeId {
        actual: i32,
    },
    InvalidEndpointPort {
        actual: i32,
    },
    NegativeLastOffsetDelta {
        actual: i32,
    },
    NegativeBaseOffset {
        actual: i64,
    },
    NextOffsetOverflow {
        last_offset: i64,
    },
    BatchOffsetOverlap {
        previous_last_offset: i64,
        base_offset: i64,
    },
    InvalidPartitionLeaderEpoch {
        actual: i32,
    },
    InvalidBatchTimestamps {
        base_timestamp: i64,
        max_timestamp: i64,
    },
    OffsetOverflow,
    TimestampOverflow,
    NegativeRecordTimestamp {
        actual: i64,
    },
    RecordTimestampAfterBatchMax {
        actual: i64,
        max_timestamp: i64,
    },
    TimestampDeltaWithoutTimestamp {
        actual: i64,
    },
    InvalidProducerIdentity {
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    },
    TransactionalIdentityMissing,
    InvalidAbortedTransaction {
        producer_id: i64,
        first_offset: i64,
    },
    RecordOffsetOutsideBatch {
        offset: i64,
        first: i64,
        last: i64,
    },
    RecordOffsetsNotIncreasing {
        previous: i64,
        actual: i64,
    },
    AccountingOverflow,
    RecordBatch {
        topic: usize,
        partition: usize,
        batch: usize,
        source: RecordError,
    },
}
