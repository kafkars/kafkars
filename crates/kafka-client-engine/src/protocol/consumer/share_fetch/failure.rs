//! Closed request and response rejection vocabulary for `ShareFetch` v1.

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchRequestFailure {
    GroupId,
    MemberId,
    SessionEpoch(i32),
    InitialRequestShape,
    TopicCount { actual: usize, limit: usize },
    PartitionCount { actual: usize, limit: usize },
    ZeroTopicId,
    EmptyTopic,
    DuplicateTopic,
    DuplicatePartition(u32),
    PartitionOutOfRange(u32),
    IncludedPartitionNotActive,
    ForgottenPartitionStillActive,
    MaxWaitOutOfRange(u32),
    MinBytesOutOfRange(u32),
    MaxBytesOutOfRange(u32),
    MaxRecordsOutOfRange(u32),
    BatchSizeOutOfRange(u32),
    MinBytesExceedMaxBytes { min_bytes: u32, max_bytes: u32 },
    Allocation,
}

/// Generated response fact that cannot enter engine policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchResponseFailure {
    UnsupportedApiVersion(i16),
    NegativeThrottleTime(i32),
    InvalidLockTimeout(i32),
    TopicCount { actual: usize, limit: usize },
    PartitionCount { actual: usize, limit: usize },
    EndpointCount { actual: usize, limit: usize },
    RangeCount { actual: usize, limit: usize },
    RecordCount { actual: u64, limit: u64 },
    RetainedBytes { actual: usize, limit: usize },
    ZeroTopicId,
    UnknownTopic,
    DuplicateTopic,
    NegativePartition(i32),
    UnknownPartition(u32),
    DuplicatePartition(u32),
    InvalidCurrentLeader { leader_id: i32, leader_epoch: i32 },
    InvalidEndpointNodeId(i32),
    InvalidEndpointPort(i32),
    EmptyEndpointHost,
    DuplicateEndpoint(i32),
    PartitionPayloadWithError,
    InvalidAcquiredOffsets { first: i64, last: i64 },
    InvalidDeliveryCount(i16),
    OverlappingAcquiredRange,
    Allocation,
}
