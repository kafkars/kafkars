//! Closed request and response rejection vocabulary for `ShareAcknowledge` v1.

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeRequestFailure {
    GroupId,
    MemberId,
    SessionFence,
    Empty,
    TopicCount { actual: usize, limit: usize },
    PartitionCount { actual: usize, limit: usize },
    BatchCount { actual: usize, limit: usize },
    ZeroTopicId,
    PartitionOutOfRange(u32),
    InvalidOffsets { first: i64, last: i64 },
    InvalidAcknowledgeTypes,
    NoncanonicalOrder,
    Allocation,
}

/// Generated response fact that cannot enter engine policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeResponseFailure {
    UnsupportedApiVersion(i16),
    NegativeThrottleTime(i32),
    UnexpectedV2LockTimeout(i32),
    TopicCount { actual: usize, limit: usize },
    PartitionCount { actual: usize, limit: usize },
    EndpointCount { actual: usize, limit: usize },
    ZeroTopicId,
    UnknownTopic,
    DuplicateTopic,
    NegativePartition(i32),
    UnknownPartition(u32),
    DuplicatePartition(u32),
    MissingPartition,
    UnexpectedErrorMessage,
    DiagnosticTooLarge { actual: usize, limit: usize },
    InvalidCurrentLeader { leader_id: i32, leader_epoch: i32 },
    MissingLeaderEndpoint(i32),
    InvalidEndpointNodeId(i32),
    InvalidEndpointPort(i32),
    EmptyEndpointHost,
    DuplicateEndpoint(i32),
    Allocation,
}
