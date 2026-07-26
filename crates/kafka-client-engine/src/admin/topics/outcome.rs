//! Engine-owned terminal representation for one ordered `DescribeTopics` query.

use core::fmt;

mod translate;
pub(crate) use translate::translate_terminal;
#[cfg(test)]
mod translate_test;

/// Stable admin delivery certainty independent of core types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one normalized topic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeTopicError {
    code: i16,
}

impl DescribeTopicError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// Stable description of one topic partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartitionDescription {
    partition_index: i32,
    error_code: Option<i16>,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    in_sync_replicas: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl TopicPartitionDescription {
    /// Consumes the description into stable adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<i16>,
        Option<i32>,
        Option<i32>,
        Vec<i32>,
        Vec<i32>,
        Vec<i32>,
    ) {
        (
            self.partition_index,
            self.error_code,
            self.leader_id,
            self.leader_epoch,
            self.replicas,
            self.in_sync_replicas,
            self.offline_replicas,
        )
    }
}

/// Stable description of one normalized topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicDescription {
    name: String,
    topic_id: Option<[u8; 16]>,
    internal: bool,
    partitions: Vec<TopicPartitionDescription>,
}

impl TopicDescription {
    /// Consumes the description into stable adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<[u8; 16]>,
        bool,
        Vec<TopicPartitionDescription>,
    ) {
        (self.name, self.topic_id, self.internal, self.partitions)
    }
}

/// One per-topic terminal in policy-defined deterministic order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicResult {
    topic: String,
    internal: bool,
    result: Result<TopicDescription, DescribeTopicError>,
}

impl DescribeTopicResult {
    /// Consumes the ordered result into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, bool, Result<TopicDescription, DescribeTopicError>) {
        (self.topic, self.internal, self.result)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicsFailureKind {
    /// The original deadline elapsed before driver ownership.
    DeadlineElapsed,
    /// The generated request was rejected before driver ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the whole Metadata request with this exact code.
    Broker(i16),
    /// A valid response exceeded the admitted retained-result budget.
    ResponseTooLarge,
    /// The broker cannot represent the operation's required read-only policy.
    Compatibility,
    /// The broker response could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeTopicsFailure {
    kind: DescribeTopicsFailureKind,
    delivery: DescribeTopicsDeliveryStatus,
}

impl DescribeTopicsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeTopicsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeTopicsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeTopicsOutcome {
    /// Ordered broker outcomes.
    Topics(Vec<DescribeTopicResult>),
    /// Whole-operation failure outside per-topic results.
    Failed(DescribeTopicsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeTopicsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DescribeTopics result was already observed",
            Self::Stale => "DescribeTopics observer is stale",
        })
    }
}

impl std::error::Error for DescribeTopicsObserverError {}
