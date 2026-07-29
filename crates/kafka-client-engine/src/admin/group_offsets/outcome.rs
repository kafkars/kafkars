//! Stable engine terminal values for consumer-group offset listing.

use core::fmt;

mod translate;
pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupOffsetBrokerError {
    code: i16,
}

impl GroupOffsetBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One normalized committed next-offset description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupOffsetDescription {
    offset: Option<i64>,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl GroupOffsetDescription {
    /// Consumes this description into stable scalar parts.
    pub fn into_parts(self) -> (Option<i64>, Option<i32>, Option<String>) {
        (self.offset, self.leader_epoch, self.metadata)
    }
}

/// One ordered topic-partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupOffsetResult {
    topic: String,
    partition: i32,
    result: Result<GroupOffsetDescription, GroupOffsetBrokerError>,
}

impl GroupOffsetResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<GroupOffsetDescription, GroupOffsetBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    offsets: Vec<GroupOffsetResult>,
}

impl ListConsumerGroupOffsetsBatch {
    /// Consumes the batch into throttle and ordered partition results.
    pub fn into_parts(self) -> (u32, Vec<GroupOffsetResult>) {
        (self.throttle_time_ms, self.offsets)
    }
}

/// Exact result for one consumer group in a caller-ordered batch operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupBatchOutcome {
    /// Kafka returned this group's ordered committed offsets.
    Offsets {
        /// Exact requested consumer-group identity.
        group_id: String,
        /// Ordered partition outcomes and this group's throttle.
        offsets: ListConsumerGroupOffsetsBatch,
    },
    /// Kafka rejected this specific consumer group.
    BrokerRejected {
        /// Exact requested consumer-group identity.
        group_id: String,
        /// Kafka's exact signed nonzero group error code.
        code: i16,
    },
}

impl ListConsumerGroupBatchOutcome {
    /// Returns the exact requested consumer-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Offsets { group_id, .. } | Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Consumes this outcome into its group and exact broker result.
    pub fn into_parts(self) -> (String, Result<ListConsumerGroupOffsetsBatch, i16>) {
        match self {
            Self::Offsets { group_id, offsets } => (group_id, Ok(offsets)),
            Self::BrokerRejected { group_id, code } => (group_id, Err(code)),
        }
    }
}

/// Caller-ordered outcomes plus maximum throttle across coordinator calls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<ListConsumerGroupBatchOutcome>,
}

impl ListConsumerGroupsOffsetsBatch {
    /// Returns the maximum nonnegative throttle observed across calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[ListConsumerGroupBatchOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<ListConsumerGroupBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the explicit group with this exact signed code.
    Broker(i16),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API cannot represent the requested semantics.
    Compatibility,
    /// The broker response could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsFailure {
    kind: ListConsumerGroupOffsetsFailureKind,
    delivery: ListConsumerGroupOffsetsDeliveryStatus,
}

impl ListConsumerGroupOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> ListConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> ListConsumerGroupOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsOutcome {
    /// Ordered partition outcomes plus broker throttle.
    Offsets(ListConsumerGroupOffsetsBatch),
    /// Every requested consumer group settled in caller order.
    Batch(ListConsumerGroupsOffsetsBatch),
    /// Whole-operation failure.
    Failed(ListConsumerGroupOffsetsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for ListConsumerGroupOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "ListConsumerGroupOffsets result was already observed",
            Self::Stale => "ListConsumerGroupOffsets observer is stale",
        })
    }
}

impl std::error::Error for ListConsumerGroupOffsetsObserverError {}
