//! Stable engine terminal values for consumer-group offset listing.

use core::fmt;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, GroupOffsetResult as CoreOffsetResult,
    ListConsumerGroupOffsetsFailureKind as CoreFailureKind,
    ListConsumerGroupOffsetsTerminal as CoreTerminal,
};

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

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> ListConsumerGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Offsets(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            ListConsumerGroupOffsetsOutcome::Offsets(ListConsumerGroupOffsetsBatch {
                throttle_time_ms,
                offsets: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreOffsetResult::Described(description) => {
                                let (offset, leader_epoch, metadata) = description.into_parts();
                                Ok(GroupOffsetDescription {
                                    offset,
                                    leader_epoch,
                                    metadata,
                                })
                            }
                            CoreOffsetResult::Failed(error) => {
                                Err(GroupOffsetBrokerError { code: error.code() })
                            }
                        };
                        GroupOffsetResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            ListConsumerGroupOffsetsOutcome::Failed(ListConsumerGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> ListConsumerGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => ListConsumerGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => ListConsumerGroupOffsetsFailureKind::Transport,
        CoreFailureKind::Broker(code) => ListConsumerGroupOffsetsFailureKind::Broker(code.get()),
        CoreFailureKind::ResponseTooLarge => ListConsumerGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => ListConsumerGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => ListConsumerGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> ListConsumerGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => ListConsumerGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => ListConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
