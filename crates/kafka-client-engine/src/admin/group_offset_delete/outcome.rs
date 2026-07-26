//! Stable engine terminal values for consumer-group offset deletion.

use core::fmt;

use kafka_client_core::{
    DeleteConsumerGroupOffsetResult as CoreOffsetResult,
    DeleteConsumerGroupOffsetsFailureKind as CoreFailureKind,
    DeleteConsumerGroupOffsetsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetBrokerError {
    code: i16,
}

impl DeleteConsumerGroupOffsetBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One caller-ordered topic-partition deletion result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetResult {
    topic: String,
    partition: i32,
    result: Result<(), DeleteConsumerGroupOffsetBrokerError>,
}

impl DeleteConsumerGroupOffsetResult {
    /// Consumes this result into stable identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<(), DeleteConsumerGroupOffsetBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    offsets: Vec<DeleteConsumerGroupOffsetResult>,
}

impl DeleteConsumerGroupOffsetsBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<DeleteConsumerGroupOffsetResult>) {
        (self.throttle_time_ms, self.offsets)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsFailureKind {
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
pub struct DeleteConsumerGroupOffsetsFailure {
    kind: DeleteConsumerGroupOffsetsFailureKind,
    delivery: DeleteConsumerGroupOffsetsDeliveryStatus,
}

impl DeleteConsumerGroupOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DeleteConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeleteConsumerGroupOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsOutcome {
    /// Ordered per-partition outcomes plus broker throttle.
    Deleted(DeleteConsumerGroupOffsetsBatch),
    /// Whole-operation failure.
    Failed(DeleteConsumerGroupOffsetsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DeleteConsumerGroupOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DeleteConsumerGroupOffsets result was already observed",
            Self::Stale => "DeleteConsumerGroupOffsets observer is stale",
        })
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DeleteConsumerGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Deleted(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DeleteConsumerGroupOffsetsOutcome::Deleted(DeleteConsumerGroupOffsetsBatch {
                throttle_time_ms,
                offsets: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreOffsetResult::Deleted => Ok(()),
                            CoreOffsetResult::Failed(error) => {
                                Err(DeleteConsumerGroupOffsetBrokerError { code: error.code() })
                            }
                        };
                        DeleteConsumerGroupOffsetResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DeleteConsumerGroupOffsetsOutcome::Failed(DeleteConsumerGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> DeleteConsumerGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DeleteConsumerGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DeleteConsumerGroupOffsetsFailureKind::Transport,
        CoreFailureKind::Broker(code) => DeleteConsumerGroupOffsetsFailureKind::Broker(code.get()),
        CoreFailureKind::ResponseTooLarge => {
            DeleteConsumerGroupOffsetsFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => DeleteConsumerGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DeleteConsumerGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DeleteConsumerGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
