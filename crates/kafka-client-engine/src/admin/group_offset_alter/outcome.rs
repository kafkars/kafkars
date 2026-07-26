//! Stable engine terminal values for consumer-group offset alteration.

use core::fmt;

use kafka_client_core::{
    AlterConsumerGroupOffsetResult as CoreOffsetResult,
    AlterConsumerGroupOffsetsFailureKind as CoreFailureKind,
    AlterConsumerGroupOffsetsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact broker rejection for one requested topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetBrokerError {
    code: i16,
}

impl AlterConsumerGroupOffsetBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One caller-ordered topic-partition alteration result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetResult {
    topic: String,
    partition: i32,
    result: Result<(), AlterConsumerGroupOffsetBrokerError>,
}

impl AlterConsumerGroupOffsetResult {
    /// Consumes this result into stable identity and exact broker outcome.
    pub fn into_parts(self) -> (String, i32, Result<(), AlterConsumerGroupOffsetBrokerError>) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    offsets: Vec<AlterConsumerGroupOffsetResult>,
}

impl AlterConsumerGroupOffsetsBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AlterConsumerGroupOffsetResult>) {
        (self.throttle_time_ms, self.offsets)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API cannot represent the requested semantics.
    Compatibility,
    /// The broker response could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetsFailure {
    kind: AlterConsumerGroupOffsetsFailureKind,
    delivery: AlterConsumerGroupOffsetsDeliveryStatus,
}

impl AlterConsumerGroupOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AlterConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AlterConsumerGroupOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsOutcome {
    /// Ordered per-partition outcomes plus broker throttle.
    Altered(AlterConsumerGroupOffsetsBatch),
    /// Whole-operation failure.
    Failed(AlterConsumerGroupOffsetsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AlterConsumerGroupOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "AlterConsumerGroupOffsets result was already observed",
            Self::Stale => "AlterConsumerGroupOffsets observer is stale",
        })
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterConsumerGroupOffsetsOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterConsumerGroupOffsetsOutcome::Altered(AlterConsumerGroupOffsetsBatch {
                throttle_time_ms,
                offsets: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreOffsetResult::Altered => Ok(()),
                            CoreOffsetResult::Failed(error) => {
                                Err(AlterConsumerGroupOffsetBrokerError { code: error.code() })
                            }
                        };
                        AlterConsumerGroupOffsetResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AlterConsumerGroupOffsetsOutcome::Failed(AlterConsumerGroupOffsetsFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AlterConsumerGroupOffsetsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterConsumerGroupOffsetsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterConsumerGroupOffsetsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AlterConsumerGroupOffsetsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AlterConsumerGroupOffsetsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterConsumerGroupOffsetsFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AlterConsumerGroupOffsetsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AlterConsumerGroupOffsetsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AlterConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    }
}
