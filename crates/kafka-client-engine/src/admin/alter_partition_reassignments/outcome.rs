//! Stable engine terminal values for partition-reassignment alteration.

use core::fmt;

use kafka_client_core::{
    AlterPartitionReassignmentResult as CoreResult,
    AlterPartitionReassignmentsFailureKind as CoreFailureKind,
    AlterPartitionReassignmentsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsDeliveryStatus {
    /// The request definitely did not reach Kafka.
    NotSent,
    /// The request may have reached Kafka.
    PossiblySent,
}

/// Exact signed broker error and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl AlterPartitionReassignmentBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's bounded nullable diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic was shortened or omitted.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into stable scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// One caller-ordered per-partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentResult {
    topic: String,
    partition: i32,
    result: Result<(), AlterPartitionReassignmentBrokerError>,
}

impl AlterPartitionReassignmentResult {
    /// Consumes the result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Result<(), AlterPartitionReassignmentBrokerError>,
    ) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentsBatch {
    throttle_time_ms: u32,
    partitions: Vec<AlterPartitionReassignmentResult>,
}

impl AlterPartitionReassignmentsBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AlterPartitionReassignmentResult>) {
        (self.throttle_time_ms, self.partitions)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsFailureKind {
    /// The public deadline elapsed.
    DeadlineElapsed,
    /// The bounded driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after admission.
    Transport,
    /// Kafka returned a request-wide broker error.
    Broker(AlterPartitionReassignmentBrokerError),
    /// Retaining the broker response exceeded the bounded byte budget.
    ResponseTooLarge,
    /// The broker supports no compatible API version.
    Compatibility,
    /// Kafka returned structurally invalid or uncorrelated data.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentsFailure {
    kind: AlterPartitionReassignmentsFailureKind,
    delivery: AlterPartitionReassignmentsDeliveryStatus,
}

impl AlterPartitionReassignmentsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &AlterPartitionReassignmentsFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> AlterPartitionReassignmentsDeliveryStatus {
        self.delivery
    }

    /// Consumes this failure into its stable parts.
    pub fn into_parts(
        self,
    ) -> (
        AlterPartitionReassignmentsFailureKind,
        AlterPartitionReassignmentsDeliveryStatus,
    ) {
        (self.kind, self.delivery)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsOutcome {
    /// Kafka returned caller-correlated per-partition outcomes.
    Altered(AlterPartitionReassignmentsBatch),
    /// The whole operation terminated without a valid per-partition batch.
    Failed(AlterPartitionReassignmentsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsObserverError {
    /// The retained terminal was already consumed.
    AlreadyObserved,
    /// The observer no longer names a retained completion.
    Stale,
}

impl fmt::Display for AlterPartitionReassignmentsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "AlterPartitionReassignments result was already observed",
            Self::Stale => "AlterPartitionReassignments observer is stale",
        })
    }
}

impl std::error::Error for AlterPartitionReassignmentsObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AlterPartitionReassignmentsOutcome {
    match terminal {
        CoreTerminal::Altered(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            AlterPartitionReassignmentsOutcome::Altered(AlterPartitionReassignmentsBatch {
                throttle_time_ms,
                partitions: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (topic, partition, result) = outcome.into_parts();
                        let result = match result {
                            CoreResult::Altered => Ok(()),
                            CoreResult::Failed(error) => Err(broker_error(error)),
                        };
                        AlterPartitionReassignmentResult {
                            topic,
                            partition,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let (kind, delivery) = failure.into_parts();
            AlterPartitionReassignmentsOutcome::Failed(AlterPartitionReassignmentsFailure {
                kind: failure_kind(kind),
                delivery: delivery_status(delivery),
            })
        }
    }
}

fn broker_error(
    error: kafka_client_core::AlterPartitionReassignmentBrokerError,
) -> AlterPartitionReassignmentBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    AlterPartitionReassignmentBrokerError {
        code,
        message,
        message_truncated,
    }
}

fn failure_kind(kind: CoreFailureKind) -> AlterPartitionReassignmentsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AlterPartitionReassignmentsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AlterPartitionReassignmentsFailureKind::DriverRejected,
        CoreFailureKind::Transport => AlterPartitionReassignmentsFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            AlterPartitionReassignmentsFailureKind::Broker(broker_error(error))
        }
        CoreFailureKind::ResponseTooLarge => {
            AlterPartitionReassignmentsFailureKind::ResponseTooLarge
        }
        CoreFailureKind::Compatibility => AlterPartitionReassignmentsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AlterPartitionReassignmentsFailureKind::InvalidResponse,
    }
}

const fn delivery_status(
    delivery: CoreDeliveryStatus,
) -> AlterPartitionReassignmentsDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => AlterPartitionReassignmentsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AlterPartitionReassignmentsDeliveryStatus::PossiblySent,
    }
}
