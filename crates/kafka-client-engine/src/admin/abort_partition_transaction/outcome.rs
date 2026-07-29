//! Stable generated-free terminals for one partition transaction abort.

use core::fmt;

use kafka_client_core::{
    AbortPartitionTransactionFailureKind as CoreFailureKind,
    AbortPartitionTransactionTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionDeliveryStatus {
    /// The destructive call definitely did not reach Kafka transport ownership.
    NotSent,
    /// Kafka may have received the destructive call.
    PossiblySent,
}

/// Exact signed Kafka rejection for the requested partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionBrokerError {
    code: i16,
}

impl AbortPartitionTransactionBrokerError {
    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent the abort.
    Compatibility,
    /// A response was malformed or contradicted the request identity.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionFailure {
    kind: AbortPartitionTransactionFailureKind,
    delivery: AbortPartitionTransactionDeliveryStatus,
}

impl AbortPartitionTransactionFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AbortPartitionTransactionFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AbortPartitionTransactionDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionOutcome {
    /// Kafka accepted the abort marker.
    Aborted,
    /// Kafka rejected the requested partition with an exact signed code.
    BrokerRejected(AbortPartitionTransactionBrokerError),
    /// Execution failed outside an exact Kafka partition rejection.
    Failed(AbortPartitionTransactionFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AbortPartitionTransactionObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "partition transaction-abort result was already observed",
            Self::Stale => "partition transaction-abort observer is stale",
        })
    }
}

impl std::error::Error for AbortPartitionTransactionObserverError {}

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> AbortPartitionTransactionOutcome {
    match terminal {
        CoreTerminal::Aborted => AbortPartitionTransactionOutcome::Aborted,
        CoreTerminal::BrokerRejected(error) => {
            AbortPartitionTransactionOutcome::BrokerRejected(AbortPartitionTransactionBrokerError {
                code: error.code(),
            })
        }
        CoreTerminal::Failed(failure) => {
            AbortPartitionTransactionOutcome::Failed(AbortPartitionTransactionFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

const fn failure_kind(kind: CoreFailureKind) -> AbortPartitionTransactionFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => AbortPartitionTransactionFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => AbortPartitionTransactionFailureKind::DriverRejected,
        CoreFailureKind::Transport => AbortPartitionTransactionFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => AbortPartitionTransactionFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => AbortPartitionTransactionFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => AbortPartitionTransactionFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> AbortPartitionTransactionDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => AbortPartitionTransactionDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => AbortPartitionTransactionDeliveryStatus::PossiblySent,
    }
}
