//! Stable terminal values for transaction initialization.

use kafka_client_core::{
    DeliveryStatus, TransactionInitializationBrokerCategory,
    TransactionInitializationFailureKind as CoreFailureKind, TransactionInitializationTerminal,
};

use super::{
    RetainedTransactionInitializationOutcome, TransactionInitializationHostError,
    TransactionInitializationObserver, TransactionalOwnerHandle,
};

/// Advisory fault reported only after initialization was accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationAcceptedFaultKind {
    /// The operation was accepted but the advisory host wake failed.
    Wake,
    /// The operation was accepted but its owner reported an invariant fault.
    HostInvariant,
}

/// Accepted initialization with one sole terminal observer.
#[derive(Debug)]
#[must_use = "accepted initialization retains its sole terminal observer"]
pub struct TransactionInitializationAccepted {
    pub(super) observer: TransactionInitializationObserver,
    pub(super) fault: Option<TransactionInitializationAcceptedFaultKind>,
}

impl TransactionInitializationAccepted {
    /// Returns an advisory post-acceptance host diagnostic.
    pub const fn fault(&self) -> Option<TransactionInitializationAcceptedFaultKind> {
        self.fault
    }

    /// Transfers the sole terminal observer.
    pub fn into_observer(self) -> TransactionInitializationObserver {
        self.observer
    }
}

/// Delivery certainty for a failed initialization attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationDeliveryStatus {
    /// The request definitely did not enter transport ownership.
    NotSent,
    /// Kafka may have observed the request.
    PossiblySent,
}

/// Stable semantic category for a terminal initialization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationFailureKind {
    /// The original absolute operation deadline elapsed.
    DeadlineElapsed,
    /// Bounded driver admission rejected before transport ownership.
    DriverRejected,
    /// Transport failed after accepting the request.
    Transport,
    /// Kafka returned an exact signed broker code and optional fencing fact.
    Broker {
        /// Kafka's exact signed protocol error code.
        code: i16,
        /// Whether core classified the broker response as fencing.
        fenced: bool,
    },
    /// The broker response violated the transaction initialization contract.
    InvalidResponse,
    /// The initialized identity could not be installed into bounded execution ownership.
    ExecutionUnavailable,
}

/// One terminal initialization failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionInitializationFailure {
    pub(crate) kind: TransactionInitializationFailureKind,
    pub(crate) delivery: TransactionInitializationDeliveryStatus,
}

impl TransactionInitializationFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> TransactionInitializationFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> TransactionInitializationDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal initialization decision.
#[derive(Debug)]
pub enum TransactionInitializationOutcome {
    /// Kafka initialized one unique idle owner.
    Initialized(TransactionalOwnerHandle),
    /// Initialization failed without producing an owner.
    Failed(TransactionInitializationFailure),
}

/// Failure to observe a named initialization completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionInitializationObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl std::fmt::Display for TransactionInitializationObserverError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "transaction initialization was already observed",
            Self::Stale => "transaction initialization observer is stale",
        })
    }
}

impl std::error::Error for TransactionInitializationObserverError {}

pub(super) fn failed_retained_outcome(
    terminal: TransactionInitializationTerminal,
) -> Option<RetainedTransactionInitializationOutcome> {
    let TransactionInitializationTerminal::Failed(failure) = terminal else {
        return None;
    };
    let kind = match failure.kind() {
        CoreFailureKind::DeadlineElapsed => {
            self::TransactionInitializationFailureKind::DeadlineElapsed
        }
        CoreFailureKind::DriverRejected => {
            self::TransactionInitializationFailureKind::DriverRejected
        }
        CoreFailureKind::Transport => self::TransactionInitializationFailureKind::Transport,
        CoreFailureKind::Broker(broker) => self::TransactionInitializationFailureKind::Broker {
            code: broker.code(),
            fenced: broker.category() == TransactionInitializationBrokerCategory::Fenced,
        },
        CoreFailureKind::InvalidResponse => {
            self::TransactionInitializationFailureKind::InvalidResponse
        }
    };
    Some(RetainedTransactionInitializationOutcome::Failed(
        TransactionInitializationFailure {
            kind,
            delivery: delivery(failure.delivery()),
        },
    ))
}

pub(super) const fn accepted_fault(
    error: TransactionInitializationHostError,
) -> TransactionInitializationAcceptedFaultKind {
    match error {
        TransactionInitializationHostError::Wake => {
            TransactionInitializationAcceptedFaultKind::Wake
        }
        _ => TransactionInitializationAcceptedFaultKind::HostInvariant,
    }
}

pub(super) const fn execution_unavailable_retained_outcome()
-> RetainedTransactionInitializationOutcome {
    RetainedTransactionInitializationOutcome::Failed(TransactionInitializationFailure {
        kind: TransactionInitializationFailureKind::ExecutionUnavailable,
        delivery: TransactionInitializationDeliveryStatus::PossiblySent,
    })
}

const fn delivery(status: DeliveryStatus) -> TransactionInitializationDeliveryStatus {
    match status {
        DeliveryStatus::NotSent => TransactionInitializationDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => TransactionInitializationDeliveryStatus::PossiblySent,
    }
}
