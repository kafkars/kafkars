//! Exact broker and mechanism terminals for a partition-transaction abort.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact signed API-27 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionBrokerError {
    code: NonZeroI16,
}

impl AbortPartitionTransactionBrokerError {
    /// Creates one exact nonzero Kafka error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Returns the conservative delivery certainty of an observed broker response.
    pub const fn delivery(self) -> DeliveryStatus {
        DeliveryStatus::PossiblySent
    }
}

/// Whole-operation failure outside an exact API-27 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared destructive request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent the request.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortPartitionTransactionFailure {
    kind: AbortPartitionTransactionFailureKind,
    delivery: DeliveryStatus,
}

impl AbortPartitionTransactionFailure {
    pub(crate) const fn new(
        kind: AbortPartitionTransactionFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AbortPartitionTransactionFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for an Admin partition-transaction abort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionTerminal {
    /// Kafka accepted the abort marker for the exact partition transaction.
    Aborted,
    /// Kafka rejected the request with an exact signed error.
    BrokerRejected(AbortPartitionTransactionBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(AbortPartitionTransactionFailure),
}
