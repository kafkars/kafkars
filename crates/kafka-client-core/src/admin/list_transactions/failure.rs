//! Whole-operation failures for Admin `ListTransactions`.

use crate::DeliveryStatus;

/// Failure outside exact discovery and per-broker Kafka errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected discovery or one exact-broker call.
    DriverRejected,
    /// Driver-owned transport failed.
    Transport,
    /// Discovered or returned facts exceeded retained capacity.
    ResponseTooLarge,
    /// No compatible protocol version represented the requested filters.
    Compatibility,
    /// A response was malformed, uncorrelatable, or contradictory.
    InvalidResponse,
}

/// Whole-operation failure with cumulative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsFailure {
    kind: AdminListTransactionsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminListTransactionsFailure {
    pub(crate) const fn new(
        kind: AdminListTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AdminListTransactionsFailureKind {
        self.kind
    }

    /// Returns driver-authoritative cumulative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}
