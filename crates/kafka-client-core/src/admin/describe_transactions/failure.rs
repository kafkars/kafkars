//! Whole-operation mechanism failures for Admin `DescribeTransactions`.

use crate::DeliveryStatus;

/// Whole-operation failure category outside exact per-ID broker outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected one prepared coordinator call.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsFailure {
    kind: AdminDescribeTransactionsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminDescribeTransactionsFailure {
    pub(crate) const fn new(
        kind: AdminDescribeTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeTransactionsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}
