//! Whole-operation mechanism failures for Admin `FenceProducers`.

use crate::DeliveryStatus;

/// Whole-operation failure category outside exact per-ID broker outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersFailureKind {
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
pub struct AdminFenceProducersFailure {
    kind: AdminFenceProducersFailureKind,
    delivery: DeliveryStatus,
}

impl AdminFenceProducersFailure {
    pub(crate) const fn new(
        kind: AdminFenceProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminFenceProducersFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}
