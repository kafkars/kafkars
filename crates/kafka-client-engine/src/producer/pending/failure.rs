//! Stable local failures for producer sends that never crossed core admission.

use crate::ProducerDeliveryStatus;

/// Why one waiting producer send settled before deterministic admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerSendFailureKind {
    /// The original absolute delivery deadline elapsed while waiting.
    DeadlineElapsed,
    /// Producer shutdown settled queued work before core admission.
    Shutdown,
    /// Producer admission was already closed at the call boundary.
    Closed,
    /// A bounded local count or byte owner could not retain the send.
    Backpressure,
    /// Explicit pre-admission cancellation won the ownership race.
    Cancelled,
}

/// Terminal local result for a send that definitely did not reach transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerSendFailure {
    kind: ProducerSendFailureKind,
}

impl ProducerSendFailure {
    /// Creates one local failure with an invariant `NotSent` status.
    pub const fn new(kind: ProducerSendFailureKind) -> Self {
        Self { kind }
    }

    /// Returns the exact local settlement category.
    pub const fn kind(self) -> ProducerSendFailureKind {
        self.kind
    }

    /// Confirms local settlement did not cross transport ownership.
    pub const fn delivery_status(self) -> ProducerDeliveryStatus {
        ProducerDeliveryStatus::NotSent
    }
}
