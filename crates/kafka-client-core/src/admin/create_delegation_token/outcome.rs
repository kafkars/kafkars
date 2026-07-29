//! Exact broker rejection, mechanism failure, and terminal decision.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::CreateDelegationTokenSuccess;

/// Exact top-level API-38 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl CreateDelegationTokenBrokerError {
    /// Creates one exact signed Kafka rejection.
    pub const fn new(throttle_time_ms: u32, code: NonZeroI16) -> Self {
        Self {
            throttle_time_ms,
            code,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes the rejection into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code.get())
    }
}

/// Whole-operation failure outside an exact API-38 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected version cannot represent the exact request.
    Compatibility,
    /// A response was malformed or contradicted explicit request identity.
    InvalidResponse,
}

/// Single-attempt mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenFailure {
    kind: CreateDelegationTokenFailureKind,
    delivery: DeliveryStatus,
}

impl CreateDelegationTokenFailure {
    pub(crate) const fn new(
        kind: CreateDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> CreateDelegationTokenFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `CreateDelegationToken`.
#[derive(Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenTerminal {
    /// Kafka created one complete token.
    Created(CreateDelegationTokenSuccess),
    /// Kafka rejected the request with an exact signed top-level error.
    BrokerRejected(CreateDelegationTokenBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(CreateDelegationTokenFailure),
}
