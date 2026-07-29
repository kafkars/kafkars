//! Exact API-39 rejection, mechanism failure, and terminal decision.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::RenewDelegationTokenSuccess;

/// Exact top-level API-39 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl RenewDelegationTokenBrokerError {
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

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes the rejection into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code.get())
    }
}

/// Whole-operation failure outside an exact API-39 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenewDelegationTokenFailureKind {
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
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Single-attempt mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenewDelegationTokenFailure {
    kind: RenewDelegationTokenFailureKind,
    delivery: DeliveryStatus,
}

impl RenewDelegationTokenFailure {
    pub(crate) const fn new(
        kind: RenewDelegationTokenFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> RenewDelegationTokenFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `RenewDelegationToken`.
#[derive(Debug, Eq, PartialEq)]
pub enum RenewDelegationTokenTerminal {
    /// Kafka renewed the token and returned its new expiry.
    Renewed(RenewDelegationTokenSuccess),
    /// Kafka rejected the request with an exact signed top-level error.
    BrokerRejected(RenewDelegationTokenBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(RenewDelegationTokenFailure),
}
