//! Exact API-41 broker rejection, mechanism failure, and terminal.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeDelegationTokensListing;

/// Exact signed top-level API-41 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl DescribeDelegationTokensBrokerError {
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

/// Whole-operation failure outside an exact API-41 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent exact request semantics.
    Compatibility,
    /// A response was malformed, contradictory, or uncorrelatable.
    InvalidResponse,
}

/// Single-attempt mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeDelegationTokensFailure {
    kind: DescribeDelegationTokensFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeDelegationTokensFailure {
    pub(crate) const fn new(
        kind: DescribeDelegationTokensFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> DescribeDelegationTokensFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeDelegationToken`.
#[derive(Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensTerminal {
    /// Kafka returned a deterministic complete token listing.
    Described(DescribeDelegationTokensListing),
    /// Kafka rejected the request with an exact signed top-level error.
    BrokerRejected(DescribeDelegationTokensBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeDelegationTokensFailure),
}
