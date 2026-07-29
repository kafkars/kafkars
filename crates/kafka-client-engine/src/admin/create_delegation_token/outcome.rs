//! Stable generated-free terminals for Admin `CreateDelegationToken`.

use core::fmt;

use super::CreateDelegationTokenResult;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenDeliveryStatus {
    /// The mutating call did not reach Kafka transport ownership.
    NotSent,
    /// Kafka may have received the mutating call.
    PossiblySent,
}

/// Exact signed top-level Kafka rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
}

impl CreateDelegationTokenBrokerError {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code
    }

    /// Consumes this rejection into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code)
    }
}

/// Stable whole-operation failure category.
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
    /// The selected API version cannot represent the exact request.
    Compatibility,
    /// A response was malformed or contradicted explicit owner identity.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateDelegationTokenFailure {
    pub(super) kind: CreateDelegationTokenFailureKind,
    pub(super) delivery: CreateDelegationTokenDeliveryStatus,
}

impl CreateDelegationTokenFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> CreateDelegationTokenFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> CreateDelegationTokenDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenOutcome {
    /// Kafka created one complete delegation token.
    Created(CreateDelegationTokenResult),
    /// Kafka rejected the request with an exact top-level error.
    BrokerRejected(CreateDelegationTokenBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(CreateDelegationTokenFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for CreateDelegationTokenObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin CreateDelegationToken result was already observed",
            Self::Stale => "Admin CreateDelegationToken observer is stale",
        })
    }
}

impl std::error::Error for CreateDelegationTokenObserverError {}
