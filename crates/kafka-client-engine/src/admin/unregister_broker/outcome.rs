//! Stable generated-free terminals for Admin `UnregisterBroker`.

use core::fmt;

use super::UnregisterBrokerResult;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerDeliveryStatus {
    /// The failed destructive call did not reach Kafka.
    NotSent,
    /// The failed destructive call may have reached Kafka.
    PossiblySent,
}

/// Exact signed top-level Kafka rejection and bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl UnregisterBrokerBrokerError {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact scalar parts.
    pub fn into_parts(self) -> (u32, i16, Option<String>, bool) {
        (
            self.throttle_time_ms,
            self.code,
            self.message,
            self.message_truncated,
        )
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent broker unregistration.
    Compatibility,
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnregisterBrokerFailure {
    pub(super) kind: UnregisterBrokerFailureKind,
    pub(super) delivery: UnregisterBrokerDeliveryStatus,
}

impl UnregisterBrokerFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> UnregisterBrokerFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> UnregisterBrokerDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerOutcome {
    /// Kafka accepted the destructive broker unregistration.
    Unregistered(UnregisterBrokerResult),
    /// Kafka rejected the request with an exact top-level error.
    BrokerRejected(UnregisterBrokerBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(UnregisterBrokerFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for UnregisterBrokerObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin UnregisterBroker result was already observed",
            Self::Stale => "Admin UnregisterBroker observer is stale",
        })
    }
}

impl std::error::Error for UnregisterBrokerObserverError {}
