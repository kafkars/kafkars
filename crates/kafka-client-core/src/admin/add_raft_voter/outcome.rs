//! Success, exact broker rejection, and mechanism terminals for `AddRaftVoter`.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const ADD_RAFT_VOTER_DIAGNOSTIC_BYTES: usize = 1024;

/// Successful committed voter addition and Kafka's throttle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddRaftVoterSuccess {
    throttle_time_ms: u32,
}

impl AddRaftVoterSuccess {
    /// Creates one successful protocol-normalized result.
    pub const fn new(throttle_time_ms: u32) -> Self {
        Self { throttle_time_ms }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }
}

/// Exact top-level API-80 broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddRaftVoterBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AddRaftVoterBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(
        throttle_time_ms: u32,
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            throttle_time_ms,
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (u32, i16, Option<String>, bool) {
        (
            self.throttle_time_ms,
            self.code.get(),
            self.message,
            self.message_truncated,
        )
    }
}

/// Whole-operation failure outside an exact API-80 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent committed voter addition.
    Compatibility,
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AddRaftVoterFailure {
    kind: AddRaftVoterFailureKind,
    delivery: DeliveryStatus,
}

impl AddRaftVoterFailure {
    pub(crate) const fn new(kind: AddRaftVoterFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AddRaftVoterFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `AddRaftVoter`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddRaftVoterTerminal {
    /// Kafka committed the voter addition.
    Added(AddRaftVoterSuccess),
    /// Kafka rejected the request with an exact top-level error.
    BrokerRejected(AddRaftVoterBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(AddRaftVoterFailure),
}
