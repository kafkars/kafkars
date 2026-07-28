//! Caller-ordered broker outcomes and terminal facts for `DescribeLogDirs`.

use crate::DeliveryStatus;

use super::{AdminDescribeLogDirsBrokerError, AdminLogDirOutcome};

/// Exact result for one requested broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsBrokerResult {
    /// Kafka returned this broker's log-directory results.
    Described(Vec<AdminLogDirOutcome>),
    /// Kafka rejected the broker-scoped request with an exact signed code.
    BrokerFailed(AdminDescribeLogDirsBrokerError),
    /// The broker could not complete because the operation mechanism failed.
    OperationFailed(AdminDescribeLogDirsFailure),
}

/// One broker result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsBrokerOutcome {
    broker_id: i32,
    result: AdminDescribeLogDirsBrokerResult,
}

impl AdminDescribeLogDirsBrokerOutcome {
    /// Creates one successful broker description.
    pub const fn described(broker_id: i32, log_dirs: Vec<AdminLogDirOutcome>) -> Self {
        Self {
            broker_id,
            result: AdminDescribeLogDirsBrokerResult::Described(log_dirs),
        }
    }

    /// Creates one exact broker-level rejection.
    pub const fn broker_failed(broker_id: i32, error: AdminDescribeLogDirsBrokerError) -> Self {
        Self {
            broker_id,
            result: AdminDescribeLogDirsBrokerResult::BrokerFailed(error),
        }
    }

    /// Creates one broker-scoped operation failure.
    pub const fn operation_failed(broker_id: i32, failure: AdminDescribeLogDirsFailure) -> Self {
        Self {
            broker_id,
            result: AdminDescribeLogDirsBrokerResult::OperationFailed(failure),
        }
    }

    /// Returns the exact requested broker identity.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns this broker's normalized result.
    pub const fn result(&self) -> &AdminDescribeLogDirsBrokerResult {
        &self.result
    }

    /// Consumes the outcome into adapter-owned parts.
    pub fn into_parts(self) -> (i32, AdminDescribeLogDirsBrokerResult) {
        (self.broker_id, self.result)
    }
}

/// Caller-ordered result for every selected broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminDescribeLogDirsBrokerOutcome>,
}

impl AdminDescribeLogDirsBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AdminDescribeLogDirsBrokerOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across broker calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns broker outcomes in exact caller order.
    pub fn outcomes(&self) -> &[AdminDescribeLogDirsBrokerOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AdminDescribeLogDirsBrokerOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Broker-scoped mechanism failure outside exact Kafka result errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a prepared exact-broker call.
    DriverRejected,
    /// Driver-owned transport failed.
    Transport,
    /// A response could not fit the admitted retained envelope.
    ResponseTooLarge,
    /// Negotiated protocol semantics were insufficient.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
    /// This broker was not attempted because an earlier broker failed.
    NotAttempted,
}

/// Broker-scoped mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeLogDirsFailure {
    kind: AdminDescribeLogDirsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminDescribeLogDirsFailure {
    pub(crate) const fn new(
        kind: AdminDescribeLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeLogDirsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for `DescribeLogDirs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeLogDirsTerminal {
    /// Every requested broker has a caller-ordered result.
    Described(AdminDescribeLogDirsBatch),
    /// A whole-operation failure outside broker-scoped execution occurred.
    Failed(AdminDescribeLogDirsFailure),
}
