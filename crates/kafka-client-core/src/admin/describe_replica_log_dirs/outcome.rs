//! Caller-ordered replica locations and exact failure facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeReplicaLogDirsReplica;

/// One current or future replica placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirLocation {
    path: String,
    offset_lag: i64,
}

impl ReplicaLogDirLocation {
    /// Creates one protocol-normalized placement.
    pub const fn new(path: String, offset_lag: i64) -> Self {
        Self { path, offset_lag }
    }

    /// Returns the broker log-directory path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns Kafka's exact signed offset lag.
    pub const fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Consumes the placement into adapter-owned scalar parts.
    pub fn into_parts(self) -> (String, i64) {
        (self.path, self.offset_lag)
    }
}

/// Optional current and future placement for one requested replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaLogDirInfo {
    current: Option<ReplicaLogDirLocation>,
    future: Option<ReplicaLogDirLocation>,
}

impl ReplicaLogDirInfo {
    /// Creates one complete placement description.
    pub const fn new(
        current: Option<ReplicaLogDirLocation>,
        future: Option<ReplicaLogDirLocation>,
    ) -> Self {
        Self { current, future }
    }

    /// Returns the current placement when Kafka reported one.
    pub const fn current(&self) -> Option<&ReplicaLogDirLocation> {
        self.current.as_ref()
    }

    /// Returns the future replacement placement when Kafka reported one.
    pub const fn future(&self) -> Option<&ReplicaLogDirLocation> {
        self.future.as_ref()
    }

    /// Consumes the information into adapter-owned placement parts.
    pub fn into_parts(self) -> (Option<ReplicaLogDirLocation>, Option<ReplicaLogDirLocation>) {
        (self.current, self.future)
    }
}

/// One broker-normalized requested replica and its optional placements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsReplicaPlacement {
    replica: DescribeReplicaLogDirsReplica,
    info: ReplicaLogDirInfo,
}

impl DescribeReplicaLogDirsReplicaPlacement {
    /// Creates one correlated placement.
    pub const fn new(replica: DescribeReplicaLogDirsReplica, info: ReplicaLogDirInfo) -> Self {
        Self { replica, info }
    }

    /// Returns the correlated requested replica.
    pub const fn replica(&self) -> &DescribeReplicaLogDirsReplica {
        &self.replica
    }

    /// Returns its optional current and future placements.
    pub const fn info(&self) -> &ReplicaLogDirInfo {
        &self.info
    }

    /// Consumes the placement into identity and information.
    pub fn into_parts(self) -> (DescribeReplicaLogDirsReplica, ReplicaLogDirInfo) {
        (self.replica, self.info)
    }
}

/// Exact top-level API-35 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsBrokerError {
    code: NonZeroI16,
}

impl DescribeReplicaLogDirsBrokerError {
    /// Creates one exact signed Kafka error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Stable broker-scoped mechanism failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a prepared exact-broker call.
    DriverRejected,
    /// Driver-owned transport failed.
    Transport,
    /// A response could not fit admitted retained capacity.
    ResponseTooLarge,
    /// Negotiated protocol semantics were insufficient.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
    /// This broker was skipped because an earlier mechanism failed.
    NotAttempted,
}

/// Broker-scoped failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsFailure {
    kind: DescribeReplicaLogDirsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeReplicaLogDirsFailure {
    pub(crate) const fn new(
        kind: DescribeReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeReplicaLogDirsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exact result for one caller-selected replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsReplicaResult {
    /// Kafka described zero, one, or two placements.
    Described(ReplicaLogDirInfo),
    /// Kafka rejected the broker-scoped request.
    BrokerFailed(DescribeReplicaLogDirsBrokerError),
    /// This replica could not complete because its mechanism failed.
    OperationFailed(DescribeReplicaLogDirsFailure),
}

/// One result retained with exact caller identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsReplicaOutcome {
    replica: DescribeReplicaLogDirsReplica,
    result: DescribeReplicaLogDirsReplicaResult,
}

impl DescribeReplicaLogDirsReplicaOutcome {
    /// Creates one successful placement description.
    pub const fn described(
        replica: DescribeReplicaLogDirsReplica,
        info: ReplicaLogDirInfo,
    ) -> Self {
        Self {
            replica,
            result: DescribeReplicaLogDirsReplicaResult::Described(info),
        }
    }

    /// Creates one exact broker-level rejection.
    pub const fn broker_failed(
        replica: DescribeReplicaLogDirsReplica,
        error: DescribeReplicaLogDirsBrokerError,
    ) -> Self {
        Self {
            replica,
            result: DescribeReplicaLogDirsReplicaResult::BrokerFailed(error),
        }
    }

    /// Creates one broker-scoped operation failure.
    pub const fn operation_failed(
        replica: DescribeReplicaLogDirsReplica,
        failure: DescribeReplicaLogDirsFailure,
    ) -> Self {
        Self {
            replica,
            result: DescribeReplicaLogDirsReplicaResult::OperationFailed(failure),
        }
    }

    /// Returns the exact requested replica.
    pub const fn replica(&self) -> &DescribeReplicaLogDirsReplica {
        &self.replica
    }

    /// Returns its exact normalized result.
    pub const fn result(&self) -> &DescribeReplicaLogDirsReplicaResult {
        &self.result
    }

    /// Consumes the outcome into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        DescribeReplicaLogDirsReplica,
        DescribeReplicaLogDirsReplicaResult,
    ) {
        (self.replica, self.result)
    }
}

/// Caller-ordered result for every selected replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeReplicaLogDirsReplicaOutcome>,
}

impl DescribeReplicaLogDirsBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<DescribeReplicaLogDirsReplicaOutcome>,
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

    /// Returns replica outcomes in exact caller order.
    pub fn outcomes(&self) -> &[DescribeReplicaLogDirsReplicaOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DescribeReplicaLogDirsReplicaOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsTerminal {
    /// Every requested replica has one caller-ordered result.
    Described(DescribeReplicaLogDirsBatch),
}
