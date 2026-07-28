//! Caller-ordered replica outcomes and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

const UNASSIGNED_CALLER_INDEX: usize = usize::MAX;

/// Exact broker-declared failure for one requested replica.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirBrokerError {
    code: NonZeroI16,
}

impl AlterReplicaLogDirBrokerError {
    /// Creates one exact signed Kafka replica error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Exact result for one requested broker replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirResult {
    /// Kafka accepted the target log-directory alteration.
    Altered,
    /// Kafka rejected this replica with an exact signed code.
    BrokerFailed(AlterReplicaLogDirBrokerError),
    /// This replica could not settle because its broker call mechanism failed.
    OperationFailed(AlterReplicaLogDirsFailure),
}

/// One replica result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirOutcome {
    broker_id: i32,
    topic: String,
    partition: i32,
    result: AlterReplicaLogDirResult,
    caller_index: usize,
}

impl AlterReplicaLogDirOutcome {
    /// Creates one successful replica alteration outcome.
    pub const fn altered(broker_id: i32, topic: String, partition: i32) -> Self {
        Self {
            broker_id,
            topic,
            partition,
            result: AlterReplicaLogDirResult::Altered,
            caller_index: UNASSIGNED_CALLER_INDEX,
        }
    }

    /// Creates one exact per-replica broker failure.
    pub const fn broker_failed(
        broker_id: i32,
        topic: String,
        partition: i32,
        error: AlterReplicaLogDirBrokerError,
    ) -> Self {
        Self {
            broker_id,
            topic,
            partition,
            result: AlterReplicaLogDirResult::BrokerFailed(error),
            caller_index: UNASSIGNED_CALLER_INDEX,
        }
    }

    /// Creates one replica-scoped operation failure.
    pub const fn operation_failed(
        broker_id: i32,
        topic: String,
        partition: i32,
        failure: AlterReplicaLogDirsFailure,
    ) -> Self {
        Self {
            broker_id,
            topic,
            partition,
            result: AlterReplicaLogDirResult::OperationFailed(failure),
            caller_index: UNASSIGNED_CALLER_INDEX,
        }
    }

    /// Returns the exact target broker identity.
    pub const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the normalized replica result.
    pub const fn result(&self) -> &AlterReplicaLogDirResult {
        &self.result
    }

    /// Consumes the outcome into adapter-owned parts.
    pub fn into_parts(self) -> (i32, String, i32, AlterReplicaLogDirResult) {
        (self.broker_id, self.topic, self.partition, self.result)
    }

    pub(crate) fn assign_caller_index(&mut self, caller_index: usize) {
        self.caller_index = caller_index;
    }

    pub(crate) const fn caller_index(&self) -> usize {
        self.caller_index
    }

    pub(crate) fn clear_caller_index(&mut self) {
        self.caller_index = UNASSIGNED_CALLER_INDEX;
    }

    pub(crate) const fn is_operation_failure(&self) -> bool {
        matches!(&self.result, AlterReplicaLogDirResult::OperationFailed(_))
    }
}

/// Caller-ordered result for every requested replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterReplicaLogDirOutcome>,
}

impl AlterReplicaLogDirsBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<AlterReplicaLogDirOutcome>) -> Self {
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
    pub fn outcomes(&self) -> &[AlterReplicaLogDirOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AlterReplicaLogDirOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Replica-scoped mechanism failure outside exact Kafka result errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsFailureKind {
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
    /// This assignment was not attempted because an earlier broker call failed.
    NotAttempted,
}

/// Replica-scoped mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsFailure {
    kind: AlterReplicaLogDirsFailureKind,
    delivery: DeliveryStatus,
}

impl AlterReplicaLogDirsFailure {
    pub(crate) const fn new(
        kind: AlterReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> AlterReplicaLogDirsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for `AlterReplicaLogDirs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsTerminal {
    /// Every requested replica has a caller-ordered result.
    Altered(AlterReplicaLogDirsBatch),
    /// A whole-operation failure outside replica-scoped execution occurred.
    Failed(AlterReplicaLogDirsFailure),
}
