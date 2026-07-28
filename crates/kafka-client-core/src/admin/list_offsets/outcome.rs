//! Protocol-normalized terminal values for Admin `ListOffsets`.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic-partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminListOffsetBrokerError {
    code: NonZeroI16,
}

impl AdminListOffsetBrokerError {
    /// Creates one exact signed Kafka partition error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Successful offset facts returned by Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminListOffset {
    offset: Option<i64>,
    timestamp_ms: Option<i64>,
    leader_epoch: Option<i32>,
}

impl AdminListOffset {
    /// Creates one successful result after protocol sentinel normalization.
    pub const fn new(
        offset: Option<i64>,
        timestamp_ms: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            offset,
            timestamp_ms,
            leader_epoch,
        }
    }

    /// Returns Kafka's selected nonnegative offset, if one exists.
    pub const fn offset(self) -> Option<i64> {
        self.offset
    }

    /// Returns Kafka's associated nonnegative timestamp, if represented.
    pub const fn timestamp_ms(self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Returns Kafka's selected nonnegative leader epoch, if represented.
    pub const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Exact per-partition result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetResult {
    /// Kafka successfully evaluated the requested offset specification.
    Listed(AdminListOffset),
    /// Kafka rejected this specific topic-partition with an exact signed code.
    Failed(AdminListOffsetBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminListOffsetOutcome {
    topic: String,
    partition: i32,
    result: AdminListOffsetResult,
}

impl AdminListOffsetOutcome {
    /// Creates one successful topic-partition result.
    pub const fn listed(topic: String, partition: i32, value: AdminListOffset) -> Self {
        Self {
            topic,
            partition,
            result: AdminListOffsetResult::Listed(value),
        }
    }

    /// Creates one failed topic-partition result.
    pub const fn failed(topic: String, partition: i32, error: AdminListOffsetBrokerError) -> Self {
        Self {
            topic,
            partition,
            result: AdminListOffsetResult::Failed(error),
        }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the exact partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact per-partition result.
    pub const fn result(&self) -> &AdminListOffsetResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, AdminListOffsetResult) {
        (self.topic, self.partition, self.result)
    }
}

/// Caller-ordered successful operation terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminListOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminListOffsetOutcome>,
}

impl AdminListOffsetsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<AdminListOffsetOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across leader calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-partition outcomes in original caller order.
    pub fn outcomes(&self) -> &[AdminListOffsetOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AdminListOffsetOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-partition broker outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminListOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a prepared call.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected broker version cannot represent the request.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminListOffsetsFailure {
    kind: AdminListOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminListOffsetsFailure {
    pub(crate) const fn new(kind: AdminListOffsetsFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> AdminListOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `ListOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminListOffsetsTerminal {
    /// Every target settled in original caller order.
    Listed(AdminListOffsetsBatch),
    /// A whole-operation mechanism or validation failure occurred.
    Failed(AdminListOffsetsFailure),
}
