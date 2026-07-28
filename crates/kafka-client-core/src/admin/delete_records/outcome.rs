//! Protocol-normalized terminal values for Admin `DeleteRecords`.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic-partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsBrokerError {
    code: NonZeroI16,
}

impl DeleteRecordsBrokerError {
    /// Creates one exact signed Kafka partition error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Successful deletion facts returned by Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletedRecords {
    low_watermark: i64,
}

impl DeletedRecords {
    /// Creates a successful result after scalar validation.
    pub const fn new(low_watermark: i64) -> Self {
        Self { low_watermark }
    }

    /// Returns the partition's first offset that may still be available.
    pub const fn low_watermark(self) -> i64 {
        self.low_watermark
    }
}

/// Exact per-partition result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteRecordsResult {
    /// Kafka completed deletion for this partition.
    Deleted(DeletedRecords),
    /// Kafka rejected this specific topic-partition with an exact signed code.
    Failed(DeleteRecordsBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsOutcome {
    topic: String,
    partition: i32,
    result: DeleteRecordsResult,
}

impl DeleteRecordsOutcome {
    /// Creates one successful topic-partition result.
    pub const fn deleted(topic: String, partition: i32, value: DeletedRecords) -> Self {
        Self {
            topic,
            partition,
            result: DeleteRecordsResult::Deleted(value),
        }
    }

    /// Creates one failed topic-partition result.
    pub const fn failed(topic: String, partition: i32, error: DeleteRecordsBrokerError) -> Self {
        Self {
            topic,
            partition,
            result: DeleteRecordsResult::Failed(error),
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
    pub const fn result(&self) -> &DeleteRecordsResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, DeleteRecordsResult) {
        (self.topic, self.partition, self.result)
    }
}

/// Caller-ordered successful operation terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DeleteRecordsOutcome>,
}

impl DeleteRecordsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<DeleteRecordsOutcome>) -> Self {
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
    pub fn outcomes(&self) -> &[DeleteRecordsOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DeleteRecordsOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-partition broker outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteRecordsFailureKind {
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

/// Partial operation failure with authoritative failed-target delivery certainty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsFailure {
    kind: DeleteRecordsFailureKind,
    delivery: DeliveryStatus,
    throttle_time_ms: u32,
    completed: Vec<DeleteRecordsOutcome>,
    failed_target: super::DeleteRecordsTarget,
    unattempted: Vec<super::DeleteRecordsTarget>,
}

impl DeleteRecordsFailure {
    pub(crate) const fn new(
        kind: DeleteRecordsFailureKind,
        delivery: DeliveryStatus,
        throttle_time_ms: u32,
        completed: Vec<DeleteRecordsOutcome>,
        failed_target: super::DeleteRecordsTarget,
        unattempted: Vec<super::DeleteRecordsTarget>,
    ) -> Self {
        Self {
            kind,
            delivery,
            throttle_time_ms,
            completed,
            failed_target,
            unattempted,
        }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> DeleteRecordsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty for the failed target.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }

    /// Returns the maximum throttle observed for completed targets.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns caller-ordered outcomes known before the failure.
    pub fn completed(&self) -> &[DeleteRecordsOutcome] {
        &self.completed
    }

    /// Returns the exact target whose attempt failed.
    pub const fn failed_target(&self) -> &super::DeleteRecordsTarget {
        &self.failed_target
    }

    /// Returns caller-ordered targets that were never attempted.
    pub fn unattempted(&self) -> &[super::DeleteRecordsTarget] {
        &self.unattempted
    }

    /// Consumes the partial terminal into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        DeleteRecordsFailureKind,
        DeliveryStatus,
        u32,
        Vec<DeleteRecordsOutcome>,
        super::DeleteRecordsTarget,
        Vec<super::DeleteRecordsTarget>,
    ) {
        (
            self.kind,
            self.delivery,
            self.throttle_time_ms,
            self.completed,
            self.failed_target,
            self.unattempted,
        )
    }
}

/// Exactly one terminal decision for Admin `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteRecordsTerminal {
    /// Every target settled in original caller order.
    Deleted(DeleteRecordsBatch),
    /// A whole-operation mechanism or validation failure occurred.
    Failed(DeleteRecordsFailure),
}
