//! Protocol-normalized terminal values for consumer-group offset deletion.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic-partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetBrokerError {
    code: NonZeroI16,
}

impl DeleteConsumerGroupOffsetBrokerError {
    /// Creates one exact signed Kafka partition error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Exact result attached to one requested topic-partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetResult {
    /// Kafka deleted the committed offset.
    Deleted,
    /// Kafka rejected this specific topic-partition.
    Failed(DeleteConsumerGroupOffsetBrokerError),
}

/// One per-partition result retained in original caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetOutcome {
    topic: String,
    partition: i32,
    result: DeleteConsumerGroupOffsetResult,
}

impl DeleteConsumerGroupOffsetOutcome {
    /// Creates one successful topic-partition result.
    pub const fn deleted(topic: String, partition: i32) -> Self {
        Self {
            topic,
            partition,
            result: DeleteConsumerGroupOffsetResult::Deleted,
        }
    }

    /// Creates one failed topic-partition result with its exact broker code.
    pub const fn failed(
        topic: String,
        partition: i32,
        error: DeleteConsumerGroupOffsetBrokerError,
    ) -> Self {
        Self {
            topic,
            partition,
            result: DeleteConsumerGroupOffsetResult::Failed(error),
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

    /// Returns the per-partition result without reclassification.
    pub const fn result(&self) -> &DeleteConsumerGroupOffsetResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, DeleteConsumerGroupOffsetResult) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DeleteConsumerGroupOffsetOutcome>,
}

impl DeleteConsumerGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<DeleteConsumerGroupOffsetOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-partition outcomes in original caller order.
    pub fn outcomes(&self) -> &[DeleteConsumerGroupOffsetOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DeleteConsumerGroupOffsetOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-partition results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// Kafka rejected the named group with this exact signed code.
    Broker(NonZeroI16),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected broker version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsFailure {
    kind: DeleteConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl DeleteConsumerGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: DeleteConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> DeleteConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for consumer-group offset deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupOffsetsTerminal {
    /// Ordered topic-partition outcomes and broker throttle.
    Deleted(DeleteConsumerGroupOffsetsBatch),
    /// Whole-operation failure outside per-partition results.
    Failed(DeleteConsumerGroupOffsetsFailure),
}
