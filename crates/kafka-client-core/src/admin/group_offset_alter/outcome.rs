//! Protocol-normalized terminal values for consumer-group offset alteration.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one requested topic-partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetBrokerError {
    code: NonZeroI16,
}

impl AlterConsumerGroupOffsetBrokerError {
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
pub enum AlterConsumerGroupOffsetResult {
    /// Kafka accepted the new committed next offset.
    Altered,
    /// Kafka rejected this specific topic-partition.
    Failed(AlterConsumerGroupOffsetBrokerError),
}

/// One per-partition result retained in original caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetOutcome {
    topic: String,
    partition: i32,
    result: AlterConsumerGroupOffsetResult,
}

impl AlterConsumerGroupOffsetOutcome {
    /// Creates one successful topic-partition result.
    pub const fn altered(topic: String, partition: i32) -> Self {
        Self {
            topic,
            partition,
            result: AlterConsumerGroupOffsetResult::Altered,
        }
    }

    /// Creates one failed topic-partition result with its exact broker code.
    pub const fn failed(
        topic: String,
        partition: i32,
        error: AlterConsumerGroupOffsetBrokerError,
    ) -> Self {
        Self {
            topic,
            partition,
            result: AlterConsumerGroupOffsetResult::Failed(error),
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
    pub const fn result(&self) -> &AlterConsumerGroupOffsetResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, AlterConsumerGroupOffsetResult) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterConsumerGroupOffsetOutcome>,
}

impl AlterConsumerGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AlterConsumerGroupOffsetOutcome>,
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
    pub fn outcomes(&self) -> &[AlterConsumerGroupOffsetOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AlterConsumerGroupOffsetOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-partition results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected broker version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsFailure {
    kind: AlterConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
}

impl AlterConsumerGroupOffsetsFailure {
    pub(crate) const fn new(
        kind: AlterConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> AlterConsumerGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for consumer-group offset alteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterConsumerGroupOffsetsTerminal {
    /// Ordered topic-partition outcomes and broker throttle.
    Altered(AlterConsumerGroupOffsetsBatch),
    /// Whole-operation failure outside per-partition results.
    Failed(AlterConsumerGroupOffsetsFailure),
}
