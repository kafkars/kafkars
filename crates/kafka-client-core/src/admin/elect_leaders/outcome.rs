//! Protocol-normalized terminal values for leader election.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker error and its bounded nullable diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElectionBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl LeaderElectionBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn with_bounded_message(
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable diagnostic message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was shortened or omitted.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into adapter-owned scalar values.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result attached to one requested topic-partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderElectionResult {
    /// Kafka accepted the replacement or cancellation.
    Elected,
    /// Kafka rejected this specific change.
    Failed(LeaderElectionBrokerError),
}

/// One per-partition result retained in original caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElectionOutcome {
    topic: String,
    partition: i32,
    result: LeaderElectionResult,
}

impl LeaderElectionOutcome {
    /// Creates one successful change result.
    pub const fn elected(topic: String, partition: i32) -> Self {
        Self {
            topic,
            partition,
            result: LeaderElectionResult::Elected,
        }
    }

    /// Creates one failed change result without reclassifying broker facts.
    pub const fn failed(topic: String, partition: i32, error: LeaderElectionBrokerError) -> Self {
        Self {
            topic,
            partition,
            result: LeaderElectionResult::Failed(error),
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
    pub const fn result(&self) -> &LeaderElectionResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, i32, LeaderElectionResult) {
        (self.topic, self.partition, self.result)
    }
}

/// Ordered successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersBatch {
    throttle_time_ms: u32,
    outcomes: Vec<LeaderElectionOutcome>,
}

impl ElectLeadersBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<LeaderElectionOutcome>) -> Self {
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
    pub fn outcomes(&self) -> &[LeaderElectionOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<LeaderElectionOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-partition results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectLeadersFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// Kafka rejected the whole controller request.
    Broker(LeaderElectionBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The broker-selected version cannot represent this operation.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersFailure {
    kind: ElectLeadersFailureKind,
    delivery: DeliveryStatus,
}

impl ElectLeadersFailure {
    pub(crate) const fn new(kind: ElectLeadersFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> &ElectLeadersFailureKind {
        &self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }

    /// Consumes the failure into its category and delivery certainty.
    pub fn into_parts(self) -> (ElectLeadersFailureKind, DeliveryStatus) {
        (self.kind, self.delivery)
    }
}

/// Exactly one terminal decision for leader election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectLeadersTerminal {
    /// Ordered topic-partition outcomes and broker throttle.
    Elected(ElectLeadersBatch),
    /// Whole-operation failure outside per-partition results.
    Failed(ElectLeadersFailure),
}
