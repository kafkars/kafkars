//! Protocol-normalized terminal values for partition-reassignment listing.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES: usize = 1024;

/// One active reassignment's ordered broker sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl PartitionReassignment {
    /// Creates one protocol-normalized active reassignment.
    pub const fn new(
        replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        removing_replicas: Vec<i32>,
    ) -> Self {
        Self {
            replicas,
            adding_replicas,
            removing_replicas,
        }
    }

    /// Returns Kafka's ordered current replica list.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns Kafka's ordered adding-replica list.
    pub fn adding_replicas(&self) -> &[i32] {
        &self.adding_replicas
    }

    /// Returns Kafka's ordered removing-replica list.
    pub fn removing_replicas(&self) -> &[i32] {
        &self.removing_replicas
    }

    /// Consumes the reassignment into adapter-owned scalar lists.
    pub fn into_parts(self) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
        (self.replicas, self.adding_replicas, self.removing_replicas)
    }
}

/// One active reassignment attached to its topic-partition identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentOutcome {
    topic: String,
    partition: i32,
    reassignment: PartitionReassignment,
}

impl PartitionReassignmentOutcome {
    /// Creates one active topic-partition reassignment.
    pub const fn new(topic: String, partition: i32, reassignment: PartitionReassignment) -> Self {
        Self {
            topic,
            partition,
            reassignment,
        }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the ordered replica-set description.
    pub const fn reassignment(&self) -> &PartitionReassignment {
        &self.reassignment
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, i32, PartitionReassignment) {
        (self.topic, self.partition, self.reassignment)
    }
}

/// Successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsBatch {
    throttle_time_ms: u32,
    reassignments: Vec<PartitionReassignmentOutcome>,
}

impl ListPartitionReassignmentsBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(
        throttle_time_ms: u32,
        reassignments: Vec<PartitionReassignmentOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            reassignments,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns active reassignments in deterministic selection order.
    pub fn reassignments(&self) -> &[PartitionReassignmentOutcome] {
        &self.reassignments
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<PartitionReassignmentOutcome>) {
        (self.throttle_time_ms, self.reassignments)
    }
}

/// Exact controller-declared failure with a bounded nullable diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl ListPartitionReassignmentsBrokerError {
    /// Creates one exact signed Kafka error and bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
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

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the broker diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }
}

/// Whole-operation failure category outside active reassignment facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartitionReassignmentsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// Transport failed after driver ownership.
    Transport,
    /// Kafka's controller rejected the query.
    Broker(ListPartitionReassignmentsBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected broker version cannot represent required semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsFailure {
    kind: ListPartitionReassignmentsFailureKind,
    delivery: DeliveryStatus,
}

impl ListPartitionReassignmentsFailure {
    pub(crate) const fn new(
        kind: ListPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> &ListPartitionReassignmentsFailureKind {
        &self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for a reassignment query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartitionReassignmentsTerminal {
    /// Active reassignment facts and broker throttle.
    Reassignments(ListPartitionReassignmentsBatch),
    /// Whole-operation failure outside active reassignment facts.
    Failed(ListPartitionReassignmentsFailure),
}
