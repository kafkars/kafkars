//! Protocol-normalized terminal values for consumer-group member removal.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Exact broker-declared failure for one selected static member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerGroupMemberRemovalBrokerError {
    code: NonZeroI16,
}

impl ConsumerGroupMemberRemovalBrokerError {
    /// Creates one exact signed Kafka member error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Exact result attached to one selected static member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerGroupMemberRemovalResult {
    /// Kafka removed the selected member.
    Removed,
    /// Kafka rejected this specific member.
    Failed(ConsumerGroupMemberRemovalBrokerError),
}

/// One per-member result retained in original caller order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberRemovalOutcome {
    group_instance_id: String,
    result: ConsumerGroupMemberRemovalResult,
}

impl ConsumerGroupMemberRemovalOutcome {
    /// Creates one successful static-member result.
    pub const fn removed(group_instance_id: String) -> Self {
        Self {
            group_instance_id,
            result: ConsumerGroupMemberRemovalResult::Removed,
        }
    }

    /// Creates one failed static-member result with its exact broker code.
    pub const fn failed(
        group_instance_id: String,
        error: ConsumerGroupMemberRemovalBrokerError,
    ) -> Self {
        Self {
            group_instance_id,
            result: ConsumerGroupMemberRemovalResult::Failed(error),
        }
    }

    /// Returns the exact static group-instance identity.
    pub fn group_instance_id(&self) -> &str {
        &self.group_instance_id
    }

    /// Returns the per-member result without reclassification.
    pub const fn result(&self) -> &ConsumerGroupMemberRemovalResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, ConsumerGroupMemberRemovalResult) {
        (self.group_instance_id, self.result)
    }
}

/// Ordered successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveConsumerGroupMembersBatch {
    throttle_time_ms: u32,
    outcomes: Vec<ConsumerGroupMemberRemovalOutcome>,
}

impl RemoveConsumerGroupMembersBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<ConsumerGroupMemberRemovalOutcome>,
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

    /// Returns per-member outcomes in original caller order.
    pub fn outcomes(&self) -> &[ConsumerGroupMemberRemovalOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<ConsumerGroupMemberRemovalOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-member results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveConsumerGroupMembersFailureKind {
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
pub struct RemoveConsumerGroupMembersFailure {
    kind: RemoveConsumerGroupMembersFailureKind,
    delivery: DeliveryStatus,
}

impl RemoveConsumerGroupMembersFailure {
    pub(crate) const fn new(
        kind: RemoveConsumerGroupMembersFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(self) -> RemoveConsumerGroupMembersFailureKind {
        self.kind
    }

    /// Returns transport delivery certainty without inventing retry policy.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for consumer-group member removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoveConsumerGroupMembersTerminal {
    /// Ordered member outcomes and broker throttle.
    Removed(RemoveConsumerGroupMembersBatch),
    /// Whole-operation failure outside per-member results.
    Failed(RemoveConsumerGroupMembersFailure),
}
