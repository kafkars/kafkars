//! Protocol-normalized terminal values for Admin `DeleteConsumerGroups`.

use crate::DeliveryStatus;

use super::DeleteConsumerGroupsBrokerError;

/// Exact per-group result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupsResult {
    /// Kafka completed deletion for this consumer group.
    Deleted,
    /// Kafka rejected this consumer group with an exact signed code.
    Failed(DeleteConsumerGroupsBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsOutcome {
    group_id: String,
    result: DeleteConsumerGroupsResult,
}

impl DeleteConsumerGroupsOutcome {
    /// Creates one successful consumer-group result.
    pub const fn deleted(group_id: String) -> Self {
        Self {
            group_id,
            result: DeleteConsumerGroupsResult::Deleted,
        }
    }

    /// Creates one failed consumer-group result.
    pub const fn failed(group_id: String, error: DeleteConsumerGroupsBrokerError) -> Self {
        Self {
            group_id,
            result: DeleteConsumerGroupsResult::Failed(error),
        }
    }

    /// Returns the exact consumer-group identifier.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the exact per-group result.
    pub const fn result(&self) -> &DeleteConsumerGroupsResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned scalar values.
    pub fn into_parts(self) -> (String, DeleteConsumerGroupsResult) {
        (self.group_id, self.result)
    }
}

/// Caller-ordered successful operation terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DeleteConsumerGroupsOutcome>,
}

impl DeleteConsumerGroupsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<DeleteConsumerGroupsOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-group outcomes in original caller order.
    pub fn outcomes(&self) -> &[DeleteConsumerGroupsOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DeleteConsumerGroupsOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside per-group broker outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteConsumerGroupsFailureKind {
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

/// Partial operation failure with authoritative failed-group delivery certainty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupsFailure {
    kind: DeleteConsumerGroupsFailureKind,
    delivery: DeliveryStatus,
    throttle_time_ms: u32,
    completed: Vec<DeleteConsumerGroupsOutcome>,
    failed_target: super::DeleteConsumerGroupsTarget,
    unattempted: Vec<super::DeleteConsumerGroupsTarget>,
}

impl DeleteConsumerGroupsFailure {
    pub(crate) const fn new(
        kind: DeleteConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
        throttle_time_ms: u32,
        completed: Vec<DeleteConsumerGroupsOutcome>,
        failed_target: super::DeleteConsumerGroupsTarget,
        unattempted: Vec<super::DeleteConsumerGroupsTarget>,
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
    pub const fn kind(&self) -> DeleteConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty for the failed group.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }

    /// Returns the maximum throttle observed for completed groups.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns caller-ordered outcomes known before the failure.
    pub fn completed(&self) -> &[DeleteConsumerGroupsOutcome] {
        &self.completed
    }

    /// Returns the exact group whose attempt failed.
    pub const fn failed_target(&self) -> &super::DeleteConsumerGroupsTarget {
        &self.failed_target
    }

    /// Returns caller-ordered groups that were never attempted.
    pub fn unattempted(&self) -> &[super::DeleteConsumerGroupsTarget] {
        &self.unattempted
    }

    /// Consumes the partial terminal into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        DeleteConsumerGroupsFailureKind,
        DeliveryStatus,
        u32,
        Vec<DeleteConsumerGroupsOutcome>,
        super::DeleteConsumerGroupsTarget,
        Vec<super::DeleteConsumerGroupsTarget>,
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

/// Exactly one terminal decision for Admin `DeleteConsumerGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteConsumerGroupsTerminal {
    /// Every target settled in original caller order.
    Deleted(DeleteConsumerGroupsBatch),
    /// A whole-operation mechanism or validation failure occurred.
    Failed(DeleteConsumerGroupsFailure),
}
