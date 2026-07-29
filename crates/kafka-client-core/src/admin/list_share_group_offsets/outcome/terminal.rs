//! Exact singular and caller-ordered batch terminals for API-90 operations.

use super::{
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsFailure,
};

/// Exact result for one share group in a caller-ordered batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsBatchOutcome {
    /// Kafka returned correlated partition outcomes for this group.
    Offsets {
        /// Exact requested share-group identity.
        group_id: String,
        /// Canonically or caller-ordered partition outcomes.
        offsets: ListShareGroupOffsetsBatch,
    },
    /// Kafka rejected this specific share group.
    BrokerRejected {
        /// Exact requested share-group identity.
        group_id: String,
        /// Exact signed group rejection.
        error: ListShareGroupOffsetsBrokerError,
    },
}

impl ListShareGroupOffsetsBatchOutcome {
    /// Creates one successful per-group batch outcome.
    pub const fn offsets(group_id: String, offsets: ListShareGroupOffsetsBatch) -> Self {
        Self::Offsets { group_id, offsets }
    }

    /// Creates one rejected per-group batch outcome.
    pub const fn broker_rejected(
        group_id: String,
        error: ListShareGroupOffsetsBrokerError,
    ) -> Self {
        Self::BrokerRejected { group_id, error }
    }

    /// Returns the exact requested share-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Offsets { group_id, .. } | Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Returns this group's nonnegative Kafka throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        match self {
            Self::Offsets { offsets, .. } => offsets.throttle_time_ms(),
            Self::BrokerRejected { error, .. } => error.throttle_time_ms(),
        }
    }
}

/// Caller-ordered outcomes for one accepted multi-group operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupsOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<ListShareGroupOffsetsBatchOutcome>,
}

impl ListShareGroupsOffsetsBatch {
    /// Creates one batch with the maximum observed broker throttle.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<ListShareGroupOffsetsBatchOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[ListShareGroupOffsetsBatchOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<ListShareGroupOffsetsBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision for one read-only API-90 operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsTerminal {
    /// Kafka returned correlated deterministic partition outcomes.
    Offsets(ListShareGroupOffsetsBatch),
    /// Kafka rejected the named share group with an exact top-level error.
    BrokerRejected(ListShareGroupOffsetsBrokerError),
    /// Every requested share group settled in original caller order.
    Batch(ListShareGroupsOffsetsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListShareGroupOffsetsFailure),
}
