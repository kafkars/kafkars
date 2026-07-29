//! Singular response batches and caller-ordered multi-group aggregation.

use core::num::NonZeroI16;

use super::GroupOffsetOutcome;

/// Ordered successful response facts plus Kafka's throttle observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<GroupOffsetOutcome>,
}

impl ListConsumerGroupOffsetsBatch {
    /// Creates one protocol-normalized response batch.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<GroupOffsetOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns ordered topic-partition outcomes.
    pub fn outcomes(&self) -> &[GroupOffsetOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<GroupOffsetOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exact result for one consumer group in a caller-ordered batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListConsumerGroupBatchOutcome {
    /// Kafka returned the group's ordered committed-offset results.
    Offsets {
        /// Exact requested consumer-group identity.
        group_id: String,
        /// Ordered partition outcomes and this response's throttle.
        offsets: ListConsumerGroupOffsetsBatch,
    },
    /// Kafka rejected this specific consumer group.
    BrokerRejected {
        /// Exact requested consumer-group identity.
        group_id: String,
        /// Kafka's exact nonzero signed group error code.
        code: NonZeroI16,
        /// This response's nonnegative throttle observation.
        throttle_time_ms: u32,
    },
}

impl ListConsumerGroupBatchOutcome {
    /// Creates one successful per-group offset outcome.
    pub const fn offsets(group_id: String, offsets: ListConsumerGroupOffsetsBatch) -> Self {
        Self::Offsets { group_id, offsets }
    }

    /// Creates one exact per-group broker rejection.
    pub const fn broker_rejected(
        group_id: String,
        code: NonZeroI16,
        throttle_time_ms: u32,
    ) -> Self {
        Self::BrokerRejected {
            group_id,
            code,
            throttle_time_ms,
        }
    }

    /// Returns the exact requested consumer-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Offsets { group_id, .. } | Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Returns this group's nonnegative broker throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        match self {
            Self::Offsets { offsets, .. } => offsets.throttle_time_ms(),
            Self::BrokerRejected {
                throttle_time_ms, ..
            } => *throttle_time_ms,
        }
    }

    /// Consumes the outcome into its group and exact broker result.
    pub fn into_parts(self) -> (String, Result<ListConsumerGroupOffsetsBatch, NonZeroI16>) {
        match self {
            Self::Offsets { group_id, offsets } => (group_id, Ok(offsets)),
            Self::BrokerRejected { group_id, code, .. } => (group_id, Err(code)),
        }
    }
}

/// Caller-ordered outcomes for one multi-consumer-group offset operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupsOffsetsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<ListConsumerGroupBatchOutcome>,
}

impl ListConsumerGroupsOffsetsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<ListConsumerGroupBatchOutcome>) -> Self {
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
    pub fn outcomes(&self) -> &[ListConsumerGroupBatchOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<ListConsumerGroupBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}
