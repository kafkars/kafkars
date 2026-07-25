//! Bounded scalar broker facts and terminal group offset-commit decisions.

use core::num::NonZeroI16;

use crate::{DeliveryStatus, PartitionIndex, TopicId};

/// Exact signed partition-level broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitBrokerError {
    code: NonZeroI16,
}

impl GroupOffsetCommitBrokerError {
    /// Creates one exact nonzero Kafka error code.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// One partition's normalized commit result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitPartitionResult {
    /// Kafka accepted the next offset.
    Committed,
    /// Kafka rejected the partition commit.
    Rejected(GroupOffsetCommitBrokerError),
}

/// One exactly correlated topic-partition result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitPartitionOutcome {
    topic_id: TopicId,
    partition: PartitionIndex,
    result: GroupOffsetCommitPartitionResult,
}

impl GroupOffsetCommitPartitionOutcome {
    /// Creates one successful partition result.
    pub const fn committed(topic_id: TopicId, partition: PartitionIndex) -> Self {
        Self {
            topic_id,
            partition,
            result: GroupOffsetCommitPartitionResult::Committed,
        }
    }

    /// Creates one broker-rejected partition result.
    pub const fn rejected(
        topic_id: TopicId,
        partition: PartitionIndex,
        error: GroupOffsetCommitBrokerError,
    ) -> Self {
        Self {
            topic_id,
            partition,
            result: GroupOffsetCommitPartitionResult::Rejected(error),
        }
    }

    /// Returns the engine-catalog topic identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Returns the normalized partition result.
    pub const fn result(self) -> GroupOffsetCommitPartitionResult {
        self.result
    }
}

/// One exactly correlated response plus Kafka's throttle observation.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitBatch {
    throttle_time_ms: u32,
    outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
}

impl GroupOffsetCommitBatch {
    /// Creates one protocol-normalized ordered response batch.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation without scheduling it.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns partition outcomes in exact checkpoint order.
    pub fn outcomes(&self) -> &[GroupOffsetCommitPartitionOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned scalar facts.
    pub fn into_parts(self) -> (u32, Vec<GroupOffsetCommitPartitionOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }

    pub(crate) fn into_terminal(self) -> GroupOffsetCommitTerminal {
        let first_rejected_index = self.outcomes.iter().position(|outcome| {
            matches!(
                outcome.result(),
                GroupOffsetCommitPartitionResult::Rejected(_)
            )
        });
        match first_rejected_index {
            Some(index) => GroupOffsetCommitTerminal::BrokerRejected(
                GroupOffsetCommitBrokerRejection::new(self, index),
            ),
            None => GroupOffsetCommitTerminal::Committed(self),
        }
    }
}

/// Broker-rejected operation retaining every ordered partial-success fact.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitBrokerRejection {
    batch: GroupOffsetCommitBatch,
    first_rejected_index: usize,
}

impl GroupOffsetCommitBrokerRejection {
    const fn new(batch: GroupOffsetCommitBatch, first_rejected_index: usize) -> Self {
        Self {
            batch,
            first_rejected_index,
        }
    }

    /// Returns the full exactly correlated batch.
    pub const fn batch(&self) -> &GroupOffsetCommitBatch {
        &self.batch
    }

    /// Returns core's first rejection in checkpoint order.
    pub fn first_rejected(&self) -> GroupOffsetCommitPartitionOutcome {
        self.batch.outcomes[self.first_rejected_index]
    }

    /// Recovers every ordered partition fact for engine translation.
    pub fn into_batch(self) -> GroupOffsetCommitBatch {
        self.batch
    }
}

/// Whole-operation failure outside correlated per-partition broker results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitFailureKind {
    /// The original absolute deadline elapsed before or during driver ownership.
    DeadlineElapsed,
    /// The driver rejected the request before transport ownership.
    DriverRejected,
    /// The engine failed before transport ownership despite reserved capacity.
    ExecutionUnavailable,
    /// Driver-owned transport execution failed.
    Transport,
    /// The broker cannot represent the required commit semantics.
    Compatibility,
    /// A broker response did not correlate exactly to the checkpoint.
    InvalidResponse,
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
}

/// Whole-operation failure with monotonic delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitFailure {
    kind: GroupOffsetCommitFailureKind,
    delivery: DeliveryStatus,
}

impl GroupOffsetCommitFailure {
    pub(crate) const fn new(kind: GroupOffsetCommitFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> GroupOffsetCommitFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one admitted group offset commit.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitTerminal {
    /// Every partition next offset was committed.
    Committed(GroupOffsetCommitBatch),
    /// At least one partition was rejected; partial results remain exact.
    BrokerRejected(GroupOffsetCommitBrokerRejection),
    /// Whole-operation failure outside partition broker results.
    Failed(GroupOffsetCommitFailure),
}
