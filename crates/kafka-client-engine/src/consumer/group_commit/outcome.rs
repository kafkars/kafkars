//! Stable generated-type-free terminal values for group checkpoint commits.

use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, GroupOffsetCommitBatch as CoreBatch,
    GroupOffsetCommitFailureKind as CoreFailureKind,
    GroupOffsetCommitPartitionResult as CorePartitionResult,
    GroupOffsetCommitTerminal as CoreTerminal,
};

use super::GroupConsumerCommitObserverError;
use crate::consumer::group_batch::GroupConsumerCheckpointObservation;

/// Stable delivery certainty for one whole-operation commit failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCommitDeliveryStatus {
    /// The request definitely did not enter transport ownership.
    NotSent,
    /// Kafka may have observed the request.
    PossiblySent,
}

/// Exact signed Kafka rejection for one checkpoint partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerCommitBrokerError {
    code: i16,
}

impl GroupConsumerCommitBrokerError {
    /// Returns Kafka's exact signed protocol error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// Stable result for one exactly correlated checkpoint partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCommitPartitionResult {
    /// Kafka committed the next offset.
    Committed,
    /// Kafka rejected this topic-partition with an exact signed code.
    Rejected(GroupConsumerCommitBrokerError),
}

/// One public topic-partition result in checkpoint order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupConsumerCommitPartitionOutcome {
    topic: Arc<str>,
    partition: i32,
    result: GroupConsumerCommitPartitionResult,
}

impl GroupConsumerCommitPartitionOutcome {
    /// Returns the catalog-retained Kafka topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact normalized partition result.
    pub const fn result(&self) -> GroupConsumerCommitPartitionResult {
        self.result
    }
}

/// Exactly correlated partition results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupConsumerCommitBatch {
    throttle_time_ms: u32,
    outcomes: Vec<GroupConsumerCommitPartitionOutcome>,
}

impl GroupConsumerCommitBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns partition outcomes in exact checkpoint order.
    pub fn outcomes(&self) -> &[GroupConsumerCommitPartitionOutcome] {
        &self.outcomes
    }
}

/// Stable whole-operation commit failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCommitFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Bounded driver admission rejected before transport ownership.
    DriverRejected,
    /// Reserved execution could not prepare after deterministic admission.
    ExecutionUnavailable,
    /// Driver-owned transport execution failed.
    Transport,
    /// The selected `OffsetCommit` version cannot represent the checkpoint.
    Compatibility,
    /// The broker response did not correlate exactly to the checkpoint.
    InvalidResponse,
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerCommitFailure {
    kind: GroupConsumerCommitFailureKind,
    delivery: GroupConsumerCommitDeliveryStatus,
}

impl GroupConsumerCommitFailure {
    /// Returns the stable terminal failure category.
    pub const fn kind(self) -> GroupConsumerCommitFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> GroupConsumerCommitDeliveryStatus {
        self.delivery
    }
}

/// Exactly one stable engine-owned commit terminal.
#[derive(Debug)]
pub enum GroupConsumerCommitOutcome {
    /// Every checkpoint partition was committed.
    Committed(GroupConsumerCommitBatch),
    /// At least one partition was rejected; all partial results and the retry checkpoint remain exact.
    BrokerRejected(
        GroupConsumerCommitBatch,
        crate::consumer::GroupConsumerCheckpoint,
    ),
    /// The entire operation failed outside correlated broker results and returns the retry checkpoint.
    Failed(
        GroupConsumerCommitFailure,
        crate::consumer::GroupConsumerCheckpoint,
    ),
}

pub(super) fn translate_terminal(
    terminal: CoreTerminal,
    observation: GroupConsumerCheckpointObservation,
) -> Result<GroupConsumerCommitOutcome, GroupConsumerCommitObserverError> {
    match terminal {
        CoreTerminal::Committed(batch) => {
            translate_batch(batch, &observation).map(GroupConsumerCommitOutcome::Committed)
        }
        CoreTerminal::BrokerRejected(rejection) => {
            let batch = translate_batch(rejection.into_batch(), &observation)?;
            Ok(GroupConsumerCommitOutcome::BrokerRejected(
                batch,
                observation.into_checkpoint(),
            ))
        }
        CoreTerminal::Failed(failure) => Ok(GroupConsumerCommitOutcome::Failed(
            GroupConsumerCommitFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            },
            observation.into_checkpoint(),
        )),
    }
}

fn translate_batch(
    batch: CoreBatch,
    observation: &GroupConsumerCheckpointObservation,
) -> Result<GroupConsumerCommitBatch, GroupConsumerCommitObserverError> {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    let [outcome] = outcomes.as_slice() else {
        return Err(GroupConsumerCommitObserverError::InternalInvariant);
    };
    if outcome.topic_id() != observation.topic_id || outcome.partition() != observation.partition_id
    {
        return Err(GroupConsumerCommitObserverError::InternalInvariant);
    }
    let result = match outcome.result() {
        CorePartitionResult::Committed => GroupConsumerCommitPartitionResult::Committed,
        CorePartitionResult::Rejected(error) => {
            GroupConsumerCommitPartitionResult::Rejected(GroupConsumerCommitBrokerError {
                code: error.code(),
            })
        }
    };
    Ok(GroupConsumerCommitBatch {
        throttle_time_ms,
        outcomes: vec![GroupConsumerCommitPartitionOutcome {
            topic: Arc::clone(observation.topic()),
            partition: observation.partition(),
            result,
        }],
    })
}

const fn failure_kind(kind: CoreFailureKind) -> GroupConsumerCommitFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => GroupConsumerCommitFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => GroupConsumerCommitFailureKind::DriverRejected,
        CoreFailureKind::ExecutionUnavailable => {
            GroupConsumerCommitFailureKind::ExecutionUnavailable
        }
        CoreFailureKind::Transport => GroupConsumerCommitFailureKind::Transport,
        CoreFailureKind::Compatibility => GroupConsumerCommitFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => GroupConsumerCommitFailureKind::InvalidResponse,
        CoreFailureKind::ResponseTooLarge => GroupConsumerCommitFailureKind::ResponseTooLarge,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> GroupConsumerCommitDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => GroupConsumerCommitDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => GroupConsumerCommitDeliveryStatus::PossiblySent,
    }
}
