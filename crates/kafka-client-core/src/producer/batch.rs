//! Producer-owned topic-partition batch membership and timer state.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchTimerGeneration, ByteCount, Deadline,
    DeliveryStatus, Moment, OperationId, PartitionIndex, ProducerBatchPolicy,
    ProducerSequenceLease, TopicId,
};

/// Topic-partition identity for one accumulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BatchRoute {
    pub(crate) topic_id: TopicId,
    pub(crate) partition: PartitionIndex,
}

/// One operation's ordered membership in a producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchMember {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: Deadline,
    pub(crate) accumulator_bytes: Option<ByteCount>,
}

/// Core-owned lifecycle of one logical producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchState {
    Open,
    Materializing,
    AwaitingIdentity,
    AwaitingDriver,
    Submitted,
    RetryWaiting,
}

/// Linear owner of batch membership, readiness, and stale-timer fencing.
#[derive(Debug)]
pub(crate) struct ProducerBatch {
    pub(crate) route: BatchRoute,
    pub(crate) policy: ProducerBatchPolicy,
    pub(crate) linger_deadline: Deadline,
    pub(crate) timer_generation: BatchTimerGeneration,
    pub(crate) timer_deadline: Deadline,
    pub(crate) linger_elapsed: bool,
    pub(crate) accumulator_bytes: ByteCount,
    pub(crate) members: Vec<BatchMember>,
    pub(crate) execution_generation: Option<BatchExecutionGeneration>,
    pub(crate) retries_started: u32,
    pub(crate) prior_delivery: DeliveryStatus,
    pub(crate) sequence_lease: Option<ProducerSequenceLease>,
    pub(crate) state: BatchState,
}

/// Preflighted membership removal consumed by the batch mutation owner.
#[derive(Debug)]
pub(crate) struct BatchRemoval {
    pub(crate) members: Vec<BatchMember>,
    pub(crate) accumulator_bytes: ByteCount,
    pub(crate) timer_update: Option<(BatchTimerGeneration, Deadline)>,
    pub(crate) linger_elapsed: bool,
}

/// Preflighted replacement of one immutable sealed-batch execution.
#[derive(Debug)]
pub(crate) struct BatchRevision {
    pub(crate) previous: BatchExecutionId,
    pub(crate) replacement: Option<BatchExecutionId>,
    pub(crate) members: Vec<BatchMember>,
    pub(crate) accumulator_bytes: ByteCount,
}

/// Pure observation of one current timer fact before any batch mutation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchTimerObservation {
    pub(crate) linger_elapsed: bool,
    pub(crate) readies_batch: bool,
}

/// Preflighted accumulator confirmation consumed without further failure.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchAccumulation {
    pub(crate) member_index: usize,
    pub(crate) accumulator_bytes: ByteCount,
    pub(crate) readies_batch: bool,
}

/// Preflighted seal transition spanning batch and operation owners.
#[derive(Debug)]
pub(crate) struct BatchSeal {
    pub(crate) batch_id: crate::BatchId,
    pub(crate) members: Vec<OperationId>,
    pub(crate) route: BatchRoute,
    pub(crate) timer_generation: BatchTimerGeneration,
    pub(crate) execution: BatchExecutionId,
}

impl ProducerBatch {
    pub(crate) fn new(
        route: BatchRoute,
        policy: ProducerBatchPolicy,
        now: Moment,
        operation_id: OperationId,
        deadline: Deadline,
    ) -> Option<Self> {
        let linger_deadline = now.checked_deadline_after(policy.linger_ticks())?;
        Some(Self {
            route,
            policy,
            linger_deadline,
            timer_generation: BatchTimerGeneration::from_raw(1),
            timer_deadline: deadline.min(linger_deadline),
            linger_elapsed: false,
            accumulator_bytes: ByteCount::new(0),
            members: vec![BatchMember {
                operation_id,
                deadline,
                accumulator_bytes: None,
            }],
            execution_generation: None,
            retries_started: 0,
            prior_delivery: DeliveryStatus::NotSent,
            sequence_lease: None,
            state: BatchState::Open,
        })
    }

    pub(crate) fn member_ids(&self) -> Vec<OperationId> {
        self.members
            .iter()
            .map(|member| member.operation_id)
            .collect()
    }

    pub(crate) fn earliest_deadline(&self) -> Option<Deadline> {
        self.earliest_deadline_owner()
            .map(|(_operation_id, deadline)| deadline)
    }

    /// Returns the earliest live member, preserving membership order on ties.
    pub(crate) fn earliest_deadline_owner(&self) -> Option<(OperationId, Deadline)> {
        let first = self.members.first()?;
        Some(self.members.iter().skip(1).fold(
            (first.operation_id, first.deadline),
            |earliest, member| {
                if member.deadline < earliest.1 {
                    (member.operation_id, member.deadline)
                } else {
                    earliest
                }
            },
        ))
    }

    pub(crate) fn contains(&self, operation_id: OperationId) -> bool {
        self.members
            .iter()
            .any(|member| member.operation_id == operation_id)
    }

    pub(crate) fn all_accumulated(&self) -> bool {
        self.members
            .iter()
            .all(|member| member.accumulator_bytes.is_some())
    }

    pub(crate) fn execution_id(&self, batch_id: crate::BatchId) -> Option<BatchExecutionId> {
        self.execution_generation
            .map(|generation| BatchExecutionId::new(batch_id, generation))
    }

    pub(crate) const fn sequence_lease(&self) -> Option<ProducerSequenceLease> {
        self.sequence_lease
    }

    pub(crate) const fn prior_delivery(&self) -> DeliveryStatus {
        self.prior_delivery
    }
}
