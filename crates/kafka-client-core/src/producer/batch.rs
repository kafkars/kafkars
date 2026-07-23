//! Producer-owned topic-partition batch membership and timer state.

use crate::{
    BatchTimerGeneration, ByteCount, Deadline, Moment, OperationId, PartitionIndex,
    ProducerBatchPolicy, TopicId,
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
    AwaitingDriver,
    Submitted,
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
        self.members.iter().map(|member| member.deadline).min()
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
}
