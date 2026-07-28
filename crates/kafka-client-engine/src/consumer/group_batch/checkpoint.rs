//! Stable public ownership of one exact core group checkpoint.

use std::sync::Arc;

use kafka_client_core::{
    GroupCheckpoint, GroupCheckpointEntry, GroupPositionFence, PartitionIndex, TopicId,
};

use crate::consumer::group::ClassicGroupFetchDelivery;

/// One linear assignment-fenced next-offset checkpoint.
///
/// The engine retains the exact group, member, assignment generation, and
/// catalog topic identity required by commit admission without exposing those
/// internal identities through the public API.
#[must_use = "a group checkpoint should be committed or deliberately discarded"]
pub struct GroupConsumerCheckpoint {
    topic: Arc<str>,
    partition: i32,
    next_offset: i64,
    position_fence: GroupPositionFence,
    checkpoint: GroupCheckpoint,
}

/// Exact stable and catalog identities retained across public commit admission.
pub(in crate::consumer) struct GroupConsumerCheckpointObservation {
    checkpoint: GroupConsumerCheckpoint,
    pub(in crate::consumer) topic_id: TopicId,
    pub(in crate::consumer) partition_id: PartitionIndex,
}

impl GroupConsumerCheckpoint {
    /// Returns the catalog-owned Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the next offset to consume.
    pub const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub(crate) const fn position_fence(&self) -> GroupPositionFence {
        self.position_fence
    }

    pub(crate) fn transaction_offset(&self) -> (&Arc<str>, i32, i64, Option<i32>) {
        let entry = self
            .checkpoint
            .entries()
            .first()
            .unwrap_or_else(|| unreachable!("public checkpoint retains one entry"));
        (
            &self.topic,
            self.partition,
            self.next_offset,
            entry.leader_epoch(),
        )
    }

    pub(in crate::consumer) fn try_into_commit_parts(
        self,
    ) -> Result<(GroupConsumerCheckpointObservation, GroupCheckpoint), Self> {
        let entry = self
            .checkpoint
            .entries()
            .first()
            .unwrap_or_else(|| unreachable!("public checkpoint retains one entry"));
        let mut entries = Vec::new();
        if entries
            .try_reserve_exact(self.checkpoint.entries().len())
            .is_err()
        {
            return Err(self);
        }
        entries.extend_from_slice(self.checkpoint.entries());
        let submission = GroupCheckpoint::try_new(
            self.checkpoint.group_id(),
            self.checkpoint.member_id(),
            self.checkpoint.assignment_generation(),
            entries,
        )
        .unwrap_or_else(|error| {
            unreachable!("the exact admitted checkpoint copy remains structurally valid: {error}")
        });
        Ok((
            GroupConsumerCheckpointObservation {
                topic_id: entry.topic_id(),
                partition_id: entry.partition(),
                checkpoint: self,
            },
            submission,
        ))
    }

    #[cfg(test)]
    pub(in crate::consumer) fn into_core(self) -> GroupCheckpoint {
        self.checkpoint
    }

    #[cfg(test)]
    pub(in crate::consumer) fn storage_identity(
        &self,
    ) -> (*const str, *const GroupCheckpointEntry) {
        (Arc::as_ptr(&self.topic), self.checkpoint.entries().as_ptr())
    }

    #[cfg(test)]
    pub(in crate::consumer) fn from_test_parts(
        topic: Arc<str>,
        partition: i32,
        next_offset: i64,
        position_fence: GroupPositionFence,
        checkpoint: GroupCheckpoint,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            position_fence,
            checkpoint,
        }
    }
}

impl GroupConsumerCheckpointObservation {
    pub(in crate::consumer) fn topic(&self) -> &Arc<str> {
        &self.checkpoint.topic
    }

    pub(in crate::consumer) const fn partition(&self) -> i32 {
        self.checkpoint.partition
    }

    pub(in crate::consumer) fn into_checkpoint(self) -> GroupConsumerCheckpoint {
        self.checkpoint
    }

    #[cfg(test)]
    pub(in crate::consumer) fn from_checkpoint(checkpoint: GroupConsumerCheckpoint) -> Self {
        let (observation, submission) = checkpoint
            .try_into_commit_parts()
            .unwrap_or_else(|_checkpoint| panic!("test checkpoint copy"));
        drop(submission);
        observation
    }

    #[cfg(test)]
    pub(in crate::consumer) fn storage_identity(
        &self,
    ) -> (*const str, *const GroupCheckpointEntry) {
        self.checkpoint.storage_identity()
    }
}

impl std::fmt::Debug for GroupConsumerCheckpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerCheckpoint")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("next_offset", &self.next_offset)
            .finish_non_exhaustive()
    }
}

pub(super) fn checkpoint_from_delivery(
    delivery: &ClassicGroupFetchDelivery,
) -> GroupConsumerCheckpoint {
    let fence = delivery.position_fence();
    let partition = delivery.partition_identity();
    let next_offset = delivery.next_offset().get();
    let entry = GroupCheckpointEntry::try_new(
        partition.topic_id(),
        partition.partition(),
        next_offset,
        None,
    )
    .unwrap_or_else(|error| unreachable!("Fetch delivery is checkpoint-valid: {error}"));
    let checkpoint = GroupCheckpoint::try_new(
        fence.group_id(),
        fence.member_id(),
        fence.assignment_generation(),
        vec![entry],
    )
    .unwrap_or_else(|error| unreachable!("one Fetch delivery is a valid checkpoint: {error}"));
    GroupConsumerCheckpoint {
        topic: Arc::clone(delivery.topic_arc()),
        partition: delivery.partition(),
        next_offset,
        position_fence: fence,
        checkpoint,
    }
}
