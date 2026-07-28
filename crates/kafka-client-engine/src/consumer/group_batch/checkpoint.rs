//! Stable public ownership of one exact core group checkpoint.

use std::sync::Arc;

use kafka_client_core::{GroupCheckpoint, GroupCheckpointEntry, GroupPositionFence};

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
    _position_fence: GroupPositionFence,
    checkpoint: GroupCheckpoint,
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

    #[cfg(test)]
    pub(in crate::consumer) fn into_core(self) -> GroupCheckpoint {
        self.checkpoint
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
        _position_fence: fence,
        checkpoint,
    }
}
