//! Existing producer partition policy adapted to one transactional send slot.

use kafka_client_core::{
    PartitionIndex, TopicId,
    partitioning::{
        StickyPartitioner, TopicPartitionFacts, TopicPartitionSource,
        select_java_keyed_topic_partition,
    },
};

use crate::{driver::TopicPartitionCountFailure, producer::ProducerPartitioningFailure};

use super::input::TransactionSendRequest;

pub(super) struct TransactionPartitionSelection {
    pub(super) partition: PartitionIndex,
    pub(super) sticky: bool,
}

struct TransactionTopicSticky {
    topic_id: TopicId,
    policy: StickyPartitioner,
}

pub(super) struct TransactionStickyPartitioners {
    capacity: usize,
    entries: Vec<TransactionTopicSticky>,
}

impl TransactionStickyPartitioners {
    pub(super) const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::new(),
        }
    }

    pub(super) fn select(
        &mut self,
        request: &TransactionSendRequest,
        source: &dyn TopicPartitionSource,
    ) -> Result<TransactionPartitionSelection, TransactionPartitioningFailure> {
        let facts = TopicPartitionFacts::new(source);
        if let Some(key) = request.key_bytes() {
            return select_java_keyed_topic_partition(key, facts)
                .map(|selection| TransactionPartitionSelection {
                    partition: selection.partition(),
                    sticky: false,
                })
                .map_err(|_| TransactionPartitioningFailure::MetadataUnavailable {
                    broker_code: None,
                });
        }
        let policy = self.policy(request.topic_id())?;
        policy
            .select(facts)
            .map(|selection| TransactionPartitionSelection {
                partition: selection.partition(),
                sticky: true,
            })
            .map_err(|_| TransactionPartitioningFailure::MetadataUnavailable { broker_code: None })
    }

    pub(super) fn partition_batch_sealed(&mut self, topic_id: TopicId, partition: PartitionIndex) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.topic_id == topic_id)
        {
            entry.policy.partition_batch_sealed(partition);
        }
    }

    fn policy(
        &mut self,
        topic_id: TopicId,
    ) -> Result<&mut StickyPartitioner, TransactionPartitioningFailure> {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.topic_id == topic_id)
        {
            return Ok(&mut self.entries[index].policy);
        }
        if self.entries.len() >= self.capacity || self.entries.try_reserve_exact(1).is_err() {
            return Err(TransactionPartitioningFailure::Capacity);
        }
        self.entries.push(TransactionTopicSticky {
            topic_id,
            policy: StickyPartitioner::new(topic_id.get().saturating_sub(1)),
        });
        Ok(&mut self
            .entries
            .last_mut()
            .unwrap_or_else(|| unreachable!("new sticky policy was just inserted"))
            .policy)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionPartitioningFailure {
    DeadlineElapsed,
    MetadataUnavailable { broker_code: Option<i16> },
    Capacity,
}

pub(super) const fn normalize_topic_view_failure(
    failure: TopicPartitionCountFailure,
) -> TransactionPartitioningFailure {
    let failure = match failure {
        TopicPartitionCountFailure::Deadline => ProducerPartitioningFailure::DeadlineElapsed,
        TopicPartitionCountFailure::Broker(code) => {
            ProducerPartitioningFailure::MetadataUnavailable {
                broker_code: Some(code),
            }
        }
        TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Malformed
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining
        | TopicPartitionCountFailure::TopicMismatch
        | TopicPartitionCountFailure::Completion
        | TopicPartitionCountFailure::UnrecognizedDriverFailure => {
            ProducerPartitioningFailure::MetadataUnavailable { broker_code: None }
        }
    };
    match failure {
        ProducerPartitioningFailure::DeadlineElapsed => {
            TransactionPartitioningFailure::DeadlineElapsed
        }
        ProducerPartitioningFailure::MetadataUnavailable { broker_code } => {
            TransactionPartitioningFailure::MetadataUnavailable { broker_code }
        }
    }
}
