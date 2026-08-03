//! Shared construction helpers for assigned-event ownership tests.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition,
    TopicId,
};

use super::AssignedConsumerEventStore;

pub(in crate::consumer) fn assign_reserved(
    store: &mut AssignedConsumerEventStore,
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> kafka_client_core::AssignedConsumerTransition {
    let prepared = store
        .prepare_replacement(partitions.len())
        .unwrap_or_else(|error| panic!("reserve replacement: {error:?}"));
    let transition = assign(machine, partitions);
    prepared
        .commit_event_claims(transition.effects())
        .unwrap_or_else(|error| panic!("commit replacement: {error:?}"));
    transition
}

pub(in crate::consumer) fn assign(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign: {error}"))
}

pub(in crate::consumer) fn entry(
    topic: u64,
    partition: u32,
    start: StartPosition,
) -> AssignedPartition {
    AssignedPartition::new(self::partition(topic, partition), start)
}

pub(in crate::consumer) fn partition(topic: u64, partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

pub(in crate::consumer) fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}

pub(in crate::consumer) fn event_store(capacity: usize) -> AssignedConsumerEventStore {
    AssignedConsumerEventStore::new(capacity)
        .unwrap_or_else(|error| panic!("event store: {error:?}"))
}

pub(in crate::consumer) fn retain(
    store: &mut AssignedConsumerEventStore,
    topic: &str,
    effect: AssignedConsumerEffect,
) {
    store
        .retain_terminal(Arc::from(topic), effect)
        .unwrap_or_else(|(error, _topic)| panic!("retain terminal: {error:?}"));
}
