//! Two-phase catalog and core-assignment composition scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use super::{AssignedPartitionInput, AssignedTopicLimits, AssignedTopics, AssignedTopicsError};

#[derive(Debug, Eq, PartialEq)]
struct CatalogSnapshot {
    next_topic_id: Option<TopicId>,
    retained_topics: usize,
    retained_name_bytes: usize,
    partitions: Vec<AssignedPartition>,
}

fn limits(max_topic_name_bytes: usize) -> AssignedTopicLimits {
    AssignedTopicLimits::new(8, 8, max_topic_name_bytes, 128)
}

fn entry(topic: &str, partition: u32) -> AssignedPartitionInput {
    let offset =
        NextFetchOffset::try_from_raw(0).unwrap_or_else(|| panic!("test offset must be valid"));
    AssignedPartitionInput::new(
        Arc::from(topic),
        PartitionIndex::from_raw(partition),
        StartPosition::Offset(offset),
    )
}

fn snapshot(owner: &AssignedTopics) -> CatalogSnapshot {
    CatalogSnapshot {
        next_topic_id: owner.next_topic_id_for_test(),
        retained_topics: owner.retained_topic_count(),
        retained_name_bytes: owner.retained_name_bytes(),
        partitions: owner.partitions().to_vec(),
    }
}

fn apply_assignment(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> Result<(), AssignedConsumerMachineError> {
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .map(|_transition| ())
}

#[test]
fn core_acceptance_precedes_infallible_catalog_commit() {
    let mut topics = AssignedTopics::from_initial_for_test(vec![entry("orders", 0)], limits(16))
        .unwrap_or_else(|error| panic!("initial catalog failed: {error:?}"));
    let orders_id = topics.partitions()[0].partition().topic_id();
    let mut machine = AssignedConsumerMachine::new();
    apply_assignment(&mut machine, topics.partitions().to_vec())
        .unwrap_or_else(|error| panic!("initial core assignment failed: {error}"));

    let prepared = topics
        .prepare_replacement(vec![entry("payments", 2)])
        .unwrap_or_else(|error| panic!("replacement preparation failed: {error:?}"));
    apply_assignment(&mut machine, prepared.partitions().to_vec())
        .unwrap_or_else(|error| panic!("replacement core assignment failed: {error}"));
    prepared.commit();

    let payments_id = topics.partitions()[0].partition().topic_id();
    assert_ne!(payments_id, orders_id);
    assert_eq!(topics.name(orders_id).map(AsRef::as_ref), Ok("orders"));
    assert_eq!(topics.name(payments_id).map(AsRef::as_ref), Ok("payments"));
}

#[test]
fn duplicate_core_rejection_and_token_drop_leave_catalog_unchanged() {
    let mut topics = AssignedTopics::from_initial_for_test(vec![entry("orders", 0)], limits(16))
        .unwrap_or_else(|error| panic!("initial catalog failed: {error:?}"));
    let before = snapshot(&topics);
    let mut machine = AssignedConsumerMachine::new();
    apply_assignment(&mut machine, topics.partitions().to_vec())
        .unwrap_or_else(|error| panic!("initial core assignment failed: {error}"));
    let initial_epoch = machine.assignment_epoch();

    let prepared = topics
        .prepare_replacement(vec![entry("payments", 2), entry("payments", 2)])
        .unwrap_or_else(|error| panic!("duplicate facts must stage: {error:?}"));
    let duplicate = prepared.partitions()[1].partition();
    assert_eq!(
        apply_assignment(&mut machine, prepared.partitions().to_vec()),
        Err(AssignedConsumerMachineError::DuplicatePartition {
            partition: duplicate,
        })
    );
    drop(prepared);

    assert_eq!(snapshot(&topics), before);
    assert_eq!(machine.assignment_epoch(), initial_epoch);
    assert_eq!(
        topics.name(TopicId::from_raw(2)),
        Err(AssignedTopicsError::UnknownTopic(TopicId::from_raw(2)))
    );
}

#[test]
fn late_staging_failure_discards_earlier_candidate_name_and_identity() {
    let mut topics = AssignedTopics::from_initial_for_test(vec![entry("orders", 0)], limits(6))
        .unwrap_or_else(|error| panic!("initial catalog failed: {error:?}"));
    let before = snapshot(&topics);

    let error = match topics.prepare_replacement(vec![entry("new", 1), entry("too-long", 2)]) {
        Ok(_prepared) => panic!("second name must fail preparation"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        AssignedTopicsError::TopicNameBytes {
            actual: "too-long".len(),
            limit: 6,
        }
    );
    assert_eq!(snapshot(&topics), before);
    assert_eq!(
        topics.name(TopicId::from_raw(2)),
        Err(AssignedTopicsError::UnknownTopic(TopicId::from_raw(2)))
    );
}
