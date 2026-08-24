//! Broker-local initial `ShareFetch` planning evidence.

use std::sync::Arc;

use kafka_client_core::{
    AssignedTopicPartition, GroupAssignmentPartition, PartitionIndex,
    SHARE_FETCH_MAX_PARTITIONS_PER_BROKER, ShareFetchBrokerId, TopicId,
    partitioning::TopicMetadataGeneration,
};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_plan::{ShareBrokerSessionPlan, ShareBrokerSessionPlanError},
};
use crate::protocol::consumer::share_fetch::ShareFetchRequestSettings;

#[test]
fn membership_subset_becomes_one_complete_canonical_initial_plan() {
    let catalog = catalog();
    let plan = ShareBrokerSessionPlan::try_initial(
        &catalog,
        broker(),
        &[partition(1, 1), partition(1, 0), partition(2, 2)],
    )
    .unwrap_or_else(|error| panic!("valid broker plan: {error:?}"));
    let (broker_id, assignment, request_plan) = plan.into_parts();
    let prepared = request_plan
        .prepare(
            "workers",
            "member-a",
            0,
            ShareFetchRequestSettings {
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: 1_024,
                max_records: 32,
                batch_size: 8,
            },
        )
        .unwrap_or_else(|error| panic!("initial request: {error:?}"));
    let (request, correlation) = prepared.into_parts();

    assert_eq!(broker_id, broker());
    assert_eq!(assignment.len(), 3);
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].topic_id.to_bytes(), [1; 16]);
    assert_eq!(
        request.topics[0]
            .partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(request.forgotten_topics_data.is_empty());
    assert!(correlation.contains([1; 16], 0));
    assert!(correlation.contains([2; 16], 2));
    assert_eq!(
        request_plan.resolve_partition([1; 16], 0),
        Some(AssignedTopicPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        ))
    );
    assert_eq!(request_plan.resolve_partition([1; 16], 2), None);

    let steady = request_plan
        .prepare(
            "workers",
            "member-a",
            1,
            ShareFetchRequestSettings {
                max_wait_ms: 500,
                min_bytes: 1,
                max_bytes: 1_024,
                max_records: 32,
                batch_size: 8,
            },
        )
        .unwrap_or_else(|error| panic!("steady request: {error:?}"));
    let (steady, steady_correlation) = steady.into_parts();
    assert!(steady.topics.is_empty());
    assert!(steady.forgotten_topics_data.is_empty());
    assert!(steady_correlation.contains([1; 16], 0));
}

#[test]
fn routed_plan_retains_the_newest_observed_generation_for_each_topic() {
    let plan = ShareBrokerSessionPlan::try_routed(
        &catalog(),
        broker(),
        &[
            (partition(1, 0), TopicMetadataGeneration::from_raw(4)),
            (partition(1, 1), TopicMetadataGeneration::from_raw(7)),
            (partition(2, 2), TopicMetadataGeneration::from_raw(5)),
        ],
    )
    .unwrap_or_else(|error| panic!("routed plan: {error:?}"));
    let (_broker, _assignment, request) = plan.into_parts();

    assert_eq!(
        request.route_refresh_requirement([1; 16]),
        Some((Arc::from("a"), TopicMetadataGeneration::from_raw(7)))
    );
    assert_eq!(
        request.route_refresh_requirement([2; 16]),
        Some((Arc::from("b"), TopicMetadataGeneration::from_raw(5)))
    );
    assert_eq!(request.route_refresh_requirement([9; 16]), None);
}

#[test]
fn unknown_duplicate_and_out_of_range_partitions_fail_before_session_open() {
    let catalog = catalog();
    assert_eq!(
        ShareBrokerSessionPlan::try_initial(&catalog, broker(), &[partition(9, 0)]).err(),
        Some(ShareBrokerSessionPlanError::UnknownTopic)
    );
    assert_eq!(
        ShareBrokerSessionPlan::try_initial(
            &catalog,
            broker(),
            &[partition(1, 0), partition(1, 0)],
        )
        .err(),
        Some(ShareBrokerSessionPlanError::DuplicatePartition)
    );
    assert_eq!(
        ShareBrokerSessionPlan::try_initial(&catalog, broker(), &[partition(1, 2)]).err(),
        Some(ShareBrokerSessionPlanError::PartitionOutOfRange)
    );
}

#[test]
fn one_broker_plan_rejects_more_partitions_than_the_core_session_can_own() {
    let catalog = ShareMembershipCatalog::try_new(
        Arc::from("workers"),
        Arc::from("member-a"),
        None,
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(1),
            Arc::from("a"),
            [1; 16],
            u32::try_from(SHARE_FETCH_MAX_PARTITIONS_PER_BROKER + 1)
                .unwrap_or_else(|error| panic!("partition count: {error}")),
        )],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    let partitions = (0..=SHARE_FETCH_MAX_PARTITIONS_PER_BROKER)
        .map(|partition| {
            GroupAssignmentPartition::new(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(
                    u32::try_from(partition).unwrap_or_else(|error| panic!("partition: {error}")),
                ),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        ShareBrokerSessionPlan::try_initial(&catalog, broker(), &partitions).err(),
        Some(ShareBrokerSessionPlanError::PartitionCapacity)
    );
}

fn catalog() -> ShareMembershipCatalog {
    ShareMembershipCatalog::try_new(
        Arc::from("workers"),
        Arc::from("member-a"),
        None,
        vec![
            ShareTopicIdentity::new(TopicId::from_raw(1), Arc::from("a"), [1; 16], 2),
            ShareTopicIdentity::new(TopicId::from_raw(2), Arc::from("b"), [2; 16], 3),
        ],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

fn partition(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn broker() -> ShareFetchBrokerId {
    ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"))
}
