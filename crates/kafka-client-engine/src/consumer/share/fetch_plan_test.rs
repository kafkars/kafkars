//! Broker-local initial `ShareFetch` planning evidence.

use std::sync::Arc;

use kafka_client_core::{GroupAssignmentPartition, PartitionIndex, ShareFetchBrokerId, TopicId};

use super::{
    catalog::{ShareMembershipCatalog, ShareTopicIdentity},
    fetch_plan::{ShareBrokerSessionPlan, ShareBrokerSessionPlanError},
};
use crate::protocol::consumer::share_fetch::{ShareFetchRequestSettings, share_fetch_request};

#[test]
fn membership_subset_becomes_one_complete_canonical_initial_plan() {
    let catalog = catalog();
    let plan = ShareBrokerSessionPlan::try_initial(
        &catalog,
        broker(),
        &[partition(1, 1), partition(1, 0), partition(2, 2)],
    )
    .unwrap_or_else(|error| panic!("valid broker plan: {error:?}"));
    let (broker_id, assignment, request) = plan.into_parts();
    let prepared = share_fetch_request(
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
        request,
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
