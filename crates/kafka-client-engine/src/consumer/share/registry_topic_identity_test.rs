//! Share topic-identity ordering, deadline, and membership-install scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{GroupId, ShareGroupHeartbeatFailure, ShareGroupHeartbeatRequestKind};

use crate::{clock::MonotonicClock, driver::TopicPartitionCountFact};

use super::{
    entry::ShareConsumerEntry,
    registry_topic_identity::{complete_topic_identity, topic_lookup_failure},
};

#[test]
fn exact_topic_facts_install_join_under_the_original_capture() {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut entry = ShareConsumerEntry::try_new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        Arc::from("workers"),
        None,
        vec![Arc::from("first"), Arc::from("second")],
    )
    .unwrap_or_else(|_error| panic!("entry"));
    entry.begin(capture).unwrap_or_else(|()| panic!("begin"));
    let first_id = entry
        .local_topic_id(0)
        .unwrap_or_else(|| panic!("first id"));
    let second_id = entry
        .local_topic_id(1)
        .unwrap_or_else(|| panic!("second id"));

    complete_topic_identity(
        &mut entry,
        first_id,
        Arc::from("first"),
        capture.operation_deadline(),
        fact([1; 16], 3),
    )
    .unwrap_or_else(|error| panic!("first: {error:?}"));
    assert!(entry.membership.is_none());
    complete_topic_identity(
        &mut entry,
        second_id,
        Arc::from("second"),
        capture.operation_deadline(),
        fact([2; 16], 2),
    )
    .unwrap_or_else(|error| panic!("second: {error:?}"));

    let membership = entry
        .membership
        .as_ref()
        .unwrap_or_else(|| panic!("membership"));
    let prepared = membership.prepared().unwrap_or_else(|| panic!("prepared"));
    assert_eq!(prepared.kind, ShareGroupHeartbeatRequestKind::Join);
    assert_eq!(prepared.deadline, capture.operation_deadline());
    assert_eq!(entry.fault, None);
}

#[test]
fn malformed_topic_identity_terminalizes_before_membership() {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut entry = ShareConsumerEntry::try_new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        Arc::from("workers"),
        None,
        vec![Arc::from("jobs")],
    )
    .unwrap_or_else(|_error| panic!("entry"));
    entry.begin(capture).unwrap_or_else(|()| panic!("begin"));
    let topic_id = entry.local_topic_id(0).unwrap_or_else(|| panic!("topic"));

    complete_topic_identity(
        &mut entry,
        topic_id,
        Arc::from("jobs"),
        capture.operation_deadline(),
        TopicPartitionCountFact {
            metadata_generation: 1,
            logical_partition_count: 1,
            kafka_topic_id: None,
        },
    )
    .unwrap_or_else(|error| panic!("terminal: {error:?}"));
    assert_eq!(
        entry.fault,
        Some(ShareGroupHeartbeatFailure::InvalidResponse)
    );
    assert!(entry.membership.is_none());
}

#[test]
fn lookup_failure_classification_is_exact_and_fail_closed() {
    use crate::driver::TopicPartitionCountFailure as Failure;

    assert_eq!(
        topic_lookup_failure(Failure::Deadline),
        ShareGroupHeartbeatFailure::DeadlineElapsed
    );
    assert_eq!(
        topic_lookup_failure(Failure::Broker(3)),
        ShareGroupHeartbeatFailure::Broker(3)
    );
    assert_eq!(
        topic_lookup_failure(Failure::Malformed),
        ShareGroupHeartbeatFailure::InvalidResponse
    );
    assert_eq!(
        topic_lookup_failure(Failure::UnrecognizedDriverFailure),
        ShareGroupHeartbeatFailure::Execution
    );
}

fn fact(kafka_topic_id: [u8; 16], logical_partition_count: u32) -> TopicPartitionCountFact {
    TopicPartitionCountFact {
        metadata_generation: 1,
        logical_partition_count,
        kafka_topic_id: Some(kafka_topic_id),
    }
}
