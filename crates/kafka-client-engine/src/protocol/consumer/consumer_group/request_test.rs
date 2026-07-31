//! API 68 v0 join, steady, leave, and bounded rejection request evidence.

use kafka_wire_core::Uuid;

use super::{
    ConsumerGroupHeartbeatOwnedTopic, ConsumerGroupHeartbeatRequestFailure,
    consumer_group_join_request, consumer_group_leave_request, consumer_group_steady_request,
};

#[test]
fn join_carries_epoch_zero_complete_subscription_and_initial_configuration() {
    let prepared = consumer_group_join_request(
        "orders-workers",
        Some("worker-a"),
        30_000,
        &["orders", "payments"],
    )
    .unwrap_or_else(|error| panic!("join request: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(request.group_id.as_str(), "orders-workers");
    assert_eq!(request.member_id.as_str(), "");
    assert_eq!(request.member_epoch, 0);
    assert_eq!(
        request.instance_id.as_ref().map(|value| value.as_str()),
        Some("worker-a")
    );
    assert_eq!(request.rebalance_timeout_ms, 30_000);
    assert_eq!(
        request.subscribed_topic_names.as_ref().map(|topics| {
            topics
                .iter()
                .map(|topic| topic.as_str())
                .collect::<Vec<_>>()
        }),
        Some(vec!["orders", "payments"])
    );
    assert!(request.topic_partitions.is_none());
}

#[test]
fn steady_heartbeat_canonicalizes_current_topic_and_partition_ownership() {
    let topics = [
        ConsumerGroupHeartbeatOwnedTopic::new([2; 16], vec![4, 1]),
        ConsumerGroupHeartbeatOwnedTopic::new([1; 16], vec![3]),
    ];
    let prepared = consumer_group_steady_request("group", "member", 7, Some(&topics))
        .unwrap_or_else(|error| panic!("steady request: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(request.member_epoch, 7);
    assert_eq!(request.rebalance_timeout_ms, -1);
    assert!(request.subscribed_topic_names.is_none());
    let owned = request
        .topic_partitions
        .as_ref()
        .unwrap_or_else(|| panic!("owned partitions"));
    assert_eq!(owned[0].topic_id, Uuid::from_bytes([1; 16]));
    assert_eq!(owned[0].partitions, [3]);
    assert_eq!(owned[1].topic_id, Uuid::from_bytes([2; 16]));
    assert_eq!(owned[1].partitions, [1, 4]);
}

#[test]
fn leave_uses_epoch_minus_one_without_replaying_unchanged_configuration() {
    let prepared = consumer_group_leave_request("group", "member")
        .unwrap_or_else(|error| panic!("leave request: {error:?}"));
    let request = prepared.into_generated_request();
    assert_eq!(request.member_epoch, -1);
    assert!(request.instance_id.is_none());
    assert!(request.subscribed_topic_names.is_none());
    assert!(request.topic_partitions.is_none());
}

#[test]
fn builders_reject_duplicate_and_unrepresentable_bounded_inputs() {
    assert_eq!(
        consumer_group_join_request("group", None, 10, &["orders", "orders"])
            .err()
            .unwrap_or_else(|| panic!("duplicate subscription must reject")),
        ConsumerGroupHeartbeatRequestFailure::DuplicateTopicName
    );
    let duplicate_topics = [
        ConsumerGroupHeartbeatOwnedTopic::new([1; 16], vec![0]),
        ConsumerGroupHeartbeatOwnedTopic::new([1; 16], vec![1]),
    ];
    assert_eq!(
        consumer_group_steady_request("group", "member", 1, Some(&duplicate_topics))
            .err()
            .unwrap_or_else(|| panic!("duplicate topic ID must reject")),
        ConsumerGroupHeartbeatRequestFailure::DuplicateTopicId
    );
    let invalid_partition = [ConsumerGroupHeartbeatOwnedTopic::new(
        [1; 16],
        vec![u32::MAX],
    )];
    assert_eq!(
        consumer_group_steady_request("group", "member", 1, Some(&invalid_partition))
            .err()
            .unwrap_or_else(|| panic!("oversized partition must reject")),
        ConsumerGroupHeartbeatRequestFailure::PartitionOutOfRange(u32::MAX)
    );
}
