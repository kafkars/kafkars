//! Public topic-builder ownership scenarios.

use super::{NewTopic, NewTopicPlacement, TopicReplicaAssignment};

#[test]
fn fluent_topic_values_preserve_ordered_configuration() {
    let topic = NewTopic::new("orders", 24)
        .replication_factor(3)
        .config("cleanup.policy", "compact")
        .config("min.insync.replicas", "2");

    assert_eq!(topic.name(), "orders");
    assert_eq!(topic.partitions(), 24);
    assert_eq!(topic.requested_replication_factor(), 3);
    let (_name, _placement, _mixed_replication, configs) = topic.into_parts();
    assert_eq!(
        configs,
        [
            ("cleanup.policy".to_owned(), "compact".to_owned()),
            ("min.insync.replicas".to_owned(), "2".to_owned()),
        ]
    );
}

#[test]
fn manual_topic_preserves_assignment_and_config_order_without_wire_types() {
    let topic = NewTopic::with_replica_assignments(
        "placed",
        [
            TopicReplicaAssignment::new(0, [7, 3]),
            TopicReplicaAssignment::new(1, [3, 9]),
        ],
    )
    .config("cleanup.policy", "compact");

    assert_eq!(topic.partitions(), -1);
    assert_eq!(topic.requested_replication_factor(), -1);
    let NewTopicPlacement::Manual { assignments } = topic.placement() else {
        panic!("manual placement must remain explicit");
    };
    assert_eq!(assignments[0].partition_index(), 0);
    assert_eq!(assignments[0].broker_ids(), &[7, 3]);
    assert_eq!(assignments[1].partition_index(), 1);
    assert_eq!(assignments[1].broker_ids(), &[3, 9]);
}
