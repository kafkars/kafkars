//! Exact scalar, nullable-list, sentinel, order, and local-shape value tests.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual construction failures"
)]

use super::{
    DescribeTopicPartition, DescribeTopicPartitionsTopic, DescribeTopicPartitionsValueError,
};

#[test]
fn partition_preserves_signed_error_optional_leader_and_every_ordered_list() {
    let partition = DescribeTopicPartition::new(
        -32_000,
        7,
        None,
        None,
        vec![9, 2],
        vec![2],
        Some(Vec::new()),
        None,
        vec![9],
    )
    .expect("valid partition");
    assert_eq!(partition.error_code(), -32_000);
    assert_eq!(partition.partition_index(), 7);
    assert_eq!(partition.leader_id(), None);
    assert_eq!(partition.leader_epoch(), None);
    assert_eq!(partition.replicas(), [9, 2]);
    assert_eq!(partition.isr(), [2]);
    assert_eq!(partition.eligible_leader_replicas(), Some(&[][..]));
    assert_eq!(partition.last_known_elr(), None);
    assert_eq!(partition.offline_replicas(), [9]);
}

#[test]
fn topic_preserves_uuid_internal_authorizations_error_and_partition_order() {
    let topic = DescribeTopicPartitionsTopic::new(
        -17,
        "orders".to_owned(),
        [0xAB; 16],
        true,
        vec![partition(9), partition(2)],
        i32::MIN,
    )
    .expect("valid topic");
    assert_eq!(topic.error_code(), -17);
    assert_eq!(topic.name(), "orders");
    assert_eq!(topic.topic_id(), [0xAB; 16]);
    assert!(topic.internal());
    assert_eq!(topic.authorized_operations(), i32::MIN);
    assert_eq!(topic.partitions()[0].partition_index(), 9);
    assert_eq!(topic.partitions()[1].partition_index(), 2);
}

#[test]
fn negative_and_duplicate_partition_and_broker_scalars_are_rejected() {
    assert_eq!(
        DescribeTopicPartition::new(
            0,
            -1,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            Vec::new(),
        ),
        Err(DescribeTopicPartitionsValueError::NegativePartition)
    );
    assert_eq!(
        DescribeTopicPartition::new(
            0,
            0,
            Some(-1),
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            Vec::new(),
        ),
        Err(DescribeTopicPartitionsValueError::NegativeLeaderId)
    );
    assert_eq!(
        DescribeTopicPartition::new(
            0,
            0,
            None,
            Some(-1),
            Vec::new(),
            Vec::new(),
            None,
            None,
            Vec::new(),
        ),
        Err(DescribeTopicPartitionsValueError::NegativeLeaderEpoch)
    );
    assert_eq!(
        DescribeTopicPartition::new(
            0,
            0,
            None,
            None,
            vec![1, 1],
            Vec::new(),
            None,
            None,
            Vec::new(),
        ),
        Err(DescribeTopicPartitionsValueError::DuplicateBrokerId)
    );
    assert_eq!(
        DescribeTopicPartitionsTopic::new(
            0,
            "orders".to_owned(),
            [0; 16],
            false,
            vec![partition(1), partition(1)],
            0,
        ),
        Err(DescribeTopicPartitionsValueError::DuplicatePartition)
    );
}

pub(super) fn partition(index: i32) -> DescribeTopicPartition {
    DescribeTopicPartition::new(
        0,
        index,
        Some(1),
        Some(2),
        vec![1, 2],
        vec![1],
        Some(vec![2]),
        Some(Vec::new()),
        Vec::new(),
    )
    .expect("valid partition")
}
