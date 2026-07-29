//! Exact protocol-normalized topic-description scenarios.

use core::num::NonZeroI16;

use super::{TopicDescription, TopicPartitionDescription};

#[test]
fn partition_error_and_replica_facts_are_lossless() {
    let code = NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("code is nonzero"));
    let partition = TopicPartitionDescription::new(
        3,
        Some(code),
        Some(7),
        Some(11),
        vec![7, 8],
        vec![7],
        vec![8],
    );
    let topic = TopicDescription::new("orders".to_owned(), Some([7; 16]), false, vec![partition])
        .with_authorized_operations(Some(-1_234_567));
    let partition = &topic.partitions()[0];
    assert_eq!(topic.name(), "orders");
    assert_eq!(topic.topic_id(), Some([7; 16]));
    assert!(!topic.is_internal());
    assert_eq!(topic.authorized_operations(), Some(-1_234_567));
    assert_eq!(partition.partition_index(), 3);
    assert_eq!(partition.error_code(), Some(-32_000));
    assert_eq!(partition.leader_id(), Some(7));
    assert_eq!(partition.leader_epoch(), Some(11));
    assert_eq!(partition.replicas(), [7, 8]);
    assert_eq!(partition.in_sync_replicas(), [7]);
    assert_eq!(partition.offline_replicas(), [8]);
}
