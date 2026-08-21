//! Exact page, topic, partition, error, and nullable-list preservation tests.

use std::time::Duration;

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::{
    DescribeTopicPartition, DescribeTopicPartitionsCursor, DescribeTopicPartitionsPage,
    DescribeTopicPartitionsTopic,
};

#[test]
fn page_preserves_api_75_only_fields_and_topic_and_partition_errors() {
    let topic_error = broker_error(-17);
    let partition_error = broker_error(-32_000);
    let partition = DescribeTopicPartition::new(
        Some(partition_error),
        7,
        None,
        Some(11),
        vec![9, 2],
        vec![2],
        Some(Vec::new()),
        None,
        vec![9],
    );
    let topic = DescribeTopicPartitionsTopic::new(
        Some(topic_error),
        "orders".to_owned(),
        [0xAB; 16],
        true,
        vec![partition],
        i32::MIN,
    );
    let page = DescribeTopicPartitionsPage::new(
        Duration::from_millis(31),
        vec![topic],
        Some(DescribeTopicPartitionsCursor::new("orders", 8)),
    );

    assert_eq!(page.throttle_time(), Duration::from_millis(31));
    let topic = &page.topics()[0];
    assert_eq!(topic.error().and_then(KafkaError::broker_code), Some(-17));
    assert_eq!(topic.name(), "orders");
    assert_eq!(topic.topic_id(), [0xAB; 16]);
    assert!(topic.is_internal());
    assert_eq!(topic.authorized_operations(), i32::MIN);

    let partition = &topic.partitions()[0];
    assert_eq!(
        partition.error().and_then(KafkaError::broker_code),
        Some(-32_000)
    );
    assert_eq!(partition.partition_index(), 7);
    assert_eq!(partition.leader_id(), None);
    assert_eq!(partition.leader_epoch(), Some(11));
    assert_eq!(partition.replicas(), [9, 2]);
    assert_eq!(partition.in_sync_replicas(), [2]);
    assert_eq!(partition.eligible_leader_replicas(), Some(&[][..]));
    assert_eq!(partition.last_known_eligible_leader_replicas(), None);
    assert_eq!(partition.offline_replicas(), [9]);
    assert_eq!(
        page.next_cursor()
            .map(super::cursor::DescribeTopicPartitionsCursor::partition_index),
        Some(8)
    );
}

#[test]
fn next_cursor_is_only_explicit_page_data() {
    let page = DescribeTopicPartitionsPage::new(Duration::ZERO, Vec::new(), None);
    let (throttle, topics, cursor) = page.into_parts();

    assert_eq!(throttle, Duration::ZERO);
    assert!(topics.is_empty());
    assert_eq!(cursor, None);
}

fn broker_error(code: i16) -> KafkaError {
    KafkaError::new(ErrorKind::Broker, format!("broker code {code}"))
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
}
