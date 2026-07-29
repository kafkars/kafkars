//! Exact nullable, signed, sentinel, ordered-list, and cursor success facts.

use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::{
        Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
};
use kafka_wire_core::Uuid;

use super::normalize_describe_topic_partitions_response;

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn response_preserves_exact_page_facts_without_request_correlation() {
    let mut response = DescribeTopicPartitionsResponse::default();
    response.throttle_time_ms = 21;
    response.topics = vec![described_topic(), nullable_error_topic()];
    let mut cursor = Cursor::default();
    cursor.topic_name = "zeta".into();
    cursor.partition_index = 4;
    response.next_cursor = Some(cursor);

    let normalized = normalize_describe_topic_partitions_response(0, &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 21);
    assert!(normalized.retained_bytes() > 0);
    assert_eq!(normalized.topics().len(), 2);

    let topic = &normalized.topics()[0];
    assert_eq!(topic.topic_name(), Some("orders"));
    assert_eq!(topic.scalar_parts(), (-17, [7; 16], true, i32::MIN));
    assert_eq!(topic.partitions().len(), 2);
    let unknown_leader = &topic.partitions()[0];
    assert_eq!(unknown_leader.scalar_parts(), (-31, 3, None, None));
    assert_eq!(unknown_leader.replicas(), [9, 2]);
    assert_eq!(unknown_leader.isr(), [2]);
    assert_eq!(unknown_leader.eligible_leader_replicas(), Some(&[][..]));
    assert_eq!(unknown_leader.last_known_elr(), None);
    assert_eq!(unknown_leader.offline_replicas(), [9]);
    assert_eq!(
        topic.partitions()[1].scalar_parts(),
        (0, 8, Some(5), Some(11))
    );

    assert_eq!(normalized.topics()[1].topic_name(), None);
    assert_eq!(
        normalized.topics()[1].scalar_parts(),
        (-32_000, [0; 16], false, 7)
    );
    let cursor = normalized.next_cursor().expect("next cursor");
    assert_eq!(cursor.topic_name_str(), "zeta");
    assert_eq!(cursor.partition_index(), 4);
}

fn described_topic() -> DescribeTopicPartitionsResponseTopic {
    let mut topic = DescribeTopicPartitionsResponseTopic::default();
    topic.error_code = -17;
    topic.name = Some("orders".into());
    topic.topic_id = Uuid::from_bytes([7; 16]);
    topic.is_internal = true;
    topic.topic_authorized_operations = i32::MIN;
    topic.partitions = vec![unknown_leader_partition(), led_partition()];
    topic
}

fn nullable_error_topic() -> DescribeTopicPartitionsResponseTopic {
    let mut topic = DescribeTopicPartitionsResponseTopic::default();
    topic.error_code = -32_000;
    topic.name = None;
    topic.topic_id = Uuid::ZERO;
    topic.topic_authorized_operations = 7;
    topic
}

fn unknown_leader_partition() -> DescribeTopicPartitionsResponsePartition {
    let mut partition = DescribeTopicPartitionsResponsePartition::default();
    partition.error_code = -31;
    partition.partition_index = 3;
    partition.leader_id = -1;
    partition.leader_epoch = -1;
    partition.replica_nodes = vec![9, 2];
    partition.isr_nodes = vec![2];
    partition.eligible_leader_replicas = Some(Vec::new());
    partition.last_known_elr = None;
    partition.offline_replicas = vec![9];
    partition
}

fn led_partition() -> DescribeTopicPartitionsResponsePartition {
    let mut partition = DescribeTopicPartitionsResponsePartition::default();
    partition.partition_index = 8;
    partition.leader_id = 5;
    partition.leader_epoch = 11;
    partition.replica_nodes = vec![5, 9];
    partition.isr_nodes = vec![5, 9];
    partition.eligible_leader_replicas = None;
    partition.last_known_elr = Some(vec![2]);
    partition
}
