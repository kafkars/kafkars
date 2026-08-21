//! Stable topic-partition replica identity tests.

use super::TopicPartitionReplica;

#[test]
fn identity_retains_exact_topic_partition_and_broker() {
    let replica = TopicPartitionReplica::new("orders", 3, 7);

    assert_eq!(replica.topic(), "orders");
    assert_eq!(replica.partition(), 3);
    assert_eq!(replica.broker_id(), 7);
    assert_eq!(replica.into_parts(), ("orders".to_owned(), 3, 7));
}
