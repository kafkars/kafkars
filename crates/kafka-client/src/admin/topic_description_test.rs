//! Stable topic-description ownership scenarios.

use crate::{ErrorKind, KafkaError};

use super::{TopicDescription, TopicPartitionDescription};

#[test]
fn partition_errors_and_replica_order_are_preserved() {
    let partition = TopicPartitionDescription::new(
        3,
        Some(
            KafkaError::new(ErrorKind::Broker, "unknown broker code")
                .with_broker_code(Some(-32_000)),
        ),
        Some(7),
        Some(11),
        vec![7, 8],
        vec![7],
        vec![8],
    );
    let description =
        TopicDescription::new("orders".to_owned(), Some([5; 16]), false, vec![partition]);
    let partition = &description.partitions()[0];
    assert_eq!(description.name(), "orders");
    assert_eq!(description.topic_id(), Some([5; 16]));
    assert!(!description.is_internal());
    assert_eq!(partition.partition_index(), 3);
    assert_eq!(
        partition.error().and_then(KafkaError::broker_code),
        Some(-32_000)
    );
    assert_eq!(partition.replicas(), [7, 8]);
    assert_eq!(partition.in_sync_replicas(), [7]);
    assert_eq!(partition.offline_replicas(), [8]);
}
