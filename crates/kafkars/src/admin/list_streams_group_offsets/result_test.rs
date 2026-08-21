//! Typed Streams-group result preserves delegated offset facts.

use std::time::Duration;

use crate::{
    KafkaError, TopicPartition,
    admin::{BatchResult, ConsumerGroupOffset, ListConsumerGroupOffsetsResult},
};

use super::ListStreamsGroupOffsetsResult;

#[test]
fn result_preserves_throttle_order_and_exact_offset_facts() {
    let offsets = BatchResult::new(vec![
        (
            TopicPartition::new("audit", 1),
            Ok(ConsumerGroupOffset::new(Some(9), None, None)),
        ),
        (
            TopicPartition::new("orders", 0),
            Err(KafkaError::new(crate::ErrorKind::Broker, "rejected")),
        ),
    ]);
    let consumer = ListConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets);
    let result = ListStreamsGroupOffsetsResult::from_consumer_group(consumer);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "audit");
    assert_eq!(result.offsets().entries()[0].0.partition(), 1);
    assert_eq!(result.into_offsets().entries().len(), 2);
}
