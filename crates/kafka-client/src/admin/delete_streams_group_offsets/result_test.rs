//! Streams-group offset-deletion result delegation tests.

use std::time::Duration;

use super::DeleteStreamsGroupOffsetsResult;
use crate::{BatchResult, DeleteConsumerGroupOffsetsResult, KafkaError, TopicPartition};

#[test]
fn result_preserves_throttle_errors_and_original_caller_order() {
    let offsets = BatchResult::new(vec![
        (TopicPartition::new("orders", 7), Ok(())),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(crate::ErrorKind::Broker, "rejected")),
        ),
    ]);
    let consumer = DeleteConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets);
    let result = DeleteStreamsGroupOffsetsResult::from_consumer(consumer);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    assert_eq!(result.offsets().entries()[0].0.partition(), 7);
    assert_eq!(result.offsets().entries()[1].0.topic(), "audit");
    assert!(result.offsets().entries()[1].1.is_err());
    assert_eq!(result.into_offsets().entries().len(), 2);
}
