//! Public group-offset deletion ordering and throttle scenarios.

use std::time::Duration;

use crate::{BatchResult, DeleteConsumerGroupOffsetsResult, KafkaError, TopicPartition};

#[test]
fn result_preserves_throttle_and_engine_supplied_caller_order() {
    let offsets = BatchResult::new(vec![
        (TopicPartition::new("orders", 7), Ok(())),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(crate::ErrorKind::Broker, "rejected")),
        ),
    ]);
    let result = DeleteConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    assert_eq!(result.offsets().entries()[0].0.partition(), 7);
    assert_eq!(result.into_offsets().entries().len(), 2);
}
