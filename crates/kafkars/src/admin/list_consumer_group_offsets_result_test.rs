//! Public group-offset batch ordering and throttle scenarios.

use std::time::Duration;

use crate::{KafkaError, TopicPartition};

use super::{BatchResult, ConsumerGroupOffset, ListConsumerGroupOffsetsResult};

#[test]
fn result_preserves_throttle_and_engine_supplied_order() {
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
    let result = ListConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "audit");
    assert_eq!(result.offsets().entries()[0].0.partition(), 1);
    assert_eq!(result.into_offsets().entries().len(), 2);
}
