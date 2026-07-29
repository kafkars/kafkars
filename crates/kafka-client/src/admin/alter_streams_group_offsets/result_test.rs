//! Typed StreamsGroup result delegation tests.

use std::time::Duration;

use super::AlterStreamsGroupOffsetsResult;
use crate::{AlterConsumerGroupOffsetsResult, BatchResult, ErrorKind, KafkaError, TopicPartition};

#[test]
fn result_preserves_throttle_errors_and_caller_order_without_remapping() {
    let offsets = BatchResult::new(vec![
        (TopicPartition::new("orders", 7), Ok(())),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(ErrorKind::Broker, "rejected")),
        ),
    ]);
    let result = AlterStreamsGroupOffsetsResult::from_consumer_group(
        AlterConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    assert_eq!(
        result.offsets().entries()[1]
            .1
            .as_ref()
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Broker)
    );
    assert_eq!(result.into_offsets().entries().len(), 2);
}
