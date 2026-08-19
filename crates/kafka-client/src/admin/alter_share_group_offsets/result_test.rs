//! Public `ShareGroup` offset-alteration result tests.

use std::time::Duration;

use crate::{BatchResult, ErrorKind, KafkaError, TopicPartition};

use super::AlterShareGroupOffsetsResult;

#[test]
fn result_preserves_throttle_topic_ids_and_engine_supplied_caller_order() {
    let offsets = BatchResult::new(vec![
        (TopicPartition::new("orders", 7), Ok([7; 16])),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(ErrorKind::Broker, "rejected")),
        ),
    ]);
    let result = AlterShareGroupOffsetsResult::new(Duration::from_millis(73), offsets);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    assert_eq!(result.offsets().entries()[0].1, Ok([7; 16]));
    assert_eq!(result.into_offsets().entries().len(), 2);
}
