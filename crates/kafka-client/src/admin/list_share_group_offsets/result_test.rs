//! Public ShareGroup offset-listing result tests.

use std::time::Duration;

use crate::{ErrorKind, KafkaError, TopicPartition};

use super::{super::BatchResult, ListShareGroupOffsetsResult, ShareGroupOffset};

#[test]
fn result_preserves_throttle_order_values_and_errors() {
    let entries = vec![
        (
            TopicPartition::new("orders", 2),
            Ok(ShareGroupOffset::new([2; 16], Some(15), Some(4), Some(8))),
        ),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(ErrorKind::Broker, "partition rejected")),
        ),
    ];
    let result =
        ListShareGroupOffsetsResult::new(Duration::from_millis(29), BatchResult::new(entries));

    assert_eq!(result.throttle_time(), Duration::from_millis(29));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    assert_eq!(result.offsets().entries()[0].0.partition(), 2);
    assert_eq!(
        result.offsets().entries()[0]
            .1
            .as_ref()
            .expect("orders offset")
            .lag(),
        Some(8)
    );
    assert_eq!(
        result.offsets().entries()[1]
            .1
            .as_ref()
            .expect_err("audit error")
            .kind(),
        ErrorKind::Broker
    );
}
