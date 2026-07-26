//! Public group-offset alteration ordering and throttle scenarios.

use std::time::Duration;

use crate::{
    AlterConsumerGroupOffsetsResult, BatchResult, DeliveryStatus, ErrorKind, KafkaError,
    TopicPartition,
};

#[test]
fn result_preserves_throttle_caller_order_and_exact_partition_failure() {
    let offsets = BatchResult::new(vec![
        (TopicPartition::new("orders", 7), Ok(())),
        (
            TopicPartition::new("audit", 1),
            Err(KafkaError::new(ErrorKind::Broker, "rejected")
                .with_broker_code(Some(-32_000))
                .with_delivery_status(DeliveryStatus::PossiblySent)),
        ),
    ]);
    let result = AlterConsumerGroupOffsetsResult::new(Duration::from_millis(73), offsets);
    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    assert_eq!(result.offsets().entries()[0].0.topic(), "orders");
    let error = result.offsets().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("partition broker failure expected"));
    assert_eq!(error.broker_code(), Some(-32_000));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(result.into_offsets().entries().len(), 2);
}
