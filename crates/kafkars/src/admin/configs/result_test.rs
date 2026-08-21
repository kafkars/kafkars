//! Public topic-configuration batch result scenarios.

use std::time::Duration;

use super::{ConfigEntry, DescribeConfigsResult};
use crate::{BatchResult, DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn throttle_and_original_topic_order_are_retained() {
    let result = DescribeConfigsResult::new(
        Duration::from_millis(77),
        BatchResult::<String, Vec<ConfigEntry>>::new(vec![
            (String::from("orders"), Ok(Vec::new())),
            (
                String::from("audit"),
                Err(
                    KafkaError::new(ErrorKind::Broker, "broker rejected resource")
                        .with_delivery_status(DeliveryStatus::PossiblySent),
                ),
            ),
        ]),
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(77));
    assert_eq!(result.topics().entries()[0].0, "orders");
    assert_eq!(result.topics().entries()[1].0, "audit");
}
