//! Public legacy replacement batch result scenarios.

use std::time::Duration;

use super::LegacyReplaceTopicConfigsResult;
use crate::{BatchResult, DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn throttle_topic_order_and_exact_broker_facts_are_retained() {
    let result = LegacyReplaceTopicConfigsResult::new(
        Duration::from_millis(37),
        BatchResult::new(vec![
            (String::from("orders"), Ok(())),
            (
                String::from("audit"),
                Err(
                    KafkaError::new(ErrorKind::Broker, "broker rejected legacy replacement")
                        .with_broker_code(Some(-31_234))
                        .with_delivery_status(DeliveryStatus::PossiblySent)
                        .with_diagnostic_truncated(true),
                ),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(37));
    assert_eq!(result.topics().entries()[0].0, "orders");
    let error = result.topics().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    assert_eq!(error.broker_code(), Some(-31_234));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}
