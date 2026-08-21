//! Public fenced producer identity and ordered batch scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts exact fenced-producer results"
)]

use std::time::Duration;

use super::{FenceProducersResult, FencedProducerIdentity};
use crate::{BatchResult, DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn throttle_identity_order_and_exact_error_remain_stable() {
    let identity = FencedProducerIdentity::new(i64::MAX - 7, i16::MAX - 3);
    let result = FenceProducersResult::new(
        Duration::from_millis(137),
        BatchResult::new(vec![
            (String::from("orders-tx"), Ok(identity)),
            (
                String::from("audit-tx"),
                Err(
                    KafkaError::new(ErrorKind::Broker, "broker rejected producer fencing")
                        .with_broker_code(Some(-31_234))
                        .with_delivery_status(DeliveryStatus::PossiblySent),
                ),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(137));
    assert_eq!(result.producers().entries(), result.entries());
    assert_eq!(result.entries()[0].0, "orders-tx");
    let fenced = result.entries()[0]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("fenced identity expected: {error}"));
    assert_eq!(fenced.producer_id(), i64::MAX - 7);
    assert_eq!(fenced.producer_epoch(), i16::MAX - 3);
    let error = result.entries()[1]
        .1
        .as_ref()
        .expect_err("exact broker rejection expected");
    assert_eq!(error.broker_code(), Some(-31_234));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(!error.diagnostic_truncated());

    let entries = result.clone().into_entries();
    assert_eq!(entries[0].0, "orders-tx");
    assert_eq!(
        entries[0]
            .1
            .as_ref()
            .unwrap_or_else(|error| panic!("fenced identity expected: {error}")),
        &identity
    );
    let producers = result.into_producers();
    assert_eq!(producers.entries()[1].0, "audit-tx");
    assert_eq!(
        producers.entries()[1]
            .1
            .as_ref()
            .expect_err("exact broker rejection expected")
            .broker_code(),
        Some(-31_234)
    );
}
