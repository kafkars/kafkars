//! Caller-ordered finalized-feature result tests.

use std::time::Duration;

use super::UpdateFeaturesResult;
use crate::{BatchResult, DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn old_broker_partial_results_preserve_order_and_exact_failure_facts() {
    let result = UpdateFeaturesResult::new(
        Duration::from_millis(23),
        BatchResult::new(vec![
            (String::from("metadata.version"), Ok(())),
            (
                String::from("transaction.version"),
                Err(KafkaError::new(ErrorKind::Broker, "feature rejected")
                    .with_broker_code(Some(-1234))
                    .with_delivery_status(DeliveryStatus::PossiblySent)
                    .with_diagnostic_truncated(true)),
            ),
            (String::from("group.version"), Ok(())),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(23));
    assert_eq!(
        result.features().entries()[0].0.as_str(),
        "metadata.version"
    );
    assert_eq!(
        result.features().entries()[1].0.as_str(),
        "transaction.version"
    );
    assert_eq!(result.features().entries()[2].0.as_str(), "group.version");

    let error = result.features().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("per-feature broker error expected"));
    assert_eq!(error.broker_code(), Some(-1234));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}

#[test]
fn result_can_be_consumed_without_losing_caller_order() {
    let result = UpdateFeaturesResult::new(
        Duration::ZERO,
        BatchResult::new(vec![
            (String::from("first"), Ok(())),
            (String::from("second"), Ok(())),
        ]),
    );
    let entries = result.into_features().into_entries();
    assert_eq!(entries[0].0, "first");
    assert_eq!(entries[1].0, "second");
}
