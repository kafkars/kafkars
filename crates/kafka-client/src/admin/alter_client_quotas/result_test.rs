//! Public client-quota alteration result ownership tests.

use std::time::Duration;

use super::{AlterClientQuotasResult, ClientQuotaEntity};
use crate::{BatchResult, DeliveryStatus, ErrorKind, KafkaError};

#[test]
fn throttle_entity_order_and_exact_error_facts_are_retained() {
    let first = ClientQuotaEntity::new([]);
    let second = ClientQuotaEntity::new([]);
    let result = AlterClientQuotasResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            (first, Ok(())),
            (
                second,
                Err(KafkaError::new(ErrorKind::Broker, "quota rejected")
                    .with_broker_code(Some(-1234))
                    .with_delivery_status(DeliveryStatus::PossiblySent)
                    .with_diagnostic_truncated(true)),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    let error = result.entities().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    assert_eq!(error.broker_code(), Some(-1234));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}
