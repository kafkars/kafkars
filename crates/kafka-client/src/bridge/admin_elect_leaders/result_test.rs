//! Public leader-election broker diagnostic translation scenarios.

use crate::{DeliveryStatus, ErrorKind};

use super::result::broker_error_parts;

#[test]
fn signed_broker_diagnostic_retains_delivery_and_truncation() {
    let error = broker_error_parts(
        "partition",
        -31_998,
        Some("controller rejected election"),
        true,
        DeliveryStatus::PossiblySent,
    );

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_998));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(
        error.to_string(),
        "Kafka rejected leader election partition with broker code -31998: controller rejected election (truncated)"
    );
    assert!(error.diagnostic_truncated());
}
