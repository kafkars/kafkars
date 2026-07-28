//! Stable partition-reassignment broker-error translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::alter_result::translate_broker_parts;

#[test]
fn broker_diagnostic_preserves_signed_code_truncation_and_delivery() {
    let error = translate_broker_parts(
        "partition",
        -31_998,
        Some("partition diagnostic".to_owned()),
        true,
        DeliveryStatus::PossiblySent,
    );

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_998));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
    assert!(error.to_string().contains("partition diagnostic"));
    assert!(error.to_string().contains("truncated"));
}
