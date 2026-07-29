//! Stable ShareGroup offset-listing error translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{DeliveryStatus as EngineDeliveryStatus, FailureKind},
    result::{translate_failure_parts, translate_partition_error_parts},
};

#[test]
fn partition_error_preserves_signed_code_diagnostic_and_truncation() {
    let error = translate_partition_error_parts(-31_000, Some("future partition diagnostic"), true);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_000));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
    assert!(error.to_string().contains("future partition diagnostic"));
}

#[test]
fn failure_preserves_delivery_certainty() {
    let error = translate_failure_parts(
        FailureKind::DeadlineElapsed,
        EngineDeliveryStatus::PossiblySent,
    );

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
}
