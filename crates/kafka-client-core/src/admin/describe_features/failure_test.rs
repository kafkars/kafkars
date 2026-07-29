//! Exact broker rejection and mechanism-failure scalar scenarios.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{DescribeFeaturesBrokerError, DescribeFeaturesFailure, DescribeFeaturesFailureKind};

#[test]
fn broker_error_preserves_throttle_and_unknown_signed_code() {
    let error = DescribeFeaturesBrokerError::new(
        17,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero code")),
    );
    assert_eq!(error.throttle_time_ms(), 17);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.into_parts(), (17, -32_000));
}

#[test]
fn mechanism_failure_preserves_kind_and_delivery() {
    let failure = DescribeFeaturesFailure::new(
        DescribeFeaturesFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), DescribeFeaturesFailureKind::Transport);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
