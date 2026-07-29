//! Successful and exact broker-rejection value scenarios.

use core::num::NonZeroI16;

use super::{UnregisterBrokerBrokerError, UnregisterBrokerSuccess};

#[test]
fn success_preserves_nonnegative_throttle_observation() {
    let success = UnregisterBrokerSuccess::new(17);

    assert_eq!(success.throttle_time_ms(), 17);
}

#[test]
fn broker_error_preserves_throttle_signed_code_and_nullable_diagnostic() {
    let error = UnregisterBrokerBrokerError::new(
        23,
        NonZeroI16::new(-41).unwrap_or_else(|| panic!("nonzero")),
        Some("broker still registered".to_owned()),
        true,
    );

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -41);
    assert_eq!(error.message(), Some("broker still registered"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (23, -41, Some("broker still registered".to_owned()), true)
    );
}
