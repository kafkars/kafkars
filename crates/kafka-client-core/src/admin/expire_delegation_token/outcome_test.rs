//! Exact signed broker-rejection scenarios.

use core::num::NonZeroI16;

use super::ExpireDelegationTokenBrokerError;

#[test]
fn broker_error_preserves_nonnegative_throttle_and_signed_code() {
    let error = ExpireDelegationTokenBrokerError::new(
        23,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.into_parts(), (23, -32_000));
}
