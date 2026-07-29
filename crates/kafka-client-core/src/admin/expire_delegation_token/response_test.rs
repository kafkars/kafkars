//! Successful expiration response validation scenarios.

use super::{ExpireDelegationTokenResponse, ExpireDelegationTokenResponseError};

#[test]
fn response_preserves_nonnegative_throttle_and_expiry() {
    let response = ExpireDelegationTokenResponse::new(17, 9_999)
        .unwrap_or_else(|error| panic!("response: {error}"));

    assert_eq!(response.throttle_time_ms(), 17);
    assert_eq!(response.expiry_timestamp_ms(), 9_999);
    assert_eq!(response.into_parts(), (17, 9_999));
}

#[test]
fn response_rejects_negative_expiry() {
    assert_eq!(
        ExpireDelegationTokenResponse::new(0, -1),
        Err(ExpireDelegationTokenResponseError::NegativeExpiryTimestamp)
    );
}
