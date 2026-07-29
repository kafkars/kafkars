//! Successful scalar result evidence.

use super::ExpireDelegationTokenResult;

#[test]
fn success_preserves_throttle_and_expiry_timestamp() {
    let result = ExpireDelegationTokenResult {
        throttle_time_ms: 17,
        expiry_timestamp_ms: 1_234_567,
    };

    assert_eq!(result.throttle_time_ms(), 17);
    assert_eq!(result.expiry_timestamp_ms(), 1_234_567);
    assert_eq!(result.into_parts(), (17, 1_234_567));
}
