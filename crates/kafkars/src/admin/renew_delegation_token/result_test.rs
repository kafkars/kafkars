//! Stable renewal result scalar evidence.

use std::time::Duration;

use super::RenewDelegationTokenResult;

#[test]
fn result_preserves_throttle_and_exact_expiry_timestamp() {
    let result = RenewDelegationTokenResult::new(Duration::from_millis(17), 1_700_000_000_123);

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.expiry_timestamp_ms(), 1_700_000_000_123);
    assert_eq!(
        result.into_parts(),
        (Duration::from_millis(17), 1_700_000_000_123)
    );
}
