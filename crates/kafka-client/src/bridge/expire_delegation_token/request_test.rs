//! Secret transfer and expiry-period translation evidence.

use std::time::Duration;

use crate::admin::DelegationTokenHmac;

use super::ExpireDelegationTokenAdminRequest;

#[test]
fn omitted_period_and_exact_secret_transfer_after_capture() {
    let request =
        ExpireDelegationTokenAdminRequest::new(secret(b"secret-expire-hmac"), None).into_engine();

    assert_eq!(request.hmac().as_bytes(), b"secret-expire-hmac");
    assert_eq!(request.expiry_period_ms(), None);
    let debug = format!("{request:?}");
    assert!(!debug.contains("secret-expire-hmac"));
}

#[test]
fn zero_and_explicit_periods_remain_exact_for_engine_validation() {
    let zero =
        ExpireDelegationTokenAdminRequest::new(secret(b"hmac"), Some(Duration::ZERO)).into_engine();
    assert_eq!(zero.expiry_period_ms(), Some(0));

    let explicit = ExpireDelegationTokenAdminRequest::new(
        secret(b"hmac"),
        Some(Duration::from_millis(86_400_000)),
    )
    .into_engine();
    assert_eq!(explicit.expiry_period_ms(), Some(86_400_000));
}

fn secret(bytes: &[u8]) -> DelegationTokenHmac {
    DelegationTokenHmac::new(bytes.to_vec())
}
