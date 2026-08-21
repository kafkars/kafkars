//! Secret transfer and renewal-period translation evidence.

use std::time::Duration;

use crate::admin::DelegationTokenHmac;

use super::RenewDelegationTokenAdminRequest;

#[test]
fn omitted_period_and_exact_secret_transfer_after_capture() {
    let request =
        RenewDelegationTokenAdminRequest::new(secret(b"secret-renew-hmac"), None).into_engine();

    assert_eq!(request.hmac().as_bytes(), b"secret-renew-hmac");
    assert_eq!(request.renew_period_ms(), None);
    let debug = format!("{request:?}");
    assert!(!debug.contains("secret-renew-hmac"));
}

#[test]
fn invalid_periods_remain_inert_for_engine_validation() {
    let zero =
        RenewDelegationTokenAdminRequest::new(secret(b"hmac"), Some(Duration::ZERO)).into_engine();
    assert_eq!(zero.renew_period_ms(), Some(0));

    let explicit = RenewDelegationTokenAdminRequest::new(
        secret(b"hmac"),
        Some(Duration::from_millis(86_400_000)),
    )
    .into_engine();
    assert_eq!(explicit.renew_period_ms(), Some(86_400_000));
}

fn secret(bytes: &[u8]) -> DelegationTokenHmac {
    DelegationTokenHmac::new(bytes.to_vec())
}
