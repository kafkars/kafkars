//! Inert secret ownership and renewal-period intent evidence.

use super::{
    RenewDelegationTokenHmac, RenewDelegationTokenRequest, model::RenewDelegationTokenPlanFailure,
};

#[test]
fn request_preserves_unique_hmac_and_optional_period_without_leaking_secret() {
    let request = RenewDelegationTokenRequest::new(
        RenewDelegationTokenHmac::new(b"renew-secret-must-not-leak".to_vec()),
        Some(60_000),
    );

    assert_eq!(request.hmac().as_bytes(), b"renew-secret-must-not-leak");
    assert_eq!(request.renew_period_ms(), Some(60_000));
    let debug = format!("{request:?}");
    assert!(debug.contains("REDACTED"));
    assert!(!debug.contains("renew-secret-must-not-leak"));

    let (hmac, period) = request.into_parts();
    assert_eq!(period, Some(60_000));
    assert_eq!(hmac.into_bytes(), b"renew-secret-must-not-leak");
}

#[test]
fn absent_period_remains_distinct_for_later_broker_default_conversion() {
    let request = RenewDelegationTokenRequest::new(RenewDelegationTokenHmac::new(vec![1]), None);

    assert_eq!(request.renew_period_ms(), None);
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("broker-default plan: {error:?}"));
    assert_eq!(plan.renew_period_ms(), None);
    assert_eq!(plan.broker_renew_period_ms(), -1);
}

#[test]
fn captured_conversion_rejects_empty_zero_and_unrepresentable_values() {
    for request in [
        RenewDelegationTokenRequest::new(RenewDelegationTokenHmac::new(Vec::new()), None),
        RenewDelegationTokenRequest::new(RenewDelegationTokenHmac::new(vec![1]), Some(0)),
        RenewDelegationTokenRequest::new(
            RenewDelegationTokenHmac::new(vec![1]),
            Some(i64::MAX as u64 + 1),
        ),
    ] {
        assert!(matches!(
            request.into_plan(),
            Err(RenewDelegationTokenPlanFailure::Invalid)
        ));
    }
}

#[test]
fn unique_hmac_zeroizes_without_exposing_bytes() {
    let mut hmac = RenewDelegationTokenHmac::new(b"ephemeral-secret".to_vec());
    hmac.zeroize_for_test();

    assert!(hmac.as_bytes().is_empty());
}
