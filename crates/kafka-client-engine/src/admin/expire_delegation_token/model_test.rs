//! Inert secret ownership and expiration-period intent evidence.

use super::{
    ExpireDelegationTokenHmac, ExpireDelegationTokenRequest,
    model::ExpireDelegationTokenPlanFailure,
};

#[test]
fn request_preserves_unique_hmac_and_optional_period_without_leaking_secret() {
    let request = ExpireDelegationTokenRequest::new(
        ExpireDelegationTokenHmac::new(b"expire-secret-must-not-leak".to_vec()),
        Some(60_000),
    );

    assert_eq!(request.hmac().as_bytes(), b"expire-secret-must-not-leak");
    assert_eq!(request.expiry_period_ms(), Some(60_000));
    let debug = format!("{request:?}");
    assert!(debug.contains("REDACTED"));
    assert!(!debug.contains("expire-secret-must-not-leak"));

    let (hmac, period) = request.into_parts();
    assert_eq!(period, Some(60_000));
    assert_eq!(hmac.into_bytes(), b"expire-secret-must-not-leak");
}

#[test]
fn absent_period_becomes_the_exact_immediate_sentinel_only_in_core() {
    let request = ExpireDelegationTokenRequest::new(ExpireDelegationTokenHmac::new(vec![1]), None);

    assert_eq!(request.expiry_period_ms(), None);
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("immediate plan: {error:?}"));
    assert_eq!(plan.expiry_period_ms(), None);
    assert_eq!(plan.broker_expiry_period_ms(), -1);
}

#[test]
fn captured_conversion_accepts_zero_but_rejects_empty_and_unrepresentable_values() {
    for request in [
        ExpireDelegationTokenRequest::new(ExpireDelegationTokenHmac::new(Vec::new()), None),
        ExpireDelegationTokenRequest::new(
            ExpireDelegationTokenHmac::new(vec![1]),
            Some(i64::MAX as u64 + 1),
        ),
    ] {
        assert!(matches!(
            request.into_plan(),
            Err(ExpireDelegationTokenPlanFailure::Invalid)
        ));
    }

    let zero = ExpireDelegationTokenRequest::new(ExpireDelegationTokenHmac::new(vec![1]), Some(0))
        .into_plan()
        .unwrap_or_else(|error| panic!("zero-period plan: {error:?}"));
    assert_eq!(zero.expiry_period_ms(), Some(0));
    assert_eq!(zero.broker_expiry_period_ms(), 0);

    let maximum = ExpireDelegationTokenRequest::new(
        ExpireDelegationTokenHmac::new(vec![1]),
        Some(i64::MAX as u64),
    )
    .into_plan()
    .unwrap_or_else(|error| panic!("maximum-period plan: {error:?}"));
    assert_eq!(maximum.expiry_period_ms(), Some(i64::MAX));
    assert_eq!(maximum.broker_expiry_period_ms(), i64::MAX);
}

#[test]
fn unique_hmac_zeroizes_without_exposing_bytes() {
    let mut hmac = ExpireDelegationTokenHmac::new(b"ephemeral-secret".to_vec());
    hmac.zeroize_for_test();

    assert!(hmac.as_bytes().is_empty());
}
