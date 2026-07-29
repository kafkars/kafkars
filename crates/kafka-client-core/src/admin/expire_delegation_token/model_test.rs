//! Immediate-expiry sentinel and explicit grace-period validation scenarios.

use super::{ExpireDelegationTokenHmac, ExpireDelegationTokenPlan, ExpireDelegationTokenPlanError};

#[test]
fn absent_period_preserves_the_exact_immediate_expiry_sentinel() {
    let plan = ExpireDelegationTokenPlan::new(hmac(), None)
        .unwrap_or_else(|error| panic!("default plan: {error}"));

    assert_eq!(plan.expiry_period_ms(), None);
    assert_eq!(plan.broker_expiry_period_ms(), -1);
    assert_eq!(plan.hmac().as_bytes(), &[1, 2, 3, 4]);
    let (hmac, period) = plan.into_parts();
    assert_eq!(hmac.as_bytes(), &[1, 2, 3, 4]);
    assert_eq!(period, -1);
}

#[test]
fn explicit_period_must_be_nonnegative_and_preserves_the_signed_domain() {
    for invalid in [i64::MIN, -2, -1] {
        assert_eq!(
            ExpireDelegationTokenPlan::new(hmac(), Some(invalid)),
            Err(ExpireDelegationTokenPlanError::NegativeExpiryPeriod)
        );
    }

    for valid in [0, 1, 86_400_000, i64::MAX] {
        let plan = ExpireDelegationTokenPlan::new(hmac(), Some(valid))
            .unwrap_or_else(|error| panic!("explicit plan: {error}"));
        assert_eq!(plan.expiry_period_ms(), Some(valid));
        assert_eq!(plan.broker_expiry_period_ms(), valid);
    }
}

fn hmac() -> ExpireDelegationTokenHmac {
    ExpireDelegationTokenHmac::new(vec![1, 2, 3, 4]).unwrap_or_else(|error| panic!("hmac: {error}"))
}
