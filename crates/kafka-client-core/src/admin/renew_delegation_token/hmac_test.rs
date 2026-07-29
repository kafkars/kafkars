//! Secret validation, redaction, transfer, and zeroization scenarios.

use super::{
    RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES, RenewDelegationTokenHmac, RenewDelegationTokenPlanError,
};

#[test]
fn hmac_is_nonempty_bounded_and_diagnostic_safe() {
    assert_eq!(
        RenewDelegationTokenHmac::new(Vec::new()),
        Err(RenewDelegationTokenPlanError::EmptyHmac)
    );
    assert_eq!(
        RenewDelegationTokenHmac::new(vec![1; RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES + 1]),
        Err(RenewDelegationTokenPlanError::HmacTooLong)
    );

    let hmac = RenewDelegationTokenHmac::new(b"renew-secret-must-not-leak".to_vec())
        .unwrap_or_else(|error| panic!("hmac: {error}"));
    assert_eq!(hmac.len(), 26);
    assert!(!hmac.is_empty());
    let debug = format!("{hmac:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("renew-secret-must-not-leak"));
}

#[test]
fn hmac_zeroizes_in_place_and_transfers_unique_bytes() {
    let mut hmac = RenewDelegationTokenHmac::new(vec![1, 2, 3, 4])
        .unwrap_or_else(|error| panic!("hmac: {error}"));
    hmac.zeroize_for_test();
    assert_eq!(hmac.as_bytes(), &[0, 0, 0, 0]);

    let hmac = RenewDelegationTokenHmac::new(vec![5, 6, 7])
        .unwrap_or_else(|error| panic!("hmac: {error}"));
    assert_eq!(hmac.into_bytes(), vec![5, 6, 7]);
}
