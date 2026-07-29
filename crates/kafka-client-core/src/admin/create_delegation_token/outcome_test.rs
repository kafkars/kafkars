//! Complete token, secret redaction, validation, and exact error scenarios.

use core::num::NonZeroI16;

use super::{
    CREATE_DELEGATION_TOKEN_MAX_HMAC_BYTES, CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES,
    CreateDelegationTokenBrokerError, CreateDelegationTokenResponse,
    CreateDelegationTokenResponseError, DelegationTokenHmac, DelegationTokenPrincipal,
};

#[test]
fn successful_response_preserves_complete_broker_facts() {
    let response = response();

    assert_eq!(response.throttle_time_ms(), 17);
    assert_eq!(response.owner().principal_name(), "alice");
    let (throttle, owner, requester, issue, expiry, maximum, token_id, hmac) =
        response.into_parts();
    assert_eq!(throttle, 17);
    assert_eq!(owner.principal_name(), "alice");
    assert_eq!(
        requester
            .as_ref()
            .map(DelegationTokenPrincipal::principal_name),
        Some("operator")
    );
    assert_eq!((issue, expiry, maximum), (100, 200, 300));
    assert_eq!(token_id, "token-1");
    assert_eq!(hmac.as_bytes(), &[1, 2, 3, 4]);
}

#[test]
fn token_hmac_is_noncloneable_by_shape_redacted_and_zeroizable() {
    let mut hmac = DelegationTokenHmac::new(b"do-not-print".to_vec())
        .unwrap_or_else(|error| panic!("hmac: {error}"));

    let debug = format!("{hmac:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("do-not-print"));

    hmac.zeroize_for_test();
    assert!(hmac.as_bytes().iter().all(|byte| *byte == 0));
}

#[test]
fn successful_response_rejects_invalid_timestamps_and_token_identity() {
    for (issue, expiry, maximum, token_id, expected) in [
        (
            -1,
            2,
            3,
            "token".to_owned(),
            CreateDelegationTokenResponseError::NegativeIssueTimestamp,
        ),
        (
            1,
            -1,
            3,
            "token".to_owned(),
            CreateDelegationTokenResponseError::NegativeExpiryTimestamp,
        ),
        (
            1,
            2,
            -1,
            "token".to_owned(),
            CreateDelegationTokenResponseError::NegativeMaxTimestamp,
        ),
        (
            2,
            1,
            3,
            "token".to_owned(),
            CreateDelegationTokenResponseError::ExpiryBeforeIssue,
        ),
        (
            1,
            3,
            2,
            "token".to_owned(),
            CreateDelegationTokenResponseError::MaxBeforeExpiry,
        ),
        (
            1,
            2,
            3,
            String::new(),
            CreateDelegationTokenResponseError::EmptyTokenId,
        ),
        (
            1,
            2,
            3,
            "x".repeat(CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES + 1),
            CreateDelegationTokenResponseError::TokenIdTooLong,
        ),
    ] {
        assert_eq!(
            CreateDelegationTokenResponse::new(
                0,
                principal("alice"),
                None,
                issue,
                expiry,
                maximum,
                token_id,
                hmac(),
            ),
            Err(expected)
        );
    }
}

#[test]
fn token_hmac_is_nonempty_and_bounded() {
    assert_eq!(
        DelegationTokenHmac::new(Vec::new()),
        Err(CreateDelegationTokenResponseError::EmptyHmac)
    );
    assert_eq!(
        DelegationTokenHmac::new(vec![1; CREATE_DELEGATION_TOKEN_MAX_HMAC_BYTES + 1]),
        Err(CreateDelegationTokenResponseError::HmacTooLong)
    );
}

#[test]
fn broker_error_preserves_nonnegative_throttle_and_signed_code() {
    let error = CreateDelegationTokenBrokerError::new(
        23,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.into_parts(), (23, -32_000));
}

fn response() -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse::new(
        17,
        principal("alice"),
        Some(principal("operator")),
        100,
        200,
        300,
        "token-1".to_owned(),
        hmac(),
    )
    .unwrap_or_else(|error| panic!("response: {error}"))
}

fn principal(name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new("User".to_owned(), name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}

fn hmac() -> DelegationTokenHmac {
    DelegationTokenHmac::new(vec![1, 2, 3, 4]).unwrap_or_else(|error| panic!("hmac: {error}"))
}
