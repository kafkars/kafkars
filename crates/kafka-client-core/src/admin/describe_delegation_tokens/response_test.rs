//! Complete token validation and secret-safety scenarios.

use super::{
    super::{DelegationTokenHmac, DelegationTokenPrincipal},
    DescribeDelegationTokenResponse, DescribeDelegationTokenResponseError,
    DescribeDelegationTokensResponse,
};

#[test]
fn response_preserves_complete_v3_and_legacy_token_facts() {
    let modern = token(
        principal("User", "owner"),
        Some(principal("User", "requester")),
        vec![principal("Service", "renewer")],
        "token-modern",
        b"modern-secret",
    );
    let legacy = token(
        principal("User", "legacy"),
        None,
        Vec::new(),
        "token-legacy",
        b"legacy-secret",
    );
    let response = DescribeDelegationTokensResponse::new(19, vec![modern, legacy])
        .unwrap_or_else(|error| panic!("response: {error}"));
    let (throttle, tokens) = response.into_parts();
    assert_eq!(throttle, 19);
    let (owner, requester, renewers, issue, expiry, maximum, token_id, hmac) = tokens
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("token"))
        .into_parts();
    assert_eq!(owner.principal_name(), "owner");
    assert_eq!(
        requester.map(|value| value.into_parts()),
        Some(("User".to_owned(), "requester".to_owned()))
    );
    assert_eq!(renewers[0].principal_name(), "renewer");
    assert_eq!((issue, expiry, maximum), (10, 20, 30));
    assert_eq!(token_id, "token-modern");
    assert_eq!(hmac.as_bytes(), b"modern-secret");
}

#[test]
fn diagnostics_redact_nested_hmac_and_secret_owns_drop_path() {
    let response = token(
        principal("User", "owner"),
        None,
        Vec::new(),
        "token",
        b"nested-secret-must-not-leak",
    );
    let diagnostic = format!("{response:?}");

    assert!(diagnostic.contains("redacted"));
    assert!(!diagnostic.contains("nested-secret-must-not-leak"));
    assert!(std::mem::needs_drop::<DelegationTokenHmac>());
}

#[test]
fn invalid_timing_identity_and_renewers_are_rejected() {
    assert_eq!(
        raw_token(-1, 20, 30, "token", Vec::new()),
        Err(DescribeDelegationTokenResponseError::NegativeIssueTimestamp)
    );
    assert_eq!(
        raw_token(20, 10, 30, "token", Vec::new()),
        Err(DescribeDelegationTokenResponseError::ExpiryBeforeIssue)
    );
    assert_eq!(
        raw_token(10, 30, 20, "token", Vec::new()),
        Err(DescribeDelegationTokenResponseError::MaxBeforeExpiry)
    );
    assert_eq!(
        raw_token(10, 20, 30, "", Vec::new()),
        Err(DescribeDelegationTokenResponseError::EmptyTokenId)
    );
    assert_eq!(
        raw_token(
            10,
            20,
            30,
            "token",
            vec![principal("User", "renewer"), principal("User", "renewer"),],
        ),
        Err(DescribeDelegationTokenResponseError::DuplicateRenewer)
    );
}

fn raw_token(
    issue: i64,
    expiry: i64,
    maximum: i64,
    token_id: &str,
    renewers: Vec<DelegationTokenPrincipal>,
) -> Result<DescribeDelegationTokenResponse, DescribeDelegationTokenResponseError> {
    DescribeDelegationTokenResponse::new(
        principal("User", "owner"),
        None,
        renewers,
        issue,
        expiry,
        maximum,
        token_id.to_owned(),
        hmac(b"secret"),
    )
}

fn token(
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    token_id: &str,
    secret: &[u8],
) -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse::new(
        owner,
        requester,
        renewers,
        10,
        20,
        30,
        token_id.to_owned(),
        hmac(secret),
    )
    .unwrap_or_else(|error| panic!("token: {error}"))
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}

fn hmac(secret: &[u8]) -> DelegationTokenHmac {
    DelegationTokenHmac::new(secret.to_vec()).unwrap_or_else(|error| panic!("hmac: {error}"))
}
