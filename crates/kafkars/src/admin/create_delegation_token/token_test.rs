//! Complete public delegation-token accessor and redaction scenarios.

use super::{DelegationToken, DelegationTokenHmac, DelegationTokenPrincipal};

#[test]
fn token_preserves_complete_identity_timing_and_secret_facts() {
    let token = DelegationToken::new(
        principal("User", "alice"),
        Some(principal("User", "requester")),
        vec![principal("Service", "renewer-a")],
        11,
        22,
        33,
        "token-7".to_owned(),
        DelegationTokenHmac::new(vec![1, 2, 3]),
    );

    assert_eq!(token.owner().principal_name(), "alice");
    assert_eq!(
        token
            .requester()
            .map(DelegationTokenPrincipal::principal_name),
        Some("requester")
    );
    assert_eq!(token.renewers()[0].principal_name(), "renewer-a");
    assert_eq!(token.issue_timestamp_ms(), 11);
    assert_eq!(token.expiry_timestamp_ms(), 22);
    assert_eq!(token.max_timestamp_ms(), 33);
    assert_eq!(token.token_id(), "token-7");
    assert_eq!(token.hmac().as_bytes(), [1, 2, 3]);
}

#[test]
fn token_debug_redacts_the_nested_hmac() {
    let token = DelegationToken::new(
        principal("User", "alice"),
        None,
        Vec::new(),
        1,
        2,
        3,
        "token-7".to_owned(),
        DelegationTokenHmac::new(b"nested-secret-must-not-leak".to_vec()),
    );

    let diagnostic = format!("{token:?}");
    assert!(diagnostic.contains("token-7"));
    assert!(diagnostic.contains("redacted"));
    assert!(!diagnostic.contains("nested-secret-must-not-leak"));
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type, principal_name)
}
