//! Complete-list ordering, exact status, requester versioning, and secret evidence.

use kafka_wire::{
    DescribeDelegationTokenResponse,
    describe_delegation_token_response::{
        DescribedDelegationToken, DescribedDelegationTokenRenewer,
    },
};
use kafka_wire_core::Bytes;

use super::{
    DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, DescribeDelegationTokenPrincipalRef,
    DescribeDelegationTokensRequestRef, DescribeDelegationTokensResponseFailure,
    NormalizedDescribeDelegationTokensResponse, normalize_describe_delegation_tokens_response,
};

#[test]
fn v1_normalizes_the_complete_list_in_deterministic_order_without_requester() {
    let mut response = DescribeDelegationTokenResponse::default();
    response.throttle_time_ms = 7;
    response.tokens = vec![
        token("User", "zoë", "token-z"),
        token("User", "alice", "token-a"),
    ];
    response.tokens[1].renewers = vec![renewer("User", "z"), renewer("Service", "a")];

    let normalized = normalize(1, all(), &response).expect("valid v1 response");
    let (throttle, code, tokens, retained) = normalized.into_parts();
    assert_eq!((throttle, code), (7, 0));
    assert_eq!(tokens.len(), 2);
    assert!(retained <= DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES);

    let (owner, requester, _, _, _, token_id, hmac, renewers) =
        tokens.into_iter().next().expect("first token").into_parts();
    assert_eq!(owner.into_parts(), ("User".to_owned(), "alice".to_owned()));
    assert_eq!(requester, None);
    assert_eq!(token_id, "token-a");
    assert_eq!(hmac.as_bytes(), b"secret-token-a");
    assert_eq!(
        renewers
            .into_iter()
            .map(|value| value.into_parts())
            .collect::<Vec<_>>(),
        vec![
            ("Service".to_owned(), "a".to_owned()),
            ("User".to_owned(), "z".to_owned()),
        ]
    );
}

#[test]
fn v3_preserves_requester_and_redacts_every_hmac_debug_path() {
    let mut token = token("User", "alice", "token-a");
    token.token_requester_principal_type = "Service".into();
    token.token_requester_principal_name = "issuer".into();
    let mut response = DescribeDelegationTokenResponse::default();
    response.tokens.push(token);

    let normalized = normalize(3, all(), &response).expect("valid v3 response");
    let debug = format!("{normalized:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("secret-token-a"));
    let (_, _, tokens, _) = normalized.into_parts();
    let (_, requester, _, _, _, _, hmac, _) =
        tokens.into_iter().next().expect("token").into_parts();
    assert_eq!(
        requester.map(|value| value.into_parts()),
        Some(("Service".to_owned(), "issuer".to_owned()))
    );
    assert_eq!(hmac.into_bytes(), b"secret-token-a");
}

#[test]
fn selected_owner_correlation_rejects_unexpected_and_duplicate_tokens() {
    let selected = [principal("User", "alice")];
    let request = DescribeDelegationTokensRequestRef::selected(&selected);
    let mut response = DescribeDelegationTokenResponse::default();
    response.tokens.push(token("User", "bob", "token-b"));
    assert_eq!(
        normalize(1, request, &response).err(),
        Some(DescribeDelegationTokensResponseFailure::UnexpectedOwner)
    );

    response.tokens = vec![
        token("User", "alice", "same"),
        token("User", "alice", "same"),
    ];
    assert_eq!(
        normalize(1, request, &response).err(),
        Some(DescribeDelegationTokensResponseFailure::DuplicateToken)
    );
}

#[test]
fn broker_error_and_version_shape_preserve_exact_semantics() {
    let mut response = DescribeDelegationTokenResponse::default();
    response.error_code = -31_234;
    response.throttle_time_ms = 19;
    let normalized = normalize(3, all(), &response).expect("exact broker error");
    let (throttle, code, tokens, _) = normalized.into_parts();
    assert_eq!((throttle, code), (19, -31_234));
    assert!(tokens.is_empty());

    response.tokens.push(token("User", "alice", "secret"));
    assert!(matches!(
        normalize(3, all(), &response),
        Err(DescribeDelegationTokensResponseFailure::TokensWithTopLevelError { actual: 1 })
    ));

    response.error_code = 0;
    response.tokens[0].token_requester_principal_type = "User".into();
    response.tokens[0].token_requester_principal_name = "issuer".into();
    assert_eq!(
        normalize(2, all(), &response).err(),
        Some(DescribeDelegationTokensResponseFailure::UnexpectedRequesterBeforeV3)
    );
}

#[test]
fn missing_version_negative_throttle_malformed_scalar_and_capacity_are_rejected() {
    let mut response = DescribeDelegationTokenResponse::default();
    response.tokens.push(token("User", "alice", "token-a"));
    assert_eq!(
        normalize_describe_delegation_tokens_response(
            None,
            all(),
            &response,
            DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES,
        )
        .err(),
        Some(DescribeDelegationTokensResponseFailure::MissingSelectedVersion)
    );
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize(1, all(), &response).err(),
        Some(DescribeDelegationTokensResponseFailure::NegativeThrottleTime { actual: -1 })
    );
    response.throttle_time_ms = 0;
    response.tokens[0].expiry_timestamp = 9;
    assert!(matches!(
        normalize(1, all(), &response),
        Err(DescribeDelegationTokensResponseFailure::InvalidExpiryTimestamp { .. })
    ));
    response.tokens[0].expiry_timestamp = 20;
    assert!(matches!(
        normalize_describe_delegation_tokens_response(Some(1), all(), &response, 1),
        Err(DescribeDelegationTokensResponseFailure::RetainedBytes { limit: 1, .. })
    ));
}

fn normalize(
    version: i16,
    request: DescribeDelegationTokensRequestRef<'_>,
    response: &DescribeDelegationTokenResponse,
) -> Result<NormalizedDescribeDelegationTokensResponse, DescribeDelegationTokensResponseFailure> {
    normalize_describe_delegation_tokens_response(
        Some(version),
        request,
        response,
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES,
    )
}

const fn all<'a>() -> DescribeDelegationTokensRequestRef<'a> {
    DescribeDelegationTokensRequestRef::all()
}

fn principal<'a>(
    principal_type: &'a str,
    principal_name: &'a str,
) -> DescribeDelegationTokenPrincipalRef<'a> {
    DescribeDelegationTokenPrincipalRef::new(principal_type, principal_name)
}

fn renewer(principal_type: &str, principal_name: &str) -> DescribedDelegationTokenRenewer {
    let mut renewer = DescribedDelegationTokenRenewer::default();
    renewer.principal_type = principal_type.to_owned().into();
    renewer.principal_name = principal_name.to_owned().into();
    renewer
}

fn token(principal_type: &str, principal_name: &str, token_id: &str) -> DescribedDelegationToken {
    let mut token = DescribedDelegationToken::default();
    token.principal_type = principal_type.to_owned().into();
    token.principal_name = principal_name.to_owned().into();
    token.issue_timestamp = 10;
    token.expiry_timestamp = 20;
    token.max_timestamp = 30;
    token.token_id = token_id.to_owned().into();
    token.hmac = Bytes::from(format!("secret-{token_id}").into_bytes());
    token
}
