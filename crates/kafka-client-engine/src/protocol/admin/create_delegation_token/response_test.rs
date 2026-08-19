//! Signed status, version shape, secret safety, scalar, and capacity evidence.

use kafka_wire::CreateDelegationTokenResponse;
use kafka_wire_core::Bytes;

use super::{
    CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, CreateDelegationTokenResponseFailure,
    normalize_create_delegation_token_response,
    retention::{MAX_HMAC_BYTES, MAX_TOKEN_ID_BYTES},
};

#[test]
fn v1_success_owns_secret_bytes_without_inventing_requester_identity() {
    let response = success();
    let normalized =
        normalize(1, &response).unwrap_or_else(|error| panic!("valid v1 success: {error:?}"));
    let (throttle, code, token, retained) = normalized.into_parts();
    let token = token.unwrap_or_else(|| panic!("success token"));
    let (owner, requester, issue, expiry, max, token_id, hmac) = token.into_parts();

    assert_eq!(throttle, 7);
    assert_eq!(code, 0);
    assert_eq!(owner.into_parts(), ("User".to_owned(), "owner".to_owned()));
    assert_eq!(requester, None);
    assert_eq!((issue, expiry, max), (10, 20, 30));
    assert_eq!(token_id, "token-id");
    assert_eq!(hmac.as_bytes(), b"secret-hmac");
    assert!(retained > 0);
    assert!(retained <= CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
}

#[test]
fn v3_success_preserves_distinct_requester_and_redacts_hmac_debug() {
    let mut response = success();
    response.token_requester_principal_type = "User".into();
    response.token_requester_principal_name = "requester".into();
    let normalized =
        normalize(3, &response).unwrap_or_else(|error| panic!("valid v3 success: {error:?}"));

    let debug = format!("{normalized:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("secret-hmac"));

    let (_, _, token, _) = normalized.into_parts();
    let (_, requester, _, _, _, _, hmac) = token
        .unwrap_or_else(|| panic!("success token"))
        .into_parts();
    assert_eq!(
        requester.map(super::model::NormalizedDelegationTokenPrincipal::into_parts),
        Some(("User".to_owned(), "requester".to_owned()))
    );
    assert_eq!(hmac.into_bytes(), b"secret-hmac");
}

#[test]
fn broker_error_preserves_exact_signed_code_and_nonnegative_throttle() {
    let mut response = CreateDelegationTokenResponse::default();
    response.error_code = -31_234;
    response.throttle_time_ms = 19;
    response.hmac = Bytes::from(vec![7; MAX_HMAC_BYTES + 1]);

    let normalized =
        normalize(3, &response).unwrap_or_else(|error| panic!("exact broker error: {error:?}"));
    let (throttle, code, token, retained) = normalized.into_parts();

    assert_eq!(throttle, 19);
    assert_eq!(code, -31_234);
    assert!(token.is_none());
    assert_eq!(
        retained,
        core::mem::size_of::<super::NormalizedCreateDelegationTokenResponse>()
    );
}

#[test]
fn missing_or_unsupported_version_and_negative_throttle_are_rejected() {
    let response = success();
    assert_eq!(
        normalize_create_delegation_token_response(
            None,
            &response,
            CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        )
        .err(),
        Some(CreateDelegationTokenResponseFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_create_delegation_token_response(
            Some(4),
            &response,
            CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        )
        .err(),
        Some(CreateDelegationTokenResponseFailure::UnsupportedApiVersion { actual: 4 })
    );
    let mut negative = response;
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize(1, &negative).err(),
        Some(CreateDelegationTokenResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn success_requires_bounded_identity_token_secret_and_ordered_timestamps() {
    let mut response = success();
    response.token_id = "".into();
    assert_eq!(
        normalize(1, &response).err(),
        Some(CreateDelegationTokenResponseFailure::EmptyTokenId)
    );

    response = success();
    response.token_id = "x".repeat(MAX_TOKEN_ID_BYTES + 1).into();
    assert!(matches!(
        normalize(1, &response),
        Err(CreateDelegationTokenResponseFailure::TokenIdTooLong { .. })
    ));

    response = success();
    response.hmac = Bytes::new();
    assert_eq!(
        normalize(1, &response).err(),
        Some(CreateDelegationTokenResponseFailure::EmptyHmac)
    );

    response = success();
    response.expiry_timestamp_ms = 9;
    assert_eq!(
        normalize(1, &response).err(),
        Some(
            CreateDelegationTokenResponseFailure::InvalidExpiryTimestamp {
                issue: 10,
                expiry: 9,
            }
        )
    );
}

#[test]
fn caller_capacity_is_honored_below_the_absolute_ceiling() {
    assert!(matches!(
        normalize_create_delegation_token_response(Some(1), &success(), 1),
        Err(CreateDelegationTokenResponseFailure::RetainedBytes { limit: 1, .. })
    ));
}

fn normalize(
    version: i16,
    response: &CreateDelegationTokenResponse,
) -> Result<super::NormalizedCreateDelegationTokenResponse, CreateDelegationTokenResponseFailure> {
    normalize_create_delegation_token_response(
        Some(version),
        response,
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    )
}

fn success() -> CreateDelegationTokenResponse {
    let mut response = CreateDelegationTokenResponse::default();
    response.principal_type = "User".into();
    response.principal_name = "owner".into();
    response.issue_timestamp_ms = 10;
    response.expiry_timestamp_ms = 20;
    response.max_timestamp_ms = 30;
    response.token_id = "token-id".into();
    response.hmac = Bytes::from_static(b"secret-hmac");
    response.throttle_time_ms = 7;
    response
}
