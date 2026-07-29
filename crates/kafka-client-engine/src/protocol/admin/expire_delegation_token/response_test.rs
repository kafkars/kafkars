//! Exact status, expiry, version, malformed scalar, and capacity evidence.

use kafka_wire::ExpireDelegationTokenResponse;

use super::{
    EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, ExpireDelegationTokenResponseFailure,
    normalize_expire_delegation_token_response,
};

#[test]
fn v1_and_v2_success_preserve_nonnegative_expiry_and_throttle() {
    for version in [1, 2] {
        let mut response = ExpireDelegationTokenResponse::default();
        response.expiry_timestamp_ms = 1_700_003_600_002;
        response.throttle_time_ms = 17;
        let normalized = normalize(Some(version), &response).expect("valid expiration response");

        let (throttle, code, expiry, retained) = normalized.into_parts();
        assert_eq!(throttle, 17);
        assert_eq!(code, 0);
        assert_eq!(expiry, Some(1_700_003_600_002));
        assert!(retained > 0);
        assert!(retained <= EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    }
}

#[test]
fn broker_rejection_preserves_exact_signed_code_and_ignores_expiry_sentinel() {
    let mut response = ExpireDelegationTokenResponse::default();
    response.error_code = -31_234;
    response.expiry_timestamp_ms = -1;
    response.throttle_time_ms = 29;

    let normalized = normalize(Some(2), &response).expect("exact broker rejection");
    assert_eq!(normalized.into_parts(), (29, -31_234, None, retained()));
}

#[test]
fn missing_unsupported_and_malformed_scalars_are_distinct() {
    let response = ExpireDelegationTokenResponse::default();
    assert_eq!(
        normalize(None, &response).err(),
        Some(ExpireDelegationTokenResponseFailure::MissingSelectedVersion)
    );
    for version in [0, 3] {
        assert_eq!(
            normalize(Some(version), &response).err(),
            Some(ExpireDelegationTokenResponseFailure::UnsupportedApiVersion { actual: version })
        );
    }

    let mut negative_throttle = response.clone();
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize(Some(1), &negative_throttle).err(),
        Some(ExpireDelegationTokenResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    let mut negative_expiry = response;
    negative_expiry.expiry_timestamp_ms = -1;
    assert_eq!(
        normalize(Some(2), &negative_expiry).err(),
        Some(ExpireDelegationTokenResponseFailure::InvalidExpiryTimestamp { actual: -1 })
    );
}

#[test]
fn caller_capacity_is_honored_below_the_absolute_envelope() {
    assert_eq!(
        normalize_expire_delegation_token_response(
            Some(1),
            &ExpireDelegationTokenResponse::default(),
            retained() - 1,
        )
        .err(),
        Some(ExpireDelegationTokenResponseFailure::RetainedBytes {
            required: retained(),
            limit: retained() - 1,
        })
    );
}

fn normalize(
    version: Option<i16>,
    response: &ExpireDelegationTokenResponse,
) -> Result<super::NormalizedExpireDelegationTokenResponse, ExpireDelegationTokenResponseFailure> {
    normalize_expire_delegation_token_response(
        version,
        response,
        EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    )
}

const fn retained() -> usize {
    core::mem::size_of::<super::NormalizedExpireDelegationTokenResponse>()
}
