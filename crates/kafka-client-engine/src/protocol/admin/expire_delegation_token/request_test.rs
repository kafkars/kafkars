//! API contract, exact period encoding, secret safety, and capacity evidence.

use kafka_wire::{ExpireDelegationTokenRequest, KafkaMessage, KafkaRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, ExpireDelegationTokenRequestFailure,
    ExpireDelegationTokenRequestRef, PreparedExpireDelegationTokenRequest,
    expire_delegation_token_request, retention::MAX_HMAC_BYTES,
};

#[test]
fn generated_contract_is_api_40_v1_through_v2_with_flexible_v2() {
    assert_eq!(
        <ExpireDelegationTokenRequest as KafkaRequest>::API_KEY.value(),
        40
    );
    assert!(ExpireDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
    assert!(ExpireDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(2)));
    assert!(!ExpireDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!ExpireDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
    assert_eq!(
        ExpireDelegationTokenRequest::FLEXIBLE_VERSIONS,
        Some(kafka_wire_core::VersionRange::new(2, 2))
    );
}

#[test]
fn immediate_is_exact_minus_one_in_v1_and_v2() {
    let request = prepare(ExpireDelegationTokenRequestRef::immediate(
        b"immediate-expiration-secret",
    ));
    for version in [1, 2] {
        let decoded = round_trip(&request, version);
        assert_eq!(decoded.hmac.as_ref(), b"immediate-expiration-secret");
        assert_eq!(decoded.expiry_time_period_ms, -1);
        assert!(decoded.unknown_tagged_fields.is_empty());
    }
}

#[test]
fn explicit_nonnegative_period_is_exact_in_v1_and_v2() {
    for period in [0, 86_400_123] {
        let request = prepare(ExpireDelegationTokenRequestRef::explicit(
            b"explicit-expiration-secret",
            period,
        ));
        for version in [1, 2] {
            let decoded = round_trip(&request, version);
            assert_eq!(decoded.hmac.as_ref(), b"explicit-expiration-secret");
            assert_eq!(decoded.expiry_time_period_ms, period);
        }
    }
}

#[test]
fn secret_owner_is_nontrivial_redacted_and_zeroizing() {
    let mut request = prepare(ExpireDelegationTokenRequestRef::immediate(
        b"expire-secret-must-not-leak",
    ));
    assert!(core::mem::needs_drop::<PreparedExpireDelegationTokenRequest>());
    let diagnostic = format!("{request:?}");
    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains("expire-secret-must-not-leak"));

    request.zeroize_hmac_for_test();
    assert!(request.hmac_for_test().is_empty());
}

#[test]
fn malformed_secret_period_and_capacity_reject_before_copy() {
    assert_eq!(
        build(ExpireDelegationTokenRequestRef::immediate(&[]), 0).err(),
        Some(ExpireDelegationTokenRequestFailure::EmptyHmac)
    );
    for period in [-2, -1] {
        assert_eq!(
            build(
                ExpireDelegationTokenRequestRef::explicit(b"hmac", period),
                EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
            )
            .err(),
            Some(ExpireDelegationTokenRequestFailure::InvalidExpiryTimePeriod { actual: period })
        );
    }
    let oversized = vec![7; MAX_HMAC_BYTES + 1];
    assert!(matches!(
        build(ExpireDelegationTokenRequestRef::immediate(&oversized), 0),
        Err(ExpireDelegationTokenRequestFailure::HmacTooLong { .. })
    ));
    assert!(matches!(
        build(ExpireDelegationTokenRequestRef::immediate(b"hmac"), 0),
        Err(ExpireDelegationTokenRequestFailure::RetainedBytes {
            required: 1..,
            limit: 0
        })
    ));
}

fn prepare(source: ExpireDelegationTokenRequestRef<'_>) -> PreparedExpireDelegationTokenRequest {
    build(source, EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES)
        .unwrap_or_else(|error| panic!("valid expiration request: {error:?}"))
}

fn build(
    source: ExpireDelegationTokenRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedExpireDelegationTokenRequest, ExpireDelegationTokenRequestFailure> {
    expire_delegation_token_request(source, retained_limit)
}

fn round_trip(
    request: &PreparedExpireDelegationTokenRequest,
    version: i16,
) -> ExpireDelegationTokenRequest {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = ExpireDelegationTokenRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
