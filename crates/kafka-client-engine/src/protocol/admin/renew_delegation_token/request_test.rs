//! API contract, exact period encoding, secret safety, and capacity evidence.

use kafka_wire::{KafkaMessage, KafkaRequest, RenewDelegationTokenRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    PreparedRenewDelegationTokenRequest, RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    RenewDelegationTokenRequestFailure, RenewDelegationTokenRequestRef,
    renew_delegation_token_request, retention::MAX_HMAC_BYTES,
};

#[test]
fn generated_contract_is_api_39_v1_through_v2_with_flexible_v2() {
    assert_eq!(
        <RenewDelegationTokenRequest as KafkaRequest>::API_KEY.value(),
        39
    );
    assert!(RenewDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
    assert!(RenewDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(2)));
    assert!(!RenewDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!RenewDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
    assert_eq!(
        RenewDelegationTokenRequest::FLEXIBLE_VERSIONS,
        Some(kafka_wire_core::VersionRange::new(2, 2))
    );
}

#[test]
fn broker_default_is_exact_minus_one_in_v1_and_v2() {
    let request = prepare(RenewDelegationTokenRequestRef::broker_default(
        b"default-period-secret",
    ));
    for version in [1, 2] {
        let decoded = round_trip(&request, version);
        assert_eq!(decoded.hmac.as_ref(), b"default-period-secret");
        assert_eq!(decoded.renew_period_ms, -1);
        assert!(decoded.unknown_tagged_fields.is_empty());
    }
}

#[test]
fn explicit_positive_period_is_exact_in_v1_and_v2() {
    let request = prepare(RenewDelegationTokenRequestRef::explicit(
        b"explicit-period-secret",
        86_400_123,
    ));
    for version in [1, 2] {
        let decoded = round_trip(&request, version);
        assert_eq!(decoded.hmac.as_ref(), b"explicit-period-secret");
        assert_eq!(decoded.renew_period_ms, 86_400_123);
    }
}

#[test]
fn secret_owner_is_nontrivial_redacted_and_zeroizing() {
    let mut request = prepare(RenewDelegationTokenRequestRef::broker_default(
        b"renew-secret-must-not-leak",
    ));
    assert!(core::mem::needs_drop::<PreparedRenewDelegationTokenRequest>());
    let diagnostic = format!("{request:?}");
    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains("renew-secret-must-not-leak"));

    request.zeroize_hmac_for_test();
    assert!(request.hmac_for_test().is_empty());
}

#[test]
fn malformed_secret_period_and_capacity_reject_before_copy() {
    assert_eq!(
        build(RenewDelegationTokenRequestRef::broker_default(&[]), 0).err(),
        Some(RenewDelegationTokenRequestFailure::EmptyHmac)
    );
    for period in [-2, 0] {
        assert_eq!(
            build(
                RenewDelegationTokenRequestRef::explicit(b"hmac", period),
                RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
            )
            .err(),
            Some(RenewDelegationTokenRequestFailure::InvalidRenewPeriod { actual: period })
        );
    }
    let oversized = vec![7; MAX_HMAC_BYTES + 1];
    assert!(matches!(
        build(
            RenewDelegationTokenRequestRef::broker_default(&oversized),
            0,
        ),
        Err(RenewDelegationTokenRequestFailure::HmacTooLong { .. })
    ));
    assert!(matches!(
        build(RenewDelegationTokenRequestRef::broker_default(b"hmac"), 0),
        Err(RenewDelegationTokenRequestFailure::RetainedBytes {
            required: 1..,
            limit: 0
        })
    ));
}

fn prepare(source: RenewDelegationTokenRequestRef<'_>) -> PreparedRenewDelegationTokenRequest {
    build(source, RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES)
        .unwrap_or_else(|error| panic!("valid renewal request: {error:?}"))
}

fn build(
    source: RenewDelegationTokenRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedRenewDelegationTokenRequest, RenewDelegationTokenRequestFailure> {
    renew_delegation_token_request(source, retained_limit)
}

fn round_trip(
    request: &PreparedRenewDelegationTokenRequest,
    version: i16,
) -> RenewDelegationTokenRequest {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = RenewDelegationTokenRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
