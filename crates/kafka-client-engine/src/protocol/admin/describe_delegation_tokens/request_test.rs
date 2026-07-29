//! Nullable selection, generated encoding, validation, and capacity evidence.

use kafka_wire::{DescribeDelegationTokenRequest, KafkaMessage, KafkaRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, DescribeDelegationTokenPrincipalRef,
    DescribeDelegationTokensRequestFailure, DescribeDelegationTokensRequestRef,
    PreparedDescribeDelegationTokensRequest, describe_delegation_tokens_request,
    retention::MAX_OWNERS,
};

#[test]
fn generated_contract_is_api_41_v1_through_v3() {
    assert_eq!(
        <DescribeDelegationTokenRequest as KafkaRequest>::API_KEY.value(),
        41
    );
    assert!(DescribeDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
    assert!(DescribeDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
    assert!(!DescribeDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!DescribeDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(4)));
}

#[test]
fn explicit_all_is_null_in_every_supported_version() {
    let request = prepare(DescribeDelegationTokensRequestRef::all());

    for version in [1, 2, 3] {
        assert_eq!(round_trip(&request, version).owners, None);
    }
}

#[test]
fn selected_nonempty_owners_remain_some_and_in_caller_order() {
    let owners = [principal("User", "zoë"), principal("Service", "alpha")];
    let request = prepare(DescribeDelegationTokensRequestRef::selected(&owners));

    for version in [1, 2, 3] {
        let decoded = round_trip(&request, version);
        let decoded = decoded
            .owners
            .unwrap_or_else(|| panic!("selected owners must not become all"));
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].principal_name.as_str(), "zoë");
        assert_eq!(decoded[1].principal_type.as_str(), "Service");
        assert_eq!(decoded[1].principal_name.as_str(), "alpha");
    }
}

#[test]
fn empty_duplicate_count_and_capacity_cannot_conflate_with_all() {
    assert_eq!(
        build(
            DescribeDelegationTokensRequestRef::selected(&[]),
            usize::MAX
        )
        .err(),
        Some(DescribeDelegationTokensRequestFailure::EmptyOwnerSelection)
    );
    let duplicate = [principal("User", "alice"), principal("User", "alice")];
    assert_eq!(
        build(
            DescribeDelegationTokensRequestRef::selected(&duplicate),
            usize::MAX,
        )
        .err(),
        Some(DescribeDelegationTokensRequestFailure::DuplicateOwner)
    );
    let too_many = vec![principal("User", "alice"); MAX_OWNERS + 1];
    assert!(matches!(
        build(
            DescribeDelegationTokensRequestRef::selected(&too_many),
            usize::MAX,
        ),
        Err(DescribeDelegationTokensRequestFailure::TooManyOwners { .. })
    ));
    assert!(matches!(
        build(DescribeDelegationTokensRequestRef::all(), 0),
        Err(DescribeDelegationTokensRequestFailure::RetainedBytes {
            required: 1..,
            limit: 0
        })
    ));
}

fn principal<'a>(
    principal_type: &'a str,
    principal_name: &'a str,
) -> DescribeDelegationTokenPrincipalRef<'a> {
    DescribeDelegationTokenPrincipalRef::new(principal_type, principal_name)
}

fn prepare(
    source: DescribeDelegationTokensRequestRef<'_>,
) -> PreparedDescribeDelegationTokensRequest {
    build(source, DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"))
}

fn build(
    source: DescribeDelegationTokensRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedDescribeDelegationTokensRequest, DescribeDelegationTokensRequestFailure> {
    describe_delegation_tokens_request(source, retained_limit)
}

fn round_trip(
    request: &PreparedDescribeDelegationTokensRequest,
    version: i16,
) -> DescribeDelegationTokenRequest {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = DescribeDelegationTokenRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
