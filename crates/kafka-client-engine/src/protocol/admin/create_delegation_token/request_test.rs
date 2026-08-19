//! API contract, owner-version adaptation, validation, and capacity evidence.

use kafka_wire::{CreateDelegationTokenRequest, KafkaMessage, KafkaRequest};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{
    CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, CreateDelegationTokenRequestFailure,
    CreateDelegationTokenRequestRef, DelegationTokenPrincipalRef,
    PreparedCreateDelegationTokenRequest, create_delegation_token_request,
    retention::{MAX_PRINCIPAL_NAME_BYTES, MAX_RENEWERS},
};

#[test]
fn generated_contract_is_api_38_v1_through_v3() {
    assert_eq!(
        <CreateDelegationTokenRequest as KafkaRequest>::API_KEY.value(),
        38
    );
    assert!(CreateDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(1)));
    assert!(CreateDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
    assert!(!CreateDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(0)));
    assert!(!CreateDelegationTokenRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(4)));
}

#[test]
fn default_owner_uses_legacy_sentinel_but_v3_null() {
    let renewers = [principal("User", "renewer")];
    let request = prepare(None, &renewers, -1);

    assert_eq!(request.minimum_version(), 1);
    for version in [1, 2] {
        let decoded = round_trip(&request, version);
        assert_eq!(
            decoded
                .owner_principal_type
                .as_ref()
                .map(kafka_wire_core::StrBytes::as_str),
            Some("")
        );
        assert_eq!(
            decoded
                .owner_principal_name
                .as_ref()
                .map(kafka_wire_core::StrBytes::as_str),
            Some("")
        );
        assert_eq!(decoded.renewers[0].principal_name.as_str(), "renewer");
        assert_eq!(decoded.max_lifetime_ms, -1);
    }
    let modern = round_trip(&request, 3);
    assert_eq!(modern.owner_principal_type, None);
    assert_eq!(modern.owner_principal_name, None);
}

#[test]
fn explicit_owner_requires_v3_and_preserves_principal() {
    let owner = principal("User", "service");
    let request = prepare(Some(owner), &[], 60_000);

    assert_eq!(request.minimum_version(), 3);
    assert!(request.encoded_len(ApiVersion::new(1)).is_err());
    assert!(request.encoded_len(ApiVersion::new(2)).is_err());
    let decoded = round_trip(&request, 3);
    assert_eq!(
        decoded
            .owner_principal_type
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("User")
    );
    assert_eq!(
        decoded
            .owner_principal_name
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("service")
    );
}

#[test]
fn invalid_scalar_principal_count_and_capacity_are_rejected() {
    assert_eq!(
        build(None, &[], -2, CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES).err(),
        Some(CreateDelegationTokenRequestFailure::InvalidMaxLifetime { actual: -2 })
    );
    assert_eq!(
        build(
            Some(principal("", "owner")),
            &[],
            -1,
            CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        )
        .err(),
        Some(CreateDelegationTokenRequestFailure::EmptyPrincipalType { field: "owner" })
    );
    let long_name = "x".repeat(MAX_PRINCIPAL_NAME_BYTES + 1);
    assert!(matches!(
        build(
            Some(principal("User", &long_name)),
            &[],
            -1,
            CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        ),
        Err(CreateDelegationTokenRequestFailure::PrincipalNameTooLong { field: "owner", .. })
    ));
    let too_many = vec![principal("User", "renewer"); MAX_RENEWERS + 1];
    assert!(matches!(
        build(
            None,
            &too_many,
            -1,
            CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        ),
        Err(CreateDelegationTokenRequestFailure::TooManyRenewers { .. })
    ));
    assert!(matches!(
        build(None, &[], -1, 0),
        Err(CreateDelegationTokenRequestFailure::RetainedBytes {
            required: 1..,
            limit: 0
        })
    ));
}

fn principal<'a>(
    principal_type: &'a str,
    principal_name: &'a str,
) -> DelegationTokenPrincipalRef<'a> {
    DelegationTokenPrincipalRef::new(principal_type, principal_name)
}

fn prepare(
    owner: Option<DelegationTokenPrincipalRef<'_>>,
    renewers: &[DelegationTokenPrincipalRef<'_>],
    max_lifetime_ms: i64,
) -> PreparedCreateDelegationTokenRequest {
    build(
        owner,
        renewers,
        max_lifetime_ms,
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("valid request: {error:?}"))
}

fn build(
    owner: Option<DelegationTokenPrincipalRef<'_>>,
    renewers: &[DelegationTokenPrincipalRef<'_>],
    max_lifetime_ms: i64,
    retained_limit: usize,
) -> Result<PreparedCreateDelegationTokenRequest, CreateDelegationTokenRequestFailure> {
    create_delegation_token_request(
        CreateDelegationTokenRequestRef::new(owner, renewers, max_lifetime_ms),
        retained_limit,
    )
}

fn round_trip(
    request: &PreparedCreateDelegationTokenRequest,
    version: i16,
) -> CreateDelegationTokenRequest {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = CreateDelegationTokenRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
