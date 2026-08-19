//! Response correlation, malformed-shape, and bounded-diagnostic evidence.

use kafka_wire::{AlterClientQuotasResponse, alter_client_quotas_response::EntryData};
use kafka_wire_core::StrBytes;

use super::{
    AlterClientQuotaAlterationRef, AlterClientQuotasRequestRef, AlterClientQuotasResponseFailure,
    normalize_alter_client_quotas_response,
    request_test::{alteration, component, remove},
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_ENTITY_COMPONENTS},
};

#[test]
fn response_canonicalizes_identities_and_restores_caller_order() {
    let first_entity = [component("user", None)];
    let second_entity = [
        component("client-id", Some("orders")),
        component("user", Some("User:a")),
    ];
    let operations = [remove("rate")];
    let alterations = [
        alteration(&first_entity, &operations),
        alteration(&second_entity, &operations),
    ];
    let response = response(vec![
        entry(
            31,
            Some("denied"),
            vec![
                response_entity("user", Some("User:a")),
                response_entity("client-id", Some("orders")),
            ],
        ),
        entry(0, None, vec![response_entity("user", None)]),
    ]);

    let normalized = normalize(&alterations, 1, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    assert_eq!(normalized.throttle_time_ms, 9);
    assert_eq!(normalized.outcomes.len(), 2);
    assert_eq!(normalized.outcomes[0].entity[0].entity_type, "user");
    assert_eq!(normalized.outcomes[0].error_code, 0);
    assert_eq!(normalized.outcomes[1].entity[0].entity_type, "client-id");
    assert_eq!(normalized.outcomes[1].entity[1].entity_type, "user");
    assert_eq!(normalized.outcomes[1].error_code, 31);
    assert_eq!(
        normalized.outcomes[1].error_message.as_deref(),
        Some("denied")
    );
}

#[test]
fn response_preserves_signed_codes_and_utf8_safe_diagnostics() {
    let entity = [component("user", None)];
    let operations = [remove("rate")];
    let alterations = [alteration(&entity, &operations)];
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let response = response(vec![entry(
        -42,
        Some(&diagnostic),
        vec![response_entity("user", None)],
    )]);

    let normalized = normalize(&alterations, 0, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let outcome = &normalized.outcomes[0];
    assert_eq!(outcome.error_code, -42);
    assert_eq!(
        outcome.error_message.as_deref().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(outcome.error_message_truncated);
}

#[test]
fn response_rejects_version_throttle_and_count_mismatch() {
    let (alterations, valid_response) = one();
    assert_eq!(
        normalize(&alterations, -1, &valid_response, usize::MAX),
        Err(AlterClientQuotasResponseFailure::UnsupportedApiVersion { actual: -1 })
    );
    assert_eq!(
        normalize(&alterations, 2, &valid_response, usize::MAX),
        Err(AlterClientQuotasResponseFailure::UnsupportedApiVersion { actual: 2 })
    );

    let mut negative = valid_response;
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize(&alterations, 1, &negative, usize::MAX),
        Err(AlterClientQuotasResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    assert_eq!(
        normalize(&alterations, 1, &response(Vec::new()), usize::MAX),
        Err(AlterClientQuotasResponseFailure::EntryCount {
            expected: 1,
            actual: 0,
        })
    );
}

#[test]
fn response_rejects_empty_oversized_and_duplicate_component_shapes() {
    let (alterations, _) = one();
    assert_eq!(
        normalize(
            &alterations,
            1,
            &response(vec![entry(0, None, Vec::new())]),
            usize::MAX
        ),
        Err(AlterClientQuotasResponseFailure::EmptyEntity)
    );
    let hostile = vec![response_entity("user", None); MAX_ENTITY_COMPONENTS + 1];
    assert_eq!(
        normalize(
            &alterations,
            1,
            &response(vec![entry(0, None, hostile)]),
            usize::MAX
        ),
        Err(AlterClientQuotasResponseFailure::TooManyEntityComponents {
            actual: MAX_ENTITY_COMPONENTS + 1,
            max: MAX_ENTITY_COMPONENTS,
        })
    );
    let duplicate = vec![
        response_entity("user", None),
        response_entity("user", Some("User:a")),
    ];
    assert_eq!(
        normalize(
            &alterations,
            1,
            &response(vec![entry(0, None, duplicate)]),
            usize::MAX
        ),
        Err(AlterClientQuotasResponseFailure::DuplicateEntityType)
    );
}

#[test]
fn response_rejects_duplicate_unexpected_and_missing_entities() {
    let first_entity = [component("user", None)];
    let second_entity = [component("client-id", Some("orders"))];
    let operations = [remove("rate")];
    let alterations = [
        alteration(&first_entity, &operations),
        alteration(&second_entity, &operations),
    ];
    let duplicate = response(vec![
        entry(0, None, vec![response_entity("user", None)]),
        entry(0, None, vec![response_entity("user", None)]),
    ]);
    assert_eq!(
        normalize(&alterations, 1, &duplicate, usize::MAX),
        Err(AlterClientQuotasResponseFailure::DuplicateResponseEntity)
    );

    let unexpected = response(vec![
        entry(0, None, vec![response_entity("ip", Some("127.0.0.1"))]),
        entry(0, None, vec![response_entity("user", None)]),
    ]);
    assert!(matches!(
        normalize(&alterations, 1, &unexpected, usize::MAX),
        Err(AlterClientQuotasResponseFailure::UnexpectedEntity
            | AlterClientQuotasResponseFailure::MissingEntity)
    ));
}

fn one() -> (
    [AlterClientQuotaAlterationRef<'static>; 1],
    AlterClientQuotasResponse,
) {
    static ENTITY: [super::AlterClientQuotaEntityComponentRef<'static>; 1] =
        [component("user", None)];
    static OPERATIONS: [super::AlterClientQuotaOperationRef<'static>; 1] = [remove("rate")];
    (
        [alteration(&ENTITY, &OPERATIONS)],
        response(vec![entry(0, None, vec![response_entity("user", None)])]),
    )
}

fn normalize(
    alterations: &[AlterClientQuotaAlterationRef<'_>],
    version: i16,
    response: &AlterClientQuotasResponse,
    limit: usize,
) -> Result<super::NormalizedAlterClientQuotasResponse, AlterClientQuotasResponseFailure> {
    normalize_alter_client_quotas_response(
        version,
        AlterClientQuotasRequestRef::new(alterations, false),
        response,
        limit,
    )
}

fn response(entries: Vec<EntryData>) -> AlterClientQuotasResponse {
    let mut response = AlterClientQuotasResponse::default();
    response.throttle_time_ms = 9;
    response.entries = entries;
    response
}

fn entry(
    error_code: i16,
    error_message: Option<&str>,
    entity: Vec<kafka_wire::alter_client_quotas_response::EntityData>,
) -> EntryData {
    let mut entry = EntryData::default();
    entry.error_code = error_code;
    entry.error_message = error_message.map(StrBytes::from);
    entry.entity = entity;
    entry
}

fn response_entity(
    entity_type: &str,
    entity_name: Option<&str>,
) -> kafka_wire::alter_client_quotas_response::EntityData {
    let mut entity = kafka_wire::alter_client_quotas_response::EntityData::default();
    entity.entity_type = StrBytes::from(entity_type);
    entity.entity_name = entity_name.map(StrBytes::from);
    entity
}
