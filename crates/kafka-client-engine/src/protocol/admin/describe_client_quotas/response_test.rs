//! Focused evidence for canonical generated client-quota response normalization.

use kafka_wire::DescribeClientQuotasResponse;
use kafka_wire_core::StrBytes;

use super::{
    DescribeClientQuotasResponseFailure, normalize_describe_client_quotas_response,
    retention::MAX_DIAGNOSTIC_BYTES,
};

#[test]
fn response_canonicalizes_entities_values_and_outer_entries() {
    let response = response(Some(vec![
        entry(
            vec![entity("ip", Some("10.0.0.1"))],
            vec![value("request_percentage", 12.5)],
        ),
        entry(
            vec![entity("user", None), entity("client-id", Some("orders"))],
            vec![value("z", 9.0), value("a", -0.25)],
        ),
    ]));

    let normalized = normalize_describe_client_quotas_response(1, &response, usize::MAX)
        .expect("valid response");
    assert_eq!(normalized.throttle_time_ms, 7);
    assert_eq!(normalized.error_code, 0);
    assert_eq!(normalized.entries.len(), 2);

    let first = &normalized.entries[0];
    assert_eq!(first.entity[0].entity_type, "client-id");
    assert_eq!(first.entity[0].entity_name.as_deref(), Some("orders"));
    assert_eq!(first.entity[1].entity_type, "user");
    assert_eq!(first.entity[1].entity_name, None);
    assert_eq!(first.values[0].key, "a");
    assert_eq!(first.values[0].value, -0.25);
    assert_eq!(first.values[1].key, "z");
    assert_eq!(normalized.entries[1].entity[0].entity_type, "ip");
}

#[test]
fn default_entity_orders_before_an_exact_entity_of_the_same_type() {
    let response = response(Some(vec![
        entry(
            vec![entity("user", Some("User:z"))],
            vec![value("rate", 2.0)],
        ),
        entry(vec![entity("user", None)], vec![value("rate", 1.0)]),
    ]));

    let normalized = normalize_describe_client_quotas_response(0, &response, usize::MAX)
        .expect("valid response");
    assert_eq!(normalized.entries[0].entity[0].entity_name, None);
    assert_eq!(
        normalized.entries[1].entity[0].entity_name.as_deref(),
        Some("User:z")
    );
}

#[test]
fn response_preserves_signed_error_and_utf8_bounded_diagnostic() {
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let mut response = response(None);
    response.error_code = -42;
    response.error_message = Some(StrBytes::from(diagnostic.as_str()));

    let normalized = normalize_describe_client_quotas_response(0, &response, usize::MAX)
        .expect("bounded broker rejection");
    assert_eq!(normalized.error_code, -42);
    assert_eq!(
        normalized.error_message.as_deref().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(normalized.error_message_truncated);
    assert!(normalized.entries.is_empty());
}

#[test]
fn response_checks_peak_scratch_and_output_before_copying() {
    let response = response(Some(vec![entry(
        vec![entity("user", Some("User:a"))],
        vec![value("producer_byte_rate", 1024.0)],
    )]));
    let normalized =
        normalize_describe_client_quotas_response(1, &response, usize::MAX).expect("measure peak");
    let required = normalized.retained_bytes;

    assert_eq!(
        normalize_describe_client_quotas_response(1, &response, required - 1),
        Err(DescribeClientQuotasResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert!(normalize_describe_client_quotas_response(1, &response, required).is_ok());
}

pub(super) fn response(
    entries: Option<Vec<kafka_wire::describe_client_quotas_response::EntryData>>,
) -> DescribeClientQuotasResponse {
    let mut response = DescribeClientQuotasResponse::default();
    response.throttle_time_ms = 7;
    response.error_code = 0;
    response.error_message = None;
    response.entries = entries;
    response
}

pub(super) fn entry(
    entity: Vec<kafka_wire::describe_client_quotas_response::EntityData>,
    values: Vec<kafka_wire::describe_client_quotas_response::ValueData>,
) -> kafka_wire::describe_client_quotas_response::EntryData {
    let mut entry = kafka_wire::describe_client_quotas_response::EntryData::default();
    entry.entity = entity;
    entry.values = values;
    entry
}

pub(super) fn entity(
    entity_type: &str,
    entity_name: Option<&str>,
) -> kafka_wire::describe_client_quotas_response::EntityData {
    let mut entity = kafka_wire::describe_client_quotas_response::EntityData::default();
    entity.entity_type = StrBytes::from(entity_type);
    entity.entity_name = entity_name.map(StrBytes::from);
    entity
}

pub(super) fn value(
    key: &str,
    value: f64,
) -> kafka_wire::describe_client_quotas_response::ValueData {
    let mut quota = kafka_wire::describe_client_quotas_response::ValueData::default();
    quota.key = StrBytes::from(key);
    quota.value = value;
    quota
}
