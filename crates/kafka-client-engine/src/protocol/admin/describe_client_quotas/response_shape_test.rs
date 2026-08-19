//! Malformed and hostile generated client-quota response shape evidence.

use kafka_wire::describe_client_quotas_response::EntryData;

use super::{
    DescribeClientQuotasResponseFailure, normalize_describe_client_quotas_response,
    response_test::{entity, entry, response, value},
    retention::{MAX_ENTITY_NAME_BYTES, MAX_ENTITY_TYPE_BYTES, MAX_ENTRIES, MAX_QUOTA_KEY_BYTES},
};

#[test]
fn response_rejects_versions_negative_throttle_and_null_success() {
    let valid = response(Some(Vec::new()));
    assert_eq!(
        normalize_describe_client_quotas_response(-1, &valid, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::UnsupportedApiVersion { actual: -1 })
    );
    assert_eq!(
        normalize_describe_client_quotas_response(2, &valid, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::UnsupportedApiVersion { actual: 2 })
    );

    let mut negative = valid;
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize_describe_client_quotas_response(0, &negative, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::NegativeThrottleTime { actual: -1 })
    );

    let null_success = response(None);
    assert_eq!(
        normalize_describe_client_quotas_response(1, &null_success, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::MissingEntriesOnSuccess)
    );
}

#[test]
fn top_level_error_accepts_no_entries_but_rejects_populated_entries() {
    let mut nullable = response(None);
    nullable.error_code = 31;
    let normalized = normalize_describe_client_quotas_response(1, &nullable, usize::MAX)
        .unwrap_or_else(|error| panic!("null entries belong to a broker error: {error:?}"));
    assert_eq!(normalized.error_code, 31);
    assert!(normalized.entries.is_empty());

    let mut empty = response(Some(Vec::new()));
    empty.error_code = -7;
    assert!(normalize_describe_client_quotas_response(0, &empty, usize::MAX).is_ok());

    let mut populated = response(Some(vec![valid_entry()]));
    populated.error_code = 12;
    assert_eq!(
        normalize_describe_client_quotas_response(1, &populated, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EntriesWithTopLevelError { actual: 1 })
    );
}

#[test]
fn response_rejects_empty_nested_shapes_and_text() {
    let empty_entity = response(Some(vec![entry(Vec::new(), vec![value("rate", 1.0)])]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &empty_entity, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EmptyEntity)
    );

    let empty_values = response(Some(vec![entry(vec![entity("user", None)], Vec::new())]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &empty_values, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EmptyValues)
    );

    let empty_type = response(Some(vec![entry(
        vec![entity("", None)],
        vec![value("rate", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &empty_type, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EmptyEntityType)
    );

    let empty_name = response(Some(vec![entry(
        vec![entity("user", Some(""))],
        vec![value("rate", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &empty_name, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EmptyEntityName)
    );

    let empty_key = response(Some(vec![entry(
        vec![entity("user", None)],
        vec![value("", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &empty_key, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EmptyQuotaKey)
    );
}

#[test]
fn response_rejects_oversized_text_and_non_finite_values() {
    let oversized_type = "x".repeat(MAX_ENTITY_TYPE_BYTES + 1);
    let oversized_type_response = response(Some(vec![entry(
        vec![entity(&oversized_type, None)],
        vec![value("rate", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &oversized_type_response, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EntityTypeTooLong {
            actual: MAX_ENTITY_TYPE_BYTES + 1,
            max: MAX_ENTITY_TYPE_BYTES,
        })
    );

    let oversized_name = "x".repeat(MAX_ENTITY_NAME_BYTES + 1);
    let oversized_name_response = response(Some(vec![entry(
        vec![entity("user", Some(&oversized_name))],
        vec![value("rate", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &oversized_name_response, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::EntityNameTooLong {
            actual: MAX_ENTITY_NAME_BYTES + 1,
            max: MAX_ENTITY_NAME_BYTES,
        })
    );

    let oversized_key = "x".repeat(MAX_QUOTA_KEY_BYTES + 1);
    let oversized_key_response = response(Some(vec![entry(
        vec![entity("user", None)],
        vec![value(&oversized_key, 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &oversized_key_response, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::QuotaKeyTooLong {
            actual: MAX_QUOTA_KEY_BYTES + 1,
            max: MAX_QUOTA_KEY_BYTES,
        })
    );

    for non_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let response = response(Some(vec![entry(
            vec![entity("user", None)],
            vec![value("rate", non_finite)],
        )]));
        assert_eq!(
            normalize_describe_client_quotas_response(1, &response, usize::MAX),
            Err(DescribeClientQuotasResponseFailure::NonFiniteQuotaValue)
        );
    }
}

#[test]
fn response_rejects_each_duplicate_identity_domain() {
    let duplicate_type = response(Some(vec![entry(
        vec![entity("user", None), entity("user", Some("User:a"))],
        vec![value("rate", 1.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &duplicate_type, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::DuplicateEntityType)
    );

    let duplicate_key = response(Some(vec![entry(
        vec![entity("user", None)],
        vec![value("rate", 1.0), value("rate", 2.0)],
    )]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &duplicate_key, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::DuplicateQuotaKey)
    );

    let duplicate_entity = response(Some(vec![
        entry(
            vec![entity("user", None), entity("client-id", Some("orders"))],
            vec![value("a", 1.0)],
        ),
        entry(
            vec![entity("client-id", Some("orders")), entity("user", None)],
            vec![value("b", 2.0)],
        ),
    ]));
    assert_eq!(
        normalize_describe_client_quotas_response(1, &duplicate_entity, usize::MAX,),
        Err(DescribeClientQuotasResponseFailure::DuplicateEntity)
    );
}

#[test]
fn response_rejects_hostile_entry_count_before_nested_validation() {
    let response = response(Some(vec![EntryData::default(); MAX_ENTRIES + 1]));

    assert_eq!(
        normalize_describe_client_quotas_response(1, &response, usize::MAX),
        Err(DescribeClientQuotasResponseFailure::TooManyEntries {
            actual: MAX_ENTRIES + 1,
            max: MAX_ENTRIES,
        })
    );
}

fn valid_entry() -> EntryData {
    entry(
        vec![entity("user", Some("User:a"))],
        vec![value("producer_byte_rate", 1024.0)],
    )
}
