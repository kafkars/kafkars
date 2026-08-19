//! API-key, feature-floor, canonical request, bounds, and capacity scenarios.

use kafka_wire::{KafkaMessage, KafkaRequest, ListTransactionsRequest};
use kafka_wire_core::ApiVersion;

use super::{
    LIST_TRANSACTIONS_MAX_VERSION, ListTransactionsRequestFailure, ListTransactionsRequestPlan,
    list_transactions_request,
    validation::{
        LIST_TRANSACTIONS_MAX_PATTERN_BYTES, LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS,
        LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES, LIST_TRANSACTIONS_MAX_STATE_FILTERS,
    },
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn generated_contract_is_flexible_api_66_v0_through_v2() {
    assert_eq!(
        <ListTransactionsRequest as KafkaRequest>::API_KEY.value(),
        66
    );
    for version in 0..=LIST_TRANSACTIONS_MAX_VERSION {
        assert!(ListTransactionsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(version)));
        assert!(ListTransactionsRequest::is_flexible(ApiVersion::new(
            version
        )));
    }
    assert!(!ListTransactionsRequest::SUPPORTED_VERSIONS.contains(ApiVersion::new(3)));
}

#[test]
fn feature_presence_raises_only_the_required_floor() {
    assert_eq!(prepared(&[], &[], None, None).1, 0);
    assert_eq!(prepared(&[], &[], Some(0), None).1, 1);
    assert_eq!(prepared(&[], &[], None, Some("orders-.*")).1, 2);
    assert_eq!(prepared(&[], &[], Some(10), Some("")).1, 2);
}

#[test]
fn request_preserves_opaque_pattern_and_signed_ids_in_canonical_order() {
    let states = vec!["Ongoing".to_owned(), "Empty".to_owned()];
    let producers = vec![i64::MAX, -7, i64::MIN];
    let (request, floor) = prepared(&states, &producers, Some(0), Some("[broker-owned"));
    assert_eq!(floor, 2);
    assert_eq!(
        request
            .state_filters
            .iter()
            .map(kafka_wire_core::StrBytes::as_str)
            .collect::<Vec<_>>(),
        ["Empty", "Ongoing"]
    );
    assert_eq!(request.producer_id_filters, [i64::MIN, -7, i64::MAX]);
    assert_eq!(request.duration_filter, 0);
    assert_eq!(
        request
            .transactional_id_pattern
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("[broker-owned")
    );
}

#[test]
fn duplicates_counts_scalars_duration_and_capacity_are_bounded() {
    let duplicate_states = vec!["Ongoing".to_owned(), "Ongoing".to_owned()];
    assert_failure(
        &duplicate_states,
        &[],
        None,
        None,
        ListTransactionsRequestFailure::DuplicateStateFilter,
    );
    assert_failure(
        &[],
        &[-1, -1],
        None,
        None,
        ListTransactionsRequestFailure::DuplicateProducerId { actual: -1 },
    );

    let too_many_states = vec![String::new(); LIST_TRANSACTIONS_MAX_STATE_FILTERS + 1];
    assert!(matches!(
        build(&too_many_states, &[], None, None, LIMIT),
        Err(ListTransactionsRequestFailure::TooManyStateFilters { .. })
    ));
    let too_many_producers = vec![0; LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS + 1];
    assert!(matches!(
        build(&[], &too_many_producers, None, None, LIMIT),
        Err(ListTransactionsRequestFailure::TooManyProducerIdFilters { .. })
    ));
    let long_state = vec!["x".repeat(LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES + 1)];
    assert!(matches!(
        build(&long_state, &[], None, None, LIMIT),
        Err(ListTransactionsRequestFailure::StateFilterTooLong { .. })
    ));
    assert!(matches!(
        build(&[], &[], Some(-1), None, LIMIT),
        Err(ListTransactionsRequestFailure::NegativeDurationFilter { .. })
    ));
    let long_pattern = "x".repeat(LIST_TRANSACTIONS_MAX_PATTERN_BYTES + 1);
    assert!(matches!(
        build(&[], &[], None, Some(&long_pattern), LIMIT),
        Err(ListTransactionsRequestFailure::PatternTooLong { .. })
    ));
    assert!(matches!(
        build(&[], &[], None, None, 0),
        Err(ListTransactionsRequestFailure::RetainedBytes { .. })
    ));
}

fn prepared(
    states: &[String],
    producers: &[i64],
    duration: Option<i64>,
    pattern: Option<&str>,
) -> (ListTransactionsRequest, i16) {
    build(states, producers, duration, pattern, LIMIT)
        .unwrap_or_else(|error| panic!("valid request: {error:?}"))
}

fn build(
    states: &[String],
    producers: &[i64],
    duration: Option<i64>,
    pattern: Option<&str>,
    limit: usize,
) -> Result<(ListTransactionsRequest, i16), ListTransactionsRequestFailure> {
    list_transactions_request(
        ListTransactionsRequestPlan::new(states, producers, duration, pattern),
        limit,
    )
}

fn assert_failure(
    states: &[String],
    producers: &[i64],
    duration: Option<i64>,
    pattern: Option<&str>,
    expected: ListTransactionsRequestFailure,
) {
    assert_eq!(
        build(states, producers, duration, pattern, LIMIT).err(),
        Some(expected)
    );
}
