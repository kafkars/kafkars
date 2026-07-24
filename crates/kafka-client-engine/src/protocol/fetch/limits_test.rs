//! Independent evidence for cumulative response and decoded-record budgets.

use kafka_wire::{FetchResponse, fetch_response::FetchableTopicResponse};

use super::{
    failure::FetchDecodeFailure,
    limits::{FetchBudget, FetchDecodeLimits},
};

#[test]
fn generated_response_footprint_is_bounded_before_normalization() {
    let mut response = FetchResponse::default();
    response.responses = vec![FetchableTopicResponse::default()];
    let limits = FetchDecodeLimits {
        max_topics: 0,
        ..FetchDecodeLimits::default()
    };
    assert!(matches!(
        FetchBudget::start(&response, limits),
        Err(FetchDecodeFailure::TopicCount {
            actual: 1,
            limit: 0,
        })
    ));

    let limits = FetchDecodeLimits {
        max_response_retained_bytes: 0,
        ..FetchDecodeLimits::default()
    };
    assert!(matches!(
        FetchBudget::start(&response, limits),
        Err(FetchDecodeFailure::ResponseRetainedBytes { actual, limit: 0 })
            if actual > 0
    ));
}

#[test]
fn record_and_header_counts_accumulate_across_batches() {
    let response = FetchResponse::default();
    let limits = FetchDecodeLimits {
        max_records: 2,
        max_headers: 2,
        max_logical_record_bytes: 5,
        ..FetchDecodeLimits::default()
    };
    let mut budget = FetchBudget::start(&response, limits)
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"));

    budget
        .add_record(1, 2)
        .unwrap_or_else(|error| panic!("first record: {error:?}"));
    budget
        .add_record(1, 3)
        .unwrap_or_else(|error| panic!("second record: {error:?}"));
    assert_eq!(
        budget.add_record(0, 0),
        Err(FetchDecodeFailure::RecordCount {
            actual: 3,
            limit: 2,
        })
    );
}

#[test]
fn aborted_transaction_count_is_cumulative() {
    let response = FetchResponse::default();
    let limits = FetchDecodeLimits {
        max_aborted_transactions: 1,
        ..FetchDecodeLimits::default()
    };
    let mut budget = FetchBudget::start(&response, limits)
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"));

    budget
        .add_aborted_transactions(1)
        .unwrap_or_else(|error| panic!("first aborted transaction: {error:?}"));
    assert_eq!(
        budget.add_aborted_transactions(1),
        Err(FetchDecodeFailure::AbortedTransactionCount {
            actual: 2,
            limit: 1,
        })
    );
}

#[test]
fn additional_retained_payload_bytes_are_cumulative() {
    let response = FetchResponse::default();
    let limits = FetchDecodeLimits {
        max_additional_retained_payload_bytes: 5,
        ..FetchDecodeLimits::default()
    };
    let mut budget = FetchBudget::start(&response, limits)
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"));

    budget
        .add_batch(2)
        .unwrap_or_else(|error| panic!("first retained payload: {error:?}"));
    budget
        .add_batch(3)
        .unwrap_or_else(|error| panic!("second retained payload: {error:?}"));
    assert_eq!(budget.remaining_additional_retained_payload_bytes(), 0);
    assert_eq!(
        budget.add_batch(1),
        Err(FetchDecodeFailure::AdditionalRetainedPayloadBytes {
            actual: 6,
            limit: 5,
        })
    );
}
