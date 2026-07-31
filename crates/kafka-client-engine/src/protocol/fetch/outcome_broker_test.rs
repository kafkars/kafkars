//! Broker-first classification and linear rollback for composed Fetch outcomes.

use core::num::NonZeroI16;

use bytes::BytesMut;
use kafka_wire::FetchResponse as WireFetchResponse;

use super::{
    FetchBrokerLevel, FetchDecodeLimits, FetchIsolation, FetchOutcomeFailure, FetchResponseFailure,
    FetchSessionRequest, FetchSessionUpdate,
    batch_model_test::batch,
    decode_test::batch_bytes,
    fetch_session_requires_reestablishment, normalize_session_fetch_outcome,
    outcome_test::{
        PARTITION, REQUESTED_OFFSET, SELECTED_VERSION, TOPIC, normalize, normalize_with, partition,
        response, response_with_partition,
    },
    retention::FetchReservationDomain,
};

#[test]
fn only_established_session_errors_request_reestablishment() {
    let incremental = FetchSessionRequest::incremental(91, 3)
        .unwrap_or_else(|| panic!("valid incremental session"));
    for error_code in [70, 71] {
        assert!(fetch_session_requires_reestablishment(
            incremental,
            error_code
        ));
        assert!(!fetch_session_requires_reestablishment(
            FetchSessionRequest::INITIAL,
            error_code
        ));
    }
    assert!(!fetch_session_requires_reestablishment(incremental, 6));
}

#[test]
fn top_level_broker_failure_needs_no_success_shape_or_session() {
    let mut response = WireFetchResponse::default();
    response.throttle_time_ms = -1;
    response.error_code = -32_000;
    response.session_id = 91;
    let normalized = normalize(response, -1, 4_096)
        .unwrap_or_else(|rejected| panic!("top broker outcome: {:?}", rejected.failure()));
    let failure = normalized
        .outcome()
        .broker_failure()
        .unwrap_or_else(|| panic!("top broker failure"));

    assert_eq!(failure.level(), FetchBrokerLevel::TopLevel);
    assert_eq!(failure.code(), nonzero(-32_000));
    assert_eq!(normalized.throttle_ticks(), None);
    assert_eq!(normalized.retained_bytes(), 0);
    assert_eq!(normalized.unused_reserved_bytes(), 4_096);
}

#[test]
fn partition_broker_failure_needs_no_record_decode_or_success_session() {
    let mut response = response_with_partition(partition(i16::MAX, Some(corrupt_records())));
    response.throttle_time_ms = -1;
    response.session_id = 91;
    let normalized = normalize(response, -1, 4_096)
        .unwrap_or_else(|rejected| panic!("partition broker outcome: {:?}", rejected.failure()));
    let failure = normalized
        .outcome()
        .broker_failure()
        .unwrap_or_else(|| panic!("partition broker failure"));

    assert_eq!(failure.level(), FetchBrokerLevel::Partition);
    assert_eq!(failure.code(), nonzero(i16::MAX));
    assert_eq!(normalized.outcome().next_offset(), None);
    assert_eq!(normalized.outcome().data_batches(), None);
    assert_eq!(normalized.retained_bytes(), 0);
}

#[test]
fn every_local_failure_returns_the_same_hard_reservation() {
    let invalid_version =
        normalize_with(response(None), 10, 13, FetchDecodeLimits::default(), 4_096);
    assert_rejected(invalid_version, 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::Response(FetchResponseFailure::UnsupportedApiVersion {
                actual: 13
            })
        )
    });

    let missing_correlation = normalize(WireFetchResponse::default(), 10, 4_096);
    assert_rejected(missing_correlation, 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::Response(FetchResponseFailure::TopicCount { actual: 0 })
        )
    });

    let decode = normalize(response(Some(corrupt_records())), 10, 4_096);
    assert_rejected(decode, 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::Response(FetchResponseFailure::Decode(_))
        )
    });

    let mut session = response(None);
    session.session_id = 7;
    assert_rejected(normalize(session, 10, 4_096), 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::UnexpectedSessionId { actual: 7 }
        )
    });

    let mut throttle = response(None);
    throttle.throttle_time_ms = -1;
    assert_rejected(normalize(throttle, 10, 4_096), 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::NegativeThrottleTime { actual: -1 }
        )
    });

    assert_rejected(normalize(response(None), -1, 4_096), 4_096, |failure| {
        matches!(
            failure,
            FetchOutcomeFailure::InvalidRequestedOffset { actual: -1 }
        )
    });
}

#[test]
fn initial_session_establishes_and_incremental_empty_response_advances_epoch() {
    let mut initial = response(None);
    initial.session_id = 91;
    let (outcome, update) =
        normalize_session(FetchSessionRequest::INITIAL, initial, REQUESTED_OFFSET)
            .unwrap_or_else(|rejected| panic!("establish session: {:?}", rejected.failure()));
    assert_eq!(outcome.outcome().next_offset(), Some(REQUESTED_OFFSET));
    let FetchSessionUpdate::Continue(metadata) = update else {
        panic!("established session");
    };
    assert_eq!((metadata.session_id(), metadata.session_epoch()), (91, 1));

    let mut incremental = WireFetchResponse::default();
    incremental.throttle_time_ms = 7;
    incremental.session_id = 91;
    let (outcome, update) = normalize_session(metadata, incremental, REQUESTED_OFFSET)
        .unwrap_or_else(|rejected| panic!("empty incremental response: {:?}", rejected.failure()));
    assert_eq!(outcome.outcome().next_offset(), Some(REQUESTED_OFFSET));
    assert_eq!(outcome.throttle_ticks(), Some(7_000_000));
    assert!(outcome.outcome().data_batches().unwrap_or(&[]).is_empty());
    let FetchSessionUpdate::Continue(metadata) = update else {
        panic!("continued session");
    };
    assert_eq!((metadata.session_id(), metadata.session_epoch()), (91, 2));
}

#[test]
fn incremental_response_cannot_switch_to_an_unowned_session() {
    let metadata = FetchSessionRequest::incremental(91, 3)
        .unwrap_or_else(|| panic!("valid incremental session"));
    let mut unexpected = response(None);
    unexpected.session_id = 92;
    let Err(rejected) = normalize_session(metadata, unexpected, REQUESTED_OFFSET) else {
        panic!("mismatched session must reject");
    };
    assert_eq!(
        rejected.failure(),
        &FetchOutcomeFailure::UnexpectedSessionId { actual: 92 }
    );
}

fn corrupt_records() -> bytes::Bytes {
    let mut encoded = BytesMut::from(batch_bytes(&batch()).as_ref());
    encoded[17] ^= 1;
    encoded.freeze()
}

fn nonzero(value: i16) -> NonZeroI16 {
    let Some(value) = NonZeroI16::new(value) else {
        panic!("test broker code must be nonzero");
    };
    value
}

fn normalize_session(
    metadata: FetchSessionRequest,
    response: WireFetchResponse,
    requested_offset: i64,
) -> Result<(super::RetainedFetchOutcome, FetchSessionUpdate), super::RejectedFetchOutcome> {
    let domain = FetchReservationDomain::create_store_domain();
    let (_, reservation) = domain.issue_pair(0, 4_096);
    normalize_session_fetch_outcome(
        FetchIsolation::ReadUncommitted,
        TOPIC,
        PARTITION,
        requested_offset,
        metadata,
        SELECTED_VERSION,
        response,
        FetchDecodeLimits::default(),
        reservation,
    )
}

fn assert_rejected(
    result: Result<super::RetainedFetchOutcome, super::RejectedFetchOutcome>,
    reserved: usize,
    expected: impl FnOnce(&FetchOutcomeFailure) -> bool,
) {
    let Err(rejected) = result else {
        panic!("local failure must reject");
    };
    assert!(expected(rejected.failure()), "{:?}", rejected.failure());
    let (_failure, reservation) = rejected.into_parts();
    assert_eq!(reservation.bytes(), reserved);
}
