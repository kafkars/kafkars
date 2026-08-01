//! Forgotten-only Fetch-session response normalization scenarios.

use kafka_wire::{FetchResponse as WireFetchResponse, fetch_response::FetchableTopicResponse};

use super::{
    FetchBrokerLevel, FetchOutcomeFailure, FetchResponseFailure, FetchSessionRequest,
    FetchSessionUpdate, normalize_forgotten_fetch_outcome,
};

#[test]
fn empty_incremental_response_advances_the_exact_session() {
    let session =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"));
    let mut response = WireFetchResponse::default();
    response.throttle_time_ms = 7;
    response.session_id = 91;

    let outcome = normalize_forgotten_fetch_outcome(session, 12, response)
        .unwrap_or_else(|error| panic!("forgotten response: {error:?}"));

    assert_eq!(outcome.throttle_ticks(), Some(7_000_000));
    let Some(FetchSessionUpdate::Continue(next)) = outcome.session() else {
        panic!("continued session");
    };
    assert_eq!((next.session_id(), next.session_epoch()), (91, 4));
}

#[test]
fn top_level_failure_precedes_success_only_shape() {
    let session =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"));
    let mut response = WireFetchResponse::default();
    response.error_code = 70;
    response.responses.push(FetchableTopicResponse::default());

    let outcome = normalize_forgotten_fetch_outcome(session, 12, response)
        .unwrap_or_else(|error| panic!("broker failure: {error:?}"));
    let failure = outcome
        .broker_failure()
        .unwrap_or_else(|| panic!("broker failure outcome"));

    assert_eq!(failure.level(), FetchBrokerLevel::TopLevel);
    assert_eq!(failure.code().get(), 70);
    assert_eq!(outcome.session(), None);
}

#[test]
fn control_only_success_rejects_partition_payloads_and_pre_session_versions() {
    let session =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"));
    let mut response = WireFetchResponse::default();
    response.session_id = 91;
    response.responses.push(FetchableTopicResponse::default());

    assert_eq!(
        normalize_forgotten_fetch_outcome(session, 12, response),
        Err(FetchOutcomeFailure::Response(
            FetchResponseFailure::TopicCount { actual: 1 }
        ))
    );
    assert_eq!(
        normalize_forgotten_fetch_outcome(session, 6, WireFetchResponse::default()),
        Err(FetchOutcomeFailure::Response(
            FetchResponseFailure::UnsupportedApiVersion { actual: 6 }
        ))
    );
}
