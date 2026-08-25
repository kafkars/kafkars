//! Retained Fetch correlation and offset-window evidence tests.

use kafka_wire::{FetchResponse, fetch_response::FetchableTopicResponse};
use kafka_wire_core::Uuid;

use super::{
    FetchDecodeLimits, FetchIsolation, FetchSessionRequest, normalize_session_fetch_outcome,
    outcome_test::{
        PARTITION, REQUESTED_OFFSET, TOPIC, partition, response_with_data_then_control,
    },
    retention::FetchReservationDomain,
};

#[test]
fn v16_success_retains_exact_correlated_uuid_window_and_charge() {
    let (_legacy, records) = response_with_data_then_control();
    let mut partition = partition(0, Some(records));
    partition.high_watermark = 90;
    partition.last_stable_offset = 80;
    partition.log_start_offset = 4;
    let mut topic = FetchableTopicResponse::default();
    topic.topic_id = Uuid::from_bytes([7; 16]);
    topic.partitions = vec![partition];
    let mut response = FetchResponse::default();
    response.throttle_time_ms = 7;
    response.responses = vec![topic];
    let domain = FetchReservationDomain::create_store_domain();
    let (_proof, reservation) = domain.issue_pair(0, usize::MAX);

    let (retained, _session) = normalize_session_fetch_outcome(
        FetchIsolation::ReadUncommitted,
        TOPIC,
        Some([7; 16]),
        PARTITION,
        REQUESTED_OFFSET,
        FetchSessionRequest::INITIAL,
        16,
        response,
        FetchDecodeLimits::default(),
        reservation,
    )
    .unwrap_or_else(|rejected| panic!("v16 evidence: {:?}", rejected.failure()));
    let evidence = retained
        .outcome()
        .evidence()
        .unwrap_or_else(|| panic!("successful Fetch evidence"));

    assert_eq!(evidence.topic_uuid(), Some([7; 16]));
    assert_eq!(evidence.requested_offset(), 10);
    assert_eq!(evidence.next_offset(), 21);
    assert_eq!(evidence.log_start_offset(), Some(4));
    assert_eq!(evidence.last_stable_offset(), Some(80));
    assert_eq!(evidence.high_watermark(), Some(90));
    assert!(evidence.advanced());
    assert_ne!(retained.retained_bytes(), 0);
}
