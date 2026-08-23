//! Strict response correlation, range validation, and resource-bound evidence.

use kafka_wire::{
    ShareFetchResponse,
    share_fetch_response::{
        AcquiredRecords, NodeEndpoint, PartitionData, ShareFetchableTopicResponse,
    },
};
use kafka_wire_core::{Bytes, Uuid};

use super::{
    ShareFetchOutcome, ShareFetchRequestPlan, ShareFetchRequestSettings, ShareFetchRequestTopic,
    ShareFetchResponseFailure, ShareFetchResponseLimits, normalize_share_fetch_response,
    share_fetch_request,
};

#[test]
fn success_retains_exact_ranges_and_raw_record_bytes() {
    let mut response = response();
    response.responses = vec![topic_response(1, vec![partition(0, 0, 2, 3)])];
    response.node_endpoints = vec![endpoint(2, "broker", 9_092, Some("rack-a"))];
    let outcome = normalize(response, ShareFetchResponseLimits::new(8, 16))
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ShareFetchOutcome::Succeeded(success) = outcome else {
        panic!("expected success");
    };
    assert_eq!(success.throttle_time_ms, 7);
    assert_eq!(success.acquisition_lock_timeout_ms, Some(30_000));
    assert_eq!((success.retained_records, success.retained_bytes), (3, 15));
    assert_eq!(success.endpoints[0].node_id, 2);
    assert_eq!(success.endpoints[0].host, Bytes::from_static(b"broker"));
    assert_eq!(success.endpoints[0].port, 9_092);
    assert_eq!(
        success.endpoints[0].rack,
        Some(Bytes::from_static(b"rack-a"))
    );
    let partition = &success.topics[0].partitions[0];
    assert_eq!(partition.partition, 0);
    assert!(partition.rejection.is_none());
    assert_eq!(partition.records, Bytes::from_static(b"raw"));
    assert_eq!(partition.acquired[0].first_offset, 0);
    assert_eq!(partition.acquired[0].last_offset, 2);
    assert_eq!(partition.acquired[0].delivery_count, 3);
}

#[test]
fn unchanged_lock_timeout_is_retained_as_session_state_absence() {
    let mut unchanged = response();
    unchanged.acquisition_lock_timeout_ms = -1;
    let outcome = normalize(unchanged, ShareFetchResponseLimits::new(8, 16))
        .unwrap_or_else(|error| panic!("valid unchanged timeout: {error:?}"));
    let ShareFetchOutcome::Succeeded(success) = outcome else {
        panic!("expected success");
    };
    assert_eq!(success.acquisition_lock_timeout_ms, None);

    let mut invalid = response();
    invalid.acquisition_lock_timeout_ms = 0;
    assert_eq!(
        normalize(invalid, ShareFetchResponseLimits::new(8, 16)),
        Err(ShareFetchResponseFailure::InvalidLockTimeout(0))
    );
}

#[test]
fn top_level_rejection_preserves_exact_code_without_parsing_payload() {
    let mut response = ShareFetchResponse::default();
    response.throttle_time_ms = 9;
    response.error_code = 16;
    let outcome = normalize(response, ShareFetchResponseLimits::new(1, 1))
        .unwrap_or_else(|error| panic!("broker rejection: {error:?}"));
    let ShareFetchOutcome::Rejected(rejection) = outcome else {
        panic!("expected rejection");
    };
    assert_eq!(rejection.throttle_time_ms, 9);
    assert_eq!(rejection.error_code.get(), 16);
}

#[test]
fn correlation_shape_and_partition_errors_fail_closed() {
    let mut unknown = response();
    unknown.responses = vec![topic_response(2, vec![partition(0, 0, 0, 1)])];
    assert_eq!(
        normalize(unknown, ShareFetchResponseLimits::new(8, 16)),
        Err(ShareFetchResponseFailure::UnknownTopic)
    );

    let mut duplicate = response();
    duplicate.responses = vec![topic_response(
        1,
        vec![partition(0, 0, 0, 1), partition(0, 1, 1, 1)],
    )];
    assert_eq!(
        normalize(duplicate, ShareFetchResponseLimits::new(8, 16)),
        Err(ShareFetchResponseFailure::DuplicatePartition(0))
    );

    let mut rejected_payload = response();
    let mut failed = partition(0, 0, 0, 1);
    failed.error_code = 6;
    rejected_payload.responses = vec![topic_response(1, vec![failed])];
    assert_eq!(
        normalize(rejected_payload, ShareFetchResponseLimits::new(8, 16)),
        Err(ShareFetchResponseFailure::PartitionPayloadWithError)
    );

    let mut invalid_endpoint = response();
    invalid_endpoint.node_endpoints = vec![endpoint(1, "", 9_092, None)];
    assert_eq!(
        normalize(invalid_endpoint, ShareFetchResponseLimits::new(8, 16)),
        Err(ShareFetchResponseFailure::EmptyEndpointHost)
    );
}

fn normalize(
    response: ShareFetchResponse,
    limits: ShareFetchResponseLimits,
) -> Result<ShareFetchOutcome, ShareFetchResponseFailure> {
    let prepared = share_fetch_request(
        "workers",
        "member-a",
        0,
        ShareFetchRequestSettings {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_024,
            max_records: 32,
            batch_size: 8,
        },
        ShareFetchRequestPlan::try_new(
            vec![request_topic(1, &[0])],
            vec![request_topic(1, &[0])],
            vec![],
        )
        .unwrap_or_else(|error| panic!("plan: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("request: {error:?}"));
    let (_request, correlation) = prepared.into_parts();
    normalize_share_fetch_response(1, response, &correlation, limits)
}

fn response() -> ShareFetchResponse {
    let mut response = ShareFetchResponse::default();
    response.throttle_time_ms = 7;
    response.acquisition_lock_timeout_ms = 30_000;
    response
}

fn topic_response(value: u8, partitions: Vec<PartitionData>) -> ShareFetchableTopicResponse {
    let mut topic = ShareFetchableTopicResponse::default();
    topic.topic_id = Uuid::from_bytes(id(value));
    topic.partitions = partitions;
    topic
}

fn partition(index: i32, first: i64, last: i64, delivery_count: i16) -> PartitionData {
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.records = Bytes::from_static(b"raw");
    partition.acquired_records = vec![acquired(first, last, delivery_count)];
    partition
}

fn acquired(first: i64, last: i64, delivery_count: i16) -> AcquiredRecords {
    let mut acquired = AcquiredRecords::default();
    acquired.first_offset = first;
    acquired.last_offset = last;
    acquired.delivery_count = delivery_count;
    acquired
}

fn endpoint(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> NodeEndpoint {
    let mut endpoint = NodeEndpoint::default();
    endpoint.node_id = node_id;
    endpoint.host = host.into();
    endpoint.port = port;
    endpoint.rack = rack.map(Into::into);
    endpoint
}

fn request_topic(value: u8, partitions: &[u32]) -> ShareFetchRequestTopic {
    ShareFetchRequestTopic::try_new(id(value), partitions.to_vec())
        .unwrap_or_else(|error| panic!("topic: {error:?}"))
}

fn id(value: u8) -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = value;
    id
}
