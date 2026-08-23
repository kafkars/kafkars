//! Exact v1 response correlation, diagnostics, leader, and failure evidence.

use bytes::Bytes;
use kafka_wire::{
    ShareAcknowledgeResponse,
    share_acknowledge_response::{
        LeaderIdAndEpoch, NodeEndpoint, PartitionData, ShareAcknowledgeTopicResponse,
    },
};
use kafka_wire_core::Uuid;

use super::{
    ShareAcknowledgeOutcome, ShareAcknowledgeResponseFailure,
    request::share_acknowledge_request,
    response::normalize_share_acknowledge_response,
    test_support::{id, prepared_acknowledgement},
};

#[test]
fn success_restores_request_order_and_preserves_partition_diagnostics() {
    let mut response = response();
    response.responses = vec![
        topic(2, vec![partition(0, 0, None, -1, -1)]),
        topic(
            1,
            vec![
                partition(1, 6, Some("moved"), 2, 3),
                partition(0, 0, None, -1, -1),
            ],
        ),
    ];
    response.node_endpoints = vec![endpoint(2, "broker", 9_092, Some("rack-a"))];
    let ShareAcknowledgeOutcome::Succeeded(success) =
        normalize(response).unwrap_or_else(|error| panic!("valid response: {error:?}"))
    else {
        panic!("expected success");
    };
    assert_eq!(success.throttle_time_ms, 7);
    assert_eq!(success.outcomes.len(), 3);
    assert_eq!(
        (success.outcomes[0].topic_id, success.outcomes[0].partition),
        (id(1), 0)
    );
    assert_eq!(
        (success.outcomes[1].topic_id, success.outcomes[1].partition),
        (id(1), 1)
    );
    assert_eq!(
        success.outcomes[1]
            .error_code
            .map(core::num::NonZeroI16::get),
        Some(6)
    );
    assert_eq!(
        success.outcomes[1].error_message,
        Some(Bytes::from_static(b"moved"))
    );
    assert_eq!(success.outcomes[1].current_leader, Some((2, 3)));
    assert_eq!(success.endpoints[0].host, Bytes::from_static(b"broker"));
    assert_eq!(
        success.endpoints[0].rack,
        Some(Bytes::from_static(b"rack-a"))
    );
}

#[test]
fn top_level_rejection_preserves_exact_code_and_message() {
    let mut response = response();
    response.error_code = 16;
    response.error_message = Some("coordinator".into());
    let ShareAcknowledgeOutcome::Rejected(rejection) =
        normalize(response).unwrap_or_else(|error| panic!("broker rejection: {error:?}"))
    else {
        panic!("expected rejection");
    };
    assert_eq!(rejection.throttle_time_ms, 7);
    assert_eq!(rejection.error_code.get(), 16);
    assert_eq!(
        rejection.error_message,
        Some(Bytes::from_static(b"coordinator"))
    );
}

#[test]
fn incomplete_unknown_and_duplicate_correlation_fail_closed() {
    let mut missing = response();
    missing.responses = vec![topic(1, vec![partition(0, 0, None, -1, -1)])];
    assert_eq!(
        normalize(missing),
        Err(ShareAcknowledgeResponseFailure::MissingPartition)
    );

    let mut unknown = response();
    unknown.responses = vec![topic(3, vec![partition(0, 0, None, -1, -1)])];
    assert_eq!(
        normalize(unknown),
        Err(ShareAcknowledgeResponseFailure::UnknownTopic)
    );

    let mut duplicate = complete_response();
    duplicate.responses[0]
        .partitions
        .push(partition(0, 0, None, -1, -1));
    assert_eq!(
        normalize(duplicate),
        Err(ShareAcknowledgeResponseFailure::DuplicatePartition(0))
    );
}

#[test]
fn version_v2_field_and_leader_shapes_fail_closed() {
    assert_eq!(
        normalize_share_acknowledge_response(0, response(), &correlation()),
        Err(ShareAcknowledgeResponseFailure::UnsupportedApiVersion(0))
    );
    let mut v2 = response();
    v2.acquisition_lock_timeout_ms = 1;
    assert_eq!(
        normalize(v2),
        Err(ShareAcknowledgeResponseFailure::UnexpectedV2LockTimeout(1))
    );

    let mut leader = complete_response();
    let mut current_leader = LeaderIdAndEpoch::default();
    current_leader.leader_id = 2;
    current_leader.leader_epoch = 3;
    leader.responses[0].partitions[0].current_leader = current_leader;
    assert_eq!(
        normalize(leader),
        Err(ShareAcknowledgeResponseFailure::MissingLeaderEndpoint(2))
    );
}

fn normalize(
    response: ShareAcknowledgeResponse,
) -> Result<ShareAcknowledgeOutcome, ShareAcknowledgeResponseFailure> {
    normalize_share_acknowledge_response(1, response, &correlation())
}

fn correlation() -> super::ShareAcknowledgeCorrelation {
    let (attempt, acknowledgement) = prepared_acknowledgement();
    let prepared = share_acknowledge_request("workers", "member-a", attempt, &acknowledgement)
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    prepared.into_parts().1
}

fn response() -> ShareAcknowledgeResponse {
    let mut response = ShareAcknowledgeResponse::default();
    response.throttle_time_ms = 7;
    response
}

fn complete_response() -> ShareAcknowledgeResponse {
    let mut response = response();
    response.responses = vec![
        topic(
            1,
            vec![partition(0, 0, None, -1, -1), partition(1, 0, None, -1, -1)],
        ),
        topic(2, vec![partition(0, 0, None, -1, -1)]),
    ];
    response
}

fn topic(value: u8, partitions: Vec<PartitionData>) -> ShareAcknowledgeTopicResponse {
    let mut topic = ShareAcknowledgeTopicResponse::default();
    topic.topic_id = Uuid::from_bytes(id(value));
    topic.partitions = partitions;
    topic
}

fn partition(
    index: i32,
    code: i16,
    message: Option<&str>,
    leader: i32,
    epoch: i32,
) -> PartitionData {
    let mut current_leader = LeaderIdAndEpoch::default();
    current_leader.leader_id = leader;
    current_leader.leader_epoch = epoch;
    let mut partition = PartitionData::default();
    partition.partition_index = index;
    partition.error_code = code;
    partition.error_message = message.map(Into::into);
    partition.current_leader = current_leader;
    partition
}

fn endpoint(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> NodeEndpoint {
    let mut endpoint = NodeEndpoint::default();
    endpoint.node_id = node_id;
    endpoint.host = host.into();
    endpoint.port = port;
    endpoint.rack = rack.map(Into::into);
    endpoint
}
