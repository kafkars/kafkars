//! API 68 v0 success, rejection, canonical assignment, and malformed response evidence.

use kafka_wire::{
    ConsumerGroupHeartbeatResponse,
    consumer_group_heartbeat_response::{Assignment, TopicPartitions},
};
use kafka_wire_core::Uuid;

use super::{
    ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatResponseFailure,
    normalize_consumer_group_heartbeat_response,
};

#[test]
fn success_retains_member_epoch_cadence_throttle_and_canonical_assignment() {
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.throttle_time_ms = 4;
    response.member_id = Some("member-a".into());
    response.member_epoch = 7;
    response.heartbeat_interval_ms = 5_000;
    let mut assignment = Assignment::default();
    assignment.topic_partitions = vec![topic([2; 16], vec![4, 1]), topic([1; 16], vec![3])];
    response.assignment = Some(assignment);

    let outcome = normalize_consumer_group_heartbeat_response(0, &response)
        .unwrap_or_else(|error| panic!("normalize: {error:?}"));
    let ConsumerGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("successful response must normalize as success")
    };
    let (throttle, member, epoch, interval, assignment) = success.into_parts();
    assert_eq!(throttle, 4);
    assert_eq!(member.as_deref(), Some("member-a"));
    assert_eq!(epoch, 7);
    assert_eq!(interval, 5_000);
    let assignment = assignment.unwrap_or_else(|| panic!("assignment"));
    assert_eq!(assignment[0].topic_id(), [1; 16]);
    assert_eq!(assignment[0].partitions(), [3]);
    assert_eq!(assignment[1].topic_id(), [2; 16]);
    assert_eq!(assignment[1].partitions(), [1, 4]);
}

#[test]
fn broker_rejection_retains_exact_code_and_throttle_without_validating_success_fields() {
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.throttle_time_ms = 9;
    response.error_code = 15;
    response.heartbeat_interval_ms = -1;
    let outcome = normalize_consumer_group_heartbeat_response(0, &response)
        .unwrap_or_else(|error| panic!("broker rejection: {error:?}"));
    let ConsumerGroupHeartbeatOutcome::Rejected(rejection) = outcome else {
        panic!("nonzero error must reject")
    };
    assert_eq!(rejection.throttle_time_ms(), 9);
    assert_eq!(rejection.error_code().get(), 15);
}

#[test]
fn malformed_success_rejects_zero_topics_negative_and_duplicate_partitions() {
    let mut response = success_response();
    response.assignment = Some(assignment(vec![topic([0; 16], vec![0])]));
    assert_eq!(
        failure(&response),
        ConsumerGroupHeartbeatResponseFailure::ZeroTopicId
    );

    response.assignment = Some(assignment(vec![topic([1; 16], vec![-1])]));
    assert_eq!(
        failure(&response),
        ConsumerGroupHeartbeatResponseFailure::NegativePartition(-1)
    );

    response.assignment = Some(assignment(vec![topic([1; 16], vec![2, 2])]));
    assert_eq!(
        failure(&response),
        ConsumerGroupHeartbeatResponseFailure::DuplicatePartition(2)
    );
}

#[test]
fn unsupported_version_and_negative_throttle_never_cross_the_seam() {
    let response = success_response();
    assert_eq!(
        normalize_consumer_group_heartbeat_response(1, &response)
            .err()
            .unwrap_or_else(|| panic!("v1 is outside first beta ownership")),
        ConsumerGroupHeartbeatResponseFailure::UnsupportedApiVersion(1)
    );
    let mut response = response;
    response.throttle_time_ms = -1;
    assert_eq!(
        failure(&response),
        ConsumerGroupHeartbeatResponseFailure::NegativeThrottleTime(-1)
    );
}

fn success_response() -> ConsumerGroupHeartbeatResponse {
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.member_id = Some("member".into());
    response.member_epoch = 1;
    response.heartbeat_interval_ms = 5_000;
    response
}

fn assignment(topic_partitions: Vec<TopicPartitions>) -> Assignment {
    let mut assignment = Assignment::default();
    assignment.topic_partitions = topic_partitions;
    assignment
}

fn topic(topic_id: [u8; 16], partitions: Vec<i32>) -> TopicPartitions {
    let mut topic = TopicPartitions::default();
    topic.topic_id = Uuid::from_bytes(topic_id);
    topic.partitions = partitions;
    topic
}

fn failure(response: &ConsumerGroupHeartbeatResponse) -> ConsumerGroupHeartbeatResponseFailure {
    normalize_consumer_group_heartbeat_response(0, response)
        .err()
        .unwrap_or_else(|| panic!("malformed response must reject"))
}
