//! Generated KIP-848 response fixtures normalized behind the protocol boundary.

use kafka_wire::{
    ConsumerGroupHeartbeatResponse,
    consumer_group_heartbeat_response::{Assignment, TopicPartitions},
};
use kafka_wire_core::Uuid;

use super::{
    ConsumerGroupHeartbeatOutcome, ConsumerGroupHeartbeatSuccess,
    normalize_consumer_group_heartbeat_response,
};

pub(crate) fn consumer_group_heartbeat_success_for_test(
    member_epoch: i32,
    partition: i32,
) -> ConsumerGroupHeartbeatSuccess {
    let mut response = base_success(member_epoch);
    let mut topic = TopicPartitions::default();
    topic.topic_id = Uuid::from_bytes([7; 16]);
    topic.partitions = vec![partition];
    let mut assignment = Assignment::default();
    assignment.topic_partitions = vec![topic];
    response.assignment = Some(assignment);
    normalize_success(&response)
}

pub(crate) fn consumer_group_heartbeat_success_without_assignment_for_test(
    member_epoch: i32,
) -> ConsumerGroupHeartbeatSuccess {
    normalize_success(&base_success(member_epoch))
}

fn base_success(member_epoch: i32) -> ConsumerGroupHeartbeatResponse {
    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.member_id = Some("member-a".into());
    response.member_epoch = member_epoch;
    response.heartbeat_interval_ms = 5_000;
    response
}

fn normalize_success(response: &ConsumerGroupHeartbeatResponse) -> ConsumerGroupHeartbeatSuccess {
    let outcome = normalize_consumer_group_heartbeat_response(0, response)
        .unwrap_or_else(|error| panic!("normalize generated heartbeat fixture: {error:?}"));
    let ConsumerGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("generated heartbeat fixture must normalize as success")
    };
    success
}
