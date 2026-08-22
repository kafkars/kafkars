//! Exact-broker generated Fetch request construction scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{
        FetchDecodeLimits, FetchRequestSettings, FetchSessionRequest, ForgottenFetchPartition,
    },
};

use super::{
    admission::PartitionFetchRequest, broker_admission::generated_broker_fetch_request,
    topic_route::FetchTopicRoute,
};

#[test]
fn broker_request_carries_bound_session_and_forgotten_partitions() {
    let mut request = request("events");
    let session = FetchSessionRequest::incremental(91, 3)
        .unwrap_or_else(|| panic!("positive session metadata"));
    request.bind_session(session);
    let (generated, _deadline) = generated_broker_fetch_request(
        std::slice::from_ref(&request),
        &[ForgottenFetchPartition::new("old", [8; 16], 7)],
        Moment::from_tick(0),
    )
    .unwrap_or_else(|error| panic!("broker request: {error:?}"));

    assert_eq!((generated.session_id, generated.session_epoch), (91, 3));
    assert_eq!(generated.topics[0].topic.as_str(), "events");
    assert_eq!(generated.topics[0].topic_id.to_bytes(), [7; 16]);
    assert_eq!(generated.topics[0].partitions[0].current_leader_epoch, 9);
    assert_eq!(generated.forgotten_topics_data[0].topic.as_str(), "old");
    assert_eq!(
        generated.forgotten_topics_data[0].topic_id.to_bytes(),
        [8; 16]
    );
    assert_eq!(generated.forgotten_topics_data[0].partitions, vec![7]);
}

#[test]
fn broker_request_aggregates_partitions_and_uses_earliest_deadline() {
    let first = request_with_deadline("events", 3, 100_000_010, Duration::from_secs(2));
    let second = request_with_deadline("events", 4, 50_000_010, Duration::from_secs(1));
    let expected_deadline = second.operation_deadline().transport();
    let (generated, deadline) =
        generated_broker_fetch_request(&[first, second], &[], Moment::from_tick(10))
            .unwrap_or_else(|error| panic!("broker request: {error:?}"));

    assert_eq!(deadline, expected_deadline);
    assert_eq!(generated.max_wait_ms, 50);
    assert_eq!(generated.topics.len(), 1);
    assert_eq!(
        generated.topics[0]
            .partitions
            .iter()
            .map(|partition| partition.partition)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
}

fn request(topic: &str) -> PartitionFetchRequest {
    request_with_deadline(topic, 3, 100, Duration::from_secs(1))
}

fn request_with_deadline(
    topic: &str,
    partition: u32,
    deadline: u64,
    transport_after: Duration,
) -> PartitionFetchRequest {
    let offset = NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("valid offset"));
    let mut machine = AssignedConsumerMachine::new();
    let effect = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(partition),
                ),
                StartPosition::Offset(offset),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(deadline),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"))
        .effects()[0];
    let mut request = PartitionFetchRequest::from_effect(
        effect,
        topic.to_owned(),
        FetchRequestSettings::new(500, 1, 1024, 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(deadline),
            Instant::now() + transport_after,
        ),
    )
    .unwrap_or_else(|error| panic!("prepared Fetch: {error:?}"));
    request.bind_topic_route(FetchTopicRoute::new([7; 16], Some(9)));
    request
}
