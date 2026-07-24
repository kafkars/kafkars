//! Raw selected-version and response ownership scenarios for Fetch terminals.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{admission::PartitionFetchRequest, terminal::retain_fetch_terminal};

#[test]
fn raw_terminal_preserves_selected_version_and_uninterpreted_broker_response() {
    let fence = fence();
    let mut response = response("wrong-topic", 99);
    response.error_code = -32_000;
    response.responses[0].partitions[0].error_code = i16::MAX;
    let terminal = retain_fetch_terminal(
        request(fence),
        Moment::from_tick(7),
        Some(ApiVersion::new(12)),
        Ok(response),
    );

    assert_eq!(terminal.fence(), fence);
    assert_eq!(terminal.observed_at(), Moment::from_tick(7));
    assert_eq!(terminal.selected_version(), Some(12));
    let Ok(raw) = terminal.result() else {
        panic!("raw response must remain owned");
    };
    assert_eq!(raw.error_code, -32_000);
    assert_eq!(raw.responses[0].topic.as_str(), "wrong-topic");
    assert_eq!(raw.responses[0].partitions[0].error_code, i16::MAX);
}

#[test]
fn absent_selected_version_is_retained_for_the_composed_outcome_owner() {
    let terminal = retain_fetch_terminal(
        request(fence()),
        Moment::from_tick(9),
        None,
        Ok(response("events", 3)),
    );

    assert_eq!(terminal.selected_version(), None);
    assert!(terminal.result().is_ok());
}

#[test]
fn transport_failure_and_exact_prepared_request_move_together() {
    let fence = fence();
    let terminal = retain_fetch_terminal(
        request(fence),
        Moment::from_tick(11),
        None,
        Err(RequestError::RouteUnavailable),
    );
    let (request, observed_at, selected_version, result) = terminal.into_parts();

    assert_eq!(request.fence(), fence);
    assert_eq!(request.topic(), "events");
    assert_eq!(request.decode_limits(), FetchDecodeLimits::default());
    assert_eq!(observed_at, Moment::from_tick(11));
    assert_eq!(selected_version, None);
    assert!(matches!(result, Err(RequestError::RouteUnavailable)));
}

fn response(topic: &str, partition: i32) -> WireFetchResponse {
    let mut partition_response = PartitionData::default();
    partition_response.partition_index = partition;
    let mut topic_response = FetchableTopicResponse::default();
    topic_response.topic = topic.into();
    topic_response.partitions.push(partition_response);
    let mut response = WireFetchResponse::default();
    response.responses.push(topic_response);
    response
}

fn request(fence: FetchFence) -> PartitionFetchRequest {
    PartitionFetchRequest::from_effect(
        AssignedConsumerEffect::FetchReady {
            fence,
            next_offset: offset(),
        },
        "events".to_owned(),
        FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(1_000_000_000),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

fn fence() -> FetchFence {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Offset(offset()),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let AssignedConsumerEffect::FetchReady { fence, .. } = transition.effects()[0] else {
        panic!("FetchReady effect");
    };
    fence
}

fn offset() -> NextFetchOffset {
    NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("valid offset"))
}
