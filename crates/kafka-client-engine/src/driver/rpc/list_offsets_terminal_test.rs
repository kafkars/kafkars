//! Version, response, and throttle settlement scenarios for position lookup.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence, StartPosition,
    TopicId,
};
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use crate::protocol::consumer::ListOffsetsIsolation;

use super::list_offsets_terminal::normalize_position_terminal;

#[test]
fn selected_version_controls_isolation_and_throttle_meaning() {
    let fence = fence();
    let response = success_response("orders", 3, 47);
    let uncommitted_v1 = normalize_position_terminal(
        fence,
        "orders",
        ListOffsetsIsolation::ReadUncommitted,
        Moment::from_tick(5),
        Some(ApiVersion::new(1)),
        Ok(response.clone()),
    );
    assert_eq!(
        uncommitted_v1.core_input(),
        AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: kafka_client_core::NextFetchOffset::try_from_raw(42)
                .unwrap_or_else(|| panic!("valid offset")),
            now: Moment::from_tick(5),
            throttle_ticks: 0,
        }
    );

    let committed_v2 = normalize_position_terminal(
        fence,
        "orders",
        ListOffsetsIsolation::ReadCommitted,
        Moment::from_tick(6),
        Some(ApiVersion::new(2)),
        Ok(response),
    );
    assert_eq!(
        committed_v2.core_input(),
        AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: kafka_client_core::NextFetchOffset::try_from_raw(42)
                .unwrap_or_else(|| panic!("valid offset")),
            now: Moment::from_tick(6),
            throttle_ticks: 47_000_000,
        }
    );
}

#[test]
fn absent_or_incompatible_selected_version_fails_the_exact_fence() {
    let fence = fence();
    for selected in [None, Some(ApiVersion::new(1)), Some(ApiVersion::new(12))] {
        let terminal = normalize_position_terminal(
            fence,
            "orders",
            ListOffsetsIsolation::ReadCommitted,
            Moment::from_tick(9),
            selected,
            Ok(success_response("orders", 3, 0)),
        );
        assert_eq!(
            terminal.core_input(),
            AssignedConsumerInput::PositionResolutionFailed {
                fence,
                now: Moment::from_tick(9),
            }
        );
    }
}

#[test]
fn broker_transport_and_structural_failures_share_core_owned_precedence() {
    let fence = fence();
    let failures = [
        (
            Some(ApiVersion::new(11)),
            Err(RequestError::RouteUnavailable),
        ),
        (
            Some(ApiVersion::new(11)),
            Ok(success_response("wrong-topic", 3, 0)),
        ),
        (
            Some(ApiVersion::new(11)),
            Ok(broker_error_response("orders", 3)),
        ),
    ];
    for (selected, result) in failures {
        assert_eq!(
            normalize_position_terminal(
                fence,
                "orders",
                ListOffsetsIsolation::ReadUncommitted,
                Moment::from_tick(13),
                selected,
                result,
            )
            .core_input(),
            AssignedConsumerInput::PositionResolutionFailed {
                fence,
                now: Moment::from_tick(13),
            }
        );
    }
}

fn fence() -> PositionFence {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(7), PartitionIndex::from_raw(3)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = transition.effects()[0] else {
        panic!("beginning assignment must resolve");
    };
    fence
}

fn success_response(topic: &str, partition: i32, throttle_time_ms: i32) -> ListOffsetsResponse {
    response(topic, partition, throttle_time_ms, 0)
}

fn broker_error_response(topic: &str, partition: i32) -> ListOffsetsResponse {
    response(topic, partition, 0, 6)
}

fn response(
    topic: &str,
    partition: i32,
    throttle_time_ms: i32,
    error_code: i16,
) -> ListOffsetsResponse {
    let mut partition_response = ListOffsetsPartitionResponse::default();
    partition_response.partition_index = partition;
    partition_response.error_code = error_code;
    partition_response.offset = 42;
    partition_response.timestamp = -1;
    partition_response.leader_epoch = -1;
    let mut topic_response = ListOffsetsTopicResponse::default();
    topic_response.name = topic.into();
    topic_response.partitions.push(partition_response);
    let mut response = ListOffsetsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics.push(topic_response);
    response
}
