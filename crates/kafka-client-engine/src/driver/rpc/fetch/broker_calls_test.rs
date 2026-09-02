//! Aggregate broker Fetch response distribution scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};
use kafka_wire::{
    FetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};
use kafka_wire_core::Uuid;

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings, FetchSessionRequest},
};

use super::{
    admission::PartitionFetchRequest, broker_calls::TrackedBrokerFetchCalls, settlement::FetchPoll,
    topic_route::FetchTopicRoute,
};

#[test]
fn aggregate_response_becomes_two_exact_partition_terminals() {
    let requests = requests();
    let fences = requests
        .iter()
        .map(PartitionFetchRequest::fence)
        .collect::<Vec<_>>();
    let mut calls = TrackedBrokerFetchCalls::new(1);
    assert!(calls.has_admission_capacity());
    calls.install_response_for_test(requests, Moment::from_tick(7), 16, response());
    assert!(!calls.has_admission_capacity());

    for (expected_fence, expected_partition) in fences.into_iter().zip([3, 4]) {
        assert_eq!(
            calls.poll_fetch(Moment::from_tick(8)),
            Ok(FetchPoll::TerminalReady {
                fence: expected_fence,
            })
        );
        let terminal = calls
            .begin_fetch_settlement(expected_fence)
            .unwrap_or_else(|error| panic!("begin partition terminal: {error:?}"));
        let response = terminal
            .result()
            .as_ref()
            .unwrap_or_else(|error| panic!("Fetch response: {error:?}"));
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].partitions.len(), 1);
        assert_eq!(
            response.responses[0].partitions[0].partition_index,
            expected_partition
        );
        calls
            .confirm_fetch_settlement(expected_fence)
            .unwrap_or_else(|error| panic!("confirm partition terminal: {error:?}"));
    }
    assert_eq!(calls.poll_fetch(Moment::from_tick(9)), Ok(FetchPoll::Idle));
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.has_admission_capacity());
}

#[test]
fn incremental_response_ignores_cached_partition_outside_current_delta() {
    let mut request = requests().pop().unwrap_or_else(|| panic!("test request"));
    request.bind_session(
        FetchSessionRequest::incremental(91, 1).unwrap_or_else(|| panic!("incremental session")),
    );
    let fence = request.fence();
    let mut response = response();
    response.responses[0].partitions.truncate(1);
    assert_ne!(
        response.responses[0].partitions[0].partition_index,
        i32::try_from(fence.position().partition().partition().get())
            .unwrap_or_else(|error| panic!("partition fits i32: {error}"))
    );
    let mut calls = TrackedBrokerFetchCalls::new(1);
    calls.install_response_for_test(vec![request], Moment::from_tick(7), 16, response);

    assert_eq!(
        calls.poll_fetch(Moment::from_tick(8)),
        Ok(FetchPoll::TerminalReady { fence })
    );
    let terminal = calls
        .begin_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("begin partition terminal: {error:?}"));
    let response = terminal
        .result()
        .as_ref()
        .unwrap_or_else(|error| panic!("Fetch response: {error:?}"));
    assert!(response.responses.is_empty());
}

#[test]
fn response_for_stale_slot_does_not_invalidate_live_partition() {
    let requests = requests();
    let stale = requests[0].fence();
    let live = requests[1].fence();
    let mut calls = TrackedBrokerFetchCalls::new(1);
    calls.install_response_after_stale_request_for_test(
        requests,
        stale,
        Moment::from_tick(7),
        16,
        response(),
    );

    assert_eq!(
        calls.poll_fetch(Moment::from_tick(8)),
        Ok(FetchPoll::StaleConfirmationReady { fence: stale })
    );
    calls
        .confirm_stale_fetch(stale)
        .unwrap_or_else(|error| panic!("confirm stale partition: {error:?}"));
    assert_eq!(
        calls.poll_fetch(Moment::from_tick(8)),
        Ok(FetchPoll::TerminalReady { fence: live })
    );
    let terminal = calls
        .begin_fetch_settlement(live)
        .unwrap_or_else(|error| panic!("begin live partition terminal: {error:?}"));
    let response = terminal
        .result()
        .as_ref()
        .unwrap_or_else(|error| panic!("live Fetch response: {error:?}"));
    assert_eq!(response.responses[0].partitions[0].partition_index, 4);
}

fn response() -> FetchResponse {
    let mut response = FetchResponse::default();
    response.session_id = 91;
    let mut topic = FetchableTopicResponse::default();
    topic.topic_id = Uuid::from_bytes([7; 16]);
    for partition_index in [3, 4] {
        let mut partition = PartitionData::default();
        partition.partition_index = partition_index;
        topic.partitions.push(partition);
    }
    response.responses.push(topic);
    response
}

fn requests() -> Vec<PartitionFetchRequest> {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![
                assigned(TopicId::from_raw(1), 3, 10),
                assigned(TopicId::from_raw(1), 4, 20),
            ],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    transition
        .into_effects()
        .into_iter()
        .map(|effect| {
            let mut request = PartitionFetchRequest::from_effect(
                effect,
                "events".to_owned(),
                FetchRequestSettings::new(500, 1, 1024, 1024, 0),
                FetchDecodeLimits::default(),
                OperationDeadline::from_parts_for_test(
                    Deadline::from_tick(1_000_000_000),
                    Instant::now() + Duration::from_secs(1),
                ),
            )
            .unwrap_or_else(|error| panic!("prepared Fetch: {error:?}"));
            request.bind_topic_route(FetchTopicRoute::new([7; 16], Some(9)));
            request
        })
        .collect()
}

fn assigned(topic_id: TopicId, partition: u32, offset: i64) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(topic_id, PartitionIndex::from_raw(partition)),
        StartPosition::Offset(
            NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("valid offset")),
        ),
    )
}
