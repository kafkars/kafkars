//! Bounded admission, active stale-drain, and completion-recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};
use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    admission::{FetchAdmissionFailureSource, FetchCallAdmission, PartitionFetchRequest},
    calls::TrackedFetchCalls,
};

#[test]
fn full_capacity_returns_the_exact_prepared_fetch_without_driver_submission() {
    let mut driver = owner();
    let (fences, _) = assignment(&[3, 4]);
    let mut calls = TrackedFetchCalls::new(1);
    calls.install_terminal_for_test(super::settlement_owner_test::terminal(fences[0], 42));
    let request = request(fetch_effect(fences[1], 52), "unique-events");
    let expected_deadline = request.operation_deadline();

    let FetchCallAdmission::Backpressured(returned) =
        calls.try_submit_fetch(&driver, request, Moment::from_tick(0))
    else {
        panic!("full registry must return the exact prepared Fetch");
    };
    assert_eq!(returned.fence(), fences[1]);
    assert_eq!(returned.next_offset(), offset(52));
    assert_eq!(returned.topic(), "unique-events");
    assert_eq!(returned.operation_deadline(), expected_deadline);
    assert_eq!(calls.retained_count(), 1);
    shutdown(&mut driver);
}

#[test]
fn accepted_driver_call_consumes_one_preflighted_slot() {
    let mut driver = owner();
    let (fences, _) = assignment(&[3]);
    let mut calls = TrackedFetchCalls::new(1);

    assert!(matches!(
        calls.try_submit_fetch(
            &driver,
            request(fetch_effect(fences[0], 42), "events"),
            Moment::from_tick(0),
        ),
        FetchCallAdmission::Accepted
    ));
    assert_eq!(calls.retained_count(), 1);
    drop(calls);
    shutdown(&mut driver);
}

#[test]
fn elapsed_deadline_precedes_full_registry_backpressure() {
    let mut driver = owner();
    let (fences, _) = assignment(&[3, 4]);
    let mut calls = TrackedFetchCalls::new(1);
    calls.install_terminal_for_test(super::settlement_owner_test::terminal(fences[0], 42));
    let request = request_with_deadline(
        fetch_effect(fences[1], 52),
        "events",
        Deadline::from_tick(5),
    );

    let FetchCallAdmission::Rejected(failure) =
        calls.try_submit_fetch(&driver, request, Moment::from_tick(5))
    else {
        panic!("elapsed deadline must precede capacity backpressure");
    };
    let (returned, source) = failure.into_parts();
    assert_eq!(returned.fence(), fences[1]);
    assert!(matches!(
        source,
        FetchAdmissionFailureSource::DeadlineElapsed
    ));
    assert_eq!(calls.retained_count(), 1);
    shutdown(&mut driver);
}

#[test]
fn accepted_stale_call_returns_request_before_the_driver_call_drains() {
    let mut driver = owner();
    let (fences, mut machine) = assignment(&[3]);
    let mut calls = TrackedFetchCalls::new(1);
    assert!(matches!(
        calls.try_submit_fetch(
            &driver,
            request(fetch_effect(fences[0], 42), "events"),
            Moment::from_tick(0),
        ),
        FetchCallAdmission::Accepted
    ));
    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fences[0].position().assignment_epoch(),
            partition: fences[0].position().partition(),
            position: StartPosition::Offset(offset(52)),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"));
    let drains = calls
        .observe_fetch_control(seek.effects()[0])
        .unwrap_or_else(|pending| panic!("no settlement pending: {:?}", pending.fence));
    let returned = drains.into_requests();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].fence(), fences[0]);
    assert_eq!(calls.retained_count(), 1);

    shutdown(&mut driver);
    let (recovered, completion) = calls.recover_fetches_after_driver_shutdown().into_parts();
    assert!(recovered.is_empty());
    assert!(completion.is_none());
}

#[test]
fn completion_corruption_recovers_the_exact_prepared_request_after_shutdown() {
    let (fences, _) = assignment(&[3]);
    let mut calls = TrackedFetchCalls::new(1);
    let request = request(fetch_effect(fences[0], 42), "events");
    calls.install_completion_failure_for_test(request, CompletionError::Consumed);

    let observation = calls
        .poll_fetch(Moment::from_tick(5))
        .err()
        .unwrap_or_else(|| panic!("completion corruption must remain fatal"));
    assert_eq!(observation.fence(), fences[0]);
    assert!(observation.is_consumed());
    assert_eq!(calls.retained_count(), 1);
    let (requests, recovered_failure) = calls.recover_fetches_after_driver_shutdown().into_parts();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].fence(), fences[0]);
    assert_eq!(recovered_failure, Some(observation));
    assert_eq!(calls.retained_count(), 0);
}

fn assignment(partitions: &[u32]) -> (Vec<FetchFence>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let partitions = partitions
        .iter()
        .map(|partition| {
            AssignedPartition::new(
                topic_partition(*partition),
                StartPosition::Offset(offset(42)),
            )
        })
        .collect();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let fences = transition
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .collect();
    (fences, machine)
}

fn fetch_effect(fence: FetchFence, raw_offset: i64) -> AssignedConsumerEffect {
    AssignedConsumerEffect::FetchReady {
        fence,
        next_offset: offset(raw_offset),
    }
}

fn request(effect: AssignedConsumerEffect, topic: &str) -> PartitionFetchRequest {
    request_with_deadline(effect, topic, Deadline::from_tick(1_000_000_000))
}

fn request_with_deadline(
    effect: AssignedConsumerEffect,
    topic: &str,
    deadline: Deadline,
) -> PartitionFetchRequest {
    PartitionFetchRequest::from_effect(
        effect,
        topic.to_owned(),
        FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(deadline, Instant::now() + Duration::from_secs(1)),
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

fn topic_partition(partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition))
}

fn offset(raw: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(raw).unwrap_or_else(|| panic!("valid offset"))
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}

fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
