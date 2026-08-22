//! Prepared Fetch identity, deadline, and definitely-unsent admission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition,
    TopicId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchDecodeLimits, FetchRequestFailure, FetchRequestSettings},
};

use super::{
    admission::{FetchAdmissionFailureSource, FetchRequestPreparationError, PartitionFetchRequest},
    legacy_request::generated_fetch_request,
    partition_submission::submit_partition_fetch,
};

#[test]
fn only_one_exact_fetch_ready_effect_can_be_prepared() {
    let unexpected = resolve_effect();
    let error = PartitionFetchRequest::from_effect(
        unexpected,
        "events".to_owned(),
        settings(),
        FetchDecodeLimits::default(),
        deadline(),
    )
    .err()
    .unwrap_or_else(|| panic!("position resolution is not a Fetch"));

    assert_eq!(error, FetchRequestPreparationError::UnexpectedEffect);
}

#[test]
fn prepared_request_retains_fence_offset_topic_and_original_deadline() {
    let effect = fetch_effect(3);
    let expected_fence = fetch_fence(effect);
    let expected_offset = fetch_offset(effect);
    let deadline = deadline();
    let request = PartitionFetchRequest::from_effect(
        effect,
        "events".to_owned(),
        settings(),
        FetchDecodeLimits::default(),
        deadline,
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"));

    assert_eq!(request.fence(), expected_fence);
    assert_eq!(request.next_offset(), expected_offset);
    assert_eq!(request.topic(), "events");
    assert_eq!(request.operation_deadline(), deadline);
}

#[test]
fn invalid_request_returns_the_exact_prepared_owner_before_driver_admission() {
    let mut driver = owner();
    let effect = fetch_effect(3);
    let expected_fence = fetch_fence(effect);
    let request = PartitionFetchRequest::from_effect(
        effect,
        String::new(),
        settings(),
        FetchDecodeLimits::default(),
        deadline(),
    )
    .unwrap_or_else(|error| panic!("prepare invalid topic: {error:?}"));
    let failure = submit_partition_fetch(&driver, request, Moment::from_tick(0))
        .err()
        .unwrap_or_else(|| panic!("empty topic must fail locally"));

    let (returned, source) = failure.into_parts();
    assert!(matches!(
        source,
        FetchAdmissionFailureSource::Request(FetchRequestFailure::EmptyTopic)
    ));
    assert_eq!(returned.fence(), expected_fence);
    assert_eq!(returned.topic(), "");
    shutdown(&mut driver);
}

#[test]
fn deadline_precedes_request_validation_and_caps_broker_max_wait() {
    let elapsed = PartitionFetchRequest::from_effect(
        fetch_effect(3),
        String::new(),
        settings(),
        FetchDecodeLimits::default(),
        operation_deadline(20),
    )
    .unwrap_or_else(|error| panic!("prepare elapsed Fetch: {error:?}"));
    assert!(matches!(
        generated_fetch_request(&elapsed, Moment::from_tick(20)),
        Err(FetchAdmissionFailureSource::DeadlineElapsed)
    ));

    let capped = PartitionFetchRequest::from_effect(
        fetch_effect(3),
        "events".to_owned(),
        settings(),
        FetchDecodeLimits::default(),
        operation_deadline(37_000_000),
    )
    .unwrap_or_else(|error| panic!("prepare capped Fetch: {error:?}"));
    let (generated, partition) = generated_fetch_request(&capped, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("generate capped Fetch: {error:?}"));
    assert_eq!(generated.max_wait_ms, 37);
    assert_eq!(partition, 3);
}

fn fetch_effect(partition: u32) -> AssignedConsumerEffect {
    let offset = NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("valid offset"));
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(partition),
                ),
                StartPosition::Offset(offset),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"))
        .effects()[0]
}

fn resolve_effect() -> AssignedConsumerEffect {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"))
        .effects()[0]
}

fn fetch_fence(effect: AssignedConsumerEffect) -> kafka_client_core::FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

fn fetch_offset(effect: AssignedConsumerEffect) -> NextFetchOffset {
    let AssignedConsumerEffect::FetchReady { next_offset, .. } = effect else {
        panic!("FetchReady effect");
    };
    next_offset
}

fn settings() -> FetchRequestSettings {
    FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0)
}

fn deadline() -> OperationDeadline {
    operation_deadline(100)
}

fn operation_deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
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
