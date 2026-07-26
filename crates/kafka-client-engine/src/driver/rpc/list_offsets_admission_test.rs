//! Exact deadline pairing and definitely-unsent position admission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionResolutionAttemptFailure,
    StartPosition, TopicId,
};

use crate::{EngineConfig, clock::OperationDeadline};

use super::{
    super::DriverOwner,
    list_offsets_admission::{
        PositionRequestPreparationError, PositionResolutionRequest, submit_position_request,
    },
};
use crate::protocol::consumer::ListOffsetsIsolation;

#[test]
fn request_pairs_the_effect_with_the_exact_operation_deadline() {
    let effect = resolve_effect(Deadline::from_tick(20));
    let mismatch = PositionResolutionRequest::from_effect(
        effect,
        "orders".to_owned(),
        ListOffsetsIsolation::ReadUncommitted,
        deadline(21),
    )
    .err()
    .unwrap_or_else(|| panic!("mismatched deadline must be an invariant fault"));

    assert_eq!(
        mismatch,
        PositionRequestPreparationError::DeadlineMismatch {
            effect: Deadline::from_tick(20),
            operation: Deadline::from_tick(21),
        }
    );
}

#[test]
fn elapsed_deadline_fails_before_request_or_driver_ownership() {
    let mut driver = owner();
    let effect = resolve_effect(Deadline::from_tick(20));
    let fence = resolve_fence(effect);
    let request = PositionResolutionRequest::from_effect(
        effect,
        "orders".to_owned(),
        ListOffsetsIsolation::ReadUncommitted,
        deadline(20),
    )
    .unwrap_or_else(|error| panic!("matching deadline: {error:?}"));
    let failure = submit_position_request(&driver, request, Moment::from_tick(20))
        .err()
        .unwrap_or_else(|| panic!("elapsed request must fail locally"));

    assert_eq!(
        failure.terminal().core_input(),
        AssignedConsumerInput::PositionResolutionFailed {
            fence,
            now: Moment::from_tick(20),
            failure: PositionResolutionAttemptFailure::DeadlineElapsed,
        }
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn invalid_catalog_topic_is_a_fenced_attempt_failure() {
    let mut driver = owner();
    let effect = resolve_effect(Deadline::from_tick(20));
    let fence = resolve_fence(effect);
    let request = PositionResolutionRequest::from_effect(
        effect,
        String::new(),
        ListOffsetsIsolation::ReadUncommitted,
        deadline(20),
    )
    .unwrap_or_else(|error| panic!("matching deadline: {error:?}"));
    let failure = submit_position_request(&driver, request, Moment::from_tick(1))
        .err()
        .unwrap_or_else(|| panic!("empty topic must fail locally"));

    assert_eq!(
        failure.terminal().core_input(),
        AssignedConsumerInput::PositionResolutionFailed {
            fence,
            now: Moment::from_tick(1),
            failure: PositionResolutionAttemptFailure::DriverRejected,
        }
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn resolve_effect(deadline: Deadline) -> AssignedConsumerEffect {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3)),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: deadline,
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    transition.effects()[0]
}

fn resolve_fence(effect: AssignedConsumerEffect) -> kafka_client_core::PositionFence {
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = effect else {
        panic!("resolution effect");
    };
    fence
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
