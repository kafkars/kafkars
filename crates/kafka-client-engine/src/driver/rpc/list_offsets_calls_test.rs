//! Bounded ownership, fencing, and fatal recovery scenarios for position calls.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence, StartPosition,
    TopicId,
};
use kafka_driver::CompletionError;

use crate::{EngineConfig, clock::OperationDeadline};

use super::{
    super::DriverOwner, list_offsets_admission::PositionResolutionRequest,
    list_offsets_calls::TrackedPositionCalls,
};
use crate::protocol::consumer::ListOffsetsIsolation;

#[test]
fn permit_preflight_keeps_accepted_capacity_bounded() {
    let mut driver = owner();
    let (effect, _) = assignment();
    let mut calls = TrackedPositionCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("first position slot"))
        .submit(&driver, request(effect), Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("tracked position admission: {error:?}"));

    assert_eq!(calls.retained_count(), 1);
    assert!(calls.try_reserve().is_none());
    drop(calls);
    shutdown(&mut driver);
}

#[test]
fn superseded_accepted_call_drains_before_releasing_capacity() {
    let mut driver = owner();
    let (effect, mut machine) = assignment();
    let fence = resolve_fence(effect);
    let mut calls = TrackedPositionCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("position slot"))
        .submit(
            &driver,
            request_with_transport(effect, Duration::from_millis(25)),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked position admission: {error:?}"));
    let suspend = seek_suspend(&mut machine, fence);
    calls.observe_control(suspend);

    assert_eq!(calls.retained_count(), 1);
    assert!(calls.try_reserve().is_none());
    for _turn in 0..64 {
        let _ = driver
            .turn(Duration::from_millis(5))
            .unwrap_or_else(|error| panic!("bounded driver turn: {error}"));
        let settled = calls
            .poll_next_ready(Moment::from_tick(30_000_000))
            .unwrap_or_else(|error| panic!("completion ownership: {error:?}"));
        assert!(settled.is_none());
        if calls.retained_count() == 0 {
            break;
        }
    }
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
    shutdown(&mut driver);
}

#[test]
fn settled_terminal_is_retained_until_explicit_core_confirmation() {
    let (effect, _) = assignment();
    let fence = resolve_fence(effect);
    let mut calls = TrackedPositionCalls::new(1);
    calls.install_terminal_for_test(fence, Moment::from_tick(7));

    let terminal = calls
        .poll_next_ready(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("completion ownership: {error:?}"))
        .unwrap_or_else(|| panic!("installed terminal"));
    assert_eq!(terminal.terminal().fence(), fence);
    assert_eq!(calls.retained_count(), 1);

    calls.discard_settled();
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn control_fence_discards_a_settled_terminal_without_core_application() {
    let (effect, mut machine) = assignment();
    let fence = resolve_fence(effect);
    let mut calls = TrackedPositionCalls::new(1);
    calls.install_terminal_for_test(fence, Moment::from_tick(7));
    calls.observe_control(seek_suspend(&mut machine, fence));

    assert!(
        calls
            .poll_next_ready(Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("completion ownership: {error:?}"))
            .is_none()
    );
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn reassignment_revoke_discards_the_old_assignment_terminal() {
    let (effect, mut machine) = assignment();
    let fence = resolve_fence(effect);
    let mut calls = TrackedPositionCalls::new(1);
    calls.install_terminal_for_test(fence, Moment::from_tick(7));
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(partition(), StartPosition::End)],
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("replacement assignment: {error}"));
    let revoke = replacement
        .effects()
        .iter()
        .copied()
        .find(|effect| matches!(effect, AssignedConsumerEffect::Revoke { .. }))
        .unwrap_or_else(|| panic!("replacement must revoke old assignment"));
    calls.observe_control(revoke);

    assert!(
        calls
            .poll_next_ready(Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("completion ownership: {error:?}"))
            .is_none()
    );
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn completion_fault_remains_owned_until_driver_shutdown_recovery() {
    let (effect, _) = assignment();
    let fence = resolve_fence(effect);
    let mut calls = TrackedPositionCalls::new(1);
    calls.install_completion_failure_for_test(fence, CompletionError::Consumed);

    let failure = calls
        .poll_next_ready(Moment::from_tick(8))
        .err()
        .unwrap_or_else(|| panic!("completion fault must remain fatal"));
    assert_eq!(failure.fence(), fence);
    assert_eq!(failure.source(), CompletionError::Consumed);
    assert_eq!(calls.retained_count(), 1);

    assert_eq!(
        calls.recover_positions_after_driver_shutdown(),
        Some(failure)
    );
    assert_eq!(calls.retained_count(), 0);
}

fn assignment() -> (AssignedConsumerEffect, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                partition(),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (transition.effects()[0], machine)
}

fn seek_suspend(
    machine: &mut AssignedConsumerMachine,
    old: PositionFence,
) -> AssignedConsumerEffect {
    let transition = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.assignment_epoch(),
            partition: old.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("seek direct assignment: {error}"));
    transition.effects()[0]
}

fn request(effect: AssignedConsumerEffect) -> PositionResolutionRequest {
    request_with_transport(effect, Duration::from_secs(1))
}

fn request_with_transport(
    effect: AssignedConsumerEffect,
    remaining: Duration,
) -> PositionResolutionRequest {
    let AssignedConsumerEffect::ResolvePosition { deadline, .. } = effect else {
        panic!("resolution effect");
    };
    PositionResolutionRequest::from_effect(
        effect,
        "orders".to_owned(),
        ListOffsetsIsolation::ReadUncommitted,
        OperationDeadline::from_parts_for_test(deadline, Instant::now() + remaining),
    )
    .unwrap_or_else(|error| panic!("prepared position request: {error:?}"))
}

fn resolve_fence(effect: AssignedConsumerEffect) -> PositionFence {
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = effect else {
        panic!("resolution effect");
    };
    fence
}

fn partition() -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(3))
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
