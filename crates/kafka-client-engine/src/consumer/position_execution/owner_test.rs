//! Concrete position executor deadline, capacity, and core-application scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, PartitionIndex, PositionFence,
    PositionResolutionFailure, StartPosition, TopicId,
};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{
    PositionExecutionError, PositionResolutionExecutor, PositionSubmission,
    PreparedPositionResolution,
};
use crate::protocol::consumer::ListOffsetsIsolation;

#[test]
fn full_registry_returns_the_unconsumed_prepared_lookup() {
    let mut driver = owner();
    let (effects, mut machine) = assignment(&[3, 4], Deadline::from_tick(1_000_000_000));
    let mut executor = PositionResolutionExecutor::new(1);
    let first = prepared(effects[0], Duration::from_secs(1));
    let second = prepared(effects[1], Duration::from_secs(1));

    assert!(matches!(
        executor
            .submit(&driver, &mut machine, first, Moment::from_tick(0))
            .unwrap_or_else(|error| panic!("first submission: {error:?}")),
        PositionSubmission::Accepted
    ));
    let backpressured = executor
        .submit(&driver, &mut machine, second, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("bounded submission: {error:?}"));
    let PositionSubmission::Backpressured(unconsumed) = backpressured else {
        panic!("second lookup must remain owned");
    };
    drop(unconsumed);
    assert_eq!(executor.retained_positions(), 1);

    drop(executor);
    shutdown(&mut driver);
}

#[test]
fn elapsed_local_admission_preserves_core_deadline_precedence() {
    let mut driver = owner();
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(20));
    let fence = resolve_fence(effects[0]);
    let mut executor = PositionResolutionExecutor::new(1);
    let submission = executor
        .submit(
            &driver,
            &mut machine,
            prepared(effects[0], Duration::from_secs(1)),
            Moment::from_tick(20),
        )
        .unwrap_or_else(|error| panic!("local deadline settlement: {error:?}"));
    let PositionSubmission::Settled(Some(transition)) = submission else {
        panic!("elapsed admission must settle through core");
    };
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::PositionResolutionFailed {
            fence,
            failure: PositionResolutionFailure::DeadlineElapsed,
        }]
    );
    assert_eq!(executor.retained_positions(), 0);
    shutdown(&mut driver);
}

#[test]
fn terminal_ownership_is_released_only_after_core_accepts_the_fact() {
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(20));
    let fence = resolve_fence(effects[0]);
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(fence, Moment::from_tick(5));

    assert_eq!(executor.retained_positions(), 1);
    let transition = executor
        .poll(&mut machine, Moment::from_tick(6))
        .unwrap_or_else(|error| panic!("apply terminal: {error:?}"))
        .unwrap_or_else(|| panic!("terminal transition"));
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::PositionResolutionFailed {
            fence,
            failure: PositionResolutionFailure::AttemptFailed,
        }]
    );
    assert_eq!(executor.retained_positions(), 0);
}

#[test]
fn superseded_terminal_drains_without_mutating_new_position_state() {
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(20));
    let old = resolve_fence(effects[0]);
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(old, Moment::from_tick(5));
    let transition = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.assignment_epoch(),
            partition: old.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"));
    executor.observe_control(transition.effects()[0]);

    assert!(
        executor
            .poll(&mut machine, Moment::from_tick(6))
            .unwrap_or_else(|error| panic!("drain stale terminal: {error:?}"))
            .is_none()
    );
    assert_eq!(executor.retained_positions(), 0);
}

#[test]
fn unexpected_core_rejection_keeps_settled_ownership_for_recovery() {
    let (effects, _) = assignment(&[3], Deadline::from_tick(20));
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_terminal_for_test(resolve_fence(effects[0]), Moment::from_tick(5));
    let mut unassigned = AssignedConsumerMachine::new();

    assert!(matches!(
        executor.poll(&mut unassigned, Moment::from_tick(6)),
        Err(PositionExecutionError::Core(
            kafka_client_core::AssignedConsumerMachineError::NoAssignment
        ))
    ));
    assert_eq!(executor.retained_positions(), 1);
}

#[test]
fn completion_corruption_is_fatal_until_post_driver_recovery() {
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(20));
    let fence = resolve_fence(effects[0]);
    let mut executor = PositionResolutionExecutor::new(1);
    executor.install_completion_failure_for_test(fence);

    let error = executor
        .poll(&mut machine, Moment::from_tick(6))
        .err()
        .unwrap_or_else(|| panic!("completion corruption must be fatal"));
    let PositionExecutionError::Completion(failure) = error else {
        panic!("completion corruption must retain its own category");
    };
    assert_eq!(failure.fence(), fence);
    assert!(failure.is_consumed());
    assert_eq!(
        executor.release_position_calls_after_driver_shutdown(),
        Some(failure)
    );
    assert_eq!(executor.retained_positions(), 0);
}

pub(in crate::consumer) fn assignment(
    partitions: &[u32],
    deadline: Deadline,
) -> (Vec<AssignedConsumerEffect>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let partitions = partitions
        .iter()
        .map(|partition| {
            AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(*partition),
                ),
                StartPosition::Beginning,
            )
        })
        .collect();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: deadline,
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    (transition.into_effects(), machine)
}

pub(super) fn prepared(
    effect: AssignedConsumerEffect,
    remaining: Duration,
) -> PreparedPositionResolution {
    let AssignedConsumerEffect::ResolvePosition { deadline, .. } = effect else {
        panic!("resolution effect");
    };
    PreparedPositionResolution::new(
        effect,
        "orders".to_owned(),
        ListOffsetsIsolation::ReadUncommitted,
        OperationDeadline::from_parts_for_test(deadline, Instant::now() + remaining),
    )
    .unwrap_or_else(|error| panic!("prepare position lookup: {error:?}"))
}

pub(in crate::consumer) fn resolve_fence(effect: AssignedConsumerEffect) -> PositionFence {
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = effect else {
        panic!("resolution effect");
    };
    fence
}

pub(super) fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}

pub(super) fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
