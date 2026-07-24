//! Pre-admission ownership reconciliation for queued position lookups.

use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachineError, Deadline, Moment, StartPosition,
};

use super::position_execution::{
    PositionExecutionError, PositionResolutionExecutor, PositionSubmission,
};
use super::position_execution_test::{assignment, owner, prepared, resolve_fence, shutdown};

#[test]
fn superseded_prepared_lookup_settles_before_zero_call_capacity() {
    let mut driver = owner();
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(100));
    let prepared = prepared(effects[0], Duration::from_secs(1));
    let fence = prepared.fence();
    machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fence.assignment_epoch(),
            partition: fence.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("supersede queued lookup: {error}"));
    let mut executor = PositionResolutionExecutor::new(0);

    assert!(matches!(
        executor
            .submit(&driver, &mut machine, prepared, Moment::from_tick(1))
            .unwrap_or_else(|error| panic!("settle superseded lookup: {error:?}")),
        PositionSubmission::Settled(None)
    ));
    assert_eq!(executor.retained_positions(), 0);
    shutdown(&mut driver);
}

#[test]
fn future_position_error_returns_the_exact_prepared_owner_before_capacity() {
    let mut driver = owner();
    let (active_effects, mut active) = assignment(&[3], Deadline::from_tick(100));
    let active_fence = resolve_fence(active_effects[0]);
    let (source_effects, mut source) = assignment(&[3], Deadline::from_tick(100));
    let source_fence = resolve_fence(source_effects[0]);
    let seek = source
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: source_fence.assignment_epoch(),
            partition: source_fence.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("advance source position: {error}"));
    let future_effect = seek.effects()[1];
    let future_fence = resolve_fence(future_effect);
    let mut executor = PositionResolutionExecutor::new(0);

    let PositionExecutionError::Ownership { error, prepared } = executor
        .submit(
            &driver,
            &mut active,
            prepared(future_effect, Duration::from_secs(1)),
            Moment::from_tick(1),
        )
        .err()
        .unwrap_or_else(|| panic!("future lookup must remain an ownership error"))
    else {
        panic!("future lookup must preserve its prepared owner");
    };
    assert_eq!(
        error,
        AssignedConsumerMachineError::StalePosition {
            active: active_fence,
            supplied: future_fence,
        }
    );
    assert_eq!(prepared.fence(), future_fence);
    assert_eq!(executor.retained_positions(), 0);
    shutdown(&mut driver);
}

#[test]
fn cross_partition_error_returns_the_exact_prepared_owner_before_capacity() {
    let mut driver = owner();
    let (_, mut active) = assignment(&[3], Deadline::from_tick(100));
    let (other_effects, _) = assignment(&[4], Deadline::from_tick(100));
    let other_effect = other_effects[0];
    let other_fence = resolve_fence(other_effect);
    let mut executor = PositionResolutionExecutor::new(0);

    let PositionExecutionError::Ownership { error, prepared } = executor
        .submit(
            &driver,
            &mut active,
            prepared(other_effect, Duration::from_secs(1)),
            Moment::from_tick(1),
        )
        .err()
        .unwrap_or_else(|| panic!("cross-partition lookup must remain an ownership error"))
    else {
        panic!("cross-partition lookup must preserve its prepared owner");
    };
    assert_eq!(
        error,
        AssignedConsumerMachineError::UnknownPartition {
            partition: other_fence.partition(),
        }
    );
    assert_eq!(prepared.fence(), other_fence);
    assert_eq!(executor.retained_positions(), 0);
    shutdown(&mut driver);
}

#[test]
fn exact_active_lookup_still_backpressures_with_its_owner() {
    let mut driver = owner();
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(100));
    let expected = resolve_fence(effects[0]);
    let mut executor = PositionResolutionExecutor::new(0);

    let PositionSubmission::Backpressured(prepared) = executor
        .submit(
            &driver,
            &mut machine,
            prepared(effects[0], Duration::from_secs(1)),
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("backpressure active lookup: {error:?}"))
    else {
        panic!("active lookup must reach bounded call capacity");
    };
    assert_eq!(prepared.fence(), expected);
    assert_eq!(executor.retained_positions(), 0);
    shutdown(&mut driver);
}
