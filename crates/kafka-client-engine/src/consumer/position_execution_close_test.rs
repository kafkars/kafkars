//! Close fencing of backpressured position-resolution ownership.

use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, Deadline, Moment, PositionOwnership,
};

use super::position_execution::{PositionResolutionExecutor, PositionSubmission};
use super::position_execution_test::{assignment, owner, prepared, shutdown};

#[test]
fn close_supersedes_backpressured_position_before_zero_call_capacity() {
    let mut driver = owner();
    let (effects, mut machine) = assignment(&[3], Deadline::from_tick(1_000_000_000));
    let mut executor = PositionResolutionExecutor::new(0);
    let queued = prepared(effects[0], Duration::from_secs(1));
    let queued_fence = queued.fence();

    let PositionSubmission::Backpressured(queued) = executor
        .submit(&driver, &mut machine, queued, Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("backpressure lookup: {error:?}"))
    else {
        panic!("lookup must retain its prepared owner");
    };
    assert_eq!(executor.retained_positions(), 0);

    let close = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));
    assert!(matches!(
        close.effects().first(),
        Some(AssignedConsumerEffect::AcceptClose { .. })
    ));
    for effect in close.effects() {
        executor.observe_control(*effect);
    }
    assert_eq!(
        machine.position_ownership(queued_fence),
        Ok(PositionOwnership::Superseded)
    );

    assert!(matches!(
        executor
            .submit(&driver, &mut machine, queued, Moment::from_tick(1))
            .unwrap_or_else(|error| panic!("settle close-fenced lookup: {error:?}")),
        PositionSubmission::Settled(None)
    ));
    assert_eq!(executor.retained_positions(), 0);

    shutdown(&mut driver);
}
