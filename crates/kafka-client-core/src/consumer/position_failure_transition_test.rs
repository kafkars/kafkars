//! Core deadline-precedence scenarios for exact position-attempt categories.

use core::num::NonZeroI16;

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, PositionFence,
    PositionResolutionAttemptFailure, PositionResolutionFailure, StartPosition,
    assignment_test::{assign_at, assigned},
};
use crate::{Deadline, Moment};

#[test]
fn unelapsed_resolution_failure_preserves_every_attempt_category() {
    let Some(code) = NonZeroI16::new(-42) else {
        panic!("negative broker code is nonzero");
    };
    let failures = [
        PositionResolutionAttemptFailure::DeadlineElapsed,
        PositionResolutionAttemptFailure::DriverRejected,
        PositionResolutionAttemptFailure::Transport,
        PositionResolutionAttemptFailure::Broker(code),
        PositionResolutionAttemptFailure::Compatibility,
        PositionResolutionAttemptFailure::InvalidResponse,
        PositionResolutionAttemptFailure::ResponseTooLarge,
    ];

    for failure in failures {
        let mut machine = AssignedConsumerMachine::new();
        let fence = resolving(&mut machine);
        let transition = machine
            .apply(AssignedConsumerInput::PositionResolutionFailed {
                fence,
                now: Moment::from_tick(99),
                failure,
            })
            .unwrap_or_else(|error| panic!("failure before deadline: {error}"));
        assert_eq!(
            transition.effects(),
            &[AssignedConsumerEffect::PositionResolutionFailed {
                fence,
                failure: PositionResolutionFailure::Attempt(failure),
            }]
        );
    }
}

fn resolving(machine: &mut AssignedConsumerMachine) -> PositionFence {
    let transition = assign_at(
        machine,
        vec![assigned(1, 0, StartPosition::Beginning)],
        Moment::from_tick(0),
        Deadline::from_tick(100),
    );
    let [AssignedConsumerEffect::ResolvePosition { fence, .. }] = transition.effects() else {
        panic!("future beginning position must resolve");
    };
    *fence
}
