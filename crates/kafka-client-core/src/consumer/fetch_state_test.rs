//! Fetch throttle overflow and control-fencing scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchThrottleFailure, StartPosition,
    assignment_test::{assign_at, assigned, offset},
    fetch_throttle_test::{advance_with_throttle, first_fetch},
};
use crate::{Deadline, Moment};

#[test]
fn seek_and_assignment_replacement_fence_old_fetch_throttle_timers() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let stale_seek = advance_with_throttle(&mut machine, completed, 12, 10, 5);
    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: stale_seek.position().assignment_epoch(),
            partition: stale_seek.position().partition(),
            position: StartPosition::Offset(offset(20)),
            now: Moment::from_tick(11),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek throttled Fetch: {error}"));
    let [
        AssignedConsumerEffect::Suspend { .. },
        AssignedConsumerEffect::FetchReady {
            fence: seek_fetch, ..
        },
    ] = seek.effects()
    else {
        panic!("seek must fence the throttle before issuing its replacement Fetch");
    };
    let seek_fetch = *seek_fetch;
    assert!(matches!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: stale_seek,
            now: Moment::from_tick(15),
        }),
        Err(AssignedConsumerMachineError::StalePosition { .. })
    ));

    let stale_assignment = advance_with_throttle(&mut machine, seek_fetch, 21, 20, 5);
    assign_at(
        &mut machine,
        vec![assigned(2, 0, StartPosition::Offset(offset(1)))],
        Moment::from_tick(21),
        Deadline::from_tick(100),
    );
    assert!(matches!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: stale_assignment,
            now: Moment::from_tick(25),
        }),
        Err(AssignedConsumerMachineError::StaleAssignment { .. })
    ));
}

#[test]
fn fetch_throttle_deadline_overflow_is_explicit_and_inert_until_seek() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let failed = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            next_offset: offset(12),
            now: Moment::from_tick(u64::MAX - 1),
            throttle_ticks: 2,
        })
        .unwrap_or_else(|error| panic!("overflowing Fetch throttle: {error}"));
    assert_eq!(
        failed.effects(),
        &[AssignedConsumerEffect::FetchThrottleFailed {
            fence: completed,
            failure: FetchThrottleFailure::DeadlineOverflow,
        }]
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            next_offset: offset(13),
            now: Moment::from_tick(u64::MAX),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StaleFetch {
            supplied: completed,
        })
    );

    let recovered = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: completed.position().assignment_epoch(),
            partition: completed.position().partition(),
            position: StartPosition::Offset(offset(13)),
            now: Moment::from_tick(u64::MAX),
            resolution_deadline: Deadline::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("seek recovers failed Fetch throttle: {error}"));
    assert!(matches!(
        recovered.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. }
        ] if *next_offset == offset(13)
    ));
}
