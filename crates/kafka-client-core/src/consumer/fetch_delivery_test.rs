//! Delivery authorization ordering and terminal Fetch failure scenarios.

use core::num::NonZeroI16;

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchFailure, FetchRecords, FetchThrottleFailure, StartPosition,
    assignment_test::offset, fetch_throttle_test::first_fetch,
};
use crate::{Deadline, Moment};

#[test]
fn deliverable_fetch_authorizes_before_unthrottled_progress() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("deliverable Fetch success: {error}"));

    let [
        AssignedConsumerEffect::AuthorizeFetchDelivery {
            fence: authorized,
            next_offset: checkpoint,
        },
        AssignedConsumerEffect::FetchReady {
            fence: next,
            next_offset,
        },
    ] = transition.effects()
    else {
        panic!("delivery authorization must precede the next Fetch");
    };
    assert_eq!(*authorized, completed);
    assert_eq!(*checkpoint, offset(12));
    assert_eq!(next.position(), completed.position());
    assert_eq!(next.revision().get(), completed.revision().get() + 1);
    assert_eq!(*next_offset, offset(12));
}

#[test]
fn progress_only_fetch_authorizes_before_advancing_the_next_fetch() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            records: FetchRecords::ProgressOnlyDelivery,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("progress-only Fetch success: {error}"));

    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::AuthorizeFetchDelivery {
                fence,
                next_offset: checkpoint,
            },
            AssignedConsumerEffect::FetchReady {
                fence: next,
                next_offset,
            },
        ] if *fence == completed
            && *checkpoint == offset(12)
            && next.revision().get() == completed.revision().get() + 1
            && *next_offset == offset(12)
    ));
}

#[test]
fn empty_or_control_only_fetch_advances_without_delivery() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("empty Fetch success: {error}"));

    assert!(matches!(
        transition.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.revision().get() == completed.revision().get() + 1
            && *next_offset == offset(12)
    ));
}

#[test]
fn deliverable_fetch_authorizes_before_positive_throttle() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("throttled deliverable Fetch success: {error}"));

    let [
        AssignedConsumerEffect::AuthorizeFetchDelivery { fence, next_offset },
        AssignedConsumerEffect::ArmFetchThrottle { deadline, .. },
    ] = transition.effects()
    else {
        panic!("delivery authorization must precede the positive throttle");
    };
    assert_eq!(*fence, completed);
    assert_eq!(*next_offset, offset(12));
    assert_eq!(*deadline, Deadline::from_tick(15));
}

#[test]
fn delivery_precedes_terminal_throttle_overflow() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(u64::MAX - 1),
            throttle_ticks: 2,
        })
        .unwrap_or_else(|error| panic!("deliverable Fetch before throttle overflow: {error}"));

    assert_eq!(
        transition.effects(),
        &[
            AssignedConsumerEffect::AuthorizeFetchDelivery {
                fence: completed,
                next_offset: offset(12),
            },
            AssignedConsumerEffect::FetchThrottleFailed {
                fence: completed,
                failure: FetchThrottleFailure::DeadlineOverflow,
            },
        ]
    );
}

#[test]
fn stale_failure_cannot_terminate_the_active_fetch_revision() {
    let mut machine = AssignedConsumerMachine::new();
    let first = first_fetch(&mut machine);
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance first Fetch: {error}"));
    let [AssignedConsumerEffect::FetchReady { fence: second, .. }] = advanced.effects() else {
        panic!("successful Fetch must issue its successor");
    };
    let second = *second;

    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchFailed {
            fence: first,
            failure: FetchFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::StaleFetch { supplied: first })
    );
    let failed = machine
        .apply(AssignedConsumerInput::FetchFailed {
            fence: second,
            failure: FetchFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("fail active Fetch: {error}"));
    assert_eq!(
        failed.effects(),
        &[AssignedConsumerEffect::FetchFailed {
            fence: second,
            failure: FetchFailure::Transport,
        }]
    );
}

#[test]
fn every_terminal_fetch_failure_preserves_its_semantic_reason() {
    let broker_code =
        NonZeroI16::new(-31_415).unwrap_or_else(|| panic!("test broker code is nonzero"));
    for failure in [
        FetchFailure::DeadlineElapsed,
        FetchFailure::DriverRejected,
        FetchFailure::Transport,
        FetchFailure::Broker(broker_code),
        FetchFailure::Compatibility,
        FetchFailure::InvalidResponse,
        FetchFailure::ResponseTooLarge,
    ] {
        let mut machine = AssignedConsumerMachine::new();
        let fence = first_fetch(&mut machine);
        let failed = machine
            .apply(AssignedConsumerInput::FetchFailed { fence, failure })
            .unwrap_or_else(|error| panic!("fail exact Fetch: {error}"));
        assert_eq!(
            failed.effects(),
            &[AssignedConsumerEffect::FetchFailed { fence, failure }]
        );
    }
}

#[test]
fn terminal_fetch_failure_is_inert_until_seek_replaces_position() {
    let mut machine = AssignedConsumerMachine::new();
    let failed_fetch = first_fetch(&mut machine);
    machine
        .apply(AssignedConsumerInput::FetchFailed {
            fence: failed_fetch,
            failure: FetchFailure::DeadlineElapsed,
        })
        .unwrap_or_else(|error| panic!("fail active Fetch: {error}"));
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence: failed_fetch,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(2),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StaleFetch {
            supplied: failed_fetch,
        })
    );

    let epoch = failed_fetch.position().assignment_epoch();
    let partition = failed_fetch.position().partition();
    machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition,
        })
        .unwrap_or_else(|error| panic!("pause failed position: {error}"));
    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition,
            now: Moment::from_tick(3),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume failed position: {error}"));
    assert!(resumed.effects().is_empty());

    let recovered = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: epoch,
            partition,
            position: StartPosition::Offset(offset(20)),
            now: Moment::from_tick(4),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek replaces failed position: {error}"));
    assert!(matches!(
        recovered.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. },
        ] if *next_offset == offset(20)
    ));
}
