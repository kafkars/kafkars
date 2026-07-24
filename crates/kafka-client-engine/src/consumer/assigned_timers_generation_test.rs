//! Generation replacement and insertion-sequence exhaustion scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, Deadline, FetchRecords, Moment, StartPosition,
};

use super::assigned_timer_model::{AssignedTimerDisposition, AssignedTimerError};
use super::assigned_timers::AssignedTimers;
use super::assigned_timers_test::{arm, assigned, offset, position_fence, position_timer};

#[test]
fn reachable_sequence_exhaustion_preserves_live_entry_and_exact_effects() {
    let (active, mut machine) = position_timer(1, 3, 15);
    let active_fence = position_fence(active);
    let mut timers = AssignedTimers::new(2);
    assert_eq!(
        arm(&mut timers, active),
        Ok(AssignedTimerDisposition::Inserted)
    );
    let ready = machine
        .apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence: active_fence,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("elapse active position timer: {error}"));
    let [AssignedConsumerEffect::FetchReady { fence, .. }] = ready.effects() else {
        panic!("position timer must become Fetch-ready");
    };
    let replacement = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: *fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(9),
            now: Moment::from_tick(16),
            throttle_ticks: 4,
        })
        .unwrap_or_else(|error| panic!("arm newer Fetch timer: {error}"))
        .effects()[0];
    let (insertion, _) = position_timer(1, 4, 12);
    timers.replace_next_sequence_for_test(u64::MAX);

    assert_eq!(
        arm(&mut timers, replacement),
        Err(AssignedTimerError::InsertionSequenceExhausted {
            effect: replacement,
        })
    );
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(
        arm(&mut timers, insertion),
        Err(AssignedTimerError::InsertionSequenceExhausted { effect: insertion })
    );
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: active_fence,
            now: Moment::from_tick(15),
        })
    );
}

#[test]
fn allocation_failure_precedes_sequence_and_preserves_live_entry() {
    let (active, _) = position_timer(1, 3, 15);
    let active_fence = position_fence(active);
    let (incoming, _) = position_timer(1, 4, 12);
    let AssignedConsumerEffect::ArmPositionThrottle { fence, deadline } = incoming else {
        panic!("insertion must be a position timer");
    };
    let mut timers = AssignedTimers::new(2);
    assert_eq!(
        arm(&mut timers, active),
        Ok(AssignedTimerDisposition::Inserted)
    );
    timers.replace_next_sequence_for_test(u64::MAX);

    assert_eq!(
        timers.arm_position_with_allocation_failure_for_test(fence, deadline),
        Err(AssignedTimerError::Allocation { effect: incoming })
    );
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(timers.next_deadline(), Some(Deadline::from_tick(15)));
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence: active_fence,
            now: Moment::from_tick(15),
        })
    );
}

#[test]
fn newer_fetch_replaces_position_timer_and_equal_control_cannot_cancel_it() {
    let (initial, mut machine) = position_timer(1, 3, 15);
    let initial_fence = position_fence(initial);
    let pause = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: initial_fence.assignment_epoch(),
            partition: initial_fence.partition(),
        })
        .unwrap_or_else(|error| panic!("pause initial throttle: {error}"));
    let [suspend @ AssignedConsumerEffect::Suspend { .. }] = pause.effects() else {
        panic!("pause must emit one position fence");
    };
    let resume = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: initial_fence.assignment_epoch(),
            partition: initial_fence.partition(),
            now: Moment::from_tick(12),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume position throttle: {error}"));
    let [position_timer @ AssignedConsumerEffect::ArmPositionThrottle { fence, .. }] =
        resume.effects()
    else {
        panic!("resume must rearm the position throttle");
    };
    let fence = *fence;
    let mut timers = AssignedTimers::new(1);
    assert_eq!(
        arm(&mut timers, *position_timer),
        Ok(AssignedTimerDisposition::Inserted)
    );

    let ready = machine
        .apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("elapse position throttle: {error}"));
    let [AssignedConsumerEffect::FetchReady { fence: fetch, .. }] = ready.effects() else {
        panic!("position throttle must become Fetch-ready");
    };
    let armed = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: *fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(9),
            now: Moment::from_tick(16),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("arm Fetch throttle: {error}"));
    let [fetch_timer @ AssignedConsumerEffect::ArmFetchThrottle { fence: fetch, .. }] =
        armed.effects()
    else {
        panic!("positive Fetch throttle must arm");
    };
    let fetch = *fetch;
    assert_eq!(
        arm(&mut timers, *fetch_timer),
        Ok(AssignedTimerDisposition::Replaced)
    );
    assert!(!timers.observe_control(*suspend));
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(
        timers.pop_due(Moment::from_tick(21)),
        Some(AssignedConsumerInput::FetchThrottleElapsed {
            fence: fetch,
            now: Moment::from_tick(21),
        })
    );
}

#[test]
fn old_assignment_revoke_cannot_cancel_the_replacement_timer() {
    let (_, mut machine) = position_timer(1, 3, 15);
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(1, 3, StartPosition::Beginning)],
            now: Moment::from_tick(6),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replace throttled assignment: {error}"));
    let revoke = replacement
        .effects()
        .iter()
        .find(|effect| matches!(effect, AssignedConsumerEffect::Revoke { .. }))
        .copied()
        .unwrap_or_else(|| panic!("replacement must revoke the old assignment"));
    let resolve = replacement
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("replacement must resolve its new position"));
    let armed = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence: resolve,
            next_offset: offset(9),
            now: Moment::from_tick(7),
            throttle_ticks: 8,
        })
        .unwrap_or_else(|error| panic!("arm replacement timer: {error}"));
    let [timer @ AssignedConsumerEffect::ArmPositionThrottle { fence, .. }] = armed.effects()
    else {
        panic!("replacement resolution must arm one timer");
    };
    let fence = *fence;
    let mut timers = AssignedTimers::new(1);
    assert_eq!(
        arm(&mut timers, *timer),
        Ok(AssignedTimerDisposition::Inserted)
    );

    assert!(!timers.observe_control(revoke));
    assert_eq!(timers.timer_count(), 1);
    assert_eq!(
        timers.pop_due(Moment::from_tick(15)),
        Some(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(15),
        })
    );
}
