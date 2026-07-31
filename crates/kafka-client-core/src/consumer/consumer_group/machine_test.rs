//! Deterministic KIP-848 join, cadence, assignment, failure, and leave scenarios.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    test_support::{deadline, epoch, joining, machine, member, moment, partition, succeed},
};
#[test]
fn initial_heartbeat_preserves_the_public_deadline_and_complete_subscription_shape() {
    let mut machine = machine();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Submit {
            attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: None,
            member_epoch: None,
            assignment_generation: None,
            deadline: actual_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("initial join must emit one epoch-zero heartbeat")
    };
    assert_eq!(*actual_deadline, deadline(40));
    assert_eq!(attempt.member_epoch(), None);
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
}

#[test]
fn join_success_installs_assignment_and_arms_the_slower_of_interval_and_throttle() {
    let (mut machine, attempt) = joining();
    let transition = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        8,
        Some(vec![partition(1, 0), partition(1, 1)]),
    );
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Reconcile {
            previous: None,
            assignment,
            member_epoch,
            schedule,
        },
    ] = effects.as_slice()
    else {
        panic!("initial success must install one assignment")
    };
    assert_eq!(assignment.partitions(), [partition(1, 0), partition(1, 1)]);
    assert_eq!(member_epoch.get(), 1);
    assert_eq!(schedule.deadline(), deadline(28));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Stable);
    assert_eq!(machine.schedule(), Some(*schedule));
}

#[test]
fn due_schedule_submits_current_epoch_and_assignment_under_a_fresh_attempt_deadline() {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = machine
        .schedule()
        .unwrap_or_else(|| panic!("armed heartbeat"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Submit {
            kind: ConsumerGroupHeartbeatRequestKind::Steady,
            member_id: Some(member_id),
            member_epoch: Some(member_epoch),
            assignment_generation: Some(generation),
            deadline: attempt_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("due cadence must submit one steady heartbeat")
    };
    assert_eq!(member_id.get(), 9);
    assert_eq!(member_epoch.get(), 1);
    assert_eq!(generation.get(), 1);
    assert_eq!(*attempt_deadline, deadline(35));
}

#[test]
fn changed_epoch_requires_assignment_and_stale_attempt_cannot_mutate_membership() {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = machine
        .schedule()
        .unwrap_or_else(|| panic!("armed heartbeat"));
    let _ = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let current = machine.in_flight().unwrap_or_else(|| panic!("in flight"));
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatSucceeded {
            attempt: current,
            now: moment(26),
            member_id: member(9),
            member_epoch: epoch(2),
            heartbeat_interval_ticks: 5,
            throttle_ticks: 0,
            assignment: None,
        })
        .err()
        .unwrap_or_else(|| panic!("changed epoch without assignment must reject"));
    assert_eq!(
        error.kind(),
        super::ConsumerGroupHeartbeatErrorKind::ChangedEpochMissingAssignment
    );
    let stale = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ConsumerGroupHeartbeatFailure::Execution,
        })
        .err()
        .unwrap_or_else(|| panic!("stale attempt must reject"));
    assert_eq!(
        stale.kind(),
        super::ConsumerGroupHeartbeatErrorKind::AttemptMismatch
    );
    assert_eq!(machine.in_flight(), Some(current));
}

#[test]
fn terminal_steady_failure_revokes_before_publishing_the_exact_fatal_cause() {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = machine
        .schedule()
        .unwrap_or_else(|| panic!("armed heartbeat"));
    let _ = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let current = machine.in_flight().unwrap_or_else(|| panic!("in flight"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt: current,
            failure: ConsumerGroupHeartbeatFailure::Broker(27),
        })
        .unwrap_or_else(|error| panic!("terminal heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects[0],
        ConsumerGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        effects[1],
        ConsumerGroupHeartbeatEffect::Fatal { fatal }
            if fatal.attempt() == current && fatal.failure() == ConsumerGroupHeartbeatFailure::Broker(27)
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
    assert!(machine.live_assignment().is_none());
}

#[test]
fn stable_leave_uses_the_current_member_then_success_revokes_and_closes() {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::BeginLeave {
            now: moment(22),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin leave: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: leave,
            kind: ConsumerGroupHeartbeatRequestKind::Leave,
            member_epoch: Some(member_epoch),
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("stable leave must emit one epoch-minus-one request")
    };
    assert_eq!(member_epoch.get(), 1);
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::LeaveSucceeded { attempt: *leave })
        .unwrap_or_else(|error| panic!("leave success: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Revoke { .. })
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Closed);
}

#[test]
fn local_close_revokes_stable_assignment_without_forging_a_leave_attempt() {
    let (mut machine, attempt) = joining();
    let _ = succeed(
        &mut machine,
        attempt,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );

    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Close)
        .unwrap_or_else(|error| panic!("local close: {error}"));

    assert!(matches!(
        transition.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Revoke { .. })
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Closed);
    assert!(machine.in_flight().is_none());
    assert!(machine.schedule().is_none());
    assert!(machine.live_assignment().is_none());
}
