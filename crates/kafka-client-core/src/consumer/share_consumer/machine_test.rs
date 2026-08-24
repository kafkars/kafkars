//! Join, cadence, assignment, failure, and leave scenarios for share membership.

use super::{
    ShareGroupHeartbeatEffect, ShareGroupHeartbeatFailure, ShareGroupHeartbeatInput,
    ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
    test_support::{deadline, epoch, joining, machine, member, moment, partition, succeed},
};

#[test]
fn initial_join_preserves_stable_member_and_public_deadline() {
    let mut machine = machine();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ShareGroupHeartbeatEffect::Submit {
            member_id,
            attempt,
            kind: ShareGroupHeartbeatRequestKind::Join,
            member_epoch: None,
            assignment_generation: None,
            deadline: actual_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("initial join must emit one epoch-zero heartbeat")
    };
    assert_eq!(*member_id, member());
    assert_eq!(*actual_deadline, deadline(40));
    assert_eq!(attempt.member_epoch(), None);
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Joining);
}

#[test]
fn join_success_installs_assignment_and_arms_broker_cadence() {
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
        ShareGroupHeartbeatEffect::ReplaceAssignment {
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
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Stable);
    assert_eq!(machine.schedule(), Some(*schedule));
}

#[test]
fn assignmentless_success_retains_membership_until_next_heartbeat() {
    let (mut machine, attempt) = joining();
    let transition = succeed(&mut machine, attempt, 20, 1, 5, 0, None);
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::AwaitAssignment { member_epoch, schedule })
            if member_epoch == epoch(1) && schedule.assignment_generation().is_none()
    ));
    assert_eq!(
        machine.phase(),
        ShareGroupHeartbeatPhase::AwaitingAssignment
    );
    let schedule = machine.schedule().unwrap_or_else(|| panic!("schedule"));
    let transition = machine
        .apply(ShareGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due assignmentless heartbeat: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::Submit {
            kind: ShareGroupHeartbeatRequestKind::Steady,
            member_epoch: Some(member_epoch),
            assignment_generation: None,
            ..
        }) if member_epoch == epoch(1)
    ));
}

#[test]
fn initial_empty_assignment_waits_for_broker_initialization() {
    let (mut machine, attempt) = joining();
    let transition = succeed(&mut machine, attempt, 20, 1, 5, 0, Some(Vec::new()));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::AwaitAssignment { member_epoch, schedule })
            if member_epoch == epoch(1) && schedule.assignment_generation().is_none()
    ));
    assert_eq!(
        machine.phase(),
        ShareGroupHeartbeatPhase::AwaitingAssignment
    );
    assert!(machine.live_assignment().is_none());
}

#[test]
fn changed_assignment_advances_only_the_local_assignment_generation() {
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
    let schedule = machine.schedule().unwrap_or_else(|| panic!("schedule"));
    let transition = machine
        .apply(ShareGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit { attempt, .. }) = transition.into_effects().next()
    else {
        panic!("steady attempt")
    };
    let transition = succeed(
        &mut machine,
        attempt,
        26,
        2,
        5,
        0,
        Some(vec![partition(1, 1)]),
    );
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::ReplaceAssignment {
            previous: Some(previous),
            assignment,
            member_epoch,
            ..
        }) if previous.assignment_generation().get() == 1
            && assignment.assignment_generation().get() == 2
            && member_epoch == epoch(2)
    ));
}

#[test]
fn terminal_steady_failure_revokes_before_retaining_fatal() {
    let (mut machine, attempt) = super::test_support::heartbeating();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ShareGroupHeartbeatFailure::Broker(27),
        })
        .unwrap_or_else(|error| panic!("terminal heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects[0],
        ShareGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        effects[1],
        ShareGroupHeartbeatEffect::Fatal { fatal }
            if fatal.attempt() == attempt
                && fatal.failure() == ShareGroupHeartbeatFailure::Broker(27)
    ));
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Fatal);
    assert!(machine.live_assignment().is_none());
}

#[test]
fn stable_leave_uses_current_epoch_then_revokes_and_closes() {
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
        .apply(ShareGroupHeartbeatInput::BeginLeave {
            now: moment(22),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin leave: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit {
        attempt: leave,
        kind: ShareGroupHeartbeatRequestKind::Leave,
        member_epoch: Some(member_epoch),
        ..
    }) = transition.into_effects().next()
    else {
        panic!("leave request")
    };
    assert_eq!(member_epoch, epoch(1));
    let transition = machine
        .apply(ShareGroupHeartbeatInput::LeaveSucceeded { attempt: leave })
        .unwrap_or_else(|error| panic!("leave success: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::Revoke { .. })
    ));
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Closed);
}

#[test]
fn unsubmitted_steady_heartbeat_is_fenced_by_a_fresh_leave_attempt() {
    let (mut machine, steady) = super::test_support::heartbeating();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::ReplaceHeartbeatWithLeave {
            attempt: steady,
            now: moment(26),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("replace steady with leave: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit {
        attempt: leave,
        kind: ShareGroupHeartbeatRequestKind::Leave,
        member_epoch: Some(member_epoch),
        deadline: actual_deadline,
        ..
    }) = transition.into_effects().next()
    else {
        panic!("fresh leave request")
    };

    assert_ne!(leave, steady);
    assert_eq!(member_epoch, epoch(1));
    assert_eq!(actual_deadline, deadline(40));
    assert_eq!(machine.in_flight(), Some(leave));
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Leaving);
    assert_eq!(machine.retry_schedule(), None);
}
