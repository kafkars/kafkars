//! Coordinator retry, fencing, and original-deadline share regressions.

use super::{
    ShareGroupHeartbeatEffect, ShareGroupHeartbeatErrorKind, ShareGroupHeartbeatFailure,
    ShareGroupHeartbeatInput, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
    ShareGroupHeartbeatRetryCause,
    test_support::{deadline, heartbeating, joining, machine, moment},
};

#[test]
fn coordinator_load_retries_same_attempt_after_positive_delay() {
    let mut machine = machine();
    let original_deadline = deadline(200_000_040);
    let begin = machine
        .apply(ShareGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: original_deadline,
        })
        .unwrap_or_else(|error| panic!("begin heartbeat: {error}"));
    let Some(ShareGroupHeartbeatEffect::Submit { attempt, .. }) = begin.into_effects().next()
    else {
        panic!("join attempt")
    };
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(20),
            failure: ShareGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("schedule coordinator load: {error}"));
    let Some(ShareGroupHeartbeatEffect::ArmRetry { schedule }) = transition.into_effects().next()
    else {
        panic!("retry schedule")
    };
    assert_eq!(schedule.attempt(), attempt);
    assert_eq!(
        schedule.cause(),
        ShareGroupHeartbeatRetryCause::CoordinatorLoad
    );
    assert!(schedule.not_before().tick() > 20);
    let early = machine
        .apply(ShareGroupHeartbeatInput::RetryDue {
            schedule,
            now: moment(schedule.not_before().tick() - 1),
        })
        .err()
        .unwrap_or_else(|| panic!("early retry must reject"));
    assert_eq!(early.kind(), ShareGroupHeartbeatErrorKind::RetryNotDue);
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .unwrap_or_else(|error| panic!("retry due: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::Submit {
            attempt: actual,
            kind: ShareGroupHeartbeatRequestKind::Join,
            deadline: actual_deadline,
            ..
        }) if actual == attempt && actual_deadline == original_deadline
    ));
}

#[test]
fn not_coordinator_reserves_fresh_attempt_and_pairs_invalidation_with_delay() {
    let (mut machine, attempt) = joining();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(20),
            failure: ShareGroupHeartbeatFailure::Broker(16),
        })
        .unwrap_or_else(|error| panic!("rediscovery retry: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ShareGroupHeartbeatEffect::Rediscover {
            previous: None,
            attempt: replacement,
            kind: ShareGroupHeartbeatRequestKind::Join,
            ..
        },
        ShareGroupHeartbeatEffect::ArmRetry { schedule },
    ] = effects.as_slice()
    else {
        panic!("rediscovery and schedule")
    };
    assert_ne!(*replacement, attempt);
    assert_eq!(schedule.attempt(), *replacement);
    assert_eq!(schedule.cause(), ShareGroupHeartbeatRetryCause::Rediscovery);
    assert_eq!(machine.in_flight(), Some(*replacement));
}

#[test]
fn expired_steady_coordinator_route_revokes_and_schedules_a_fresh_join() {
    let (mut machine, attempt) = heartbeating();
    let original_deadline = deadline(35);
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(original_deadline.tick()),
            failure: ShareGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("recover expired coordinator route: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ShareGroupHeartbeatEffect::Rediscover {
            previous: Some(previous),
            attempt: replacement,
            kind: ShareGroupHeartbeatRequestKind::Join,
            member_epoch: None,
            assignment_generation: None,
            deadline: replacement_deadline,
            ..
        },
        ShareGroupHeartbeatEffect::ArmRetry { schedule },
    ] = effects.as_slice()
    else {
        panic!("revoking rediscovery and retry schedule")
    };
    assert_eq!(previous.assignment_generation().get(), 1);
    assert_ne!(*replacement, attempt);
    assert_eq!(replacement_deadline.tick(), 45);
    assert_eq!(schedule.attempt(), *replacement);
    assert_eq!(schedule.deadline(), *replacement_deadline);
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Joining);
    assert_eq!(machine.member_epoch(), None);
    assert!(machine.live_assignment().is_none());
}

#[test]
fn expired_join_coordinator_route_remains_terminal() {
    let (mut machine, attempt) = joining();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(40),
            failure: ShareGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("settle expired join route: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::Fatal { fatal })
            if fatal.failure() == ShareGroupHeartbeatFailure::DeadlineElapsed
    ));
}

#[test]
fn stale_retry_schedule_cannot_mutate_replacement_ownership() {
    let (mut machine, attempt) = joining();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(20),
            failure: ShareGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("rediscovery retry: {error}"));
    let schedule = transition
        .into_effects()
        .find_map(|effect| match effect {
            ShareGroupHeartbeatEffect::ArmRetry { schedule } => Some(schedule),
            _ => None,
        })
        .unwrap_or_else(|| panic!("retry schedule"));
    let stale = super::ShareGroupHeartbeatRetrySchedule {
        attempt,
        ..schedule
    };
    let error = machine
        .apply(ShareGroupHeartbeatInput::RetryDue {
            schedule: stale,
            now: moment(stale.not_before().tick()),
        })
        .err()
        .unwrap_or_else(|| panic!("stale schedule must reject"));
    assert_eq!(error.kind(), ShareGroupHeartbeatErrorKind::AttemptMismatch);
    assert_eq!(machine.retry_schedule(), Some(schedule));
}

#[test]
fn retry_at_original_deadline_terminalizes_without_another_submission() {
    let (mut machine, attempt) = joining();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(39),
            failure: ShareGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("schedule retry: {error}"));
    let Some(ShareGroupHeartbeatEffect::ArmRetry { schedule }) = transition.into_effects().next()
    else {
        panic!("retry schedule")
    };
    assert_eq!(schedule.not_before(), deadline(40));
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RetryDue {
            schedule,
            now: moment(40),
        })
        .unwrap_or_else(|error| panic!("deadline retry: {error}"));
    assert!(matches!(
        transition.into_effects().next(),
        Some(ShareGroupHeartbeatEffect::Fatal { fatal })
            if fatal.failure() == ShareGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Fatal);
    assert_eq!(machine.startup_fatal(), machine.fatal());
}

#[test]
fn fenced_steady_member_revokes_then_rejoins_with_same_stable_identity() {
    let (mut machine, attempt) = heartbeating();
    let transition = machine
        .apply(ShareGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(26),
            failure: ShareGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("recover fenced share member: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects[0],
        ShareGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        effects[1],
        ShareGroupHeartbeatEffect::Submit {
            member_id,
            kind: ShareGroupHeartbeatRequestKind::Join,
            member_epoch: None,
            ..
        } if member_id == super::test_support::member()
    ));
    assert_eq!(machine.phase(), ShareGroupHeartbeatPhase::Joining);
    assert_eq!(machine.member_epoch(), None);
}

#[test]
fn first_success_suppresses_later_membership_failure_from_startup_observation() {
    let (mut machine, attempt) = heartbeating();
    let _ = machine
        .apply(ShareGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ShareGroupHeartbeatFailure::Broker(27),
        })
        .unwrap_or_else(|error| panic!("terminal heartbeat: {error}"));
    assert!(machine.fatal().is_some());
    assert_eq!(machine.startup_fatal(), None);
}
