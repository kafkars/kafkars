//! Coordinator-load retry rejection, fencing, and deadline-precedence scenarios.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatSequence,
    test_support::{deadline, heartbeating, joining, moment},
};

#[test]
fn exact_broker_code_fourteen_is_the_only_load_retry_authority() {
    for failure in [
        ConsumerGroupHeartbeatFailure::Broker(13),
        ConsumerGroupHeartbeatFailure::Broker(15),
        ConsumerGroupHeartbeatFailure::Broker(-14),
        ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        ConsumerGroupHeartbeatFailure::Compatibility,
        ConsumerGroupHeartbeatFailure::Execution,
        ConsumerGroupHeartbeatFailure::InvalidResponse,
    ] {
        let (mut machine, attempt) = joining();
        let error = machine
            .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
                attempt,
                now: moment(20),
                failure,
            })
            .err()
            .unwrap_or_else(|| panic!("non-14 failure must reject"));
        assert_eq!(
            error.kind(),
            ConsumerGroupHeartbeatErrorKind::FailureNotCoordinatorLoad
        );
        assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
        assert_eq!(machine.in_flight(), Some(attempt));
        assert_eq!(machine.retry_schedule(), None);
    }
}

#[test]
fn stale_attempt_and_wrong_phase_cannot_arm_a_retry() {
    let (mut joining_machine, current) = joining();
    let stale = ConsumerGroupHeartbeatAttempt::new(
        ConsumerGroupHeartbeatSequence::try_from_raw(current.sequence().get() + 1)
            .unwrap_or_else(|| panic!("stale sequence")),
        current.member_epoch(),
    );
    let error = joining_machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt: stale,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .err()
        .unwrap_or_else(|| panic!("stale load retry must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::AttemptMismatch
    );
    assert_eq!(joining_machine.retry_schedule(), None);

    let (mut stable, prior) = joining();
    let _ = super::test_support::succeed(
        &mut stable,
        prior,
        20,
        1,
        5,
        0,
        Some(vec![super::test_support::partition(1, 0)]),
    );
    let error = stable
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt: prior,
            now: moment(21),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .err()
        .unwrap_or_else(|| panic!("stable phase must reject"));
    assert_eq!(error.kind(), ConsumerGroupHeartbeatErrorKind::InvalidPhase);
    assert_eq!(stable.phase(), ConsumerGroupHeartbeatPhase::Stable);
}

#[test]
fn one_pending_schedule_consumes_the_fixed_retry_effect_capacity() {
    let (mut machine, attempt) = joining();
    let schedule = arm(&mut machine, attempt, 20);
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(21),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .err()
        .unwrap_or_else(|| panic!("second schedule must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending
    );
    assert_eq!(machine.retry_schedule(), Some(schedule));

    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ConsumerGroupHeartbeatFailure::Execution,
        })
        .err()
        .unwrap_or_else(|| panic!("settlement while waiting must reject"));
    assert_eq!(
        terminal.kind(),
        ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryPending
    );
    assert_eq!(machine.retry_schedule(), Some(schedule));
}

#[test]
fn consumed_and_closed_schedules_are_stale_without_mutation() {
    let (mut machine, attempt) = long_joining();
    let schedule = arm(&mut machine, attempt, 20);
    let _ = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .unwrap_or_else(|error| panic!("retry due: {error}"));
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .err()
        .unwrap_or_else(|| panic!("consumed schedule must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryScheduleMismatch
    );
    assert_eq!(machine.in_flight(), Some(attempt));

    let schedule = arm(&mut machine, attempt, schedule.not_before().tick() + 1);
    let _ = machine
        .apply(ConsumerGroupHeartbeatInput::Close)
        .unwrap_or_else(|error| panic!("close waiting Join: {error}"));
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .err()
        .unwrap_or_else(|| panic!("closed schedule must reject"));
    assert_eq!(error.kind(), ConsumerGroupHeartbeatErrorKind::InvalidPhase);
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Closed);
}

#[test]
fn original_deadline_precedes_scheduling_for_join_steady_and_leave() {
    let (mut join, join_attempt) = joining();
    assert_deadline_terminal(&mut join, join_attempt, 40, false);

    let (mut steady, steady_attempt) = heartbeating();
    assert_deadline_terminal(&mut steady, steady_attempt, 35, true);

    let (mut leave, join_attempt) = joining();
    let _ = super::test_support::succeed(
        &mut leave,
        join_attempt,
        20,
        1,
        5,
        0,
        Some(vec![super::test_support::partition(1, 0)]),
    );
    let transition = leave
        .apply(ConsumerGroupHeartbeatInput::BeginLeave {
            now: moment(22),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin leave: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit {
        attempt: leave_attempt,
        ..
    }) = transition.into_effects().next()
    else {
        panic!("leave attempt")
    };
    assert_deadline_terminal(&mut leave, leave_attempt, 40, true);
}

#[test]
fn backoff_arithmetic_overflow_clamps_to_the_original_deadline() {
    let mut machine = super::test_support::machine();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("begin maximum-deadline Join: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("maximum-deadline Join attempt")
    };
    let schedule = arm(&mut machine, attempt, u64::MAX - 1);
    assert_eq!(schedule.not_before(), deadline(u64::MAX));
    assert_eq!(schedule.deadline(), deadline(u64::MAX));
    assert_eq!(machine.retry_schedule(), Some(schedule));
    let effects = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("maximum retry deadline: {error}"));
    assert!(matches!(
        effects.into_effects().last(),
        Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            if fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.retry_schedule(), None);
}

fn assert_deadline_terminal(
    machine: &mut super::ConsumerGroupHeartbeatMachine,
    attempt: ConsumerGroupHeartbeatAttempt,
    now: u64,
    expects_revoke: bool,
) {
    let schedule = arm(machine, attempt, now - 1);
    assert_eq!(schedule.not_before(), deadline(now));
    assert_eq!(schedule.deadline(), deadline(now));
    let effects = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(now),
        })
        .unwrap_or_else(|error| panic!("deadline terminal: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert_eq!(effects.len(), usize::from(expects_revoke) + 1);
    assert_eq!(
        matches!(
            effects.first(),
            Some(ConsumerGroupHeartbeatEffect::Revoke { .. })
        ),
        expects_revoke
    );
    assert!(matches!(
        effects.last(),
        Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            if fatal.attempt() == attempt
                && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
    assert_eq!(machine.retry_schedule(), None);
}

fn long_joining() -> (
    super::ConsumerGroupHeartbeatMachine,
    ConsumerGroupHeartbeatAttempt,
) {
    let mut machine = super::test_support::machine();
    machine.policy = ConsumerGroupHeartbeatPolicy::try_new(300_000_000)
        .unwrap_or_else(|_| panic!("long attempt policy"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(500_000_000),
        })
        .unwrap_or_else(|error| panic!("begin long Join: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("long Join attempt")
    };
    (machine, attempt)
}

fn arm(
    machine: &mut super::ConsumerGroupHeartbeatMachine,
    attempt: ConsumerGroupHeartbeatAttempt,
    now: u64,
) -> super::ConsumerGroupHeartbeatRetrySchedule {
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryCoordinatorLoad {
            attempt,
            now: moment(now),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .unwrap_or_else(|error| panic!("arm retry: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::ArmCoordinatorLoadRetry { schedule }) =
        transition.into_effects().next()
    else {
        panic!("retry schedule")
    };
    schedule
}
