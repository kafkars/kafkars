//! Repeated KIP-848 coordinator rediscovery, fencing, and deadline regressions.

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatMachine,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatRetryCause, ConsumerGroupHeartbeatRetrySchedule,
    ConsumerGroupHeartbeatTransition,
    test_support::{deadline, moment, partition, succeed},
};

const ORIGINAL_DEADLINE: u64 = 500_000_000;

#[test]
fn two_coordinator_rejections_allocate_fresh_attempts_then_join_succeeds() {
    for code in [15, 16] {
        let (mut machine, rejected) = long_joining();
        let (first, first_schedule) = rediscover(&mut machine, rejected, 20, code);
        assert_ne!(first, rejected);
        assert_eq!(first_schedule.not_before(), deadline(100_000_020));
        assert_eq!(first_schedule.deadline(), deadline(ORIGINAL_DEADLINE));

        let stale = machine
            .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
                attempt: rejected,
                now: moment(21),
                failure: ConsumerGroupHeartbeatFailure::Broker(code),
            })
            .err()
            .unwrap_or_else(|| panic!("old attempt must remain fenced"));
        assert_eq!(
            stale.kind(),
            ConsumerGroupHeartbeatErrorKind::AttemptMismatch
        );
        assert_eq!(machine.retry_schedule(), Some(first_schedule));

        let first_submit = due(&mut machine, first_schedule);
        assert_submit(&first_submit, first);
        let second_now = first_schedule.not_before().tick() + 1;
        let (second, second_schedule) = rediscover(&mut machine, first, second_now, code);
        assert_ne!(second, first);
        assert_eq!(second_schedule.not_before(), deadline(200_000_021));
        assert_eq!(second_schedule.deadline(), deadline(ORIGINAL_DEADLINE));

        assert_submit(&due(&mut machine, second_schedule), second);
        let success = succeed(
            &mut machine,
            second,
            second_schedule.not_before().tick() + 1,
            1,
            5,
            0,
            Some(vec![partition(1, 0)]),
        );
        assert!(matches!(
            success.into_effects().next(),
            Some(ConsumerGroupHeartbeatEffect::Reconcile { .. })
        ));
        assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Stable);
        assert!(machine.fatal().is_none());
    }
}

#[test]
fn rediscovery_delay_is_positive_and_original_deadline_wins() {
    let (mut machine, rejected) = long_joining();
    let (replacement, schedule) = rediscover(&mut machine, rejected, 400_000_001, 16);
    assert_eq!(schedule.not_before(), deadline(ORIGINAL_DEADLINE));

    let early = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(ORIGINAL_DEADLINE - 1),
        })
        .err()
        .unwrap_or_else(|| panic!("early retry must reject"));
    assert_eq!(
        early.kind(),
        ConsumerGroupHeartbeatErrorKind::CoordinatorLoadRetryNotDue
    );
    assert_eq!(machine.retry_schedule(), Some(schedule));

    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(ORIGINAL_DEADLINE),
        })
        .unwrap_or_else(|error| panic!("deadline terminal: {error}"));
    assert!(matches!(
        terminal.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            if fatal.attempt() == replacement
                && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}

#[test]
fn exhausted_sequence_retains_an_execution_terminal() {
    let (mut machine, rejected) = long_joining();
    machine.next_sequence = None;
    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: rejected,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Broker(16),
        })
        .unwrap_or_else(|error| panic!("exhausted rediscovery: {error}"));
    assert!(matches!(
        terminal.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            if fatal.attempt() == rejected
                && fatal.failure() == ConsumerGroupHeartbeatFailure::Execution
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
    assert_eq!(
        machine
            .startup_fatal()
            .map(super::ConsumerGroupHeartbeatFatal::failure),
        Some(ConsumerGroupHeartbeatFailure::Execution)
    );
}

#[test]
fn invalidation_failure_consumes_the_paired_delay_and_terminalizes() {
    let (mut machine, rejected) = long_joining();
    let (replacement, schedule) = rediscover(&mut machine, rejected, 20, 16);
    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::RediscoveryFailed {
            schedule,
            failure: ConsumerGroupHeartbeatFailure::Execution,
        })
        .unwrap_or_else(|error| panic!("invalidation failure: {error}"));
    assert!(matches!(
        terminal.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
            if fatal.attempt() == replacement
                && fatal.failure() == ConsumerGroupHeartbeatFailure::Execution
    ));
    assert_ne!(schedule.not_before(), schedule.deadline());
    assert!(machine.retry_schedule().is_none());
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}

#[test]
fn fatal_after_first_assignmentless_success_is_not_a_startup_terminal() {
    let (mut machine, join) = long_joining();
    let accepted = succeed(&mut machine, join, 20, 1, 5, 0, None);
    let Some(ConsumerGroupHeartbeatEffect::AwaitAssignment { schedule, .. }) =
        accepted.into_effects().next()
    else {
        panic!("assignmentless startup acceptance")
    };
    let due = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("steady heartbeat due: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) = due.into_effects().next()
    else {
        panic!("steady submission")
    };
    let _ = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ConsumerGroupHeartbeatFailure::Execution,
        })
        .unwrap_or_else(|error| panic!("steady terminal: {error}"));
    assert!(machine.fatal().is_some());
    assert!(machine.startup_fatal().is_none());
}

fn rediscover(
    machine: &mut ConsumerGroupHeartbeatMachine,
    rejected: ConsumerGroupHeartbeatAttempt,
    now: u64,
    code: i16,
) -> (
    ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatRetrySchedule,
) {
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: rejected,
            now: moment(now),
            failure: ConsumerGroupHeartbeatFailure::Broker(code),
        })
        .unwrap_or_else(|error| panic!("rediscovery: {error}"));
    rediscovery_effects(transition)
}

fn rediscovery_effects(
    transition: ConsumerGroupHeartbeatTransition,
) -> (
    ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatRetrySchedule,
) {
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Rediscover {
            attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            deadline: original,
            ..
        },
        ConsumerGroupHeartbeatEffect::ArmRediscoveryRetry { schedule },
    ] = effects.as_slice()
    else {
        panic!("one rediscovery and one retry delay")
    };
    assert_eq!(*original, deadline(ORIGINAL_DEADLINE));
    assert_eq!(schedule.attempt(), *attempt);
    assert_eq!(
        schedule.cause(),
        ConsumerGroupHeartbeatRetryCause::Rediscovery
    );
    (*attempt, *schedule)
}

fn due(
    machine: &mut ConsumerGroupHeartbeatMachine,
    schedule: ConsumerGroupHeartbeatRetrySchedule,
) -> ConsumerGroupHeartbeatEffect {
    machine
        .apply(ConsumerGroupHeartbeatInput::CoordinatorLoadRetryDue {
            schedule,
            now: moment(schedule.not_before().tick()),
        })
        .unwrap_or_else(|error| panic!("rediscovery retry due: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("retry submission"))
}

fn assert_submit(effect: &ConsumerGroupHeartbeatEffect, attempt: ConsumerGroupHeartbeatAttempt) {
    assert!(matches!(
        effect,
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: actual,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            deadline: original,
            ..
        } if *actual == attempt && *original == deadline(ORIGINAL_DEADLINE)
    ));
}

fn long_joining() -> (ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatAttempt) {
    let mut machine = super::test_support::machine();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::Begin {
            now: moment(10),
            deadline: deadline(ORIGINAL_DEADLINE),
        })
        .unwrap_or_else(|error| panic!("begin Join: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt, .. }) =
        transition.into_effects().next()
    else {
        panic!("Join attempt")
    };
    (machine, attempt)
}
