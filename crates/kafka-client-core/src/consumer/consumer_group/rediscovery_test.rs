//! Coordinator-rediscovery replacement bounds for KIP-848 heartbeat ownership.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    test_support::{deadline, heartbeating, joining, moment, partition, succeed},
};

#[test]
fn joining_coordinator_rediscovery_preserves_the_exact_attempt_deadline_and_facts() {
    let (mut machine, attempt) = joining();
    let next_sequence = machine.next_sequence;

    let execution = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Execution,
        })
        .err()
        .unwrap_or_else(|| panic!("execution failure must not authorize rediscovery"));
    assert_eq!(
        execution.kind(),
        super::ConsumerGroupHeartbeatErrorKind::FailureNotRetryable
    );

    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("join rediscovery: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Rediscover {
            attempt: replacement,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: None,
            member_epoch: None,
            assignment_generation: None,
            deadline: original_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("join retry must request one exact coordinator rediscovery")
    };
    assert_eq!(*replacement, attempt);
    assert_eq!(*original_deadline, deadline(40));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);
    assert_eq!(machine.deadline, Some(deadline(40)));
    assert!(machine.rediscovery_replacement_used);
}

#[test]
fn second_steady_rediscovery_for_the_same_attempt_revokes_and_terminalizes() {
    let (mut machine, attempt) = heartbeating();
    let next_sequence = machine.next_sequence;

    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("steady rediscovery: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Rediscover {
            attempt: replacement,
            kind: ConsumerGroupHeartbeatRequestKind::Steady,
            member_id: Some(member_id),
            member_epoch: Some(member_epoch),
            assignment_generation: Some(generation),
            deadline: original_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("steady retry must preserve every live membership fact")
    };
    assert_eq!(*replacement, attempt);
    assert_eq!(member_id.get(), 9);
    assert_eq!(member_epoch.get(), 1);
    assert_eq!(generation.get(), 1);
    assert_eq!(*original_deadline, deadline(35));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Heartbeating);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(machine.next_sequence, next_sequence);
    assert_eq!(machine.deadline, Some(deadline(35)));

    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(27),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("bounded rediscovery terminal: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        terminal[0],
        ConsumerGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        terminal[1],
        ConsumerGroupHeartbeatEffect::Fatal { fatal }
            if fatal.attempt() == attempt
                && fatal.failure() == ConsumerGroupHeartbeatFailure::CoordinatorUnavailable
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}

#[test]
fn elapsed_rediscovery_deadline_is_terminal_without_restarting_time() {
    let (mut machine, attempt) = heartbeating();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(35),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("elapsed rediscovery: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects[0],
        ConsumerGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        effects[1],
        ConsumerGroupHeartbeatEffect::Fatal { fatal }
            if fatal.attempt() == attempt
                && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}

#[test]
fn exact_coordinator_broker_codes_rediscover_once_then_retain_the_terminal_code() {
    for code in [15, 16] {
        let (mut machine, attempt) = joining();
        let retry = machine
            .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
                attempt,
                now: moment(20),
                failure: ConsumerGroupHeartbeatFailure::Broker(code),
            })
            .unwrap_or_else(|error| panic!("broker {code} rediscovery: {error}"));
        assert!(matches!(
            retry.into_effects().next(),
            Some(ConsumerGroupHeartbeatEffect::Rediscover {
                attempt: replacement,
                ..
            }) if replacement == attempt
        ));

        let terminal = machine
            .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
                attempt,
                now: moment(21),
                failure: ConsumerGroupHeartbeatFailure::Broker(code),
            })
            .unwrap_or_else(|error| panic!("broker {code} terminal: {error}"));
        assert!(matches!(
            terminal.into_effects().next(),
            Some(ConsumerGroupHeartbeatEffect::Fatal { fatal })
                if fatal.attempt() == attempt
                    && fatal.failure() == ConsumerGroupHeartbeatFailure::Broker(code)
        ));
    }
}

#[test]
fn unrelated_broker_code_does_not_authorize_rediscovery() {
    let (mut machine, attempt) = joining();
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Broker(14),
        })
        .err()
        .unwrap_or_else(|| panic!("unrelated broker code must reject"));
    assert_eq!(
        error.kind(),
        super::ConsumerGroupHeartbeatErrorKind::FailureNotRetryable
    );
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
    assert_eq!(machine.in_flight(), Some(attempt));
    assert!(!machine.rediscovery_replacement_used);
}

#[test]
fn rediscovery_rejects_a_stale_attempt() {
    let (mut heartbeating, stale) = joining();
    let _ = succeed(
        &mut heartbeating,
        stale,
        20,
        1,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    let schedule = heartbeating
        .schedule()
        .unwrap_or_else(|| panic!("armed heartbeat"));
    let _ = heartbeating
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(25),
        })
        .unwrap_or_else(|error| panic!("due heartbeat: {error}"));
    let stale_error = heartbeating
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: stale,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .err()
        .unwrap_or_else(|| panic!("stale retry must reject"));
    assert_eq!(
        stale_error.kind(),
        super::ConsumerGroupHeartbeatErrorKind::AttemptMismatch
    );
}

#[test]
fn leaving_coordinator_rediscovery_preserves_the_exact_attempt_deadline_and_facts() {
    let (mut machine, join) = joining();
    let _ = succeed(&mut machine, join, 20, 1, 5, 0, Some(vec![partition(1, 0)]));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::BeginLeave {
            now: moment(22),
            deadline: deadline(40),
        })
        .unwrap_or_else(|error| panic!("begin leave: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit { attempt: leave, .. }) =
        transition.into_effects().next()
    else {
        panic!("leave attempt")
    };
    let next_sequence = machine.next_sequence;
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: leave,
            now: moment(23),
            failure: ConsumerGroupHeartbeatFailure::Broker(16),
        })
        .unwrap_or_else(|error| panic!("leave rediscovery: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Rediscover {
            attempt: replacement,
            kind: ConsumerGroupHeartbeatRequestKind::Leave,
            member_id: Some(member_id),
            member_epoch: Some(member_epoch),
            assignment_generation: Some(generation),
            deadline: original_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("leave retry must preserve every live membership fact")
    };
    assert_eq!(*replacement, leave);
    assert_eq!(member_id.get(), 9);
    assert_eq!(member_epoch.get(), 1);
    assert_eq!(generation.get(), 1);
    assert_eq!(*original_deadline, deadline(40));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Leaving);
    assert_eq!(machine.in_flight(), Some(leave));
    assert_eq!(machine.next_sequence, next_sequence);
    assert_eq!(machine.deadline, Some(deadline(40)));

    let terminal = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: leave,
            now: moment(24),
            failure: ConsumerGroupHeartbeatFailure::Broker(16),
        })
        .unwrap_or_else(|error| panic!("bounded leave rediscovery terminal: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        terminal[0],
        ConsumerGroupHeartbeatEffect::Revoke { .. }
    ));
    assert!(matches!(
        terminal[1],
        ConsumerGroupHeartbeatEffect::Fatal { fatal }
            if fatal.attempt() == leave
                && fatal.failure() == ConsumerGroupHeartbeatFailure::Broker(16)
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
}
