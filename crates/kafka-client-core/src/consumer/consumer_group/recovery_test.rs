//! Fenced steady-member recovery ownership and identity scenarios.

use crate::{AssignmentGeneration, LiveGroupAssignment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind,
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, ConsumerGroupHeartbeatSequence,
    test_support::{deadline, epoch, heartbeating, joining, member, moment, partition, succeed},
};

#[test]
fn fenced_steady_member_revokes_then_rejoins_with_retained_identity_and_fresh_deadline() {
    let (mut machine, steady_attempt) = heartbeating();
    let expected_join_sequence = machine
        .next_sequence
        .unwrap_or_else(|| panic!("available recovery sequence"));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: steady_attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("recover fenced membership: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Revoke { assignment },
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: join_attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: Some(member_id),
            member_epoch: None,
            assignment_generation: None,
            deadline: recovery_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("fencing recovery must revoke before submitting one epoch-zero Join")
    };
    assert_eq!(assignment.member_id(), member(9));
    assert_eq!(assignment.assignment_generation().get(), 1);
    assert_eq!(assignment.partitions(), [partition(1, 0)]);
    assert_eq!(*member_id, member(9));
    assert_eq!(join_attempt.sequence(), expected_join_sequence);
    assert_ne!(*join_attempt, steady_attempt);
    assert_eq!(join_attempt.member_epoch(), None);
    assert_eq!(*recovery_deadline, deadline(36));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
    assert_eq!(machine.in_flight(), Some(*join_attempt));
    assert_eq!(machine.deadline, Some(deadline(36)));
    assert_eq!(machine.member_id, Some(member(9)));
    assert_eq!(machine.member_epoch(), None);
    assert!(machine.live_assignment().is_none());
    assert!(machine.schedule().is_none());
    let next_sequence = machine.next_sequence;
    let recovered = succeed(
        &mut machine,
        *join_attempt,
        30,
        2,
        5,
        0,
        Some(vec![partition(1, 1)]),
    );
    let Some(ConsumerGroupHeartbeatEffect::Reconcile {
        previous: None,
        assignment,
        ..
    }) = recovered.into_effects().next()
    else {
        panic!("recovered Join must install a fresh assignment")
    };
    assert_eq!(assignment.assignment_generation().get(), 2);
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Stable);
    assert_ne!(machine.next_sequence, next_sequence);
}
#[test]
fn unknown_member_and_fenced_epoch_are_the_only_recoverable_broker_codes() {
    for code in [25, 110] {
        let (mut machine, attempt) = heartbeating();
        let transition = machine
            .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                attempt,
                now: moment(26),
                failure: ConsumerGroupHeartbeatFailure::Broker(code),
            })
            .unwrap_or_else(|error| panic!("broker {code} recovery: {error}"));
        assert_eq!(transition.effects().count(), 2);
        assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Joining);
    }
    for failure in [
        ConsumerGroupHeartbeatFailure::Broker(14),
        ConsumerGroupHeartbeatFailure::Broker(111),
        ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        ConsumerGroupHeartbeatFailure::Execution,
    ] {
        let (mut machine, attempt) = heartbeating();
        let next_sequence = machine.next_sequence;
        let error = machine
            .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                attempt,
                now: moment(26),
                failure,
            })
            .err()
            .unwrap_or_else(|| panic!("unrelated failure must reject"));
        assert_eq!(
            error.kind(),
            ConsumerGroupHeartbeatErrorKind::FailureNotRecoverable
        );
        assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Heartbeating);
        assert_eq!(machine.in_flight(), Some(attempt));
        assert_eq!(machine.next_sequence, next_sequence);
        assert!(machine.live_assignment().is_some());
    }
}

#[test]
fn recovery_rejects_stale_and_wrong_phase_facts_without_mutation() {
    let (mut machine, current) = heartbeating();
    let stale = ConsumerGroupHeartbeatAttempt::new(
        ConsumerGroupHeartbeatSequence::try_from_raw(current.sequence().get() - 1)
            .unwrap_or_else(|| panic!("stale sequence")),
        current.member_epoch(),
    );
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: stale,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(25),
        })
        .err()
        .unwrap_or_else(|| panic!("stale recovery must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::AttemptMismatch
    );
    assert_eq!(machine.in_flight(), Some(current));
    assert!(machine.live_assignment().is_some());
    let (mut joining, join_attempt) = joining();
    let error = joining
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: join_attempt,
            now: moment(20),
            failure: ConsumerGroupHeartbeatFailure::Broker(25),
        })
        .err()
        .unwrap_or_else(|| panic!("initial Join recovery must reject"));
    assert_eq!(error.kind(), ConsumerGroupHeartbeatErrorKind::InvalidPhase);
    assert_eq!(joining.phase(), ConsumerGroupHeartbeatPhase::Joining);
    assert_eq!(joining.member_id, None);
}
#[test]
fn elapsed_original_attempt_terminalizes_before_a_fresh_recovery_can_start() {
    let (mut machine, attempt) = heartbeating();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(35),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("elapsed original attempt: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    assert!(matches!(
        effects.as_slice(),
        [
            ConsumerGroupHeartbeatEffect::Revoke { .. },
            ConsumerGroupHeartbeatEffect::Fatal { fatal },
        ] if fatal.attempt() == attempt
            && fatal.failure() == ConsumerGroupHeartbeatFailure::DeadlineElapsed
    ));
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Fatal);
    assert!(machine.live_assignment().is_none());
}
#[test]
fn recovery_deadline_overflow_and_identity_exhaustion_precede_mutation() {
    let (mut overflow, attempt) = heartbeating();
    overflow.deadline = Some(deadline(u64::MAX));
    let next_sequence = overflow.next_sequence;
    let error = overflow
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(u64::MAX - 5),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .err()
        .unwrap_or_else(|| panic!("overflowing recovery deadline must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::DeadlineOverflow
    );
    assert_eq!(overflow.next_sequence, next_sequence);
    assert_eq!(overflow.in_flight(), Some(attempt));
    assert!(overflow.live_assignment().is_some());
    let (mut exhausted, attempt) = heartbeating();
    exhausted.next_sequence = None;
    let error = exhausted
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .err()
        .unwrap_or_else(|| panic!("exhausted recovery attempt must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::AttemptExhausted
    );
    assert_eq!(exhausted.phase(), ConsumerGroupHeartbeatPhase::Heartbeating);
    assert_eq!(exhausted.in_flight(), Some(attempt));
    assert!(exhausted.live_assignment().is_some());
}
#[test]
fn recovery_rejects_inconsistent_member_epoch_and_assignment_identity() {
    let (mut wrong_epoch, attempt) = heartbeating();
    let inconsistent_attempt =
        ConsumerGroupHeartbeatAttempt::new(attempt.sequence(), Some(epoch(2)));
    wrong_epoch.in_flight = Some(inconsistent_attempt);
    let error = wrong_epoch
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: inconsistent_attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(25),
        })
        .err()
        .unwrap_or_else(|| panic!("inconsistent epoch must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::InvariantViolation
    );
    assert!(wrong_epoch.live_assignment().is_some());
    let (mut wrong_member, attempt) = heartbeating();
    wrong_member.live_assignment = Some(
        LiveGroupAssignment::try_new(
            wrong_member.group_id(),
            member(8),
            AssignmentGeneration::try_from_raw(1)
                .unwrap_or_else(|| panic!("assignment generation")),
            vec![partition(1, 0)],
        )
        .unwrap_or_else(|error| panic!("mismatched assignment fixture: {error}")),
    );
    let error = wrong_member
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .err()
        .unwrap_or_else(|| panic!("mismatched assignment identity must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::InvariantViolation
    );
    assert!(wrong_member.live_assignment().is_some());
}
#[test]
fn rediscovery_of_a_recovery_join_retains_the_member_identity() {
    let (mut machine, steady_attempt) = heartbeating();
    let recovery = machine
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt: steady_attempt,
            now: moment(26),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("recover fenced membership: {error}"));
    let Some(ConsumerGroupHeartbeatEffect::Submit {
        attempt: join_attempt,
        ..
    }) = recovery.into_effects().nth(1)
    else {
        panic!("recovery Join")
    };
    let retry = machine
        .apply(ConsumerGroupHeartbeatInput::RetryHeartbeat {
            attempt: join_attempt,
            now: moment(30),
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        })
        .unwrap_or_else(|error| panic!("rediscover recovery Join: {error}"));
    assert!(matches!(
        retry.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::Rediscover {
            attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            member_id: Some(member_id),
            member_epoch: None,
            assignment_generation: None,
            deadline: actual_deadline,
            ..
        }) if attempt != join_attempt
            && member_id == member(9)
            && actual_deadline == deadline(36)
    ));
}
