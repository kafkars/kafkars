//! Deterministic KIP-848 reportable ownership, empty acknowledgement, and target installation.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    test_support::{
        deadline, due_attempt, epoch, member, moment, partition, staged_reconciliation, succeed,
    },
};

#[test]
fn replacement_keeps_old_ownership_and_heartbeats_new_epoch_while_target_waits() {
    let mut machine = staged_reconciliation();
    let current = machine
        .live_assignment()
        .unwrap_or_else(|| panic!("reportable assignment"));
    let target = machine
        .pending_assignment()
        .unwrap_or_else(|| panic!("pending target"));
    assert_eq!(current.assignment_generation().get(), 1);
    assert_eq!(current.partitions(), [partition(1, 0)]);
    assert_eq!(target.assignment_generation().get(), 2);
    assert_eq!(target.partitions(), [partition(1, 1)]);

    let schedule = machine.schedule().unwrap_or_else(|| panic!("cadence"));
    assert_eq!(schedule.assignment_generation().get(), 1);
    assert_eq!(schedule.attempt().member_epoch(), Some(epoch(2)));
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatDue {
            schedule,
            now: moment(schedule.deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("draining heartbeat: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Submit {
            attempt,
            kind: ConsumerGroupHeartbeatRequestKind::Steady,
            member_epoch: Some(member_epoch),
            assignment_generation: Some(assignment_generation),
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("draining cadence must report the old assignment")
    };
    assert_eq!(*member_epoch, epoch(2));
    assert_eq!(assignment_generation.get(), 1);

    let transition = succeed(&mut machine, *attempt, 31, 2, 5, 0, None);
    assert!(matches!(
        transition.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule })
            if schedule.assignment_generation().get() == 1
    ));
    assert_eq!(
        machine
            .pending_assignment()
            .map(|assignment| assignment.assignment_generation().get()),
        Some(2)
    );
}

#[test]
fn repeated_target_is_idempotent_but_changed_same_epoch_target_is_rejected() {
    let mut machine = staged_reconciliation();
    let attempt = due_attempt(&mut machine);
    let transition = succeed(
        &mut machine,
        attempt,
        31,
        2,
        5,
        0,
        Some(vec![partition(1, 1)]),
    );
    assert!(matches!(
        transition.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule })
            if schedule.assignment_generation().get() == 1
    ));

    let attempt = due_attempt(&mut machine);
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::HeartbeatSucceeded {
            attempt,
            now: moment(37),
            member_id: member(9),
            member_epoch: epoch(2),
            heartbeat_interval_ticks: 5,
            throttle_ticks: 0,
            assignment: Some(vec![partition(1, 2)]),
        })
        .err()
        .unwrap_or_else(|| panic!("changed target must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::AssignmentChangedWithoutEpoch
    );
    assert_eq!(machine.in_flight(), Some(attempt));
    assert_eq!(
        machine
            .pending_assignment()
            .map(|assignment| assignment.partitions()),
        Some(&[partition(1, 1)][..])
    );
}

#[test]
fn exact_retirement_emits_empty_owned_ack_then_success_authorizes_target_install() {
    let mut machine = staged_reconciliation();
    let old_generation = machine
        .live_assignment()
        .unwrap_or_else(|| panic!("old assignment"))
        .assignment_generation();
    let transition = machine
        .apply(ConsumerGroupHeartbeatInput::AssignmentRetired {
            now: moment(27),
            member_id: member(9),
            member_epoch: epoch(2),
            assignment_generation: old_generation,
        })
        .unwrap_or_else(|error| panic!("exact retirement: {error}"));
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::Submit {
            attempt: ack,
            kind: ConsumerGroupHeartbeatRequestKind::Steady,
            member_epoch: Some(member_epoch),
            assignment_generation: None,
            deadline: attempt_deadline,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("retirement must immediately acknowledge empty ownership")
    };
    assert_eq!(*member_epoch, epoch(2));
    assert_eq!(*attempt_deadline, deadline(37));
    assert!(machine.live_assignment().is_none());
    assert!(machine.pending_assignment().is_some());

    let transition = succeed(&mut machine, *ack, 28, 2, 7, 0, None);
    let effects = transition.into_effects().collect::<Vec<_>>();
    let [
        ConsumerGroupHeartbeatEffect::InstallReconciled {
            member_id,
            member_epoch,
            assignment_generation,
            schedule,
        },
    ] = effects.as_slice()
    else {
        panic!("ack success solely authorizes target installation")
    };
    assert_eq!(*member_id, member(9));
    assert_eq!(*member_epoch, epoch(2));
    assert_eq!(assignment_generation.get(), 2);
    assert_eq!(schedule.assignment_generation(), *assignment_generation);
    assert_eq!(machine.phase(), ConsumerGroupHeartbeatPhase::Stable);
    assert_eq!(
        machine
            .live_assignment()
            .map(|assignment| assignment.partitions()),
        Some(&[partition(1, 1)][..])
    );
    assert!(machine.pending_assignment().is_none());
}

#[test]
fn stale_retirement_fence_cannot_clear_reportable_ownership() {
    let mut machine = staged_reconciliation();
    let old_generation = machine
        .live_assignment()
        .unwrap_or_else(|| panic!("old assignment"))
        .assignment_generation();
    let stale_generation = old_generation
        .checked_next()
        .unwrap_or_else(|| panic!("next generation"));
    let error = machine
        .apply(ConsumerGroupHeartbeatInput::AssignmentRetired {
            now: moment(27),
            member_id: member(9),
            member_epoch: epoch(2),
            assignment_generation: stale_generation,
        })
        .err()
        .unwrap_or_else(|| panic!("stale retirement must reject"));
    assert_eq!(
        error.kind(),
        ConsumerGroupHeartbeatErrorKind::ReconciliationMismatch
    );
    assert_eq!(
        machine
            .live_assignment()
            .map(|assignment| assignment.assignment_generation()),
        Some(old_generation)
    );
    assert!(machine.pending_assignment().is_some());
}
