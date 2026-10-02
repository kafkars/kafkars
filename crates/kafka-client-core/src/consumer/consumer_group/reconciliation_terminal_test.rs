//! Terminal and unchanged-target behavior around KIP-848 reconciliation ownership.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatRequestKind,
    test_support::{
        deadline, due_attempt, epoch, heartbeating, joining, moment, partition,
        staged_reconciliation, succeed,
    },
};

#[test]
fn repeated_fenced_recovery_replaces_assignment_once_and_rejects_old_attempts() {
    let (mut machine, mut attempt) = heartbeating();
    let mut now = 26;
    for cycle in 0..256 {
        let generation = machine
            .live_assignment()
            .unwrap_or_else(|| panic!("live assignment"))
            .assignment_generation();
        let effects = machine
            .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                attempt,
                now: moment(now),
                failure: ConsumerGroupHeartbeatFailure::Broker(if cycle % 2 == 0 {
                    25
                } else {
                    110
                }),
            })
            .unwrap_or_else(|error| panic!("recover: {error}"))
            .into_effects()
            .collect::<Vec<_>>();
        let [
            ConsumerGroupHeartbeatEffect::Revoke { assignment },
            ConsumerGroupHeartbeatEffect::Submit {
                attempt: join,
                kind: ConsumerGroupHeartbeatRequestKind::Join,
                deadline: captured,
                ..
            },
        ] = effects.as_slice()
        else {
            panic!("one revoke and one join");
        };
        assert_eq!(assignment.assignment_generation(), generation);
        assert!(join.sequence() > attempt.sequence());
        assert_eq!(*captured, deadline(now + 10));
        assert!(machine.live_assignment().is_none());
        assert!(machine.pending_assignment().is_none());
        assert!(
            machine
                .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
                    attempt,
                    now: moment(now + 1),
                    failure: ConsumerGroupHeartbeatFailure::Broker(110),
                })
                .is_err()
        );
        assert_eq!(machine.in_flight(), Some(*join));
        let installed = succeed(
            &mut machine,
            *join,
            now + 1,
            cycle + 2,
            5,
            0,
            Some(vec![partition(1, u32::from(cycle % 2 == 0))]),
        );
        assert!(matches!(installed.into_effects().next(),
            Some(ConsumerGroupHeartbeatEffect::Reconcile { previous: None, assignment, .. })
                if assignment.assignment_generation().get() == generation.get() + 1
        ));
        attempt = due_attempt(&mut machine);
        now += 7;
    }
    let closed = machine
        .apply(ConsumerGroupHeartbeatInput::Close)
        .unwrap_or_else(|error| panic!("close: {error}"));
    assert_eq!(closed.effects().count(), 1);
    assert!(machine.live_assignment().is_none());
    assert!(machine.pending_assignment().is_none());
    assert!(machine.schedule().is_none());
}

#[test]
fn close_fatal_and_fenced_recovery_drop_target_and_revoke_only_still_live_assignment() {
    let mut closing = staged_reconciliation();
    let effects = closing
        .apply(ConsumerGroupHeartbeatInput::Close)
        .unwrap_or_else(|error| panic!("close: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        effects.as_slice(),
        [ConsumerGroupHeartbeatEffect::Revoke { assignment }]
            if assignment.assignment_generation().get() == 1
    ));
    assert!(closing.live_assignment().is_none());
    assert!(closing.pending_assignment().is_none());

    let mut fatal = staged_reconciliation();
    let attempt = due_attempt(&mut fatal);
    let effects = fatal
        .apply(ConsumerGroupHeartbeatInput::HeartbeatFailed {
            attempt,
            failure: ConsumerGroupHeartbeatFailure::Broker(27),
        })
        .unwrap_or_else(|error| panic!("fatal: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        effects.as_slice(),
        [
            ConsumerGroupHeartbeatEffect::Revoke { assignment },
            ConsumerGroupHeartbeatEffect::Fatal { .. }
        ] if assignment.assignment_generation().get() == 1
    ));
    assert!(fatal.pending_assignment().is_none());

    let mut recovering = staged_reconciliation();
    let attempt = due_attempt(&mut recovering);
    let effects = recovering
        .apply(ConsumerGroupHeartbeatInput::RecoverFencedMembership {
            attempt,
            now: moment(31),
            failure: ConsumerGroupHeartbeatFailure::Broker(110),
        })
        .unwrap_or_else(|error| panic!("recovery: {error}"))
        .into_effects()
        .collect::<Vec<_>>();
    assert!(matches!(
        effects.as_slice(),
        [
            ConsumerGroupHeartbeatEffect::Revoke { assignment },
            ConsumerGroupHeartbeatEffect::Submit {
                kind: ConsumerGroupHeartbeatRequestKind::Join,
                ..
            }
        ] if assignment.assignment_generation().get() == 1
    ));
    assert!(recovering.pending_assignment().is_none());
}

#[test]
fn equal_current_assignment_at_new_epoch_advances_without_local_replacement() {
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
    let attempt = due_attempt(&mut machine);
    let transition = succeed(
        &mut machine,
        attempt,
        26,
        2,
        5,
        0,
        Some(vec![partition(1, 0)]),
    );
    assert!(matches!(
        transition.into_effects().next(),
        Some(ConsumerGroupHeartbeatEffect::ArmHeartbeat { schedule })
            if schedule.assignment_generation().is_some_and(|generation| generation.get() == 1)
                && schedule.attempt().member_epoch() == Some(epoch(2))
    ));
    assert!(machine.pending_assignment().is_none());
    assert_eq!(
        machine
            .live_assignment()
            .map(|assignment| assignment.assignment_generation().get()),
        Some(1)
    );
}
