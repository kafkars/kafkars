//! Terminal and unchanged-target behavior around KIP-848 reconciliation ownership.

use super::{
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatRequestKind,
    test_support::{
        due_attempt, epoch, joining, moment, partition, staged_reconciliation, succeed,
    },
};

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
            if schedule.assignment_generation().get() == 1
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
