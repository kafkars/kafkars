//! Exact acknowledgment, deadline, and owner-loss transition evidence.

use crate::{Deadline, Moment, consumer::AssignmentEpoch};

use super::{
    ClassicGracefulRevocation, ClassicGracefulRevocationEffect, ClassicGracefulRevocationError,
    ClassicGracefulRevocationInput, ClassicGracefulRevocationLease,
    ClassicGracefulRevocationLossReason, ClassicGracefulRevocationTerminal,
    ClassicGracefulRevocationTransition,
};

#[test]
fn begin_arms_the_one_exact_assignment_deadline() {
    let mut owner = ClassicGracefulRevocation::new();
    let lease = lease(epoch(1), 20);
    let transition = owner
        .apply(ClassicGracefulRevocationInput::Begin {
            lease,
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("begin: {error:?}"));

    assert_eq!(owner.active_lease(), Some(lease));
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(20)));
    assert_eq!(owner.terminal(), None);
    assert_eq!(
        effects(&transition),
        [ClassicGracefulRevocationEffect::Arm { lease }]
    );
}

#[test]
fn exact_acknowledgment_before_deadline_is_retained_as_success() {
    let lease = lease(epoch(1), 20);
    let mut owner = active(lease, 10);
    let transition = owner
        .apply(ClassicGracefulRevocationInput::Acknowledge {
            assignment_epoch: epoch(1),
            now: Moment::from_tick(19),
        })
        .unwrap_or_else(|error| panic!("acknowledge: {error:?}"));
    let terminal = ClassicGracefulRevocationTerminal::Acknowledged(lease);

    assert_eq!(owner.active_lease(), None);
    assert_eq!(owner.next_deadline(), None);
    assert_eq!(owner.terminal(), Some(terminal));
    assert_eq!(
        effects(&transition),
        [ClassicGracefulRevocationEffect::Complete { terminal }]
    );
}

#[test]
fn acknowledgment_at_deadline_loses_instead_of_resurrecting_success() {
    let lease = lease(epoch(1), 20);
    let mut owner = active(lease, 10);
    let transition = owner
        .apply(ClassicGracefulRevocationInput::Acknowledge {
            assignment_epoch: epoch(1),
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("boundary acknowledge: {error:?}"));
    let terminal = lost(lease, ClassicGracefulRevocationLossReason::DeadlineElapsed);

    assert_eq!(owner.terminal(), Some(terminal));
    assert_eq!(
        effects(&transition),
        [ClassicGracefulRevocationEffect::Complete { terminal }]
    );
    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::Acknowledge {
            assignment_epoch: epoch(1),
            now: Moment::from_tick(20),
        }),
        Err(ClassicGracefulRevocationError::TerminalRetained)
    );
    assert_eq!(owner.terminal(), Some(terminal));
}

#[test]
fn owner_loss_is_terminal_and_late_acknowledgment_cannot_succeed() {
    let lease = lease(epoch(1), 50);
    let mut owner = active(lease, 10);
    let transition = owner
        .apply(ClassicGracefulRevocationInput::OwnerLost {
            assignment_epoch: epoch(1),
        })
        .unwrap_or_else(|error| panic!("owner loss: {error:?}"));
    let terminal = lost(lease, ClassicGracefulRevocationLossReason::OwnerLost);

    assert_eq!(
        effects(&transition),
        [ClassicGracefulRevocationEffect::Complete { terminal }]
    );
    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::Acknowledge {
            assignment_epoch: epoch(1),
            now: Moment::from_tick(11),
        }),
        Err(ClassicGracefulRevocationError::TerminalRetained)
    );
    assert_eq!(owner.terminal(), Some(terminal));
}

#[test]
fn stale_epoch_and_early_deadline_facts_leave_active_owner_unchanged() {
    let lease = lease(epoch(2), 30);
    let mut owner = active(lease, 10);

    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::Acknowledge {
            assignment_epoch: epoch(1),
            now: Moment::from_tick(20),
        }),
        Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
    );
    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::OwnerLost {
            assignment_epoch: epoch(1),
        }),
        Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
    );
    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::DeadlineElapsed {
            assignment_epoch: epoch(2),
            now: Moment::from_tick(29),
        }),
        Err(ClassicGracefulRevocationError::DeadlineNotElapsed)
    );
    assert_eq!(owner.active_lease(), Some(lease));
    assert_eq!(owner.terminal(), None);
}

#[test]
fn elapsed_begin_uses_the_original_deadline_and_never_arms() {
    let lease = lease(epoch(1), 20);
    let mut owner = ClassicGracefulRevocation::new();
    let transition = owner
        .apply(ClassicGracefulRevocationInput::Begin {
            lease,
            now: Moment::from_tick(21),
        })
        .unwrap_or_else(|error| panic!("elapsed begin: {error:?}"));
    let terminal = lost(lease, ClassicGracefulRevocationLossReason::DeadlineElapsed);

    assert_eq!(owner.active_lease(), None);
    assert_eq!(owner.next_deadline(), None);
    assert_eq!(owner.terminal(), Some(terminal));
    assert_eq!(
        effects(&transition),
        [ClassicGracefulRevocationEffect::Complete { terminal }]
    );
}

#[test]
fn exact_terminal_release_allows_a_new_epoch_without_reusing_old_ownership() {
    let first = lease(epoch(1), 20);
    let second = lease(epoch(2), 40);
    let mut owner = active(first, 10);
    owner
        .apply(ClassicGracefulRevocationInput::OwnerLost {
            assignment_epoch: epoch(1),
        })
        .unwrap_or_else(|error| panic!("first loss: {error:?}"));

    assert_eq!(
        owner.apply(ClassicGracefulRevocationInput::Release {
            assignment_epoch: epoch(2),
        }),
        Err(ClassicGracefulRevocationError::AssignmentEpochMismatch)
    );
    let released = owner
        .apply(ClassicGracefulRevocationInput::Release {
            assignment_epoch: epoch(1),
        })
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert!(released.effects().next().is_none());
    assert_eq!(owner.terminal(), None);

    owner
        .apply(ClassicGracefulRevocationInput::Begin {
            lease: second,
            now: Moment::from_tick(30),
        })
        .unwrap_or_else(|error| panic!("second begin: {error:?}"));
    assert_eq!(owner.active_lease(), Some(second));
}

fn active(lease: ClassicGracefulRevocationLease, now: u64) -> ClassicGracefulRevocation {
    let mut owner = ClassicGracefulRevocation::new();
    owner
        .apply(ClassicGracefulRevocationInput::Begin {
            lease,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    owner
}

fn lease(assignment_epoch: AssignmentEpoch, deadline: u64) -> ClassicGracefulRevocationLease {
    ClassicGracefulRevocationLease::new(assignment_epoch, Deadline::from_tick(deadline))
}

fn epoch(raw: u64) -> AssignmentEpoch {
    let mut epoch = AssignmentEpoch::initial();
    for _ in 1..raw {
        epoch = epoch
            .checked_next()
            .unwrap_or_else(|| panic!("assignment epoch"));
    }
    epoch
}

fn lost(
    lease: ClassicGracefulRevocationLease,
    reason: ClassicGracefulRevocationLossReason,
) -> ClassicGracefulRevocationTerminal {
    ClassicGracefulRevocationTerminal::Lost { lease, reason }
}

fn effects(
    transition: &ClassicGracefulRevocationTransition,
) -> Vec<ClassicGracefulRevocationEffect> {
    transition.effects().copied().collect()
}
