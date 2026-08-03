//! Prepared activation, deadline, and revocation evidence for processing-lease state.

use crate::{
    AssignmentGeneration, Deadline, GroupId, Moment,
    consumer::{
        ClassicProcessingLease, ClassicProcessingLeaseEffect, ClassicProcessingLeaseError,
        ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy,
        ClassicProcessingLeaseSchedule, MembershipCycle,
    },
};

#[test]
fn exact_deadline_retains_assignment_loss_until_matching_revoke() {
    let fence = ClassicProcessingLeaseFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("assignment")),
    );
    let mut lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(10)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
    );
    lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    assert_eq!(
        lease
            .active_schedule()
            .map(ClassicProcessingLeaseSchedule::fence),
        Some(fence)
    );

    let transition = lease
        .apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("expire: {error:?}"));
    let effects = transition.effects().copied().collect::<Vec<_>>();
    let [ClassicProcessingLeaseEffect::AssignmentLost { expiration }] = effects.as_slice() else {
        panic!("one assignment-loss effect");
    };
    assert_eq!(expiration.schedule().fence(), fence);
    assert_eq!(expiration.schedule().deadline().tick(), 15);
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::Progress {
            fence,
            now: Moment::from_tick(15),
        }),
        Err(ClassicProcessingLeaseError::ExpirationPending)
    );

    let released = lease
        .apply(ClassicProcessingLeaseInput::AssignmentRevoked { fence })
        .unwrap_or_else(|error| panic!("revoke: {error:?}"));
    assert_eq!(released.effects().count(), 0);
    assert_eq!(lease.pending_expiration(), None);
}

#[test]
fn dropped_preparation_leaves_dormant_and_commit_arms_exact_schedule() {
    let fence = ClassicProcessingLeaseFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("assignment")),
    );
    let mut lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(10)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
    );
    let prepared = lease
        .prepare_activation(fence, Moment::from_tick(5))
        .unwrap_or_else(|error| panic!("prepare: {error:?}"));
    assert_eq!(prepared.schedule().fence(), fence);
    assert_eq!(prepared.schedule().deadline().tick(), 15);
    drop(prepared);
    assert_eq!(lease.next_deadline(), None);

    let transition = lease
        .prepare_activation(fence, Moment::from_tick(7))
        .unwrap_or_else(|error| panic!("prepare again: {error:?}"))
        .commit();
    assert_eq!(lease.next_deadline().map(Deadline::tick), Some(17));
    assert_eq!(transition.effects().count(), 1);
}

#[test]
fn dropped_revocation_preserves_lease_and_commit_releases_it() {
    let fence = ClassicProcessingLeaseFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("assignment")),
    );
    let mut lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(10)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
    );
    lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    let prepared = lease
        .prepare_revocation(fence)
        .unwrap_or_else(|error| panic!("prepare revoke: {error:?}"));
    assert_eq!(prepared.fence(), fence);
    drop(prepared);
    assert_eq!(lease.next_deadline().map(Deadline::tick), Some(15));

    let transition = lease
        .prepare_revocation(fence)
        .unwrap_or_else(|error| panic!("prepare revoke again: {error:?}"))
        .commit();
    assert_eq!(transition.effects().count(), 0);
    assert_eq!(lease.next_deadline(), None);
}

#[test]
fn reconciliation_preserves_the_original_absolute_deadline_on_prepare_and_drop() {
    let expected = fence(1, 1);
    let replacement = fence(2, 2);
    let mut lease = active_lease(expected, 5);

    let prepared = lease
        .prepare_reconciliation(expected, replacement)
        .unwrap_or_else(|error| panic!("prepare reconciliation: {error:?}"));
    assert_eq!(prepared.schedule().fence(), replacement);
    assert_eq!(prepared.schedule().deadline(), Deadline::from_tick(15));
    drop(prepared);

    let retained = lease
        .active_schedule()
        .unwrap_or_else(|| panic!("original schedule retained"));
    assert_eq!(retained.fence(), expected);
    assert_eq!(retained.deadline(), Deadline::from_tick(15));
}

#[test]
fn reconciliation_validates_expected_fence_before_accepting_replacement() {
    let expected = fence(1, 1);
    let replacement = fence(2, 2);
    let wrong_expected = fence(1, 3);
    let mut lease = active_lease(expected, 5);

    assert_eq!(
        lease
            .prepare_reconciliation(wrong_expected, replacement)
            .err(),
        Some(ClassicProcessingLeaseError::FenceMismatch)
    );
    let schedule = lease
        .active_schedule()
        .unwrap_or_else(|| panic!("original schedule retained"));
    assert_eq!(schedule.fence(), expected);
    assert_eq!(schedule.deadline(), Deadline::from_tick(15));

    let prepared = lease
        .prepare_reconciliation(expected, replacement)
        .unwrap_or_else(|error| panic!("matching expected fence: {error:?}"));
    assert_eq!(prepared.schedule().fence(), replacement);
}

#[test]
fn reconciliation_failure_preserves_pending_expiration_losslessly() {
    let expected = fence(1, 1);
    let replacement = fence(2, 2);
    let mut lease = active_lease(expected, 5);
    lease
        .apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence: expected,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("expire: {error:?}"));
    let before = lease
        .pending_expiration()
        .unwrap_or_else(|| panic!("pending expiration"));

    assert_eq!(
        lease.prepare_reconciliation(expected, replacement).err(),
        Some(ClassicProcessingLeaseError::ExpirationPending)
    );
    assert_eq!(lease.pending_expiration(), Some(before));
    assert_eq!(lease.next_deadline(), None);
}

#[test]
fn reconciliation_commit_transfers_owner_and_arms_only_the_prepared_schedule() {
    let expected = fence(1, 1);
    let replacement = fence(2, 2);
    let mut lease = active_lease(expected, 5);

    let transition = lease
        .prepare_reconciliation(expected, replacement)
        .unwrap_or_else(|error| panic!("prepare reconciliation: {error:?}"))
        .commit();
    let effects = transition.effects().copied().collect::<Vec<_>>();
    let [ClassicProcessingLeaseEffect::Arm { schedule }] = effects.as_slice() else {
        panic!("one replacement Arm effect: {effects:?}");
    };
    assert_eq!(schedule.fence(), replacement);
    assert_eq!(schedule.deadline(), Deadline::from_tick(15));
    assert_eq!(lease.active_schedule(), Some(*schedule));
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::Progress {
            fence: expected,
            now: Moment::from_tick(6),
        }),
        Err(ClassicProcessingLeaseError::FenceMismatch)
    );
    assert_eq!(lease.active_schedule(), Some(*schedule));
}

fn active_lease(fence: ClassicProcessingLeaseFence, now: u64) -> ClassicProcessingLease {
    let mut lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(10)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
    );
    lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    lease
}

fn fence(cycle: u64, generation: u64) -> ClassicProcessingLeaseFence {
    ClassicProcessingLeaseFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(cycle).unwrap_or_else(|| panic!("cycle")),
        AssignmentGeneration::try_from_raw(generation).unwrap_or_else(|| panic!("assignment")),
    )
}
