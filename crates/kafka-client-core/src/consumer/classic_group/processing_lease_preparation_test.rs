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
