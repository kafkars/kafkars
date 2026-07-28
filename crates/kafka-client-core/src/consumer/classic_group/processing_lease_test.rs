//! Deterministic application-processing lease transition evidence.

use crate::{
    AssignmentGeneration, Deadline, GroupId, Moment,
    consumer::{
        ClassicProcessingLease, ClassicProcessingLeaseEffect, ClassicProcessingLeaseError,
        ClassicProcessingLeaseExpiration, ClassicProcessingLeaseExpirationReason,
        ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy,
        ClassicProcessingLeasePolicyError, MembershipCycle,
    },
};

#[test]
fn policy_requires_positive_progress_time() {
    assert_eq!(
        ClassicProcessingLeasePolicy::try_new(0),
        Err(ClassicProcessingLeasePolicyError::TimeoutZero)
    );
    assert_eq!(
        ClassicProcessingLeasePolicy::try_new(9)
            .unwrap_or_else(|error| panic!("positive policy: {error:?}"))
            .timeout_ticks(),
        9
    );
}

#[test]
fn activation_and_progress_replace_one_exact_deadline() {
    let mut lease = lease(10);
    let fence = fence(1);

    let activated = lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(5),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    assert_arm(&activated, fence, 15);
    assert_eq!(lease.next_deadline().map(Deadline::tick), Some(15));

    let renewed = lease
        .apply(ClassicProcessingLeaseInput::Progress {
            fence,
            now: Moment::from_tick(14),
        })
        .unwrap_or_else(|error| panic!("renew: {error:?}"));
    assert_arm(&renewed, fence, 24);
    assert_eq!(lease.next_deadline().map(Deadline::tick), Some(24));
}

#[test]
fn progress_at_the_deadline_expires_instead_of_resurrecting_assignment() {
    let mut lease = active_lease(10, 5, fence(1));

    let expired = lease
        .apply(ClassicProcessingLeaseInput::Progress {
            fence: fence(1),
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("expire on boundary: {error:?}"));
    assert_expired(
        &expired,
        fence(1),
        15,
        ClassicProcessingLeaseExpirationReason::DeadlineElapsed,
    );
    assert_eq!(lease.next_deadline(), None);
    assert_eq!(
        lease
            .pending_expiration()
            .map(ClassicProcessingLeaseExpiration::reason),
        Some(ClassicProcessingLeaseExpirationReason::DeadlineElapsed)
    );
}

#[test]
fn early_and_cross_assignment_deadline_facts_do_not_mutate_owner() {
    let mut lease = active_lease(10, 5, fence(1));

    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence: fence(2),
            now: Moment::from_tick(15),
        }),
        Err(ClassicProcessingLeaseError::FenceMismatch)
    );
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence: fence(1),
            now: Moment::from_tick(14),
        }),
        Err(ClassicProcessingLeaseError::DeadlineNotElapsed)
    );
    assert_eq!(lease.next_deadline().map(Deadline::tick), Some(15));
}

#[test]
fn exact_due_observation_retains_expiration_until_exact_revoke() {
    let mut lease = active_lease(10, 5, fence(1));

    let expired = lease
        .apply(ClassicProcessingLeaseInput::DeadlineElapsed {
            fence: fence(1),
            now: Moment::from_tick(16),
        })
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));
    assert_expired(
        &expired,
        fence(1),
        15,
        ClassicProcessingLeaseExpirationReason::DeadlineElapsed,
    );
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::Progress {
            fence: fence(1),
            now: Moment::from_tick(16),
        }),
        Err(ClassicProcessingLeaseError::ExpirationPending)
    );
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::AssignmentRevoked { fence: fence(2) }),
        Err(ClassicProcessingLeaseError::FenceMismatch)
    );
    let released = lease
        .apply(ClassicProcessingLeaseInput::AssignmentRevoked { fence: fence(1) })
        .unwrap_or_else(|error| panic!("revoke: {error:?}"));
    assert_eq!(released.effects().count(), 0);
    assert_eq!(lease.pending_expiration(), None);
}

#[test]
fn progress_deadline_overflow_conservatively_expires_assignment() {
    let mut lease = active_lease(10, u64::MAX - 15, fence(1));

    let expired = lease
        .apply(ClassicProcessingLeaseInput::Progress {
            fence: fence(1),
            now: Moment::from_tick(u64::MAX - 6),
        })
        .unwrap_or_else(|error| panic!("overflow expiration: {error:?}"));
    assert_expired(
        &expired,
        fence(1),
        u64::MAX - 5,
        ClassicProcessingLeaseExpirationReason::DeadlineOverflow,
    );
}

#[test]
fn initial_deadline_overflow_leaves_the_owner_dormant() {
    let mut lease = lease(10);
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::Activate {
            fence: fence(1),
            now: Moment::from_tick(u64::MAX - 5),
        }),
        Err(ClassicProcessingLeaseError::DeadlineOverflow)
    );
    assert_eq!(lease.next_deadline(), None);
    assert_eq!(
        lease.apply(ClassicProcessingLeaseInput::AssignmentRevoked { fence: fence(1) }),
        Err(ClassicProcessingLeaseError::NotActive)
    );
}

fn lease(timeout: u64) -> ClassicProcessingLease {
    ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(timeout)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
    )
}

fn active_lease(
    timeout: u64,
    now: u64,
    fence: ClassicProcessingLeaseFence,
) -> ClassicProcessingLease {
    let mut lease = lease(timeout);
    lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("activate: {error:?}"));
    lease
}

fn fence(generation: u64) -> ClassicProcessingLeaseFence {
    ClassicProcessingLeaseFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::initial(),
        assignment_generation(generation),
    )
}

fn assignment_generation(raw: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(raw).unwrap_or_else(|| panic!("assignment generation"))
}

fn assert_arm(
    transition: &crate::consumer::ClassicProcessingLeaseTransition,
    fence: ClassicProcessingLeaseFence,
    deadline: u64,
) {
    let effects = transition.effects().copied().collect::<Vec<_>>();
    let [ClassicProcessingLeaseEffect::Arm { schedule }] = effects.as_slice() else {
        panic!("one arm effect: {effects:?}");
    };
    assert_eq!(schedule.fence(), fence);
    assert_eq!(schedule.deadline().tick(), deadline);
}

fn assert_expired(
    transition: &crate::consumer::ClassicProcessingLeaseTransition,
    fence: ClassicProcessingLeaseFence,
    deadline: u64,
    reason: ClassicProcessingLeaseExpirationReason,
) {
    let effects = transition.effects().copied().collect::<Vec<_>>();
    let [ClassicProcessingLeaseEffect::AssignmentLost { expiration }] = effects.as_slice() else {
        panic!("one assignment-loss effect: {effects:?}");
    };
    assert_eq!(expiration.schedule().fence(), fence);
    assert_eq!(expiration.schedule().deadline().tick(), deadline);
    assert_eq!(expiration.reason(), reason);
}
