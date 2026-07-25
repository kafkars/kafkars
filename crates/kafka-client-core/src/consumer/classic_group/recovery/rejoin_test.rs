//! Positive classic rejoin policy and immutable schedule evidence.

use crate::{AssignmentGeneration, Deadline};

use super::{
    ClassicRejoinPolicy, ClassicRejoinPolicyError, ClassicRejoinSchedule, MembershipCycle,
};

#[test]
fn policy_requires_two_positive_intervals() {
    assert_eq!(
        ClassicRejoinPolicy::try_new(0, 1),
        Err(ClassicRejoinPolicyError::ZeroBackoff)
    );
    assert_eq!(
        ClassicRejoinPolicy::try_new(1, 0),
        Err(ClassicRejoinPolicyError::ZeroAttemptTimeout)
    );
    let policy = ClassicRejoinPolicy::try_new(7, 11)
        .unwrap_or_else(|error| panic!("valid policy: {error:?}"));
    assert_eq!(policy.backoff_ticks(), 7);
    assert_eq!(policy.attempt_timeout_ticks(), 11);
}

#[test]
fn schedule_retains_cycle_assignment_and_absolute_due_fences() {
    let cycle = MembershipCycle::initial();
    let assignment = AssignmentGeneration::try_from_raw(3)
        .unwrap_or_else(|| panic!("nonzero assignment generation"));
    let schedule = ClassicRejoinSchedule::new(cycle, Some(assignment), Deadline::from_tick(99));
    assert_eq!(schedule.cycle(), cycle);
    assert_eq!(schedule.assignment_generation(), Some(assignment));
    assert_eq!(schedule.due(), Deadline::from_tick(99));
}
