//! Lossless position, core, and active-owner activation rejection scenarios.

use kafka_client_core::{
    AssignedConsumerMachine, InstallResolvedAssignment, InstallResolvedAssignmentErrorKind, Moment,
    RetireAssignment,
};

use super::{
    super::classic_group_position::ClassicGroupPositionActivationError,
    ClassicGroupFetchActivationErrorKind, ClassicGroupFetchActivationFailureKind,
    ClassicGroupFetchActivationFault, ClassicGroupFetchOwner, ClassicGroupFetchPostCoreFaultKind,
    ClassicGroupFetchPreflightError,
    owner::FIRST_GROUP_FETCH_PARTITIONS,
    test_support::{committed, completed_ready, position_fence},
};

#[test]
fn stale_position_fence_returns_the_exact_completed_owner_without_mutation() {
    let completed = completed_ready(
        position_fence(7),
        Moment::from_tick(41),
        0,
        vec![committed(2, 1, 17)],
    );
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    let failure = owner
        .try_activate(completed, position_fence(8))
        .err()
        .unwrap_or_else(|| panic!("stale fence must reject"))
        .into_returned()
        .unwrap_or_else(|| panic!("stale fence must return the completed owner"));

    assert_eq!(
        failure.kind(),
        ClassicGroupFetchActivationFailureKind::Position(
            ClassicGroupPositionActivationError::FenceMismatch {
                completed: position_fence(7),
                current: position_fence(8),
            }
        )
    );
    assert_eq!(failure.completed().fence(), position_fence(7));
    assert!(failure.rejected_input().is_none());
    assert!(owner.activation().is_none());
    assert_eq!(owner.machine_assignment_epoch(), None);

    let (completed, input) = failure.into_parts();
    assert_eq!(completed.fence(), position_fence(7));
    assert!(input.is_none());
}

#[test]
fn core_rejection_returns_both_exact_linear_inputs_and_leaves_owner_dormant() {
    let observed_at = Moment::from_tick(u64::MAX - 500_000);
    let completed = completed_ready(position_fence(7), observed_at, 1, vec![committed(2, 1, 17)]);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    let failure = owner
        .try_activate(completed, position_fence(7))
        .err()
        .unwrap_or_else(|| panic!("throttle deadline overflow must reject"))
        .into_returned()
        .unwrap_or_else(|| panic!("core rejection must return the completed owner"));

    assert_eq!(
        failure.kind(),
        ClassicGroupFetchActivationFailureKind::Core(
            InstallResolvedAssignmentErrorKind::InitialFetchThrottleDeadlineOverflow
        )
    );
    let rejected = failure
        .rejected_input()
        .unwrap_or_else(|| panic!("rejected resolved input expected"));
    assert_eq!(rejected.now(), observed_at);
    assert_eq!(rejected.throttle_ticks(), 1_000_000);
    assert!(owner.activation().is_none());
    assert_eq!(owner.machine_assignment_epoch(), None);

    let (completed, input) = failure.into_parts();
    assert_eq!(completed.fence(), position_fence(7));
    let input = input.unwrap_or_else(|| panic!("rejected input retained"));
    assert_eq!(input.partitions().len(), 1);
}

#[test]
fn post_core_fault_type_retains_both_mutated_owners_without_a_returned_rejection() {
    let completed = completed_ready(
        position_fence(7),
        Moment::from_tick(41),
        0,
        vec![committed(2, 1, 17)],
    );
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(InstallResolvedAssignment::new(
            None,
            Vec::new(),
            Moment::from_tick(41),
            0,
        ))
        .unwrap_or_else(|error| panic!("empty resolved assignment: {error}"));
    let transition = machine
        .retire_assignment(RetireAssignment::new(machine.assignment_epoch()))
        .unwrap_or_else(|error| panic!("retired assignment transition: {error}"));
    let fault = ClassicGroupFetchActivationFault::new(
        completed,
        transition,
        ClassicGroupFetchPostCoreFaultKind::MissingAssignmentEpoch,
    );

    assert_eq!(
        fault.kind(),
        ClassicGroupFetchPostCoreFaultKind::MissingAssignmentEpoch
    );
    assert_eq!(fault.completed().fence(), position_fence(7));
    assert_eq!(fault.transition().assignment_epoch(), None);
    assert_eq!(machine.assignment_epoch(), None);
    assert_eq!(
        super::activation::ClassicGroupFetchActivationError::Retained(fault.kind()).kind(),
        ClassicGroupFetchActivationErrorKind::Retained(
            ClassicGroupFetchPostCoreFaultKind::MissingAssignmentEpoch
        )
    );
}

#[test]
fn preflight_capacity_returns_copied_input_before_machine_or_claim_mutation() {
    let fence = position_fence(7);
    let facts = (0..=FIRST_GROUP_FETCH_PARTITIONS)
        .map(|partition| {
            committed(
                2,
                u32::try_from(partition).unwrap_or_else(|error| panic!("partition fits: {error}")),
                i64::try_from(partition).unwrap_or_else(|error| panic!("offset fits: {error}")),
            )
        })
        .collect();
    let completed = completed_ready(fence, Moment::from_tick(41), 0, facts);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

    let failure = owner
        .try_activate(completed, fence)
        .err()
        .unwrap_or_else(|| panic!("oversized activation must reject"))
        .into_returned()
        .unwrap_or_else(|| panic!("preflight must return exact owners"));

    assert_eq!(
        failure.kind(),
        ClassicGroupFetchActivationFailureKind::Preflight(
            ClassicGroupFetchPreflightError::PreparedCapacity {
                actual: FIRST_GROUP_FETCH_PARTITIONS + 1,
                limit: FIRST_GROUP_FETCH_PARTITIONS,
            }
        )
    );
    assert_eq!(failure.completed().fence(), fence);
    assert_eq!(
        failure
            .rejected_input()
            .unwrap_or_else(|| panic!("copied input retained"))
            .partitions()
            .len(),
        FIRST_GROUP_FETCH_PARTITIONS + 1
    );
    assert_eq!(owner.machine_assignment_epoch(), None);
    assert_eq!(owner.effect_count_for_test(), 0);
    assert_eq!(owner.events.retained(), (0, 0));
    assert!(owner.activation().is_none());
    assert!(owner.fault().is_none());
}
