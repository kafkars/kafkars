//! Exact single activation, binding, and uninterpreted effect ownership scenarios.

use kafka_client_core::{AssignedConsumerEffect, Moment};

use super::{
    ClassicGroupFetchActivationErrorKind, ClassicGroupFetchActivationFailureKind,
    ClassicGroupFetchOwner,
    test_support::{committed, completed_ready, position_fence},
};

#[test]
fn one_confirmed_position_installs_one_machine_and_retains_exact_binding() {
    let fence = position_fence(7);
    let completed = completed_ready(
        fence,
        Moment::from_tick(41),
        0,
        vec![committed(2, 1, 17), committed(2, 4, 23)],
    );
    let mut owner = ClassicGroupFetchOwner::new();

    owner
        .try_activate(completed, fence)
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));

    let activation = owner
        .activation()
        .unwrap_or_else(|| panic!("activation retained"));
    let binding = activation.binding();
    assert_eq!(binding.position_fence(), fence);
    assert_eq!(binding.assignment_epoch().get(), 1);
    assert_eq!(
        activation.transition().assignment_epoch(),
        Some(binding.assignment_epoch())
    );
    assert_eq!(
        owner.machine_assignment_epoch(),
        Some(binding.assignment_epoch())
    );
    assert_eq!(activation.transition().effects().len(), 2);
    assert!(activation.transition().effects().iter().all(|effect| {
        matches!(
            effect,
            AssignedConsumerEffect::FetchReady { fence, .. }
                if fence.position().assignment_epoch() == binding.assignment_epoch()
        )
    }));
}

#[test]
fn active_owner_rejects_a_second_completed_position_without_reinstallation() {
    let fence = position_fence(7);
    let mut owner = ClassicGroupFetchOwner::new();
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("first activation: {:?}", error.kind()));
    let installed_epoch = owner.machine_assignment_epoch();

    let second = completed_ready(fence, Moment::from_tick(42), 0, vec![committed(2, 1, 19)]);
    let failure = owner
        .try_activate(second, fence)
        .err()
        .unwrap_or_else(|| panic!("second activation must reject"));

    assert_eq!(
        failure.kind(),
        ClassicGroupFetchActivationErrorKind::Returned(
            ClassicGroupFetchActivationFailureKind::AlreadyActive
        )
    );
    let failure = failure
        .into_returned()
        .unwrap_or_else(|| panic!("active rejection returns the second completed owner"));
    assert_eq!(failure.completed().observed_at(), Moment::from_tick(42));
    assert!(failure.rejected_input().is_none());
    assert_eq!(owner.machine_assignment_epoch(), installed_epoch);
    assert_eq!(
        owner
            .activation()
            .unwrap_or_else(|| panic!("first activation retained"))
            .binding()
            .position_fence(),
        fence
    );
}
