//! Exact retained Fetch accounting released only by post-driver recovery.

use kafka_client_core::{AssignedConsumerEffect, Moment};

use crate::clock::MonotonicClock;

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner, ClassicGroupFetchOwnerFaultKind,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn post_driver_recovery_reports_every_retained_group_fetch_owner() {
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 1, vec![committed(2, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let arm = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("Fetch throttle"));
    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &MonotonicClock::new()),
        ClassicGroupFetchFront::Interpreted
    );
    let AssignedConsumerEffect::ArmFetchThrottle { fence, .. } = arm else {
        panic!("armed Fetch fence");
    };
    owner
        .fetches
        .reserve_output_for_test(fence, 4_096)
        .unwrap_or_else(|error| panic!("output reservation: {error:?}"));
    owner.fetches.install_fault_for_test();
    owner.effects.push_back(AssignedConsumerEffect::Revoke {
        assignment_epoch: fence.position().assignment_epoch(),
        partition: fence.position().partition(),
    });
    assert_eq!(
        owner.interpret_front_effect(&catalog(&[]), &MonotonicClock::new()),
        ClassicGroupFetchFront::Idle
    );
    let unsettled = owner.unsettled();

    let recovery = owner.release_after_driver_shutdown();

    assert_eq!(unsettled, 6);
    assert_eq!(
        recovery.activation(),
        Some((position_fence(7), fence.position().assignment_epoch()))
    );
    assert_eq!(
        recovery.machine_assignment(),
        Some(fence.position().assignment_epoch())
    );
    assert_eq!(recovery.effects(), 1);
    assert_eq!(recovery.prepared(), 0);
    assert_eq!(recovery.timers(), 1);
    assert_eq!(recovery.fetch_retained(), (0, 1, 4_096));
    assert_eq!(recovery.recovered_fetch_requests(), 0);
    assert_eq!(recovery.fetch_completion(), None);
    assert!(recovery.fetch_executor_faulted());
    assert_eq!(recovery.recovered_events().claimed(), 1);
    assert_eq!(recovery.recovered_events().ready(), 0);
    assert_eq!(
        recovery.owner_fault(),
        Some(ClassicGroupFetchOwnerFaultKind::Fetch(
            crate::consumer::fetch_execution::FetchExecutionError::Faulted
        ))
    );
}
