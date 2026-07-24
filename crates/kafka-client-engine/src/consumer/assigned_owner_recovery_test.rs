//! Exact assigned-owner reclamation after embedded-driver shutdown.

use super::{
    assigned_owner_close_test::ready_owner,
    assigned_owner_fault::{AssignedConsumerFaultKind, AssignedConsumerOwnerFault},
    assigned_owner_test::{driver, shutdown},
    position_execution_test::{assignment, resolve_fence},
};
use kafka_client_core::{Deadline, Moment};

#[test]
fn recovery_releases_faulted_delivery_only_after_driver_shutdown() {
    let mut owner = ready_owner();
    let delivery = owner
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    owner.fetches.install_fault_for_test();
    assert!(owner.reclaim_delivery(delivery).is_err());
    owner.fault = Some(AssignedConsumerOwnerFault::Clock(
        crate::clock::ClockError::TickOverflow,
    ));
    let mut driver = driver();
    shutdown(&mut driver);

    let recovery = owner.release_assigned_after_driver_shutdown();

    assert!(recovery.had_fault());
    assert_eq!(
        recovery.owner_fault(),
        Some(AssignedConsumerFaultKind::Clock)
    );
    assert_eq!(recovery.reclaim_failures(), 1);
    assert_eq!(
        recovery.first_reclaim_failure(),
        Some(super::fetch_execution::FetchExecutionError::Faulted)
    );
    assert_eq!(recovery.recovered_fetch_requests(), 0);
}

#[test]
fn recovery_releases_a_retained_position_call_after_driver_shutdown() {
    let mut owner = ready_owner();
    let (effects, _machine) = assignment(&[3], Deadline::from_tick(20));
    owner
        .positions
        .install_terminal_for_test(resolve_fence(effects[0]), Moment::from_tick(5));
    let mut driver = driver();
    shutdown(&mut driver);

    let recovery = owner.release_assigned_after_driver_shutdown();

    assert_eq!(recovery.recovered_position_calls(), 1);
}
