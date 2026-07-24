//! Exact assigned-owner reclamation after embedded-driver shutdown.

use std::{sync::Arc, time::Duration};

use super::{
    assigned_owner_close_test::ready_owner,
    assigned_owner_fault::{AssignedConsumerFaultKind, AssignedConsumerOwnerFault},
    assigned_owner_test::{driver, input, owner, shutdown},
    position_execution::{assignment, resolve_fence},
};
use kafka_client_core::{
    AssignedConsumerEffect, Deadline, FetchFailure, Moment, NextFetchOffset, StartPosition,
};

#[test]
fn accepted_close_settles_execution_unavailable_after_driver_shutdown() {
    let mut owner = owner(1);
    let observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    let mut driver = driver();
    shutdown(&mut driver);

    let recovery = owner.release_assigned_after_driver_shutdown();

    assert_eq!(recovery.close_completion_error(), None);
    assert_eq!(
        observer.wait(),
        Err(super::assigned_host::AssignedConsumerCloseObserverError::ExecutionUnavailable)
    );
}

#[test]
fn recovery_retries_an_injected_close_publication_backpressure() {
    let mut owner = owner(1);
    let observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    owner.inject_close_publish_fault(
        crate::completion::CompletionRegistryError::NotificationBackpressure,
    );
    let mut driver = driver();
    shutdown(&mut driver);

    let recovery = owner.release_assigned_after_driver_shutdown();

    assert_eq!(recovery.close_completion_error(), None);
    assert_eq!(
        observer.wait(),
        Err(super::assigned_host::AssignedConsumerCloseObserverError::ExecutionUnavailable)
    );
}

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

    assert!(recovery.requires_report());
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

#[test]
fn recovery_reports_exact_claimed_and_ready_event_counts() {
    let mut owner = owner(2);
    owner
        .replace_assignment(
            vec![
                input("orders", 0, StartPosition::Beginning),
                input("orders", 1, StartPosition::Offset(offset(10))),
            ],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let fence = owner
        .effects
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("Fetch claim"));
    let topic = Arc::clone(
        owner
            .topics
            .name(fence.position().partition().topic_id())
            .unwrap_or_else(|error| panic!("topic: {error:?}")),
    );
    owner
        .events
        .retain_terminal(
            topic,
            AssignedConsumerEffect::FetchFailed {
                fence,
                failure: FetchFailure::Transport,
            },
        )
        .unwrap_or_else(|(error, _topic)| panic!("retain event: {error:?}"));

    assert_eq!(owner.recovery_audit().event_retained(), (1, 1));
    let mut driver = driver();
    shutdown(&mut driver);
    let recovery = owner.release_assigned_after_driver_shutdown();

    assert_eq!(recovery.recovered_event_claims(), 1);
    assert_eq!(recovery.recovered_ready_events(), 1);
    assert!(recovery.requires_report());
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
